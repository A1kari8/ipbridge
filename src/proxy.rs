use crate::config::{Protocol, Tunnel, TunnelConfig};
use log::{error, info, warn};
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, ToSocketAddrs};
use std::sync::{Arc, Mutex};
use tokio::io::copy_bidirectional;
use tokio::net::{TcpListener, TcpSocket, UdpSocket};
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant};

// ===== Constants =====

const BUF_SIZE: usize = 65_535;
const TCP_SND_BUF: usize = 256 * 1024;
const TCP_RCV_BUF: usize = 256 * 1024;
const MIN_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_TIMEOUT: Duration = Duration::from_secs(180);
const HEARTBEAT_MAGIC: &[u8] = b"IPBR";

fn session_timeout(max_gap: Duration) -> Duration {
    let t = max_gap * 3;
    if t > MAX_TIMEOUT {
        MAX_TIMEOUT
    } else if t < Duration::from_secs(30) {
        Duration::from_secs(30)
    } else {
        t
    }
}

// ===== Utility =====

fn resolve_addr(addr: &str, label: &str) -> SocketAddr {
    addr.to_socket_addrs()
        .ok()
        .and_then(|mut iter| iter.next())
        .unwrap_or_else(|| panic!("Failed to resolve {} address: {}", label, addr))
}

fn new_tcp_socket(addr: SocketAddr) -> Result<TcpSocket, tokio::io::Error> {
    match addr {
        SocketAddr::V4(_) => TcpSocket::new_v4(),
        SocketAddr::V6(_) => TcpSocket::new_v6(),
    }
}

fn configure_tcp_socket(socket: &TcpSocket) {
    let _ = socket.set_nodelay(true);
    let _ = socket.set_send_buffer_size(TCP_SND_BUF as u32);
    let _ = socket.set_recv_buffer_size(TCP_RCV_BUF as u32);
    let _ = socket.set_keepalive(true);
}

async fn connect_tcp(addr: SocketAddr) -> Option<tokio::net::TcpStream> {
    let socket = match new_tcp_socket(addr) {
        Ok(s) => s,
        Err(e) => {
            error!("Failed to create TCP socket: {}", e);
            return None;
        }
    };
    configure_tcp_socket(&socket);
    match socket.connect(addr).await {
        Ok(stream) => Some(stream),
        Err(e) => {
            error!("TCP connect to {} failed: {}", addr, e);
            None
        }
    }
}

async fn new_outbound() -> Arc<UdpSocket> {
    Arc::new(bind_udp_any().await.expect("Failed to bind UDP socket"))
}

async fn bind_udp_any() -> std::io::Result<UdpSocket> {
    let v6 = SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0);
    match UdpSocket::bind(v6).await {
        Ok(s) => Ok(s),
        Err(_) => {
            let v4 = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0);
            UdpSocket::bind(v4).await
        }
    }
}

// ===== Top-level =====

pub async fn run_proxy(config: TunnelConfig) {
    let mut handles = Vec::new();

    for tunnel in config.tunnel.into_iter().filter(|t| t.enable) {
        match tunnel.protocol {
            Protocol::Udp => handles.push(tokio::spawn(run_udp_tunnel(tunnel))),
            Protocol::Tcp => handles.push(tokio::spawn(run_tcp_tunnel(tunnel))),
        }
    }

    for handle in handles {
        let _ = handle.await;
    }
}

// ===== UDP Tunnel =====

struct UdpSession {
    socket: Arc<UdpSocket>,
    last_active: Instant,
    max_gap: Duration,
    response_task: JoinHandle<()>,
}

pub async fn run_udp_tunnel(tunnel: Tunnel) {
    let listen_addr = resolve_addr(&tunnel.listen, "udp listen");
    let target_addr = resolve_addr(&tunnel.forward, "udp target");

    let listener = Arc::new(UdpSocket::bind(listen_addr).await.expect("UDP bind failed"));
    info!("UDP tunnel listening on {} -> {}", listen_addr, target_addr);

    let sessions: Arc<Mutex<HashMap<SocketAddr, UdpSession>>> =
        Arc::new(Mutex::new(HashMap::new()));

    let cleanup = sessions.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(MAX_TIMEOUT / 2).await;
            cleanup_expired(&cleanup);
        }
    });

    let mut buf = vec![0u8; BUF_SIZE];

    loop {
        let (n, src_addr) = match listener.recv_from(&mut buf).await {
            Ok(v) => v,
            Err(e) => {
                warn!("UDP receive error: {}", e);
                continue;
            }
        };

        if n == HEARTBEAT_MAGIC.len() && &buf[..n] == HEARTBEAT_MAGIC {
            if let Err(e) = listener.send_to(HEARTBEAT_MAGIC, src_addr).await {
                warn!("Heartbeat echo failed: {}", e);
            }
            continue;
        }

        let session = get_or_create_session(&sessions, src_addr, listener.clone()).await;

        if let Err(e) = session.send_to(&buf[..n], target_addr).await {
            warn!("Failed to send packet to {}: {}", target_addr, e);
        }
    }
}

async fn get_or_create_session(
    sessions: &Arc<Mutex<HashMap<SocketAddr, UdpSession>>>,
    src_addr: SocketAddr,
    listener: Arc<UdpSocket>,
) -> Arc<UdpSocket> {
    cleanup_expired(sessions);

    {
        let mut map = sessions.lock().unwrap();
        if let Some(session) = map.get_mut(&src_addr) {
            let now = Instant::now();
            let gap = now - session.last_active;
            if gap > session.max_gap {
                session.max_gap = gap;
            }
            session.last_active = now;
            return session.socket.clone();
        }
    }

    let socket = new_outbound().await;
    let response_task = spawn_session_response(socket.clone(), listener, src_addr);

    sessions.lock().unwrap().insert(
        src_addr,
        UdpSession {
            socket: socket.clone(),
            last_active: Instant::now(),
            max_gap: MIN_TIMEOUT,
            response_task,
        },
    );

    info!("New UDP session for peer {}", src_addr);
    socket
}

fn spawn_session_response(
    session_socket: Arc<UdpSocket>,
    listener: Arc<UdpSocket>,
    peer_addr: SocketAddr,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut buf = vec![0u8; BUF_SIZE];
        loop {
            match session_socket.recv_from(&mut buf).await {
                Ok((n, _)) => {
                    if let Err(e) = listener.send_to(&buf[..n], peer_addr).await {
                        warn!("Failed to send response to {}: {}", peer_addr, e);
                    }
                }
                Err(e) => warn!("Session receive error: {}", e),
            }
        }
    })
}

fn cleanup_expired(sessions: &Arc<Mutex<HashMap<SocketAddr, UdpSession>>>) {
    let now = Instant::now();
    let expired: Vec<SocketAddr> = {
        let map = sessions.lock().unwrap();
        map.iter()
            .filter(|(_, s)| {
                let timeout = session_timeout(s.max_gap);
                now.duration_since(s.last_active) >= timeout
            })
            .map(|(k, _)| *k)
            .collect()
    };
    for key in expired {
        if let Some(s) = sessions.lock().unwrap().remove(&key) {
            s.response_task.abort();
        }
    }
}

// ===== TCP Tunnel =====

pub async fn run_tcp_tunnel(tunnel: Tunnel) {
    let forward_addr = resolve_addr(&tunnel.forward, "tcp forward");

    let listener = TcpListener::bind(&tunnel.listen)
        .await
        .expect("TCP bind failed");
    info!(
        "TCP tunnel listening on {} -> {}",
        tunnel.listen, tunnel.forward
    );

    loop {
        let (inbound, src) = match listener.accept().await {
            Ok(v) => v,
            Err(e) => {
                warn!("TCP accept error: {}", e);
                continue;
            }
        };

        tokio::spawn(handle_tcp_connection(inbound, src, forward_addr));
    }
}

async fn handle_tcp_connection(
    mut inbound: tokio::net::TcpStream,
    src: SocketAddr,
    forward_addr: SocketAddr,
) {
    let _ = inbound.set_nodelay(true);

    let mut outbound = match connect_tcp(forward_addr).await {
        Some(s) => s,
        None => return,
    };

    info!("TCP connection {} <-> {} established", src, forward_addr);

    match copy_bidirectional(&mut inbound, &mut outbound).await {
        Ok((c2s, s2c)) => info!(
            "TCP closed: {} <-> {} ({}b ->, {}b <-)",
            src, forward_addr, c2s, s2c,
        ),
        Err(e) => warn!("TCP forwarding error {} <-> {}: {}", src, forward_addr, e),
    }
}

// ===== Connectivity Check =====

fn safe_resolve(addr: &str) -> Option<SocketAddr> {
    addr.to_socket_addrs().ok().and_then(|mut iter| iter.next())
}

fn fmt_tunnel_header(tunnel: &Tunnel) -> String {
    let label = match tunnel.protocol {
        Protocol::Udp => "UDP",
        Protocol::Tcp => "TCP",
    };
    format!("[{}] {} -> {}", label, tunnel.listen, tunnel.forward)
}

pub async fn check_tunnel(tunnel: &Tunnel) {
    let ok = |s: String| format!("  \x1b[32m[ok]\x1b[0m {s}");
    let fail = |s: String| format!("  \x1b[31m[fail]\x1b[0m {s}");
    let warn = |s: String| format!("  \x1b[33m[warn]\x1b[0m {s}");

    println!("{}", fmt_tunnel_header(tunnel));

    let listen_addr = match safe_resolve(&tunnel.listen) {
        Some(a) => {
            println!("{}", ok(format!("listen resolves to {a}")));
            a
        }
        None => {
            println!("{}", fail("listen DNS resolution failed".into()));
            return;
        }
    };

    let target_addr = match safe_resolve(&tunnel.forward) {
        Some(a) => {
            println!("{}", ok(format!("forward resolves to {a}")));
            a
        }
        None => {
            println!("{}", fail("forward DNS resolution failed".into()));
            return;
        }
    };

    match tunnel.protocol {
        Protocol::Tcp => {
            match TcpListener::bind(listen_addr).await {
                Ok(_) => println!("{}", ok("listen port available".into())),
                Err(e) => println!("{}", fail(format!("listen port unavailable: {e}"))),
            }
            match tokio::time::timeout(
                Duration::from_secs(3),
                tokio::net::TcpStream::connect(target_addr),
            )
            .await
            {
                Ok(Ok(_)) => println!("{}", ok("TCP connect succeeded".into())),
                Ok(Err(e)) => println!("{}", fail(format!("TCP connect failed: {e}"))),
                Err(_) => println!("{}", fail("TCP connect timed out (3s)".into())),
            }
        }
        Protocol::Udp => {
            match UdpSocket::bind(listen_addr).await {
                Ok(s) => drop(s),
                Err(e) => {
                    println!("{}", fail(format!("listen port unavailable: {e}")));
                    return;
                }
            }
            println!("{}", ok("listen port available".into()));

            let probe = match bind_udp_any().await {
                Ok(s) => s,
                Err(e) => {
                    println!("{}", fail(format!("cannot create probe socket: {e}")));
                    return;
                }
            };

            if let Err(e) = probe.send_to(HEARTBEAT_MAGIC, target_addr).await {
                println!("{}", fail(format!("heartbeat send failed: {e}")));
                return;
            }

            let mut buf = vec![0u8; HEARTBEAT_MAGIC.len()];
            match tokio::time::timeout(Duration::from_secs(2), probe.recv_from(&mut buf)).await {
                Ok(Ok((n, src))) if n == HEARTBEAT_MAGIC.len() && buf[..n] == *HEARTBEAT_MAGIC => {
                    println!(
                        "{}",
                        ok(format!("heartbeat echo from {src} (ipbridge running)"))
                    );
                }
                Ok(Ok((n, src))) => {
                    println!(
                        "{}",
                        warn(format!(
                            "response from {src} ({n} bytes) not a valid heartbeat"
                        ))
                    );
                }
                Ok(Err(e)) => {
                    println!("{}", warn(format!("heartbeat receive error: {e}")));
                }
                Err(_) => {
                    println!(
                        "{}",
                        warn(
                            "no heartbeat response within 2s (ipbridge may not be running on the remote end)"
                                .into()
                        )
                    );
                }
            }
        }
    }
}

// ===== Tests =====

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn resolve_addr_ipv4() {
        let addr = resolve_addr("127.0.0.1:0", "test");
        assert!(addr.is_ipv4());
    }

    #[tokio::test]
    async fn resolve_addr_ipv6() {
        let addr = resolve_addr("[::1]:0", "test");
        assert!(addr.is_ipv6());
    }

    #[test]
    fn test_safe_resolve_valid() {
        assert!(safe_resolve("127.0.0.1:0").is_some());
    }

    #[test]
    fn test_safe_resolve_invalid_port() {
        assert!(safe_resolve("127.0.0.1:99999").is_none());
    }

    #[tokio::test]
    async fn test_cleanup_expired_removes_stale_sessions() {
        let sessions: Arc<Mutex<HashMap<SocketAddr, UdpSession>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let addr: SocketAddr = "127.0.0.1:10001".parse().unwrap();
        let dummy = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let handle = tokio::spawn(async {
            loop {
                tokio::time::sleep(Duration::from_secs(3600)).await;
            }
        });
        sessions.lock().unwrap().insert(
            addr,
            UdpSession {
                socket: dummy,
                last_active: Instant::now() - Duration::from_secs(61),
                max_gap: Duration::from_secs(10),
                response_task: handle,
            },
        );

        cleanup_expired(&sessions);

        assert!(sessions.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_cleanup_expired_keeps_active_sessions() {
        let sessions: Arc<Mutex<HashMap<SocketAddr, UdpSession>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let addr: SocketAddr = "127.0.0.1:10002".parse().unwrap();
        let dummy = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let handle = tokio::spawn(async {
            loop {
                tokio::time::sleep(Duration::from_secs(3600)).await;
            }
        });
        sessions.lock().unwrap().insert(
            addr,
            UdpSession {
                socket: dummy,
                last_active: Instant::now(),
                max_gap: Duration::from_secs(10),
                response_task: handle,
            },
        );

        cleanup_expired(&sessions);

        assert_eq!(sessions.lock().unwrap().len(), 1);
    }
}
