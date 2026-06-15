use crate::config::{Protocol, Role, Tunnel, TunnelConfig};
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
const SESSION_TIMEOUT: Duration = Duration::from_secs(60);
const HEARTBEAT_MAGIC: &[u8] = b"IPBR";

// ===== Utility =====

fn resolve_addr(addr: &str, label: &str) -> SocketAddr {
    addr.to_socket_addrs()
        .ok()
        .and_then(|mut iter| iter.next())
        .unwrap_or_else(|| panic!("Failed to resolve {} address: {}", label, addr))
}

fn unspecified_addr(target: SocketAddr) -> SocketAddr {
    match target {
        SocketAddr::V4(_) => SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
        SocketAddr::V6(_) => SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0),
    }
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

// ===== Top-level =====

pub async fn run_proxy(config: TunnelConfig) {
    let mut handles = Vec::new();

    for tunnel in config.tunnel.into_iter().filter(|t| t.enable) {
        match tunnel.protocol {
            Protocol::Udp => match tunnel.role {
                Some(Role::Server) => handles.push(tokio::spawn(run_server_udp_tunnel(tunnel))),
                Some(Role::Client) => handles.push(tokio::spawn(run_client_udp_tunnel(tunnel))),
                None => warn!("UDP tunnel skipped: role field is required"),
            },
            Protocol::Tcp => handles.push(tokio::spawn(run_tcp_tunnel(tunnel))),
        }
    }

    for handle in handles {
        let _ = handle.await;
    }
}

// ===== UDP Server =====

struct UdpServerSession {
    socket: Arc<UdpSocket>,
    last_active: Instant,
    response_task: JoinHandle<()>,
}

pub async fn run_server_udp_tunnel(tunnel: Tunnel) {
    let listen_addr = resolve_addr(&tunnel.listen, "udp server listen");
    let target_addr = resolve_addr(&tunnel.forward, "udp target");

    let listener = Arc::new(
        UdpSocket::bind(listen_addr)
            .await
            .expect("UDP server bind failed"),
    );
    info!("UDP server listening on {} -> {}", listen_addr, target_addr);

    let sessions: Arc<Mutex<HashMap<SocketAddr, UdpServerSession>>> =
        Arc::new(Mutex::new(HashMap::new()));

    let cleanup = sessions.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(SESSION_TIMEOUT).await;
            cleanup_expired(&cleanup);
        }
    });

    let mut buf = vec![0u8; BUF_SIZE];

    loop {
        let (n, client_addr) = match listener.recv_from(&mut buf).await {
            Ok(v) => v,
            Err(e) => {
                warn!("UDP server receive error: {}", e);
                continue;
            }
        };

        if n == HEARTBEAT_MAGIC.len() && &buf[..n] == HEARTBEAT_MAGIC {
            if let Err(e) = listener.send_to(HEARTBEAT_MAGIC, client_addr).await {
                warn!("Heartbeat echo failed: {}", e);
            }
            continue;
        }

        let outbound =
            get_or_create_server_session(&sessions, client_addr, target_addr, listener.clone())
                .await;

        if let Err(e) = outbound.send_to(&buf[..n], target_addr).await {
            warn!("Failed to send packet to {}: {}", target_addr, e);
        }
    }
}

async fn get_or_create_server_session(
    sessions: &Arc<Mutex<HashMap<SocketAddr, UdpServerSession>>>,
    client_addr: SocketAddr,
    target_addr: SocketAddr,
    listener: Arc<UdpSocket>,
) -> Arc<UdpSocket> {
    cleanup_expired(sessions);

    if let Some(socket) = refresh_existing(sessions, client_addr) {
        return socket;
    }

    let socket = new_outbound().await;
    let response_task = spawn_server_response(socket.clone(), listener, client_addr, target_addr);

    sessions.lock().unwrap().insert(
        client_addr,
        UdpServerSession {
            socket: socket.clone(),
            last_active: Instant::now(),
            response_task,
        },
    );

    info!("New UDP session for client {}", client_addr);
    socket
}

fn cleanup_expired(sessions: &Arc<Mutex<HashMap<SocketAddr, UdpServerSession>>>) {
    let expired: Vec<SocketAddr> = {
        let map = sessions.lock().unwrap();
        map.iter()
            .filter(|(_, s)| Instant::now().duration_since(s.last_active) >= SESSION_TIMEOUT)
            .map(|(k, _)| *k)
            .collect()
    };
    for key in expired {
        if let Some(s) = sessions.lock().unwrap().remove(&key) {
            s.response_task.abort();
        }
    }
}

fn refresh_existing(
    sessions: &Arc<Mutex<HashMap<SocketAddr, UdpServerSession>>>,
    client_addr: SocketAddr,
) -> Option<Arc<UdpSocket>> {
    let mut map = sessions.lock().unwrap();
    let session = map.get_mut(&client_addr)?;
    session.last_active = Instant::now();
    Some(session.socket.clone())
}

async fn new_outbound() -> Arc<UdpSocket> {
    Arc::new(
        UdpSocket::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0))
            .await
            .expect("Failed to bind outbound UDP socket"),
    )
}

fn spawn_server_response(
    outbound: Arc<UdpSocket>,
    listener: Arc<UdpSocket>,
    client_addr: SocketAddr,
    target_addr: SocketAddr,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut buf = vec![0u8; BUF_SIZE];
        loop {
            match outbound.recv_from(&mut buf).await {
                Ok((n, src)) if src == target_addr => {
                    if let Err(e) = listener.send_to(&buf[..n], client_addr).await {
                        warn!("Failed to send response to {}: {}", client_addr, e);
                    }
                }
                Ok(_) => {}
                Err(e) => warn!("Response receive error: {}", e),
            }
        }
    })
}

// ===== UDP Client =====

struct UdpClientSession {
    client_addr: SocketAddr,
    last_active: Instant,
    response_task: JoinHandle<()>,
}

struct UdpClientCtx {
    upstream: Arc<UdpSocket>,
    local: Arc<UdpSocket>,
}

pub async fn run_client_udp_tunnel(tunnel: Tunnel) {
    let remote_addr = resolve_addr(&tunnel.forward, "udp remote");

    let upstream = Arc::new(
        UdpSocket::bind(unspecified_addr(remote_addr))
            .await
            .expect("UDP client bind failed"),
    );
    info!("UDP client forwarding to {}", remote_addr);

    let local_addr = resolve_addr(&tunnel.listen, "udp client listen");
    let local = Arc::new(
        UdpSocket::bind(local_addr)
            .await
            .unwrap_or_else(|_| panic!("UDP client listener bind {} failed", tunnel.listen)),
    );
    info!("UDP client listening on {}", local_addr);

    let ctx = UdpClientCtx { upstream, local };
    let mut buf = vec![0u8; BUF_SIZE];
    let mut session: Option<UdpClientSession> = None;

    loop {
        let (n, game_client_src) = match ctx.local.recv_from(&mut buf).await {
            Ok(v) => v,
            Err(e) => {
                warn!("UDP client receive error: {}", e);
                continue;
            }
        };

        if n == HEARTBEAT_MAGIC.len() && &buf[..n] == HEARTBEAT_MAGIC {
            if let Err(e) = ctx.local.send_to(HEARTBEAT_MAGIC, game_client_src).await {
                warn!("Heartbeat echo failed: {}", e);
            }
            continue;
        }

        session = handle_client_session(session, game_client_src, &ctx, remote_addr);

        if let Err(e) = ctx.upstream.send_to(&buf[..n], remote_addr).await {
            warn!("Failed to send packet to {}: {}", remote_addr, e);
        }
    }
}

fn handle_client_session(
    current: Option<UdpClientSession>,
    game_client_src: SocketAddr,
    ctx: &UdpClientCtx,
    remote_addr: SocketAddr,
) -> Option<UdpClientSession> {
    if let Some(client) = current {
        if Instant::now().duration_since(client.last_active) > SESSION_TIMEOUT {
            client.response_task.abort();
            return None;
        }
        if client.client_addr != game_client_src {
            error!(
                "Client mode: only one game client allowed (current: {}, new: {})",
                client.client_addr, game_client_src,
            );
            return Some(client);
        }
        return Some(UdpClientSession {
            last_active: Instant::now(),
            ..client
        });
    }

    let upstream = ctx.upstream.clone();
    let local = ctx.local.clone();
    let handle = tokio::spawn(async move {
        let mut buf = vec![0u8; BUF_SIZE];
        loop {
            match upstream.recv_from(&mut buf).await {
                Ok((n, src)) if src == remote_addr => {
                    if let Err(e) = local.send_to(&buf[..n], game_client_src).await {
                        warn!("Failed to send response to {}: {}", game_client_src, e);
                    }
                }
                Ok(_) => {}
                Err(e) => warn!("Response receive error: {}", e),
            }
        }
    });

    Some(UdpClientSession {
        client_addr: game_client_src,
        last_active: Instant::now(),
        response_task: handle,
    })
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
        Protocol::Udp => match tunnel.role {
            Some(Role::Server) => "UDP Server",
            Some(Role::Client) => "UDP Client",
            None => "UDP",
        },
        Protocol::Tcp => "TCP",
    };
    format!("[{}] {} -> {}", label, tunnel.listen, tunnel.forward)
}

pub async fn check_tunnel(tunnel: &Tunnel) {
    println!("{}", fmt_tunnel_header(tunnel));

    let listen_addr = match safe_resolve(&tunnel.listen) {
        Some(a) => {
            println!("  ✓ listen resolves to {}", a);
            a
        }
        None => {
            println!("  ✗ listen DNS resolution failed");
            return;
        }
    };

    let target_addr = match safe_resolve(&tunnel.forward) {
        Some(a) => {
            println!("  ✓ forward resolves to {}", a);
            a
        }
        None => {
            println!("  ✗ forward DNS resolution failed");
            return;
        }
    };

    match tunnel.protocol {
        Protocol::Tcp => {
            match TcpListener::bind(listen_addr).await {
                Ok(_) => println!("  ✓ listen port available"),
                Err(e) => println!("  ✗ listen port unavailable: {}", e),
            }
            match tokio::time::timeout(
                Duration::from_secs(3),
                tokio::net::TcpStream::connect(target_addr),
            )
            .await
            {
                Ok(Ok(_)) => println!("  ✓ TCP connect succeeded"),
                Ok(Err(e)) => println!("  ✗ TCP connect failed: {}", e),
                Err(_) => println!("  ✗ TCP connect timed out (3s)"),
            }
        }
        Protocol::Udp => {
            match UdpSocket::bind(listen_addr).await {
                Ok(s) => drop(s),
                Err(e) => {
                    println!("  ✗ listen port unavailable: {}", e);
                    return;
                }
            }
            println!("  ✓ listen port available");

            let probe = match UdpSocket::bind(unspecified_addr(target_addr)).await {
                Ok(s) => s,
                Err(e) => {
                    println!("  ✗ cannot create probe socket: {}", e);
                    return;
                }
            };

            if let Err(e) = probe.send_to(HEARTBEAT_MAGIC, target_addr).await {
                println!("  ✗ heartbeat send failed: {}", e);
                return;
            }

            let mut buf = vec![0u8; HEARTBEAT_MAGIC.len()];
            match tokio::time::timeout(Duration::from_secs(2), probe.recv_from(&mut buf)).await {
                Ok(Ok((n, src))) if n == HEARTBEAT_MAGIC.len() && buf[..n] == *HEARTBEAT_MAGIC => {
                    println!("  ✓ heartbeat echo from {} (ipbridge running)", src);
                }
                Ok(Ok((n, src))) => {
                    println!(
                        "  ⚠ response from {} ({n} bytes) not a valid heartbeat",
                        src
                    );
                }
                Ok(Err(e)) => {
                    println!("  ⚠ heartbeat receive error: {}", e);
                }
                Err(_) => {
                    println!(
                        "  ⚠ no heartbeat response within 2s (ipbridge may not be running on the remote end)"
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

    #[tokio::test]
    async fn handle_client_session_new_and_conflict() {
        let game_client_src: SocketAddr = "127.0.0.1:12345".parse().unwrap();
        let remote_addr: SocketAddr = "127.0.0.1:54321".parse().unwrap();
        let upstream = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let local = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let ctx = UdpClientCtx { upstream, local };

        let session = handle_client_session(None, game_client_src, &ctx, remote_addr);
        assert!(session.is_some());
        assert_eq!(session.as_ref().unwrap().client_addr, game_client_src);

        let handle = tokio::spawn(async {
            loop {
                tokio::time::sleep(Duration::from_secs(3600)).await;
            }
        });
        let existing = UdpClientSession {
            client_addr: "127.0.0.1:11111".parse().unwrap(),
            last_active: tokio::time::Instant::now(),
            response_task: handle,
        };
        let session2 = handle_client_session(Some(existing), game_client_src, &ctx, remote_addr);
        assert!(session2.is_some());
        assert_eq!(
            session2.unwrap().client_addr,
            "127.0.0.1:11111".parse().unwrap()
        );
    }

    #[tokio::test]
    async fn handle_client_session_expired() {
        let game_client_src: SocketAddr = "127.0.0.1:12345".parse().unwrap();
        let remote_addr: SocketAddr = "127.0.0.1:54321".parse().unwrap();
        let upstream = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let local = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let ctx = UdpClientCtx { upstream, local };

        let handle = tokio::spawn(async {
            loop {
                tokio::time::sleep(Duration::from_secs(3600)).await;
            }
        });
        let expired = UdpClientSession {
            client_addr: game_client_src,
            last_active: tokio::time::Instant::now() - Duration::from_secs(61),
            response_task: handle,
        };
        let session = handle_client_session(Some(expired), game_client_src, &ctx, remote_addr);
        assert!(session.is_none());
    }

    #[test]
    fn test_unspecified_addr_v4() {
        let target: SocketAddr = "1.2.3.4:5678".parse().unwrap();
        let result = unspecified_addr(target);
        assert_eq!(result.ip(), IpAddr::V4(Ipv4Addr::UNSPECIFIED));
        assert_eq!(result.port(), 0);
    }

    #[test]
    fn test_unspecified_addr_v6() {
        let target: SocketAddr = "[::1]:5678".parse().unwrap();
        let result = unspecified_addr(target);
        assert_eq!(result.ip(), IpAddr::V6(Ipv6Addr::UNSPECIFIED));
        assert_eq!(result.port(), 0);
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
        let sessions: Arc<Mutex<HashMap<SocketAddr, UdpServerSession>>> =
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
            UdpServerSession {
                socket: dummy,
                last_active: Instant::now() - Duration::from_secs(61),
                response_task: handle,
            },
        );

        cleanup_expired(&sessions);

        assert!(sessions.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_cleanup_expired_keeps_active_sessions() {
        let sessions: Arc<Mutex<HashMap<SocketAddr, UdpServerSession>>> =
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
            UdpServerSession {
                socket: dummy,
                last_active: Instant::now(),
                response_task: handle,
            },
        );

        cleanup_expired(&sessions);

        assert_eq!(sessions.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_refresh_existing_updates_last_active() {
        let sessions: Arc<Mutex<HashMap<SocketAddr, UdpServerSession>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let addr: SocketAddr = "127.0.0.1:10003".parse().unwrap();
        let dummy = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let handle = tokio::spawn(async {
            loop {
                tokio::time::sleep(Duration::from_secs(3600)).await;
            }
        });
        let old = Instant::now() - Duration::from_secs(30);
        sessions.lock().unwrap().insert(
            addr,
            UdpServerSession {
                socket: dummy,
                last_active: old,
                response_task: handle,
            },
        );

        let result = refresh_existing(&sessions, addr);
        assert!(result.is_some());

        let map = sessions.lock().unwrap();
        let session = map.get(&addr).unwrap();
        assert!(session.last_active > old);
    }

    #[tokio::test]
    async fn test_refresh_existing_returns_none_for_unknown() {
        let sessions: Arc<Mutex<HashMap<SocketAddr, UdpServerSession>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let addr: SocketAddr = "127.0.0.1:10004".parse().unwrap();
        assert!(refresh_existing(&sessions, addr).is_none());
    }
}
