use crate::compress::{self, Compression};
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
const MIN_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_TIMEOUT: Duration = Duration::from_secs(180);
const HEARTBEAT_MAGIC: &[u8] = b"IPBR";
const TCP_STATS_INTERVAL: Duration = Duration::from_secs(60);

fn session_timeout(max_gap: Duration) -> Duration {
    (max_gap * 3).clamp(MIN_TIMEOUT, MAX_TIMEOUT)
}

#[derive(Default)]
struct ByteStats {
    payload: u64,
    wire: u64,
}

impl ByteStats {
    fn add(&mut self, payload: u64, wire: u64) {
        self.payload += payload;
        self.wire += wire;
    }
}

fn fmt_compress_effect(payload: u64, wire: u64) -> String {
    if payload == 0 {
        return String::new();
    }
    let diff = (payload as f64 - wire as f64) / payload as f64 * 100.0;
    if diff >= 0.0 {
        format!("{:.1}% saved", diff)
    } else {
        format!("{:.1}% overhead", -diff)
    }
}

fn fmt_compress(c: Option<Compression>) -> String {
    match c {
        Some(c) => format!("compress {}:{}", c.codec.name(), c.level),
        None => "no compression".into(),
    }
}

// ===== Utility =====

fn resolve_addr(addr: &str, label: &str) -> SocketAddr {
    let mut addrs = addr
        .to_socket_addrs()
        .unwrap_or_else(|e| panic!("Failed to resolve {} address '{}': {}", label, addr, e));
    addrs
        .next()
        .unwrap_or_else(|| panic!("No addresses found for {} '{}'", label, addr))
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

pub async fn run(config: TunnelConfig) {
    let mut handles = Vec::new();

    for tunnel in config.tunnel.into_iter().filter(|t| t.enable) {
        match tunnel.protocol {
            Protocol::Udp => handles.push(tokio::spawn(run_udp(tunnel))),
            Protocol::Tcp => handles.push(tokio::spawn(run_tcp(tunnel))),
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
    stats: Arc<Mutex<ByteStats>>,
}

pub async fn run_udp(tunnel: Tunnel) {
    let listen_addr = resolve_addr(&tunnel.listen, "udp listen");
    let target_addr = resolve_addr(&tunnel.forward, "udp target");

    let sock2 = socket2::Socket::new(
        if listen_addr.is_ipv6() {
            socket2::Domain::IPV6
        } else {
            socket2::Domain::IPV4
        },
        socket2::Type::DGRAM,
        Some(socket2::Protocol::UDP),
    )
    .expect("Failed to create UDP socket");
    if listen_addr.is_ipv6() {
        sock2.set_only_v6(true).expect("Failed to set IPV6_V6ONLY");
    }
    sock2
        .set_nonblocking(true)
        .expect("Failed to set nonblocking");
    sock2.bind(&listen_addr.into()).expect("UDP bind failed");
    let listener =
        Arc::new(UdpSocket::from_std(sock2.into()).expect("Failed to create tokio socket"));
    info!(
        "UDP tunnel listening on {} -> {} ({})",
        listen_addr,
        target_addr,
        fmt_compress(tunnel.compress)
    );

    let sessions: Arc<Mutex<HashMap<SocketAddr, UdpSession>>> =
        Arc::new(Mutex::new(HashMap::new()));

    let cleanup = sessions.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(MAX_TIMEOUT / 2).await;
            cleanup_expired(&cleanup);
            log_udp_summary(&cleanup);
        }
    });

    let mut buf = vec![0u8; BUF_SIZE];
    let mut pbuf = vec![0u8; BUF_SIZE];
    let mut cbuf = Vec::new();

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

        let compress = tunnel.compress;
        let (packet, link) = match compress::decompress(&buf[..n], &mut pbuf) {
            Some(len) => (&pbuf[..len], true),
            None => (&buf[..n], false),
        };

        let (session, stats) =
            get_or_create_session(&sessions, src_addr, listener.clone(), link, compress).await;

        if let Some(c) = compress {
            if link {
                stats.lock().unwrap().add(packet.len() as u64, n as u64);
            }
            if !link {
                compress::compress_frame(c, packet, &mut cbuf);
                stats
                    .lock()
                    .unwrap()
                    .add(packet.len() as u64, cbuf.len() as u64);
                if let Err(e) = session.send_to(&cbuf, target_addr).await {
                    warn!("Failed to send packet to {}: {}", target_addr, e);
                }
            } else if let Err(e) = session.send_to(packet, target_addr).await {
                warn!("Failed to send packet to {}: {}", target_addr, e);
            }
        } else if let Err(e) = session.send_to(packet, target_addr).await {
            warn!("Failed to send packet to {}: {}", target_addr, e);
        }
    }
}

async fn get_or_create_session(
    sessions: &Arc<Mutex<HashMap<SocketAddr, UdpSession>>>,
    src_addr: SocketAddr,
    listener: Arc<UdpSocket>,
    link: bool,
    compress: Option<Compression>,
) -> (Arc<UdpSocket>, Arc<Mutex<ByteStats>>) {
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
            return (session.socket.clone(), session.stats.clone());
        }
    }

    let socket = new_outbound().await;
    let stats = Arc::new(Mutex::new(ByteStats::default()));
    let response_task = spawn_session_response(
        socket.clone(),
        listener,
        src_addr,
        compress,
        link,
        stats.clone(),
    );

    sessions.lock().unwrap().insert(
        src_addr,
        UdpSession {
            socket: socket.clone(),
            last_active: Instant::now(),
            max_gap: MIN_TIMEOUT,
            response_task,
            stats: stats.clone(),
        },
    );

    info!("New UDP session for peer {}", src_addr);
    (socket, stats)
}

fn spawn_session_response(
    session_socket: Arc<UdpSocket>,
    listener: Arc<UdpSocket>,
    peer_addr: SocketAddr,
    compress: Option<Compression>,
    link: bool,
    stats: Arc<Mutex<ByteStats>>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut buf = vec![0u8; BUF_SIZE];
        let mut pbuf = vec![0u8; BUF_SIZE];
        let mut cbuf = Vec::new();
        loop {
            match session_socket.recv_from(&mut buf).await {
                Ok((n, _)) => {
                    let packet = match compress::decompress(&buf[..n], &mut pbuf) {
                        Some(len) => {
                            stats.lock().unwrap().add(len as u64, n as u64);
                            &pbuf[..len]
                        }
                        None => &buf[..n],
                    };
                    if let Some(c) = compress {
                        if link {
                            compress::compress_frame(c, packet, &mut cbuf);
                            stats
                                .lock()
                                .unwrap()
                                .add(packet.len() as u64, cbuf.len() as u64);
                            if let Err(e) = listener.send_to(&cbuf, peer_addr).await {
                                warn!("Failed to send response to {}: {}", peer_addr, e);
                            }
                        } else if let Err(e) = listener.send_to(packet, peer_addr).await {
                            warn!("Failed to send response to {}: {}", peer_addr, e);
                        }
                    } else if let Err(e) = listener.send_to(packet, peer_addr).await {
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
            let st = s.stats.lock().unwrap();
            if st.payload > 0 {
                info!(
                    "UDP session {} closed: {} -> {} bytes ({})",
                    key,
                    st.payload,
                    st.wire,
                    fmt_compress_effect(st.payload, st.wire)
                );
            }
        }
    }
}

fn log_udp_summary(sessions: &Arc<Mutex<HashMap<SocketAddr, UdpSession>>>) {
    let (payload, wire) = {
        let map = sessions.lock().unwrap();
        map.values().fold((0u64, 0u64), |(p, w), s| {
            let st = s.stats.lock().unwrap();
            (p + st.payload, w + st.wire)
        })
    };
    if payload > 0 {
        info!(
            "UDP compression summary: {} -> {} bytes ({})",
            payload,
            wire,
            fmt_compress_effect(payload, wire)
        );
    }
}

// ===== TCP Tunnel =====

pub async fn run_tcp(tunnel: Tunnel) {
    let forward_addr = resolve_addr(&tunnel.forward, "tcp forward");

    let listener = TcpListener::bind(&tunnel.listen)
        .await
        .expect("TCP bind failed");
    info!(
        "TCP tunnel listening on {} -> {} ({})",
        tunnel.listen,
        tunnel.forward,
        fmt_compress(tunnel.compress)
    );

    loop {
        let (inbound, src) = match listener.accept().await {
            Ok(v) => v,
            Err(e) => {
                warn!("TCP accept error: {}", e);
                continue;
            }
        };

        tokio::spawn(handle_tcp_connection(
            inbound,
            src,
            forward_addr,
            tunnel.compress,
        ));
    }
}

async fn handle_tcp_connection(
    mut inbound: tokio::net::TcpStream,
    src: SocketAddr,
    forward_addr: SocketAddr,
    compress: Option<Compression>,
) {
    let _ = inbound.set_nodelay(true);

    let mut outbound = match connect_tcp(forward_addr).await {
        Some(s) => s,
        None => return,
    };

    info!("TCP connection {} <-> {} established", src, forward_addr);

    if let Some(c) = compress {
        let (in_r, in_w) = tokio::io::split(inbound);
        let (out_r, out_w) = tokio::io::split(outbound);
        let stats = Arc::new(Mutex::new(ByteStats::default()));
        let a = tokio::spawn(pump(in_r, out_w, c, stats.clone()));
        let b = tokio::spawn(pump(out_r, in_w, c, stats.clone()));
        let reporter = tokio::spawn(report_tcp(stats.clone(), src, forward_addr));
        let _ = tokio::join!(a, b);
        reporter.abort();
        let st = stats.lock().unwrap();
        if st.payload > 0 {
            info!(
                "TCP closed: {} <-> {} compressed {} -> {} bytes ({})",
                src,
                forward_addr,
                st.payload,
                st.wire,
                fmt_compress_effect(st.payload, st.wire)
            );
        }
    } else {
        match copy_bidirectional(&mut inbound, &mut outbound).await {
            Ok((c2s, s2c)) => info!(
                "TCP closed: {} <-> {} ({}b ->, {}b <-)",
                src, forward_addr, c2s, s2c,
            ),
            Err(e) => warn!("TCP forwarding error {} <-> {}: {}", src, forward_addr, e),
        }
    }
}

async fn report_tcp(stats: Arc<Mutex<ByteStats>>, src: SocketAddr, forward_addr: SocketAddr) {
    loop {
        tokio::time::sleep(TCP_STATS_INTERVAL).await;
        let st = stats.lock().unwrap();
        if st.payload > 0 {
            info!(
                "TCP active: {} <-> {} compressed {} -> {} bytes ({})",
                src,
                forward_addr,
                st.payload,
                st.wire,
                fmt_compress_effect(st.payload, st.wire)
            );
        }
    }
}

async fn pump<R, W>(
    mut reader: R,
    mut writer: W,
    compression: Compression,
    stats: Arc<Mutex<ByteStats>>,
) where
    R: tokio::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut head = [0u8; 4];
    let mut filled = 0;
    while filled < 4 {
        match reader.read(&mut head[filled..]).await {
            Ok(0) => break,
            Ok(n) => filled += n,
            Err(_) => break,
        }
    }
    if filled == 4 && &head == compress::MAGIC {
        pump_framed(&mut reader, &mut writer, head, &stats).await;
    } else {
        pump_plain(
            &mut reader,
            &mut writer,
            &head[..filled],
            compression,
            &stats,
        )
        .await;
    }
    let _ = writer.shutdown().await;
}

async fn pump_framed<R, W>(
    reader: &mut R,
    writer: &mut W,
    mut head: [u8; 4],
    stats: &Arc<Mutex<ByteStats>>,
) where
    R: tokio::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncWriteExt;

    let mut pbuf = vec![0u8; BUF_SIZE];
    loop {
        let mut header = [0u8; 6];
        if reader.read_exact(&mut header).await.is_err() {
            return;
        }
        let orig_len = u16::from_le_bytes([header[2], header[3]]) as usize;
        let pay_len = u16::from_le_bytes([header[4], header[5]]) as usize;
        if orig_len > BUF_SIZE || pay_len > BUF_SIZE + compress::HEADER_LEN {
            return;
        }
        let mut payload = vec![0u8; pay_len];
        if reader.read_exact(&mut payload).await.is_err() {
            return;
        }
        let mut block = Vec::with_capacity(compress::HEADER_LEN + pay_len);
        block.extend_from_slice(&head);
        block.extend_from_slice(&header);
        block.extend_from_slice(&payload);
        match compress::decompress(&block, &mut pbuf) {
            Some(len) => {
                stats.lock().unwrap().add(len as u64, block.len() as u64);
                if writer.write_all(&pbuf[..len]).await.is_err() {
                    return;
                }
            }
            None => return,
        }
        if reader.read_exact(&mut head).await.is_err() || &head != compress::MAGIC {
            return;
        }
    }
}

async fn pump_plain<R, W>(
    reader: &mut R,
    writer: &mut W,
    pending: &[u8],
    compression: Compression,
    stats: &Arc<Mutex<ByteStats>>,
) where
    R: tokio::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncWriteExt;

    let mut buf = vec![0u8; BUF_SIZE];
    let mut cbuf = Vec::new();

    if !pending.is_empty() {
        compress::compress_frame(compression, pending, &mut cbuf);
        stats
            .lock()
            .unwrap()
            .add(pending.len() as u64, cbuf.len() as u64);
        if writer.write_all(&cbuf).await.is_err() {
            return;
        }
    }
    loop {
        let n = match reader.read(&mut buf).await {
            Ok(0) => return,
            Ok(n) => n,
            Err(_) => return,
        };
        compress::compress_frame(compression, &buf[..n], &mut cbuf);
        stats.lock().unwrap().add(n as u64, cbuf.len() as u64);
        if writer.write_all(&cbuf).await.is_err() {
            return;
        }
    }
}

// ===== Connectivity Check =====

fn safe_resolve(addr: &str) -> Option<SocketAddr> {
    addr.to_socket_addrs().ok().and_then(|mut iter| iter.next())
}

fn is_domain_addr(addr: &str) -> bool {
    let host = if addr.starts_with('[') {
        let end = addr.find(']').unwrap_or(0);
        &addr[1..end]
    } else {
        let colon = addr.rfind(':').unwrap_or(0);
        &addr[..colon]
    };
    host.parse::<std::net::IpAddr>().is_err()
}

fn fmt_tunnel_header(tunnel: &Tunnel) -> String {
    let label = match tunnel.protocol {
        Protocol::Udp => "UDP",
        Protocol::Tcp => "TCP",
    };
    format!("[{}] {} -> {}", label, tunnel.listen, tunnel.forward)
}

pub async fn check(tunnel: &Tunnel) {
    let ok = |s: String| format!("  \x1b[32m[ok]\x1b[0m {s}");
    let fail = |s: String| format!("  \x1b[31m[fail]\x1b[0m {s}");
    let warn = |s: String| format!("  \x1b[33m[warn]\x1b[0m {s}");

    println!("{}", fmt_tunnel_header(tunnel));

    let listen_addr = match safe_resolve(&tunnel.listen) {
        Some(a) => {
            let note = if is_domain_addr(&tunnel.listen) {
                format!("listen \"{}\" resolves to {}", tunnel.listen, a)
            } else {
                format!("listen resolves to {a}")
            };
            println!("{}", ok(note));
            a
        }
        None => {
            println!("{}", fail("listen DNS resolution failed".into()));
            return;
        }
    };

    let target_addr = match safe_resolve(&tunnel.forward) {
        Some(a) => {
            let note = if is_domain_addr(&tunnel.forward) {
                format!("forward \"{}\" resolves to {}", tunnel.forward, a)
            } else {
                format!("forward resolves to {a}")
            };
            println!("{}", ok(note));
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
                            "heartbeat: no response within 2s (this is normal if forward is not another ipbridge)"
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
    use crate::compress::Codec;

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
                stats: Arc::new(Mutex::new(ByteStats::default())),
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
                stats: Arc::new(Mutex::new(ByteStats::default())),
            },
        );

        cleanup_expired(&sessions);

        assert_eq!(sessions.lock().unwrap().len(), 1);
    }

    #[test]
    fn is_domain_addr_returns_false_for_ipv4() {
        assert!(!is_domain_addr("127.0.0.1:7777"));
    }

    #[test]
    fn is_domain_addr_returns_false_for_ipv6() {
        assert!(!is_domain_addr("[::1]:7777"));
    }

    #[test]
    fn is_domain_addr_returns_true_for_hostname() {
        assert!(is_domain_addr("example.com:7777"));
    }

    #[test]
    fn is_domain_addr_returns_false_for_ipv4_port_range() {
        assert!(!is_domain_addr("0.0.0.0:9000-9005"));
    }

    #[tokio::test]
    async fn test_tcp_pump_framing_roundtrip() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let plain: Vec<u8> = (0..50_000u32)
            .flat_map(|i| format!("{:04x}", i % 4096).into_bytes())
            .collect();

        let (mut src_w, src_r) = tokio::io::duplex(1 << 20);
        let (dst_w, mut dst_r) = tokio::io::duplex(1 << 20);
        let stats = Arc::new(Mutex::new(ByteStats::default()));
        let stats_e = stats.clone();
        let encoder = tokio::spawn(async move {
            pump(src_r, dst_w, Compression::default(), stats_e).await;
        });
        src_w.write_all(&plain).await.unwrap();
        drop(src_w);
        encoder.await.unwrap();

        let mut framed = Vec::new();
        dst_r.read_to_end(&mut framed).await.unwrap();
        assert!(
            framed.len() < plain.len(),
            "compression should shrink traffic"
        );

        let (mut in_w, in_r) = tokio::io::duplex(1 << 20);
        let (out_w, mut out_r) = tokio::io::duplex(1 << 20);
        let decoder = tokio::spawn(async move {
            pump(in_r, out_w, Compression::default(), stats).await;
        });
        in_w.write_all(&framed).await.unwrap();
        drop(in_w);
        decoder.await.unwrap();

        let mut decoded = Vec::new();
        out_r.read_to_end(&mut decoded).await.unwrap();
        assert_eq!(decoded, plain);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_udp_compressed_tunnel_roundtrip() {
        let server_port: u16 = 24010;
        let client_port: u16 = 24011;
        let service_port: u16 = 24012;

        let server = Tunnel {
            protocol: Protocol::Udp,
            listen: format!("[::1]:{server_port}"),
            forward: format!("[::1]:{service_port}"),
            enable: true,
            compress: Some(Compression::new(Codec::Zlib, Some(9))),
        };
        let client = Tunnel {
            protocol: Protocol::Udp,
            listen: format!("[::1]:{client_port}"),
            forward: format!("[::1]:{server_port}"),
            enable: true,
            compress: Some(Compression::new(Codec::Zstd, None)),
        };

        let service = Arc::new(
            UdpSocket::bind(format!("[::1]:{service_port}"))
                .await
                .unwrap(),
        );
        let service_echo = service.clone();
        tokio::spawn(async move {
            let mut buf = vec![0u8; BUF_SIZE];
            loop {
                let Ok((n, from)) = service_echo.recv_from(&mut buf).await else {
                    return;
                };
                let _ = service_echo.send_to(&buf[..n], from).await;
            }
        });

        tokio::spawn(run_udp(server));
        tokio::spawn(run_udp(client));
        tokio::time::sleep(Duration::from_millis(100)).await;

        let app = UdpSocket::bind("[::1]:0").await.unwrap();
        app.connect(format!("[::1]:{client_port}")).await.unwrap();

        let payload: Vec<u8> = (0..4096u32)
            .flat_map(|i| format!("{:04x}", i % 4096).into_bytes())
            .collect();

        app.send(&payload).await.unwrap();
        let mut buf = vec![0u8; BUF_SIZE];
        let resp = tokio::time::timeout(Duration::from_secs(3), app.recv_from(&mut buf))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(resp.0, payload.len());
        assert_eq!(&buf[..resp.0], &payload[..]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_udp_client_only_compression_roundtrip() {
        let server_port: u16 = 24030;
        let client_port: u16 = 24031;
        let service_port: u16 = 24032;

        let server = Tunnel {
            protocol: Protocol::Udp,
            listen: format!("[::1]:{server_port}"),
            forward: format!("[::1]:{service_port}"),
            enable: true,
            compress: None,
        };
        let client = Tunnel {
            protocol: Protocol::Udp,
            listen: format!("[::1]:{client_port}"),
            forward: format!("[::1]:{server_port}"),
            enable: true,
            compress: Some(Compression::new(Codec::Zstd, None)),
        };

        let service = Arc::new(
            UdpSocket::bind(format!("[::1]:{service_port}"))
                .await
                .unwrap(),
        );
        let service_echo = service.clone();
        tokio::spawn(async move {
            let mut buf = vec![0u8; BUF_SIZE];
            loop {
                let Ok((n, from)) = service_echo.recv_from(&mut buf).await else {
                    return;
                };
                let _ = service_echo.send_to(&buf[..n], from).await;
            }
        });

        tokio::spawn(run_udp(server));
        tokio::spawn(run_udp(client));
        tokio::time::sleep(Duration::from_millis(100)).await;

        let app = UdpSocket::bind("[::1]:0").await.unwrap();
        app.connect(format!("[::1]:{client_port}")).await.unwrap();

        let compressible: Vec<u8> = (0..2048u32)
            .flat_map(|i| format!("{:04x}", i % 4096).into_bytes())
            .collect();
        let mut state: u64 = 0x9E3779B97F4A7C15;
        let incompressible: Vec<u8> = (0..600u32)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state as u8
            })
            .collect();

        for payload in [&compressible, &incompressible] {
            app.send(payload).await.unwrap();
            let mut buf = vec![0u8; BUF_SIZE];
            let resp = tokio::time::timeout(Duration::from_secs(3), app.recv_from(&mut buf))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(resp.0, payload.len());
            assert_eq!(&buf[..resp.0], &payload[..]);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tcp_compressed_tunnel_roundtrip() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let server_port: u16 = 24020;
        let client_port: u16 = 24021;
        let service_port: u16 = 24022;

        let server = Tunnel {
            protocol: Protocol::Tcp,
            listen: format!("[::1]:{server_port}"),
            forward: format!("[::1]:{service_port}"),
            enable: true,
            compress: Some(Compression::new(Codec::Zstd, Some(5))),
        };
        let client = Tunnel {
            protocol: Protocol::Tcp,
            listen: format!("[::1]:{client_port}"),
            forward: format!("[::1]:{server_port}"),
            enable: true,
            compress: Some(Compression::new(Codec::Zstd, None)),
        };

        let service_listener = TcpListener::bind(format!("[::1]:{service_port}"))
            .await
            .unwrap();
        tokio::spawn(async move {
            loop {
                let (sock, _) = service_listener.accept().await.unwrap();
                tokio::spawn(async move {
                    let (mut r, mut w) = tokio::io::split(sock);
                    let _ = tokio::io::copy(&mut r, &mut w).await;
                });
            }
        });

        tokio::spawn(run_tcp(server));
        tokio::spawn(run_tcp(client));
        tokio::time::sleep(Duration::from_millis(100)).await;

        let payload: Vec<u8> = (0..20_000u32)
            .flat_map(|i| format!("{:04x}", i % 4096).into_bytes())
            .collect();

        let mut app = tokio::net::TcpStream::connect(format!("[::1]:{client_port}"))
            .await
            .unwrap();
        app.write_all(&payload).await.unwrap();
        app.shutdown().await.unwrap();

        let mut decoded = Vec::new();
        app.read_to_end(&mut decoded).await.unwrap();
        assert_eq!(decoded, payload);
    }
}
