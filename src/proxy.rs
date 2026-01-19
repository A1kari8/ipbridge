use crate::config::{Protocol, Role, Tunnel, TunnelConfig};
use bytes::BytesMut;
use dashmap::DashMap;
use log::{error, info, warn};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, ToSocketAddrs};
use std::sync::{
    Arc,
    atomic::{AtomicU64, AtomicUsize, Ordering},
};
use tokio::io::copy_bidirectional;
use tokio::net::{TcpListener, TcpSocket, UdpSocket};

use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant};

/// Runs the main proxy logic, spawning tasks for each enabled tunnel.
/// Role semantics (UDP only):
/// - Role::Server => bridge is on the "near-game-client" side: listen on `listen`, forward to `forward`.
/// - Role::Client => bridge is on the "near-game-server" side: connect to `forward`, expose `listen` to the server.
/// TCP ignores role (acts as a generic forwarder listen <-> forward).
pub async fn run_proxy(config: TunnelConfig) {
    let mut handles = Vec::new();

    for tunnel in config.tunnel.into_iter().filter(|t| t.enable) {
        match tunnel.protocol {
            Protocol::Udp => match tunnel.role {
                Some(Role::Server) => handles.push(tokio::spawn(run_server_udp_tunnel(tunnel))),
                Some(Role::Client) => handles.push(tokio::spawn(run_client_udp_tunnel(tunnel))),
                None => warn!("UDP 隧道缺少 role=server/client，已跳过该条配置"),
            },
            Protocol::Tcp => handles.push(tokio::spawn(run_tcp_tunnel(tunnel))),
        }
    }

    futures::future::join_all(handles).await;
}

/// Represents an active UDP session in server mode.
/// Each session corresponds to a client connection, with its own virtual socket to the game server.
#[derive(Debug)]
struct UdpServerSession {
    /// The virtual UDP socket connected to the game server.
    socket: Arc<UdpSocket>,
    /// Timestamp of the last activity to manage session timeouts.
    last_active: Instant,
    /// Background task forwarding game-server responses back to the client.
    response_task: JoinHandle<()>,
}

/// Represents the state of a UDP client session in client mode.
/// Client mode only supports one active session at a time.
#[derive(Debug)]
struct UdpClientSession {
    /// The address of the connected game client.
    client_addr: SocketAddr,
    /// Timestamp of the last activity.
    last_active: Instant,
    /// Handle to the background task that forwards server responses.
    response_task: JoinHandle<()>,
}

type UdpSessionMap = DashMap<SocketAddr, UdpServerSession>;

// Buffer size for UDP I/O. Set large since connection count is low and we want to
// avoid UDP packet truncation and reduce the need for reassembly. 65535 covers
// the maximum UDP payload size.
const BUF_SIZE: usize = 65_535;

// TCP socket buffer sizes for higher throughput (tunable)
const TCP_SND_BUF: usize = 256 * 1024; // 256 KiB
const TCP_RCV_BUF: usize = 256 * 1024; // 256 KiB

use crossbeam_queue::SegQueue;

/// Simple lock-free buffer pool for reusing `BytesMut` instances to avoid frequent
/// allocations when the number of concurrent connections is small. Tracks simple
/// statistics: total allocations, reuses, and current pool size.
#[derive(Debug)]
struct BufferPool {
    inner: Arc<SegQueue<BytesMut>>,
    allocated: Arc<AtomicUsize>,
    reused: Arc<AtomicUsize>,
    pool_size: Arc<AtomicUsize>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
struct PoolStats {
    pub allocated: usize,
    pub reused: usize,
    pub pool_size: usize,
}

impl BufferPool {
    fn new() -> Self {
        Self {
            inner: Arc::new(SegQueue::new()),
            allocated: Arc::new(AtomicUsize::new(0)),
            reused: Arc::new(AtomicUsize::new(0)),
            pool_size: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn get(&self) -> BytesMut {
        match self.inner.pop() {
            Some(b) => {
                self.reused.fetch_add(1, Ordering::Relaxed);
                self.pool_size.fetch_sub(1, Ordering::Relaxed);
                b
            }
            None => {
                self.allocated.fetch_add(1, Ordering::Relaxed);
                let mut b = BytesMut::with_capacity(BUF_SIZE);
                b.resize(BUF_SIZE, 0);
                b
            }
        }
    }

    fn put(&self, mut buf: BytesMut) {
        buf.resize(BUF_SIZE, 0);
        self.pool_size.fetch_add(1, Ordering::Relaxed);
        self.inner.push(buf);
    }

    fn stats(&self) -> PoolStats {
        PoolStats {
            allocated: self.allocated.load(Ordering::Relaxed),
            reused: self.reused.load(Ordering::Relaxed),
            pool_size: self.pool_size.load(Ordering::Relaxed),
        }
    }
}

/// RAII guard that returns buffer to pool on Drop. This ensures buffers are
/// returned even if the task is cancelled.
struct BufferGuard {
    pool: Arc<BufferPool>,
    buf: Option<BytesMut>,
}

impl BufferGuard {
    fn new(pool: Arc<BufferPool>) -> Self {
        let buf = pool.get();
        Self {
            pool,
            buf: Some(buf),
        }
    }
}

impl std::ops::Deref for BufferGuard {
    type Target = BytesMut;
    fn deref(&self) -> &BytesMut {
        self.buf.as_ref().unwrap()
    }
}

impl std::ops::DerefMut for BufferGuard {
    fn deref_mut(&mut self) -> &mut BytesMut {
        self.buf.as_mut().unwrap()
    }
}

impl Drop for BufferGuard {
    fn drop(&mut self) {
        if let Some(b) = self.buf.take() {
            self.pool.put(b);
        }
    }
}

/// Spawn a background task that periodically logs `BufferPool` statistics.
fn spawn_stats_reporter(
    pool: Arc<BufferPool>,
    name: &'static str,
    interval: Duration,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(interval).await;
            let s = pool.stats();
            info!(
                "BufferPool[{}] stats: allocated={} reused={} pool_size={}",
                name, s.allocated, s.reused, s.pool_size
            );
        }
    })
}

/// Simple connection-level statistics for TCP tunnels.
#[derive(Debug)]
struct ConnectionStats {
    total_connections: AtomicUsize,
    active_connections: AtomicUsize,
    bytes_client_to_server: AtomicU64,
    bytes_server_to_client: AtomicU64,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
struct ConnStats {
    pub total_connections: usize,
    pub active_connections: usize,
    pub bytes_client_to_server: u64,
    pub bytes_server_to_client: u64,
}

impl ConnectionStats {
    fn new() -> Self {
        Self {
            total_connections: AtomicUsize::new(0),
            active_connections: AtomicUsize::new(0),
            bytes_client_to_server: AtomicU64::new(0),
            bytes_server_to_client: AtomicU64::new(0),
        }
    }

    fn incr_total(&self) {
        self.total_connections.fetch_add(1, Ordering::Relaxed);
    }

    fn incr_active(&self) {
        self.active_connections.fetch_add(1, Ordering::Relaxed);
    }

    fn decr_active(&self) {
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
    }

    fn add_client_to_server(&self, n: u64) {
        self.bytes_client_to_server.fetch_add(n, Ordering::Relaxed);
    }

    fn add_server_to_client(&self, n: u64) {
        self.bytes_server_to_client.fetch_add(n, Ordering::Relaxed);
    }

    fn snapshot(&self) -> ConnStats {
        ConnStats {
            total_connections: self.total_connections.load(Ordering::Relaxed),
            active_connections: self.active_connections.load(Ordering::Relaxed),
            bytes_client_to_server: self.bytes_client_to_server.load(Ordering::Relaxed),
            bytes_server_to_client: self.bytes_server_to_client.load(Ordering::Relaxed),
        }
    }
}

fn spawn_conn_stats_reporter(stats: Arc<ConnectionStats>, interval: Duration) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(interval).await;
            let s = stats.snapshot();
            info!(
                "TCP Connection stats: total={} active={} c2s={} s2c={}",
                s.total_connections,
                s.active_connections,
                s.bytes_client_to_server,
                s.bytes_server_to_client
            );
        }
    })
}

fn resolve_addr(addr: &str, label: &str) -> SocketAddr {
    addr.to_socket_addrs()
        .ok()
        .and_then(|mut iter| iter.next())
        .unwrap_or_else(|| panic!("Failed to resolve {} address: {}", label, addr))
}

/// Manages UDP server sessions: cleans up expired ones, creates new sessions if needed,
/// and returns the virtual socket for the client.
async fn manage_server_session(
    sessions: &UdpSessionMap,
    client_src_addr_v6: SocketAddr,
    target_addr: SocketAddr,
    proxy_listener: Arc<UdpSocket>,
    pool: Arc<BufferPool>,
) -> Arc<UdpSocket> {
    // Clean up expired sessions
    sessions.retain(|_, s| {
        let alive = Instant::now().duration_since(s.last_active) < Duration::from_secs(60);
        if !alive {
            s.response_task.abort();
        }
        alive
    });

    // Check if session exists
    if !sessions.contains_key(&client_src_addr_v6) {
        // Create new virtual socket
        let bind_addr = match target_addr {
            SocketAddr::V4(_) => SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
            SocketAddr::V6(_) => SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0),
        };

        let virtual_client = Arc::new(
            UdpSocket::bind(bind_addr)
                .await
                .expect("Failed to bind virtual socket"),
        );

        // Connect to game server
        if let Err(e) = virtual_client.connect(target_addr).await {
            error!("Failed to connect to {}: {}", target_addr, e);
            // Return dummy socket to skip
            return Arc::new(UdpSocket::bind(bind_addr).await.unwrap());
        }

        info!("New UDP session for client {}", client_src_addr_v6);

        // Spawn response forwarding task
        let proxy_listener_clone = Arc::clone(&proxy_listener);
        let virtual_client_clone = Arc::clone(&virtual_client);
        let virtual_addr = virtual_client.local_addr().unwrap();
        let tunnel_local = target_addr;

        let pool_clone = Arc::clone(&pool);
        let response_task = tokio::spawn(async move {
            // get RAII guard that returns buffer to pool on drop
            let mut response_buf = BufferGuard::new(pool_clone);
            loop {
                // 虚拟客户端接收来自游戏服务器的响应
                match virtual_client_clone.recv(&mut response_buf[..]).await {
                    Ok(bytes_received) => {
                        info!(
                            "Received {} bytes virtual client {} <-- game server {}",
                            bytes_received, client_src_addr_v6, tunnel_local
                        );
                        let bytes = response_buf.split_to(bytes_received).freeze();
                        match proxy_listener_clone
                            .send_to(&bytes, client_src_addr_v6)
                            .await
                        {
                            Ok(sent) => info!(
                                "Sent {} bytes virtual client proxy {} --> game client {}",
                                sent, virtual_addr, client_src_addr_v6
                            ),
                            Err(e) => warn!(
                                "Failed to send UDP response to client {}: {}",
                                client_src_addr_v6, e
                            ),
                        }
                        // refill buffer for next recv
                        response_buf.resize(BUF_SIZE, 0);
                    }
                    Err(e) => {
                        warn!("Receive error from game server on {}: {}", tunnel_local, e);
                        continue;
                    }
                }
            }
            // BufferGuard will return the buffer to the pool when dropped
        });

        // Insert new session
        sessions.insert(
            client_src_addr_v6,
            UdpServerSession {
                socket: virtual_client.clone(),
                last_active: Instant::now(),
                response_task,
            },
        );
    }

    // Update last active and return socket
    let mut session = sessions.get_mut(&client_src_addr_v6).unwrap();
    session.last_active = Instant::now();
    Arc::clone(&session.socket)
}

/// Runs a UDP tunnel in server mode.
/// Listens for client connections, creates virtual sockets to forward traffic to the game server.
/// Supports multiple concurrent clients, each with their own session.
pub async fn run_server_udp_tunnel(tunnel: Tunnel) {
    let proxy_bind_addr = resolve_addr(&tunnel.listen, "udp server listen");
    let target_addr = resolve_addr(&tunnel.forward, "udp target");

    let proxy_listener = Arc::new(
        UdpSocket::bind(proxy_bind_addr)
            .await
            .expect("UDP bind failed"),
    );
    let proxy_listener_addr = proxy_listener.local_addr().unwrap();
    info!("UDP proxy listener bound to {}", proxy_listener_addr);

    let sessions: UdpSessionMap = DashMap::new();
    let pool = Arc::new(BufferPool::new());
    // Spawn periodic stats reporter (quietly run in background)
    let _stats_handle = spawn_stats_reporter(Arc::clone(&pool), "server", Duration::from_secs(30));
    let mut recv_buf = BufferGuard::new(Arc::clone(&pool));

    loop {
        match proxy_listener.recv_from(&mut recv_buf[..]).await {
            Ok((len, client_src_addr)) => {
                let data = recv_buf.split_to(len).freeze();
                info!(
                    "Received {} bytes virtual client proxy {} <-- game client {}",
                    len, proxy_listener_addr, client_src_addr
                );

                let virtual_client = manage_server_session(
                    &sessions,
                    client_src_addr,
                    target_addr,
                    Arc::clone(&proxy_listener),
                    Arc::clone(&pool),
                )
                .await;

                if let Err(e) = virtual_client.send(data.as_ref()).await {
                    warn!("Failed to forward data: {}", e);
                } else {
                    info!(
                        "Sent {} bytes virtual client {} --> game server {}",
                        len,
                        virtual_client.local_addr().unwrap(),
                        target_addr
                    );
                }
            }
            Err(e) => warn!("Receive error: {}", e),
        }
    }
}

fn handle_client_session(
    current_session: Option<UdpClientSession>,
    game_client_src: SocketAddr,
    proxy_listener: Arc<UdpSocket>,
    remote_addr: SocketAddr,
    virtual_server: Arc<UdpSocket>,
    proxy_listener_addr: SocketAddr,
    virtual_server_addr: SocketAddr,
    pool: Arc<BufferPool>,
) -> Option<UdpClientSession> {
    if let Some(client) = current_session {
        if Instant::now().duration_since(client.last_active) > Duration::from_secs(60) {
            client.response_task.abort();
            None
        } else if client.client_addr != game_client_src {
            error!(
                "In client mode, only one game client is allowed. Current client: {}, new client: {}. Ignoring new client.",
                client.client_addr, game_client_src
            );
            Some(client)
        } else {
            Some(UdpClientSession {
                client_addr: client.client_addr,
                last_active: Instant::now(),
                response_task: client.response_task,
            })
        }
    } else {
        let proxy_listener_clone = Arc::clone(&proxy_listener);
        let remote_addr = remote_addr;
        let virtual_server_clone = Arc::clone(&virtual_server);

        let pool_clone = Arc::clone(&pool);
        let handle = tokio::spawn(async move {
            let mut response_buf = BufferGuard::new(pool_clone);
            loop {
                match proxy_listener_clone.recv(&mut response_buf[..]).await {
                    Ok(bytes_received) => {
                        info!(
                            "UDP response received {} bytes virtual server proxy {} <-- game server {}",
                            bytes_received, proxy_listener_addr, remote_addr
                        );
                        let bytes = response_buf.split_to(bytes_received).freeze();
                        if let Err(e) = virtual_server_clone.send_to(&bytes, game_client_src).await
                        {
                            error!("UDP response send failed: {}", e);
                        } else {
                            info!(
                                "UDP response sent {} bytes virtual server {} --> game client {}",
                                bytes_received, virtual_server_addr, game_client_src
                            );
                        }
                        response_buf.resize(BUF_SIZE, 0);
                    }
                    Err(e) => {
                        error!("UDP response receive failed: {}", e);
                        continue;
                    }
                }
            }
            // BufferGuard will return buffer to pool when dropped
        });
        Some(UdpClientSession {
            client_addr: game_client_src,
            last_active: Instant::now(),
            response_task: handle,
        })
    }
}

/// Runs a UDP tunnel in "client-side bridge" mode (near game server).
/// Exposes `local` to the game server and forwards to the remote bridge (`remote`).
/// Only one active game client session is supported to keep response routing correct.
pub async fn run_client_udp_tunnel(tunnel: Tunnel) {
    let remote_addr = resolve_addr(&tunnel.forward, "udp remote");
    let proxy_bind_addr = match remote_addr {
        SocketAddr::V4(_) => SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
        SocketAddr::V6(_) => SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0),
    };

    let proxy_listener = Arc::new(
        UdpSocket::bind(proxy_bind_addr)
            .await
            .expect("UDP bind failed"),
    );
    proxy_listener
        .connect(remote_addr)
        .await
        .expect("Server listener failed connect to remote");
    let proxy_listener_addr = proxy_listener
        .local_addr()
        .expect("Failed to get server listener bind addr");

    info!("UDP proxy listener bind {} ", proxy_listener_addr);

    let local_addr = resolve_addr(&tunnel.listen, "udp client listen");
    let virtual_server = Arc::new(
        UdpSocket::bind(local_addr)
            .await
            .expect(format!("UDP client listener bind {} failed", tunnel.listen).as_str()),
    );
    let virtual_server_addr = virtual_server
        .local_addr()
        .expect("Failed to get client listener addr");

    let pool = Arc::new(BufferPool::new());
    // Spawn periodic stats reporter (quietly run in background)
    let _stats_handle = spawn_stats_reporter(Arc::clone(&pool), "client", Duration::from_secs(30));
    let mut recv_buf = BufferGuard::new(Arc::clone(&pool));
    let mut current_game_client_session: Option<UdpClientSession> = None;

    loop {
        match virtual_server.recv_from(&mut recv_buf[..]).await {
            Ok((len, game_client_src)) => {
                let data = recv_buf.split_to(len).freeze();

                info!(
                    "UDP received {} bytes virtual server {} <-- game client {}",
                    len, local_addr, game_client_src
                );

                current_game_client_session = handle_client_session(
                    current_game_client_session,
                    game_client_src,
                    Arc::clone(&proxy_listener),
                    remote_addr,
                    Arc::clone(&virtual_server),
                    proxy_listener_addr,
                    virtual_server_addr,
                    Arc::clone(&pool),
                );

                if let Err(e) = proxy_listener.send(data.as_ref()).await {
                    warn!("UDP forward failed: {}", e);
                    continue;
                }
                info!(
                    "UDP sent {} bytes virtual server proxy {} --> game server {}",
                    len, proxy_listener_addr, remote_addr
                );
            }
            Err(e) => warn!("UDP receive failed: {}", e),
        }
    }
}

/// Runs a TCP tunnel.
/// Listens for incoming TCP connections and forwards them to the target address.
/// Supports multiple concurrent connections.
pub async fn run_tcp_tunnel(tunnel: Tunnel) {
    let listener = TcpListener::bind(&tunnel.listen)
        .await
        .expect("TCP bind failed");
    info!(
        "TCP tunnel listening on {} forwarding to {}",
        tunnel.listen, tunnel.forward
    );

    // Connection statistics and reporter
    let conn_stats = Arc::new(ConnectionStats::new());
    let _conn_stats_handle =
        spawn_conn_stats_reporter(Arc::clone(&conn_stats), Duration::from_secs(30));

    let forward_addr = resolve_addr(&tunnel.forward, "tcp forward");

    loop {
        match listener.accept().await {
            Ok((mut inbound, src)) => {
                // count total (accepted)
                conn_stats.incr_total();
                let stats_clone = Arc::clone(&conn_stats);
                let forward_addr_clone = forward_addr;
                tokio::spawn(async move {
                    // mark active
                    stats_clone.incr_active();

                    // reduce latency on inbound
                    if let Err(e) = inbound.set_nodelay(true) {
                        warn!("Failed to set TCP_NODELAY on inbound {}: {}", src, e);
                    }

                    // Build a TcpSocket and set options before connect
                    let socket = match forward_addr_clone {
                        SocketAddr::V4(_) => match TcpSocket::new_v4() {
                            Ok(s) => s,
                            Err(e) => {
                                error!("Failed to create TcpSocket: {}", e);
                                stats_clone.decr_active();
                                return;
                            }
                        },
                        SocketAddr::V6(_) => match TcpSocket::new_v6() {
                            Ok(s) => s,
                            Err(e) => {
                                error!("Failed to create TcpSocket: {}", e);
                                stats_clone.decr_active();
                                return;
                            }
                        },
                    };

                    // set socket options
                    if let Err(e) = socket.set_nodelay(true) {
                        warn!(
                            "Failed to set TCP_NODELAY on outbound socket {}: {}",
                            forward_addr_clone, e
                        );
                    }
                    if let Err(e) = socket.set_send_buffer_size(TCP_SND_BUF as u32) {
                        warn!(
                            "Failed to set SO_SNDBUF={} on outbound {}: {}",
                            TCP_SND_BUF, forward_addr_clone, e
                        );
                    } else {
                        info!(
                            "Set SO_SNDBUF={} on outbound {}",
                            TCP_SND_BUF, forward_addr_clone
                        );
                    }
                    if let Err(e) = socket.set_recv_buffer_size(TCP_RCV_BUF as u32) {
                        warn!(
                            "Failed to set SO_RCVBUF={} on outbound {}: {}",
                            TCP_RCV_BUF, forward_addr_clone, e
                        );
                    } else {
                        info!(
                            "Set SO_RCVBUF={} on outbound {}",
                            TCP_RCV_BUF, forward_addr_clone
                        );
                    }
                    // Enable keepalive (system default interval); tuning intervals requires platform-specific APIs
                    let _ = socket.set_keepalive(true);

                    match socket.connect(forward_addr_clone).await {
                        Ok(mut outbound) => {
                            info!(
                                "TCP connection established from {} to {}",
                                src, forward_addr_clone
                            );

                            match copy_bidirectional(&mut inbound, &mut outbound).await {
                                Ok((c2s, s2c)) => {
                                    stats_clone.add_client_to_server(c2s);
                                    stats_clone.add_server_to_client(s2c);
                                }
                                Err(e) => {
                                    warn!("TCP forward failed: {}", e);
                                }
                            }
                        }
                        Err(e) => {
                            error!(
                                "TCP connect to target failed: {} -> {}: {}",
                                src, forward_addr_clone, e
                            );
                        }
                    }

                    // connection finished
                    stats_clone.decr_active();
                });
            }
            Err(e) => warn!("TCP accept failed: {}", e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use tokio::net::UdpSocket;
    use tokio::time::Duration;

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
        let proxy_listener = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let proxy_listener_addr = proxy_listener.local_addr().unwrap();
        let virtual_server = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let virtual_server_addr = virtual_server.local_addr().unwrap();
        let remote_addr: SocketAddr = "127.0.0.1:54321".parse().unwrap();

        // New session
        let pool = Arc::new(BufferPool::new());
        let session = handle_client_session(
            None,
            game_client_src,
            Arc::clone(&proxy_listener),
            remote_addr,
            Arc::clone(&virtual_server),
            proxy_listener_addr,
            virtual_server_addr,
            Arc::clone(&pool),
        );
        assert!(session.is_some());
        assert_eq!(session.as_ref().unwrap().client_addr, game_client_src);

        // Conflict: existing different client
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
        let session2 = handle_client_session(
            Some(existing),
            game_client_src,
            Arc::clone(&proxy_listener),
            remote_addr,
            Arc::clone(&virtual_server),
            proxy_listener_addr,
            virtual_server_addr,
            Arc::clone(&pool),
        );
        assert!(session2.is_some());
        assert_eq!(
            session2.unwrap().client_addr,
            "127.0.0.1:11111".parse().unwrap()
        );
    }

    #[tokio::test]
    async fn handle_client_session_expired() {
        let game_client_src: SocketAddr = "127.0.0.1:12345".parse().unwrap();
        let proxy_listener = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let proxy_listener_addr = proxy_listener.local_addr().unwrap();
        let virtual_server = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let virtual_server_addr = virtual_server.local_addr().unwrap();
        let remote_addr: SocketAddr = "127.0.0.1:54321".parse().unwrap();

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
        let pool = Arc::new(BufferPool::new());
        let session = handle_client_session(
            Some(expired),
            game_client_src,
            Arc::clone(&proxy_listener),
            remote_addr,
            Arc::clone(&virtual_server),
            proxy_listener_addr,
            virtual_server_addr,
            Arc::clone(&pool),
        );
        assert!(session.is_none());
    }

    #[tokio::test]
    async fn buffer_pool_stats() {
        let pool = Arc::new(BufferPool::new());
        // initial stats
        let s0 = pool.stats();
        assert_eq!(s0.allocated, 0);
        assert_eq!(s0.reused, 0);
        assert_eq!(s0.pool_size, 0);

        // allocate via guard
        {
            let _g = BufferGuard::new(Arc::clone(&pool));
            let s1 = pool.stats();
            assert_eq!(s1.allocated, 1);
        }

        // guard dropped, buffer returned
        let s2 = pool.stats();
        assert_eq!(s2.pool_size, 1);

        // reuse should be observed
        {
            let _g = BufferGuard::new(Arc::clone(&pool));
            let s3 = pool.stats();
            assert_eq!(s3.reused, 1);
        }

        let s4 = pool.stats();
        assert_eq!(s4.pool_size, 1);
    }

    #[test]
    fn connection_stats_basic() {
        let stats = ConnectionStats::new();
        assert_eq!(stats.snapshot().total_connections, 0);
        assert_eq!(stats.snapshot().active_connections, 0);
        assert_eq!(stats.snapshot().bytes_client_to_server, 0);
        assert_eq!(stats.snapshot().bytes_server_to_client, 0);

        stats.incr_total();
        stats.incr_active();
        stats.add_client_to_server(123);
        stats.add_server_to_client(456);
        let s = stats.snapshot();
        assert_eq!(s.total_connections, 1);
        assert_eq!(s.active_connections, 1);
        assert_eq!(s.bytes_client_to_server, 123);
        assert_eq!(s.bytes_server_to_client, 456);

        stats.decr_active();
        let s2 = stats.snapshot();
        assert_eq!(s2.active_connections, 0);
    }
}
