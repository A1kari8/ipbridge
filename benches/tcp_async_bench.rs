use criterion::{criterion_group, criterion_main, Criterion};
use futures::future::join_all;
use ipbridge::config::{Protocol, Tunnel};
use ipbridge::proxy::run_tcp_tunnel;
use std::net::TcpListener as StdTcpListener;
use std::time::Duration;
use tokio::runtime::Runtime;

fn wait_for_port_ready(rt: &Runtime, port: u16) {
    // Try to connect to the proxy port until success or timeout
    let addr = format!("127.0.0.1:{}", port);
    let timeout = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < timeout {
        if rt.block_on(async { tokio::net::TcpStream::connect(&addr).await.is_ok() }) {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    // If not ready, proceed anyway (bench will show errors)
}

fn bench_tcp_proxy(_: &mut Criterion) {
    // create a dedicated runtime for async bench setup and tasks
    let rt = Runtime::new().expect("failed to create runtime");

    // start an async echo server inside the runtime
    let echo_addr = rt.block_on(async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let (mut s, _) = listener.accept().await.unwrap();
                tokio::spawn(async move {
                    use tokio::io::{AsyncReadExt, AsyncWriteExt};
                    let mut buf = [0u8; 8192];
                    loop {
                        match s.read(&mut buf).await {
                            Ok(0) | Err(_) => break,
                            Ok(n) => {
                                if s.write_all(&buf[..n]).await.is_err() {
                                    break;
                                }
                            }
                        }
                    }
                });
            }
        });
        addr
    });

    // reserve a free port for the proxy run
    let proxy_port = StdTcpListener::bind("127.0.0.1:0")
        .expect("failed to bind to get port")
        .local_addr()
        .unwrap()
        .port();

    let tunnel = Tunnel {
        protocol: Protocol::Tcp,
        role: None,
        listen: format!("127.0.0.1:{}", proxy_port),
        forward: echo_addr.to_string(),
        enable: true,
    };

    // spawn the proxy in background inside the runtime
    rt.spawn(async move {
        run_tcp_tunnel(tunnel).await;
    });

    // wait until the proxy is (likely) ready
    wait_for_port_ready(&rt, proxy_port);
    println!("Starting async TCP proxy bench against port {}", proxy_port);

    // configure Criterion to run longer and produce measurable results
    let mut c = Criterion::default()
        .configure_from_args()
        .sample_size(50)
        .measurement_time(Duration::from_secs(5));

    c.bench_function("tcp_proxy_async_64k_100conns", |b| {
        b.iter(|| {
            rt.block_on(async {
                let concurrency = 100usize;
                let payload = vec![0u8; 64 * 1024];
                let mut tasks = Vec::with_capacity(concurrency);
                for _ in 0..concurrency {
                    let addr = format!("127.0.0.1:{}", proxy_port);
                    let data = payload.clone();
                    tasks.push(tokio::spawn(async move {
                        let mut s = tokio::net::TcpStream::connect(&addr).await.expect("connect");
                        use tokio::io::AsyncWriteExt;
                        s.write_all(&data).await.expect("write");
                    }));
                }
                let _ = join_all(tasks).await;
            });
        })
    });

    c.final_summary();
}

criterion_group!(benches, bench_tcp_proxy);
criterion_main!(benches);
