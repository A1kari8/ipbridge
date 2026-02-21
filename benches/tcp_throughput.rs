use criterion::{criterion_group, criterion_main, Criterion};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;

fn run_echo_server(addr: &str) -> std::net::SocketAddr {
    let listener = TcpListener::bind(addr).expect("bind");
    let local = listener.local_addr().unwrap();
    thread::spawn(move || {
        for stream in listener.incoming() {
            match stream {
                Ok(mut s) => {
                    // echo server: read and discard
                    let mut buf = [0u8; 8 * 1024];
                    loop {
                        match s.read(&mut buf) {
                            Ok(0) => break,
                            Ok(_) => continue,
                            Err(_) => break,
                        }
                    }
                }
                Err(_) => break,
            }
        }
    });
    local
}

fn bench_tcp_throughput(c: &mut Criterion) {
    let addr = run_echo_server("127.0.0.1:0");

    c.bench_function("tcp_throughput_1mb", |b| {
        b.iter(|| {
            let mut s = TcpStream::connect(addr).unwrap();
            // send 1 MiB
            let data = vec![0u8; 1024 * 1024];
            s.write_all(&data).unwrap();
            // close
        })
    });
}

criterion_group!(benches, bench_tcp_throughput);
criterion_main!(benches);
