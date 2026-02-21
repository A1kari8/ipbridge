use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::sync::Arc;

use ipbridge::proxy::{BufferPool, BufferGuard};

fn bench_buffer_pool_get_put(c: &mut Criterion) {
    let pool = Arc::new(BufferPool::new());

    c.bench_function("buffer_pool_get_put", |b| {
        b.iter(|| {
            // create a guard, touch buffer, then drop
            let mut g = BufferGuard::new(Arc::clone(&pool));
            // write something to prevent optimizing away
            g[0] = black_box(1u8);
            // guard drops here and returns buffer
        })
    });
}

criterion_group!(benches, bench_buffer_pool_get_put);
criterion_main!(benches);
