//! Criterion micro-benchmarks for the store hot path.
//!
//! These measure the in-process cost the macro (loadgen) benchmark can't isolate:
//! the slab allocator + open-addressed index on put/get. Run with:
//!
//!   cargo bench -p store
//!
//! HTML reports land in target/criterion/. Track them over commits with Bencher:
//!
//!   bencher run --adapter rust_criterion "cargo bench -p store"
//!
//! Value inputs are pre-allocated in the (untimed) batch setup so the numbers
//! reflect the store's own copy-into-slab + index work, not Vec allocation.

use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use std::hint::black_box;
use store::Store;

const VALSZ: usize = 128;
const KEYSPACE: u64 = 1 << 16; // bounded so the slab stays resident during a run

fn put(c: &mut Criterion) {
    let mut g = c.benchmark_group("store_put");
    g.throughput(Throughput::Elements(1));
    let val = vec![0xABu8; VALSZ];
    g.bench_function(BenchmarkId::new("put", "128B"), |b| {
        let store = Store::new();
        let mut i = 0u64;
        b.iter_batched(
            || {
                let key = (i % KEYSPACE).to_le_bytes().to_vec();
                i += 1;
                (key, val.clone())
            },
            |(k, v)| {
                black_box(store.put("m", k, v));
            },
            BatchSize::SmallInput,
        );
    });
    g.finish();
}

fn get(c: &mut Criterion) {
    let mut g = c.benchmark_group("store_get");
    g.throughput(Throughput::Elements(1));
    let val = vec![0xABu8; VALSZ];

    // Prepopulate, then measure hits.
    let store = Store::new();
    for i in 0..KEYSPACE {
        store.put("m", i.to_le_bytes().to_vec(), val.clone());
    }
    g.bench_function(BenchmarkId::new("get_hit", "128B"), |b| {
        let mut i = 0u64;
        b.iter(|| {
            let key = (i % KEYSPACE).to_le_bytes();
            i += 1;
            black_box(store.get("m", &key))
        });
    });
    g.bench_function(BenchmarkId::new("get_miss", "128B"), |b| {
        b.iter(|| black_box(store.get("m", &[0xDE, 0xAD, 0xBE, 0xEF, 0, 0, 0, 0])));
    });
    g.finish();
}

criterion_group!(benches, put, get);
criterion_main!(benches);
