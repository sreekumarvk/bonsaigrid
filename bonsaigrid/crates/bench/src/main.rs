//! Raw-protocol load driver for BonsaiGrid. Speaks the Hazelcast client wire
//! format directly (no client library), so measurements isolate server cost.
//!
//! Modes:
//!   bench latency [n]            sequential put+get round-trips; reports ops/sec + p50/p99
//!   bench load <count> <valsz>   insert <count> entries of <valsz> bytes, then idle-exit
//!                                (the caller samples server RSS while entries are resident)
//!
//! Address defaults to 127.0.0.1:5701; override with BENCH_ADDR.

use protocol::fixed::{write_i32_le, write_i64_le};
use protocol::frame::{read_message, write_message, Frame, UNFRAGMENTED};
use protocol::message::set_correlation_id;
use protocol::primitives::{data_frame, null_frame, string_frame};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Instant;

struct Client {
    stream: TcpStream,
    buf: Vec<u8>,
    corr: i64,
}

impl Client {
    fn connect(addr: &str) -> Client {
        let mut stream = TcpStream::connect(addr).expect("connect");
        stream.set_nodelay(true).ok();
        stream.write_all(b"CP2").expect("preamble");
        let mut c = Client { stream, buf: Vec::new(), corr: 0 };
        c.authenticate();
        c
    }

    fn next_corr(&mut self) -> i64 {
        self.corr += 1;
        self.corr
    }

    /// Send one request message, read exactly one response message, discard it.
    fn call(&mut self, mut req: Vec<Frame>) {
        let corr = self.next_corr();
        set_correlation_id(&mut req, corr);
        self.stream.write_all(&write_message(&req)).expect("write");
        loop {
            if let Some((_frames, used)) = read_message(&self.buf) {
                self.buf.drain(0..used);
                return;
            }
            let mut chunk = [0u8; 16384];
            let n = self.stream.read(&mut chunk).expect("read");
            assert!(n > 0, "server closed");
            self.buf.extend_from_slice(&chunk[..n]);
        }
    }

    fn authenticate(&mut self) {
        let mut initial = vec![0u8; 36];
        write_i32_le(&mut initial, 0, 256);
        write_i32_le(&mut initial, 12, -1); // partitionId
        let req = vec![
            Frame { flags: UNFRAGMENTED, content: initial },
            string_frame("dev"),       // clusterName
            null_frame(),              // username
            null_frame(),              // password
            string_frame("rust-bench"), // clientType
        ];
        self.call(req);
    }

    fn put(&mut self, name: &str, key: &[u8], value: &[u8]) {
        let mut initial = vec![0u8; 32];
        write_i32_le(&mut initial, 0, 65792);
        write_i32_le(&mut initial, 12, -1);
        write_i64_le(&mut initial, 16, 1); // threadId
        write_i64_le(&mut initial, 24, 0); // ttl
        self.call(vec![
            Frame { flags: UNFRAGMENTED, content: initial },
            string_frame(name),
            data_frame(key),
            data_frame(value),
        ]);
    }

    fn get(&mut self, name: &str, key: &[u8]) {
        let mut initial = vec![0u8; 24];
        write_i32_le(&mut initial, 0, 66048);
        write_i32_le(&mut initial, 12, -1);
        write_i64_le(&mut initial, 16, 1);
        self.call(vec![
            Frame { flags: UNFRAGMENTED, content: initial },
            string_frame(name),
            data_frame(key),
        ]);
    }
}

fn percentile(sorted_us: &[u64], p: f64) -> u64 {
    if sorted_us.is_empty() {
        return 0;
    }
    let idx = ((sorted_us.len() as f64 - 1.0) * p).round() as usize;
    sorted_us[idx]
}

fn latency_bench(addr: &str, n: usize) {
    let mut cli = Client::connect(addr);
    let key = b"benchkey";
    let val = vec![0xABu8; 64];

    // Warmup
    for _ in 0..1000 {
        cli.put("m", key, &val);
        cli.get("m", key);
    }

    let mut lat = Vec::with_capacity(2 * n);
    let start = Instant::now();
    for i in 0..n {
        let k = format!("k{}", i % 4096);
        let t0 = Instant::now();
        cli.put("m", k.as_bytes(), &val);
        lat.push(t0.elapsed().as_micros() as u64);
        let t1 = Instant::now();
        cli.get("m", k.as_bytes());
        lat.push(t1.elapsed().as_micros() as u64);
    }
    let elapsed = start.elapsed();
    let ops = (2 * n) as f64;
    lat.sort_unstable();
    println!("ops           {}", 2 * n);
    println!("wall_sec      {:.3}", elapsed.as_secs_f64());
    println!("throughput    {:.0} ops/sec", ops / elapsed.as_secs_f64());
    println!("p50_us        {}", percentile(&lat, 0.50));
    println!("p99_us        {}", percentile(&lat, 0.99));
    println!("p999_us       {}", percentile(&lat, 0.999));
}

fn concurrent_bench(addr: &str, conns: usize, ops_per_conn: usize) {
    let start = Instant::now();
    let mut handles = Vec::new();
    for c in 0..conns {
        let addr = addr.to_string();
        handles.push(std::thread::spawn(move || {
            let mut cli = Client::connect(&addr);
            let val = vec![0xABu8; 64];
            for i in 0..ops_per_conn {
                let k = format!("c{}k{}", c, i % 1024);
                cli.put("m", k.as_bytes(), &val);
                cli.get("m", k.as_bytes());
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    let elapsed = start.elapsed();
    let ops = (conns * ops_per_conn * 2) as f64;
    println!("conns         {}", conns);
    println!("total_ops     {}", conns * ops_per_conn * 2);
    println!("wall_sec      {:.3}", elapsed.as_secs_f64());
    println!("throughput    {:.0} ops/sec", ops / elapsed.as_secs_f64());
}

fn load_bench(addr: &str, count: usize, valsz: usize) {
    let mut cli = Client::connect(addr);
    let val = vec![0xCDu8; valsz];
    for i in 0..count {
        let key = format!("entry-{:09}", i);
        cli.put("mem", key.as_bytes(), &val);
    }
    println!("inserted {} entries of {} bytes", count, valsz);
}

fn main() {
    let addr = std::env::var("BENCH_ADDR").unwrap_or_else(|_| "127.0.0.1:5701".to_string());
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("latency") => {
            let n = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(50_000);
            latency_bench(&addr, n);
        }
        Some("load") => {
            let count = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(100_000);
            let valsz = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(100);
            load_bench(&addr, count, valsz);
        }
        Some("concurrent") => {
            let conns = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(64);
            let ops = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(20_000);
            concurrent_bench(&addr, conns, ops);
        }
        _ => {
            eprintln!("usage: bench latency [n] | bench load <count> <valsz>");
            std::process::exit(2);
        }
    }
}
