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
use std::time::{Duration, Instant};

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
        let mut c = Client {
            stream,
            buf: Vec::new(),
            corr: 0,
        };
        c.authenticate();
        c
    }

    fn next_corr(&mut self) -> i64 {
        self.corr += 1;
        self.corr
    }

    /// Send one request, read exactly one response message, return its frames.
    fn call_resp(&mut self, mut req: Vec<Frame>) -> Vec<Frame> {
        let corr = self.next_corr();
        set_correlation_id(&mut req, corr);
        self.stream.write_all(&write_message(&req)).expect("write");
        loop {
            if let Some((frames, used)) = read_message(&self.buf) {
                self.buf.drain(0..used);
                return frames;
            }
            let mut chunk = [0u8; 16384];
            let n = self.stream.read(&mut chunk).expect("read");
            assert!(n > 0, "server closed");
            self.buf.extend_from_slice(&chunk[..n]);
        }
    }

    /// Send one request message, read exactly one response message, discard it.
    fn call(&mut self, req: Vec<Frame>) {
        let _ = self.call_resp(req);
    }

    /// GET returning the stored value bytes (None on a miss). Response layout:
    /// frame[0] is the fixed response header, frame[1] is the nullable value.
    fn get_value(&mut self, name: &str, key: &[u8]) -> Option<Vec<u8>> {
        let mut initial = vec![0u8; 24];
        write_i32_le(&mut initial, 0, 66048);
        write_i32_le(&mut initial, 12, -1);
        write_i64_le(&mut initial, 16, 1);
        let frames = self.call_resp(vec![
            Frame {
                flags: UNFRAGMENTED,
                content: initial,
            },
            string_frame(name),
            data_frame(key),
        ]);
        frames
            .get(1)
            .and_then(|f| if f.is_null() { None } else { Some(f.content.clone()) })
    }

    fn authenticate(&mut self) {
        let mut initial = vec![0u8; 36];
        write_i32_le(&mut initial, 0, 256);
        write_i32_le(&mut initial, 12, -1); // partitionId
        let req = vec![
            Frame {
                flags: UNFRAGMENTED,
                content: initial,
            },
            string_frame("dev"),        // clusterName
            null_frame(),               // username
            null_frame(),               // password
            string_frame("rust-bench"), // clientType
        ];
        self.call(req);
    }

    fn put(&mut self, name: &str, key: &[u8], value: &[u8]) {
        self.put_ttl(name, key, value, 0);
    }

    fn put_ttl(&mut self, name: &str, key: &[u8], value: &[u8], ttl_ms: i64) {
        self.store(65792, name, key, value, ttl_ms); // MapPut
    }

    // MapSet (69376) — the op the Hazelcast client's IMap.Set / SetWithTTL sends.
    fn set(&mut self, name: &str, key: &[u8], value: &[u8], ttl_ms: i64) {
        self.store(69376, name, key, value, ttl_ms);
    }

    fn store(&mut self, msg_type: i32, name: &str, key: &[u8], value: &[u8], ttl_ms: i64) {
        let mut initial = vec![0u8; 32];
        write_i32_le(&mut initial, 0, msg_type);
        write_i32_le(&mut initial, 12, -1);
        write_i64_le(&mut initial, 16, 1); // threadId
        write_i64_le(&mut initial, 24, ttl_ms); // ttl (ms; 0 == infinite)
        self.call(vec![
            Frame {
                flags: UNFRAGMENTED,
                content: initial,
            },
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
            Frame {
                flags: UNFRAGMENTED,
                content: initial,
            },
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

/// One closed-loop stage: `level` worker threads, **each with its own TCP
/// connection**, loop put+get until the deadline, capturing per-op latencies
/// (microseconds). Mirrors the Go loadgen's closed-loop stage — but with a real
/// connection per worker (the official Hazelcast Go client multiplexes all
/// workers over a single connection, which caps throughput at one reactor core).
fn run_stage(addr: &str, level: usize, dur: Duration, valsz: usize) -> (Vec<u64>, Vec<u64>) {
    let deadline = Instant::now() + dur;
    let mut handles = Vec::new();
    for c in 0..level {
        let addr = addr.to_string();
        handles.push(std::thread::spawn(move || {
            let mut cli = Client::connect(&addr);
            let val = vec![0xABu8; valsz];
            let mut sets: Vec<u64> = Vec::new();
            let mut gets: Vec<u64> = Vec::new();
            let mut i: usize = 0;
            while Instant::now() < deadline {
                // Bounded per-thread keyspace so the working set stays resident
                // (throughput/latency measurement, not a slab-exhaustion test).
                let k = format!("t{}k{}", c, i % 4096);
                let t0 = Instant::now();
                cli.put("bench", k.as_bytes(), &val);
                sets.push(t0.elapsed().as_micros() as u64);
                let t1 = Instant::now();
                cli.get("bench", k.as_bytes());
                gets.push(t1.elapsed().as_micros() as u64);
                i += 1;
            }
            (sets, gets)
        }));
    }
    let mut all_sets = Vec::new();
    let mut all_gets = Vec::new();
    for h in handles {
        let (s, g) = h.join().unwrap();
        all_sets.extend_from_slice(&s);
        all_gets.extend_from_slice(&g);
    }
    (all_sets, all_gets)
}

/// Staged throughput+latency ladder, emitting the same JSON shape as the Go
/// loadgen so the results drop straight into the dashboard.
fn ladder_bench(addr: &str, levels: &[usize], stage: Duration, valsz: usize) {
    // Warmup (discarded).
    let _ = run_stage(addr, 16, Duration::from_secs(2), valsz);

    let mut stages_json: Vec<String> = Vec::new();
    for &level in levels {
        let (mut sets, mut gets) = run_stage(addr, level, stage, valsz);
        let secs = stage.as_secs_f64();
        sets.sort_unstable();
        gets.sort_unstable();
        // Both ops run once per loop iteration, so set/get counts are equal and
        // rps here is the achieved op-rate for that op (put OR get).
        let s_rps = sets.len() as f64 / secs;
        let g_rps = gets.len() as f64 / secs;
        eprintln!(
            "level={} set: {:.0} rps p99={}us | get: {:.0} rps p99={}us | n={}",
            level,
            s_rps,
            percentile(&sets, 0.99),
            g_rps,
            percentile(&gets, 0.99),
            sets.len()
        );
        let set_json = format!(
            "{{ \"op\": \"set\", \"count\": {}, \"rps\": {:.2}, \"p50_us\": {}, \"p90_us\": {}, \"p99_us\": {} }}",
            sets.len(), s_rps,
            percentile(&sets, 0.50), percentile(&sets, 0.90), percentile(&sets, 0.99),
        );
        let get_json = format!(
            "{{ \"op\": \"get\", \"count\": {}, \"rps\": {:.2}, \"p50_us\": {}, \"p90_us\": {}, \"p99_us\": {} }}",
            gets.len(), g_rps,
            percentile(&gets, 0.50), percentile(&gets, 0.90), percentile(&gets, 0.99),
        );
        stages_json.push(format!(
            "    {{ \"level\": {}, \"set\": {}, \"get\": {}, \"errors\": 0 }}",
            level, set_json, get_json,
        ));
    }
    println!(
        "{{\n  \"target\": \"bonsaigrid\",\n  \"stages\": [\n{}\n  ]\n}}",
        stages_json.join(",\n")
    );
}

/// Correctness check the benchmark loadgen does NOT do: write `count` unique keys
/// each with a distinct value, read them all back, and compare the bytes. Uses no
/// TTL so this also proves retention (catches silent drops/overwrites under
/// volume, which throughput+latency numbers can't see because a miss is cheap).
fn verify_bench(addr: &str, count: usize, valsz: usize, ttl_ms: i64, op: &str) {
    let mut cli = Client::connect(addr);
    let name = "verify";
    let use_set = op == "set";
    let expected = |i: usize| -> Vec<u8> {
        let mut v = vec![0u8; valsz];
        for (j, b) in v.iter_mut().enumerate() {
            *b = (i.wrapping_mul(131).wrapping_add(j) & 0xff) as u8;
        }
        // stamp the index in the clear so a wrong-entry match is still caught
        let idx = (i as u64).to_le_bytes();
        let n = valsz.min(idx.len());
        v[..n].copy_from_slice(&idx[..n]);
        v
    };
    for i in 0..count {
        let k = format!("vkey-{:09}", i);
        if use_set {
            cli.set(name, k.as_bytes(), &expected(i), ttl_ms);
        } else {
            cli.put_ttl(name, k.as_bytes(), &expected(i), ttl_ms);
        }
    }
    let (mut hit, mut miss, mut mismatch) = (0usize, 0usize, 0usize);
    for i in 0..count {
        let k = format!("vkey-{:09}", i);
        match cli.get_value(name, k.as_bytes()) {
            None => miss += 1,
            Some(v) if v == expected(i) => hit += 1,
            Some(_) => mismatch += 1,
        }
    }
    println!("verify: wrote+read {} unique keys via {}, {}-byte values, ttl_ms={}",
             count, if use_set { "MapSet(69376)" } else { "MapPut(65792)" }, valsz, ttl_ms);
    println!("  hit       {}", hit);
    println!("  miss      {}", miss);
    println!("  mismatch  {}", mismatch);
    println!(
        "  result    {}",
        if hit == count {
            "PASS — every value returned correctly"
        } else {
            "FAIL — server did not faithfully return stored data"
        }
    );
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
        Some("verify") => {
            let count = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(100_000);
            let valsz = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(64);
            let ttl_ms = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(0i64);
            let op = args.get(5).map(String::as_str).unwrap_or("put");
            verify_bench(&addr, count, valsz, ttl_ms, op);
        }
        Some("ladder") => {
            // bench ladder [stage_secs] [valsz]   (LEVELS env overrides the ramp)
            let stage_secs = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(4u64);
            let valsz = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(128usize);
            let levels: Vec<usize> = std::env::var("LEVELS")
                .unwrap_or_else(|_| "1,2,4,8,16,32,64,128".to_string())
                .split(',')
                .filter_map(|s| s.trim().parse().ok())
                .collect();
            ladder_bench(&addr, &levels, Duration::from_secs(stage_secs), valsz);
        }
        _ => {
            eprintln!("usage: bench latency [n] | bench load <count> <valsz> | bench concurrent <conns> <ops> | bench ladder [stage_secs] [valsz]");
            std::process::exit(2);
        }
    }
}
