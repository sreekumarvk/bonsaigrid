//! BonsaiGrid increment-2 server binary: single-core io_uring reactor.

fn main() -> std::io::Result<()> {
    let store = store::Store::new();
    let addr = "127.0.0.1:5701";
    let listener = std::net::TcpListener::bind(addr)?;
    eprintln!("BonsaiGrid listening on {addr} (io_uring reactor)");
    server::reactor::run(listener, |req| server::handlers::dispatch(req, &store))
}
