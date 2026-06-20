//! BonsaiGrid increment-0 server binary: single-node, thread-per-connection.

use std::sync::Arc;

fn main() -> std::io::Result<()> {
    let store = Arc::new(store::Store::new());
    let addr = "127.0.0.1:5701";
    let listener = std::net::TcpListener::bind(addr)?;
    eprintln!("BonsaiGrid listening on {addr}");
    for stream in listener.incoming() {
        let stream = stream?;
        let store = store.clone();
        std::thread::spawn(move || {
            let _ = server::connection::handle(stream, |req| server::handlers::dispatch(req, &store));
        });
    }
    Ok(())
}
