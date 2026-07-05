use store::WalSink;
use wan::{WanOp, WanPublisher};

#[test]
fn publisher_captures_puts_and_removes() {
    let (tx, rx) = spsc::channel::<wan::WanRecord>(16);
    let p = WanPublisher::new(tx);
    p.map_put(5, 0, "m", b"k", b"v");
    p.map_remove(6, "m", b"k");
    let a = rx.pop().unwrap();
    assert_eq!(a.op, WanOp::Put);
    assert_eq!(a.key, b"k");
    assert_eq!(a.stamp, 5);
    let b = rx.pop().unwrap();
    assert_eq!(b.op, WanOp::Remove);
    assert_eq!(b.stamp, 6);
}
