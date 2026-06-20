//! Bounded, lock-free single-producer / single-consumer ring buffer.
//!
//! This is the cross-core coordination primitive from the BonsaiGrid routing
//! spec (§4): one ring per ordered core pair, no locks, no allocation after
//! construction. `head` is written only by the consumer, `tail` only by the
//! producer; acquire/release ordering publishes each slot's data before the
//! index that exposes it. Capacity is rounded up to a power of two.

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

struct Ring<T> {
    buf: Box<[UnsafeCell<MaybeUninit<T>>]>,
    mask: usize,
    head: AtomicUsize, // consumer index
    tail: AtomicUsize, // producer index
}

// Safe: Producer/Consumer split enforces single-producer/single-consumer; T must
// be Send to cross the thread boundary.
unsafe impl<T: Send> Sync for Ring<T> {}
unsafe impl<T: Send> Send for Ring<T> {}

/// Create a ring with at least `capacity` slots; returns the producer/consumer pair.
pub fn channel<T>(capacity: usize) -> (Producer<T>, Consumer<T>) {
    let cap = capacity.next_power_of_two().max(2);
    let mut v = Vec::with_capacity(cap);
    for _ in 0..cap {
        v.push(UnsafeCell::new(MaybeUninit::uninit()));
    }
    let ring = Arc::new(Ring {
        buf: v.into_boxed_slice(),
        mask: cap - 1,
        head: AtomicUsize::new(0),
        tail: AtomicUsize::new(0),
    });
    (Producer { ring: ring.clone() }, Consumer { ring })
}

pub struct Producer<T> {
    ring: Arc<Ring<T>>,
}
pub struct Consumer<T> {
    ring: Arc<Ring<T>>,
}

impl<T> Producer<T> {
    /// Push an item; returns `Err(item)` if the ring is full.
    pub fn push(&self, item: T) -> Result<(), T> {
        let r = &*self.ring;
        let tail = r.tail.load(Ordering::Relaxed);
        let head = r.head.load(Ordering::Acquire);
        if tail.wrapping_sub(head) == r.buf.len() {
            return Err(item); // full
        }
        // SAFETY: single producer owns this slot until tail is published.
        unsafe {
            (*r.buf[tail & r.mask].get()).write(item);
        }
        r.tail.store(tail.wrapping_add(1), Ordering::Release);
        Ok(())
    }
}

impl<T> Consumer<T> {
    /// Pop an item, or `None` if the ring is empty.
    pub fn pop(&self) -> Option<T> {
        let r = &*self.ring;
        let head = r.head.load(Ordering::Relaxed);
        let tail = r.tail.load(Ordering::Acquire);
        if head == tail {
            return None; // empty
        }
        // SAFETY: producer published this slot via the Release on tail.
        let item = unsafe { (*r.buf[head & r.mask].get()).assume_init_read() };
        r.head.store(head.wrapping_add(1), Ordering::Release);
        Some(item)
    }
}

impl<T> Drop for Ring<T> {
    fn drop(&mut self) {
        // Drop any items still in the ring.
        let mut head = self.head.load(Ordering::Relaxed);
        let tail = self.tail.load(Ordering::Relaxed);
        while head != tail {
            unsafe {
                (*self.buf[head & self.mask].get()).assume_init_drop();
            }
            head = head.wrapping_add(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_threaded_fifo_and_full_empty() {
        let (p, c) = channel::<u32>(4); // cap rounds to 4
        assert!(c.pop().is_none());
        for i in 0..4 {
            assert!(p.push(i).is_ok());
        }
        assert!(p.push(99).is_err(), "ring is full");
        for i in 0..4 {
            assert_eq!(c.pop(), Some(i));
        }
        assert!(c.pop().is_none());
    }

    #[test]
    fn concurrent_producer_consumer_transfers_in_order() {
        let (p, c) = channel::<u64>(1024);
        const N: u64 = 2_000_000;
        let prod = std::thread::spawn(move || {
            let mut i = 0u64;
            while i < N {
                if p.push(i).is_ok() {
                    i += 1;
                } else {
                    std::hint::spin_loop();
                }
            }
        });
        let mut next = 0u64;
        while next < N {
            match c.pop() {
                Some(v) => {
                    assert_eq!(v, next, "items arrive in FIFO order with no loss/dup");
                    next += 1;
                }
                None => std::hint::spin_loop(),
            }
        }
        prod.join().unwrap();
        assert_eq!(next, N);
    }
}
