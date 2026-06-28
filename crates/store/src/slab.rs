//! Size-classed slab allocator. Bytes live in contiguous per-class arenas with
//! O(1) free-list reclamation — no per-entry heap allocation, no `malloc`
//! metadata or capacity slack per object. Objects larger than the biggest class
//! fall back to an overflow list (rare).
//!
//! Increment-1 simplification: a class arena doubles when exhausted (amortized).
//! Increment 2/3 will pre-size and hard-cap per the zero-allocation guardrail.

const CLASSES: &[usize] = &[
    8, 16, 24, 32, 48, 64, 96, 128, 192, 256, 384, 512, 768, 1024, 1536, 2048, 3072, 4096, 6144,
    8192,
];
const OVERFLOW: u16 = u16::MAX;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Handle {
    pub class: u16,
    pub slot: u32,
}

struct SizeClass {
    obj: usize,
    bytes: Vec<u8>,
    free: Vec<u32>,
    used: u32,
    cap: u32,
}

impl SizeClass {
    fn new(obj: usize, cap: u32) -> Self {
        SizeClass {
            obj,
            bytes: vec![0u8; obj * cap as usize],
            free: Vec::new(),
            used: 0,
            cap,
        }
    }
    fn alloc(&mut self) -> u32 {
        if let Some(s) = self.free.pop() {
            return s;
        }
        if self.used == self.cap {
            // Grow 1.5x to bound slack (vs doubling).
            self.cap = (self.cap + self.cap / 2).max(self.cap + 1);
            self.bytes.resize(self.obj * self.cap as usize, 0);
        }
        let s = self.used;
        self.used += 1;
        s
    }
    fn off(&self, slot: u32) -> usize {
        slot as usize * self.obj
    }
}

pub struct Slab {
    classes: Vec<SizeClass>,
    overflow: Vec<Vec<u8>>,
    overflow_free: Vec<u32>,
}

impl Default for Slab {
    fn default() -> Self {
        Self::new()
    }
}

impl Slab {
    pub fn new() -> Self {
        Slab {
            classes: CLASSES.iter().map(|&o| SizeClass::new(o, 64)).collect(),
            overflow: Vec::new(),
            overflow_free: Vec::new(),
        }
    }

    fn class_for(len: usize) -> Option<usize> {
        CLASSES.iter().position(|&o| o >= len)
    }

    /// Store the concatenation `a ++ b` and return a handle to it.
    pub fn put_two(&mut self, a: &[u8], b: &[u8]) -> Handle {
        let len = a.len() + b.len();
        match Self::class_for(len) {
            Some(ci) => {
                let slot = self.classes[ci].alloc();
                let off = self.classes[ci].off(slot);
                let buf = &mut self.classes[ci].bytes[off..off + len];
                buf[..a.len()].copy_from_slice(a);
                buf[a.len()..].copy_from_slice(b);
                Handle { class: ci as u16, slot }
            }
            None => {
                let mut v = Vec::with_capacity(len);
                v.extend_from_slice(a);
                v.extend_from_slice(b);
                let slot = if let Some(f) = self.overflow_free.pop() {
                    self.overflow[f as usize] = v;
                    f
                } else {
                    self.overflow.push(v);
                    (self.overflow.len() - 1) as u32
                };
                Handle { class: OVERFLOW, slot }
            }
        }
    }

    /// Borrow the first `len` bytes stored at `h`.
    pub fn get(&self, h: Handle, len: usize) -> &[u8] {
        if h.class == OVERFLOW {
            &self.overflow[h.slot as usize][..len]
        } else {
            let c = &self.classes[h.class as usize];
            let off = c.off(h.slot);
            &c.bytes[off..off + len]
        }
    }

    /// Reclaim the slot in O(1).
    pub fn free(&mut self, h: Handle) {
        if h.class == OVERFLOW {
            self.overflow[h.slot as usize] = Vec::new();
            self.overflow_free.push(h.slot);
        } else {
            self.classes[h.class as usize].free.push(h.slot);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stores_and_reads_back_concatenation() {
        let mut s = Slab::new();
        let h = s.put_two(b"key", b"value");
        assert_eq!(s.get(h, 8), b"keyvalue");
    }

    #[test]
    fn free_reuses_the_slot() {
        let mut s = Slab::new();
        let h1 = s.put_two(b"ab", b"cd"); // len 4 -> class 8
        s.free(h1);
        let h2 = s.put_two(b"ef", b"gh");
        assert_eq!(h1, h2, "freed slot is reused O(1)");
        assert_eq!(s.get(h2, 4), b"efgh");
    }

    #[test]
    fn overflow_for_large_objects() {
        let mut s = Slab::new();
        let big = vec![7u8; 20_000];
        let h = s.put_two(&big, &[]);
        assert_eq!(h.class, OVERFLOW);
        assert_eq!(s.get(h, 20_000), &big[..]);
    }
}
