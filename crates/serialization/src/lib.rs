//! Hazelcast `Data` envelope parsing and server-side partition computation.
//!
//! A serialized `Data` (HeapData) blob is laid out as:
//!   [partitionHash i32 big-endian @0][serializer type i32 big-endian @4][payload @8..]
//!
//! The partition a key belongs to is `hashToIndex(partitionHash, partitionCount)`,
//! where `partitionHash` is the stored value if non-zero, else MurmurHash3_x86_32
//! of the payload. This is identical to how a Hazelcast client computes it, so
//! the server can derive the same partition the client routed to — needed for
//! queries, backup placement, and TPC zero-contention alignment.

pub mod compact;
pub mod schema;

pub const PARTITION_HASH_OFFSET: usize = 0;
pub const TYPE_OFFSET: usize = 4;
pub const DATA_OFFSET: usize = 8;
const MURMUR_SEED: u32 = 0x01000193;

fn read_i32_be(b: &[u8], off: usize) -> i32 {
    i32::from_be_bytes([b[off], b[off + 1], b[off + 2], b[off + 3]])
}

fn fmix(mut h: u32) -> u32 {
    h ^= h >> 16;
    h = h.wrapping_mul(0x85ebca6b);
    h ^= h >> 13;
    h = h.wrapping_mul(0xc2b2ae35);
    h ^= h >> 16;
    h
}

/// MurmurHash3 x86_32, matching Hazelcast `HashUtil.MurmurHash3_x86_32`.
pub fn murmur3_x86_32(data: &[u8], seed: u32) -> i32 {
    const C1: u32 = 0xcc9e2d51;
    const C2: u32 = 0x1b873593;
    let len = data.len();
    let nblocks = len / 4;
    let mut h1 = seed;

    for i in 0..nblocks {
        let off = i * 4;
        let mut k1 = u32::from_le_bytes([data[off], data[off + 1], data[off + 2], data[off + 3]]);
        k1 = k1.wrapping_mul(C1);
        k1 = k1.rotate_left(15);
        k1 = k1.wrapping_mul(C2);
        h1 ^= k1;
        h1 = h1.rotate_left(13);
        h1 = h1.wrapping_mul(5).wrapping_add(0xe6546b64);
    }

    let tail = &data[nblocks * 4..];
    let mut k1 = 0u32;
    let rem = len & 3;
    if rem == 3 {
        k1 ^= (tail[2] as u32) << 16;
    }
    if rem >= 2 {
        k1 ^= (tail[1] as u32) << 8;
    }
    if rem >= 1 {
        k1 ^= tail[0] as u32;
        k1 = k1.wrapping_mul(C1);
        k1 = k1.rotate_left(15);
        k1 = k1.wrapping_mul(C2);
        h1 ^= k1;
    }

    h1 ^= len as u32;
    fmix(h1) as i32
}

/// The partition hash of a `Data` blob (stored value if non-zero, else murmur3
/// of the payload).
pub fn partition_hash(data: &[u8]) -> i32 {
    if data.len() < DATA_OFFSET {
        return 0;
    }
    let stored = read_i32_be(data, PARTITION_HASH_OFFSET);
    if stored != 0 {
        stored
    } else {
        murmur3_x86_32(&data[DATA_OFFSET..], MURMUR_SEED)
    }
}

/// The serializer type id of a `Data` blob.
pub fn type_id(data: &[u8]) -> i32 {
    if data.len() < DATA_OFFSET {
        0
    } else {
        read_i32_be(data, TYPE_OFFSET)
    }
}

/// `hashToIndex(partition_hash(data), partition_count)` — the partition id.
pub fn partition_id(data: &[u8], partition_count: i32) -> i32 {
    let h = partition_hash(data);
    if h == i32::MIN {
        0
    } else {
        h.wrapping_abs() % partition_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn data_envelope_fields() {
        // [partitionHash=0x01020304 BE][type=0xFFFFFFF5 BE][payload "ab"]
        let mut d = vec![0u8; DATA_OFFSET];
        d[0..4].copy_from_slice(&0x01020304i32.to_be_bytes());
        d[4..8].copy_from_slice(&(-11i32).to_be_bytes()); // String constant type
        d.extend_from_slice(b"ab");
        assert_eq!(partition_hash(&d), 0x01020304);
        assert_eq!(type_id(&d), -11);
    }

    #[test]
    fn stored_zero_hash_falls_back_to_murmur() {
        let mut d = vec![0u8; DATA_OFFSET]; // partitionHash = 0
        d.extend_from_slice(b"hello");
        // partition_hash uses murmur of the payload; deterministic and nonzero-ish
        assert_eq!(partition_hash(&d), murmur3_x86_32(b"hello", MURMUR_SEED));
    }

    #[test]
    fn partition_id_in_range() {
        for n in 0u32..1000 {
            let mut d = vec![0u8; DATA_OFFSET];
            d.extend_from_slice(&n.to_le_bytes());
            let p = partition_id(&d, 271);
            assert!((0..271).contains(&p), "p={p} out of range");
        }
    }

    #[test]
    fn murmur_is_deterministic() {
        assert_eq!(
            murmur3_x86_32(b"the quick brown fox", 0),
            murmur3_x86_32(b"the quick brown fox", 0)
        );
        assert_ne!(murmur3_x86_32(b"a", 0), murmur3_x86_32(b"b", 0));
    }
}
