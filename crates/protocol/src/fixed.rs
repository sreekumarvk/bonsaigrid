//! Little-endian fixed-size encoders, mirroring Hazelcast `FixedSizeTypesCodec`.

pub const UUID_SIZE: usize = 17; // 1 null-flag byte + 2 * i64

pub fn write_i32_le(buf: &mut [u8], pos: usize, v: i32) {
    buf[pos..pos + 4].copy_from_slice(&v.to_le_bytes());
}
pub fn read_i32_le(buf: &[u8], pos: usize) -> i32 {
    i32::from_le_bytes(buf[pos..pos + 4].try_into().unwrap())
}
pub fn write_i64_le(buf: &mut [u8], pos: usize, v: i64) {
    buf[pos..pos + 8].copy_from_slice(&v.to_le_bytes());
}
pub fn read_i64_le(buf: &[u8], pos: usize) -> i64 {
    i64::from_le_bytes(buf[pos..pos + 8].try_into().unwrap())
}
pub fn write_u16_le(buf: &mut [u8], pos: usize, v: u16) {
    buf[pos..pos + 2].copy_from_slice(&v.to_le_bytes());
}
pub fn read_u16_le(buf: &[u8], pos: usize) -> u16 {
    u16::from_le_bytes(buf[pos..pos + 2].try_into().unwrap())
}

/// UUID layout: 1 null-flag byte, then most-significant i64, then least-significant i64.
pub fn write_uuid(buf: &mut [u8], pos: usize, uuid: Option<(i64, i64)>) {
    match uuid {
        None => buf[pos] = 1,
        Some((msb, lsb)) => {
            buf[pos] = 0;
            write_i64_le(buf, pos + 1, msb);
            write_i64_le(buf, pos + 9, lsb);
        }
    }
}
pub fn read_uuid(buf: &[u8], pos: usize) -> Option<(i64, i64)> {
    if buf[pos] == 1 {
        None
    } else {
        Some((read_i64_le(buf, pos + 1), read_i64_le(buf, pos + 9)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn i32_roundtrip_is_little_endian() {
        let mut b = [0u8; 4];
        write_i32_le(&mut b, 0, 66048); // MapGet request type 0x010200
        assert_eq!(b, [0x00, 0x02, 0x01, 0x00]);
        assert_eq!(read_i32_le(&b, 0), 66048);
    }

    #[test]
    fn uuid_null_is_single_flag_byte() {
        let mut b = [0xFFu8; UUID_SIZE];
        write_uuid(&mut b, 0, None);
        assert_eq!(b[0], 1); // isNull = true
    }

    #[test]
    fn uuid_present_roundtrips() {
        let mut b = [0u8; UUID_SIZE];
        write_uuid(
            &mut b,
            0,
            Some((0x1122334455667788, 0x99AABBCCDDEEFF00u64 as i64)),
        );
        assert_eq!(b[0], 0); // isNull = false
        assert_eq!(
            read_uuid(&b, 0),
            Some((0x1122334455667788, 0x99AABBCCDDEEFF00u64 as i64))
        );
    }
}
