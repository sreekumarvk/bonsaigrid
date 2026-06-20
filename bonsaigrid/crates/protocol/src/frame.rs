//! Hazelcast client-protocol frame envelope.
//! Wire frame = [u32 LE length = 6 + content.len][u16 LE flags][content].
//! A message is a sequence of frames; the last frame has IS_FINAL set.

use crate::fixed::{read_i32_le, read_u16_le, write_i32_le, write_u16_le};

pub const PREFIX_LEN: usize = 6;
pub const UNFRAGMENTED: u16 = 0xC000;
pub const IS_FINAL: u16 = 0x2000;
pub const BEGIN_DS: u16 = 0x1000;
pub const END_DS: u16 = 0x0800;
pub const IS_NULL: u16 = 0x0400;
pub const IS_EVENT: u16 = 0x0200;

#[derive(Clone, Debug)]
pub struct Frame {
    pub flags: u16,
    pub content: Vec<u8>,
}

impl Frame {
    pub fn is_null(&self) -> bool {
        self.flags & IS_NULL != 0
    }
    pub fn is_begin(&self) -> bool {
        self.flags & BEGIN_DS != 0
    }
    pub fn is_end(&self) -> bool {
        self.flags & END_DS != 0
    }
}

/// Serialize a full message; sets IS_FINAL on the last frame.
pub fn write_message(frames: &[Frame]) -> Vec<u8> {
    let mut out = Vec::new();
    for (i, f) in frames.iter().enumerate() {
        let len = (PREFIX_LEN + f.content.len()) as i32;
        let mut prefix = [0u8; PREFIX_LEN];
        write_i32_le(&mut prefix, 0, len);
        let flags = if i + 1 == frames.len() {
            f.flags | IS_FINAL
        } else {
            f.flags
        };
        write_u16_le(&mut prefix, 4, flags);
        out.extend_from_slice(&prefix);
        out.extend_from_slice(&f.content);
    }
    out
}

/// Length in bytes of the first complete message in `bytes`, without allocating
/// (walks frame prefixes to the IS_FINAL frame). None if more bytes are needed.
pub fn message_len(bytes: &[u8]) -> Option<usize> {
    let mut off = 0;
    loop {
        if bytes.len() < off + PREFIX_LEN {
            return None;
        }
        let len = read_i32_le(bytes, off) as usize;
        let flags = read_u16_le(bytes, off + 4);
        if len < PREFIX_LEN || bytes.len() < off + len {
            return None;
        }
        off += len;
        if flags & IS_FINAL != 0 {
            return Some(off);
        }
    }
}

/// Parse one complete message; returns frames + bytes consumed, or None if more bytes are needed.
pub fn read_message(bytes: &[u8]) -> Option<(Vec<Frame>, usize)> {
    let mut frames = Vec::new();
    let mut off = 0;
    loop {
        if bytes.len() < off + PREFIX_LEN {
            return None;
        }
        let len = read_i32_le(bytes, off) as usize;
        let flags = read_u16_le(bytes, off + 4);
        if len < PREFIX_LEN || bytes.len() < off + len {
            return None;
        }
        let content = bytes[off + PREFIX_LEN..off + len].to_vec();
        frames.push(Frame { flags, content });
        off += len;
        if flags & IS_FINAL != 0 {
            return Some((frames, off));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_frame_message_roundtrips_with_final_flag() {
        let f = Frame {
            flags: UNFRAGMENTED,
            content: vec![1, 2, 3],
        };
        let wire = write_message(&[f]);
        // length = 6 + 3 = 9 (LE), flags = UNFRAGMENTED | IS_FINAL (LE)
        assert_eq!(&wire[0..4], &[9, 0, 0, 0]);
        assert_eq!(&wire[4..6], &(UNFRAGMENTED | IS_FINAL).to_le_bytes());
        assert_eq!(&wire[6..9], &[1, 2, 3]);

        let (frames, used) = read_message(&wire).unwrap();
        assert_eq!(used, 9);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].content, vec![1, 2, 3]);
    }

    #[test]
    fn read_message_returns_none_when_incomplete() {
        let f = Frame {
            flags: UNFRAGMENTED,
            content: vec![1, 2, 3],
        };
        let wire = write_message(&[f]);
        assert!(read_message(&wire[..5]).is_none());
    }

    #[test]
    fn multi_frame_message_parses_until_final() {
        let frames = vec![
            Frame { flags: UNFRAGMENTED, content: vec![0xAA] },
            Frame { flags: 0, content: vec![0xBB, 0xCC] },
        ];
        let wire = write_message(&frames);
        let (got, used) = read_message(&wire).unwrap();
        assert_eq!(used, wire.len());
        assert_eq!(got.len(), 2);
        assert_eq!(got[1].content, vec![0xBB, 0xCC]);
    }
}
