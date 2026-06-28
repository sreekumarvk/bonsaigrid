//! String/Data/null primitive frame codecs.

use crate::frame::{Frame, IS_NULL, UNFRAGMENTED};

/// A string is a frame whose content is the raw UTF-8 bytes.
pub fn string_frame(s: &str) -> Frame {
    Frame {
        flags: 0,
        content: s.as_bytes().to_vec(),
    }
}
pub fn decode_string(f: &Frame) -> String {
    String::from_utf8(f.content.clone()).expect("utf8")
}

/// A Data field is a frame whose content is the serialized blob, verbatim and opaque.
pub fn data_frame(blob: &[u8]) -> Frame {
    Frame {
        flags: 0,
        content: blob.to_vec(),
    }
}

pub fn null_frame() -> Frame {
    Frame {
        flags: IS_NULL,
        content: Vec::new(),
    }
}

/// Build an initial (header-bearing) frame from raw content.
pub fn initial_frame(content: Vec<u8>) -> Frame {
    Frame {
        flags: UNFRAGMENTED,
        content,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn string_frame_is_utf8_content() {
        let f = string_frame("dev");
        assert_eq!(f.content, b"dev");
        assert_eq!(decode_string(&f), "dev");
    }

    #[test]
    fn data_frame_is_verbatim_blob() {
        let blob = [0x00u8, 0x00, 0x00, 0x01, 0xAB];
        assert_eq!(data_frame(&blob).content, blob);
    }

    #[test]
    fn null_frame_sets_null_flag() {
        assert!(null_frame().is_null());
    }
}
