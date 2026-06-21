//! Member wire protocol: length-prefixed frames `[u32 len][u8 kind][body]`, all
//! big-endian. `len` covers `kind + body`. Strings and blobs are `[u32 len][bytes]`.
//! Custom/BonsaiGrid-only — not the Hazelcast client format.

#[derive(Clone, Debug, PartialEq)]
pub enum Msg {
    /// First message on each new connection; identifies the sender's member index.
    Hello { index: u32 },
    /// Replicate a put to a backup.
    BackupPut { op_id: u64, name: String, key: Vec<u8>, value: Vec<u8>, ttl_ms: u64 },
    /// Replicate a remove to a backup.
    BackupRemove { op_id: u64, name: String, key: Vec<u8> },
    /// Backup → primary acknowledgement for `op_id`.
    Ack { op_id: u64 },
}

const KIND_HELLO: u8 = 0;
const KIND_PUT: u8 = 1;
const KIND_REMOVE: u8 = 2;
const KIND_ACK: u8 = 3;

fn put_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_be_bytes());
}
fn put_u64(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(&v.to_be_bytes());
}
fn put_blob(out: &mut Vec<u8>, b: &[u8]) {
    put_u32(out, b.len() as u32);
    out.extend_from_slice(b);
}

/// Encode a message into a complete `[u32 len][kind][body]` frame.
pub fn encode(msg: &Msg) -> Vec<u8> {
    let mut body = Vec::new();
    match msg {
        Msg::Hello { index } => {
            body.push(KIND_HELLO);
            put_u32(&mut body, *index);
        }
        Msg::BackupPut { op_id, name, key, value, ttl_ms } => {
            body.push(KIND_PUT);
            put_u64(&mut body, *op_id);
            put_blob(&mut body, name.as_bytes());
            put_blob(&mut body, key);
            put_blob(&mut body, value);
            put_u64(&mut body, *ttl_ms);
        }
        Msg::BackupRemove { op_id, name, key } => {
            body.push(KIND_REMOVE);
            put_u64(&mut body, *op_id);
            put_blob(&mut body, name.as_bytes());
            put_blob(&mut body, key);
        }
        Msg::Ack { op_id } => {
            body.push(KIND_ACK);
            put_u64(&mut body, *op_id);
        }
    }
    let mut frame = Vec::with_capacity(4 + body.len());
    put_u32(&mut frame, body.len() as u32);
    frame.extend_from_slice(&body);
    frame
}

struct Reader<'a> {
    b: &'a [u8],
    pos: usize,
}
impl Reader<'_> {
    fn u32(&mut self) -> Option<u32> {
        let s = self.b.get(self.pos..self.pos + 4)?;
        self.pos += 4;
        Some(u32::from_be_bytes(s.try_into().unwrap()))
    }
    fn u64(&mut self) -> Option<u64> {
        let s = self.b.get(self.pos..self.pos + 8)?;
        self.pos += 8;
        Some(u64::from_be_bytes(s.try_into().unwrap()))
    }
    fn blob(&mut self) -> Option<Vec<u8>> {
        let len = self.u32()? as usize;
        let s = self.b.get(self.pos..self.pos + len)?;
        self.pos += len;
        Some(s.to_vec())
    }
    fn string(&mut self) -> Option<String> {
        Some(String::from_utf8_lossy(&self.blob()?).into_owned())
    }
}

/// Decode one frame at the start of `buf`. Returns `(msg, bytes_consumed)`, or
/// `None` if a full frame isn't buffered yet (or the frame is malformed).
pub fn decode(buf: &[u8]) -> Option<(Msg, usize)> {
    if buf.len() < 4 {
        return None;
    }
    let len = u32::from_be_bytes(buf[0..4].try_into().unwrap()) as usize;
    let total = 4 + len;
    if buf.len() < total {
        return None;
    }
    let body = &buf[4..total];
    let mut r = Reader { b: body, pos: 0 };
    let kind = *body.first()?;
    r.pos = 1;
    let msg = match kind {
        KIND_HELLO => Msg::Hello { index: r.u32()? },
        KIND_PUT => Msg::BackupPut {
            op_id: r.u64()?,
            name: r.string()?,
            key: r.blob()?,
            value: r.blob()?,
            ttl_ms: r.u64()?,
        },
        KIND_REMOVE => Msg::BackupRemove { op_id: r.u64()?, name: r.string()?, key: r.blob()? },
        KIND_ACK => Msg::Ack { op_id: r.u64()? },
        _ => return None,
    };
    Some((msg, total))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let msgs = [
            Msg::Hello { index: 2 },
            Msg::Ack { op_id: 7 },
            Msg::BackupPut {
                op_id: 9,
                name: "m".into(),
                key: b"k".to_vec(),
                value: b"v".to_vec(),
                ttl_ms: 0,
            },
            Msg::BackupRemove { op_id: 3, name: "people".into(), key: b"alice".to_vec() },
        ];
        for m in msgs {
            let b = encode(&m);
            let (d, n) = decode(&b).unwrap();
            assert_eq!(d, m);
            assert_eq!(n, b.len());
        }
    }

    #[test]
    fn incomplete_frame_is_none() {
        assert!(decode(&[0, 0, 0, 9]).is_none()); // declares 9 body bytes, has 0
        assert!(decode(&[0, 0]).is_none());
    }

    #[test]
    fn two_frames_consume_independently() {
        let mut buf = encode(&Msg::Ack { op_id: 1 });
        buf.extend(encode(&Msg::Ack { op_id: 2 }));
        let (m1, n1) = decode(&buf).unwrap();
        assert_eq!(m1, Msg::Ack { op_id: 1 });
        let (m2, _) = decode(&buf[n1..]).unwrap();
        assert_eq!(m2, Msg::Ack { op_id: 2 });
    }
}
