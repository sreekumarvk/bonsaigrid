//! Member wire protocol: length-prefixed frames `[u32 len][u8 kind][body]`, all
//! big-endian. `len` covers `kind + body`. Strings and blobs are `[u32 len][bytes]`.
//! Custom/BonsaiGrid-only — not the Hazelcast client format.

/// One member's identity in a published view.
#[derive(Clone, Debug, PartialEq)]
pub struct MemberRec {
    pub uuid: (i64, i64),
    pub host: String,
    pub client_port: i32,
    pub member_port: i32,
    pub join_id: u64,
}

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
    /// Periodic liveness from `from_join_id`, carrying the sender's generation.
    Heartbeat { from_join_id: u64, generation: u64 },
    /// A new member asks the master to admit it.
    JoinRequest { uuid: (i64, i64), host: String, client_port: i32, member_port: i32 },
    /// The master's authoritative member list (only alive members) at `generation`.
    MemberView { generation: u64, members: Vec<MemberRec> },
}

const KIND_HELLO: u8 = 0;
const KIND_PUT: u8 = 1;
const KIND_REMOVE: u8 = 2;
const KIND_ACK: u8 = 3;
const KIND_HEARTBEAT: u8 = 4;
const KIND_JOIN: u8 = 5;
const KIND_VIEW: u8 = 6;

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
fn put_i64(out: &mut Vec<u8>, v: i64) {
    out.extend_from_slice(&v.to_be_bytes());
}
fn put_uuid(out: &mut Vec<u8>, u: (i64, i64)) {
    put_i64(out, u.0);
    put_i64(out, u.1);
}
fn put_member(out: &mut Vec<u8>, m: &MemberRec) {
    put_uuid(out, m.uuid);
    put_blob(out, m.host.as_bytes());
    put_u32(out, m.client_port as u32);
    put_u32(out, m.member_port as u32);
    put_u64(out, m.join_id);
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
        Msg::Heartbeat { from_join_id, generation } => {
            body.push(KIND_HEARTBEAT);
            put_u64(&mut body, *from_join_id);
            put_u64(&mut body, *generation);
        }
        Msg::JoinRequest { uuid, host, client_port, member_port } => {
            body.push(KIND_JOIN);
            put_uuid(&mut body, *uuid);
            put_blob(&mut body, host.as_bytes());
            put_u32(&mut body, *client_port as u32);
            put_u32(&mut body, *member_port as u32);
        }
        Msg::MemberView { generation, members } => {
            body.push(KIND_VIEW);
            put_u64(&mut body, *generation);
            put_u32(&mut body, members.len() as u32);
            for m in members {
                put_member(&mut body, m);
            }
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
    fn i64(&mut self) -> Option<i64> {
        let s = self.b.get(self.pos..self.pos + 8)?;
        self.pos += 8;
        Some(i64::from_be_bytes(s.try_into().unwrap()))
    }
    fn uuid(&mut self) -> Option<(i64, i64)> {
        Some((self.i64()?, self.i64()?))
    }
    fn member(&mut self) -> Option<MemberRec> {
        Some(MemberRec {
            uuid: self.uuid()?,
            host: self.string()?,
            client_port: self.u32()? as i32,
            member_port: self.u32()? as i32,
            join_id: self.u64()?,
        })
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
        KIND_HEARTBEAT => Msg::Heartbeat { from_join_id: r.u64()?, generation: r.u64()? },
        KIND_JOIN => Msg::JoinRequest {
            uuid: r.uuid()?,
            host: r.string()?,
            client_port: r.u32()? as i32,
            member_port: r.u32()? as i32,
        },
        KIND_VIEW => {
            let generation = r.u64()?;
            let count = r.u32()? as usize;
            let mut members = Vec::with_capacity(count);
            for _ in 0..count {
                members.push(r.member()?);
            }
            Msg::MemberView { generation, members }
        }
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
            Msg::Heartbeat { from_join_id: 2, generation: 7 },
            Msg::JoinRequest { uuid: (1, 4), host: "127.0.0.1".into(), client_port: 5704, member_port: 7704 },
            Msg::MemberView {
                generation: 9,
                members: vec![
                    MemberRec { uuid: (1, 1), host: "127.0.0.1".into(), client_port: 5701, member_port: 7701, join_id: 0 },
                    MemberRec { uuid: (1, 2), host: "10.0.0.2".into(), client_port: 5701, member_port: 7701, join_id: 1 },
                ],
            },
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
