//! WAN wire messages between clusters: a sequence-numbered batch of records, and
//! an ack of the highest applied sequence. Records reuse the `record` framing.

use crate::record::{decode, encode, Decoded, WanRecord};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WanMsg {
    Batch {
        up_to_seq: u64,
        records: Vec<WanRecord>,
    },
    Ack {
        up_to_seq: u64,
    },
}

pub fn encode_msg(m: &WanMsg) -> Vec<u8> {
    let mut b = Vec::new();
    match m {
        WanMsg::Batch { up_to_seq, records } => {
            b.push(0);
            b.extend_from_slice(&up_to_seq.to_le_bytes());
            b.extend_from_slice(&(records.len() as u32).to_le_bytes());
            for r in records {
                b.extend_from_slice(&encode(r)); // self-delimiting (len-prefixed)
            }
        }
        WanMsg::Ack { up_to_seq } => {
            b.push(1);
            b.extend_from_slice(&up_to_seq.to_le_bytes());
        }
    }
    b
}

pub fn decode_msg(b: &[u8]) -> Option<WanMsg> {
    match *b.first()? {
        0 => {
            let up_to_seq = u64::from_le_bytes(b.get(1..9)?.try_into().ok()?);
            let n = u32::from_le_bytes(b.get(9..13)?.try_into().ok()?) as usize;
            let mut off = 13;
            let mut records = Vec::with_capacity(n);
            for _ in 0..n {
                match decode(b.get(off..)?) {
                    Decoded::Record { rec, consumed } => {
                        records.push(rec);
                        off += consumed;
                    }
                    _ => return None,
                }
            }
            Some(WanMsg::Batch { up_to_seq, records })
        }
        1 => Some(WanMsg::Ack {
            up_to_seq: u64::from_le_bytes(b.get(1..9)?.try_into().ok()?),
        }),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{WanOp, WanRecord};

    #[test]
    fn msg_roundtrip() {
        let batch = WanMsg::Batch {
            up_to_seq: 9,
            records: vec![
                WanRecord {
                    op: WanOp::Put,
                    stamp: 1,
                    ttl_ms: 0,
                    map: "m".into(),
                    key: b"a".to_vec(),
                    value: b"1".to_vec(),
                },
                WanRecord {
                    op: WanOp::Remove,
                    stamp: 2,
                    ttl_ms: 0,
                    map: "m".into(),
                    key: b"b".to_vec(),
                    value: vec![],
                },
            ],
        };
        assert_eq!(decode_msg(&encode_msg(&batch)).unwrap(), batch);
        let ack = WanMsg::Ack { up_to_seq: 9 };
        assert_eq!(decode_msg(&encode_msg(&ack)).unwrap(), ack);
    }
}
