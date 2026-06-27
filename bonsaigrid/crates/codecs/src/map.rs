//! MapPutCodec (65792/65793) and MapGetCodec (66048/66049).
//!
//! Put request initial-frame offsets: threadId@16, ttl@24. Var-frames: name,
//! key (Data), value (Data). Get request: threadId@16; var-frames: name, key.
//! Responses carry a single nullable Data (the previous/looked-up value).

use protocol::fixed::{read_i32_le, read_i64_le, write_i32_le, write_i64_le, write_uuid};
use protocol::frame::{write_message, Frame, IS_EVENT, UNFRAGMENTED};
use protocol::primitives::{data_frame, decode_string, initial_frame, null_frame};

// Entry event types (Hazelcast EntryEventType bit flags).
pub const ADDED: i32 = 1;
pub const REMOVED: i32 = 2;
pub const UPDATED: i32 = 4;

/// MapAddEntryListener request (71936): includeValue@16, listenerFlags@17,
/// localOnly@21; var-frame name. Returns (name, flags, include_value).
pub fn decode_add_entry_listener(frames: &[Frame]) -> (String, i32, bool) {
    let c = &frames[0].content;
    let include_value = c[16] == 1;
    let flags = read_i32_le(c, 17);
    (decode_string(&frames[1]), flags, include_value)
}

fn push_nullable(frames: &mut Vec<Frame>, v: Option<&[u8]>) {
    match v {
        Some(b) => frames.push(data_frame(b)),
        None => frames.push(null_frame()),
    }
}

/// Encode a MapAddEntryListener entry event (71938) message for one listener.
/// `corr` is the listener's registration correlation id (routes the event to
/// its client-side handler).
pub fn encode_entry_event(
    corr: i64,
    event_type: i32,
    uuid: (i64, i64),
    key: Option<&[u8]>,
    value: Option<&[u8]>,
    old: Option<&[u8]>,
) -> Vec<u8> {
    let mut c = vec![0u8; 41]; // type@0, corr@4, partitionId@12, eventType@16, uuid@20, numAffected@37
    write_i32_le(&mut c, 0, 71938);
    write_i64_le(&mut c, 4, corr);
    write_i32_le(&mut c, 12, -1);
    write_i32_le(&mut c, 16, event_type);
    write_uuid(&mut c, 20, Some(uuid));
    write_i32_le(&mut c, 37, 1); // numberOfAffectedEntries
    let mut frames = vec![Frame { flags: UNFRAGMENTED | IS_EVENT, content: c }];
    push_nullable(&mut frames, key);
    push_nullable(&mut frames, value);
    push_nullable(&mut frames, old);
    push_nullable(&mut frames, None); // mergingValue
    write_message(&frames)
}

/// name-only request (Size/IsEmpty/Clear/KeySet/Values/EntrySet): map name @[1].
pub fn decode_name(frames: &[Frame]) -> String {
    decode_string(&frames[1])
}

/// Decode a `List<Data>` starting at frame `start` (frames[start] is BEGIN).
pub fn decode_data_list(frames: &[Frame], start: usize) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    let mut i = start + 1; // skip BEGIN
    while i < frames.len() && !frames[i].is_end() {
        out.push(frames[i].content.clone());
        i += 1;
    }
    out
}

/// Decode an `EntryList<Data,Data>` starting at frame `start` (BEGIN, then
/// key,value pairs, END).
pub fn decode_entry_list(frames: &[Frame], start: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut out = Vec::new();
    let mut i = start + 1; // skip BEGIN
    while i + 1 < frames.len() && !frames[i].is_end() {
        out.push((frames[i].content.clone(), frames[i + 1].content.clone()));
        i += 2;
    }
    out
}

/// Response carrying a `List<Data>` (KeySet/Values).
pub fn encode_data_list_response(msg_type: i32, items: &[Vec<u8>]) -> Vec<Frame> {
    let mut c = vec![0u8; 13];
    write_i32_le(&mut c, 0, msg_type);
    let mut frames = vec![initial_frame(c), crate::begin_frame()];
    for it in items {
        frames.push(data_frame(it));
    }
    frames.push(crate::end_frame());
    frames
}

/// Response carrying an `EntryList<Data,Data>` (GetAll/EntrySet).
pub fn encode_entry_list_response(msg_type: i32, entries: &[(Vec<u8>, Vec<u8>)]) -> Vec<Frame> {
    let mut c = vec![0u8; 13];
    write_i32_le(&mut c, 0, msg_type);
    let mut frames = vec![initial_frame(c), crate::begin_frame()];
    for (k, v) in entries {
        frames.push(data_frame(k));
        frames.push(data_frame(v));
    }
    frames.push(crate::end_frame());
    frames
}

/// name + value request (ContainsValue / SetAdd / QueueOffer): no threadId.
pub fn decode_name_value(frames: &[Frame]) -> (String, Vec<u8>) {
    (decode_string(&frames[1]), frames[2].content.clone())
}

/// name + key request (MultiMapGet / Lock): key is the data frame @[2].
pub fn decode_name_key(frames: &[Frame]) -> (String, Vec<u8>) {
    (decode_string(&frames[1]), frames[2].content.clone())
}

/// Near-cache invalidation event (81666): sourceUuid@16, partitionUuid@33,
/// sequence@50, then the invalidated key (Data).
pub fn encode_invalidation_event(
    corr: i64,
    source: (i64, i64),
    partition: (i64, i64),
    sequence: i64,
    key: &[u8],
) -> Vec<u8> {
    use protocol::frame::IS_EVENT;
    let mut c = vec![0u8; 58];
    write_i32_le(&mut c, 0, 81666);
    write_i64_le(&mut c, 4, corr);
    write_i32_le(&mut c, 12, -1);
    write_uuid(&mut c, 16, Some(source));
    write_uuid(&mut c, 33, Some(partition));
    write_i64_le(&mut c, 50, sequence);
    let mut frames = vec![Frame { flags: UNFRAGMENTED | IS_EVENT, content: c }];
    frames.push(data_frame(key));
    write_message(&frames)
}

/// MapFetchNearCacheInvalidationMetadata response (81153): empty
/// namePartitionSequenceList (EntryList -> BEGIN/END) + empty partitionUuidList
/// (a 0-length fixed frame). The client starts with no baseline and relies on
/// the delivered invalidations.
pub fn encode_metadata_response(msg_type: i32) -> Vec<Frame> {
    let mut c = vec![0u8; 13];
    write_i32_le(&mut c, 0, msg_type);
    vec![
        initial_frame(c),
        crate::begin_frame(),
        crate::end_frame(),
        Frame { flags: 0, content: Vec::new() },
    ]
}

/// Topic message event (262658): publishTime@16, uuid@24, then the item Data.
pub fn encode_topic_event(corr: i64, publish_time: i64, uuid: (i64, i64), item: &[u8]) -> Vec<u8> {
    use protocol::frame::IS_EVENT;
    let mut c = vec![0u8; 41]; // type@0, corr@4, partitionId@12, publishTime@16, uuid@24
    write_i32_le(&mut c, 0, 262658);
    write_i64_le(&mut c, 4, corr);
    write_i32_le(&mut c, 12, -1);
    write_i64_le(&mut c, 16, publish_time);
    write_uuid(&mut c, 24, Some(uuid));
    let mut frames = vec![Frame { flags: UNFRAGMENTED | IS_EVENT, content: c }];
    frames.push(data_frame(item));
    write_message(&frames)
}

/// Replace: threadId@16; var-frames name, key, value (no ttl).
pub struct ReplaceRequest {
    pub name: String,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub thread_id: i64,
}

pub fn decode_replace(frames: &[Frame]) -> ReplaceRequest {
    ReplaceRequest {
        thread_id: read_i64_le(&frames[0].content, 16),
        name: decode_string(&frames[1]),
        key: frames[2].content.clone(),
        value: frames[3].content.clone(),
    }
}

/// Response carrying a single nullable Data (remove/putIfAbsent/replace).
pub fn data_response(msg_type: i32, val: Option<&[u8]>) -> Vec<Frame> {
    response(msg_type, val)
}

/// Response carrying a boolean at offset 13 (containsKey/containsValue/isEmpty).
pub fn bool_response(msg_type: i32, b: bool) -> Vec<Frame> {
    let mut c = vec![0u8; 14]; // type@0, corr@4, backupAcks@12, bool@13
    write_i32_le(&mut c, 0, msg_type);
    c[13] = if b { 1 } else { 0 };
    vec![initial_frame(c)]
}

/// Response carrying an i32 at offset 13 (size).
pub fn int_response(msg_type: i32, v: i32) -> Vec<Frame> {
    let mut c = vec![0u8; 17]; // type@0, corr@4, backupAcks@12, int@13..17
    write_i32_le(&mut c, 0, msg_type);
    write_i32_le(&mut c, 13, v);
    vec![initial_frame(c)]
}

/// Response carrying an i64 at offset 13 (Ringbuffer size/seq/etc).
pub fn long_response(msg_type: i32, v: i64) -> Vec<Frame> {
    let mut c = vec![0u8; 21]; // type@0, corr@4, backupAcks@12, long@13..21
    write_i32_le(&mut c, 0, msg_type);
    write_i64_le(&mut c, 13, v);
    vec![initial_frame(c)]
}

/// PNCounter response: value (long @13) + replicaCount (int @21), then a
/// replicaTimestamps EntryList<UUID,Long> (one entry: this replica's logical
/// clock, so the client's CRDT vector advances monotonically).
pub fn pncounter_response(
    msg_type: i32,
    value: i64,
    replica_count: i32,
    ts_uuid: (i64, i64),
    ts: i64,
) -> Vec<Frame> {
    let mut c = vec![0u8; 25];
    write_i32_le(&mut c, 0, msg_type);
    write_i64_le(&mut c, 13, value);
    write_i32_le(&mut c, 21, replica_count);
    // single timestamp entry: uuid (17B) + long (8B) = 25 bytes
    let mut ts_frame = vec![0u8; 25];
    write_uuid(&mut ts_frame, 0, Some(ts_uuid));
    write_i64_le(&mut ts_frame, 17, ts);
    vec![initial_frame(c), Frame { flags: 0, content: ts_frame }]
}

/// FlakeId NewIdBatch response: base (long @13), increment (long @21), batchSize (int @29).
pub fn flakeid_response(msg_type: i32, base: i64, increment: i64, batch_size: i32) -> Vec<Frame> {
    let mut c = vec![0u8; 33];
    write_i32_le(&mut c, 0, msg_type);
    write_i64_le(&mut c, 13, base);
    write_i64_le(&mut c, 21, increment);
    write_i32_le(&mut c, 29, batch_size);
    vec![initial_frame(c)]
}

pub struct PutRequest {
    pub name: String,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub thread_id: i64,
    pub ttl: i64,
}

pub fn decode_put(frames: &[Frame]) -> PutRequest {
    let initial = &frames[0].content;
    PutRequest {
        thread_id: read_i64_le(initial, 16),
        ttl: read_i64_le(initial, 24),
        name: decode_string(&frames[1]),
        key: frames[2].content.clone(),
        value: frames[3].content.clone(),
    }
}

pub struct GetRequest {
    pub name: String,
    pub key: Vec<u8>,
    pub thread_id: i64,
}

pub fn decode_get(frames: &[Frame]) -> GetRequest {
    let initial = &frames[0].content;
    GetRequest {
        thread_id: read_i64_le(initial, 16),
        name: decode_string(&frames[1]),
        key: frames[2].content.clone(),
    }
}

fn response(msg_type: i32, value: Option<&[u8]>) -> Vec<Frame> {
    let mut c = vec![0u8; 13]; // type@0, corr@4, backupAcks@12
    write_i32_le(&mut c, 0, msg_type);
    let mut out = vec![initial_frame(c)];
    match value {
        Some(v) => out.push(data_frame(v)),
        None => out.push(null_frame()),
    }
    out
}

pub fn encode_put_response(old: Option<&[u8]>) -> Vec<Frame> {
    response(65793, old)
}

pub fn encode_get_response(val: Option<&[u8]>) -> Vec<Frame> {
    response(66049, val)
}

/// Decode MapAddIndex request. Returns (map_name, type, attributes).
pub fn decode_add_index(frames: &[Frame]) -> (String, i32, Vec<String>) {
    use protocol::fixed::read_i32_le;
    let map_name = decode_string(&frames[1]);
    let ty = if frames.len() > 3 && frames[3].content.len() >= 4 {
        read_i32_le(&frames[3].content, 0)
    } else {
        0
    };
    let mut attributes = Vec::new();
    let mut idx = 6;
    while idx < frames.len() {
        if frames[idx].is_end() {
            break;
        }
        if !frames[idx].is_null() {
            attributes.push(decode_string(&frames[idx]));
        }
        idx += 1;
    }
    (map_name, ty, attributes)
}

pub fn encode_add_index_response() -> Vec<Frame> {
    let mut c = vec![0u8; 14];
    write_i32_le(&mut c, 0, 76033);
    vec![initial_frame(c)]
}

pub fn decode_project(frames: &[Frame]) -> (String, Vec<u8>) {
    (decode_string(&frames[1]), frames[2].content.clone())
}

pub fn decode_project_with_predicate(frames: &[Frame]) -> (String, Vec<u8>, Vec<u8>) {
    (decode_string(&frames[1]), frames[2].content.clone(), frames[3].content.clone())
}

pub fn decode_aggregate(frames: &[Frame]) -> (String, Vec<u8>) {
    (decode_string(&frames[1]), frames[2].content.clone())
}

pub fn decode_aggregate_with_predicate(frames: &[Frame]) -> (String, Vec<u8>, Vec<u8>) {
    (decode_string(&frames[1]), frames[2].content.clone(), frames[3].content.clone())
}

pub fn encode_project_response(list: &[Vec<u8>]) -> Vec<Frame> {
    let mut c = vec![0u8; 13];
    write_i32_le(&mut c, 0, 80641);
    let mut out = vec![initial_frame(c)];
    out.push(crate::begin_frame());
    for item in list {
        out.push(data_frame(item));
    }
    out.push(crate::end_frame());
    out
}

pub fn encode_aggregate_response(val: &[u8]) -> Vec<Frame> {
    let mut c = vec![0u8; 13];
    write_i32_le(&mut c, 0, 87553);
    let mut out = vec![initial_frame(c)];
    out.push(data_frame(val));
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use protocol::message::msg_type;

    #[test]
    fn put_response_null_is_one_null_frame() {
        let f = encode_put_response(None);
        assert_eq!(msg_type(&f), 65793);
        assert!(f[1].is_null());
    }

    #[test]
    fn get_response_carries_value_blob() {
        let f = encode_get_response(Some(&[9, 9, 9]));
        assert_eq!(msg_type(&f), 66049);
        assert_eq!(f[1].content, vec![9, 9, 9]);
    }
}
