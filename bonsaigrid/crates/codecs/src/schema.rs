//! SchemaCodec / FieldDescriptorCodec decode + the schema-list response.
//!
//! Wire form (custom types use BEGIN/END markers):
//!   Schema          = BEGIN, typeName(string), List<FieldDescriptor>, END
//!   FieldDescriptor = BEGIN, initial[kind i32 @0], fieldName(string), END
//!   List<T>         = BEGIN, items..., END   (ListMultiFrame)

use protocol::fixed::{read_i32_le, write_i32_le, write_uuid};
use protocol::frame::Frame;
use protocol::primitives::{decode_string, initial_frame};
use serialization::schema::{FieldDescriptor, Schema};

/// Decode a `Schema` whose BEGIN frame is at `start` (e.g. `start = 1` for a
/// ClientSendSchema request, right after the header frame).
pub fn decode_schema(frames: &[Frame], start: usize) -> Schema {
    // frames[start] = schema BEGIN
    let type_name = decode_string(&frames[start + 1]);
    // frames[start + 2] = field-list BEGIN; fields start at start + 3.
    let mut i = start + 3;
    let mut fields = Vec::new();
    while i < frames.len() && !frames[i].is_end() {
        // FieldDescriptor = BEGIN(i), initial[kind@0](i+1), fieldName(i+2), END(i+3)
        let kind = read_i32_le(&frames[i + 1].content, 0);
        let name = decode_string(&frames[i + 2]);
        fields.push(FieldDescriptor::new(name, kind));
        i += 4;
    }
    Schema::new(type_name, fields)
}

/// Encode a `Schema` custom type (BEGIN, typeName, field list, END) into `out`.
pub fn encode_schema(out: &mut Vec<Frame>, schema: &Schema) {
    use crate::{begin_frame, end_frame};
    use protocol::primitives::string_frame;
    out.push(begin_frame());
    out.push(string_frame(&schema.type_name));
    out.push(begin_frame()); // field list
    for f in &schema.fields {
        out.push(begin_frame());
        let mut initial = vec![0u8; 4];
        write_i32_le(&mut initial, 0, f.kind);
        out.push(Frame { flags: 0, content: initial });
        out.push(string_frame(&f.name));
        out.push(end_frame());
    }
    out.push(end_frame()); // field list
    out.push(end_frame());
}

/// ClientFetchSchema response: nullable Schema.
pub fn encode_fetch_schema_response(msg_type: i32, schema: Option<&Schema>) -> Vec<Frame> {
    let mut c = vec![0u8; 13];
    write_i32_le(&mut c, 0, msg_type);
    let mut frames = vec![initial_frame(c)];
    match schema {
        Some(s) => encode_schema(&mut frames, s),
        None => frames.push(protocol::primitives::null_frame()),
    }
    frames
}

/// Response carrying a `List<UUID>` (e.g. ClientSendSchema's replicated members):
/// a single frame of N * 17-byte UUIDs.
pub fn encode_uuid_list_response(msg_type: i32, uuids: &[(i64, i64)]) -> Vec<Frame> {
    let mut c = vec![0u8; 13];
    write_i32_le(&mut c, 0, msg_type);
    let mut list = vec![0u8; uuids.len() * 17];
    for (i, u) in uuids.iter().enumerate() {
        write_uuid(&mut list, i * 17, Some(*u));
    }
    vec![initial_frame(c), Frame { flags: 0, content: list }]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::begin_frame;
    use crate::end_frame;
    use protocol::primitives::string_frame;
    use serialization::schema::{INT32, STRING};

    fn field_frames(kind: i32, name: &str) -> Vec<Frame> {
        let mut initial = vec![0u8; 4];
        write_i32_le(&mut initial, 0, kind);
        vec![
            begin_frame(),
            Frame { flags: 0, content: initial },
            string_frame(name),
            end_frame(),
        ]
    }

    #[test]
    fn decodes_a_two_field_schema() {
        // header frame + Schema(BEGIN, typeName, listBEGIN, field*, listEND, END)
        let mut frames = vec![Frame { flags: protocol::frame::UNFRAGMENTED, content: vec![0u8; 16] }];
        frames.push(begin_frame()); // schema BEGIN
        frames.push(string_frame("person"));
        frames.push(begin_frame()); // list BEGIN
        frames.extend(field_frames(STRING, "name"));
        frames.extend(field_frames(INT32, "age"));
        frames.push(end_frame()); // list END
        frames.push(end_frame()); // schema END

        let s = decode_schema(&frames, 1);
        assert_eq!(s.type_name, "person");
        assert_eq!(s.fields.len(), 2);
        assert_eq!(s.fields[0].name, "name");
        assert_eq!(s.fields[0].kind, STRING);
        assert_eq!(s.fields[1].name, "age");
        assert_eq!(s.fields[1].kind, INT32);
    }
}
