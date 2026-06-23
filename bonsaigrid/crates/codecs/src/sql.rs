//! SqlExecute / SqlClose / SqlFetch codecs (MVP: single-page text results).
//!
//! Request layout: an initial frame with fixed fields (timeout/cursor/result-type/
//! skip), then the SQL string, parameters, schema, query id. We only need the SQL.
//!
//! Response (SqlExecute, type 2163713): initial frame [update_count i64 @13], then
//! `List<SqlColumnMetadata>` (nullable), a nullable `SqlPage`, and a nullable
//! `SqlError`. Every column is VARCHAR (type id 0); the page is single and final.

use crate::{begin_frame, end_frame};
use protocol::fixed::{write_i32_le, write_i64_le};
use protocol::frame::Frame;
use protocol::primitives::{decode_string, initial_frame, null_frame, string_frame};

const SQL_VARCHAR: i32 = 0;

/// The SQL string from a SqlExecute request (first var-frame after the header).
pub fn decode_execute_sql(frames: &[Frame]) -> String {
    decode_string(&frames[1])
}

/// A successful SqlExecute response: `columns` (all VARCHAR) and `rows` (each cell
/// text or NULL).
pub fn encode_execute_response(columns: &[String], rows: &[Vec<Option<String>>]) -> Vec<Frame> {
    let mut hdr = vec![0u8; 21]; // type@0, corr@4, backupAcks@12, update_count i64 @13
    write_i32_le(&mut hdr, 0, 2163713);
    write_i64_le(&mut hdr, 13, -1); // -1 == this is a row result, not an update
    let mut out = vec![initial_frame(hdr)];

    // row_metadata: List<SqlColumnMetadata> (non-null).
    out.push(begin_frame());
    for name in columns {
        out.push(begin_frame()); // column struct BEGIN
        let mut meta = vec![0u8; 5]; // type i32 @0 (VARCHAR), nullable u8 @4
        write_i32_le(&mut meta, 0, SQL_VARCHAR);
        meta[4] = 1; // nullable
        out.push(Frame { flags: 0, content: meta });
        out.push(string_frame(name));
        out.push(end_frame());
    }
    out.push(end_frame());

    // row_page (non-null): BEGIN, is_last, column_type_ids, per-column data, END.
    out.push(begin_frame());
    out.push(Frame { flags: 0, content: vec![1u8] }); // is_last = true
    out.push(Frame { flags: 0, content: vec![0u8; columns.len() * 4] }); // type ids: all VARCHAR (0)
    for col in 0..columns.len() {
        out.push(begin_frame()); // List<String> contains-nullable
        for row in rows {
            match &row[col] {
                Some(s) => out.push(string_frame(s)),
                None => out.push(null_frame()),
            }
        }
        out.push(end_frame());
    }
    out.push(end_frame()); // page END

    out.push(null_frame()); // error: null
    out
}

/// SqlClose response (empty ack), type 2163457.
pub fn encode_close_response() -> Vec<Frame> {
    let mut c = vec![0u8; 13];
    write_i32_le(&mut c, 0, 2163457);
    vec![initial_frame(c)]
}

/// SqlFetch response (type 2163969): an empty final page + null error.
pub fn encode_fetch_response() -> Vec<Frame> {
    let mut c = vec![0u8; 13];
    write_i32_le(&mut c, 0, 2163969);
    vec![
        initial_frame(c),
        begin_frame(),                          // page BEGIN
        Frame { flags: 0, content: vec![1u8] }, // is_last
        Frame { flags: 0, content: Vec::new() }, // zero columns
        end_frame(),                            // page END
        null_frame(),                           // error null
    ]
}
