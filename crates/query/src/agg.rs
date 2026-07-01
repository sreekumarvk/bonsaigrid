use serialization::compact::FieldValue;

#[derive(Clone, Debug, PartialEq)]
pub enum AggregationType {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

#[derive(Clone, Debug)]
pub struct Aggregator {
    pub ty: AggregationType,
    pub attribute_path: Option<String>,
}

pub fn decode_aggregator(data: &[u8]) -> Option<Aggregator> {
    if data.len() < 17 {
        return None;
    }
    let payload = &data[8..];
    if payload[0] != 1 {
        return None;
    }
    let class_id = i32::from_be_bytes(payload[5..9].try_into().unwrap());

    let ty = match class_id {
        4 => AggregationType::Count,
        1 | 3 | 7 | 8 | 9 | 11 | 13 => AggregationType::Sum,
        0 | 2 | 6 | 10 | 12 | 16 => AggregationType::Avg,
        14 => AggregationType::Max,
        15 => AggregationType::Min,
        _ => return None,
    };

    let is_present = payload[9];
    let attribute_path = if is_present != 0 {
        if payload.len() >= 14 {
            let len = i32::from_be_bytes(payload[10..14].try_into().unwrap()) as usize;
            if 14 + len <= payload.len() {
                Some(String::from_utf8_lossy(&payload[14..14 + len]).into_owned())
            } else {
                None
            }
        } else {
            None
        }
    } else {
        None
    };

    Some(Aggregator { ty, attribute_path })
}

pub fn execute_aggregation(
    agg: &Aggregator,
    entries: &[(Vec<u8>, Vec<u8>)],
    schemas: &serialization::schema::SchemaService,
) -> FieldValue {
    let ex = serialization::compact::AutoExtractor;
    let attr = agg.attribute_path.as_deref().unwrap_or("");

    match agg.ty {
        AggregationType::Count => {
            let count = if attr.is_empty() {
                entries.len()
            } else {
                entries
                    .iter()
                    .filter(|(_, v)| {
                        serialization::compact::FieldExtractor::extract(&ex, v, schemas, attr)
                            != FieldValue::Null
                    })
                    .count()
            };
            FieldValue::I64(count as i64)
        }
        AggregationType::Sum => {
            if attr.is_empty() {
                return FieldValue::Null;
            }
            let mut sum = 0.0;
            let mut has_val = false;
            for (_, v) in entries {
                let fv = serialization::compact::FieldExtractor::extract(&ex, v, schemas, attr);
                match fv {
                    FieldValue::I32(i) => {
                        sum += i as f64;
                        has_val = true;
                    }
                    FieldValue::I64(i) => {
                        sum += i as f64;
                        has_val = true;
                    }
                    FieldValue::F64(f) => {
                        sum += f;
                        has_val = true;
                    }
                    _ => {}
                }
            }
            if has_val {
                FieldValue::F64(sum)
            } else {
                FieldValue::Null
            }
        }
        AggregationType::Avg => {
            if attr.is_empty() {
                return FieldValue::Null;
            }
            let mut sum = 0.0;
            let mut count = 0;
            for (_, v) in entries {
                let fv = serialization::compact::FieldExtractor::extract(&ex, v, schemas, attr);
                match fv {
                    FieldValue::I32(i) => {
                        sum += i as f64;
                        count += 1;
                    }
                    FieldValue::I64(i) => {
                        sum += i as f64;
                        count += 1;
                    }
                    FieldValue::F64(f) => {
                        sum += f;
                        count += 1;
                    }
                    _ => {}
                }
            }
            if count > 0 {
                FieldValue::F64(sum / count as f64)
            } else {
                FieldValue::Null
            }
        }
        AggregationType::Min => {
            if attr.is_empty() {
                return FieldValue::Null;
            }
            let mut min_fv = None;
            for (_, v) in entries {
                let fv = serialization::compact::FieldExtractor::extract(&ex, v, schemas, attr);
                if fv != FieldValue::Null {
                    if let Some(ref m) = min_fv {
                        if fv.compare(m) == Some(std::cmp::Ordering::Less) {
                            min_fv = Some(fv);
                        }
                    } else {
                        min_fv = Some(fv);
                    }
                }
            }
            min_fv.unwrap_or(FieldValue::Null)
        }
        AggregationType::Max => {
            if attr.is_empty() {
                return FieldValue::Null;
            }
            let mut max_fv = None;
            for (_, v) in entries {
                let fv = serialization::compact::FieldExtractor::extract(&ex, v, schemas, attr);
                if fv != FieldValue::Null {
                    if let Some(ref m) = max_fv {
                        if fv.compare(m) == Some(std::cmp::Ordering::Greater) {
                            max_fv = Some(fv);
                        }
                    } else {
                        max_fv = Some(fv);
                    }
                }
            }
            max_fv.unwrap_or(FieldValue::Null)
        }
    }
}

pub fn execute_projection(
    attribute_path: &str,
    entries: &[(Vec<u8>, Vec<u8>)],
    schemas: &serialization::schema::SchemaService,
) -> Vec<Vec<u8>> {
    let ex = serialization::compact::AutoExtractor;
    let mut out = Vec::with_capacity(entries.len());
    for (_, v) in entries {
        let fv = serialization::compact::FieldExtractor::extract(&ex, v, schemas, attribute_path);
        let data = serialization::compact::encode_scalar(&fv);
        out.push(data);
    }
    out
}

pub fn extract_attribute_from_projection(data: &[u8]) -> Option<String> {
    if data.len() < 12 {
        return None;
    }
    let payload = &data[8..];
    if payload[0] == 1 && payload.len() >= 15 {
        let is_present = payload[9];
        if is_present != 0 {
            let len = i32::from_be_bytes(payload[10..14].try_into().unwrap()) as usize;
            if 14 + len <= payload.len() {
                return Some(String::from_utf8_lossy(&payload[14..14 + len]).into_owned());
            }
        }
    }
    None
}
