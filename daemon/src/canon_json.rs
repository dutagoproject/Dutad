use serde_json::Value;

/// Deterministic JSON encoding for consensus hashing (txid/merkle).
///
/// Notes:
/// - Objects are serialized with keys sorted lexicographically.
/// - Arrays preserve order.
/// - Numbers must be integer (u64/i64). Floats are rejected.
///
/// This is a stepping stone toward full binary consensus serialization.
pub fn canonical_json_bytes(v: &Value) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    write_canon(v, &mut out)?;
    Ok(out)
}

fn write_canon(v: &Value, out: &mut Vec<u8>) -> Result<(), String> {
    match v {
        Value::Null => out.extend_from_slice(b"null"),
        Value::Bool(b) => {
            if *b {
                out.extend_from_slice(b"true")
            } else {
                out.extend_from_slice(b"false")
            }
        }
        Value::Number(n) => {
            if let Some(u) = n.as_u64() {
                out.extend_from_slice(u.to_string().as_bytes());
            } else if let Some(i) = n.as_i64() {
                out.extend_from_slice(i.to_string().as_bytes());
            } else {
                return Err("float_not_allowed".to_string());
            }
        }
        Value::String(s) => {
            // Use serde_json for correct escaping/quoting.
            let q = serde_json::to_string(s).map_err(|e| format!("json_string_failed: {}", e))?;
            out.extend_from_slice(q.as_bytes());
        }
        Value::Array(arr) => {
            out.push(b'[');
            for (idx, item) in arr.iter().enumerate() {
                if idx != 0 {
                    out.push(b',');
                }
                write_canon(item, out)?;
            }
            out.push(b']');
        }
        Value::Object(obj) => {
            out.push(b'{');
            let mut keys: Vec<&String> = obj.keys().collect();
            keys.sort();
            for (idx, k) in keys.iter().enumerate() {
                if idx != 0 {
                    out.push(b',');
                }
                let qk = serde_json::to_string(k).map_err(|e| format!("json_key_failed: {}", e))?;
                out.extend_from_slice(qk.as_bytes());
                out.push(b':');
                write_canon(&obj[*k], out)?;
            }
            out.push(b'}');
        }
    }
    Ok(())
}
