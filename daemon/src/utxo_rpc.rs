use crate::store;
use serde_json::json;

/// GET /utxo?txid=<hex>&vout=<n>
/// Response: {"found":bool, ...}
pub fn handle_utxo(
    request: tiny_http::Request,
    data_dir: &str,
    respond_json: fn(tiny_http::Request, tiny_http::StatusCode, String),
) {
    let url = request.url().to_string();

    let mut txid: Option<String> = None;
    let mut vout: Option<u64> = None;

    if let Some((_, q)) = url.split_once('?') {
        for part in q.split('&') {
            if part.is_empty() {
                continue;
            }
            let (k, v) = part.split_once('=').unwrap_or((part, ""));
            match k {
                "txid" => {
                    let t = percent_decode(v);
                    if !t.is_empty() {
                        txid = Some(t);
                    }
                }
                "vout" => {
                    if let Ok(n) = v.parse::<u64>() {
                        vout = Some(n);
                    }
                }
                _ => {}
            }
        }
    }

    let txid = match txid {
        Some(t) => t,
        None => {
            respond_json(
                request,
                tiny_http::StatusCode(400),
                json!({"ok":false,"error":"bad_request","detail":"missing_txid"}).to_string(),
            );
            return;
        }
    };
    let vout = match vout {
        Some(n) => n,
        None => {
            respond_json(
                request,
                tiny_http::StatusCode(400),
                json!({"ok":false,"error":"bad_request","detail":"missing_vout"}).to_string(),
            );
            return;
        }
    };

    match store::utxo_get(data_dir, &txid, vout) {
        Some((amount, height, coinbase, pkh)) => respond_json(
            request,
            tiny_http::StatusCode(200),
            json!({"found":true,"txid":txid,"vout":vout,"amount":amount,"height":height,"coinbase":coinbase,"pkh":pkh}).to_string(),
        ),
        None => respond_json(
            request,
            tiny_http::StatusCode(200),
            json!({"found":false,"txid":txid,"vout":vout}).to_string(),
        ),
    }
}

fn percent_decode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i: usize = 0;

    fn hex(c: u8) -> Option<u8> {
        match c {
            b'0'..=b'9' => Some(c - b'0'),
            b'a'..=b'f' => Some(c - b'a' + 10),
            b'A'..=b'F' => Some(c - b'A' + 10),
            _ => None,
        }
    }

    while i < bytes.len() {
        match bytes[i] {
            b'%' => {
                if i + 2 < bytes.len() {
                    if let (Some(a), Some(b)) = (hex(bytes[i + 1]), hex(bytes[i + 2])) {
                        out.push((a * 16 + b) as char);
                        i += 3;
                        continue;
                    }
                }
                out.push('%');
                i += 1;
            }
            b'+' => {
                out.push(' ');
                i += 1;
            }
            c => {
                out.push(c as char);
                i += 1;
            }
        }
    }
    out
}
