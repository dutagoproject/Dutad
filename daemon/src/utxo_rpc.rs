use crate::store;
use duta_core::amount::{format_dut_u64, BASE_UNIT, DISPLAY_UNIT, DUTA_DECIMALS};
use serde_json::json;

fn utxo_response_json(
    txid: &str,
    vout: u64,
    entry: Option<(u64, u64, bool, String)>,
) -> serde_json::Value {
    match entry {
        Some((amount, height, coinbase, pkh)) => json!({
            "found": true,
            "txid": txid,
            "vout": vout,
            "amount": format_dut_u64(amount),
            "amount_display": format_dut_u64(amount),
            "amount_dut": amount,
            "height": height,
            "coinbase": coinbase,
            "pkh": pkh,
            "unit": DISPLAY_UNIT,
            "display_unit": DISPLAY_UNIT,
            "base_unit": BASE_UNIT,
            "decimals": DUTA_DECIMALS
        }),
        None => json!({
            "found": false,
            "txid": txid,
            "vout": vout,
            "unit": DISPLAY_UNIT,
            "display_unit": DISPLAY_UNIT,
            "base_unit": BASE_UNIT,
            "decimals": DUTA_DECIMALS
        }),
    }
}

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

    let body = utxo_response_json(&txid, vout, store::utxo_get(data_dir, &txid, vout));
    respond_json(request, tiny_http::StatusCode(200), body.to_string());
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

#[cfg(test)]
mod tests {
    use super::utxo_response_json;

    #[test]
    fn utxo_response_uses_display_amount_and_raw_base_unit_side_by_side() {
        let body = utxo_response_json("ab", 1, Some((1, 22, false, "pkh".to_string())));
        assert_eq!(body.get("amount").and_then(|x| x.as_str()), Some("0.00000001"));
        assert_eq!(body.get("amount_display").and_then(|x| x.as_str()), Some("0.00000001"));
        assert_eq!(body.get("amount_dut").and_then(|x| x.as_u64()), Some(1));
        assert_eq!(body.get("unit").and_then(|x| x.as_str()), Some("DUTA"));
        assert_eq!(body.get("display_unit").and_then(|x| x.as_str()), Some("DUTA"));
        assert_eq!(body.get("base_unit").and_then(|x| x.as_str()), Some("dut"));
        assert_eq!(body.get("decimals").and_then(|x| x.as_u64()), Some(8));
    }

    #[test]
    fn utxo_not_found_still_exposes_unit_metadata() {
        let body = utxo_response_json("ab", 2, None);
        assert_eq!(body.get("found").and_then(|x| x.as_bool()), Some(false));
        assert_eq!(body.get("unit").and_then(|x| x.as_str()), Some("DUTA"));
        assert_eq!(body.get("display_unit").and_then(|x| x.as_str()), Some("DUTA"));
        assert_eq!(body.get("base_unit").and_then(|x| x.as_str()), Some("dut"));
        assert_eq!(body.get("decimals").and_then(|x| x.as_u64()), Some(8));
    }
}
