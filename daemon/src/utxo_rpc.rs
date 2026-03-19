use crate::amount_display::format_dut_fixed_8;
use crate::store;
use duta_core::amount::{BASE_UNIT, DISPLAY_UNIT, DUTA_DECIMALS};
use serde_json::json;

fn wallet_utxo_snapshot_response_json(
    tip_height: u64,
    prune_below: u64,
    utxos: Vec<(String, u64, u64, u64, bool, String)>,
) -> serde_json::Value {
    let entries: Vec<serde_json::Value> = utxos
        .into_iter()
        .map(|(txid, vout, amount, height, coinbase, address)| {
            json!({
                "txid": txid,
                "vout": vout,
                "amount": format_dut_fixed_8(amount),
                "amount_display": format_dut_fixed_8(amount),
                "amount_dut": amount,
                "height": height,
                "coinbase": coinbase,
                "address": address,
                "unit": DISPLAY_UNIT,
                "display_unit": DISPLAY_UNIT,
                "base_unit": BASE_UNIT,
                "decimals": DUTA_DECIMALS
            })
        })
        .collect();
    json!({
        "ok": true,
        "tip_height": tip_height,
        "prune_below": prune_below,
        "count": entries.len(),
        "utxos": entries,
        "unit": DISPLAY_UNIT,
        "display_unit": DISPLAY_UNIT,
        "base_unit": BASE_UNIT,
        "decimals": DUTA_DECIMALS
    })
}

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
            "amount": format_dut_fixed_8(amount),
            "amount_display": format_dut_fixed_8(amount),
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

/// POST /wallet_utxos {"addresses":["dut..."]}
/// Response: {"ok":true,"tip_height":n,"utxos":[...]}
pub fn handle_wallet_utxos(
    mut request: tiny_http::Request,
    data_dir: &str,
    respond_json: fn(tiny_http::Request, tiny_http::StatusCode, String),
) {
    let body = match crate::read_body_limited(&mut request) {
        Ok(body) => body,
        Err(detail) => {
            respond_json(
                request,
                tiny_http::StatusCode(413),
                json!({"ok":false,"error":"body_too_large","detail":detail}).to_string(),
            );
            return;
        }
    };
    let value: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(_) => {
            respond_json(
                request,
                tiny_http::StatusCode(400),
                json!({"ok":false,"error":"invalid_json","detail":"invalid_json"}).to_string(),
            );
            return;
        }
    };
    let Some(addresses_v) = value.get("addresses").and_then(|x| x.as_array()) else {
        respond_json(
            request,
            tiny_http::StatusCode(400),
            json!({"ok":false,"error":"bad_request","detail":"missing_addresses"}).to_string(),
        );
        return;
    };
    let mut addresses = Vec::new();
    for address in addresses_v {
        let Some(address) = address.as_str() else {
            respond_json(
                request,
                tiny_http::StatusCode(400),
                json!({"ok":false,"error":"bad_request","detail":"addresses_must_be_strings"}).to_string(),
            );
            return;
        };
        if !address.trim().is_empty() {
            addresses.push(address.trim().to_string());
        }
    }
    if addresses.is_empty() {
        let tip_height = store::tip_fields(data_dir).map(|(h, _, _, _)| h).unwrap_or(0);
        let body = wallet_utxo_snapshot_response_json(tip_height, store::prune_below(data_dir), Vec::new());
        respond_json(request, tiny_http::StatusCode(200), body.to_string());
        return;
    }
    match store::utxos_for_addresses(data_dir, &addresses) {
        Ok(utxos) => {
            let tip_height = store::tip_fields(data_dir).map(|(h, _, _, _)| h).unwrap_or(0);
            let body = wallet_utxo_snapshot_response_json(tip_height, store::prune_below(data_dir), utxos);
            respond_json(request, tiny_http::StatusCode(200), body.to_string());
        }
        Err(detail) => respond_json(
            request,
            tiny_http::StatusCode(400),
            json!({"ok":false,"error":"wallet_utxo_snapshot_failed","detail":detail}).to_string(),
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

#[cfg(test)]
mod tests {
    use super::{utxo_response_json, wallet_utxo_snapshot_response_json};

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
    fn utxo_response_keeps_fixed_eight_decimal_display_amounts() {
        let body = utxo_response_json("ab", 1, Some((10_000, 22, false, "pkh".to_string())));
        assert_eq!(body.get("amount").and_then(|x| x.as_str()), Some("0.00010000"));
        assert_eq!(
            body.get("amount_display").and_then(|x| x.as_str()),
            Some("0.00010000")
        );
        assert_eq!(body.get("amount_dut").and_then(|x| x.as_u64()), Some(10_000));
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

    #[test]
    fn wallet_utxo_snapshot_response_exposes_raw_and_display_amounts() {
        let body = wallet_utxo_snapshot_response_json(
            77,
            55,
            vec![(
                "ab".to_string(),
                1,
                10_000,
                33,
                false,
                "dut1111111111111111111111111111111111111111".to_string(),
            )],
        );
        assert_eq!(body.get("ok").and_then(|x| x.as_bool()), Some(true));
        assert_eq!(body.get("tip_height").and_then(|x| x.as_u64()), Some(77));
        assert_eq!(body.get("prune_below").and_then(|x| x.as_u64()), Some(55));
        let utxos = body.get("utxos").and_then(|x| x.as_array()).unwrap();
        assert_eq!(utxos.len(), 1);
        assert_eq!(utxos[0].get("amount").and_then(|x| x.as_str()), Some("0.00010000"));
        assert_eq!(utxos[0].get("amount_dut").and_then(|x| x.as_u64()), Some(10_000));
        assert_eq!(utxos[0].get("coinbase").and_then(|x| x.as_bool()), Some(false));
    }
}
