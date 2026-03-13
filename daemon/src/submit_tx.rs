use serde_json::json;
use std::collections::HashSet;
use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::canon_json;
use crate::store;
use duta_core::address;
use duta_core::hash;
use ed25519_dalek::{Signature, VerifyingKey};

const COINBASE_MATURITY: u64 = 60;

// Production-hardening caps (phase 1)
const MAX_TX_BYTES: usize = 100_000;
const MAX_MEMPOOL_TXS: usize = 10_000;
const MAX_MEMPOOL_BYTES: usize = 5_000_000; // 5MB
const MIN_RELAY_FEE_PER_KB: u64 = 1;

// Gate C (phase 1): anti-UTXO-bloat policy.
// Reject outputs below this value and cap outputs per tx.
const MIN_OUTPUT_VALUE: u64 = 1;

// Gate C: orphan tx policy (P2P-only, phase 1)
const ORPHAN_MAX_TXS: usize = 256;
const ORPHAN_MAX_BYTES: usize = 2_000_000;
const ORPHAN_TTL_SECS: u64 = 600;
const MAX_TX_VOUTS: usize = 64;
const TXID_HEX_LEN: usize = 64;

fn decode_hex_fixed(s: &str, n: usize) -> Result<Vec<u8>, String> {
    let b = hex::decode(s).map_err(|_| "invalid_hex".to_string())?;
    if b.len() != n {
        return Err(format!("invalid_len: expected {} got {}", n, b.len()));
    }
    Ok(b)
}

/// Sighash preimage: tx with each vin's "sig" and "pubkey" stripped.
/// Canonical JSON bytes are hashed with SHA3-256.
fn sighash(txv: &serde_json::Value) -> Result<[u8; 32], String> {
    let mut t = txv.clone();
    if let Some(vins) = t.get_mut("vin").and_then(|x| x.as_array_mut()) {
        for vin in vins.iter_mut() {
            if let Some(o) = vin.as_object_mut() {
                o.remove("sig");
                o.remove("pubkey");
            }
        }
    }
    let b = canon_json::canonical_json_bytes(&t)?;
    Ok(hash::sha3_256(&b).0)
}

fn txid_from_value(v: &serde_json::Value) -> Result<String, String> {
    let b = canon_json::canonical_json_bytes(v)?;
    Ok(hash::sha3_256_hex(&b))
}

fn required_relay_fee(tx_bytes: usize) -> u64 {
    // Fee market (phase 1): require at least MIN_RELAY_FEE_PER_KB per started 1000 bytes.
    let kb = (tx_bytes.saturating_add(999) / 1000).max(1);
    (kb as u64).saturating_mul(MIN_RELAY_FEE_PER_KB)
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn ensure_orphan_shape(mp: &mut serde_json::Value) {
    if mp.get("orphans").and_then(|x| x.as_object()).is_none() {
        mp["orphans"] = json!({});
    }
    if let Some(o) = mp.get_mut("orphans").and_then(|x| x.as_object_mut()) {
        if o.get("txids").and_then(|x| x.as_array()).is_none() {
            o.insert("txids".to_string(), json!([]));
        }
        if o.get("txs").and_then(|x| x.as_object()).is_none() {
            o.insert("txs".to_string(), json!({}));
        }
        if o.get("meta").and_then(|x| x.as_object()).is_none() {
            o.insert("meta".to_string(), json!({}));
        }
    }
}

fn orphan_prune(mp: &mut serde_json::Value, now: u64) {
    ensure_orphan_shape(mp);
    let ttl = ORPHAN_TTL_SECS;
    // Drop expired
    let mut keep: Vec<String> = Vec::new();
    let txids = mp["orphans"]["txids"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    for v in txids {
        if let Some(txid) = v.as_str() {
            let ts = mp["orphans"]["meta"][txid]
                .get("ts")
                .and_then(|x| x.as_u64())
                .unwrap_or(0);
            if ts == 0 || now.saturating_sub(ts) <= ttl {
                keep.push(txid.to_string());
            } else {
                mp["orphans"]["txs"].as_object_mut().map(|o| o.remove(txid));
                mp["orphans"]["meta"]
                    .as_object_mut()
                    .map(|o| o.remove(txid));
            }
        }
    }
    // Enforce caps (deterministic: oldest ts then txid)
    loop {
        let txs_obj = match mp["orphans"]["txs"].as_object() {
            Some(o) => o,
            None => break,
        };
        let mut bytes: usize = 0;
        for (k, _v) in txs_obj.iter() {
            let b = mp["orphans"]["meta"][k]
                .get("bytes")
                .and_then(|x| x.as_u64())
                .unwrap_or(0) as usize;
            bytes = bytes.saturating_add(b);
        }
        let txs_len = txs_obj.len();
        if txs_len <= ORPHAN_MAX_TXS && bytes <= ORPHAN_MAX_BYTES {
            break;
        }
        // pick oldest
        let mut victims: Vec<(u64, String)> = Vec::new();
        for k in txs_obj.keys() {
            let ts = mp["orphans"]["meta"][k]
                .get("ts")
                .and_then(|x| x.as_u64())
                .unwrap_or(0);
            victims.push((ts, k.clone()));
        }
        victims.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
        if let Some((_ts, victim)) = victims.first().cloned() {
            mp["orphans"]["txs"]
                .as_object_mut()
                .map(|o| o.remove(&victim));
            mp["orphans"]["meta"]
                .as_object_mut()
                .map(|o| o.remove(&victim));
            keep.retain(|x| x != &victim);
        } else {
            break;
        }
    }
    mp["orphans"]["txids"] = json!(keep);
}

fn orphan_add(mp: &mut serde_json::Value, txid: &str, tx: &serde_json::Value, bytes: usize) {
    ensure_orphan_shape(mp);
    let now = now_secs();
    if mp["orphans"]["txs"].get(txid).is_none() {
        let mut txids = mp["orphans"]["txids"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        txids.push(json!(txid));
        mp["orphans"]["txids"] = serde_json::Value::Array(txids);
    }
    mp["orphans"]["txs"][txid] = tx.clone();
    mp["orphans"]["meta"][txid] = json!({"ts": now, "bytes": bytes as u64});
    orphan_prune(mp, now);
}

fn orphan_try_promote(data_dir: &str, mp: &mut serde_json::Value) {
    ensure_orphan_shape(mp);
    let txids = mp["orphans"]["txids"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let mut promoted: Vec<String> = Vec::new();
    for v in txids.iter() {
        let txid = match v.as_str() {
            Some(s) => s.to_string(),
            None => continue,
        };
        let tx = match mp["orphans"]["txs"].get(&txid) {
            Some(t) => t.clone(),
            None => continue,
        };
        let bytes = serde_json::to_vec(&tx).map(|b| b.len()).unwrap_or(0);
        if bytes == 0 {
            continue;
        }
        match validate_tx_basic_db(data_dir, mp, &tx, bytes) {
            Ok(fee) => {
                // insert like normal
                let mut tx_store = tx.clone();
                if let Some(obj) = tx_store.as_object_mut() {
                    obj.insert("fee".to_string(), json!(fee));
                    obj.insert("size".to_string(), json!(bytes as u64));
                }
                mp["txs"][txid.clone()] = tx_store;
                let mut mptxids = mp["txids"].as_array().cloned().unwrap_or_default();
                if !mptxids.iter().any(|x| x.as_str() == Some(&txid)) {
                    mptxids.push(json!(txid.clone()));
                    mp["txids"] = serde_json::Value::Array(mptxids);
                }
                promoted.push(txid.clone());
            }
            Err(_) => {}
        }
    }
    // remove promoted from orphan pool
    if !promoted.is_empty() {
        for txid in promoted.iter() {
            mp["orphans"]["txs"].as_object_mut().map(|o| o.remove(txid));
            mp["orphans"]["meta"]
                .as_object_mut()
                .map(|o| o.remove(txid));
        }
        let mut keep: Vec<String> = Vec::new();
        let txids2 = mp["orphans"]["txids"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        for v in txids2 {
            if let Some(s) = v.as_str() {
                if !promoted.iter().any(|p| p == s) {
                    keep.push(s.to_string());
                }
            }
        }
        mp["orphans"]["txids"] = json!(keep);
    }
}

pub fn orphan_try_promote_file(data_dir: &str) {
    // Best-effort: load mempool.json, prune/expire orphans, try promote, save.
    let mut mp = load_mempool(data_dir);
    ensure_orphan_shape(&mut mp);
    let now = now_secs();
    orphan_prune(&mut mp, now);
    orphan_try_promote(data_dir, &mut mp);
    let _ = save_mempool(data_dir, &mp);
}

fn feerate_cmp(a_fee: u64, a_size: usize, b_fee: u64, b_size: usize) -> std::cmp::Ordering {
    let asz = (a_size.max(1)) as u128;
    let bsz = (b_size.max(1)) as u128;
    let lhs = (a_fee as u128).saturating_mul(bsz);
    let rhs = (b_fee as u128).saturating_mul(asz);
    lhs.cmp(&rhs)
}

fn mempool_path(data_dir: &str) -> String {
    format!("{}/mempool.json", data_dir)
}

fn load_mempool(data_dir: &str) -> serde_json::Value {
    let path = mempool_path(data_dir);
    let s = match fs::read_to_string(&path) {
        Ok(s) => s,
        Err(_) => return json!({"txids": [], "txs": {}}),
    };
    match serde_json::from_str::<serde_json::Value>(&s) {
        Ok(v) => v,
        Err(_) => json!({"txids": [], "txs": {}}),
    }
}

fn save_mempool(data_dir: &str, v: &serde_json::Value) -> Result<(), String> {
    let path = mempool_path(data_dir);
    let body = serde_json::to_string(v).map_err(|e| format!("json_encode_failed: {}", e))?;
    store::durable_write_string(&path, &body).map_err(|e| format!("write_failed: {}", e))?;
    Ok(())
}

fn outpoint_str(txid: &str, vout: u64) -> String {
    format!("{}:{}", txid, vout)
}

fn short_txid(txid: &str) -> &str {
    if txid.len() <= 8 {
        txid
    } else {
        &txid[..8]
    }
}

fn txid_is_valid(txid: &str) -> bool {
    txid.len() == TXID_HEX_LEN && txid.bytes().all(|b| b.is_ascii_hexdigit())
}

fn mempool_spent_outpoints(mp: &serde_json::Value) -> HashSet<String> {
    let mut s = HashSet::new();
    let Some(txs) = mp.get("txs").and_then(|x| x.as_object()) else {
        return s;
    };
    for (_txid, txv) in txs {
        let vin = txv
            .get("vin")
            .and_then(|x| x.as_array())
            .cloned()
            .unwrap_or_default();
        for inv in vin {
            let prev_txid = inv.get("txid").and_then(|x| x.as_str()).unwrap_or("");
            let prev_vout = inv.get("vout").and_then(|x| x.as_u64()).unwrap_or(0);
            if !prev_txid.is_empty() {
                s.insert(outpoint_str(prev_txid, prev_vout));
            }
        }
    }
    s
}

fn validate_tx_basic_db(
    data_dir: &str,
    mp: &serde_json::Value,
    tx: &serde_json::Value,
    tx_bytes: usize,
) -> Result<u64, String> {
    let vin = tx
        .get("vin")
        .and_then(|x| x.as_array())
        .cloned()
        .unwrap_or_default();
    let vout = tx
        .get("vout")
        .and_then(|x| x.as_array())
        .cloned()
        .unwrap_or_default();

    if vin.is_empty() {
        return Err("coinbase_not_allowed".to_string());
    }
    if vout.is_empty() {
        return Err("invalid_tx_no_outputs".to_string());
    }
    if vout.len() > MAX_TX_VOUTS {
        return Err("too_many_outputs".to_string());
    }

    let tip_h = crate::store::tip_fields(data_dir)
        .map(|(h, _, _, _)| h)
        .unwrap_or(0);

    let mp_spent = mempool_spent_outpoints(mp);

    let mut seen_in: HashSet<String> = HashSet::new();
    let mut sum_in: u64 = 0;

    for inv in &vin {
        let prev_txid = inv.get("txid").and_then(|x| x.as_str()).unwrap_or("");
        let prev_vout = inv.get("vout").and_then(|x| x.as_u64()).unwrap_or(0);
        if prev_txid.is_empty() {
            return Err("invalid_input".to_string());
        }
        let op = outpoint_str(prev_txid, prev_vout);

        if !seen_in.insert(op.clone()) {
            return Err("double_spend".to_string());
        }
        if mp_spent.contains(&op) {
            return Err("double_spend".to_string());
        }

        let (value, created_height, is_coinbase, pkh) =
            crate::store::utxo_get(data_dir, prev_txid, prev_vout)
                .ok_or_else(|| "input_not_found".to_string())?;

        if is_coinbase {
            let age = tip_h.saturating_sub(created_height);
            if age < COINBASE_MATURITY {
                return Err("immature_coinbase".to_string());
            }
        }

        // Ownership check (Gate 1 step 3): P2PKH-like with Ed25519.
        let pubkey_hex = inv.get("pubkey").and_then(|x| x.as_str()).unwrap_or("");
        let sig_hex = inv.get("sig").and_then(|x| x.as_str()).unwrap_or("");
        if pubkey_hex.is_empty() || sig_hex.is_empty() {
            return Err("missing_sig".to_string());
        }

        let pubkey_b = decode_hex_fixed(pubkey_hex, 32).map_err(|_| "bad_pubkey".to_string())?;
        let sig_b = decode_hex_fixed(sig_hex, 64).map_err(|_| "bad_sig".to_string())?;

        let mut pk_arr = [0u8; 32];
        pk_arr.copy_from_slice(&pubkey_b);
        let vk = VerifyingKey::from_bytes(&pk_arr).map_err(|_| "bad_pubkey".to_string())?;

        let mut sig_arr = [0u8; 64];
        sig_arr.copy_from_slice(&sig_b);
        let sig = Signature::from_bytes(&sig_arr);

        let msg = sighash(tx)?;
        vk.verify_strict(&msg, &sig)
            .map_err(|_| "bad_sig".to_string())?;

        let want = address::pkh_to_hex(&address::pkh_from_pubkey(&pubkey_b));
        if !pkh.is_empty() && want != pkh {
            return Err("wrong_pubkey".to_string());
        }

        sum_in = sum_in.saturating_add(value);
    }

    let mut sum_out: u64 = 0;
    for ov in &vout {
        let addr = ov.get("address").and_then(|x| x.as_str()).unwrap_or("");
        let net = crate::store::read_datadir_network(data_dir).unwrap_or_else(|| {
            if data_dir.contains("testnet") {
                duta_core::netparams::Network::Testnet
            } else if data_dir.contains("stagenet") {
                duta_core::netparams::Network::Stagenet
            } else {
                duta_core::netparams::Network::Mainnet
            }
        });
        if address::parse_address_for_network(net, addr).is_none() {
            return Err("invalid_address".to_string());
        }
        let value = ov.get("value").and_then(|x| x.as_u64()).unwrap_or(0);
        if value < MIN_OUTPUT_VALUE {
            return Err("dust_output".to_string());
        }
        sum_out = sum_out.saturating_add(value);
    }

    if sum_in < sum_out {
        return Err("insufficient_inputs".to_string());
    }

    let actual_fee = sum_in.saturating_sub(sum_out);

    let need_fee = required_relay_fee(tx_bytes);
    if actual_fee < need_fee {
        return Err("fee_too_low".to_string());
    }

    Ok(actual_fee)
}

fn remove_tx_from_mempool(mp: &mut serde_json::Value, txid: &str) -> bool {
    mp.get_mut("txs")
        .and_then(|x| x.as_object_mut())
        .map(|txs| txs.remove(txid).is_some())
        .unwrap_or(false)
}

fn parse_submit_tx_request(
    v: &serde_json::Value,
) -> Result<(serde_json::Value, Option<String>), &'static str> {
    let tx = v.get("tx").ok_or("missing_tx")?;
    if !tx.is_object() {
        return Err("invalid_tx");
    }
    let txid = v
        .get("txid")
        .and_then(|x| x.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty());
    if let Some(txid) = txid {
        if !txid_is_valid(txid) {
            return Err("invalid_txid");
        }
        return Ok((tx.clone(), Some(txid.to_string())));
    }
    Ok((tx.clone(), None))
}

fn submit_tx_status_and_detail(code: &str) -> (tiny_http::StatusCode, serde_json::Value) {
    match code {
        "fee_too_low" => (tiny_http::StatusCode(422), json!({"error":"fee_too_low"})),
        "dust_output" => (
            tiny_http::StatusCode(422),
            json!({"error":"dust_output","min_output":MIN_OUTPUT_VALUE}),
        ),
        "too_many_outputs" => (
            tiny_http::StatusCode(422),
            json!({"error":"too_many_outputs","max_outputs":MAX_TX_VOUTS}),
        ),
        "coinbase_not_allowed"
        | "double_spend"
        | "input_not_found"
        | "immature_coinbase"
        | "insufficient_inputs"
        | "invalid_address"
        | "invalid_fee"
        | "invalid_input"
        | "invalid_tx"
        | "invalid_tx_no_outputs"
        | "invalid_txid"
        | "missing_sig"
        | "missing_tx"
        | "wrong_pubkey"
        | "bad_pubkey"
        | "bad_sig"
        | "tx_too_large"
        | "mempool_full" => (
            tiny_http::StatusCode(400),
            json!({"error":code,"detail":code}),
        ),
        _ => (
            tiny_http::StatusCode(500),
            json!({"error":code,"detail":code}),
        ),
    }
}

/// Ingest a tx into mempool (used by both HTTP submit and P2P). Returns txid.
pub fn ingest_tx(
    data_dir: &str,
    txid_opt: Option<&str>,
    tx: &serde_json::Value,
) -> Result<String, String> {
    let tx_bytes = serde_json::to_vec(tx).map_err(|e| format!("json_encode_failed: {}", e))?;
    if tx_bytes.len() > MAX_TX_BYTES {
        return Err("tx_too_large".to_string());
    }
    let txid = txid_opt
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .unwrap_or_else(|| txid_from_value(tx).unwrap_or_else(|_| hash::sha3_256_hex(&tx_bytes)));

    let mut mp = load_mempool(data_dir);

    // Ensure shape: {"txids":[...], "txs":{...}}
    if mp.get("txids").and_then(|x| x.as_array()).is_none() {
        mp["txids"] = json!([]);
    }
    if mp.get("txs").and_then(|x| x.as_object()).is_none() {
        mp["txs"] = json!({});
    }

    // Validate before insert unless already present.
    if mp["txs"].get(&txid).is_none() {
        let fee = validate_tx_basic_db(data_dir, &mp, tx, tx_bytes.len())?;

        let mut tx_store = tx.clone();
        // Store computed fee for eviction ranking and mining fee aggregation.
        if let Some(obj) = tx_store.as_object_mut() {
            obj.insert("fee".to_string(), json!(fee));
            obj.insert("size".to_string(), json!(tx_bytes.len() as u64));
        }
        mp["txs"][txid.clone()] = tx_store;
        let mut txids = mp["txids"].as_array().cloned().unwrap_or_default();
        txids.push(json!(txid.clone()));
        mp["txids"] = serde_json::Value::Array(txids);

        // Enforce mempool caps with eviction of lowest-fee txs (deterministic).
        // If the incoming tx gets evicted, treat as mempool_full.
        let mut removed: HashSet<String> = HashSet::new();
        loop {
            let txs_obj = match mp.get("txs").and_then(|x| x.as_object()) {
                Some(o) => o,
                None => break,
            };
            let cur_count = txs_obj.len();
            let mut cur_bytes: usize = 0;
            for (_k, v) in txs_obj.iter() {
                if let Ok(b) = serde_json::to_vec(v) {
                    cur_bytes = cur_bytes.saturating_add(b.len());
                }
            }
            if cur_count <= MAX_MEMPOOL_TXS && cur_bytes <= MAX_MEMPOOL_BYTES {
                break;
            }

            // Build candidates (txid, fee, size)
            let mut candidates: Vec<(String, u64, usize)> = Vec::new();
            for (k, v) in txs_obj.iter() {
                let fee_k = v.get("fee").and_then(|x| x.as_u64()).unwrap_or(0);
                let sz0 = v.get("size").and_then(|x| x.as_u64()).unwrap_or(0) as usize;
                let sz = if sz0 == 0 {
                    serde_json::to_vec(v).map(|b| b.len()).unwrap_or(0)
                } else {
                    sz0
                };
                candidates.push((k.clone(), fee_k, sz));
            }

            candidates.sort_by(|a, b| {
                // feerate asc (fee/size), then fee asc, then txid lex asc
                match feerate_cmp(a.1, a.2, b.1, b.2) {
                    std::cmp::Ordering::Equal => match a.1.cmp(&b.1) {
                        std::cmp::Ordering::Equal => a.0.cmp(&b.0),
                        o => o,
                    },
                    o => o,
                }
            });

            if let Some((victim, _vf, _vs)) = candidates.first().cloned() {
                let _ = remove_tx_from_mempool(&mut mp, &victim);
                removed.insert(victim);
            } else {
                break;
            }
        }

        // Rebuild txids array to match remaining txs.
        let txs_obj = mp
            .get("txs")
            .and_then(|x| x.as_object())
            .cloned()
            .unwrap_or_default();
        let mut new_txids: Vec<serde_json::Value> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();
        if let Some(old) = mp.get("txids").and_then(|x| x.as_array()) {
            for v in old {
                if let Some(t) = v.as_str() {
                    if txs_obj.contains_key(t) && seen.insert(t.to_string()) {
                        new_txids.push(json!(t));
                    }
                }
            }
        }
        // Add any txs not present in txids (should not happen but keep consistent).
        for k in txs_obj.keys() {
            if seen.insert(k.clone()) {
                new_txids.push(json!(k));
            }
        }
        mp["txids"] = serde_json::Value::Array(new_txids);

        if mp["txs"].get(&txid).is_none() {
            return Err("mempool_full".to_string());
        }

        save_mempool(data_dir, &mp)?;
    }

    Ok(txid)
}

/// Called from P2P receive path. Best-effort ingest; does NOT rebroadcast.
pub fn ingest_tx_p2p(data_dir: &str, txid: &str, tx: &serde_json::Value) -> Result<(), String> {
    // P2P orphan policy (phase 1): if inputs are missing, store as orphan (bounded + TTL)
    // instead of hard-rejecting. Do not rebroadcast from here.
    match ingest_tx(data_dir, Some(txid), tx) {
        Ok(_tid) => {
            // Best-effort promote any orphans that may now be satisfiable.
            let mut mp = load_mempool(data_dir);
            if mp.get("txids").and_then(|x| x.as_array()).is_none() {
                mp["txids"] = json!([]);
            }
            if mp.get("txs").and_then(|x| x.as_object()).is_none() {
                mp["txs"] = json!({});
            }
            orphan_try_promote(data_dir, &mut mp);
            let _ = save_mempool(data_dir, &mp);
            Ok(())
        }
        Err(e) if e == "input_not_found" => {
            let tx_bytes =
                serde_json::to_vec(tx).map_err(|er| format!("json_encode_failed: {}", er))?;
            let mut mp = load_mempool(data_dir);
            if mp.get("txids").and_then(|x| x.as_array()).is_none() {
                mp["txids"] = json!([]);
            }
            if mp.get("txs").and_then(|x| x.as_object()).is_none() {
                mp["txs"] = json!({});
            }
            orphan_add(&mut mp, txid, tx, tx_bytes.len());
            save_mempool(data_dir, &mp)?;
            Ok(())
        }
        Err(e) => Err(e),
    }
}

pub fn handle_submit_tx(
    mut request: tiny_http::Request,
    data_dir: &str,
    respond_json: fn(tiny_http::Request, tiny_http::StatusCode, String),
) {
    if request.method() != &tiny_http::Method::Post {
        crate::respond_error(request, tiny_http::StatusCode(405), "method_not_allowed");
        return;
    }
    if !crate::request_content_type_is_json(&request) {
        crate::respond_415(request);
        return;
    }

    let body_bytes = match crate::read_body_limited(&mut request) {
        Ok(b) => b,
        Err(e) if e == "payload_too_large" => {
            crate::respond_error_detail(
                request,
                tiny_http::StatusCode(413),
                "payload_too_large",
                json!({"max_body_bytes":crate::MAX_RPC_BODY_BYTES}),
            );
            return;
        }
        Err(e) => {
            crate::respond_error_detail(
                request,
                tiny_http::StatusCode(400),
                "bad_request",
                json!({"detail":e}),
            );
            return;
        }
    };
    let v: serde_json::Value = match serde_json::from_slice(&body_bytes) {
        Ok(v) => v,
        Err(_) => {
            wlog!("[dutad] SUBMIT_TX_REJECT reason=invalid_json");
            crate::respond_error_detail(
                request,
                tiny_http::StatusCode(400),
                "bad_request",
                json!({"detail":"invalid_json"}),
            );
            return;
        }
    };

    let (tx, txid_field) = match parse_submit_tx_request(&v) {
        Ok(parsed) => parsed,
        Err(code) => {
            wlog!("[dutad] SUBMIT_TX_REJECT reason={}", code);
            crate::respond_error_detail(
                request,
                tiny_http::StatusCode(400),
                "bad_request",
                json!({"detail":code}),
            );
            return;
        }
    };

    let tx_bytes_len = serde_json::to_vec(&tx).map(|b| b.len()).unwrap_or(0);
    let min_fee = required_relay_fee(tx_bytes_len);

    let txid = match ingest_tx(data_dir, txid_field.as_deref(), &tx) {
        Ok(t) => t,
        Err(e) => {
            wlog!(
                "[dutad] SUBMIT_TX_REJECT txid={} reason={} size={}",
                txid_field.as_deref().map(short_txid).unwrap_or("-"),
                e,
                tx_bytes_len
            );
            if e == "fee_too_low" {
                respond_json(
                    request,
                    tiny_http::StatusCode(422),
                    json!({"ok":false,"error":"fee_too_low","min_fee":min_fee,"size":tx_bytes_len})
                        .to_string(),
                );
                return;
            }
            let (status, mut body) = submit_tx_status_and_detail(&e);
            if let Some(obj) = body.as_object_mut() {
                obj.insert("ok".to_string(), json!(false));
                obj.insert("size".to_string(), json!(tx_bytes_len));
            }
            crate::respond_json(request, status, body.to_string());
            return;
        }
    };

    // Best-effort broadcast to peers.
    crate::p2p::broadcast_tx(&txid, &tx);
    wlog!(
        "[dutad] SUBMIT_TX_OK txid={} size={}",
        short_txid(&txid),
        tx_bytes_len
    );

    respond_json(
        request,
        tiny_http::StatusCode(200),
        json!({"ok":true,"txid":txid}).to_string(),
    );
}

#[cfg(test)]
mod tests {
    use super::{parse_submit_tx_request, txid_is_valid};
    use serde_json::json;

    #[test]
    fn txid_validation_requires_fixed_hex_shape() {
        assert!(txid_is_valid(
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        ));
        assert!(!txid_is_valid("abcd"));
        assert!(!txid_is_valid(
            "gggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggg"
        ));
    }

    #[test]
    fn submit_tx_payload_requires_tx_object_and_valid_optional_txid() {
        assert_eq!(parse_submit_tx_request(&json!({})), Err("missing_tx"));
        assert_eq!(
            parse_submit_tx_request(&json!({"tx":"bad"})),
            Err("invalid_tx")
        );
        assert_eq!(
            parse_submit_tx_request(&json!({"tx":{},"txid":"abcd"})),
            Err("invalid_txid")
        );

        let parsed = parse_submit_tx_request(&json!({
            "tx": {"vin":[{"txid":"a","vout":0}],"vout":[{"address":"dut1111111111111111111111111111111111111111","value":1}]},
            "txid":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        }))
        .expect("payload should parse");
        assert!(parsed.1.is_some());
    }
}
