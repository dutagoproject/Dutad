use serde_json::json;
use std::collections::HashSet;
use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::canon_json;
use crate::store;
use duta_core::address;
use duta_core::amount::{DEFAULT_MIN_OUTPUT_VALUE_DUT, DEFAULT_MIN_RELAY_FEE_PER_KB_DUT};
use duta_core::hash;
use ed25519_dalek::{Signature, VerifyingKey};

const COINBASE_MATURITY: u64 = 60;

// Production-hardening caps (phase 1)
const MAX_TX_BYTES: usize = 100_000;
const MAX_MEMPOOL_TXS: usize = 10_000;
const MAX_MEMPOOL_BYTES: usize = 5_000_000; // 5MB
const MIN_RELAY_FEE_PER_KB: u64 = DEFAULT_MIN_RELAY_FEE_PER_KB_DUT;

// Gate C (phase 1): anti-UTXO-bloat policy.
// Reject outputs below this value and cap outputs per tx.
const MIN_OUTPUT_VALUE: u64 = DEFAULT_MIN_OUTPUT_VALUE_DUT;

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
    store::txid_from_value(v)
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

fn enforce_mempool_caps(mp: &mut serde_json::Value) -> HashSet<String> {
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

        candidates.sort_by(|a, b| match feerate_cmp(a.1, a.2, b.1, b.2) {
            std::cmp::Ordering::Equal => match a.1.cmp(&b.1) {
                std::cmp::Ordering::Equal => a.0.cmp(&b.0),
                o => o,
            },
            o => o,
        });

        if let Some((victim, _vf, _vs)) = candidates.first().cloned() {
            let _ = remove_tx_from_mempool(mp, &victim);
            removed.insert(victim);
        } else {
            break;
        }
    }
    removed
}

fn rebuild_mempool_txids(mp: &mut serde_json::Value) {
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
    for k in txs_obj.keys() {
        if seen.insert(k.clone()) {
            new_txids.push(json!(k));
        }
    }
    mp["txids"] = serde_json::Value::Array(new_txids);
}

pub fn prune_confirmed_txids_from_mempool_file(data_dir: &str, txids: &[String]) {
    if txids.is_empty() {
        return;
    }
    let mut mp = load_mempool(data_dir);
    let mut changed = false;
    for txid in txids {
        if remove_tx_from_mempool(&mut mp, txid) {
            changed = true;
        }
        if mp["orphans"]["txs"]
            .as_object_mut()
            .map(|o| o.remove(txid).is_some())
            .unwrap_or(false)
        {
            changed = true;
        }
        if mp["orphans"]["meta"]
            .as_object_mut()
            .map(|o| o.remove(txid).is_some())
            .unwrap_or(false)
        {
            changed = true;
        }
    }
    if changed {
        if let Some(old) = mp["orphans"]["txids"].as_array() {
            let keep: Vec<serde_json::Value> = old
                .iter()
                .filter_map(|v| {
                    let txid = v.as_str()?;
                    if txids.iter().any(|confirmed| confirmed == txid) {
                        None
                    } else {
                        Some(json!(txid))
                    }
                })
                .collect();
            mp["orphans"]["txids"] = serde_json::Value::Array(keep);
        }
        rebuild_mempool_txids(&mut mp);
        let _ = save_mempool(data_dir, &mp);
    }
}

fn txid_is_confirmed_in_chain(data_dir: &str, txid: &str) -> bool {
    let tip_h = crate::store::tip_fields(data_dir)
        .map(|(height, _, _, _)| height)
        .unwrap_or(0);
    for h in (0..=tip_h).rev() {
        let Some(block) = crate::store::block_at(data_dir, h) else {
            continue;
        };
        let Some(txs) = block.txs.as_ref().and_then(|v| v.as_object()) else {
            continue;
        };
        if txs.contains_key(txid) {
            return true;
        }
    }
    false
}

fn reconcile_confirmed_txs_against_chain(data_dir: &str, mp: &mut serde_json::Value) -> bool {
    let Some(txs_obj) = mp.get("txs").and_then(|x| x.as_object()) else {
        return false;
    };
    let confirmed: Vec<String> = txs_obj
        .keys()
        .filter(|txid| txid_is_confirmed_in_chain(data_dir, txid))
        .cloned()
        .collect();
    if confirmed.is_empty() {
        return false;
    }
    for txid in &confirmed {
        let _ = remove_tx_from_mempool(mp, txid);
        if mp["orphans"]["txs"]
            .as_object_mut()
            .map(|o| o.remove(txid).is_some())
            .unwrap_or(false)
        {}
        if mp["orphans"]["meta"]
            .as_object_mut()
            .map(|o| o.remove(txid).is_some())
            .unwrap_or(false)
        {}
    }
    if let Some(old) = mp["orphans"]["txids"].as_array() {
        let keep: Vec<serde_json::Value> = old
            .iter()
            .filter_map(|v| {
                let txid = v.as_str()?;
                if confirmed.iter().any(|done| done == txid) {
                    None
                } else {
                    Some(json!(txid))
                }
            })
            .collect();
        mp["orphans"]["txids"] = serde_json::Value::Array(keep);
    }
    rebuild_mempool_txids(mp);
    true
}

pub fn reconcile_confirmed_mempool_file(data_dir: &str) -> bool {
    let path = mempool_path(data_dir);
    let s = match fs::read_to_string(&path) {
        Ok(s) => s,
        Err(_) => return false,
    };
    let mut mp = match serde_json::from_str::<serde_json::Value>(&s) {
        Ok(v) => sanitize_mempool_value(&v).unwrap_or(v),
        Err(_) => return false,
    };
    ensure_orphan_shape(&mut mp);
    let changed = reconcile_confirmed_txs_against_chain(data_dir, &mut mp);
    if changed {
        let _ = save_mempool(data_dir, &mp);
    }
    changed
}

fn orphan_try_promote(data_dir: &str, mp: &mut serde_json::Value) -> Vec<String> {
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

        let removed = enforce_mempool_caps(mp);
        if !removed.is_empty() {
            promoted.retain(|txid| !removed.contains(txid));
        }
        rebuild_mempool_txids(mp);
    }
    promoted
}

pub fn orphan_try_promote_file(data_dir: &str) {
    // Best-effort: load mempool.json, prune/expire orphans, try promote, save.
    let mut mp = load_mempool(data_dir);
    ensure_orphan_shape(&mut mp);
    let now = now_secs();
    orphan_prune(&mut mp, now);
    let _ = orphan_try_promote(data_dir, &mut mp);
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
        Ok(v) => sanitize_mempool_value(&v).unwrap_or(v),
        Err(_) => json!({"txids": [], "txs": {}}),
    }
}

pub fn load_orphan_pool(data_dir: &str) -> serde_json::Value {
    let mut mp = load_mempool(data_dir);
    ensure_orphan_shape(&mut mp);
    mp.get("orphans").cloned().unwrap_or_else(|| json!({
        "txids": [],
        "txs": {},
        "meta": {}
    }))
}

pub fn find_orphan_tx(
    data_dir: &str,
    txid: &str,
) -> Option<(serde_json::Value, Option<serde_json::Value>)> {
    let orphans = load_orphan_pool(data_dir);
    let tx = orphans.get("txs").and_then(|x| x.get(txid)).cloned()?;
    let meta = orphans.get("meta").and_then(|x| x.get(txid)).cloned();
    Some((tx, meta))
}

fn sanitize_mempool_value(v: &serde_json::Value) -> Option<serde_json::Value> {
    let mut mp = v.clone();
    let txs_obj = mp.get("txs").and_then(|x| x.as_object())?.clone();
    let mut changed = false;
    let mut new_txs = serde_json::Map::new();
    let mut ordered: Vec<String> = Vec::new();
    let original_ids: Vec<String> = mp
        .get("txids")
        .and_then(|x| x.as_array())
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();

    if let Some(old_ids) = mp.get("txids").and_then(|x| x.as_array()) {
        for item in old_ids {
            let Some(old_key) = item.as_str() else {
                changed = true;
                continue;
            };
            let Some(txv) = txs_obj.get(old_key) else {
                changed = true;
                continue;
            };
            let mut tx_for_id = txv.clone();
            if let Some(obj) = tx_for_id.as_object_mut() {
                obj.remove("size");
            }
            let new_key = txid_from_value(&tx_for_id).unwrap_or_else(|_| old_key.to_string());
            if new_key != old_key {
                changed = true;
            }
            if !new_txs.contains_key(&new_key) {
                new_txs.insert(new_key.clone(), txv.clone());
                ordered.push(new_key);
            }
        }
    }

    for (old_key, txv) in txs_obj.iter() {
        let mut tx_for_id = txv.clone();
        if let Some(obj) = tx_for_id.as_object_mut() {
            obj.remove("size");
        }
        let new_key = txid_from_value(&tx_for_id).unwrap_or_else(|_| old_key.clone());
        if new_key != *old_key {
            changed = true;
        }
        if !new_txs.contains_key(&new_key) {
            new_txs.insert(new_key.clone(), txv.clone());
            ordered.push(new_key);
        }
    }

    if original_ids.len() != ordered.len()
        || original_ids
            .iter()
            .zip(ordered.iter())
            .any(|(left, right)| left != right)
    {
        changed = true;
    }

    if changed {
        mp["txs"] = serde_json::Value::Object(new_txs);
        mp["txids"] =
            serde_json::Value::Array(ordered.into_iter().map(serde_json::Value::String).collect());
        Some(mp)
    } else {
        None
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
    let canonical_txid = txid_from_value(tx).map_err(|_| "invalid_tx")?;
    let txid = v
        .get("txid")
        .and_then(|x| x.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty());
    if let Some(txid) = txid {
        if !txid_is_valid(txid) {
            return Err("invalid_txid");
        }
        if !txid.eq_ignore_ascii_case(&canonical_txid) {
            return Err("txid_mismatch");
        }
        return Ok((tx.clone(), Some(canonical_txid)));
    }
    Ok((tx.clone(), Some(canonical_txid)))
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
        | "txid_mismatch"
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
    let canonical_txid = txid_from_value(tx).unwrap_or_else(|_| hash::sha3_256_hex(&tx_bytes));
    let txid = txid_opt
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .unwrap_or_else(|| canonical_txid.clone());

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
        enforce_mempool_caps(&mut mp);
        rebuild_mempool_txids(&mut mp);

        if mp["txs"].get(&txid).is_none() {
            return Err("mempool_full".to_string());
        }

        save_mempool(data_dir, &mp)?;
    }

    Ok(txid)
}

#[derive(Clone, Debug, Default)]
pub struct P2pIngestOutcome {
    pub accepted_new: bool,
    pub promoted: Vec<(String, serde_json::Value)>,
}

/// Called from P2P receive path. Best-effort ingest; caller decides whether to rebroadcast.
pub fn ingest_tx_p2p(
    data_dir: &str,
    txid: &str,
    tx: &serde_json::Value,
) -> Result<P2pIngestOutcome, String> {
    let before = load_mempool(data_dir);
    let already_in_mempool = before
        .get("txs")
        .and_then(|x| x.as_object())
        .map(|txs| txs.contains_key(txid))
        .unwrap_or(false);
    // P2P orphan policy (phase 1): if inputs are missing, store as orphan (bounded + TTL)
    // instead of hard-rejecting.
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
            let promoted_ids = orphan_try_promote(data_dir, &mut mp);
            let promoted = promoted_ids
                .into_iter()
                .filter_map(|promoted_txid| {
                    mp.get("txs")
                        .and_then(|x| x.get(&promoted_txid))
                        .cloned()
                        .map(|txv| (promoted_txid, txv))
                })
                .collect();
            let _ = save_mempool(data_dir, &mp);
            Ok(P2pIngestOutcome {
                accepted_new: !already_in_mempool,
                promoted,
            })
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
            Ok(P2pIngestOutcome::default())
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
    use super::{
        enforce_mempool_caps, ingest_tx_p2p, load_mempool, parse_submit_tx_request,
        prune_confirmed_txids_from_mempool_file, rebuild_mempool_txids,
        reconcile_confirmed_mempool_file, sanitize_mempool_value, save_mempool,
        txid_from_value,
        txid_is_valid,
    };
    use duta_core::address;
    use ed25519_dalek::{Signer, SigningKey};
    use serde_json::json;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_datadir(tag: &str) -> String {
        let mut p = std::env::temp_dir();
        let uniq = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        p.push(format!("duta-submit-tx-test-{}-{}", tag, uniq));
        std::fs::create_dir_all(&p).unwrap();
        p.to_string_lossy().to_string()
    }

    fn put_utxo_with_pkh(
        data_dir: &str,
        txid: &str,
        vout: u64,
        value: u64,
        created_height: u64,
        pkh_hex: &str,
    ) {
        let db_path = format!("{}/db", data_dir.trim_end_matches('/'));
        let db = sled::open(&db_path).unwrap();
        let utxo = db.open_tree(b"utxo").unwrap();
        let k = format!("{}:{}", txid, vout).into_bytes();
        let v = json!({
            "value": value,
            "created_height": created_height,
            "is_coinbase": false,
            "pkh": pkh_hex
        });
        utxo.insert(k, serde_json::to_vec(&v).unwrap()).unwrap();
        utxo.flush().unwrap();
        db.flush().unwrap();
    }

    fn signed_tx_for_network(
        prev_txid: &str,
        prev_vout: u64,
        input_value: u64,
        net: duta_core::netparams::Network,
    ) -> serde_json::Value {
        let sk = SigningKey::from_bytes(&[7u8; 32]);
        let pub_hex = hex::encode(sk.verifying_key().to_bytes());
        let pkh = address::pkh_from_pubkey(&sk.verifying_key().to_bytes());
        let dest = address::pkh_to_address_for_network(net, &pkh);
        let mut tx = json!({
            "vin":[{"txid":prev_txid,"vout":prev_vout,"pubkey":pub_hex,"sig":""}],
            "vout":[{"address":dest,"value":input_value - super::required_relay_fee(256)}]
        });
        let msg = super::sighash(&tx).expect("sighash");
        let sig = sk.sign(&msg);
        tx["vin"][0]["sig"] = json!(hex::encode(sig.to_bytes()));
        tx
    }

    fn signed_tx_for_test(prev_txid: &str, prev_vout: u64, input_value: u64) -> serde_json::Value {
        signed_tx_for_network(
            prev_txid,
            prev_vout,
            input_value,
            duta_core::netparams::Network::Mainnet,
        )
    }

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

        let tx = json!({
            "vin":[{"txid":"a","vout":0}],
            "vout":[{"address":"dut1111111111111111111111111111111111111111","value":1}]
        });
        let canonical = super::txid_from_value(&tx).expect("canonical txid");

        let parsed = parse_submit_tx_request(&json!({
            "tx": tx,
            "txid": canonical.to_uppercase()
        }))
        .expect("payload should parse");
        assert!(parsed.1.is_some());
        assert_eq!(parsed.1.as_deref(), Some(canonical.as_str()));

        assert_eq!(
            parse_submit_tx_request(&json!({
                "tx": {
                    "vin":[{"txid":"a","vout":0}],
                    "vout":[{"address":"dut1111111111111111111111111111111111111111","value":1}]
                },
                "txid":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
            })),
            Err("txid_mismatch")
        );
    }

    #[test]
    fn mempool_cap_enforcement_rebuilds_txids_after_eviction() {
        let mut txids = Vec::new();
        let mut txs = serde_json::Map::new();
        for idx in 0..=super::MAX_MEMPOOL_TXS {
            let txid = format!("{idx:064x}");
            txids.push(json!(txid.clone()));
            txs.insert(
                txid.clone(),
                json!({
                    "vin":[{"txid":"a","vout":idx as u64}],
                    "vout":[{"address":"dut1111111111111111111111111111111111111111","value":1}],
                    "fee": idx as u64,
                    "size": 1
                }),
            );
        }
        let mut mp = json!({
            "txids": txids,
            "txs": txs
        });

        let removed = enforce_mempool_caps(&mut mp);
        assert_eq!(removed.len(), 1);

        rebuild_mempool_txids(&mut mp);
        let txids = mp["txids"].as_array().cloned().unwrap_or_default();
        assert_eq!(txids.len(), super::MAX_MEMPOOL_TXS);
        assert_eq!(
            mp["txs"].as_object().map(|o| o.len()),
            Some(super::MAX_MEMPOOL_TXS)
        );
        for txid in txids {
            let key = txid.as_str().expect("txid string");
            assert!(mp["txs"].get(key).is_some());
        }
    }

    #[test]
    fn sanitize_mempool_value_ignores_fee_and_size_for_txid_identity() {
        let tx = json!({
            "vin":[{"txid":"a","vout":0}],
            "vout":[{"address":"dut1111111111111111111111111111111111111111","value":1}],
            "fee": 10000,
            "size": 321
        });
        let mut tx_for_id = tx.clone();
        tx_for_id.as_object_mut().expect("tx object").remove("size");
        let canonical = txid_from_value(&tx_for_id).expect("canonical txid");
        let mp = json!({
            "txids": [canonical.clone()],
            "txs": {
                canonical.clone(): tx
            }
        });

        assert!(sanitize_mempool_value(&mp).is_none());
    }

    #[test]
    fn txid_stays_identical_across_submit_mempool_template_and_accepted_block() {
        let data_dir = temp_datadir("txid-e2e");
        crate::store::ensure_datadir_meta(&data_dir, "testnet").unwrap();

        let sk = SigningKey::from_bytes(&[7u8; 32]);
        let input_pkh = address::pkh_from_pubkey(&sk.verifying_key().to_bytes());
        let input_addr =
            address::pkh_to_address_for_network(duta_core::netparams::Network::Testnet, &input_pkh);
        let input_value = 50_000u64;
        let confirmed_tx = json!({
            "vin":[{"txid":"ff".repeat(32),"vout":0}],
            "vout":[{"address":input_addr,"value":input_value}]
        });
        let prev_txid = txid_from_value(&confirmed_tx).unwrap();
        let merkle = crate::store::merkle32_from_txids(std::slice::from_ref(&prev_txid)).unwrap();
        let chain = vec![json!({
            "height": 1,
            "hash32": "11".repeat(32),
            "bits": duta_core::netparams::pow_start_bits(duta_core::Network::Testnet),
            "chainwork": 1,
            "timestamp": 1_700_000_000u64,
            "prevhash32": duta_core::netparams::genesis_hash(duta_core::Network::Testnet),
            "merkle32": merkle,
            "nonce": 1u64,
            "miner": "test1miner",
            "txs": {
                prev_txid.clone(): confirmed_tx
            }
        })];
        std::fs::write(
            format!("{}/chain.json", data_dir),
            serde_json::to_string(&chain).unwrap(),
        )
        .unwrap();
        crate::store::bootstrap(&data_dir).unwrap();

        let tx = signed_tx_for_network(
            &prev_txid,
            0,
            input_value,
            duta_core::netparams::Network::Testnet,
        );
        let submit_txid = super::ingest_tx(&data_dir, None, &tx).expect("ingest tx");

        let mp = load_mempool(&data_dir);
        let stored_txid = mp["txids"][0].as_str().expect("stored txid");
        assert_eq!(stored_txid, submit_txid);

        let miner_pkh = address::pkh_from_pubkey(&SigningKey::from_bytes(&[9u8; 32]).verifying_key().to_bytes());
        let miner_addr =
            address::pkh_to_address_for_network(duta_core::netparams::Network::Testnet, &miner_pkh);
        let work_scope = format!("{}#work", miner_addr);
        let tpl =
            crate::work::build_work_template(&data_dir, &miner_addr, false, &work_scope).unwrap();
        let template_txids: Vec<String> = tpl["transactions"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect();
        assert!(template_txids.iter().any(|txid| txid == &submit_txid));

        let template_txs = tpl["txs"].as_object().expect("template tx object");
        assert!(template_txs.contains_key(&submit_txid));

        let accepted_dir = temp_datadir("txid-e2e-accepted");
        crate::store::ensure_datadir_meta(&accepted_dir, "testnet").unwrap();
        let accepted_merkle =
            crate::store::merkle32_from_txids(std::slice::from_ref(&submit_txid)).unwrap();
        let accepted_chain = vec![json!({
            "height": 1,
            "hash32": "22".repeat(32),
            "bits": duta_core::netparams::pow_start_bits(duta_core::Network::Testnet),
            "chainwork": 1,
            "timestamp": 1_700_000_100u64,
            "prevhash32": duta_core::netparams::genesis_hash(duta_core::Network::Testnet),
            "merkle32": accepted_merkle,
            "nonce": 1u64,
            "miner": miner_addr,
            "txs": {
                submit_txid.clone(): template_txs.get(&submit_txid).cloned().unwrap()
            }
        })];
        std::fs::write(
            format!("{}/chain.json", accepted_dir),
            serde_json::to_string(&accepted_chain).unwrap(),
        )
        .unwrap();
        crate::store::bootstrap(&accepted_dir).unwrap();
        let accepted = crate::store::block_at(&accepted_dir, 1).expect("accepted block");
        let accepted_txs = accepted
            .txs
            .as_ref()
            .and_then(|v| v.as_object())
            .expect("accepted tx object");
        assert!(accepted_txs.contains_key(&submit_txid));
    }

    #[test]
    fn sanitize_mempool_value_repairs_txid_index_when_array_is_stale() {
        let tx = json!({
            "vin":[{"txid":"a","vout":0}],
            "vout":[{"address":"dut1111111111111111111111111111111111111111","value":1}],
            "fee": 10000,
            "size": 321
        });
        let mut tx_for_id = tx.clone();
        tx_for_id.as_object_mut().expect("tx object").remove("size");
        let canonical = txid_from_value(&tx_for_id).expect("canonical txid");
        let stale = "ff".repeat(32);
        let mp = json!({
            "txids": [stale],
            "txs": {
                canonical.clone(): tx
            }
        });

        let repaired = sanitize_mempool_value(&mp).expect("stale txids should be repaired");
        assert_eq!(repaired["txids"], json!([canonical.clone()]));
        assert!(repaired["txs"].get(&canonical).is_some());
    }

    #[test]
    fn prune_confirmed_txids_from_mempool_file_removes_confirmed_from_pool_and_orphans() {
        let data_dir = temp_datadir("prune-confirmed-mempool");
        let tx = json!({
            "vin":[{"txid":"a","vout":0}],
            "vout":[{"address":"dut1111111111111111111111111111111111111111","value":1}],
            "fee": 1000,
            "size": 123
        });
        let mut tx_for_id = tx.clone();
        tx_for_id.as_object_mut().expect("tx object").remove("size");
        let txid = txid_from_value(&tx_for_id).expect("canonical txid");
        let orphan_txid = "bb".repeat(32);
        let mp = json!({
            "txids": [txid.clone()],
            "txs": {
                txid.clone(): tx
            },
            "orphans": {
                "txids": [txid.clone(), orphan_txid.clone()],
                "txs": {
                    txid.clone(): {"fee": 1000},
                    orphan_txid.clone(): {"fee": 2000}
                },
                "meta": {
                    txid.clone(): {"ts": 1, "bytes": 123},
                    orphan_txid.clone(): {"ts": 2, "bytes": 234}
                }
            }
        });
        save_mempool(&data_dir, &mp).expect("save mempool");

        prune_confirmed_txids_from_mempool_file(&data_dir, std::slice::from_ref(&txid));

        let updated = load_mempool(&data_dir);
        assert_eq!(updated["txids"], json!([]));
        assert!(updated["txs"].get(&txid).is_none());
        assert_eq!(updated["orphans"]["txids"], json!([orphan_txid]));
        assert!(updated["orphans"]["txs"].get(&txid).is_none());
        assert!(updated["orphans"]["meta"].get(&txid).is_none());
    }

    #[test]
    fn reconcile_confirmed_mempool_file_prunes_confirmed_txids_from_stale_file() {
        let data_dir = temp_datadir("load-mempool-confirmed-reconcile");
        crate::store::ensure_datadir_meta(&data_dir, "mainnet").unwrap();

        let tx = json!({
            "vin":[{"txid":"a","vout":0}],
            "vout":[{"address":"dut1111111111111111111111111111111111111111","value":1}],
            "fee": 1000,
            "size": 123
        });
        let mut tx_for_id = tx.clone();
        tx_for_id.as_object_mut().expect("tx object").remove("size");
        let txid = txid_from_value(&tx_for_id).expect("canonical txid");
        let merkle = crate::store::merkle32_from_txids(std::slice::from_ref(&txid)).unwrap();

        let chain = vec![json!({
            "height": 1,
            "hash32": "11".repeat(32),
            "bits": duta_core::netparams::pow_start_bits(duta_core::Network::Mainnet),
            "chainwork": 1,
            "timestamp": 1_700_000_000u64,
            "prevhash32": duta_core::netparams::genesis_hash(duta_core::Network::Mainnet),
            "merkle32": merkle,
            "nonce": 1u64,
            "miner": "dut1miner",
            "txs": {
                txid.clone(): tx_for_id.clone()
            }
        })];
        std::fs::write(
            format!("{}/chain.json", data_dir),
            serde_json::to_string(&chain).unwrap(),
        )
        .unwrap();
        crate::store::bootstrap(&data_dir).unwrap();

        let mp = json!({
            "txids": [txid.clone()],
            "txs": {
                txid.clone(): tx
            }
        });
        save_mempool(&data_dir, &mp).expect("save stale mempool");

        assert!(reconcile_confirmed_mempool_file(&data_dir));
        let updated = load_mempool(&data_dir);
        assert_eq!(updated["txids"], json!([]));
        assert!(updated["txs"].get(&txid).is_none());
    }

    #[test]
    fn ingest_tx_p2p_marks_new_accepts_but_not_duplicates() {
        let data_dir = temp_datadir("p2p-ingest");
        crate::store::ensure_datadir_meta(&data_dir, "mainnet").unwrap();

        let sk = SigningKey::from_bytes(&[7u8; 32]);
        let pkh = address::pkh_to_hex(&address::pkh_from_pubkey(&sk.verifying_key().to_bytes()));
        put_utxo_with_pkh(&data_dir, &"aa".repeat(32), 0, 50_000, 100, &pkh);
        let tx = signed_tx_for_test(&"aa".repeat(32), 0, 50_000);
        let txid = super::txid_from_value(&tx).expect("txid");

        let first = ingest_tx_p2p(&data_dir, &txid, &tx).expect("first ingest");
        assert!(first.accepted_new);
        assert!(first.promoted.is_empty());

        let stored_txid = super::load_mempool(&data_dir)["txids"][0]
            .as_str()
            .expect("stored txid")
            .to_string();
        let second = ingest_tx_p2p(&data_dir, &stored_txid, &tx).expect("second ingest");
        assert!(!second.accepted_new);
        assert!(second.promoted.is_empty());
    }
}
