use crate::amount_display::format_dut_fixed_8;
use duta_core::amount::{BASE_UNIT, DEFAULT_MIN_RELAY_FEE_PER_KB_DUT, DISPLAY_UNIT, DUTA_DECIMALS};
use duta_core::netparams::{genesis_hash, pow_start_bits, Network};
use duta_core::types::H32;
use serde_json::json;
use std::fs;
use std::sync::OnceLock;
use std::time::Instant;

static PROCESS_START: OnceLock<Instant> = OnceLock::new();

pub fn mark_process_start() {
    let _ = PROCESS_START.set(Instant::now());
}

fn uptime_secs() -> u64 {
    PROCESS_START
        .get()
        .map(|t| t.elapsed().as_secs())
        .unwrap_or(0)
}

fn ok(id: serde_json::Value, result: serde_json::Value) -> String {
    json!({"result": result, "error": null, "id": id}).to_string()
}

fn err(id: serde_json::Value, code: i64, message: &str) -> String {
    json!({
        "result": null,
        "error": {"code": code, "message": message},
        "id": id
    })
    .to_string()
}

fn invalid_params(id: serde_json::Value, message: &str) -> String {
    err(id, -32602, message)
}

fn ensure_params_len(
    params: &[serde_json::Value],
    min_len: usize,
    max_len: usize,
) -> Result<(), String> {
    if params.len() < min_len || params.len() > max_len {
        return Err("invalid_params".to_string());
    }
    Ok(())
}

fn as_u64(v: &serde_json::Value) -> Option<u64> {
    v.as_u64().or_else(|| {
        v.as_i64()
            .and_then(|x| if x >= 0 { Some(x as u64) } else { None })
    })
}

fn as_bool(v: &serde_json::Value) -> Option<bool> {
    v.as_bool()
        .or_else(|| v.as_u64().map(|x| x != 0))
        .or_else(|| v.as_i64().map(|x| x != 0))
}

fn as_str(v: &serde_json::Value) -> Option<&str> {
    v.as_str().filter(|s| !s.is_empty())
}

fn mempool_value(data_dir: &str) -> serde_json::Value {
    let path = format!("{}/mempool.json", data_dir.trim_end_matches('/'));
    let s = match fs::read_to_string(&path) {
        Ok(s) => s,
        Err(_) => return json!({"txids": [], "txs": {}}),
    };
    serde_json::from_str::<serde_json::Value>(&s)
        .unwrap_or_else(|_| json!({"txids": [], "txs": {}}))
}

fn network_from_data_dir(data_dir: &str) -> Network {
    if let Some(net) = crate::store::read_datadir_network(data_dir) {
        net
    } else if data_dir.contains("testnet") {
        Network::Testnet
    } else if data_dir.contains("stagenet") {
        Network::Stagenet
    } else {
        Network::Mainnet
    }
}

fn canonical_genesis_block(data_dir: &str) -> crate::ChainBlock {
    let net = network_from_data_dir(data_dir);
    crate::ChainBlock {
        height: 0,
        hash32: genesis_hash(net).to_string(),
        bits: pow_start_bits(net),
        chainwork: 0,
        timestamp: None,
        prevhash32: None,
        merkle32: None,
        nonce: None,
        miner: None,
        pow_digest32: None,
        txs: None,
    }
}

fn block_hash_by_height_or_genesis(data_dir: &str, height: u64) -> Option<String> {
    if height == 0 {
        return Some(canonical_genesis_block(data_dir).hash32);
    }
    crate::store::block_at(data_dir, height).map(|b| b.hash32)
}

fn block_by_height_or_genesis(data_dir: &str, height: u64) -> Option<crate::ChainBlock> {
    if height == 0 {
        return Some(canonical_genesis_block(data_dir));
    }
    crate::store::block_at(data_dir, height)
}

fn header_hex_from_block(b: &crate::ChainBlock) -> Result<String, String> {
    let prev = b
        .prevhash32
        .as_deref()
        .ok_or_else(|| "prevhash_missing".to_string())?;
    let merkle = b
        .merkle32
        .as_deref()
        .ok_or_else(|| "merkle_missing".to_string())?;
    let ts = b.timestamp.ok_or_else(|| "timestamp_missing".to_string())?;
    let nonce = b.nonce.unwrap_or(0);
    let prev_h = H32::from_hex(prev).ok_or_else(|| "prevhash_invalid".to_string())?;
    let merkle_h = H32::from_hex(merkle).ok_or_else(|| "merkle_invalid".to_string())?;

    let mut out = [0u8; 88];
    out[0..32].copy_from_slice(prev_h.as_bytes());
    out[32..64].copy_from_slice(merkle_h.as_bytes());
    out[64..72].copy_from_slice(&ts.to_le_bytes());
    out[72..80].copy_from_slice(&b.bits.to_le_bytes());
    out[80..88].copy_from_slice(&nonce.to_le_bytes());
    Ok(hex::encode(out))
}

fn block_hex_from_block(b: &crate::ChainBlock) -> Result<String, String> {
    let raw = serde_json::to_vec(b).map_err(|e| format!("block_encode_failed: {}", e))?;
    Ok(hex::encode(raw))
}

fn block_txids(b: &crate::ChainBlock) -> Vec<String> {
    b.txs
        .as_ref()
        .and_then(|v| v.as_object())
        .map(|o| o.keys().cloned().collect())
        .unwrap_or_default()
}

fn block_json_from_chainblock(
    data_dir: &str,
    b: &crate::ChainBlock,
    verbosity: u64,
) -> serde_json::Value {
    let tip_h = crate::store::tip_fields(data_dir)
        .map(|(tip_h, _, _, _)| tip_h)
        .unwrap_or(b.height);
    let confirmations = if tip_h >= b.height {
        tip_h - b.height + 1
    } else {
        0
    };
    let next_hash =
        crate::store::block_at(data_dir, b.height.saturating_add(1)).map(|nb| nb.hash32);
    let txids = block_txids(b);
    let size = serde_json::to_vec(b).map(|v| v.len() as u64).unwrap_or(0);
    let tx_value = if verbosity >= 2 {
        let mut out = Vec::new();
        if let Some(obj) = b.txs.as_ref().and_then(|v| v.as_object()) {
            for (txid, txv) in obj {
                let mut txj = txv.clone();
                if let Some(m) = txj.as_object_mut() {
                    m.insert("txid".to_string(), json!(txid));
                    if let Ok(hexs) = tx_hex_from_json(txv) {
                        m.insert("hex".to_string(), json!(hexs));
                    }
                }
                out.push(txj);
            }
        }
        json!(out)
    } else {
        json!(txids)
    };
    json!({
        "hash": b.hash32,
        "confirmations": confirmations,
        "size": size,
        "strippedsize": size,
        "weight": size,
        "height": b.height,
        "version": 1,
        "versionHex": "00000001",
        "merkleroot": b.merkle32,
        "tx": tx_value,
        "time": b.timestamp.unwrap_or(0),
        "mediantime": b.timestamp.unwrap_or(0),
        "nonce": b.nonce.unwrap_or(0),
        "bits": format!("{:016x}", b.bits),
        "difficulty_bits": b.bits,
        "difficulty": bits_to_difficulty(b.bits),
        "chainwork": format!("{:016x}", b.chainwork),
        "previousblockhash": b.prevhash32,
        "nextblockhash": next_hash,
        "miner": b.miner,
        "pow_digest32": b.pow_digest32
    })
}

fn bits_to_difficulty(bits: u64) -> f64 {
    if bits == 0 {
        return 0.0;
    }
    2f64.powi(bits.saturating_sub(8) as i32)
}

fn net_from_datadir(data_dir: &str) -> duta_core::netparams::Network {
    if let Some(net) = crate::store::read_datadir_network(data_dir) {
        net
    } else if data_dir.contains("testnet") {
        duta_core::netparams::Network::Testnet
    } else if data_dir.contains("stagenet") {
        duta_core::netparams::Network::Stagenet
    } else {
        duta_core::netparams::Network::Mainnet
    }
}

fn target_bytes_from_leading_zero_bits(bits: u64) -> [u8; 32] {
    let mut out = [0xffu8; 32];
    if bits == 0 {
        return out;
    }
    if bits >= 256 {
        return [0u8; 32];
    }
    let full_zero_bytes = (bits / 8) as usize;
    let partial_bits = (bits % 8) as u8;
    for i in 0..full_zero_bytes.min(32) {
        out[i] = 0;
    }
    if full_zero_bytes < 32 && partial_bits > 0 {
        out[full_zero_bytes] = 0xffu8 >> partial_bits;
    }
    out
}

fn target_hex_from_leading_zero_bits(bits: u64) -> String {
    hex::encode(target_bytes_from_leading_zero_bits(bits))
}

fn compact_from_target_bytes(target: [u8; 32]) -> u32 {
    let first_non_zero = target.iter().position(|&b| b != 0).unwrap_or(31);
    let mut mantissa = [0u8; 3];
    let mut size = (32 - first_non_zero) as u32;
    if size == 0 {
        return 0;
    }
    if size <= 3 {
        let src = &target[first_non_zero..];
        mantissa[3 - src.len()..].copy_from_slice(src);
        let value = u32::from_be_bytes([0, mantissa[0], mantissa[1], mantissa[2]]);
        return value;
    }
    mantissa.copy_from_slice(&target[first_non_zero..first_non_zero + 3]);
    if mantissa[0] & 0x80 != 0 {
        size += 1;
        let value = u32::from_be_bytes([0, 0, mantissa[0], mantissa[1]]);
        return (size << 24) | value;
    }
    let value = u32::from_be_bytes([0, mantissa[0], mantissa[1], mantissa[2]]);
    (size << 24) | value
}

fn compact_hex_from_leading_zero_bits(bits: u64) -> String {
    format!(
        "{:08x}",
        compact_from_target_bytes(target_bytes_from_leading_zero_bits(bits))
    )
}

fn canonical_submit_reject_reason(detail: &str) -> &'static str {
    if detail.contains("stale_work") || detail.contains("stale_or_out_of_order_block") {
        "stale"
    } else if detail.contains("pow_invalid") {
        "high-hash"
    } else if detail.contains("bad_prevhash")
        || detail.contains("out_of_order")
        || detail.contains("work_mismatch")
    {
        "bad-prevblk"
    } else if detail.contains("syncing") {
        "inconclusive"
    } else {
        "rejected"
    }
}

fn network_hash_ps(data_dir: &str, lookup: u64) -> f64 {
    let (tip_h, _tip_hash, tip_cw, _tip_bits) =
        crate::store::tip_fields(data_dir).unwrap_or((0, "0".repeat(64), 0, 0));
    if tip_h == 0 {
        return 0.0;
    }
    let look = lookup.max(1).min(tip_h);
    let start_h = tip_h.saturating_sub(look);
    let Some(start_b) = crate::store::block_at(data_dir, start_h) else {
        return 0.0;
    };
    let Some(end_b) = crate::store::block_at(data_dir, tip_h) else {
        return 0.0;
    };
    let dt = end_b
        .timestamp
        .unwrap_or(0)
        .saturating_sub(start_b.timestamp.unwrap_or(0))
        .max(1);
    let dcw = tip_cw.saturating_sub(start_b.chainwork);
    dcw as f64 / dt as f64
}

fn sync_progress(data_dir: &str) -> f64 {
    let tip_h = crate::store::tip_fields(data_dir)
        .map(|(h, _, _, _)| h)
        .unwrap_or(0);
    let best_seen = crate::p2p::best_seen_height();
    if best_seen == 0 || tip_h >= best_seen {
        1.0
    } else {
        tip_h as f64 / best_seen as f64
    }
}

fn rpc_info_json() -> serde_json::Value {
    json!({
        "active_commands": [{"method": "rpc", "duration": uptime_secs(), "user": "local"}],
        "logpath": "dutad.stdout.log"
    })
}

fn chain_block_from_submit_hex(block_hex: &str) -> Result<crate::ChainBlock, String> {
    let raw = hex::decode(block_hex).map_err(|_| "block_hex_invalid".to_string())?;
    serde_json::from_slice::<crate::ChainBlock>(&raw)
        .map_err(|e| format!("block_decode_failed: {}", e))
}

fn parse_submit_params(
    data_dir: &str,
    v: &serde_json::Value,
    consume_work: bool,
) -> Result<Result<crate::ChainBlock, serde_json::Value>, String> {
    if let Some(obj) = v.as_object() {
        if obj.get("height").is_some() && obj.get("hash32").is_some() {
            let b: crate::ChainBlock = serde_json::from_value(v.clone())
                .map_err(|e| format!("block_decode_failed: {}", e))?;
            if b.height > 0 && b.txs.is_none() {
                return Err("submitblock_missing_txs".to_string());
            }
            return Ok(Ok(b));
        }
        if let Some(work_id) = obj
            .get("workid")
            .or_else(|| obj.get("work_id"))
            .and_then(|x| x.as_str())
        {
            let nonce = obj
                .get("nonce")
                .and_then(as_u64)
                .ok_or_else(|| "missing_nonce".to_string())?;
            let block = crate::submit_work::build_mined_block_from_work_nonce(
                work_id,
                nonce,
                consume_work,
            )?;
            return Ok(Ok(block));
        }
        return Err("unsupported_submitblock_object".to_string());
    }

    let s = as_str(v).ok_or_else(|| "missing_block_hex".to_string())?;
    if let Ok(b) = chain_block_from_submit_hex(s) {
        return Ok(Ok(b));
    }
    if let Ok(val) = serde_json::from_str::<serde_json::Value>(s) {
        return parse_submit_params(data_dir, &val, consume_work);
    }
    if let Ok(bytes) = hex::decode(s) {
        if let Ok(val) = serde_json::from_slice::<serde_json::Value>(&bytes) {
            return parse_submit_params(data_dir, &val, consume_work);
        }
    }
    Err("unsupported_submitblock_format".to_string())
}

fn validate_address_json(data_dir: &str, addr: &str) -> serde_json::Value {
    match duta_core::address::parse_address_for_network(net_from_datadir(data_dir), addr) {
        Some(pkh) => json!({
            "isvalid": true,
            "address": addr,
            "scriptPubKey": format!("76a914{}88ac", hex::encode(pkh)),
            "isscript": false,
            "iswitness": false
        }),
        None => json!({"isvalid": false, "address": addr}),
    }
}

fn getblocktemplate_json(
    data_dir: &str,
    req: Option<&serde_json::Value>,
) -> Result<serde_json::Value, String> {
    let mode = req
        .and_then(|v| v.get("mode"))
        .and_then(|x| x.as_str())
        .unwrap_or("template");
    if mode == "proposal" {
        let data = req
            .and_then(|v| v.get("data"))
            .ok_or_else(|| "missing_data".to_string())?;
        return match parse_submit_params(data_dir, data, false) {
            Ok(Ok(block)) => match crate::store::validate_candidate_block(data_dir, &block) {
                Ok(_) => Ok(serde_json::Value::Null),
                Err(e) => Ok(json!(canonical_submit_reject_reason(&e))),
            },
            Ok(Err(reason)) => Ok(reason),
            Err(e) => Ok(json!(canonical_submit_reject_reason(&e))),
        };
    }

    let requested_miner = req
        .and_then(|v| {
            v.get("address")
                .or_else(|| v.get("miner_address"))
                .or_else(|| v.get("coinbase_address"))
        })
        .and_then(|x| x.as_str())
        .map(|s| s.trim())
        .filter(|s| !s.is_empty());

    let miner = requested_miner.ok_or_else(|| "missing_miner_address".to_string())?;
    if !crate::work::mining_address_is_valid(miner) {
        return Err("invalid_miner_address".to_string());
    }

    let work = crate::work::build_work_template(data_dir, miner, false, miner)?;
    let bits_internal = work.get("bits").and_then(|x| x.as_u64()).unwrap_or(0);
    let bits_compact = compact_hex_from_leading_zero_bits(bits_internal);
    let target_hex = target_hex_from_leading_zero_bits(bits_internal);
    let txids = work
        .get("transactions")
        .and_then(|x| x.as_array())
        .cloned()
        .unwrap_or_default();
    let txs_obj = work
        .get("txs")
        .and_then(|x| x.as_object())
        .cloned()
        .unwrap_or_default();
    let txs: Vec<serde_json::Value> = txids
        .iter()
        .skip(1)
        .filter_map(|txidv| {
            let txid = txidv.as_str()?;
            let tx = txs_obj.get(txid)?;
            let data = tx_hex_from_json(tx).ok()?;
            Some(json!({
                "txid": txid,
                "hash": txid,
                "data": data,
                "fee": tx.get("fee").and_then(|x| x.as_i64()).unwrap_or(0),
                "depends": [],
                "weight": serde_json::to_vec(tx).map(|v| v.len()).unwrap_or(0),
                "sigops": 0
            }))
        })
        .collect();
    Ok(json!({
        "capabilities": ["proposal", "coinbasetxn", "workid"],
        "mutable": ["time", "nonce", "transactions/add", "prevblock"],
        "rules": ["duta-pow-v3"],
        "vbavailable": {},
        "vbrequired": 0,
        "version": 1,
        "previousblockhash": work.get("prevhash32").cloned().unwrap_or(json!("")),
        "height": work.get("height").cloned().unwrap_or(json!(0)),
        "bits": bits_compact,
        "target": target_hex,
        "difficulty": bits_to_difficulty(bits_internal),
        "curtime": work.get("timestamp").cloned().unwrap_or(json!(0)),
        "mintime": work.get("timestamp").cloned().unwrap_or(json!(0)),
        "noncerange": "0000000000000000ffffffffffffffff",
        "submitold": false,
        "coinbaseaux": {},
        "coinbasevalue": work.get("coinbasevalue").cloned().unwrap_or(json!(0)),
        "coinbasetxn": work.get("coinbasetxn").cloned().unwrap_or(json!({})),
        "transactions": txs,
        "longpollid": work.get("work_id").cloned().unwrap_or(json!("")),
        "sigoplimit": 80000,
        "sizelimit": 4000000,
        "weightlimit": 4000000,
        "workid": work.get("work_id").cloned().unwrap_or(json!("")),
        "default_address_used": false,
        "duta": {
            "algorithm": "duta-pow-v3",
            "template_address": miner,
            "default_address_used": false,
            "coinbase_address_required": true,
            "difficulty_bits": bits_internal,
            "work_id": work.get("work_id").cloned().unwrap_or(json!("")),
            "work_expires_at": work.get("expires_at").cloned().unwrap_or(json!(0)),
            "merkleroot": work.get("merkle32").cloned().unwrap_or(json!("")),
            "header80": work.get("header80").cloned().unwrap_or(json!("")),
            "anchor_hash32": work.get("anchor_hash32").cloned().unwrap_or(json!("")),
            "epoch": work.get("epoch").cloned().unwrap_or(json!(0)),
            "mem_mb": work.get("mem_mb").cloned().unwrap_or(json!(0))
        }
    }))
}

fn resolve_block_height(data_dir: &str, v: Option<&serde_json::Value>) -> Option<u64> {
    match v {
        None => None,
        Some(v0) => {
            if let Some(h) = as_u64(v0) {
                return Some(h);
            }
            if let Some(s) = as_str(v0) {
                let sl = s.to_ascii_lowercase();
                if sl == "best" || sl == "tip" || sl == "latest" {
                    return crate::store::tip_fields(data_dir).map(|(h, _, _, _)| h);
                }
                return crate::store::height_by_hash(data_dir, s);
            }
            None
        }
    }
}

fn pkh_script_hex(pkh_hex: &str) -> String {
    format!("76a914{}88ac", pkh_hex)
}

fn blockheader_json(
    b: &crate::ChainBlock,
    confirmations: u64,
    next_hash: Option<String>,
) -> serde_json::Value {
    json!({
        "hash": b.hash32,
        "confirmations": confirmations,
        "height": b.height,
        "version": 1,
        "versionHex": "00000001",
        "merkleroot": b.merkle32,
        "time": b.timestamp.unwrap_or(0),
        "mediantime": b.timestamp.unwrap_or(0),
        "nonce": b.nonce.unwrap_or(0),
        "bits": format!("{:016x}", b.bits),
        "difficulty_bits": b.bits,
        "chainwork": format!("{:016x}", b.chainwork),
        "previousblockhash": b.prevhash32,
        "nextblockhash": next_hash
    })
}

fn tx_json_from_hex(hexs: &str) -> Result<serde_json::Value, String> {
    let bytes = hex::decode(hexs).map_err(|_| "invalid_hex".to_string())?;
    serde_json::from_slice::<serde_json::Value>(&bytes).map_err(|_| "tx_decode_failed".to_string())
}

fn tx_hex_from_json(tx: &serde_json::Value) -> Result<String, String> {
    let bytes = serde_json::to_vec(tx).map_err(|e| format!("tx_encode_failed: {}", e))?;
    Ok(hex::encode(bytes))
}

fn txid_from_json(tx: &serde_json::Value) -> Result<String, String> {
    let bytes = crate::canon_json::canonical_json_bytes(tx)?;
    Ok(duta_core::hash::sha3_256_hex(&bytes))
}

fn find_tx(
    data_dir: &str,
    txid: &str,
) -> Option<(serde_json::Value, Option<String>, Option<u64>, Option<u64>)> {
    let mp = mempool_value(data_dir);
    if let Some(tx) = mp.get("txs").and_then(|x| x.get(txid)).cloned() {
        return Some((tx, None, None, None));
    }
    let (tip_h, _hash, _cw, _bits) = crate::store::tip_fields(data_dir)?;
    for h in 0..=tip_h {
        let b = crate::store::block_at(data_dir, h)?;
        if let Some(tx) = b.txs.as_ref().and_then(|x| x.get(txid)).cloned() {
            return Some((tx, Some(b.hash32), Some(h), Some(tip_h - h + 1)));
        }
    }
    None
}

fn scan_utxo_set_info(data_dir: &str) -> serde_json::Value {
    let (tip_h, best_hash, _cw, _bits) =
        crate::store::tip_fields(data_dir).unwrap_or((0, "0".repeat(64), 0, 0));
    let mut spent = std::collections::HashSet::<String>::new();
    let mut total_amount: u64 = 0;
    let mut txouts: u64 = 0;
    let mut bogosize: u64 = 0;
    let mut transactions: u64 = 0;
    for h in 0..=tip_h {
        let Some(b) = crate::store::block_at(data_dir, h) else {
            continue;
        };
        let Some(txs_obj) = b.txs.as_ref().and_then(|x| x.as_object()) else {
            continue;
        };
        for (_txid, txv) in txs_obj.iter() {
            transactions = transactions.saturating_add(1);
            for vin in txv
                .get("vin")
                .and_then(|x| x.as_array())
                .cloned()
                .unwrap_or_default()
            {
                let prev_txid = vin.get("txid").and_then(|x| x.as_str()).unwrap_or("");
                let prev_vout = vin.get("vout").and_then(|x| x.as_u64()).unwrap_or(0);
                if !prev_txid.is_empty() {
                    spent.insert(format!("{}:{}", prev_txid, prev_vout));
                }
            }
        }
    }
    for h in 0..=tip_h {
        let Some(b) = crate::store::block_at(data_dir, h) else {
            continue;
        };
        let Some(txs_obj) = b.txs.as_ref().and_then(|x| x.as_object()) else {
            continue;
        };
        for (txid, txv) in txs_obj.iter() {
            for (i, ov) in txv
                .get("vout")
                .and_then(|x| x.as_array())
                .cloned()
                .unwrap_or_default()
                .iter()
                .enumerate()
            {
                let key = format!("{}:{}", txid, i);
                if spent.contains(&key) {
                    continue;
                }
                let value = ov.get("value").and_then(|x| x.as_u64()).unwrap_or(0);
                total_amount = total_amount.saturating_add(value);
                txouts = txouts.saturating_add(1);
                bogosize = bogosize.saturating_add(64);
            }
        }
    }
    json!({
        "height": tip_h,
        "bestblock": best_hash,
        "transactions": transactions,
        "txouts": txouts,
        "bogosize": bogosize,
        "total_amount": total_amount,
        "disk_size": bogosize,
        "hash_serialized_2": format!("{:064x}", total_amount ^ txouts ^ tip_h),
        "total_unspendable_amount": 0
    })
}

pub fn handle_rpc(body: &[u8], data_dir: &str, net: &str) -> Result<String, String> {
    let v: serde_json::Value =
        serde_json::from_slice(body).map_err(|_| "invalid_json".to_string())?;
    if v.is_array() {
        return Ok(err(serde_json::Value::Null, -32600, "batch_not_supported"));
    }
    let obj = match v.as_object() {
        Some(o) => o,
        None => return Ok(err(serde_json::Value::Null, -32600, "invalid_request")),
    };
    let id = obj.get("id").cloned().unwrap_or(serde_json::Value::Null);
    let method = match obj.get("method").and_then(|m| m.as_str()) {
        Some(m) if !m.is_empty() => m,
        _ => return Ok(err(id, -32600, "missing_method")),
    };
    let params = match obj.get("params") {
        Some(p) => match p.as_array() {
            Some(a) => a.clone(),
            None => return Ok(err(id, -32602, "params_must_be_array")),
        },
        None => Vec::new(),
    };

    match method {
        "help" => {
            let methods = json!([
                "help",
                "getblockcount",
                "getbestblockhash",
                "getblockhash(height)",
                "getblock(hash_or_height, verbosity=1)",
                "getblockchaininfo",
                "getblockheader(hash_or_height, verbose=true)",
                "getrawmempool(verbose=false)",
                "getmempoolinfo",
                "getdifficulty",
                "getnetworkhashps(lookup=120)",
                "getblocktemplate(template_request={})",
                "submitblock(block_hex)",
                "gettxout(txid, vout, include_mempool=true)",
                "gettxoutsetinfo",
                "decoderawtransaction(tx_hex)",
                "sendrawtransaction(tx_hex)",
                "getrawtransaction(txid, verbose=false)",
                "getchaintips",
                "getmininginfo",
                "getinfo",
                "getnetworkinfo",
                "getpeerinfo",
                "listbanned",
                "getconnectioncount",
                "ping",
                "uptime",
                "validateaddress(address)",
                "getrpcinfo",
                "addpeer(peer)",
                "addnode(peer)",
                "banpeer(ip, reason=\"manual_operator_ban\")",
                "unbanpeer(ip)",
                "setban(ip, command=\"add\"|\"remove\", reason=\"manual_operator_ban\")"
            ]);
            Ok(ok(id, methods))
        }

        "getblockcount" => {
            let (h, _hash, _cw, _bits) =
                crate::store::tip_fields(data_dir).unwrap_or((0, "0".repeat(64), 0, 0));
            Ok(ok(id, json!(h)))
        }

        "getbestblockhash" => {
            let (_h, hash, _cw, _bits) =
                crate::store::tip_fields(data_dir).unwrap_or((0, "0".repeat(64), 0, 0));
            Ok(ok(id, json!(hash)))
        }

        "getblockhash" => {
            if let Err(_) = ensure_params_len(&params, 1, 1) {
                return Ok(invalid_params(id, "getblockhash(height)"));
            }
            let height = match params.first().and_then(as_u64) {
                Some(h) => h,
                None => return Ok(invalid_params(id, "missing_height")),
            };
            match block_hash_by_height_or_genesis(data_dir, height) {
                Some(hash32) => Ok(ok(id, json!(hash32))),
                None => Ok(err(id, -5, "block_not_found")),
            }
        }

        "getblock" => {
            let arg0 = params.first();
            let verbosity = params.get(1).and_then(as_u64).unwrap_or(1);

            let h_opt = resolve_block_height(data_dir, arg0);

            let h = match h_opt {
                Some(h) => h,
                None => return Ok(err(id, -32602, "missing_hash_or_height")),
            };

            let b = match block_by_height_or_genesis(data_dir, h) {
                Some(b) => b,
                None => return Ok(err(id, -5, "block_not_found")),
            };

            if verbosity == 0 {
                match block_hex_from_block(&b) {
                    Ok(hexs) => Ok(ok(id, json!(hexs))),
                    Err(e) => Ok(err(id, -22, &e)),
                }
            } else {
                Ok(ok(id, block_json_from_chainblock(data_dir, &b, verbosity)))
            }
        }

        "getblockchaininfo" => {
            let (h, hash, cw, bits) =
                crate::store::tip_fields(data_dir).unwrap_or((0, "0".repeat(64), 0, 0));
            let p2p = crate::p2p::p2p_public_info();
            Ok(ok(
                id,
                json!({
                    "chain": net,
                    "blocks": h,
                    "headers": std::cmp::max(h, crate::p2p::best_seen_height()),
                    "bestblockhash": hash,
                    "difficulty_bits": bits,
                    "difficulty": bits_to_difficulty(bits),
                    "chainwork": format!("{:016x}", cw),
                    "mediantime": crate::store::block_at(data_dir, h).and_then(|b| b.timestamp).unwrap_or(0),
                    "verificationprogress": sync_progress(data_dir),
                    "initialblockdownload": sync_progress(data_dir) < 0.999,
                    "pruned": false,
                    "connections": p2p.get("connections").cloned().unwrap_or(json!(0)),
                    "warnings": ""
                }),
            ))
        }

        "getblockheader" => {
            let arg0 = params.first();
            let verbose = params.get(1).and_then(as_bool).unwrap_or(true);

            let h_opt = resolve_block_height(data_dir, arg0);

            let h = match h_opt {
                Some(h) => h,
                None => return Ok(err(id, -32602, "missing_hash_or_height")),
            };
            let b = match block_by_height_or_genesis(data_dir, h) {
                Some(b) => b,
                None => return Ok(err(id, -5, "block_not_found")),
            };
            if !verbose {
                return match header_hex_from_block(&b) {
                    Ok(hex) => Ok(ok(id, json!(hex))),
                    Err(e) => Ok(err(id, -1, &e)),
                };
            }
            let tip_h = crate::store::tip_fields(data_dir)
                .map(|(tip_h, _, _, _)| tip_h)
                .unwrap_or(h);
            let confirmations = if tip_h >= h { tip_h - h + 1 } else { 0 };
            let next_hash =
                crate::store::block_at(data_dir, h.saturating_add(1)).map(|nb| nb.hash32);
            Ok(ok(id, blockheader_json(&b, confirmations, next_hash)))
        }

        "getrawmempool" => {
            let verbose = params.first().and_then(as_bool).unwrap_or(false);
            let mp = mempool_value(data_dir);
            if verbose {
                let mut out = serde_json::Map::new();
                let txs = mp
                    .get("txs")
                    .and_then(|v| v.as_object())
                    .cloned()
                    .unwrap_or_default();
                for (txid, txv) in txs {
                    let bytes = serde_json::to_vec(&txv).map(|v| v.len()).unwrap_or(0);
                    out.insert(txid, json!({
                        "size": bytes,
                        "vsize": bytes,
                        "weight": bytes,
                        "time": crate::now_ts(),
                        "height": crate::store::tip_fields(data_dir).map(|(h, _, _, _)| h.saturating_add(1)).unwrap_or(0),
                        "fee": txv.get("fee").cloned().unwrap_or(json!(0)),
                        "depends": []
                    }));
                }
                Ok(ok(id, serde_json::Value::Object(out)))
            } else {
                Ok(ok(
                    id,
                    mp.get("txids").cloned().unwrap_or_else(|| json!([])),
                ))
            }
        }

        "getmempoolinfo" => {
            let mp = mempool_value(data_dir);
            let txids = mp
                .get("txids")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            let txs = mp
                .get("txs")
                .and_then(|v| v.as_object())
                .cloned()
                .unwrap_or_default();
            let bytes = serde_json::to_vec(&txs).map(|v| v.len()).unwrap_or(0);
            Ok(ok(
                id,
                json!({
                    "loaded": true,
                    "size": txids.len(),
                    "bytes": bytes,
                    "usage": bytes,
                    "unbroadcastcount": 0,
                    "fullrbf": false
                }),
            ))
        }

        "gettxout" => {
            let txid = match params.first().and_then(as_str) {
                Some(s) if !s.is_empty() => s,
                _ => return Ok(err(id, -32602, "missing_txid")),
            };
            let vout = match params.get(1).and_then(as_u64) {
                Some(v) => v,
                None => return Ok(err(id, -32602, "missing_vout")),
            };
            match crate::store::utxo_get(data_dir, txid, vout) {
                Some((value, created_height, is_coinbase, pkh_hex)) => {
                    let confs = crate::store::tip_fields(data_dir)
                        .map(|(tip_h, _, _, _)| {
                            if tip_h >= created_height {
                                tip_h - created_height + 1
                            } else {
                                0
                            }
                        })
                        .unwrap_or(0);
                    let address_s = duta_core::address::pkh_from_hex(&pkh_hex)
                        .map(|pkh| duta_core::address::pkh_to_address(&pkh))
                        .unwrap_or_default();
                    let script_hex = pkh_script_hex(&pkh_hex);
                    Ok(ok(
                        id,
                        json!({
                            "bestblock": crate::store::tip_fields(data_dir).map(|(_, h, _, _)| h).unwrap_or_else(|| "0".repeat(64)),
                            "confirmations": confs,
                            "value": value,
                            "scriptPubKey": {
                                "asm": format!("OP_DUP OP_HASH160 {} OP_EQUALVERIFY OP_CHECKSIG", pkh_hex),
                                "hex": script_hex,
                                "type": "pubkeyhash",
                                "address": address_s,
                                "addresses": if address_s.is_empty() { json!([]) } else { json!([address_s]) },
                                "pkh": pkh_hex,
                            },
                            "coinbase": is_coinbase
                        }),
                    ))
                }
                None => Ok(ok(id, serde_json::Value::Null)),
            }
        }

        "gettxoutsetinfo" => Ok(ok(id, scan_utxo_set_info(data_dir))),

        "decoderawtransaction" => {
            let tx_hex = match params.first().and_then(as_str) {
                Some(s) if !s.is_empty() => s,
                _ => return Ok(err(id, -32602, "missing_tx_hex")),
            };
            let tx = match tx_json_from_hex(tx_hex) {
                Ok(v) => v,
                Err(e) => return Ok(err(id, -22, &e)),
            };
            let txid = match txid_from_json(&tx) {
                Ok(t) => t,
                Err(e) => return Ok(err(id, -22, &e)),
            };
            let size = serde_json::to_vec(&tx).map(|b| b.len() as u64).unwrap_or(0);
            let mut out = tx;
            if let Some(obj) = out.as_object_mut() {
                obj.insert("txid".to_string(), json!(txid));
                obj.insert("hash".to_string(), json!(txid));
                obj.insert("size".to_string(), json!(size));
                obj.insert("vsize".to_string(), json!(size));
                obj.insert("weight".to_string(), json!(size));
            }
            Ok(ok(id, out))
        }

        "sendrawtransaction" => {
            let tx_hex = match params.first().and_then(as_str) {
                Some(s) if !s.is_empty() => s,
                _ => return Ok(err(id, -32602, "missing_tx_hex")),
            };
            let tx = match tx_json_from_hex(tx_hex) {
                Ok(v) => v,
                Err(e) => return Ok(err(id, -22, &e)),
            };
            match crate::submit_tx::ingest_tx(data_dir, None, &tx) {
                Ok(txid) => {
                    crate::p2p::broadcast_tx(&txid, &tx);
                    Ok(ok(id, json!(txid)))
                }
                Err(e) => Ok(err(id, -26, &e)),
            }
        }

        "getrawtransaction" => {
            let txid = match params.first().and_then(as_str) {
                Some(s) if !s.is_empty() => s.to_string(),
                _ => return Ok(err(id, -32602, "missing_txid")),
            };
            let verbose = params.get(1).and_then(as_bool).unwrap_or(false);
            let (tx, blockhash, height, confirmations) = match find_tx(data_dir, &txid) {
                Some(v) => v,
                None => return Ok(err(id, -5, "tx_not_found")),
            };
            if !verbose {
                return match tx_hex_from_json(&tx) {
                    Ok(hex) => Ok(ok(id, json!(hex))),
                    Err(e) => Ok(err(id, -22, &e)),
                };
            }
            let hex_tx = tx_hex_from_json(&tx).unwrap_or_default();
            let mut out = tx;
            let size = serde_json::to_vec(&out)
                .map(|b| b.len() as u64)
                .unwrap_or(0);
            if let Some(obj) = out.as_object_mut() {
                obj.insert("txid".to_string(), json!(txid));
                obj.insert("hash".to_string(), json!(txid));
                obj.insert("in_active_chain".to_string(), json!(blockhash.is_some()));
                obj.insert("hex".to_string(), json!(hex_tx));
                obj.insert("size".to_string(), json!(size));
                obj.insert("vsize".to_string(), json!(size));
                obj.insert("weight".to_string(), json!(size));
                if let Some(h) = blockhash {
                    obj.insert("blockhash".to_string(), json!(h));
                }
                if let Some(h) = height {
                    obj.insert("height".to_string(), json!(h));
                }
                if let Some(c) = confirmations {
                    obj.insert("confirmations".to_string(), json!(c));
                }
            }
            Ok(ok(id, out))
        }

        "getchaintips" => {
            let (tip_h, tip_hash, _cw, _bits) =
                crate::store::tip_fields(data_dir).unwrap_or((0, "0".repeat(64), 0, 0));
            Ok(ok(
                id,
                json!([{
                    "height": tip_h,
                    "hash": tip_hash,
                    "branchlen": 0,
                    "status": "active"
                }]),
            ))
        }

        "getdifficulty" => {
            let (_h, _hash, _cw, bits) =
                crate::store::tip_fields(data_dir).unwrap_or((0, "0".repeat(64), 0, 0));
            Ok(ok(id, json!(bits_to_difficulty(bits))))
        }

        "getnetworkhashps" => {
            let lookup = params.first().and_then(as_u64).unwrap_or(120);
            Ok(ok(id, json!(network_hash_ps(data_dir, lookup))))
        }

        "validateaddress" => {
            let addr = match params.first().and_then(as_str) {
                Some(s) if !s.is_empty() => s,
                _ => return Ok(err(id, -32602, "missing_address")),
            };
            Ok(ok(id, validate_address_json(data_dir, addr)))
        }

        "getblocktemplate" => {
            if params.len() > 1 {
                return Ok(invalid_params(id, "getblocktemplate(template_request={})"));
            }
            match getblocktemplate_json(data_dir, params.first()) {
                Ok(v) => Ok(ok(id, v)),
                Err(e) => Ok(err(id, -32602, &e)),
            }
        }

        "submitblock" => {
            if params.is_empty() || params.len() > 1 {
                return Ok(invalid_params(id, "submitblock(block_hex_or_object)"));
            }
            let arg0 = match params.first() {
                Some(v) => v,
                None => return Ok(invalid_params(id, "missing_block_hex")),
            };
            match parse_submit_params(data_dir, arg0, true) {
                Ok(Ok(block)) => match crate::submit_work::accept_mined_block(data_dir, &block) {
                    Ok(_) => Ok(ok(id, serde_json::Value::Null)),
                    Err(e) => Ok(ok(id, json!(canonical_submit_reject_reason(&e)))),
                },
                Ok(Err(reason)) => Ok(ok(id, reason)),
                Err(e) => Ok(ok(id, json!(canonical_submit_reject_reason(&e)))),
            }
        }

        "getmininginfo" => {
            let (h, hash, _cw, bits) =
                crate::store::tip_fields(data_dir).unwrap_or((0, "0".repeat(64), 0, 0));
            let next_bits = crate::store::expected_bits_next(data_dir).unwrap_or(bits);
            let mp = mempool_value(data_dir);
            let pooledtx = mp
                .get("txids")
                .and_then(|v| v.as_array())
                .map(|a| a.len())
                .unwrap_or(0);
            Ok(ok(
                id,
                json!({
                    "blocks": h,
                    "currentblock": h.saturating_add(1),
                    "currentblocktx": pooledtx,
                    "pooledtx": pooledtx,
                    "difficulty_bits": bits,
                    "difficulty": bits_to_difficulty(bits),
                    "next_bits": next_bits,
                    "networkhashps": network_hash_ps(data_dir, 120),
                    "bestblockhash": hash,
                    "network": net,
                    "chain": net,
                    "errors": "",
                    "warnings": ""
                }),
            ))
        }

        "getinfo" => {
            let (h, hash, cw, bits) =
                crate::store::tip_fields(data_dir).unwrap_or((0, "0".repeat(64), 0, 0));
            let next_bits = crate::store::expected_bits_next(data_dir).unwrap_or(bits);
            Ok(ok(
                id,
                json!({
                    "version": env!("CARGO_PKG_VERSION"),
                    "protocolversion": 1,
                    "blocks": h,
                    "bestblockhash": hash,
                    "chainwork": cw,
                    "difficulty_bits": bits,
                    "next_bits": next_bits,
                    "network": net,
                    "testnet": net != "mainnet",
                    "errors": "",
                    "warnings": "",
                    "unit": DISPLAY_UNIT,
                    "display_unit": DISPLAY_UNIT,
                    "base_unit": BASE_UNIT,
                    "decimals": DUTA_DECIMALS
                }),
            ))
        }

        "getnetworkinfo" => {
            let p2p = crate::p2p::p2p_public_info();
            let v = json!({
                "version": env!("CARGO_PKG_VERSION"),
                "subversion": format!("/dutad:{}/", env!("CARGO_PKG_VERSION")),
                "protocolversion": 1,
                "network": net,
                "connections": p2p.get("connections").cloned().unwrap_or(json!(0)),
                "best_seen_height": p2p.get("best_seen_height").cloned().unwrap_or(json!(0)),
                "sync_gate_active": p2p.get("sync_gate_active").cloned().unwrap_or(json!(false)),
                "sync_gate_hard_active": p2p.get("sync_gate_hard_active").cloned().unwrap_or(json!(false)),
                "sync_gate_unhealthy": p2p.get("sync_gate_unhealthy").cloned().unwrap_or(json!(false)),
                "sync_gate_tip_height": p2p.get("sync_gate_tip_height").cloned().unwrap_or(json!(0)),
                "sync_gate_required_backbone_peers": p2p.get("sync_gate_required_backbone_peers").cloned().unwrap_or(json!(0)),
                "sync_gate_backbone_peers": p2p.get("sync_gate_backbone_peers").cloned().unwrap_or(json!(0)),
                "sync_gate_backbone_heights": p2p.get("sync_gate_backbone_heights").cloned().unwrap_or(json!([])),
                "sync_gate_detail": p2p.get("sync_gate_detail").cloned().unwrap_or(serde_json::Value::Null),
                "bootstrap_source": p2p.get("bootstrap_source").cloned().unwrap_or(json!("")),
                "configured_seed_count": p2p.get("configured_seed_count").cloned().unwrap_or(json!(0)),
                "seeds_file_count": p2p.get("seeds_file_count").cloned().unwrap_or(json!(0)),
                "persisted_peer_count": p2p.get("persisted_peer_count").cloned().unwrap_or(json!(0)),
                "outbound_candidate_count": p2p.get("outbound_candidate_count").cloned().unwrap_or(json!(0)),
                "outbound_quarantined_peer_count": p2p.get("outbound_quarantined_peer_count").cloned().unwrap_or(json!(0)),
                "last_bootstrap_refresh_secs": p2p.get("last_bootstrap_refresh_secs").cloned().unwrap_or(serde_json::Value::Null),
                "last_outbound_ok_peer": p2p.get("last_outbound_ok_peer").cloned().unwrap_or(serde_json::Value::Null),
                "last_outbound_ok_secs": p2p.get("last_outbound_ok_secs").cloned().unwrap_or(serde_json::Value::Null),
                "last_outbound_error_peer": p2p.get("last_outbound_error_peer").cloned().unwrap_or(serde_json::Value::Null),
                "last_outbound_error": p2p.get("last_outbound_error").cloned().unwrap_or(serde_json::Value::Null),
                "last_outbound_error_secs": p2p.get("last_outbound_error_secs").cloned().unwrap_or(serde_json::Value::Null),
                "localservices": "network",
                "relayfee": format_dut_fixed_8(DEFAULT_MIN_RELAY_FEE_PER_KB_DUT),
                "relayfee_dut": DEFAULT_MIN_RELAY_FEE_PER_KB_DUT,
                "unit": DISPLAY_UNIT,
                "display_unit": DISPLAY_UNIT,
                "base_unit": BASE_UNIT,
                "decimals": DUTA_DECIMALS,
                "warnings": ""
            });
            Ok(ok(id, v))
        }

        "getconnectioncount" => {
            let p2p = crate::p2p::p2p_public_info();
            Ok(ok(id, p2p.get("connections").cloned().unwrap_or(json!(0))))
        }

        "ping" => Ok(ok(id, serde_json::Value::Null)),

        "uptime" => Ok(ok(id, json!(uptime_secs()))),

        "getrpcinfo" => Ok(ok(id, rpc_info_json())),

        "addpeer" | "addnode" => {
            let peer = params.first().and_then(as_str).unwrap_or("").trim();
            if peer.is_empty() {
                return Ok(err(id, -32602, "missing_peer"));
            }
            if let Err(e) = crate::p2p::add_peer_manual(data_dir, peer) {
                return Ok(err(id, -1, &e));
            }
            Ok(ok(id, json!({"ok": true, "peer": peer})))
        }

        "getpeerinfo" => {
            let p2p = crate::p2p::p2p_public_info();
            let mut out = Vec::new();
            if let Some(arr) = p2p.get("inbound_live_peers").and_then(|v| v.as_array()) {
                for (i, ent) in arr.iter().enumerate() {
                    let addr = ent.get("addr").and_then(|x| x.as_str()).unwrap_or("");
                    let connected_secs = ent
                        .get("connected_secs")
                        .and_then(|x| x.as_u64())
                        .unwrap_or(0);
                    let last_seen_secs = ent
                        .get("last_seen_secs")
                        .and_then(|x| x.as_u64())
                        .unwrap_or(0);
                    let last_tip_height = ent
                        .get("last_tip_height")
                        .and_then(|x| x.as_u64())
                        .unwrap_or(0);
                    out.push(json!({
                        "id": i,
                        "addr": addr,
                        "inbound": true,
                        "conntime": uptime_secs().saturating_sub(connected_secs),
                        "lastseen": uptime_secs().saturating_sub(last_seen_secs),
                        "synced_headers": last_tip_height,
                        "synced_blocks": last_tip_height,
                        "last_error": ent.get("last_error").cloned().unwrap_or(serde_json::Value::Null),
                        "success_count": ent.get("success_count").cloned().unwrap_or(json!(0)),
                        "failure_count": ent.get("failure_count").cloned().unwrap_or(json!(0))
                    }));
                }
            }
            let base = out.len();
            if let Some(arr) = p2p.get("outbound_recent_peers").and_then(|v| v.as_array()) {
                for (i, ent) in arr.iter().enumerate() {
                    let addr = ent.get("addr").and_then(|x| x.as_str()).unwrap_or("");
                    let connected_secs = ent
                        .get("connected_secs")
                        .and_then(|x| x.as_u64())
                        .unwrap_or(0);
                    let last_seen_secs = ent
                        .get("last_seen_secs")
                        .and_then(|x| x.as_u64())
                        .unwrap_or(0);
                    let last_tip_height = ent
                        .get("last_tip_height")
                        .and_then(|x| x.as_u64())
                        .unwrap_or(0);
                    out.push(json!({
                        "id": base + i,
                        "addr": addr,
                        "inbound": false,
                        "conntime": uptime_secs().saturating_sub(connected_secs),
                        "lastseen": uptime_secs().saturating_sub(last_seen_secs),
                        "synced_headers": last_tip_height,
                        "synced_blocks": last_tip_height,
                        "last_error": ent.get("last_error").cloned().unwrap_or(serde_json::Value::Null),
                        "success_count": ent.get("success_count").cloned().unwrap_or(json!(0)),
                        "failure_count": ent.get("failure_count").cloned().unwrap_or(json!(0))
                    }));
                }
            }
            Ok(ok(id, json!(out)))
        }

        "listbanned" => Ok(ok(id, crate::p2p::list_banned_json())),

        "banpeer" => {
            let ip = params.first().and_then(as_str).unwrap_or("").trim();
            if ip.is_empty() {
                return Ok(err(id, -32602, "missing_ip"));
            }
            let reason = params.get(1).and_then(as_str);
            match crate::p2p::ban_peer_manual(ip, reason) {
                Ok(v) => Ok(ok(id, v)),
                Err(e) => Ok(err(id, -32602, &e)),
            }
        }

        "unbanpeer" => {
            let ip = params.first().and_then(as_str).unwrap_or("").trim();
            if ip.is_empty() {
                return Ok(err(id, -32602, "missing_ip"));
            }
            match crate::p2p::unban_peer_manual(ip) {
                Ok(v) => Ok(ok(id, v)),
                Err(e) => Ok(err(id, -32602, &e)),
            }
        }

        "setban" => {
            let ip = params.first().and_then(as_str).unwrap_or("").trim();
            if ip.is_empty() {
                return Ok(err(id, -32602, "missing_ip"));
            }
            let cmd = params
                .get(1)
                .and_then(as_str)
                .map(|s| s.trim().to_ascii_lowercase())
                .unwrap_or_else(|| "add".to_string());
            let reason = params.get(2).and_then(as_str);
            match cmd.as_str() {
                "add" | "ban" => match crate::p2p::ban_peer_manual(ip, reason) {
                    Ok(v) => Ok(ok(id, v)),
                    Err(e) => Ok(err(id, -32602, &e)),
                },
                "remove" | "unban" => match crate::p2p::unban_peer_manual(ip) {
                    Ok(v) => Ok(ok(id, v)),
                    Err(e) => Ok(err(id, -32602, &e)),
                },
                _ => Ok(invalid_params(
                    id,
                    "setban(ip, command=\"add\"|\"remove\", reason=\"manual_operator_ban\")",
                )),
            }
        }

        _ => Ok(err(id, -32601, "method_not_found")),
    }
}

#[cfg(test)]
mod tests {
    use super::{getblocktemplate_json, handle_rpc, parse_submit_params};
    use serde_json::json;

    #[test]
    fn getblocktemplate_requires_explicit_miner_address() {
        let err = getblocktemplate_json("C:/dutaproject/testnet", Some(&json!({}))).unwrap_err();
        assert_eq!(err, "missing_miner_address");
    }

    #[test]
    fn getblocktemplate_rejects_invalid_miner_address() {
        let err = getblocktemplate_json(
            "C:/dutaproject/testnet",
            Some(&json!({"address":"not-a-valid-address"})),
        )
        .unwrap_err();
        assert_eq!(err, "invalid_miner_address");
    }

    #[test]
    fn rpc_getblockhash_serves_genesis_on_db_only_node() {
        let mut p = std::env::temp_dir();
        let uniq = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        p.push(format!("duta-rpc-json-test-genesis-hash-{}", uniq));
        std::fs::create_dir_all(&p).unwrap();
        let data_dir = p.to_string_lossy().to_string();
        crate::store::ensure_datadir_meta(&data_dir, "mainnet").unwrap();
        crate::store::bootstrap(&data_dir).unwrap();

        let body = json!({"id":1,"method":"getblockhash","params":[0]}).to_string();
        let out = handle_rpc(body.as_bytes(), &data_dir, "mainnet").unwrap();
        let v: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert_eq!(
            v.get("result").and_then(|x| x.as_str()).unwrap_or(""),
            duta_core::netparams::genesis_hash(duta_core::netparams::Network::Mainnet)
        );
    }

    #[test]
    fn rpc_getblock_serves_genesis_on_db_only_node() {
        let mut p = std::env::temp_dir();
        let uniq = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        p.push(format!("duta-rpc-json-test-genesis-block-{}", uniq));
        std::fs::create_dir_all(&p).unwrap();
        let data_dir = p.to_string_lossy().to_string();
        crate::store::ensure_datadir_meta(&data_dir, "mainnet").unwrap();
        crate::store::bootstrap(&data_dir).unwrap();

        let body = json!({"id":1,"method":"getblock","params":[0,1]}).to_string();
        let out = handle_rpc(body.as_bytes(), &data_dir, "mainnet").unwrap();
        let v: serde_json::Value = serde_json::from_str(&out).unwrap();
        let result = v.get("result").cloned().unwrap_or_else(|| json!({}));
        assert_eq!(result.get("height").and_then(|x| x.as_u64()), Some(0));
        assert_eq!(
            result.get("hash").and_then(|x| x.as_str()).unwrap_or(""),
            duta_core::netparams::genesis_hash(duta_core::netparams::Network::Mainnet)
        );
    }

    #[test]
    fn rpc_getblockheader_serves_genesis_metadata_on_db_only_node() {
        let mut p = std::env::temp_dir();
        let uniq = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        p.push(format!("duta-rpc-json-test-genesis-header-{}", uniq));
        std::fs::create_dir_all(&p).unwrap();
        let data_dir = p.to_string_lossy().to_string();
        crate::store::ensure_datadir_meta(&data_dir, "mainnet").unwrap();
        crate::store::bootstrap(&data_dir).unwrap();

        let body = json!({"id":1,"method":"getblockheader","params":[0,true]}).to_string();
        let out = handle_rpc(body.as_bytes(), &data_dir, "mainnet").unwrap();
        let v: serde_json::Value = serde_json::from_str(&out).unwrap();
        let result = v.get("result").cloned().unwrap_or_else(|| json!({}));
        assert_eq!(result.get("height").and_then(|x| x.as_u64()), Some(0));
        assert_eq!(
            result.get("hash").and_then(|x| x.as_str()).unwrap_or(""),
            duta_core::netparams::genesis_hash(duta_core::netparams::Network::Mainnet)
        );
    }

    #[test]
    fn rpc_net_from_datadir_prefers_meta_over_path_name() {
        let mut p = std::env::temp_dir();
        let uniq = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        p.push(format!("duta-rpc-net-meta-{}", uniq));
        std::fs::create_dir_all(&p).unwrap();
        let data_dir = p.to_string_lossy().to_string();
        crate::store::ensure_datadir_meta(&data_dir, "testnet").unwrap();

        assert_eq!(
            super::net_from_datadir(&data_dir),
            duta_core::netparams::Network::Testnet
        );
    }

    #[test]
    fn rpc_info_exposes_display_and_base_unit_metadata() {
        let out = handle_rpc(
            json!({"id":1,"method":"getinfo","params":[]}).to_string().as_bytes(),
            "C:/dutaproject/testnet",
            "testnet",
        )
        .unwrap();
        let v: serde_json::Value = serde_json::from_str(&out).unwrap();
        let result = v.get("result").cloned().unwrap_or_else(|| json!({}));
        assert_eq!(result.get("unit").and_then(|x| x.as_str()), Some("DUTA"));
        assert_eq!(
            result.get("display_unit").and_then(|x| x.as_str()),
            Some("DUTA")
        );
        assert_eq!(result.get("base_unit").and_then(|x| x.as_str()), Some("dut"));
        assert_eq!(result.get("decimals").and_then(|x| x.as_u64()), Some(8));
    }

    #[test]
    fn rpc_network_info_exposes_display_and_base_unit_metadata() {
        let out = handle_rpc(
            json!({"id":1,"method":"getnetworkinfo","params":[]})
                .to_string()
                .as_bytes(),
            "C:/dutaproject/testnet",
            "testnet",
        )
        .unwrap();
        let v: serde_json::Value = serde_json::from_str(&out).unwrap();
        let result = v.get("result").cloned().unwrap_or_else(|| json!({}));
        assert_eq!(result.get("unit").and_then(|x| x.as_str()), Some("DUTA"));
        assert_eq!(
            result.get("display_unit").and_then(|x| x.as_str()),
            Some("DUTA")
        );
        assert_eq!(result.get("base_unit").and_then(|x| x.as_str()), Some("dut"));
        assert_eq!(result.get("decimals").and_then(|x| x.as_u64()), Some(8));
        assert_eq!(
            result.get("relayfee").and_then(|x| x.as_str()),
            Some("0.00010000")
        );
        assert_eq!(result.get("relayfee_dut").and_then(|x| x.as_u64()), Some(10_000));
    }

    #[test]
    fn submitblock_object_requires_txs_for_non_genesis() {
        let err = parse_submit_params(
            "C:/dutaproject/testnet",
            &json!({
                "height": 1,
                "hash32": "11".repeat(32),
                "bits": 12,
                "chainwork": 0,
                "timestamp": 1,
                "prevhash32": "22".repeat(32),
                "merkle32": "33".repeat(32),
                "nonce": 1,
                "miner": "test1111111111111111111111111111111111111111",
                "pow_digest32": "44".repeat(32)
            }),
            false,
        )
        .unwrap_err();
        assert_eq!(err, "submitblock_missing_txs");
    }

    #[test]
    fn rpc_ban_and_unban_peer_round_trip() {
        let out = handle_rpc(
            json!({"id":1,"method":"banpeer","params":["203.0.113.11","test"]})
                .to_string()
                .as_bytes(),
            "C:/dutaproject/testnet",
            "testnet",
        )
        .unwrap();
        let v: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert_eq!(
            v.get("result")
                .and_then(|x| x.get("ip"))
                .and_then(|x| x.as_str()),
            Some("203.0.113.11")
        );

        let out = handle_rpc(
            json!({"id":1,"method":"listbanned","params":[]})
                .to_string()
                .as_bytes(),
            "C:/dutaproject/testnet",
            "testnet",
        )
        .unwrap();
        let v: serde_json::Value = serde_json::from_str(&out).unwrap();
        let arr = v
            .get("result")
            .and_then(|x| x.as_array())
            .cloned()
            .unwrap_or_default();
        assert!(arr
            .iter()
            .any(|x| x.get("ip").and_then(|y| y.as_str()) == Some("203.0.113.11")));

        let out = handle_rpc(
            json!({"id":1,"method":"unbanpeer","params":["203.0.113.11"]})
                .to_string()
                .as_bytes(),
            "C:/dutaproject/testnet",
            "testnet",
        )
        .unwrap();
        let v: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert_eq!(
            v.get("result")
                .and_then(|x| x.get("removed"))
                .and_then(|x| x.as_bool()),
            Some(true)
        );
    }
}

