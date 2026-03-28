use crate::p2p;
use crate::store;
use crate::submit_work;
use duta_core::address;
use duta_core::amount::DUT_PER_DUTA;
use duta_core::dutahash;
use duta_core::netparams::{self, Network};
use duta_core::types::H32;
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::net::IpAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};

#[derive(Clone, Debug)]
pub(crate) struct WorkItem {
    pub created_at: u64,
    pub expires_at: u64,
    pub height: u64,
    pub pow_version: u8,
    pub prevhash32: String,
    pub merkle32: String,
    pub timestamp: u64,
    pub bits: u64,
    pub chainwork: u64,
    pub miner: String,
    pub work_scope: String,
    pub tx_count: u64,
    pub fees_total: u64,
    pub txs_obj: serde_json::Value,
    pub header: [u8; 80],
    pub anchor_hash32: String,
    pub epoch: u64,
    pub mem_mb: usize,
}

static WORK_MAP: OnceLock<Mutex<HashMap<String, WorkItem>>> = OnceLock::new();
const MAX_OUTSTANDING_WORK_TOTAL: usize = 4096;
const MAX_OUTSTANDING_WORK_PER_MINER: usize = 64;
static WORK_SEQ: AtomicU64 = AtomicU64::new(1);

fn work_map() -> &'static Mutex<HashMap<String, WorkItem>> {
    WORK_MAP.get_or_init(|| Mutex::new(HashMap::new()))
}

fn work_map_lock() -> std::sync::MutexGuard<'static, HashMap<String, WorkItem>> {
    crate::lock_or_recover(work_map(), "work_map")
}

fn now_ts() -> u64 {
    crate::now_ts()
}

fn verbose_mining_log() -> bool {
    std::env::var_os("DUTA_VERBOSE_MINING_LOG").is_some()
}

fn hash32_from_bytes(bytes: &[u8]) -> String {
    duta_core::hash::sha3_256_hex(bytes)
}

fn header80(
    prevhash32: &str,
    merkle32: &str,
    timestamp: u64,
    bits: u64,
) -> Result<[u8; 80], String> {
    let prev = H32::from_hex(prevhash32).ok_or_else(|| "prevhash_invalid".to_string())?;
    let merkle = H32::from_hex(merkle32).ok_or_else(|| "merkle_invalid".to_string())?;
    let mut out = [0u8; 80];
    out[0..32].copy_from_slice(prev.as_bytes());
    out[32..64].copy_from_slice(merkle.as_bytes());
    out[64..72].copy_from_slice(&timestamp.to_le_bytes());
    out[72..80].copy_from_slice(&bits.to_le_bytes());
    Ok(out)
}

fn block_subsidy(height: u64) -> u64 {
    const INITIAL_BLOCK_REWARD: u64 = 50 * DUT_PER_DUTA;
    const HALVING_INTERVAL: u64 = 210_000;
    const MAX_HALVINGS: u64 = 64;
    let halvings = height / HALVING_INTERVAL;
    if halvings >= MAX_HALVINGS {
        return 0;
    }
    INITIAL_BLOCK_REWARD >> (halvings as u32)
}

fn read_mempool_value(data_dir: &str) -> serde_json::Value {
    let path = format!("{}/mempool.json", data_dir.trim_end_matches('/'));
    if let Ok(s) = fs::read_to_string(&path) {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&s) {
            if let Some(sanitized) = sanitize_mempool_value(&v) {
                return sanitized;
            }
            return v;
        }
    }
    json!({"txids": [], "txs": {}})
}

fn sanitize_mempool_value(v: &serde_json::Value) -> Option<serde_json::Value> {
    let mut mp = v.clone();
    let txs_obj = mp.get("txs").and_then(|x| x.as_object())?.clone();
    let mut changed = false;
    let mut new_txs = serde_json::Map::new();
    let mut ordered: Vec<String> = Vec::new();

    if let Some(old_ids) = mp.get("txids").and_then(|x| x.as_array()) {
        for item in old_ids {
            let Some(old_key) = item.as_str() else {
                continue;
            };
            let Some(txv) = txs_obj.get(old_key) else {
                continue;
            };
            let mut tx_for_id = txv.clone();
            if let Some(obj) = tx_for_id.as_object_mut() {
                obj.remove("size");
            }
            let new_key =
                crate::store::txid_from_value(&tx_for_id).unwrap_or_else(|_| old_key.to_string());
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
        let new_key = crate::store::txid_from_value(&tx_for_id).unwrap_or_else(|_| old_key.clone());
        if new_key != *old_key {
            changed = true;
        }
        if !new_txs.contains_key(&new_key) {
            new_txs.insert(new_key.clone(), txv.clone());
            ordered.push(new_key);
        }
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

fn template_tx_for_block(txv: &serde_json::Value) -> serde_json::Value {
    let mut tx = txv.clone();
    if let Some(obj) = tx.as_object_mut() {
        obj.remove("size");
    }
    tx
}

enum TemplateFilterDecision {
    Keep,
    Reject,
    Pending,
}

fn log_template_filter_reject(
    txid: &str,
    reason: &str,
    prev_txid: &str,
    prev_vout: u64,
    next_height: u64,
) {
    wlog!(
        "[dutad] TEMPLATE_FILTER_DROP txid={} reason={} prev={}:{} next_height={}",
        short_id(txid),
        reason,
        short_id(prev_txid),
        prev_vout,
        next_height
    );
}

fn log_template_filter_trace(
    txid: &str,
    reason: &str,
    prev_txid: &str,
    prev_vout: u64,
    next_height: u64,
) {
    edlog!(
        "[dutad] TEMPLATE_FILTER_TRACE txid={} reason={} prev={}:{} next_height={}",
        short_id(txid),
        reason,
        short_id(prev_txid),
        prev_vout,
        next_height
    );
}

fn filter_template_mempool_with_lookup<F>(
    next_height: u64,
    mp: &serde_json::Value,
    mut utxo_lookup: F,
) -> serde_json::Value
where
    F: FnMut(&str, u64) -> Option<(u64, u64, bool, String)>,
{
    let Some(txs_obj) = mp.get("txs").and_then(|v| v.as_object()) else {
        return mp.clone();
    };
    let mut ordered_ids: Vec<String> = mp
        .get("txids")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();

    for txid in txs_obj.keys() {
        if !ordered_ids.iter().any(|existing| existing == txid) {
            ordered_ids.push(txid.clone());
        }
    }

    let mut kept_ids: Vec<String> = Vec::new();
    let mut kept_txs = serde_json::Map::new();
    let mut produced_outpoints: HashSet<String> = HashSet::new();
    let mut consumed_outpoints: HashSet<String> = HashSet::new();
    let mut pending_ids = ordered_ids.clone();
    let mut rejected_ids: HashSet<String> = HashSet::new();

    loop {
        let mut next_pending = Vec::new();
        let mut progressed = false;

        for txid in pending_ids {
            let Some(txv) = txs_obj.get(&txid) else {
                continue;
            };
            edlog!(
                "[dutad] TEMPLATE_FILTER_CHECK txid={} next_height={} vin_count={} vout_count={}",
                short_id(&txid),
                next_height,
                txv.get("vin").and_then(|v| v.as_array()).map(|v| v.len()).unwrap_or(0),
                txv.get("vout").and_then(|v| v.as_array()).map(|v| v.len()).unwrap_or(0),
            );
            let vin = txv
                .get("vin")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            let vout = txv
                .get("vout")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();

            let mut decision = TemplateFilterDecision::Keep;
            let mut pending_prev: Option<(String, u64, &'static str)> = None;
            for input in &vin {
                let Some(prev_txid) = input.get("txid").and_then(|v| v.as_str()) else {
                    log_template_filter_reject(&txid, "prev_txid_missing", "-", 0, next_height);
                    decision = TemplateFilterDecision::Reject;
                    break;
                };
                let Some(prev_vout) = input.get("vout").and_then(|v| v.as_u64()) else {
                    log_template_filter_reject(
                        &txid,
                        "prev_vout_missing",
                        prev_txid,
                        0,
                        next_height,
                    );
                    decision = TemplateFilterDecision::Reject;
                    break;
                };
                let outpoint = format!("{}:{}", prev_txid, prev_vout);
                if consumed_outpoints.contains(&outpoint) {
                    log_template_filter_reject(
                        &txid,
                        "outpoint_conflict",
                        prev_txid,
                        prev_vout,
                        next_height,
                    );
                    decision = TemplateFilterDecision::Reject;
                    break;
                }
                if produced_outpoints.contains(&outpoint) {
                    log_template_filter_trace(
                        &txid,
                        "parent_created_in_same_template",
                        prev_txid,
                        prev_vout,
                        next_height,
                    );
                    continue;
                }
                if rejected_ids.contains(prev_txid) {
                    log_template_filter_reject(
                        &txid,
                        "parent_rejected",
                        prev_txid,
                        prev_vout,
                        next_height,
                    );
                    decision = TemplateFilterDecision::Reject;
                    break;
                }
                if txs_obj.contains_key(prev_txid) {
                    log_template_filter_trace(
                        &txid,
                        "parent_missing_in_mempool_order",
                        prev_txid,
                        prev_vout,
                        next_height
                    );
                    pending_prev = Some((prev_txid.to_string(), prev_vout, "parent_missing_in_mempool_order"));
                    decision = TemplateFilterDecision::Pending;
                    break;
                }
                match utxo_lookup(prev_txid, prev_vout) {
                    Some((_value, created_height, is_coinbase, _pkh)) => {
                        if is_coinbase
                            && next_height
                                < created_height.saturating_add(store::COINBASE_MATURITY)
                        {
                            wlog!(
                                "[dutad] TEMPLATE_FILTER_DROP txid={} reason=coinbase_immature prev={}:{} created_height={} maturity={} next_height={}",
                                short_id(&txid),
                                short_id(prev_txid),
                                prev_vout,
                                created_height,
                                store::COINBASE_MATURITY,
                                next_height
                            );
                            decision = TemplateFilterDecision::Reject;
                            break;
                        }
                        log_template_filter_trace(
                            &txid,
                            "utxo_present",
                            prev_txid,
                            prev_vout,
                            next_height,
                        );
                    }
                    None => {
                        log_template_filter_reject(
                            &txid,
                            "utxo_missing",
                            prev_txid,
                            prev_vout,
                            next_height,
                        );
                        decision = TemplateFilterDecision::Reject;
                        break;
                    }
                }
            }

            match decision {
                TemplateFilterDecision::Keep => {
                    for input in &vin {
                        let prev_txid = input.get("txid").and_then(|v| v.as_str()).unwrap_or_default();
                        let prev_vout = input.get("vout").and_then(|v| v.as_u64()).unwrap_or(0);
                        consumed_outpoints.insert(format!("{}:{}", prev_txid, prev_vout));
                    }
                    for (idx, _output) in vout.iter().enumerate() {
                        produced_outpoints.insert(format!("{}:{}", txid, idx));
                    }
                    edlog!(
                        "[dutad] TEMPLATE_FILTER_KEEP txid={} next_height={} vin_count={} vout_count={}",
                        short_id(&txid),
                        next_height,
                        vin.len(),
                        vout.len(),
                    );
                    kept_ids.push(txid.clone());
                    kept_txs.insert(txid, txv.clone());
                    progressed = true;
                }
                TemplateFilterDecision::Pending => {
                    if let Some((prev_txid, prev_vout, reason)) = pending_prev.as_ref() {
                        log_template_filter_trace(
                            &txid,
                            reason,
                            prev_txid,
                            *prev_vout,
                            next_height,
                        );
                    }
                    next_pending.push(txid)
                }
                TemplateFilterDecision::Reject => {
                    rejected_ids.insert(txid);
                }
            }
        }

        if next_pending.is_empty() || !progressed {
            if !next_pending.is_empty() && !progressed {
                for txid in &next_pending {
                    edlog!(
                        "[dutad] TEMPLATE_FILTER_DROP txid={} reason=parent_missing_unresolved prev=-:0 next_height={}",
                        short_id(txid),
                        next_height
                    );
                }
            }
            break;
        }
        pending_ids = next_pending;
    }

    json!({
        "txids": kept_ids,
        "txs": kept_txs
    })
}

fn filter_template_mempool(
    data_dir: &str,
    next_height: u64,
    mp: &serde_json::Value,
) -> serde_json::Value {
    filter_template_mempool_with_lookup(next_height, mp, |prev_txid, prev_vout| {
        store::utxo_get(data_dir, prev_txid, prev_vout)
    })
}

fn same_tip_scope(existing: &WorkItem, next: &WorkItem) -> bool {
    existing.work_scope == next.work_scope
        && existing.prevhash32 == next.prevhash32
        && existing.height == next.height
}

fn materially_changed_template(existing: &WorkItem, next: &WorkItem) -> bool {
    existing.tx_count != next.tx_count || existing.merkle32 != next.merkle32
}

fn short_id(id: &str) -> &str {
    if id.len() <= 8 {
        id
    } else {
        &id[..8]
    }
}

fn short_scope(scope: &str) -> &str {
    if scope.len() <= 24 {
        scope
    } else {
        &scope[..24]
    }
}

fn short_addr(addr: &str) -> &str {
    if addr.len() <= 16 {
        addr
    } else {
        &addr[..16]
    }
}

fn net_from_datadir(data_dir: &str) -> Network {
    if let Some(net) = store::read_datadir_network(data_dir) {
        net
    } else if data_dir.contains("testnet") {
        Network::Testnet
    } else if data_dir.contains("stagenet") {
        Network::Stagenet
    } else {
        Network::Mainnet
    }
}

pub(crate) fn mining_address_is_valid(addr: &str) -> bool {
    address::parse_address(addr.trim()).is_some()
}

fn mining_address_is_valid_for_network(net: Network, addr: &str) -> bool {
    address::parse_address_for_network(net, addr.trim()).is_some()
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

pub(crate) fn build_work_template(
    data_dir: &str,
    miner: &str,
    _stratum_pool_request: bool,
    work_scope: &str,
) -> Result<serde_json::Value, String> {
    let miner = miner.trim();
    if miner.is_empty() {
        return Err("missing_address".to_string());
    }
    let work_scope = work_scope.trim();
    if work_scope.is_empty() {
        return Err("missing_work_scope".to_string());
    }
    let net = net_from_datadir(data_dir);
    if !mining_address_is_valid_for_network(net, miner) {
        return Err("invalid_address".to_string());
    }
    let dev_addr = netparams::devfee_addrs(net)[0];
    if miner == dev_addr {
        return Err("miner_address_conflicts_with_devfee".to_string());
    }

    let (tip_h, tip_hash32, _tip_bits, tip_chainwork) =
        store::tip_fields(data_dir).unwrap_or((0, "0".repeat(64), 0, 0));

    let best_h = p2p::best_seen_height();
    let max_lag = netparams::max_local_mining_sync_lag_blocks(net);
    let next_height = tip_h.saturating_add(1);
    let sync_gate_active = next_height <= netparams::mining_sync_gate_until_height(net);
    if sync_gate_active && best_h > tip_h.saturating_add(max_lag) {
        return Err(format!(
            "syncing tip_height={} best_seen_height={}",
            tip_h, best_h
        ));
    }
    let height = next_height;
    let mp = filter_template_mempool(data_dir, next_height, &read_mempool_value(data_dir));
    let mut txids = mp
        .get("txids")
        .and_then(|x| x.as_array())
        .cloned()
        .unwrap_or_default();
    let prevhash32 = tip_hash32;
    let bits = store::expected_bits_next(data_dir)?;
    let chainwork =
        tip_chainwork.saturating_add(1u64.checked_shl(bits.min(63) as u32).unwrap_or(0));
    let ts = now_ts();

    let fees_total: u64 = mp
        .get("txs")
        .and_then(|x| x.as_object())
        .map(|o| {
            o.values()
                .map(|txv| txv.get("fee").and_then(|x| x.as_u64()).unwrap_or(0))
                .sum::<u64>()
        })
        .unwrap_or(0);

    let subsidy = block_subsidy(height);
    let reward_total = subsidy.saturating_add(fees_total);
    let dev_bps = netparams::devfee_bps(net, height);
    let dev_amt = reward_total.saturating_mul(dev_bps) / 10_000;
    let miner_amt = reward_total.saturating_sub(dev_amt);

    let coinbase_tx = json!({
        "coinbase_data": {
            "height": height,
            "prevhash32": prevhash32,
        },
        "vin": [],
        "vout": [
            {"addr": miner, "address": miner, "value": miner_amt},
            {"addr": dev_addr, "address": dev_addr, "value": dev_amt}
        ]
    });
    let coinbase_txid = store::txid_from_value(&coinbase_tx)?;

    txids.insert(0, json!(coinbase_txid.clone()));
    let tx_count = txids.len();

    let mut txs_obj = mp.get("txs").cloned().unwrap_or_else(|| json!({}));
    if let Some(obj) = txs_obj.as_object_mut() {
        for (_txid, txv) in obj.iter_mut() {
            *txv = template_tx_for_block(txv);
        }
        obj.insert(coinbase_txid.clone(), coinbase_tx.clone());
        obj.insert(
            "__order".to_string(),
            serde_json::Value::Array(txids.clone()),
        );
    }

    let ordered_txids: Vec<String> = txids
        .iter()
        .filter_map(|t| t.as_str().map(|s| s.to_string()))
        .collect();
    let merkle32 = store::merkle32_from_txids(&ordered_txids)?;
    let header = header80(&prevhash32, &merkle32, ts, bits)?;

    let pow_version = netparams::pow_consensus_version(net, height);
    let anchor_h = dutahash::anchor_height_for_version(pow_version, height);
    let anchor_hash32 = if anchor_h == 0 {
        H32::zero()
    } else {
        store::block_at(data_dir, anchor_h)
            .and_then(|b| H32::from_hex(&b.hash32))
            .unwrap_or_else(H32::zero)
    };

    let epoch = dutahash::epoch_number_for_version(pow_version, height);
    let mem_mb = dutahash::stage_mem_mb_for_version(pow_version, height);
    let created_at = ts;
    let expires_at = created_at.saturating_add(30);

    let seq = WORK_SEQ.fetch_add(1, Ordering::Relaxed);
    let mut id_bytes = Vec::new();
    id_bytes.extend_from_slice(&seq.to_le_bytes());
    id_bytes.extend_from_slice(&created_at.to_le_bytes());
    id_bytes.extend_from_slice(prevhash32.as_bytes());
    id_bytes.extend_from_slice(merkle32.as_bytes());
    let work_id = hash32_from_bytes(&id_bytes);

    let item = WorkItem {
        created_at,
        expires_at,
        height,
        pow_version,
        prevhash32: prevhash32.clone(),
        merkle32: merkle32.clone(),
        timestamp: ts,
        bits,
        chainwork,
        miner: miner.to_string(),
        work_scope: work_scope.to_string(),
        tx_count: tx_count as u64,
        fees_total,
        txs_obj: txs_obj.clone(),
        header,
        anchor_hash32: anchor_hash32.to_hex(),
        epoch,
        mem_mb,
    };

    {
        let mut map = work_map_lock();
        let before_total = map.len();
        let same_tip_matches: Vec<(String, WorkItem)> = map
            .iter()
            .filter(|(_, v)| same_tip_scope(v, &item))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let same_tip_match_count = same_tip_matches.len();
        if let Some((existing_work_id, existing_item)) = same_tip_matches.iter().max_by_key(|(_, v)| v.created_at) {
            if !materially_changed_template(existing_item, &item) {
                if let Some(existing_mut) = map.get_mut(existing_work_id) {
                    existing_mut.expires_at = expires_at;
                }
                let refreshed = map
                    .get(existing_work_id)
                    .cloned()
                    .unwrap_or_else(|| existing_item.clone());
                wlog!(
                    "[dutad] WORK_REUSE work={} scope={} height={} prev={} ts={} tx_count={} fees_total={} reuse_from={} old_expires_at={} new_expires_at={} reason=same_tip_no_material_change",
                    short_id(existing_work_id),
                    short_scope(work_scope),
                    refreshed.height,
                    short_id(&refreshed.prevhash32),
                    refreshed.timestamp,
                    refreshed.tx_count,
                    refreshed.fees_total,
                    short_id(existing_work_id),
                    existing_item.expires_at,
                    refreshed.expires_at
                );
                return Ok(json!({
                    "work_id": existing_work_id,
                    "workid": existing_work_id,
                    "expires_at": refreshed.expires_at,
                    "height": refreshed.height,
                    "pow_version": refreshed.pow_version,
                    "prevhash32": refreshed.prevhash32,
                    "merkle32": refreshed.merkle32,
                    "merkleroot": refreshed.merkle32,
                    "previousblockhash": refreshed.prevhash32,
                    "timestamp": refreshed.timestamp,
                    "curtime": refreshed.timestamp,
                    "mintime": refreshed.timestamp,
                    "bits": refreshed.bits,
                    "bits_compact": compact_hex_from_leading_zero_bits(refreshed.bits),
                    "target": target_hex_from_leading_zero_bits(refreshed.bits),
                    "anchor_h": anchor_h,
                    "anchor_hash32": refreshed.anchor_hash32,
                    "epoch": refreshed.epoch,
                    "mem_mb": refreshed.mem_mb,
                    "tx_count": refreshed.tx_count,
                    "coinbasevalue": reward_total,
                    "coinbase_txid": coinbase_txid,
                    "coinbasetxn": coinbase_tx,
                    "transactions": txids,
                    "txs": txs_obj,
                    "header80": hex::encode(refreshed.header),
                    "duta": {
                        "algorithm": netparams::pow_algorithm_name(net, height),
                        "pow_version": refreshed.pow_version,
                        "difficulty_bits": refreshed.bits,
                        "template_address": miner,
                        "work_expires_at": refreshed.expires_at,
                        "anchor_height": anchor_h,
                        "anchor_hash32": refreshed.anchor_hash32,
                        "epoch": refreshed.epoch,
                        "mem_mb": refreshed.mem_mb,
                        "header80": hex::encode(refreshed.header)
                    }
                }));
            }
        }
        map.retain(|_, v| v.expires_at > created_at);
        let after_expiry = map.len();
        let evicted_matches: Vec<(String, WorkItem)> = same_tip_matches
            .into_iter()
            .filter(|(_, v)| materially_changed_template(v, &item))
            .collect();
        let evicted_same_tip = evicted_matches.len();
        let evicted_ids: Vec<String> = evicted_matches
            .iter()
            .map(|(k, v)| {
                format!(
                    "{}:{}:{}:{}:{}",
                    short_id(k),
                    v.height,
                    short_id(&v.prevhash32),
                    v.tx_count,
                    v.fees_total
                )
            })
            .collect();
        if evicted_same_tip > 0 {
            for (old_work_id, old_item) in &evicted_matches {
                wlog!(
                    "[dutad] WORK_SUPERSEDE old_work={} new_work={} scope={} height={} prev={} old_ts={} new_ts={} old_tx_count={} new_tx_count={} old_fees_total={} new_fees_total={} reason=same_tip_scope_material_change",
                    short_id(old_work_id),
                    short_id(&work_id),
                    short_scope(work_scope),
                    height,
                    short_id(&prevhash32),
                    old_item.timestamp,
                    ts,
                    old_item.tx_count,
                    tx_count,
                    old_item.fees_total,
                    fees_total
                );
            }
        }
        map.retain(|k, _| !evicted_matches.iter().any(|(old_k, _)| old_k == k));
        if map.len() >= MAX_OUTSTANDING_WORK_TOTAL {
            return Err("busy".to_string());
        }
        let per_scope = map.values().filter(|v| v.work_scope == work_scope).count();
        if per_scope >= MAX_OUTSTANDING_WORK_PER_MINER {
            return Err("too_many_outstanding_work".to_string());
        }
        map.insert(work_id.clone(), item);
        wlog!(
            "[dutad] WORK_ISSUE work={} scope={} height={} prev={} ts={} tx_count={} fees_total={} before_total={} after_expiry={} same_tip_matches={} evicted_same_tip={} per_scope_after={} evicted_ids={:?}",
            short_id(&work_id),
            short_scope(work_scope),
            height,
            short_id(&prevhash32),
            ts,
            tx_count,
            fees_total,
            before_total,
            after_expiry,
            same_tip_match_count,
            evicted_same_tip,
            per_scope + 1,
            evicted_ids
        );
    }

    submit_work::prewarm_dataset(pow_version, epoch, anchor_hash32, mem_mb);
    {
        let anchor_for_store = anchor_hash32;
        std::thread::spawn(move || {
            let started = std::time::Instant::now();
            store::prewarm_pow_dataset(pow_version, height, anchor_for_store);
            wlog!(
                "[dutad] STORE_DATASET_PREWARM pow_version={} height={} mem_mb={} elapsed_ms={}",
                pow_version,
                height,
                mem_mb,
                started.elapsed().as_millis()
            );
        });
    }

    Ok(json!({
        "work_id": work_id,
        "workid": work_id,
        "expires_at": expires_at,
        "height": height,
        "pow_version": pow_version,
        "prevhash32": prevhash32,
        "merkle32": merkle32,
        "merkleroot": merkle32,
        "previousblockhash": prevhash32,
        "timestamp": ts,
        "curtime": ts,
        "mintime": ts,
        "bits": bits,
        "bits_compact": compact_hex_from_leading_zero_bits(bits),
        "target": target_hex_from_leading_zero_bits(bits),
        "anchor_h": anchor_h,
        "anchor_hash32": anchor_hash32.to_hex(),
        "epoch": epoch,
        "mem_mb": mem_mb,
        "tx_count": tx_count,
        "coinbasevalue": reward_total,
        "coinbase_txid": coinbase_txid,
        "coinbasetxn": coinbase_tx,
        "transactions": txids,
        "txs": txs_obj,
        "header80": hex::encode(header),
        "duta": {
            "algorithm": netparams::pow_algorithm_name(net, height),
            "pow_version": pow_version,
            "difficulty_bits": bits,
            "template_address": miner,
            "work_expires_at": expires_at,
            "anchor_height": anchor_h,
            "anchor_hash32": anchor_hash32.to_hex(),
            "epoch": epoch,
            "mem_mb": mem_mb,
            "header80": hex::encode(header)
        }
    }))
}

fn request_is_stratum_pool_work(request: &tiny_http::Request) -> bool {
    let source_ok = request.headers().iter().any(|h| {
        h.field.equiv("X-DUTA-Work-Source")
            && h.value.as_str().to_ascii_lowercase().contains("stratum")
    });
    if !source_ok {
        return false;
    }
    request
        .remote_addr()
        .map(|addr| match addr.ip() {
            IpAddr::V4(v4) => v4.is_loopback(),
            IpAddr::V6(v6) => v6.is_loopback(),
        })
        .unwrap_or(false)
}

fn stratum_work_scope(miner: &str, worker: &str) -> String {
    let worker = worker.trim();
    if worker.is_empty() {
        return format!("{}#work", miner);
    }
    format!("{}#{}#work", miner, worker)
}

fn daemon_work_scope(miner: &str) -> String {
    format!("{}#work", miner)
}

pub(crate) fn gbt_work_scope(miner: &str) -> String {
    format!("{}#gbt", miner)
}

fn request_work_scope(
    request: &tiny_http::Request,
    miner: &str,
    stratum_pool_bypass: bool,
) -> String {
    if stratum_pool_bypass {
        if let Some(worker) = request
            .headers()
            .iter()
            .find(|h| h.field.equiv("X-DUTA-Worker"))
            .map(|h| h.value.as_str())
        {
            return stratum_work_scope(miner, worker);
        }
    }
    daemon_work_scope(miner)
}

pub fn handle_work(
    request: tiny_http::Request,
    data_dir: &str,
    respond_json: fn(tiny_http::Request, tiny_http::StatusCode, String),
) {
    if request.method() != &tiny_http::Method::Get {
        crate::respond_error(request, tiny_http::StatusCode(405), "method_not_allowed");
        return;
    }

    let (path, params) = crate::parse_query(request.url());
    let miner_q = crate::get_param(&params, "address")
        .unwrap_or("")
        .to_string();
    let miner_p = path.strip_prefix("/work/").unwrap_or("").to_string();
    let miner = if !miner_q.is_empty() {
        miner_q
    } else {
        miner_p
    };

    let stratum_pool_bypass = request_is_stratum_pool_work(&request);
    let work_scope = request_work_scope(&request, &miner, stratum_pool_bypass);
    let tpl = match build_work_template(data_dir, &miner, stratum_pool_bypass, &work_scope) {
        Ok(v) => v,
        Err(e) if e == "missing_address" => json!({"ok":false,"error":"missing_address"}),
        Err(e) if e == "invalid_address" => json!({"ok":false,"error":"invalid_address"}),
        Err(e) if e == "miner_address_conflicts_with_devfee" => {
            json!({"ok":false,"error":"miner_address_conflicts_with_devfee"})
        }
        Err(e) if e == "busy" => {
            wlog!(
                "[dutad] WORK_REJECT addr={} reason=busy",
                short_addr(&miner)
            );
            crate::respond_error(request, tiny_http::StatusCode(503), "busy");
            return;
        }
        Err(e) if e == "too_many_outstanding_work" => {
            wlog!(
                "[dutad] WORK_REJECT addr={} reason=too_many_outstanding_work",
                short_addr(&miner)
            );
            crate::respond_error(
                request,
                tiny_http::StatusCode(429),
                "too_many_outstanding_work",
            );
            return;
        }
        Err(e) if e.starts_with("syncing") => {
            wlog!(
                "[dutad] WORK_REJECT addr={} reason=syncing detail={} ",
                short_addr(&miner),
                e
            );
            crate::respond_error_detail(
                request,
                tiny_http::StatusCode(503),
                "syncing",
                json!({"detail": e}),
            );
            return;
        }
        Err(e) => {
            edlog!(
                "[dutad] WORK_REJECT addr={} reason=template_failed detail={}",
                short_addr(&miner),
                e
            );
            crate::respond_error_detail(
                request,
                tiny_http::StatusCode(500),
                "template_failed",
                json!({"detail": e}),
            );
            return;
        }
    };

    if tpl.get("error").is_some() {
        wlog!(
            "[dutad] WORK_REJECT addr={} reason={}",
            short_addr(&miner),
            tpl.get("error")
                .and_then(|x| x.as_str())
                .unwrap_or("bad_request")
        );
        respond_json(request, tiny_http::StatusCode(400), tpl.to_string());
        return;
    }
    if verbose_mining_log() {
        wlog!(
            "[dutad] WORK_OK addr={} height={} bits={} work={} txs={} epoch={} mem_mb={}",
            short_addr(&miner),
            tpl.get("height").and_then(|x| x.as_u64()).unwrap_or(0),
            tpl.get("bits").and_then(|x| x.as_u64()).unwrap_or(0),
            tpl.get("work_id")
                .and_then(|x| x.as_str())
                .map(short_id)
                .unwrap_or("-"),
            tpl.get("tx_count").and_then(|x| x.as_u64()).unwrap_or(0),
            tpl.get("epoch").and_then(|x| x.as_u64()).unwrap_or(0),
            tpl.get("mem_mb").and_then(|x| x.as_u64()).unwrap_or(0),
        );
    }
    respond_json(request, tiny_http::StatusCode(200), tpl.to_string());
}

pub(crate) fn peek_work(work_id: &str) -> Option<WorkItem> {
    let mut map = work_map_lock();
    let now = now_ts();
    map.retain(|_, v| v.expires_at > now);
    let found = map.get(work_id).cloned();
    match &found {
        Some(item) => {
            wlog!(
                "[dutad] WORK_PEEK hit work={} scope={} height={} prev={} tx_count={} fees_total={} created_at={} expires_at={} map_size={}",
                short_id(work_id),
                short_scope(&item.work_scope),
                item.height,
                short_id(&item.prevhash32),
                item.tx_count,
                item.fees_total,
                item.created_at,
                item.expires_at,
                map.len()
            );
        }
        None => {
            wlog!(
                "[dutad] WORK_PEEK miss work={} map_size={}",
                short_id(work_id),
                map.len()
            );
        }
    }
    found
}

pub(crate) fn take_work(work_id: &str) -> Option<WorkItem> {
    let mut map = work_map_lock();
    let now = now_ts();
    map.retain(|_, v| v.expires_at > now);
    let removed = map.remove(work_id);
    match &removed {
        Some(item) => {
            wlog!(
                "[dutad] WORK_TAKE hit work={} scope={} height={} prev={} tx_count={} fees_total={} created_at={} expires_at={} map_size_after={}",
                short_id(work_id),
                short_scope(&item.work_scope),
                item.height,
                short_id(&item.prevhash32),
                item.tx_count,
                item.fees_total,
                item.created_at,
                item.expires_at,
                map.len()
            );
        }
        None => {
            wlog!(
                "[dutad] WORK_TAKE miss work={} map_size_after={}",
                short_id(work_id),
                map.len()
            );
        }
    }
    removed
}

#[cfg(test)]
pub(crate) fn insert_test_work(work_id: &str, item: WorkItem) {
    let mut map = work_map_lock();
    map.insert(work_id.to_string(), item);
}

#[cfg(test)]
mod tests {
    use super::{
        block_subsidy, filter_template_mempool_with_lookup, mining_address_is_valid,
        mining_address_is_valid_for_network, net_from_datadir, sanitize_mempool_value,
        same_tip_scope, stratum_work_scope, template_tx_for_block, materially_changed_template,
    };
    use crate::store;
    use duta_core::amount::DUT_PER_DUTA;
    use duta_core::netparams::{self, Network};
    use serde_json::json;

    #[test]
    fn net_from_datadir_prefers_meta_over_path_name() {
        let mut p = std::env::temp_dir();
        let uniq = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        p.push(format!("duta-work-net-meta-{}", uniq));
        std::fs::create_dir_all(&p).unwrap();
        let data_dir = p.to_string_lossy().to_string();
        store::ensure_datadir_meta(&data_dir, "testnet").unwrap();
        assert_eq!(net_from_datadir(&data_dir), Network::Testnet);
    }

    #[test]
    fn mining_address_validation_accepts_any_known_network_prefix() {
        assert!(mining_address_is_valid(
            "dut1111111111111111111111111111111111111111"
        ));
        assert!(mining_address_is_valid(
            "test1111111111111111111111111111111111111111"
        ));
        assert!(mining_address_is_valid(
            "stg1111111111111111111111111111111111111111"
        ));
        assert!(!mining_address_is_valid(
            "btc1111111111111111111111111111111111111111"
        ));
    }

    #[test]
    fn mining_address_validation_enforces_active_network_prefix() {
        assert!(mining_address_is_valid_for_network(
            Network::Testnet,
            "test1111111111111111111111111111111111111111"
        ));
        assert!(!mining_address_is_valid_for_network(
            Network::Testnet,
            "dut1111111111111111111111111111111111111111"
        ));
    }

    #[test]
    fn devfee_address_is_not_allowed_as_miner_address() {
        let mut p = std::env::temp_dir();
        let uniq = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        p.push(format!("duta-work-devfee-check-{}", uniq));
        std::fs::create_dir_all(&p).unwrap();
        let data_dir = p.to_string_lossy().to_string();
        store::ensure_datadir_meta(&data_dir, "testnet").unwrap();
        let err = super::build_work_template(
            &data_dir,
            netparams::DEVFEE_ADDRS_TESTNET[0],
            false,
            netparams::DEVFEE_ADDRS_TESTNET[0],
        )
        .unwrap_err();
        assert_eq!(err, "miner_address_conflicts_with_devfee");
    }

    #[test]
    fn stratum_work_scope_uses_worker_identity() {
        assert_eq!(
            stratum_work_scope("dut1wallet", "rig-a"),
            "dut1wallet#rig-a#work"
        );
        assert_eq!(stratum_work_scope("dut1wallet", "   "), "dut1wallet#work");
    }

    #[test]
    fn work_template_subsidy_uses_base_unit_scaling() {
        assert_eq!(block_subsidy(0), 50 * DUT_PER_DUTA);
        assert_eq!(block_subsidy(210_000), 25 * DUT_PER_DUTA);
    }

    #[test]
    fn sanitize_mempool_value_ignores_fee_and_size_for_txid_identity() {
        let tx = json!({
            "vin":[{"txid":"a","vout":0}],
            "vout":[{"address":"test1dest","value":1000}],
            "fee": 10000,
            "size": 123,
        });
        let canonical = crate::store::txid_from_value(&json!({
            "vin":[{"txid":"a","vout":0}],
            "vout":[{"address":"test1dest","value":1000}],
            "fee": 10000,
        }))
        .expect("canonical txid");
        let mp = json!({
            "txids": [canonical.clone()],
            "txs": {
                canonical.clone(): tx
            }
        });
        assert!(sanitize_mempool_value(&mp).is_none());
    }

    #[test]
    fn template_tx_for_block_preserves_fee_backed_txid() {
        let tx = json!({
            "vin":[{"txid":"a","vout":0}],
            "vout":[{"address":"test1dest","value":1_000_000}],
            "fee": 10_000,
            "size": 222
        });
        let txid = crate::store::txid_from_value(&json!({
            "vin":[{"txid":"a","vout":0}],
            "vout":[{"address":"test1dest","value":1_000_000}],
            "fee": 10_000
        }))
        .expect("txid");
        let embedded = template_tx_for_block(&tx);
        assert_eq!(embedded.get("fee").and_then(|v| v.as_u64()), Some(10_000));
        assert_eq!(
            crate::store::txid_from_value(&embedded).expect("embedded txid"),
            txid
        );
    }

    #[test]
    fn work_template_switches_to_pow_v4_after_activation() {
        let mut p = std::env::temp_dir();
        let uniq = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        p.push(format!("duta-work-pow-v4-{}", uniq));
        std::fs::create_dir_all(&p).unwrap();
        let data_dir = p.to_string_lossy().to_string();
        store::ensure_datadir_meta(&data_dir, "testnet").unwrap();
        store::bootstrap(&data_dir).unwrap();

        let tpl = super::build_work_template(
            &data_dir,
            "test1111111111111111111111111111111111111111",
            false,
            "test1111111111111111111111111111111111111111",
        )
        .unwrap();

        assert_eq!(tpl.get("height").and_then(|v| v.as_u64()), Some(1));
        assert_eq!(tpl.get("pow_version").and_then(|v| v.as_u64()), Some(4));
        assert_eq!(
            tpl.get("duta")
                .and_then(|v| v.get("algorithm"))
                .and_then(|v| v.as_str()),
            Some("duta-pow-v4")
        );
    }

    #[test]
    fn filter_template_mempool_promotes_parent_before_child_even_if_order_is_reversed() {
        let mp = json!({
            "txids": ["child", "parent"],
            "txs": {
                "child": {
                    "vin": [{"txid":"parent","vout":0}],
                    "vout": [{"address":"test1dest","value":900}],
                    "fee": 100
                },
                "parent": {
                    "vin": [{"txid":"chain","vout":0}],
                    "vout": [{"address":"test1dest","value":1000}],
                    "fee": 100
                }
            }
        });

        let filtered = filter_template_mempool_with_lookup(10, &mp, |txid, vout| {
            if txid == "chain" && vout == 0 {
                Some((2_000, 1, false, String::new()))
            } else {
                None
            }
        });

        let txids: Vec<String> = filtered
            .get("txids")
            .and_then(|v| v.as_array())
            .expect("txids")
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect();
        assert_eq!(txids, vec!["parent".to_string(), "child".to_string()]);
    }

    #[test]
    fn build_work_template_does_not_prune_mempool_file_on_invalid_template_tx() {
        let mut p = std::env::temp_dir();
        let uniq = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        p.push(format!("duta-work-mempool-preserve-{}", uniq));
        std::fs::create_dir_all(&p).unwrap();
        let data_dir = p.to_string_lossy().to_string();
        store::ensure_datadir_meta(&data_dir, "testnet").unwrap();
        store::bootstrap(&data_dir).unwrap();

        let invalid_tx = json!({
            "vin":[{"txid":"missing-parent","vout":0}],
            "vout":[{"address":"test1111111111111111111111111111111111111111","value":1000}],
            "fee": 10
        });
        let txid = crate::store::txid_from_value(&invalid_tx).unwrap();
        let mempool = json!({
            "txids":[txid.clone()],
            "txs": { txid.clone(): invalid_tx }
        });
        let mempool_path = format!("{}/mempool.json", data_dir);
        std::fs::write(&mempool_path, serde_json::to_vec(&mempool).unwrap()).unwrap();

        let tpl = super::build_work_template(
            &data_dir,
            "test1111111111111111111111111111111111111111",
            false,
            "test1111111111111111111111111111111111111111",
        )
        .unwrap();

        assert_eq!(tpl.get("tx_count").and_then(|v| v.as_u64()), Some(1));
        let stored_after: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&mempool_path).unwrap()).unwrap();
        assert_eq!(stored_after, mempool);
    }

    #[test]
    fn same_tip_scope_matches_older_scope_snapshot() {
        let older = super::WorkItem {
            created_at: 100,
            expires_at: 130,
            height: 10,
            pow_version: 4,
            prevhash32: "11".repeat(32),
            merkle32: "22".repeat(32),
            timestamp: 100,
            bits: 20,
            chainwork: 123,
            miner: "test1111111111111111111111111111111111111111".to_string(),
            work_scope: "test1111111111111111111111111111111111111111".to_string(),
            tx_count: 2,
            fees_total: 100,
            txs_obj: json!({}),
            header: [0u8; 80],
            anchor_hash32: "00".repeat(32),
            epoch: 0,
            mem_mb: 256,
        };
        let newer = super::WorkItem {
            created_at: 101,
            expires_at: 131,
            height: 10,
            pow_version: 4,
            prevhash32: "11".repeat(32),
            merkle32: "33".repeat(32),
            timestamp: 101,
            bits: 20,
            chainwork: 123,
            miner: "test1111111111111111111111111111111111111111".to_string(),
            work_scope: "test1111111111111111111111111111111111111111".to_string(),
            tx_count: 1,
            fees_total: 1,
            txs_obj: json!({}),
            header: [0u8; 80],
            anchor_hash32: "00".repeat(32),
            epoch: 0,
            mem_mb: 256,
        };
        assert!(same_tip_scope(&older, &newer));
        assert!(materially_changed_template(&older, &newer));
    }

    #[test]
    fn build_work_template_supersedes_older_same_tip_work_for_same_scope() {
        let mut p = std::env::temp_dir();
        let uniq = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        p.push(format!("duta-work-supersede-{}", uniq));
        std::fs::create_dir_all(&p).unwrap();
        let data_dir = p.to_string_lossy().to_string();
        store::ensure_datadir_meta(&data_dir, "testnet").unwrap();
        store::bootstrap(&data_dir).unwrap();

        let tx1 = json!({
            "vin":[],
            "vout":[{"address":"test1111111111111111111111111111111111111111","value":1000}],
            "fee": 10
        });
        let txid1 = crate::store::txid_from_value(&tx1).unwrap();
        let mempool1 = json!({
            "txids":[txid1.clone()],
            "txs": { txid1.clone(): tx1 }
        });
        let mempool_path = format!("{}/mempool.json", data_dir);
        std::fs::write(&mempool_path, serde_json::to_vec(&mempool1).unwrap()).unwrap();

        let tpl1 = super::build_work_template(
            &data_dir,
            "test1111111111111111111111111111111111111111",
            false,
            "test1111111111111111111111111111111111111111",
        )
        .unwrap();
        let work1 = tpl1.get("work_id").and_then(|v| v.as_str()).unwrap().to_string();
        assert!(super::peek_work(&work1).is_some());

        let tx2 = json!({
            "vin":[],
            "vout":[{"address":"test1111111111111111111111111111111111111111","value":2000}],
            "fee": 20
        });
        let txid2 = crate::store::txid_from_value(&tx2).unwrap();
        let mempool2 = json!({
            "txids":[txid1.clone(), txid2.clone()],
            "txs": {
                txid1.clone(): mempool1["txs"][txid1.clone()].clone(),
                txid2.clone(): tx2
            }
        });
        std::fs::write(&mempool_path, serde_json::to_vec(&mempool2).unwrap()).unwrap();

        let tpl2 = super::build_work_template(
            &data_dir,
            "test1111111111111111111111111111111111111111",
            false,
            "test1111111111111111111111111111111111111111",
        )
        .unwrap();
        let work2 = tpl2.get("work_id").and_then(|v| v.as_str()).unwrap().to_string();
        assert_ne!(work1, work2);
        assert!(super::peek_work(&work2).is_some());
        assert!(super::peek_work(&work1).is_none());
    }

    #[test]
    fn build_work_template_reuses_same_tip_scope_without_material_change() {
        let mut p = std::env::temp_dir();
        let uniq = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        p.push(format!("duta-work-single-active-{}", uniq));
        std::fs::create_dir_all(&p).unwrap();
        let data_dir = p.to_string_lossy().to_string();
        store::ensure_datadir_meta(&data_dir, "testnet").unwrap();
        store::bootstrap(&data_dir).unwrap();

        let tx = json!({
            "vin":[{"txid":"chain","vout":0}],
            "vout":[{"address":"test1111111111111111111111111111111111111111","value":1000}],
            "fee": 10
        });
        let txid = crate::store::txid_from_value(&tx).unwrap();
        let mempool = json!({
            "txids":[txid.clone()],
            "txs": { txid.clone(): tx }
        });
        let mempool_path = format!("{}/mempool.json", data_dir);
        std::fs::write(&mempool_path, serde_json::to_vec(&mempool).unwrap()).unwrap();

        let tpl1 = super::build_work_template(
            &data_dir,
            "test1111111111111111111111111111111111111111",
            false,
            "test1111111111111111111111111111111111111111",
        )
        .unwrap();
        let work1 = tpl1.get("work_id").and_then(|v| v.as_str()).unwrap().to_string();
        assert!(super::peek_work(&work1).is_some());

        std::thread::sleep(std::time::Duration::from_millis(5));

        let tpl2 = super::build_work_template(
            &data_dir,
            "test1111111111111111111111111111111111111111",
            false,
            "test1111111111111111111111111111111111111111",
        )
        .unwrap();
        let work2 = tpl2.get("work_id").and_then(|v| v.as_str()).unwrap().to_string();
        assert_eq!(work1, work2);
        assert!(super::peek_work(&work2).is_some());
        assert!(super::peek_work(&work1).is_some());
    }

    #[test]
    fn build_work_template_reuses_and_refreshes_expired_same_tip_scope_without_material_change() {
        let mut p = std::env::temp_dir();
        let uniq = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        p.push(format!("duta-work-reuse-expired-{}", uniq));
        std::fs::create_dir_all(&p).unwrap();
        let data_dir = p.to_string_lossy().to_string();
        store::ensure_datadir_meta(&data_dir, "testnet").unwrap();
        store::bootstrap(&data_dir).unwrap();

        let tx = json!({
            "vin":[{"txid":"chain","vout":0}],
            "vout":[{"address":"test1111111111111111111111111111111111111111","value":1000}],
            "fee": 10
        });
        let txid = crate::store::txid_from_value(&tx).unwrap();
        let mempool = json!({
            "txids":[txid.clone()],
            "txs": { txid.clone(): tx }
        });
        let mempool_path = format!("{}/mempool.json", data_dir);
        std::fs::write(&mempool_path, serde_json::to_vec(&mempool).unwrap()).unwrap();

        let tpl1 = super::build_work_template(
            &data_dir,
            "test1111111111111111111111111111111111111111",
            false,
            "test1111111111111111111111111111111111111111",
        )
        .unwrap();
        let work1 = tpl1.get("work_id").and_then(|v| v.as_str()).unwrap().to_string();
        let expires1 = tpl1.get("expires_at").and_then(|v| v.as_u64()).unwrap();

        let forced_expired = {
            let mut map = super::work_map_lock();
            let item = map.get_mut(&work1).unwrap();
            let forced_expired = item.created_at.saturating_sub(1);
            item.expires_at = forced_expired;
            forced_expired
        };

        let tpl2 = super::build_work_template(
            &data_dir,
            "test1111111111111111111111111111111111111111",
            false,
            "test1111111111111111111111111111111111111111",
        )
        .unwrap();
        let work2 = tpl2.get("work_id").and_then(|v| v.as_str()).unwrap().to_string();
        let expires2 = tpl2.get("expires_at").and_then(|v| v.as_u64()).unwrap();

        assert_eq!(work1, work2);
        assert!(expires2 > forced_expired);
        assert!(expires2 >= expires1);
        assert!(super::peek_work(&work1).is_some());
    }

    #[test]
    fn build_work_template_does_not_supersede_across_work_and_gbt_scopes() {
        let mut p = std::env::temp_dir();
        let uniq = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        p.push(format!("duta-work-source-split-{}", uniq));
        std::fs::create_dir_all(&p).unwrap();
        let data_dir = p.to_string_lossy().to_string();
        store::ensure_datadir_meta(&data_dir, "testnet").unwrap();
        store::bootstrap(&data_dir).unwrap();

        let tx = json!({
            "vin":[{"txid":"chain","vout":0}],
            "vout":[{"address":"test1111111111111111111111111111111111111111","value":1000}],
            "fee": 10
        });
        let txid = crate::store::txid_from_value(&tx).unwrap();
        let mempool = json!({
            "txids":[txid.clone()],
            "txs": { txid.clone(): tx }
        });
        let mempool_path = format!("{}/mempool.json", data_dir);
        std::fs::write(&mempool_path, serde_json::to_vec(&mempool).unwrap()).unwrap();

        let miner = "test1111111111111111111111111111111111111111";
        let work_tpl =
            super::build_work_template(&data_dir, miner, false, &super::daemon_work_scope(miner))
                .unwrap();
        let work_work_id = work_tpl
            .get("work_id")
            .and_then(|v| v.as_str())
            .unwrap()
            .to_string();
        assert!(super::peek_work(&work_work_id).is_some());

        let gbt_tpl =
            super::build_work_template(&data_dir, miner, false, &super::gbt_work_scope(miner))
                .unwrap();
        let gbt_work_id = gbt_tpl
            .get("work_id")
            .and_then(|v| v.as_str())
            .unwrap()
            .to_string();
        assert_ne!(work_work_id, gbt_work_id);
        assert!(super::peek_work(&work_work_id).is_some());
        assert!(super::peek_work(&gbt_work_id).is_some());
    }
}
