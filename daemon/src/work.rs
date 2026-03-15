use crate::p2p;
use crate::store;
use duta_core::address;
use duta_core::dutahash;
use duta_core::netparams::{self, Network};
use duta_core::types::H32;
use serde_json::json;
use std::collections::HashMap;
use std::fs;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};

#[derive(Clone, Debug)]
pub(crate) struct WorkItem {
    pub expires_at: u64,
    pub height: u64,
    pub prevhash32: String,
    pub merkle32: String,
    pub timestamp: u64,
    pub bits: u64,
    pub chainwork: u64,
    pub miner: String,
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
    const INITIAL_BLOCK_REWARD: u64 = 50;
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
            return v;
        }
    }
    json!({"txids": [], "txs": {}})
}

fn short_id(id: &str) -> &str {
    if id.len() <= 8 {
        id
    } else {
        &id[..8]
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
) -> Result<serde_json::Value, String> {
    let miner = miner.trim();
    if miner.is_empty() {
        return Err("missing_address".to_string());
    }
    let net = net_from_datadir(data_dir);
    if !mining_address_is_valid_for_network(net, miner) {
        return Err("invalid_address".to_string());
    }
    let dev_addr = netparams::devfee_addrs(net)[0];
    if miner == dev_addr {
        return Err("miner_address_conflicts_with_devfee".to_string());
    }

    let mp = read_mempool_value(data_dir);
    let mut txids = mp
        .get("txids")
        .and_then(|x| x.as_array())
        .cloned()
        .unwrap_or_default();
    let (tip_h, tip_hash32, _tip_bits, tip_chainwork) =
        store::tip_fields(data_dir).unwrap_or((0, "0".repeat(64), 0, 0));

    let best_h = p2p::best_seen_height();
    if best_h > tip_h.saturating_add(1) {
        return Err(format!(
            "syncing tip_height={} best_seen_height={}",
            tip_h, best_h
        ));
    }

    let height = tip_h + 1;
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
        obj.insert(coinbase_txid.clone(), coinbase_tx.clone());
        obj.insert("__order".to_string(), serde_json::Value::Array(txids.clone()));
    }

    let ordered_txids: Vec<String> = txids
        .iter()
        .filter_map(|t| t.as_str().map(|s| s.to_string()))
        .collect();
    let merkle32 = store::merkle32_from_txids(&ordered_txids)?;
    let header = header80(&prevhash32, &merkle32, ts, bits)?;

    let anchor_h = dutahash::anchor_height(height);
    let anchor_hash32 = if anchor_h == 0 {
        H32::zero()
    } else {
        store::block_at(data_dir, anchor_h)
            .and_then(|b| H32::from_hex(&b.hash32))
            .unwrap_or_else(H32::zero)
    };

    let epoch = dutahash::epoch_number(height);
    let mem_mb = dutahash::stage_mem_mb(height);
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
        expires_at,
        height,
        prevhash32: prevhash32.clone(),
        merkle32: merkle32.clone(),
        timestamp: ts,
        bits,
        chainwork,
        miner: miner.to_string(),
        txs_obj: txs_obj.clone(),
        header,
        anchor_hash32: anchor_hash32.to_hex(),
        epoch,
        mem_mb,
    };

    {
        let mut map = work_map_lock();
        map.retain(|_, v| v.expires_at > created_at);
        if map.len() >= MAX_OUTSTANDING_WORK_TOTAL {
            return Err("busy".to_string());
        }
        let per_miner = map.values().filter(|v| v.miner == miner).count();
        if per_miner >= MAX_OUTSTANDING_WORK_PER_MINER {
            return Err("too_many_outstanding_work".to_string());
        }
        map.insert(work_id.clone(), item);
    }

    Ok(json!({
        "work_id": work_id,
        "workid": work_id,
        "expires_at": expires_at,
        "height": height,
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
            "algorithm": "duta-pow-v3",
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

    let tpl = match build_work_template(data_dir, &miner) {
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
    map.get(work_id).cloned()
}

pub(crate) fn take_work(work_id: &str) -> Option<WorkItem> {
    let mut map = work_map_lock();
    let now = now_ts();
    map.retain(|_, v| v.expires_at > now);
    map.remove(work_id)
}

#[cfg(test)]
pub(crate) fn insert_test_work(work_id: &str, item: WorkItem) {
    let mut map = work_map_lock();
    map.insert(work_id.to_string(), item);
}

#[cfg(test)]
mod tests {
    use super::{mining_address_is_valid, mining_address_is_valid_for_network, net_from_datadir};
    use crate::store;
    use duta_core::netparams::{self, Network};

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
        assert!(!mining_address_is_valid("btc1111111111111111111111111111111111111111"));
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
        p.push(format!("duta-work-devfee-guard-{}", uniq));
        std::fs::create_dir_all(&p).unwrap();
        let data_dir = p.to_string_lossy().to_string();
        store::ensure_datadir_meta(&data_dir, "testnet").unwrap();
        let err =
            super::build_work_template(&data_dir, netparams::DEVFEE_ADDRS_TESTNET[0]).unwrap_err();
        assert_eq!(err, "miner_address_conflicts_with_devfee");
    }
}
