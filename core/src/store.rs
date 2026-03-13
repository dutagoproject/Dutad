use crate::ChainBlock;
use serde::{Deserialize, Serialize};
use serde_json::json;
use duta_core::netparams::{devfee_addrs, devfee_bps, genesis_hash, pow_launch_guard_enabled, pow_launch_guard_recent_span, pow_max_bits, pow_min_bits, pow_retarget_window, pow_start_bits, pow_target_secs, Network};
use duta_core::dutahash;
use duta_core::address;
use duta_core::types::H32;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

/// Maximum rollback depth we keep undo records for (also used as reorg depth limit).
pub const REORG_UNDO_WINDOW: u64 = 2000;

/// Maximum allowed future timestamp drift for a block (policy).
/// Chosen generous to avoid splits due to clock skew.
pub const MAX_FUTURE_DRIFT_SECS: u64 = 2 * 60 * 60;

/// Coinbase maturity (in blocks) enforced by consensus.
pub const COINBASE_MATURITY: u64 = 60;

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

/// On-disk data dir schema version marker.
///
/// Bump this only when making incompatible changes to the DB/data layout.
pub const DATA_DIR_SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Debug, Deserialize, Serialize)]
struct DataDirMeta {
    schema_version: u32,
    network: String,
    created_at: u64,
    app_version: String,
}

/// Ensure a stable, explicit migration marker exists in the data directory.
///
/// This helps prevent accidental reuse of a data dir across networks or
/// across incompatible schema versions.
pub fn ensure_datadir_meta(data_dir: &str, network: &str) -> Result<(), String> {
    fs::create_dir_all(data_dir)
        .map_err(|e| format!("data_dir_create_failed: {}", e))?;

    let meta_path = format!("{}/datadir_meta.json", data_dir.trim_end_matches('/'));
    if let Ok(s) = fs::read_to_string(&meta_path) {
        let m = serde_json::from_str::<DataDirMeta>(&s)
            .map_err(|e| format!("datadir_meta_invalid: {}", e))?;
        if m.schema_version > DATA_DIR_SCHEMA_VERSION {
            return Err(format!(
                "datadir_meta_schema_too_new: have={} supported={}",
                m.schema_version, DATA_DIR_SCHEMA_VERSION
            ));
        }
        if m.network != network {
            return Err(format!(
                "datadir_meta_network_mismatch: have={} expected={}",
                m.network, network
            ));
        }
        return Ok(());
    }

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let m = DataDirMeta {
        schema_version: DATA_DIR_SCHEMA_VERSION,
        network: network.to_string(),
        created_at: now,
        app_version: env!("CARGO_PKG_VERSION").to_string(),
    };
    let s = serde_json::to_string_pretty(&m)
        .map_err(|e| format!("datadir_meta_encode_failed: {}", e))?;
    fs::write(&meta_path, s)
        .map_err(|e| format!("datadir_meta_write_failed: {}", e))?;
    Ok(())
}

// Cached CPUCOIN_POW_V3 dataset (per-epoch) to avoid rebuilding it on every verify.
static POW_CACHE: OnceLock<Mutex<Option<(u64, H32, Arc<Vec<u8>>)>>> = OnceLock::new();

fn pow_cache() -> &'static Mutex<Option<(u64, H32, Arc<Vec<u8>>)>> {
    POW_CACHE.get_or_init(|| Mutex::new(None))
}

fn leading_zero_bits(d: &H32) -> u32 {
    let mut n: u32 = 0;
    for &b in d.as_bytes().iter() {
        if b == 0 {
            n += 8;
            continue;
        }
        n += b.leading_zeros();
        break;
    }
    n
}

fn header80_from_block(b: &ChainBlock) -> Result<[u8; 80], String> {
    let prev = b
        .prevhash32
        .as_deref()
        .ok_or_else(|| "prevhash_missing".to_string())?;
    let merkle = b
        .merkle32
        .as_deref()
        .ok_or_else(|| "merkle_missing".to_string())?;
    let ts = b.timestamp.ok_or_else(|| "timestamp_missing".to_string())?;
    let prev_h = H32::from_hex(prev).ok_or_else(|| "prevhash_invalid".to_string())?;
    let merkle_h = H32::from_hex(merkle).ok_or_else(|| "merkle_invalid".to_string())?;

    let mut out = [0u8; 80];
    out[0..32].copy_from_slice(prev_h.as_bytes());
    out[32..64].copy_from_slice(merkle_h.as_bytes());
    out[64..72].copy_from_slice(&ts.to_le_bytes());
    out[72..80].copy_from_slice(&b.bits.to_le_bytes());
    Ok(out)
}

fn anchor_hash32_from_tree(blocks: &sled::Tree, height: u64) -> H32 {
    let ah = dutahash::anchor_height(height);
    if ah == 0 {
        return H32::zero();
    }
    if let Some(b) = block_at_from_tree(blocks, ah) {
        if let Some(h) = H32::from_hex(&b.hash32) {
            return h;
        }
    }
    H32::zero()
}

fn dataset_for(height: u64, anchor_hash32: H32) -> Arc<Vec<u8>> {
    let epoch = dutahash::epoch_number(height);
    let mem_mb = dutahash::stage_mem_mb(height);

    // Fast path: cached epoch+anchor.
    if let Ok(mut g) = pow_cache().lock() {
        if let Some((ep, a, ds)) = g.as_ref() {
            if *ep == epoch && *a == anchor_hash32 {
                return Arc::clone(ds);
            }
        }
        let ds = Arc::new(dutahash::build_dataset_for_epoch(epoch, anchor_hash32, mem_mb));
        *g = Some((epoch, anchor_hash32, Arc::clone(&ds)));
        return ds;
    }

    // If poisoned, just rebuild without caching.
        Arc::new(dutahash::build_dataset_for_epoch(epoch, anchor_hash32, mem_mb))
}

pub fn work_from_bits(bits: u64) -> u64 {
    if bits >= 63 { u64::MAX } else { 1u64 << bits }
}

/// Return chainwork at a given height from DB (0 for height 0 / unknown).
pub fn chainwork_at_height(data_dir: &str, height: u64) -> Option<u64> {
    if height == 0 {
        return Some(0);
    }
    block_at(data_dir, height).map(|b| b.chainwork)
}

/// Compute candidate cumulative chainwork for a fork that diverges after `fork_point`.
///
/// `blocks` must be a contiguous sequence starting at `fork_point + 1` (validated in p2p),
/// but we don't trust any `chainwork` values provided by peers/miners.
pub fn compute_chainwork_for_candidate(
    data_dir: &str,
    fork_point: u64,
    blocks: &[ChainBlock],
) -> Option<u64> {
    let base = if fork_point == 0 {
        0u64
    } else {
        chainwork_at_height(data_dir, fork_point)?
    };
    let mut cw = base;
    for b in blocks {
        cw = cw.saturating_add(work_from_bits(b.bits));
    }
    Some(cw)
}


fn adjust_bits_capped(
    bits: u64,
    actual: u64,
    target: u64,
    min_bits: u64,
    max_bits: u64,
    max_up_bits: i64,
    max_down_bits: i64,
) -> u64 {
    // Stable retarget:
    // - clamp time delta to avoid extreme timestamp effects
    // - compute delta via log2(target/actual), capped
    let mut bits = bits.clamp(min_bits, max_bits);
    if target == 0 {
        return bits;
    }

    let min_a = (target / 4).max(1);
    let max_a = target.saturating_mul(4).max(min_a);
    let actual = actual.clamp(min_a, max_a);

    let ratio = (target as f64) / (actual as f64);

    if ratio > 1.0 {
        let delta = ratio.log2().floor() as i64;
        if delta > 0 {
            let delta = delta.min(max_up_bits.max(0)) as u64;
            bits = bits.saturating_add(delta);
        }
    } else if ratio < 1.0 {
        let delta = (1.0 / ratio).log2().floor() as i64;
        if delta > 0 {
            let delta = delta.min(max_down_bits.max(0)) as u64;
            bits = bits.saturating_sub(delta);
        }
    }

    bits.clamp(min_bits, max_bits)
}

fn adjust_bits_asymmetric(bits: u64, actual: u64, target: u64, min_bits: u64, max_bits: u64) -> u64 {
    adjust_bits_capped(bits, actual, target, min_bits, max_bits, 2, 1)
}

fn adjust_bits_normal(bits: u64, actual: u64, target: u64, min_bits: u64, max_bits: u64) -> u64 {
    adjust_bits_capped(bits, actual, target, min_bits, max_bits, 1, 1)
}

fn expected_bits_for_next_height(
    blocks: &sled::Tree,
    net: Network,
    next_height: u64,
) -> Result<u64, String> {
    if next_height <= 1 {
        return Ok(pow_start_bits(net));
    }

    let window = pow_retarget_window(net).max(1);
    let min_bits = pow_min_bits(net);
    let max_bits = pow_max_bits(net);

    let last = block_at_from_tree(blocks, next_height - 1)
        .ok_or_else(|| "prev_block_missing".to_string())?;
    let mut bits = last.bits;
    let launch_guard = pow_launch_guard_enabled(net, next_height, bits);

    // Special-case window=1: we need two distinct timestamps (prev block and its parent).
    // The generic anchor logic below would pick the same block for first/last and always
    // see actual=0, forcing monotonic difficulty changes.
    if window == 1 {
        if next_height <= 2 {
            return Ok(bits.clamp(min_bits, max_bits));
        }

        let first = block_at_from_tree(blocks, next_height - 2)
            .ok_or_else(|| "retarget_anchor_missing".to_string())?;

        let t_last = last.timestamp.unwrap_or(0);
        let t_first = first.timestamp.unwrap_or(0);
        if t_last <= t_first {
            // bad timestamps => make harder a bit (safe default)
            bits = bits.saturating_add(1);
            return Ok(bits.clamp(min_bits, max_bits));
        }

        let actual = t_last - t_first;
        let target = pow_target_secs(net);
        bits = if launch_guard {
            adjust_bits_asymmetric(bits, actual, target, min_bits, max_bits)
        } else {
            adjust_bits_normal(bits, actual, target, min_bits, max_bits)
        };
        return Ok(bits);
    }

    // Retarget only at window boundary.
    //
    // Emergency hardening: if blocks are coming in *much* faster than target,
    // allow difficulty to step up between boundaries.
    //
    // Rationale:
    // - Fresh/wiped networks (or sudden hashrate spikes) can produce extremely
    //   fast blocks while waiting for the next window boundary.
    // - We only ever *increase* difficulty in this path (never decrease), and
    //   we reuse the same asymmetric adjustment caps.
    if (next_height - 1) % window != 0 {
        // Launch guard is mainnet-only and temporary.
        // Keep testnet/stagenet easy for testing and only protect early mainnet from
        // runaway fast blocks or sudden solo-mining bumps.
        if launch_guard {
            let recent_span = pow_launch_guard_recent_span(net);
            if recent_span > 0 && next_height > recent_span + 1 {
                let span = recent_span.min(next_height.saturating_sub(1));
                let first_h = (next_height - 1).saturating_sub(span);

                if let Some(first) = block_at_from_tree(blocks, first_h) {
                    let t_last = last.timestamp.unwrap_or(0);
                    let t_first = first.timestamp.unwrap_or(0);
                    if t_last > t_first {
                        let actual = t_last - t_first;
                        let target = pow_target_secs(net).saturating_mul(span);

                        // Only bump early if blocks are coming in much too fast.
                        // This is intentionally softer than the old guard and auto-turns off.
                        if actual > 0 && target > 0 && actual < (target / 3).max(1) {
                            bits = adjust_bits_capped(bits, actual, target, min_bits, max_bits, 1, 0);
                        }
                    }
                }
            }
        }

        return Ok(bits.clamp(min_bits, max_bits));
    }

    // Use a window of `window` blocks ending at `next_height - 1`.
    // For example, when window=30 and next_height=31, we compare height 1..30.
    // This avoids requiring a genesis block to be present in the DB.
    let first_h = next_height.saturating_sub(window);
    let first = block_at_from_tree(blocks, first_h)
        .ok_or_else(|| "retarget_anchor_missing".to_string())?;

    let t_last = last.timestamp.unwrap_or(0);
    let t_first = first.timestamp.unwrap_or(0);
    if t_last <= t_first {
        // bad timestamps => make harder a bit (safe default)
        bits = bits.saturating_add(1);
        return Ok(bits.clamp(min_bits, max_bits));
    }

    let actual = t_last - t_first;
    let target = pow_target_secs(net).saturating_mul(window);

    bits = if launch_guard {
        adjust_bits_asymmetric(bits, actual, target, min_bits, max_bits)
    } else {
        adjust_bits_normal(bits, actual, target, min_bits, max_bits)
    };

    Ok(bits)
}

fn median_u64(mut v: Vec<u64>) -> u64 {
    if v.is_empty() {
        return 0;
    }
    v.sort_unstable();
    v[v.len() / 2]
}

/// Median Time Past (MTP) of the previous blocks, using up to the last 11 timestamps.
/// Missing timestamps are ignored (legacy compatibility).
fn median_time_past(blocks: &sled::Tree, prev_height: u64) -> u64 {
    let mut ts = Vec::with_capacity(11);
    // Walk backwards from prev_height.
    for i in 0..11u64 {
        let h = match prev_height.checked_sub(i) {
            Some(h) => h,
            None => break,
        };
        if let Some(b) = block_at_from_tree(blocks, h) {
            if let Some(t) = b.timestamp {
                ts.push(t);
            }
        }
    }
    median_u64(ts)
}

fn verify_timestamp_policy(blocks: &sled::Tree, b: &ChainBlock) -> Result<(), String> {
    if b.height == 0 {
        return Ok(());
    }

    let ts = b.timestamp.ok_or_else(|| "timestamp_missing".to_string())?;
    // Consensus-style anti time-warp: must be >= MTP of previous blocks.
    let mtp = median_time_past(blocks, b.height.saturating_sub(1));
    if ts < mtp {
        return Err(format!("timestamp_before_mtp mtp={} got={}", mtp, ts));
    }

    // Policy: avoid far-future blocks.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| "time_now_invalid".to_string())?
        .as_secs();
    if ts > now.saturating_add(MAX_FUTURE_DRIFT_SECS) {
        return Err(format!(
            "timestamp_too_far_future now={} max_drift={} got={}",
            now, MAX_FUTURE_DRIFT_SECS, ts
        ));
    }
    Ok(())
}

fn verify_pow_consensus(
    blocks: &sled::Tree,
    net: Network,
    b: &ChainBlock,
) -> Result<(), String> {
    if b.height == 0 {
        return Ok(());
    }
    let expected_bits = expected_bits_for_next_height(blocks, net, b.height)?;
    if b.bits != expected_bits {
        return Err(format!("bits_invalid expected={} got={}", expected_bits, b.bits));
    }

    let pow_hex = b
        .pow_digest32
        .as_deref()
        .ok_or_else(|| "pow_missing".to_string())?;
    let pow_h = H32::from_hex(pow_hex).ok_or_else(|| "pow_invalid".to_string())?;

    // Canonical: hash32 must equal pow digest.
    if b.hash32 != pow_hex {
        return Err("hash32_must_equal_pow".to_string());
    }

    let nonce = b.nonce.ok_or_else(|| "nonce_missing".to_string())?;
    let header80 = header80_from_block(b)?;
    let anchor = anchor_hash32_from_tree(blocks, b.height);
    let ds = dataset_for(b.height, anchor);
        let recomputed = dutahash::pow_digest(&header80, nonce, b.height, anchor, &ds);

    if recomputed != pow_h {
        return Err("pow_mismatch".to_string());
    }
    let lz = leading_zero_bits(&recomputed);
    if lz < (b.bits as u32) {
        return Err(format!("pow_low lz_bits={} need={}", lz, b.bits));
    }
    Ok(())
}

/// Open sled DB under {data_dir}/db (e.g. /opt/cpucoin/data/testnet/db)
static DB_CACHE: OnceLock<(String, sled::Db)> = OnceLock::new();
fn open_db(data_dir: &str) -> Result<sled::Db, String> {
    let p = format!("{}/db", data_dir.trim_end_matches('/'));

    // Reuse a single sled::Db handle within the process to avoid lock contention
    // when multiple async RPC handlers open the DB concurrently.
    if let Some((cached_p, db)) = DB_CACHE.get() {
        if cached_p == &p {
            return Ok(db.clone());
        }
    }

    // First open: Sled uses a file lock; on restart storms or concurrent opens we can hit WouldBlock.
    // Minimal retry to avoid spurious RPC failures under load.
    for attempt in 0..20u32 {
        match sled::open(&p) {
            Ok(db) => {
                // Best-effort cache; ignore if already set/raced.
                let _ = DB_CACHE.set((p.clone(), db.clone()));
                return Ok(db);
            }
            Err(e) => {
                let is_wouldblock = matches!(
                    &e,
                    sled::Error::Io(ioe) if ioe.kind() == std::io::ErrorKind::WouldBlock
                );
                if is_wouldblock && attempt < 19 {
                    std::thread::sleep(std::time::Duration::from_millis(25 * (attempt as u64 + 1)));
                    continue;
                }
                return Err(format!("db_open_failed: {}", e));
            }
        }
    }
    Err("db_open_failed: retry_exhausted".to_string())
}



fn tree_meta(db: &sled::Db) -> Result<sled::Tree, String> {
    db.open_tree(b"meta")
        .map_err(|e| format!("db_open_tree_failed: {}", e))
}
fn tree_blocks(db: &sled::Db) -> Result<sled::Tree, String> {
    db.open_tree(b"blocks")
        .map_err(|e| format!("db_open_tree_failed: {}", e))
}

fn tree_hash_index(db: &sled::Db) -> Result<sled::Tree, String> {
    db.open_tree(b"hash_index")
        .map_err(|e| format!("db_open_tree_failed: {}", e))
}

fn put_hash_index_db(hash_index: &sled::Tree, hash32: &str, height: u64) -> Result<(), String> {
    let k = hash32.as_bytes();
    let v = height.to_be_bytes();
    hash_index
        .insert(k, v.as_ref())
        .map_err(|e| format!("hash_index_insert_failed: {}", e))?;
    Ok(())
}

pub fn height_by_hash(data_dir: &str, hash32: &str) -> Option<u64> {
    let net = infer_network(data_dir);
    if hash32.eq_ignore_ascii_case(genesis_hash(net)) {
        return Some(0);
    }
    let db = open_db(data_dir).ok()?;
    let hash_index = tree_hash_index(&db).ok()?;
    let bytes = hash_index.get(hash32.as_bytes()).ok()??;
    if bytes.len() != 8 {
        return None;
    }
    let mut arr = [0u8; 8];
    arr.copy_from_slice(&bytes);
    Some(u64::from_be_bytes(arr))
}
fn tree_utxo(db: &sled::Db) -> Result<sled::Tree, String> {
    db.open_tree(b"utxo")
        .map_err(|e| format!("db_open_tree_failed: {}", e))
}
fn tree_undo(db: &sled::Db) -> Result<sled::Tree, String> {
    db.open_tree(b"undo")
        .map_err(|e| format!("db_open_tree_failed: {}", e))
}

fn tip_key() -> &'static [u8] {
    b"tip"
}

fn prune_below_key() -> &'static [u8] {
    b"prune_below"
}

pub fn prune_below(data_dir: &str) -> u64 {
    if let Ok(db) = open_db(data_dir) {
        if let Ok(meta) = tree_meta(&db) {
            if let Ok(Some(bytes)) = meta.get(prune_below_key()) {
                if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&bytes) {
                    if let Some(x) = v.get("prune_below").and_then(|x| x.as_u64()) {
                        return x;
                    }
                }
            }
        }
    }
    0
}

fn key_height(prefix: &str, height: u64) -> Vec<u8> {
    // zero-padded so lexicographic order == numeric order
    format!("{}{:016}", prefix, height).into_bytes()
}

fn block_key(height: u64) -> Vec<u8> {
    key_height("b:", height)
}
fn undo_key(height: u64) -> Vec<u8> {
    key_height("u:", height)
}

fn outpoint_key(txid: &str, vout: u64) -> Vec<u8> {
    format!("{}:{}", txid, vout).into_bytes()
}

fn set_tip_fields_db(meta: &sled::Tree, height: u64, hash32: String, chainwork: u64, bits: u64) -> Result<(), String> {
    let v = json!({
        "height": height,
        "hash32": hash32,
        "chainwork": chainwork,
        "bits": bits
    });
    let bytes = serde_json::to_vec(&v).map_err(|e| format!("tip_encode_failed: {}", e))?;
    meta.insert(tip_key(), bytes)
        .map_err(|e| format!("db_insert_failed: {}", e))?;
    Ok(())
}

fn put_block_db(blocks: &sled::Tree, b: &ChainBlock) -> Result<(), String> {
    let k = block_key(b.height);
    let v = serde_json::to_vec(b).map_err(|e| format!("block_encode_failed: {}", e))?;
    blocks
        .insert(k, v)
        .map_err(|e| format!("db_insert_failed: {}", e))?;
    Ok(())
}

fn parse_txs_from_block(b: &ChainBlock) -> Result<Vec<(String, serde_json::Value)>, String> {
    // Work off JSON representation to avoid coupling to internal tx types.
    let v = serde_json::to_value(b).map_err(|e| format!("block_to_value_failed: {}", e))?;
    let txs_obj = v
        .get("txs")
        .and_then(|x| x.as_object())
        .ok_or_else(|| "block_txs_missing".to_string())?;
    Ok(txs_obj
        .iter()
        .map(|(k, vv)| (k.clone(), vv.clone()))
        .collect())
}

fn infer_network(data_dir: &str) -> Network {
    let d = data_dir.to_ascii_lowercase();
    if d.contains("testnet") {
        Network::Testnet
    } else if d.contains("stagenet") {
        Network::Stagenet
    } else {
        Network::Mainnet
    }
}

/// Validate that the coinbase tx pays exactly the scheduled devfee share of the coinbase total
/// to one (or more) devfee addresses. Coinbase is identified as a tx with empty vin.
fn validate_coinbase_devfee(
    txs: &[(String, serde_json::Value)],
    net: Network,
    height: u64,
) -> Result<(), String> {
    let bps = devfee_bps(net, height) as u64;
    if bps == 0 {
        return Ok(());
    }
    let dev_addrs = devfee_addrs(net);

    // find coinbase tx (vin empty)
    let coinbase = txs
        .iter()
        .find(|(_, txv)| txv.get("vin").and_then(|x| x.as_array()).map(|a| a.is_empty()).unwrap_or(false))
        .ok_or_else(|| "coinbase_missing".to_string())?
        .1
        .clone();

    let vout = coinbase.get("vout").and_then(|x| x.as_array()).cloned().unwrap_or_default();
    if vout.is_empty() {
        return Err("coinbase_vout_missing".to_string());
    }

    let mut total: u64 = 0;
    let mut dev_total: u64 = 0;
    for o in &vout {
        let value = o.get("value").and_then(|x| x.as_u64()).unwrap_or(0);
        total = total.saturating_add(value);
        let addr = o.get("addr").and_then(|x| x.as_str()).unwrap_or("");
        if dev_addrs.iter().any(|a| *a == addr) {
            dev_total = dev_total.saturating_add(value);
        }
    }

    let expected = total.saturating_mul(bps) / 10_000;
    if dev_total != expected {
        return Err(format!("devfee_invalid expected={} got={}", expected, dev_total));
    }
    Ok(())
}

fn validate_coinbase_subsidy(
    utxo: &sled::Tree,
    height: u64,
    txs: &[(String, serde_json::Value)],
) -> Result<(), String> {
    let coinbase = txs
        .iter()
        .find(|(_, txv)| {
            txv.get("vin")
                .and_then(|x| x.as_array())
                .map(|a| a.is_empty())
                .unwrap_or(false)
        })
        .ok_or_else(|| "coinbase_missing".to_string())?
        .1
        .clone();

    let coinbase_vout = coinbase
        .get("vout")
        .and_then(|x| x.as_array())
        .cloned()
        .unwrap_or_default();
    let mut coinbase_total: u64 = 0;
    for ov in &coinbase_vout {
        let value = ov.get("value").and_then(|x| x.as_u64()).unwrap_or(0);
        coinbase_total = coinbase_total
            .checked_add(value)
            .ok_or_else(|| "coinbase_value_overflow".to_string())?;
    }

    let mut fees_total: u64 = 0;
    for (_txid, txv) in txs {
        let vin = txv
            .get("vin")
            .and_then(|x| x.as_array())
            .cloned()
            .unwrap_or_default();
        if vin.is_empty() {
            continue;
        }

        let vout = txv
            .get("vout")
            .and_then(|x| x.as_array())
            .cloned()
            .unwrap_or_default();

        let mut sum_in: u64 = 0;
        for inv in &vin {
            let prev_txid = inv.get("txid").and_then(|x| x.as_str()).unwrap_or("");
            let prev_vout = inv.get("vout").and_then(|x| x.as_u64()).unwrap_or(0);
            if prev_txid.is_empty() {
                return Err("tx_input_missing_prevout".to_string());
            }
            let prev_key = outpoint_key(prev_txid, prev_vout);
            let prev_bytes = utxo
                .get(&prev_key)
                .map_err(|e| format!("db_get_failed: {}", e))?
                .ok_or_else(|| format!("tx_input_missing {}:{}", prev_txid, prev_vout))?;
            let prev_json: serde_json::Value = serde_json::from_slice(&prev_bytes)
                .map_err(|e| format!("utxo_decode_failed: {}", e))?;
            let value = prev_json.get("value").and_then(|x| x.as_u64()).unwrap_or(0);
            sum_in = sum_in
                .checked_add(value)
                .ok_or_else(|| "tx_input_value_overflow".to_string())?;
        }

        let mut sum_out: u64 = 0;
        for ov in &vout {
            let value = ov.get("value").and_then(|x| x.as_u64()).unwrap_or(0);
            sum_out = sum_out
                .checked_add(value)
                .ok_or_else(|| "tx_output_value_overflow".to_string())?;
        }

        let fee = sum_in
            .checked_sub(sum_out)
            .ok_or_else(|| "tx_negative_fee".to_string())?;
        fees_total = fees_total
            .checked_add(fee)
            .ok_or_else(|| "block_fee_overflow".to_string())?;
    }

    let subsidy = block_subsidy(height);
    let max_coinbase = subsidy
        .checked_add(fees_total)
        .ok_or_else(|| "coinbase_value_overflow".to_string())?;
    if coinbase_total > max_coinbase {
        return Err(format!(
            "coinbase_overpay max={} got={} subsidy={} fees={}",
            max_coinbase, coinbase_total, subsidy, fees_total
        ));
    }
    Ok(())
}


/// Validate per-tx UTXO invariants against the current UTXO set (consensus).
/// Enforces:
/// - referenced inputs must exist
/// - no double-spend within the same block
/// - coinbase maturity (COINBASE_MATURITY)
/// - implicit fee non-negative: sum(inputs) >= sum(outputs) for non-coinbase txs
fn validate_utxo_invariants_for_block(
    utxo: &sled::Tree,
    height: u64,
    txs: &[(String, serde_json::Value)],
) -> Result<(), String> {
    let mut spent_in_block: HashSet<String> = HashSet::new();

    for (_txid, txv) in txs {
        let vin = txv.get("vin").and_then(|x| x.as_array()).cloned().unwrap_or_default();
        let is_coinbase = vin.is_empty();

        // Outputs sum (u64; reject on overflow by using checked_add).
        let vout = txv.get("vout").and_then(|x| x.as_array()).cloned().unwrap_or_default();
        let mut sum_out: u64 = 0;
        for ov in &vout {
            let value = ov
                .get("value")
                .and_then(|x| x.as_u64())
                .ok_or_else(|| "tx_output_value_missing".to_string())?;
            sum_out = sum_out
                .checked_add(value)
                .ok_or_else(|| "tx_output_value_overflow".to_string())?;
        }

        if is_coinbase {
            // Coinbase spends nothing; maturity enforced when it is spent by others.
            continue;
        }

        // Inputs: must exist, must not double-spend in this block, must satisfy maturity.
        let mut sum_in: u64 = 0;
        for inv in &vin {
            let prev_txid = inv.get("txid").and_then(|x| x.as_str()).unwrap_or("");
            let prev_vout = inv.get("vout").and_then(|x| x.as_u64()).unwrap_or(0);
            if prev_txid.is_empty() {
                return Err("tx_input_prev_missing".to_string());
            }
            let k_bytes = outpoint_key(prev_txid, prev_vout);
            let k = String::from_utf8_lossy(&k_bytes).to_string();

            // no double-spend within a block
            if !spent_in_block.insert(k.clone()) {
                return Err(format!("double_spend_in_block outpoint={}", k));
            }

            let prev_bytes = utxo
                .get(&k_bytes)
                .map_err(|e| format!("db_get_failed: {}", e))?
                .ok_or_else(|| format!("utxo_missing outpoint={}", k))?;

            let prev_json: serde_json::Value = serde_json::from_slice(&prev_bytes)
                .map_err(|e| format!("utxo_decode_failed: {}", e))?;

            let value = prev_json.get("value").and_then(|x| x.as_u64()).unwrap_or(0);
            let created_h = prev_json.get("created_height").and_then(|x| x.as_u64()).unwrap_or(0);
            let is_cb = prev_json.get("is_coinbase").and_then(|x| x.as_bool()).unwrap_or(false);

            if is_cb {
                // must be >= created_height + maturity
                if height < created_h.saturating_add(COINBASE_MATURITY) {
                    return Err(format!(
                        "coinbase_immature created_h={} need_min_h={} got_h={}",
                        created_h,
                        created_h.saturating_add(COINBASE_MATURITY),
                        height
                    ));
                }
            }

            sum_in = sum_in
                .checked_add(value)
                .ok_or_else(|| "tx_input_value_overflow".to_string())?;
        }

        if sum_in < sum_out {
            return Err(format!("fee_negative sum_in={} sum_out={}", sum_in, sum_out));
        }
    }

    Ok(())
}

/// Apply block to UTXO set and persist undo for rollback.
/// - removes spent outpoints in vin (if present in UTXO), records them in undo.spent
/// - creates new outpoints for each vout, records them in undo.created
fn apply_utxo_for_block(
    utxo: &sled::Tree,
    undo: &sled::Tree,
    height: u64,
    txs: &[(String, serde_json::Value)],
) -> Result<(), String> {
    let mut spent_map: HashMap<String, serde_json::Value> = HashMap::new();
    let mut created: Vec<String> = Vec::new();

    for (txid, txv) in txs {
        let vin = txv.get("vin").and_then(|x| x.as_array()).cloned().unwrap_or_default();
        let is_coinbase = vin.is_empty();

        // spends
        for inv in &vin {
            let prev_txid = inv.get("txid").and_then(|x| x.as_str()).unwrap_or("");
            let prev_vout = inv.get("vout").and_then(|x| x.as_u64()).unwrap_or(0);
            if prev_txid.is_empty() {
                continue;
            }
            let k = outpoint_key(prev_txid, prev_vout);
            if let Some(prev_bytes) = utxo.get(&k).map_err(|e| format!("db_get_failed: {}", e))? {
                // record for undo then delete
                let prev_json: serde_json::Value =
                    serde_json::from_slice(&prev_bytes).map_err(|e| format!("utxo_decode_failed: {}", e))?;
                spent_map.insert(String::from_utf8_lossy(&k).to_string(), prev_json);
                utxo.remove(&k).map_err(|e| format!("db_remove_failed: {}", e))?;
            }
        }

        // creates
        let vout = txv.get("vout").and_then(|x| x.as_array()).cloned().unwrap_or_default();
        for (i, ov) in vout.iter().enumerate() {
            let value = ov.get("value").and_then(|x| x.as_u64()).unwrap_or(0);
            let out_k = outpoint_key(txid, i as u64);
            let out_s = String::from_utf8_lossy(&out_k).to_string();
            // Accept both legacy "addr" and newer "address" output keys.
            let addr = ov
                .get("address")
                .and_then(|x| x.as_str())
                .or_else(|| ov.get("addr").and_then(|x| x.as_str()))
                .unwrap_or("");
let pkh = address::parse_address(addr)
    .ok_or_else(|| format!("invalid_address: {}", addr))?;
let out_v = json!({
    "value": value,
    "created_height": height,
    "is_coinbase": is_coinbase,
    "pkh": address::pkh_to_hex(&pkh)
});
            let out_bytes = serde_json::to_vec(&out_v).map_err(|e| format!("utxo_encode_failed: {}", e))?;
            utxo.insert(&out_k, out_bytes)
                .map_err(|e| format!("db_insert_failed: {}", e))?;
            created.push(out_s);
        }
    }

    let undo_v = json!({
        "spent": spent_map,
        "created": created
    });
    let undo_bytes = serde_json::to_vec(&undo_v).map_err(|e| format!("undo_encode_failed: {}", e))?;
    undo.insert(undo_key(height), undo_bytes)
        .map_err(|e| format!("db_insert_failed: {}", e))?;
    Ok(())
}

fn rollback_one(
    meta: &sled::Tree,
    blocks: &sled::Tree,
    utxo: &sled::Tree,
    undo: &sled::Tree,
) -> Result<(), String> {
    let (tip_h, _tip_hash, _tip_cw, _tip_bits) =
        tip_fields_from_tree(meta).unwrap_or((0, "0".repeat(64), 0, 0));
    if tip_h == 0 {
        return Ok(());
    }

    let undo_bytes = undo
        .get(undo_key(tip_h))
        .map_err(|e| format!("db_get_failed: {}", e))?
        .ok_or_else(|| "undo_missing".to_string())?;
    let u: serde_json::Value =
        serde_json::from_slice(&undo_bytes).map_err(|e| format!("undo_decode_failed: {}", e))?;

    // remove created outputs
    if let Some(arr) = u.get("created").and_then(|x| x.as_array()) {
        for s in arr {
            if let Some(kstr) = s.as_str() {
                utxo.remove(kstr.as_bytes())
                    .map_err(|e| format!("db_remove_failed: {}", e))?;
            }
        }
    }
    // restore spent outputs
    if let Some(obj) = u.get("spent").and_then(|x| x.as_object()) {
        for (kstr, v) in obj.iter() {
            let bytes = serde_json::to_vec(v).map_err(|e| format!("utxo_encode_failed: {}", e))?;
            utxo.insert(kstr.as_bytes(), bytes)
                .map_err(|e| format!("db_insert_failed: {}", e))?;
        }
    }

    // drop block + undo entry
    blocks
        .remove(block_key(tip_h))
        .map_err(|e| format!("db_remove_failed: {}", e))?;
    undo.remove(undo_key(tip_h))
        .map_err(|e| format!("db_remove_failed: {}", e))?;

    // set tip to previous block (if any)
    let prev_h = tip_h.saturating_sub(1);
    if prev_h == 0 {
        set_tip_fields_db(meta, 0, "0".repeat(64), 0, 0)?;
    } else if let Some(prev_b) = block_at_from_tree(blocks, prev_h) {
        set_tip_fields_db(meta, prev_b.height, prev_b.hash32, prev_b.chainwork, prev_b.bits)?;
    } else {
        // should not happen if DB consistent; fall back to zero
        set_tip_fields_db(meta, 0, "0".repeat(64), 0, 0)?;
    }
    Ok(())
}

/// Roll back tip down to target_height (inclusive).
/// Reorg depth is limited to available undo records.
pub fn rollback_to_height(data_dir: &str, target_height: u64) -> Result<(), String> {
    let db = open_db(data_dir)?;
    let meta = tree_meta(&db)?;
    let blocks = tree_blocks(&db)?;
    let utxo = tree_utxo(&db)?;
    let undo = tree_undo(&db)?;

    loop {
        let (h, _hash, _cw, _bits) = tip_fields_from_tree(&meta).unwrap_or((0, "0".repeat(64), 0, 0));
        if h <= target_height {
            break;
        }
        rollback_one(&meta, &blocks, &utxo, &undo)?;
    }

    // Keep legacy chain.json consistent (best-effort).
    let chain_path = format!("{}/chain.json", data_dir.trim_end_matches('/'));
    if let Ok(s) = fs::read_to_string(&chain_path) {
        if let Ok(mut chain) = serde_json::from_str::<Vec<ChainBlock>>(&s) {
            let keep_len = (target_height.saturating_add(1)) as usize;
            if chain.len() > keep_len {
                chain.truncate(keep_len);
                if let Ok(body) = serde_json::to_string(&chain) {
                    let tmp = format!("{}.tmp", chain_path);
                    let _ = fs::write(&tmp, body);
                    let _ = fs::rename(&tmp, &chain_path);
                }
            }
        }
    }

    db.flush().map_err(|e| format!("db_flush_failed: {}", e))?;
    Ok(())
}

fn tip_fields_from_tree(meta: &sled::Tree) -> Option<(u64, String, u64, u64)> {
    let bytes = meta.get(tip_key()).ok()??;
    let v: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
    let h = v.get("height").and_then(|x| x.as_u64()).unwrap_or(0);
    let hash32 = v
        .get("hash32")
        .and_then(|x| x.as_str())
        .unwrap_or("0000000000000000000000000000000000000000000000000000000000000000")
        .to_string();
    let cw = v.get("chainwork").and_then(|x| x.as_u64()).unwrap_or(0);
    let bits = v.get("bits").and_then(|x| x.as_u64()).unwrap_or(0);
    Some((h, hash32, cw, bits))
}

fn block_at_from_tree(blocks: &sled::Tree, height: u64) -> Option<ChainBlock> {
    let bytes = blocks.get(block_key(height)).ok()??;
    serde_json::from_slice::<ChainBlock>(&bytes).ok()
}



/// Find a block height by its hash32 hex string by scanning backwards from the current tip.
/// Returns Some(height) if found within max_depth, else None.
/// Special-case: the all-zero hash is treated as height 0 even if genesis is not stored in DB.

/// Prune block bodies + undo records older than (tip - keep).
/// NOTE: UTXO is kept fully; reorg depth is limited by available undo.
pub fn prune(data_dir: &str, keep: u64) -> Result<(), String> {
    let db = open_db(data_dir)?;
    let meta = tree_meta(&db)?;
    let blocks = tree_blocks(&db)?;
    let undo = tree_undo(&db)?;
    let (tip_h, _hash, _cw, _bits) = tip_fields_from_tree(&meta).unwrap_or((0, "0".repeat(64), 0, 0));
    if tip_h <= keep {
        return Ok(());
    }
    let prune_below = tip_h - keep;

    // Persist watermark so RPC can report pruned range deterministically.
    let pb_v = serde_json::to_vec(&json!({"prune_below": prune_below}))
        .map_err(|e| format!("prune_below_encode_failed: {}", e))?;
    meta.insert(prune_below_key(), pb_v)
        .map_err(|e| format!("db_insert_failed: {}", e))?;

    // delete blocks/undo below prune_below (exclusive): [0, prune_below)
    // Keep genesis(0) and blocks >= prune_below.
    for h in 1..prune_below {
        let _ = blocks.remove(block_key(h));
        let _ = undo.remove(undo_key(h));
    }
    db.flush().map_err(|e| format!("db_flush_failed: {}", e))?;

    // Best-effort: also prune legacy chain.json so /blocks_from fallback cannot serve pruned heights.
    // Keep genesis (index 0) and blocks >= prune_below.
    let chain_path = format!("{}/chain.json", data_dir.trim_end_matches('/'));
    if let Ok(s) = fs::read_to_string(&chain_path) {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&s) {
            if let Some(arr) = v.as_array() {
                let start = prune_below as usize;
                if arr.len() > start && !arr.is_empty() {
                    let mut new_arr: Vec<serde_json::Value> = Vec::with_capacity(1 + (arr.len() - start));
                    new_arr.push(arr[0].clone()); // genesis
                    for x in &arr[start..] {
                        new_arr.push(x.clone());
                    }
                    let tmp = format!("{}.tmp.{}", chain_path, std::process::id());
                    if let Ok(body) = serde_json::to_string_pretty(&new_arr) {
                        let _ = fs::write(&tmp, body);
                        let _ = fs::rename(&tmp, &chain_path);
                    }
                }
            }
        }
    }

    Ok(())
}

/// Bootstrap DB from chain.json if DB is empty (no tip key).
pub fn bootstrap(data_dir: &str) -> Result<(), String> {
    let db = open_db(data_dir)?;
    let meta = tree_meta(&db)?;
    let blocks = tree_blocks(&db)?;
    let hash_index = tree_hash_index(&db)?;
    let utxo = tree_utxo(&db)?;
    let undo = tree_undo(&db)?;

    if meta
        .get(tip_key())
        .map_err(|e| format!("db_get_failed: {}", e))?
        .is_some()
    {
        return Ok(());
    }

    let chain_path = format!("{}/chain.json", data_dir.trim_end_matches('/'));
    let s = match fs::read_to_string(&chain_path) {
        Ok(s) => s,
        Err(_) => {
            // no chain.json yet; initialize canonical genesis tip identity for this network
            let net = infer_network(data_dir);
            set_tip_fields_db(&meta, 0, genesis_hash(net).to_string(), 0, pow_start_bits(net))?;
            let _ = meta.insert(prune_below_key(), serde_json::to_vec(&json!({"prune_below":0})).unwrap_or_default());
            db.flush().map_err(|e| format!("db_flush_failed: {}", e))?;
            return Ok(());
        }
    };

    let chain: Vec<ChainBlock> =
        serde_json::from_str(&s).map_err(|e| format!("chain_json_invalid: {}", e))?;

    for b in &chain {
        put_block_db(&blocks, b)?;
        let _ = put_hash_index_db(&hash_index, &b.hash32, b.height);
        if let Ok(txs) = parse_txs_from_block(b) {
            // best-effort: build UTXO + undo for historical chain
            let _ = apply_utxo_for_block(&utxo, &undo, b.height, &txs);
        }
    }

    if let Some(tip) = chain.last() {
        set_tip_fields_db(&meta, tip.height, tip.hash32.clone(), tip.chainwork, tip.bits)?;
    } else {
        let net = infer_network(data_dir);
        set_tip_fields_db(&meta, 0, genesis_hash(net).to_string(), 0, pow_start_bits(net))?;
    }

    db.flush().map_err(|e| format!("db_flush_failed: {}", e))?;
    Ok(())
}

/// Best-effort record a newly accepted block into DB (keeps JSON compatibility).
/// This also updates the UTXO set + undo.
pub fn validate_candidate_block(data_dir: &str, b: &ChainBlock) -> Result<ChainBlock, String> {
let db = open_db(data_dir)?;
let meta = tree_meta(&db)?;
let blocks = tree_blocks(&db)?;
let utxo = tree_utxo(&db)?;

let net = infer_network(data_dir);

let (tip_h, tip_hash, tip_cw, _tip_bits) =
    tip_fields_from_tree(&meta).unwrap_or((0, "0".repeat(64), 0, 0));

if b.height == 0 {
    // genesis
} else {
    if b.height != tip_h.saturating_add(1) {
        return Err(format!("stale_or_out_of_order_block: height={} tip={}", b.height, tip_h));
    }
    let prev = b
        .prevhash32
        .as_deref()
        .ok_or_else(|| "missing_prevhash32".to_string())?;
    if prev != tip_hash {
        return Err(format!(
            "bad_prevhash: height={} prev={} tip_hash={}",
            b.height, prev, tip_hash
        ));
    }
}

let prev_cw = if b.height == 0 { 0 } else { tip_cw };
let computed_cw = prev_cw.saturating_add(work_from_bits(b.bits));

let mut b2 = b.clone();
b2.chainwork = computed_cw;

verify_timestamp_policy(&blocks, &b2)?;
verify_pow_consensus(&blocks, net, &b2)?;
if let Ok(txs) = parse_txs_from_block(&b2) {
    validate_coinbase_devfee(&txs, net, b2.height)?;
    validate_coinbase_subsidy(&utxo, b2.height, &txs)?;
    validate_utxo_invariants_for_block(&utxo, b2.height, &txs)?;
}
Ok(b2)
}

pub fn note_accepted_block(data_dir: &str, b: &ChainBlock) -> Result<(), String> {
let db = open_db(data_dir)?;
let meta = tree_meta(&db)?;
let blocks = tree_blocks(&db)?;
let hash_index = tree_hash_index(&db)?;
let utxo = tree_utxo(&db)?;
let undo = tree_undo(&db)?;

let net = infer_network(data_dir);

// Compute canonical cumulative chainwork; do NOT trust incoming b.chainwork (miner/peer provided).
let (tip_h, tip_hash, tip_cw, _tip_bits) =
    tip_fields_from_tree(&meta).unwrap_or((0, "0".repeat(64), 0, 0));

if b.height == 0 {
    // genesis
} else {
    // We only accept blocks that extend the current tip. Reorg must be handled by rollback + sequential apply.
    if b.height != tip_h.saturating_add(1) {
        return Err(format!("stale_or_out_of_order_block: height={} tip={}", b.height, tip_h));
    }
    let prev = b
        .prevhash32
        .as_deref()
        .ok_or_else(|| "missing_prevhash32".to_string())?;
    if prev != tip_hash {
        return Err(format!(
            "bad_prevhash: height={} prev={} tip_hash={}",
            b.height, prev, tip_hash
        ));
    }
}

let prev_cw = if b.height == 0 { 0 } else { tip_cw };
let computed_cw = prev_cw.saturating_add(work_from_bits(b.bits));

let mut b2 = b.clone();
b2.chainwork = computed_cw;

verify_timestamp_policy(&blocks, &b2)?;
verify_pow_consensus(&blocks, net, &b2)?;

put_block_db(&blocks, &b2)?;
put_hash_index_db(&hash_index, &b2.hash32, b2.height)?;
if let Ok(txs) = parse_txs_from_block(&b2) {
    validate_coinbase_devfee(&txs, net, b2.height)?;
    validate_coinbase_subsidy(&utxo, b2.height, &txs)?;
    validate_utxo_invariants_for_block(&utxo, b2.height, &txs)?;
    apply_utxo_for_block(&utxo, &undo, b2.height, &txs)?;
    // Best-effort: newly updated UTXO may satisfy some orphan txs
    let _ = crate::submit_tx::orphan_try_promote_file(data_dir);
}
set_tip_fields_db(&meta, b2.height, b2.hash32.clone(), b2.chainwork, b2.bits)?;
// prune body/undo beyond a safe window (reorg depth window)
let _ = prune(data_dir, REORG_UNDO_WINDOW);

db.flush().map_err(|e| format!("db_flush_failed: {}", e))?;
Ok(())
}

/// Tip fields from DB (if present), else None.
pub fn tip_fields(data_dir: &str) -> Option<(u64, String, u64, u64)> {
    let db = open_db(data_dir).ok()?;
    let meta = tree_meta(&db).ok()?;
    let net = infer_network(data_dir);
    let (height, hash32, chainwork, bits) = tip_fields_from_tree(&meta)?;
    if height == 0 && (hash32.is_empty() || hash32.chars().all(|c| c == '0')) {
        return Some((0, genesis_hash(net).to_string(), chainwork, pow_start_bits(net)));
    }
    Some((height, hash32, chainwork, bits))
}

/// Compute expected `bits` for the next block height (consensus).
pub fn expected_bits_next(data_dir: &str) -> Result<u64, String> {
    let db = open_db(data_dir)?;
    let blocks = tree_blocks(&db)?;
    let net = infer_network(data_dir);
    let (tip_h, _tip_hash, _tip_cw, _tip_bits) = tip_fields(data_dir).unwrap_or((0, "0".repeat(64), 0, 0));
    expected_bits_for_next_height(&blocks, net, tip_h + 1)
}

pub fn block_at(data_dir: &str, height: u64) -> Option<ChainBlock> {
    let db = open_db(data_dir).ok()?;
    let blocks = tree_blocks(&db).ok()?;
    block_at_from_tree(&blocks, height)
}

/// Lookup a UTXO outpoint in DB.
/// Returns (value, created_height, is_coinbase)
pub fn utxo_get(data_dir: &str, txid: &str, vout: u64) -> Option<(u64, u64, bool, String)> {
    let db = open_db(data_dir).ok()?;
    let utxo = tree_utxo(&db).ok()?;
    let bytes = utxo.get(outpoint_key(txid, vout)).ok()??;
    let v: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
    let value = v.get("value").and_then(|x| x.as_u64()).unwrap_or(0);
    let ch = v.get("created_height").and_then(|x| x.as_u64()).unwrap_or(0);
    let cb = v.get("is_coinbase").and_then(|x| x.as_bool()).unwrap_or(false);
    let pkh = v.get("pkh").and_then(|x| x.as_str()).unwrap_or("").to_string();
    Some((value, ch, cb, pkh))
}


#[cfg(test)]
mod tests_a5 {
    use super::*;
    use serde_json::json;

    fn tmp_tree(name: &str) -> sled::Tree {
        let db = sled::Config::new().temporary(true).open().unwrap();
        db.open_tree(name).unwrap()
    }

    fn put_utxo(utxo: &sled::Tree, txid: &str, vout: u64, value: u64, created_height: u64, is_coinbase: bool) {
        let k = outpoint_key(txid, vout);
        let v = json!({"value": value, "created_height": created_height, "is_coinbase": is_coinbase});
        utxo.insert(k, serde_json::to_vec(&v).unwrap()).unwrap();
    }

    #[test]
    fn a5_reject_missing_input_utxo() {
        let utxo = tmp_tree("utxo");
        let txs = vec![("tx1".to_string(), json!({"vin":[{"txid":"nope","vout":0}], "vout":[{"value":1}]}))];
        let err = validate_utxo_invariants_for_block(&utxo, 10, &txs).unwrap_err();
        assert!(err.contains("utxo_missing"));
    }

    #[test]
    fn a5_reject_double_spend_in_block() {
        let utxo = tmp_tree("utxo");
        put_utxo(&utxo, "a", 0, 5, 1, false);
        let txs = vec![(
            "tx1".to_string(),
            json!({"vin":[{"txid":"a","vout":0},{"txid":"a","vout":0}], "vout":[{"value":1}]}),
        )];
        let err = validate_utxo_invariants_for_block(&utxo, 10, &txs).unwrap_err();
        assert!(err.contains("double_spend_in_block"));
    }

    #[test]
    fn a5_reject_immature_coinbase_spend() {
        let utxo = tmp_tree("utxo");
        put_utxo(&utxo, "cb", 0, 50, 1, true);
        // height 10, need >= 61
        let txs = vec![("tx1".to_string(), json!({"vin":[{"txid":"cb","vout":0}], "vout":[{"value":1}]}))];
        let err = validate_utxo_invariants_for_block(&utxo, 10, &txs).unwrap_err();
        assert!(err.contains("coinbase_immature") || err.contains("immature"));
    }

    #[test]
    fn a5_reject_coinbase_overpay_after_halving_height() {
        let utxo = tmp_tree("utxo");
        let txs = vec![(
            "cb".to_string(),
            json!({
                "vin": [],
                "vout": [
                    {"addr":"dut34eb9f0d0d9e0ec7fbf78e6bfc7277f10f01aaf9","value": 5},
                    {"addr":"dut34eb9f0d0d9e0ec7fbf78e6bfc7277f10f01aaf8","value": 21}
                ]
            }),
        )];
        let err = validate_coinbase_subsidy(&utxo, 210_000, &txs).unwrap_err();
        assert!(err.contains("coinbase_overpay"));
    }

    #[test]
    fn a5_reject_negative_fee() {
        let utxo = tmp_tree("utxo");
        put_utxo(&utxo, "a", 0, 5, 1, false);
        let txs = vec![("tx1".to_string(), json!({"vin":[{"txid":"a","vout":0}], "vout":[{"value":6}]}))];
        let err = validate_utxo_invariants_for_block(&utxo, 10, &txs).unwrap_err();
        assert!(err.contains("fee_negative"));
    }
}
