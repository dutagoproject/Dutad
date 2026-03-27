use crate::canon_json;
use crate::ChainBlock;
use duta_core::address;
use duta_core::amount::DUT_PER_DUTA;
use duta_core::dutahash;
use duta_core::hash;
use duta_core::netparams;
use duta_core::netparams::{
    devfee_addrs, devfee_bps, genesis_hash, pow_bootstrap_sync_recent_span,
    pow_launch_difficulty_hardening_enabled, pow_mandatory_recovery_bits,
    pow_mandatory_recovery_height, pow_max_bits, pow_min_bits, pow_retarget_window,
    pow_start_bits, pow_target_secs, Network,
};
use duta_core::types::H32;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
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
    const INITIAL_BLOCK_REWARD: u64 = 50 * DUT_PER_DUTA;
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

use std::sync::atomic::{AtomicU64, Ordering};

static DURABLE_WRITE_SEQ: OnceLock<AtomicU64> = OnceLock::new();

pub fn durable_write_bytes(path: &str, body: &[u8]) -> Result<(), String> {
    let path_ref = std::path::Path::new(path);
    if let Some(parent) = path_ref.parent() {
        fs::create_dir_all(parent).map_err(|e| format!("dir_create_failed: {}", e))?;
    }

    let seq = DURABLE_WRITE_SEQ
        .get_or_init(|| AtomicU64::new(0))
        .fetch_add(1, Ordering::Relaxed);
    let now_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let tmp = format!("{}.tmp.{}.{}.{}", path, std::process::id(), now_nanos, seq);
    let cleanup_tmp = |tmp: &str| {
        let _ = fs::remove_file(tmp);
    };
    let mut f = fs::File::create(&tmp).map_err(|e| format!("tmp_create_failed: {}", e))?;
    if let Err(e) = f.write_all(body) {
        drop(f);
        cleanup_tmp(&tmp);
        return Err(format!("tmp_write_failed: {}", e));
    }
    if let Err(e) = f.sync_all() {
        drop(f);
        cleanup_tmp(&tmp);
        return Err(format!("tmp_sync_failed: {}", e));
    }
    drop(f);

    #[cfg(unix)]
    {
        if let Err(rename_err) = fs::rename(&tmp, path) {
            cleanup_tmp(&tmp);
            return Err(format!("rename_failed: {}", rename_err));
        }
    }

    #[cfg(not(unix))]
    {
        match fs::rename(&tmp, path) {
            Ok(()) => {}
            Err(rename_err) => {
                if path_ref.exists() {
                    if let Err(e) = fs::remove_file(path) {
                        cleanup_tmp(&tmp);
                        return Err(format!("replace_remove_failed: {}", e));
                    }
                    if let Err(e) = fs::rename(&tmp, path) {
                        cleanup_tmp(&tmp);
                        return Err(format!("rename_failed: {} (initial={})", e, rename_err));
                    }
                } else {
                    cleanup_tmp(&tmp);
                    return Err(format!("rename_failed: {}", rename_err));
                }
            }
        }
    }

    if let Some(parent) = path_ref.parent() {
        if let Ok(df) = fs::File::open(parent) {
            let _ = df.sync_all();
        }
    }
    Ok(())
}

pub fn durable_write_string(path: &str, body: &str) -> Result<(), String> {
    durable_write_bytes(path, body.as_bytes())
}

/// Ensure a stable, explicit migration marker exists in the data directory.
///
/// This helps prevent accidental reuse of a data dir across networks or
/// across incompatible schema versions.
pub fn ensure_datadir_meta(data_dir: &str, network: &str) -> Result<(), String> {
    fs::create_dir_all(data_dir).map_err(|e| format!("data_dir_create_failed: {}", e))?;

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
    durable_write_string(&meta_path, &s)
        .map_err(|e| format!("datadir_meta_write_failed: {}", e))?;
    Ok(())
}

pub fn read_datadir_network(data_dir: &str) -> Option<duta_core::netparams::Network> {
    let meta_path = format!("{}/datadir_meta.json", data_dir.trim_end_matches('/'));
    let s = fs::read_to_string(&meta_path).ok()?;
    let meta = serde_json::from_str::<DataDirMeta>(&s).ok()?;
    duta_core::netparams::Network::parse_name(&meta.network)
}

// Cached CPUCOIN_POW_V3 dataset (per-epoch) to avoid rebuilding it on every verify.
static POW_CACHE: OnceLock<Mutex<Option<(u8, u64, H32, usize, Arc<Vec<u8>>)>>> = OnceLock::new();

fn pow_cache() -> &'static Mutex<Option<(u8, u64, H32, usize, Arc<Vec<u8>>)>> {
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

pub fn txid_from_value(v: &serde_json::Value) -> Result<String, String> {
    let mut tx = v.clone();
    if let Some(obj) = tx.as_object_mut() {
        for key in [
            "size",
            "txid",
            "hash",
            "hex",
            "vsize",
            "weight",
            "confirmations",
            "blockhash",
            "height",
            "in_active_chain",
            "time",
            "timereceived",
        ] {
            obj.remove(key);
        }
    }
    let body = canon_json::canonical_json_bytes(&tx)?;
    Ok(hash::sha3_256_hex(&body))
}

pub fn merkle32_from_txids(txids: &[String]) -> Result<String, String> {
    if txids.is_empty() {
        return Err("merkle_empty".to_string());
    }

    let mut level: Vec<[u8; 32]> = txids
        .iter()
        .map(|txid| {
            let raw = hex::decode(txid).map_err(|_| format!("txid_invalid: {}", txid))?;
            if raw.len() != 32 {
                return Err(format!("txid_len_invalid: {}", txid));
            }
            let mut out = [0u8; 32];
            out.copy_from_slice(&raw);
            Ok(out)
        })
        .collect::<Result<Vec<_>, String>>()?;

    while level.len() > 1 {
        let mut next: Vec<[u8; 32]> = Vec::with_capacity((level.len() + 1) / 2);
        let mut i = 0usize;
        while i < level.len() {
            let left = level[i];
            let right = if i + 1 < level.len() {
                level[i + 1]
            } else {
                level[i]
            };
            let mut data = Vec::with_capacity(64);
            data.extend_from_slice(&left);
            data.extend_from_slice(&right);
            next.push(hash::sha3_256(&data).0);
            i += 2;
        }
        level = next;
    }

    Ok(hex::encode(level[0]))
}

fn anchor_hash32_from_tree(blocks: &sled::Tree, pow_version: u8, height: u64) -> H32 {
    let ah = dutahash::anchor_height_for_version(pow_version, height);
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

fn dataset_for(pow_version: u8, height: u64, anchor_hash32: H32) -> Arc<Vec<u8>> {
    let epoch = dutahash::epoch_number_for_version(pow_version, height);
    let mem_mb = dutahash::stage_mem_mb_for_version(pow_version, height);

    // Fast path: cached epoch+anchor.
    if let Ok(mut g) = pow_cache().lock() {
        if let Some((ver, ep, a, mem, ds)) = g.as_ref() {
            if *ver == pow_version && *ep == epoch && *a == anchor_hash32 && *mem == mem_mb {
                return Arc::clone(ds);
            }
        }
        let ds = Arc::new(dutahash::build_dataset_for_version(
            pow_version,
            epoch,
            anchor_hash32,
            mem_mb,
        ));
        *g = Some((pow_version, epoch, anchor_hash32, mem_mb, Arc::clone(&ds)));
        return ds;
    }

    // If poisoned, just rebuild without caching.
    Arc::new(dutahash::build_dataset_for_version(
        pow_version,
        epoch,
        anchor_hash32,
        mem_mb,
    ))
}

pub(crate) fn prewarm_pow_dataset(pow_version: u8, height: u64, anchor_hash32: H32) {
    let _ = dataset_for(pow_version, height, anchor_hash32);
}

pub fn work_from_bits(bits: u64) -> u64 {
    if bits >= 63 {
        u64::MAX
    } else {
        1u64 << bits
    }
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

fn adjust_bits_asymmetric(
    bits: u64,
    actual: u64,
    target: u64,
    min_bits: u64,
    max_bits: u64,
) -> u64 {
    adjust_bits_capped(bits, actual, target, min_bits, max_bits, 2, 1)
}

fn adjust_bits_normal(bits: u64, actual: u64, target: u64, min_bits: u64, max_bits: u64) -> u64 {
    adjust_bits_capped(bits, actual, target, min_bits, max_bits, 1, 1)
}

fn apply_normalization_stage(
    current_bits: u64,
    computed_bits: u64,
    stage_floor: Option<u64>,
    min_bits: u64,
    max_bits: u64,
) -> u64 {
    let computed_bits = computed_bits.clamp(min_bits, max_bits);
    match stage_floor {
        Some(stage_floor) => {
            let stage_floor = stage_floor.clamp(min_bits, max_bits);
            if computed_bits < current_bits {
                computed_bits
            } else {
                computed_bits.max(stage_floor)
            }
        }
        None => computed_bits,
    }
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
    let current_bits = last.bits;
    let mut bits = current_bits;
    let mut normalization_stage_floor = None;
    if let (Some(recovery_h), Some(recovery_bits)) = (
        pow_mandatory_recovery_height(net),
        pow_mandatory_recovery_bits(net),
    ) {
        let recovery_window = duta_core::netparams::pow_mandatory_recovery_window(net);
        let recovery_end = recovery_h.saturating_add(recovery_window);
        if next_height >= recovery_h && next_height < recovery_end {
            let stage_bits = if next_height < recovery_h + 5 {
                recovery_bits
            } else if next_height < recovery_h + 10 {
                recovery_bits + 1
            } else if next_height < recovery_h + 15 {
                recovery_bits + 2
            } else {
                recovery_bits + 3
            };
            if next_height == recovery_h {
                return Ok(stage_bits.clamp(min_bits, max_bits));
            }
            normalization_stage_floor = Some(stage_bits);
        }
    }
    let sync_gate = pow_launch_difficulty_hardening_enabled(net, next_height, bits);

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
            return Ok(apply_normalization_stage(
                current_bits,
                bits,
                normalization_stage_floor,
                min_bits,
                max_bits,
            ));
        }

        let actual = t_last - t_first;
        let target = pow_target_secs(net);
        bits = if sync_gate {
            adjust_bits_asymmetric(bits, actual, target, min_bits, max_bits)
        } else {
            adjust_bits_normal(bits, actual, target, min_bits, max_bits)
        };
        return Ok(apply_normalization_stage(
            current_bits,
            bits,
            normalization_stage_floor,
            min_bits,
            max_bits,
        ));
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
        // Bootstrap sync hardening is mainnet-only and temporary.
        // Keep testnet/stagenet easy for testing and only protect early mainnet from
        // runaway fast blocks or sudden solo-mining bumps.
        if sync_gate {
            let recent_span = pow_bootstrap_sync_recent_span(net);
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
                        // Push harder toward the bootstrap target bits so solo rushes do not keep
                        // the chain unrealistically easy while nodes are still converging.
                        if actual > 0 && target > 0 && actual < (target / 3).max(1) {
                            bits = adjust_bits_capped(
                                bits,
                                actual,
                                target,
                                min_bits,
                                netparams::pow_bootstrap_sync_target_bits(net).min(max_bits),
                                2,
                                0,
                            );
                        }
                    }
                }
            }
        }

        return Ok(apply_normalization_stage(
            current_bits,
            bits,
            normalization_stage_floor,
            min_bits,
            max_bits,
        ));
    }

    // Use a window of `window` blocks ending at `next_height - 1`.
    // For example, when window=30 and next_height=31, we compare height 1..30.
    // This avoids requiring a genesis block to be present in the DB.
    let first_h = next_height.saturating_sub(window);
    let first =
        block_at_from_tree(blocks, first_h).ok_or_else(|| "retarget_anchor_missing".to_string())?;

    let t_last = last.timestamp.unwrap_or(0);
    let t_first = first.timestamp.unwrap_or(0);
    if t_last <= t_first {
        // bad timestamps => make harder a bit (safe default)
        bits = bits.saturating_add(1);
        return Ok(apply_normalization_stage(
            current_bits,
            bits,
            normalization_stage_floor,
            min_bits,
            max_bits,
        ));
    }

    let actual = t_last - t_first;
    let target = pow_target_secs(net).saturating_mul(window);

    bits = if sync_gate {
        adjust_bits_asymmetric(bits, actual, target, min_bits, max_bits)
    } else {
        adjust_bits_normal(bits, actual, target, min_bits, max_bits)
    };

    Ok(apply_normalization_stage(
        current_bits,
        bits,
        normalization_stage_floor,
        min_bits,
        max_bits,
    ))
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

fn verify_pow_consensus(blocks: &sled::Tree, net: Network, b: &ChainBlock) -> Result<(), String> {
    if b.height == 0 {
        return Ok(());
    }
    let pow_version = netparams::pow_consensus_version(net, b.height);
    let expected_bits = expected_bits_for_next_height(blocks, net, b.height)?;
    if b.bits != expected_bits {
        return Err(format!(
            "bits_invalid expected={} got={}",
            expected_bits, b.bits
        ));
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
    let anchor = anchor_hash32_from_tree(blocks, pow_version, b.height);
    let ds = dataset_for(pow_version, b.height, anchor);
    let recomputed =
        dutahash::pow_digest_for_version(pow_version, &header80, nonce, b.height, anchor, &ds);

    let lz = leading_zero_bits(&recomputed);
    let legacy = if pow_version == dutahash::POW_VERSION_V4 && recomputed != pow_h {
        let legacy_anchor = anchor_hash32_from_tree(blocks, dutahash::POW_VERSION_V3, b.height);
        let legacy_ds = dataset_for(dutahash::POW_VERSION_V3, b.height, legacy_anchor);
        Some(dutahash::pow_digest_for_version(
            dutahash::POW_VERSION_V3,
            &header80,
            nonce,
            b.height,
            legacy_anchor,
            &legacy_ds,
        ))
    } else {
        None
    };

    classify_pow_outcome(pow_version, b.bits, pow_h, recomputed, lz, legacy)?;
    Ok(())
}

fn classify_pow_outcome(
    pow_version: u8,
    bits: u64,
    provided: H32,
    canonical: H32,
    canonical_lz: u32,
    legacy: Option<H32>,
) -> Result<(), String> {
    if canonical != provided {
        if pow_version == dutahash::POW_VERSION_V4 {
            if let Some(legacy) = legacy {
                if legacy == provided && leading_zero_bits(&legacy) >= (bits as u32) {
                    return Err("legacy_pow_after_activation".to_string());
                }
            }
        }
        return Err("pow_mismatch".to_string());
    }
    if canonical_lz < (bits as u32) {
        return Err(format!("pow_low lz_bits={} need={}", canonical_lz, bits));
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
    let height = u64::from_be_bytes(arr);
    let blocks = tree_blocks(&db).ok()?;
    let block = block_at_from_tree(&blocks, height)?;
    if block.hash32.eq_ignore_ascii_case(hash32) {
        Some(height)
    } else {
        None
    }
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

fn set_tip_fields_db(
    meta: &sled::Tree,
    height: u64,
    hash32: String,
    chainwork: u64,
    bits: u64,
) -> Result<(), String> {
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
    let txs_val = b
        .txs
        .as_ref()
        .ok_or_else(|| "block_txs_missing".to_string())?;
    let txs_obj = txs_val
        .as_object()
        .ok_or_else(|| "block_txs_missing".to_string())?;

    let mut tx_map: HashMap<String, serde_json::Value> = HashMap::new();
    for (k, vv) in txs_obj.iter() {
        if matches!(k.as_str(), "__order" | "transactions" | "txids") {
            continue;
        }
        let computed = txid_from_value(vv)?;
        if computed != *k {
            return Err(format!("txid_mismatch key={} computed={}", k, computed));
        }
        if tx_map.insert(k.clone(), vv.clone()).is_some() {
            return Err(format!("txid_duplicate: {}", k));
        }
    }
    if tx_map.is_empty() {
        return Err("block_txs_empty".to_string());
    }

    let ordered_ids = if let Some(arr) = txs_obj
        .get("__order")
        .or_else(|| txs_obj.get("transactions"))
        .or_else(|| txs_obj.get("txids"))
        .and_then(|v| v.as_array())
    {
        let mut out = Vec::with_capacity(arr.len());
        let mut seen = HashSet::new();
        for item in arr {
            let txid = item
                .as_str()
                .ok_or_else(|| "tx_order_invalid".to_string())?
                .to_string();
            if !tx_map.contains_key(&txid) {
                return Err(format!("tx_order_missing_entry: {}", txid));
            }
            if !seen.insert(txid.clone()) {
                return Err(format!("tx_order_duplicate: {}", txid));
            }
            out.push(txid);
        }
        if out.len() != tx_map.len() {
            return Err("tx_order_len_mismatch".to_string());
        }
        out
    } else {
        let mut ids: Vec<String> = tx_map.keys().cloned().collect();
        ids.sort();
        ids.sort_by_key(|txid| {
            let vin_len = tx_map[txid]
                .get("vin")
                .and_then(|x| x.as_array())
                .map(|a| a.len())
                .unwrap_or(0);
            if vin_len == 0 {
                0usize
            } else {
                1usize
            }
        });
        ids
    };

    let txids: Vec<String> = ordered_ids.clone();
    let expected_merkle = merkle32_from_txids(&txids)?;
    let block_merkle = b
        .merkle32
        .as_deref()
        .ok_or_else(|| "merkle_missing".to_string())?;
    if expected_merkle != block_merkle {
        return Err(format!(
            "merkle_mismatch expected={} got={}",
            expected_merkle, block_merkle
        ));
    }

    ordered_ids
        .into_iter()
        .map(|txid| {
            let tx = tx_map
                .remove(&txid)
                .ok_or_else(|| format!("tx_missing: {}", txid))?;
            Ok((txid, tx))
        })
        .collect()
}

fn infer_network(data_dir: &str) -> Network {
    if let Some(net) = read_datadir_network(data_dir) {
        net
    } else {
        let d = data_dir.to_ascii_lowercase();
        if d.contains("testnet") {
            Network::Testnet
        } else if d.contains("stagenet") {
            Network::Stagenet
        } else {
            Network::Mainnet
        }
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
        .find(|(_, txv)| {
            txv.get("vin")
                .and_then(|x| x.as_array())
                .map(|a| a.is_empty())
                .unwrap_or(false)
        })
        .ok_or_else(|| "coinbase_missing".to_string())?
        .1
        .clone();

    let vout = coinbase
        .get("vout")
        .and_then(|x| x.as_array())
        .cloned()
        .unwrap_or_default();
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
        return Err(format!(
            "devfee_invalid expected={} got={}",
            expected, dev_total
        ));
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
        let vin = txv
            .get("vin")
            .and_then(|x| x.as_array())
            .cloned()
            .unwrap_or_default();
        let is_coinbase = vin.is_empty();

        // Outputs sum (u64; reject on overflow by using checked_add).
        let vout = txv
            .get("vout")
            .and_then(|x| x.as_array())
            .cloned()
            .unwrap_or_default();
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
            let created_h = prev_json
                .get("created_height")
                .and_then(|x| x.as_u64())
                .unwrap_or(0);
            let is_cb = prev_json
                .get("is_coinbase")
                .and_then(|x| x.as_bool())
                .unwrap_or(false);

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
            return Err(format!(
                "fee_negative sum_in={} sum_out={}",
                sum_in, sum_out
            ));
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
        let vin = txv
            .get("vin")
            .and_then(|x| x.as_array())
            .cloned()
            .unwrap_or_default();
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
                let prev_json: serde_json::Value = serde_json::from_slice(&prev_bytes)
                    .map_err(|e| format!("utxo_decode_failed: {}", e))?;
                spent_map.insert(String::from_utf8_lossy(&k).to_string(), prev_json);
                utxo.remove(&k)
                    .map_err(|e| format!("db_remove_failed: {}", e))?;
            }
        }

        // creates
        let vout = txv
            .get("vout")
            .and_then(|x| x.as_array())
            .cloned()
            .unwrap_or_default();
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
            let pkh =
                address::parse_address(addr).ok_or_else(|| format!("invalid_address: {}", addr))?;
            let out_v = json!({
                "value": value,
                "created_height": height,
                "is_coinbase": is_coinbase,
                "pkh": address::pkh_to_hex(&pkh)
            });
            let out_bytes =
                serde_json::to_vec(&out_v).map_err(|e| format!("utxo_encode_failed: {}", e))?;
            utxo.insert(&out_k, out_bytes)
                .map_err(|e| format!("db_insert_failed: {}", e))?;
            created.push(out_s);
        }
    }

    let undo_v = json!({
        "spent": spent_map,
        "created": created
    });
    let undo_bytes =
        serde_json::to_vec(&undo_v).map_err(|e| format!("undo_encode_failed: {}", e))?;
    undo.insert(undo_key(height), undo_bytes)
        .map_err(|e| format!("db_insert_failed: {}", e))?;
    Ok(())
}

fn rollback_one(
    net: Network,
    meta: &sled::Tree,
    blocks: &sled::Tree,
    hash_index: &sled::Tree,
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

    // drop block + hash index + undo entry
    if let Some(tip_block) = block_at_from_tree(blocks, tip_h) {
        hash_index
            .remove(tip_block.hash32.as_bytes())
            .map_err(|e| format!("db_remove_failed: {}", e))?;
    }
    blocks
        .remove(block_key(tip_h))
        .map_err(|e| format!("db_remove_failed: {}", e))?;
    undo.remove(undo_key(tip_h))
        .map_err(|e| format!("db_remove_failed: {}", e))?;

    // set tip to previous block (if any)
    let prev_h = tip_h.saturating_sub(1);
    if prev_h == 0 {
        set_tip_fields_db(
            meta,
            0,
            genesis_hash(net).to_string(),
            0,
            pow_start_bits(net),
        )?;
    } else if let Some(prev_b) = block_at_from_tree(blocks, prev_h) {
        set_tip_fields_db(
            meta,
            prev_b.height,
            prev_b.hash32,
            prev_b.chainwork,
            prev_b.bits,
        )?;
    } else {
        // should not happen if DB consistent; fall back to canonical genesis identity
        set_tip_fields_db(
            meta,
            0,
            genesis_hash(net).to_string(),
            0,
            pow_start_bits(net),
        )?;
    }
    Ok(())
}

fn append_chain_mirror_if_present(data_dir: &str, b: &ChainBlock) {
    let chain_path = format!("{}/chain.json", data_dir.trim_end_matches('/'));
    let Ok(s) = fs::read_to_string(&chain_path) else {
        return;
    };
    let Ok(mut chain) = serde_json::from_str::<Vec<ChainBlock>>(&s) else {
        return;
    };
    if chain
        .last()
        .map(|last| last.height == b.height && last.hash32 == b.hash32)
        .unwrap_or(false)
    {
        return;
    }
    chain.push(b.clone());
    if let Ok(body) = serde_json::to_string(&chain) {
        let _ = durable_write_string(&chain_path, &body);
    }
}

/// Roll back tip down to target_height (inclusive).
/// Reorg depth is limited to available undo records.
pub fn rollback_to_height(data_dir: &str, target_height: u64) -> Result<(), String> {
    let db = open_db(data_dir)?;
    let meta = tree_meta(&db)?;
    let blocks = tree_blocks(&db)?;
    let hash_index = tree_hash_index(&db)?;
    let utxo = tree_utxo(&db)?;
    let undo = tree_undo(&db)?;
    let net = infer_network(data_dir);
    let prune_floor = if let Ok(Some(bytes)) = meta.get(prune_below_key()) {
        serde_json::from_slice::<serde_json::Value>(&bytes)
            .ok()
            .and_then(|v| v.get("prune_below").and_then(|x| x.as_u64()))
            .unwrap_or(0)
    } else {
        0
    };

    if target_height < prune_floor {
        return Err(format!(
            "rollback_pruned target_height={} prune_below={}",
            target_height, prune_floor
        ));
    }

    loop {
        let (h, _hash, _cw, _bits) =
            tip_fields_from_tree(&meta).unwrap_or((0, "0".repeat(64), 0, 0));
        if h <= target_height {
            break;
        }
        rollback_one(net, &meta, &blocks, &hash_index, &utxo, &undo)?;
    }

    // Keep legacy chain.json consistent (best-effort).
    let chain_path = format!("{}/chain.json", data_dir.trim_end_matches('/'));
    if let Ok(s) = fs::read_to_string(&chain_path) {
        if let Ok(mut chain) = serde_json::from_str::<Vec<ChainBlock>>(&s) {
            let keep_len = (target_height.saturating_add(1)) as usize;
            if chain.len() > keep_len {
                chain.truncate(keep_len);
                if let Ok(body) = serde_json::to_string(&chain) {
                    let _ = durable_write_string(&chain_path, &body);
                }
            }
        }
    }

    db.flush().map_err(|e| format!("db_flush_failed: {}", e))?;
    Ok(())
}

fn copy_dir_recursive(src: &Path, dst: &Path) -> Result<(), String> {
    if !src.exists() {
        return Ok(());
    }
    fs::create_dir_all(dst).map_err(|e| format!("dir_create_failed: {}", e))?;
    for entry in fs::read_dir(src).map_err(|e| format!("dir_read_failed: {}", e))? {
        let entry = entry.map_err(|e| format!("dir_entry_failed: {}", e))?;
        let ty = entry
            .file_type()
            .map_err(|e| format!("file_type_failed: {}", e))?;
        let from = entry.path();
        let to = dst.join(entry.file_name());
        if ty.is_dir() {
            copy_dir_recursive(&from, &to)?;
        } else if ty.is_file() {
            fs::copy(&from, &to).map_err(|e| format!("file_copy_failed: {}", e))?;
        }
    }
    Ok(())
}

fn temp_reorg_validation_dir() -> Result<PathBuf, String> {
    let mut p = std::env::temp_dir();
    let uniq = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    p.push(format!("duta-reorg-validate-{}-{}", std::process::id(), uniq));
    fs::create_dir_all(&p).map_err(|e| format!("dir_create_failed: {}", e))?;
    Ok(p)
}

pub fn prevalidate_reorg_candidate(
    data_dir: &str,
    rollback_to: u64,
    blocks: &[ChainBlock],
) -> Result<(), String> {
    if blocks.is_empty() {
        return Ok(());
    }

    let src = Path::new(data_dir);
    let tmp = temp_reorg_validation_dir()?;
    let db_src = src.join("db");
    let db_dst = tmp.join("db");
    let meta_src = src.join("datadir_meta.json");
    let meta_dst = tmp.join("datadir_meta.json");

    let result = (|| -> Result<(), String> {
        copy_dir_recursive(&db_src, &db_dst)?;
        if meta_src.exists() {
            fs::copy(&meta_src, &meta_dst).map_err(|e| format!("file_copy_failed: {}", e))?;
        }
        let tmp_str = tmp.to_string_lossy().to_string();
        rollback_to_height(&tmp_str, rollback_to)?;
        for block in blocks {
            note_accepted_block(&tmp_str, block)?;
        }
        Ok(())
    })();

    let _ = fs::remove_dir_all(&tmp);
    result
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

fn reconcile_tip_fields_with_blocks(
    meta: &sled::Tree,
    blocks: &sled::Tree,
    net: Network,
) -> Result<(u64, String, u64, u64), String> {
    let (meta_h, meta_hash, meta_cw, meta_bits) =
        tip_fields_from_tree(meta).unwrap_or((0, "0".repeat(64), 0, 0));
    if meta_h == 0 && (meta_hash.is_empty() || meta_hash.chars().all(|c| c == '0')) {
        let genesis = genesis_hash(net).to_string();
        set_tip_fields_db(meta, 0, genesis.clone(), 0, pow_start_bits(net))?;
        return Ok((0, genesis, 0, pow_start_bits(net)));
    }
    if meta_h == 0 {
        return Ok((meta_h, meta_hash, meta_cw, meta_bits));
    }
    if let Some(block) = block_at_from_tree(blocks, meta_h) {
        if block.hash32 == meta_hash {
            return Ok((meta_h, meta_hash, meta_cw, meta_bits));
        }
        let cw = if block.chainwork > 0 {
            block.chainwork
        } else {
            meta_cw
        };
        set_tip_fields_db(meta, block.height, block.hash32.clone(), cw, block.bits)?;
        return Ok((block.height, block.hash32, cw, block.bits));
    }
    let mut h = meta_h.saturating_sub(1);
    while h > 0 {
        if let Some(block) = block_at_from_tree(blocks, h) {
            let cw = if block.chainwork > 0 {
                block.chainwork
            } else {
                meta_cw
            };
            set_tip_fields_db(meta, block.height, block.hash32.clone(), cw, block.bits)?;
            return Ok((block.height, block.hash32, cw, block.bits));
        }
        h = h.saturating_sub(1);
    }
    let genesis = genesis_hash(net).to_string();
    set_tip_fields_db(meta, 0, genesis.clone(), 0, pow_start_bits(net))?;
    Ok((0, genesis, 0, pow_start_bits(net)))
}

fn validate_bootstrap_chain(net: Network, chain: &[ChainBlock]) -> Result<(), String> {
    if chain.is_empty() {
        return Ok(());
    }

    let genesis = genesis_hash(net);
    let first = &chain[0];
    match first.height {
        0 => {
            if !first.hash32.eq_ignore_ascii_case(genesis) {
                return Err("chain_bootstrap_bad_genesis_hash".to_string());
            }
        }
        1 => {
            let prev = first.prevhash32.as_deref().unwrap_or("");
            if !prev.eq_ignore_ascii_case(genesis) {
                return Err("chain_bootstrap_bad_genesis_link".to_string());
            }
        }
        _ => {
            return Err(format!(
                "chain_bootstrap_bad_start_height: {}",
                first.height
            ));
        }
    }

    for pair in chain.windows(2) {
        let prev = &pair[0];
        let next = &pair[1];
        if next.height != prev.height.saturating_add(1) {
            return Err(format!(
                "chain_bootstrap_non_contiguous_height: prev={} next={}",
                prev.height, next.height
            ));
        }
        let next_prevhash = next.prevhash32.as_deref().unwrap_or("");
        if !next_prevhash.eq_ignore_ascii_case(&prev.hash32) {
            return Err(format!(
                "chain_bootstrap_bad_prevhash: height={} prevhash={} expected={}",
                next.height, next_prevhash, prev.hash32
            ));
        }
    }

    Ok(())
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
    let hash_index = tree_hash_index(&db)?;
    let undo = tree_undo(&db)?;
    let (tip_h, _hash, _cw, _bits) =
        tip_fields_from_tree(&meta).unwrap_or((0, "0".repeat(64), 0, 0));
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
        if let Some(block) = block_at_from_tree(&blocks, h) {
            let _ = hash_index.remove(block.hash32.as_bytes());
        }
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
                    let mut new_arr: Vec<serde_json::Value> =
                        Vec::with_capacity(1 + (arr.len() - start));
                    new_arr.push(arr[0].clone()); // genesis
                    for x in &arr[start..] {
                        new_arr.push(x.clone());
                    }
                    if let Ok(body) = serde_json::to_string_pretty(&new_arr) {
                        let _ = durable_write_string(&chain_path, &body);
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
            set_tip_fields_db(
                &meta,
                0,
                genesis_hash(net).to_string(),
                0,
                pow_start_bits(net),
            )?;
            let _ = meta.insert(
                prune_below_key(),
                serde_json::to_vec(&json!({"prune_below":0})).unwrap_or_default(),
            );
            db.flush().map_err(|e| format!("db_flush_failed: {}", e))?;
            return Ok(());
        }
    };

    let chain: Vec<ChainBlock> =
        serde_json::from_str(&s).map_err(|e| format!("chain_json_invalid: {}", e))?;
    let net = infer_network(data_dir);
    validate_bootstrap_chain(net, &chain)?;

    for b in &chain {
        put_block_db(&blocks, b)?;
        let _ = put_hash_index_db(&hash_index, &b.hash32, b.height);
        if b.txs.is_some() {
            let txs = parse_txs_from_block(b)?;
            apply_utxo_for_block(&utxo, &undo, b.height, &txs)?;
        }
    }

    if let Some(tip) = chain.last() {
        set_tip_fields_db(
            &meta,
            tip.height,
            tip.hash32.clone(),
            tip.chainwork,
            tip.bits,
        )?;
    } else {
        set_tip_fields_db(
            &meta,
            0,
            genesis_hash(net).to_string(),
            0,
            pow_start_bits(net),
        )?;
    }

    db.flush().map_err(|e| format!("db_flush_failed: {}", e))?;
    Ok(())
}

/// Record a newly accepted block into DB and update UTXO/undo.
/// If a legacy chain mirror already exists, keep it in sync best-effort.
pub fn validate_candidate_block(data_dir: &str, b: &ChainBlock) -> Result<ChainBlock, String> {
    let db = open_db(data_dir)?;
    let meta = tree_meta(&db)?;
    let blocks = tree_blocks(&db)?;
    let utxo = tree_utxo(&db)?;

    let net = infer_network(data_dir);

    let (tip_h, tip_hash, tip_cw, _tip_bits) =
        reconcile_tip_fields_with_blocks(&meta, &blocks, net)?;

    if b.height == 0 {
        // genesis
    } else {
        if b.height != tip_h.saturating_add(1) {
            return Err(format!(
                "stale_or_out_of_order_block: height={} tip={}",
                b.height, tip_h
            ));
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
    if let Some(_) = b2.txs.as_ref() {
        let txs = parse_txs_from_block(&b2)?;
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
        reconcile_tip_fields_with_blocks(&meta, &blocks, net)?;

    if b.height == 0 {
        // genesis
    } else {
        // We only accept blocks that extend the current tip. Reorg must be handled by rollback + sequential apply.
        if b.height != tip_h.saturating_add(1) {
            return Err(format!(
                "stale_or_out_of_order_block: height={} tip={}",
                b.height, tip_h
            ));
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

    if let Some(_) = b2.txs.as_ref() {
        let txs = parse_txs_from_block(&b2)?;
        validate_coinbase_devfee(&txs, net, b2.height)?;
        validate_coinbase_subsidy(&utxo, b2.height, &txs)?;
        validate_utxo_invariants_for_block(&utxo, b2.height, &txs)?;
        apply_utxo_for_block(&utxo, &undo, b2.height, &txs)?;
        let confirmed_txids: Vec<String> = txs.iter().map(|(txid, _)| txid.clone()).collect();
        put_block_db(&blocks, &b2)?;
        put_hash_index_db(&hash_index, &b2.hash32, b2.height)?;
        crate::submit_tx::prune_confirmed_txids_from_mempool_file(data_dir, &confirmed_txids);
        // Best-effort: newly updated UTXO may satisfy some orphan txs
        let _ = crate::submit_tx::orphan_try_promote_file(data_dir);
    } else {
        put_block_db(&blocks, &b2)?;
        put_hash_index_db(&hash_index, &b2.hash32, b2.height)?;
    }
    set_tip_fields_db(&meta, b2.height, b2.hash32.clone(), b2.chainwork, b2.bits)?;
    append_chain_mirror_if_present(data_dir, &b2);
    // Default node behavior must keep full block history unless pruning is requested explicitly.
    maybe_prune_after_accepted_block(data_dir)?;

    db.flush().map_err(|e| format!("db_flush_failed: {}", e))?;
    Ok(())
}

fn maybe_prune_after_accepted_block(_data_dir: &str) -> Result<(), String> {
    Ok(())
}

/// Tip fields from DB (if present), else None.
pub fn tip_fields(data_dir: &str) -> Option<(u64, String, u64, u64)> {
    let db = open_db(data_dir).ok()?;
    let meta = tree_meta(&db).ok()?;
    let blocks = tree_blocks(&db).ok()?;
    let net = infer_network(data_dir);
    let (height, hash32, chainwork, bits) =
        reconcile_tip_fields_with_blocks(&meta, &blocks, net).ok()?;
    if height == 0 && (hash32.is_empty() || hash32.chars().all(|c| c == '0')) {
        return Some((
            0,
            genesis_hash(net).to_string(),
            chainwork,
            pow_start_bits(net),
        ));
    }
    Some((height, hash32, chainwork, bits))
}

pub fn flush_db(data_dir: &str) -> Result<(), String> {
    let db = open_db(data_dir)?;
    db.flush().map_err(|e| format!("db_flush_failed: {}", e))?;
    Ok(())
}

/// Compute expected `bits` for the next block height (consensus).
pub fn expected_bits_next(data_dir: &str) -> Result<u64, String> {
    let db = open_db(data_dir)?;
    let blocks = tree_blocks(&db)?;
    let net = infer_network(data_dir);
    let (tip_h, _tip_hash, _tip_cw, _tip_bits) =
        tip_fields(data_dir).unwrap_or((0, "0".repeat(64), 0, 0));
    expected_bits_for_next_height(&blocks, net, tip_h + 1)
}

pub fn block_at(data_dir: &str, height: u64) -> Option<ChainBlock> {
    let db = open_db(data_dir).ok()?;
    let blocks = tree_blocks(&db).ok()?;
    block_at_from_tree(&blocks, height)
}

pub fn blocks_from_range(data_dir: &str, from: u64, limit: usize) -> Vec<ChainBlock> {
    if limit == 0 {
        return Vec::new();
    }
    let db = match open_db(data_dir) {
        Ok(db) => db,
        Err(_) => return Vec::new(),
    };
    let blocks = match tree_blocks(&db) {
        Ok(blocks) => blocks,
        Err(_) => return Vec::new(),
    };
    let mut out: Vec<ChainBlock> = Vec::with_capacity(limit.min(256));
    for item in blocks.range(block_key(from)..) {
        let Ok((_, bytes)) = item else {
            break;
        };
        let Ok(block) = serde_json::from_slice::<ChainBlock>(&bytes) else {
            break;
        };
        if block.height < from {
            continue;
        }
        if let Some(prev) = out.last() {
            if block.height != prev.height.saturating_add(1) {
                break;
            }
        }
        out.push(block);
        if out.len() >= limit {
            break;
        }
    }
    out
}

/// Lookup a UTXO outpoint in DB.
/// Returns (value, created_height, is_coinbase)
pub fn utxo_get(data_dir: &str, txid: &str, vout: u64) -> Option<(u64, u64, bool, String)> {
    let db = open_db(data_dir).ok()?;
    let utxo = tree_utxo(&db).ok()?;
    let bytes = utxo.get(outpoint_key(txid, vout)).ok()??;
    let v: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
    let value = v.get("value").and_then(|x| x.as_u64()).unwrap_or(0);
    let ch = v
        .get("created_height")
        .and_then(|x| x.as_u64())
        .unwrap_or(0);
    let cb = v
        .get("is_coinbase")
        .and_then(|x| x.as_bool())
        .unwrap_or(false);
    let pkh = v
        .get("pkh")
        .and_then(|x| x.as_str())
        .unwrap_or("")
        .to_string();
    Some((value, ch, cb, pkh))
}

pub fn utxos_for_addresses(
    data_dir: &str,
    addresses: &[String],
) -> Result<Vec<(String, u64, u64, u64, bool, String)>, String> {
    let net = infer_network(data_dir);
    let mut target_by_pkh: HashMap<String, String> = HashMap::new();
    for addr in addresses {
        let pkh = address::parse_address_for_network(net, addr)
            .ok_or_else(|| format!("invalid_address: {}", addr))?;
        target_by_pkh
            .entry(address::pkh_to_hex(&pkh))
            .or_insert_with(|| address::pkh_to_address_for_network(net, &pkh));
    }
    if target_by_pkh.is_empty() {
        return Ok(Vec::new());
    }

    let db = open_db(data_dir)?;
    let utxo = tree_utxo(&db)?;
    let mut out = Vec::new();

    for item in utxo.iter() {
        let (key, bytes) = item.map_err(|e| format!("utxo_iter_failed: {}", e))?;
        let key = std::str::from_utf8(key.as_ref())
            .map_err(|e| format!("utxo_key_invalid_utf8: {}", e))?;
        let Some((txid, vout_s)) = key.rsplit_once(':') else {
            return Err(format!("utxo_key_invalid: {}", key));
        };
        let vout = vout_s
            .parse::<u64>()
            .map_err(|e| format!("utxo_key_invalid_vout: {}", e))?;
        let value: serde_json::Value =
            serde_json::from_slice(&bytes).map_err(|e| format!("utxo_decode_failed: {}", e))?;
        let pkh = value.get("pkh").and_then(|x| x.as_str()).unwrap_or("");
        let Some(address) = target_by_pkh.get(pkh) else {
            continue;
        };
        let amount = value.get("value").and_then(|x| x.as_u64()).unwrap_or(0);
        let height = value
            .get("created_height")
            .and_then(|x| x.as_u64())
            .unwrap_or(0);
        let coinbase = value
            .get("is_coinbase")
            .and_then(|x| x.as_bool())
            .unwrap_or(false);
        out.push((
            txid.to_string(),
            vout,
            amount,
            height,
            coinbase,
            address.clone(),
        ));
    }

    out.sort_by(|a, b| (a.0.clone(), a.1).cmp(&(b.0.clone(), b.1)));
    Ok(out)
}

#[cfg(test)]
mod tests_a5 {
    use super::*;
    use duta_core::amount::DUT_PER_DUTA;
    use serde_json::json;
    fn tmp_tree(name: &str) -> sled::Tree {
        let db = sled::Config::new().temporary(true).open().unwrap();
        db.open_tree(name).unwrap()
    }

    fn temp_datadir(tag: &str) -> String {
        let mut p = std::env::temp_dir();
        let uniq = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        p.push(format!("duta-store-test-{}-{}", tag, uniq));
        std::fs::create_dir_all(&p).unwrap();
        p.to_string_lossy().to_string()
    }

    fn put_utxo(
        utxo: &sled::Tree,
        txid: &str,
        vout: u64,
        value: u64,
        created_height: u64,
        is_coinbase: bool,
    ) {
        let k = outpoint_key(txid, vout);
        let v =
            json!({"value": value, "created_height": created_height, "is_coinbase": is_coinbase});
        utxo.insert(k, serde_json::to_vec(&v).unwrap()).unwrap();
    }

    fn put_utxo_with_pkh(
        utxo: &sled::Tree,
        txid: &str,
        vout: u64,
        value: u64,
        created_height: u64,
        is_coinbase: bool,
        pkh_hex: &str,
    ) {
        let k = outpoint_key(txid, vout);
        let v = json!({
            "value": value,
            "created_height": created_height,
            "is_coinbase": is_coinbase,
            "pkh": pkh_hex
        });
        utxo.insert(k, serde_json::to_vec(&v).unwrap()).unwrap();
    }

    #[test]
    fn a5_reject_missing_input_utxo() {
        let utxo = tmp_tree("utxo");
        let txs = vec![(
            "tx1".to_string(),
            json!({"vin":[{"txid":"nope","vout":0}], "vout":[{"value":1}]}),
        )];
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
        let txs = vec![(
            "tx1".to_string(),
            json!({"vin":[{"txid":"cb","vout":0}], "vout":[{"value":1}]}),
        )];
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
                    {"addr":"dut34eb9f0d0d9e0ec7fbf78e6bfc7277f10f01aaf9","value": 2_500_000_000u64},
                    {"addr":"dut34eb9f0d0d9e0ec7fbf78e6bfc7277f10f01aaf8","value": 1u64}
                ]
            }),
        )];
        let err = validate_coinbase_subsidy(&utxo, 210_000, &txs).unwrap_err();
        assert!(err.contains("coinbase_overpay"));
    }

    #[test]
    fn utxos_for_addresses_reads_active_set_without_block_history() {
        let data_dir = temp_datadir("wallet-utxo-snapshot");
        let db_path = format!("{}/db", data_dir.trim_end_matches('/'));
        ensure_datadir_meta(&data_dir, "mainnet").unwrap();
        let db = sled::open(&db_path).unwrap();
        let utxo = db.open_tree(b"utxo").unwrap();
        let addr_a = "dut1111111111111111111111111111111111111111".to_string();
        let addr_b = "dut2222222222222222222222222222222222222222".to_string();
        let pkh_a = address::pkh_to_hex(&address::parse_address(&addr_a).unwrap());
        let pkh_b = address::pkh_to_hex(&address::parse_address(&addr_b).unwrap());
        put_utxo_with_pkh(&utxo, "aa", 0, 50, 100, false, &pkh_a);
        put_utxo_with_pkh(&utxo, "bb", 1, 75, 101, true, &pkh_b);
        put_utxo_with_pkh(
            &utxo,
            "cc",
            2,
            99,
            102,
            false,
            "3333333333333333333333333333333333333333",
        );
        utxo.flush().unwrap();
        db.flush().unwrap();
        drop(utxo);
        drop(db);

        let got = utxos_for_addresses(&data_dir, &[addr_b.clone(), addr_a.clone()]).unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(got[0], ("aa".to_string(), 0, 50, 100, false, addr_a));
        assert_eq!(got[1], ("bb".to_string(), 1, 75, 101, true, addr_b));
    }

    #[test]
    fn utxos_for_addresses_rejects_wrong_network_prefix() {
        let data_dir = temp_datadir("wallet-utxo-network-lock");
        ensure_datadir_meta(&data_dir, "mainnet").unwrap();
        let err = utxos_for_addresses(
            &data_dir,
            &["test1111111111111111111111111111111111111111".to_string()],
        )
        .unwrap_err();
        assert!(err.contains("invalid_address"));
    }

    #[test]
    fn a5_reject_negative_fee() {
        let utxo = tmp_tree("utxo");
        put_utxo(&utxo, "a", 0, 5, 1, false);
        let txs = vec![(
            "tx1".to_string(),
            json!({"vin":[{"txid":"a","vout":0}], "vout":[{"value":6}]}),
        )];
        let err = validate_utxo_invariants_for_block(&utxo, 10, &txs).unwrap_err();
        assert!(err.contains("fee_negative"));
    }

    #[test]
    fn a5_block_subsidy_uses_base_unit_scaling() {
        assert_eq!(block_subsidy(0), 50 * DUT_PER_DUTA);
        assert_eq!(block_subsidy(210_000), 25 * DUT_PER_DUTA);
    }

    #[test]
    fn rollback_to_height_moves_tip_back_and_removes_undo_entry() {
        let data_dir = temp_datadir("rollback");
        let db = open_db(&data_dir).unwrap();
        let meta = tree_meta(&db).unwrap();
        let blocks = tree_blocks(&db).unwrap();
        let hash_index = tree_hash_index(&db).unwrap();
        let undo = tree_undo(&db).unwrap();

        let block1 = ChainBlock {
            height: 1,
            hash32: "11".repeat(32),
            bits: 1,
            chainwork: 2,
            timestamp: Some(1),
            prevhash32: Some("00".repeat(32)),
            merkle32: Some("22".repeat(32)),
            nonce: Some(0),
            miner: Some("miner".to_string()),
            pow_digest32: Some("33".repeat(32)),
            txs: None,
        };
        let block2 = ChainBlock {
            height: 2,
            hash32: "44".repeat(32),
            bits: 1,
            chainwork: 4,
            timestamp: Some(2),
            prevhash32: Some(block1.hash32.clone()),
            merkle32: Some("55".repeat(32)),
            nonce: Some(0),
            miner: Some("miner".to_string()),
            pow_digest32: Some("66".repeat(32)),
            txs: None,
        };

        put_block_db(&blocks, &block1).unwrap();
        put_block_db(&blocks, &block2).unwrap();
        put_hash_index_db(&hash_index, &block1.hash32, 1).unwrap();
        put_hash_index_db(&hash_index, &block2.hash32, 2).unwrap();
        undo.insert(
            undo_key(2),
            serde_json::to_vec(&json!({"created":[],"spent":{}})).unwrap(),
        )
        .unwrap();
        set_tip_fields_db(
            &meta,
            2,
            block2.hash32.clone(),
            block2.chainwork,
            block2.bits,
        )
        .unwrap();
        db.flush().unwrap();
        drop(undo);
        drop(hash_index);
        drop(blocks);
        drop(meta);
        drop(db);

        rollback_to_height(&data_dir, 1).unwrap();

        let (tip_h, tip_hash, tip_cw, _tip_bits) = tip_fields(&data_dir).unwrap();
        assert_eq!(tip_h, 1);
        assert_eq!(tip_hash, block1.hash32);
        assert_eq!(tip_cw, block1.chainwork);
        assert!(block_at(&data_dir, 2).is_none());
        assert!(height_by_hash(&data_dir, &block2.hash32).is_none());
        let db = open_db(&data_dir).unwrap();
        let undo = tree_undo(&db).unwrap();
        assert!(undo.get(undo_key(2)).unwrap().is_none());
    }

    #[test]
    fn rollback_to_zero_restores_canonical_genesis_tip_identity() {
        let data_dir = temp_datadir("rollback-zero");
        ensure_datadir_meta(&data_dir, "mainnet").unwrap();
        let db = open_db(&data_dir).unwrap();
        let meta = tree_meta(&db).unwrap();
        let blocks = tree_blocks(&db).unwrap();
        let undo = tree_undo(&db).unwrap();

        let block1 = ChainBlock {
            height: 1,
            hash32: "11".repeat(32),
            bits: 1,
            chainwork: 2,
            timestamp: Some(1),
            prevhash32: Some(genesis_hash(Network::Mainnet).to_string()),
            merkle32: Some("22".repeat(32)),
            nonce: Some(0),
            miner: Some("miner".to_string()),
            pow_digest32: Some("33".repeat(32)),
            txs: None,
        };

        put_block_db(&blocks, &block1).unwrap();
        undo.insert(
            undo_key(1),
            serde_json::to_vec(&json!({"created":[],"spent":{}})).unwrap(),
        )
        .unwrap();
        set_tip_fields_db(
            &meta,
            1,
            block1.hash32.clone(),
            block1.chainwork,
            block1.bits,
        )
        .unwrap();
        db.flush().unwrap();
        drop(undo);
        drop(blocks);
        drop(meta);
        drop(db);

        rollback_to_height(&data_dir, 0).unwrap();

        let (tip_h, tip_hash, tip_cw, tip_bits) = tip_fields(&data_dir).unwrap();
        assert_eq!(tip_h, 0);
        assert_eq!(tip_hash, genesis_hash(Network::Mainnet));
        assert_eq!(tip_cw, 0);
        assert_eq!(tip_bits, pow_start_bits(Network::Mainnet));
    }

    #[test]
    fn prune_updates_prune_below_watermark() {
        let data_dir = temp_datadir("prune");
        let db = open_db(&data_dir).unwrap();
        let meta = tree_meta(&db).unwrap();
        let blocks = tree_blocks(&db).unwrap();
        let hash_index = tree_hash_index(&db).unwrap();
        let undo = tree_undo(&db).unwrap();

        for h in 1..=3u64 {
            let block = ChainBlock {
                height: h,
                hash32: format!("{:064x}", h),
                bits: 1,
                chainwork: h * 2,
                timestamp: Some(h),
                prevhash32: Some(format!("{:064x}", h.saturating_sub(1))),
                merkle32: Some(format!("{:064x}", h + 100)),
                nonce: Some(0),
                miner: Some("miner".to_string()),
                pow_digest32: Some(format!("{:064x}", h + 200)),
                txs: None,
            };
            put_block_db(&blocks, &block).unwrap();
            put_hash_index_db(&hash_index, &block.hash32, h).unwrap();
            undo.insert(
                undo_key(h),
                serde_json::to_vec(&json!({"created":[],"spent":{}})).unwrap(),
            )
            .unwrap();
        }
        set_tip_fields_db(&meta, 3, format!("{:064x}", 3), 6, 1).unwrap();
        db.flush().unwrap();
        drop(undo);
        drop(hash_index);
        drop(blocks);
        drop(meta);
        drop(db);

        prune(&data_dir, 1).unwrap();

        assert_eq!(prune_below(&data_dir), 2);
        assert!(block_at(&data_dir, 1).is_none());
        assert!(height_by_hash(&data_dir, &format!("{:064x}", 1)).is_none());
        assert!(block_at(&data_dir, 2).is_some());
    }

    #[test]
    fn accepted_block_path_does_not_auto_prune_by_default() {
        let data_dir = temp_datadir("no-auto-prune");
        let db = open_db(&data_dir).unwrap();
        let meta = tree_meta(&db).unwrap();
        let blocks = tree_blocks(&db).unwrap();
        let hash_index = tree_hash_index(&db).unwrap();
        let undo = tree_undo(&db).unwrap();

        for h in 1..=3u64 {
            let block = ChainBlock {
                height: h,
                hash32: format!("{:064x}", h),
                bits: 1,
                chainwork: h * 2,
                timestamp: Some(h),
                prevhash32: Some(format!("{:064x}", h.saturating_sub(1))),
                merkle32: Some(format!("{:064x}", h + 100)),
                nonce: Some(0),
                miner: Some("miner".to_string()),
                pow_digest32: Some(format!("{:064x}", h + 200)),
                txs: None,
            };
            put_block_db(&blocks, &block).unwrap();
            put_hash_index_db(&hash_index, &block.hash32, h).unwrap();
            undo.insert(
                undo_key(h),
                serde_json::to_vec(&json!({"created":[],"spent":{}})).unwrap(),
            )
            .unwrap();
        }
        set_tip_fields_db(&meta, 3, format!("{:064x}", 3), 6, 1).unwrap();
        db.flush().unwrap();
        drop(undo);
        drop(hash_index);
        drop(blocks);
        drop(meta);
        drop(db);

        maybe_prune_after_accepted_block(&data_dir).unwrap();

        assert_eq!(prune_below(&data_dir), 0);
        assert!(block_at(&data_dir, 1).is_some());
        assert!(height_by_hash(&data_dir, &format!("{:064x}", 1)).is_some());
    }

    #[test]
    fn rollback_rejects_targets_below_pruned_floor() {
        let data_dir = temp_datadir("rollback-pruned");
        let db = open_db(&data_dir).unwrap();
        let meta = tree_meta(&db).unwrap();
        let blocks = tree_blocks(&db).unwrap();
        let undo = tree_undo(&db).unwrap();

        for h in 1..=3u64 {
            let block = ChainBlock {
                height: h,
                hash32: format!("{:064x}", h),
                bits: 1,
                chainwork: h * 2,
                timestamp: Some(h),
                prevhash32: Some(format!("{:064x}", h.saturating_sub(1))),
                merkle32: Some(format!("{:064x}", h + 100)),
                nonce: Some(0),
                miner: Some("miner".to_string()),
                pow_digest32: Some(format!("{:064x}", h + 200)),
                txs: None,
            };
            put_block_db(&blocks, &block).unwrap();
            undo.insert(
                undo_key(h),
                serde_json::to_vec(&json!({"created":[],"spent":{}})).unwrap(),
            )
            .unwrap();
        }
        set_tip_fields_db(&meta, 3, format!("{:064x}", 3), 6, 1).unwrap();
        db.flush().unwrap();
        drop(undo);
        drop(blocks);
        drop(meta);
        drop(db);

        prune(&data_dir, 1).unwrap();

        let err = rollback_to_height(&data_dir, 1).unwrap_err();
        assert!(err.contains("rollback_pruned"));
        assert!(err.contains("prune_below=2"));
    }

    #[test]
    fn chain_mirror_is_not_created_for_fresh_db_only_nodes() {
        let data_dir = temp_datadir("mirror-fresh");
        let block = ChainBlock {
            height: 1,
            hash32: "aa".repeat(32),
            bits: 1,
            chainwork: 2,
            timestamp: Some(1),
            prevhash32: Some("00".repeat(32)),
            merkle32: Some("bb".repeat(32)),
            nonce: Some(0),
            miner: Some("miner".to_string()),
            pow_digest32: Some("cc".repeat(32)),
            txs: None,
        };

        append_chain_mirror_if_present(&data_dir, &block);

        assert!(!std::path::Path::new(&format!("{}/chain.json", data_dir)).exists());
    }

    #[test]
    fn chain_mirror_appends_when_legacy_file_exists() {
        let data_dir = temp_datadir("mirror-existing");
        let chain_path = format!("{}/chain.json", data_dir);
        std::fs::write(&chain_path, "[]").unwrap();
        let block = ChainBlock {
            height: 1,
            hash32: "dd".repeat(32),
            bits: 1,
            chainwork: 2,
            timestamp: Some(1),
            prevhash32: Some("00".repeat(32)),
            merkle32: Some("ee".repeat(32)),
            nonce: Some(0),
            miner: Some("miner".to_string()),
            pow_digest32: Some("ff".repeat(32)),
            txs: None,
        };

        append_chain_mirror_if_present(&data_dir, &block);

        let body = std::fs::read_to_string(&chain_path).unwrap();
        let chain: Vec<ChainBlock> = serde_json::from_str(&body).unwrap();
        assert_eq!(chain.len(), 1);
        assert_eq!(chain[0].hash32, block.hash32);
    }

    #[test]
    fn rejected_block_does_not_leave_partial_block_or_hash_index_artifacts() {
        let data_dir = temp_datadir("reject-no-partial-state");
        ensure_datadir_meta(&data_dir, "mainnet").unwrap();
        bootstrap(&data_dir).unwrap();
        let block = ChainBlock {
            height: 0,
            hash32: "77".repeat(32),
            bits: pow_start_bits(Network::Mainnet),
            chainwork: 0,
            timestamp: Some(1),
            prevhash32: Some("00".repeat(32)),
            merkle32: Some("88".repeat(32)),
            nonce: Some(0),
            miner: Some("dut111111111111111111111111111111111111111111".to_string()),
            pow_digest32: Some("77".repeat(32)),
            txs: Some(json!({
                "deadbeef": {
                    "vin": [],
                    "vout": [
                        {
                            "addr":"dut111111111111111111111111111111111111111111",
                            "value": 50
                        }
                    ],
                    "genesis_message": "lab-bad-txid-key"
                }
            })),
        };

        let err = note_accepted_block(&data_dir, &block).unwrap_err();
        assert!(err.contains("txid_mismatch"));
        assert!(block_at(&data_dir, 0).is_none());
        assert!(height_by_hash(&data_dir, &block.hash32).is_none());
    }

    #[test]
    fn bootstrap_rejects_non_contiguous_legacy_chain() {
        let data_dir = temp_datadir("bootstrap-invalid-gap");
        ensure_datadir_meta(&data_dir, "mainnet").unwrap();

        let chain = vec![
            ChainBlock {
                height: 1,
                hash32: "11".repeat(32),
                bits: 1,
                chainwork: 1,
                timestamp: Some(100),
                prevhash32: Some(genesis_hash(Network::Mainnet).to_string()),
                merkle32: Some("22".repeat(32)),
                nonce: Some(1),
                miner: Some("miner-a".to_string()),
                pow_digest32: Some("33".repeat(32)),
                txs: None,
            },
            ChainBlock {
                height: 3,
                hash32: "44".repeat(32),
                bits: 1,
                chainwork: 2,
                timestamp: Some(101),
                prevhash32: Some("11".repeat(32)),
                merkle32: Some("55".repeat(32)),
                nonce: Some(2),
                miner: Some("miner-b".to_string()),
                pow_digest32: Some("66".repeat(32)),
                txs: None,
            },
        ];
        let body = serde_json::to_string(&chain).unwrap();
        std::fs::write(format!("{}/chain.json", data_dir), body).unwrap();

        let err = bootstrap(&data_dir).unwrap_err();
        assert!(err.contains("chain_bootstrap_non_contiguous_height"));
    }

    #[test]
    fn bootstrap_rejects_legacy_chain_with_wrong_genesis_link() {
        let data_dir = temp_datadir("bootstrap-invalid-genesis-link");
        ensure_datadir_meta(&data_dir, "mainnet").unwrap();

        let chain = vec![ChainBlock {
            height: 1,
            hash32: "11".repeat(32),
            bits: 1,
            chainwork: 1,
            timestamp: Some(100),
            prevhash32: Some("ff".repeat(32)),
            merkle32: Some("22".repeat(32)),
            nonce: Some(1),
            miner: Some("miner-a".to_string()),
            pow_digest32: Some("33".repeat(32)),
            txs: None,
        }];
        let body = serde_json::to_string(&chain).unwrap();
        std::fs::write(format!("{}/chain.json", data_dir), body).unwrap();

        let err = bootstrap(&data_dir).unwrap_err();
        assert!(err.contains("chain_bootstrap_bad_genesis_link"));
    }

    #[test]
    fn bootstrap_rejects_legacy_chain_with_malformed_txs() {
        let data_dir = temp_datadir("bootstrap-invalid-txs");
        ensure_datadir_meta(&data_dir, "mainnet").unwrap();

        let chain = vec![ChainBlock {
            height: 1,
            hash32: "11".repeat(32),
            bits: 1,
            chainwork: 1,
            timestamp: Some(100),
            prevhash32: Some(genesis_hash(Network::Mainnet).to_string()),
            merkle32: Some("22".repeat(32)),
            nonce: Some(1),
            miner: Some("miner-a".to_string()),
            pow_digest32: Some("33".repeat(32)),
            txs: Some(json!({
                "deadbeef": {
                    "vin": [],
                    "vout": [
                        {
                            "addr":"dut111111111111111111111111111111111111111111",
                            "value": 50
                        }
                    ],
                    "genesis_message": "legacy-bad-txid-key"
                }
            })),
        }];
        let body = serde_json::to_string(&chain).unwrap();
        std::fs::write(format!("{}/chain.json", data_dir), body).unwrap();

        let err = bootstrap(&data_dir).unwrap_err();
        assert!(err.contains("txid_mismatch"));
    }

    #[test]
    fn infer_network_prefers_datadir_meta_over_path_name() {
        let data_dir = temp_datadir("infer-network-meta");
        ensure_datadir_meta(&data_dir, "testnet").unwrap();
        assert_eq!(infer_network(&data_dir), Network::Testnet);
    }

    #[test]
    fn durable_write_bytes_uses_unique_tmp_files_per_write() {
        let data_dir = temp_datadir("durable-write-unique");
        let path = format!("{}/mempool.json", data_dir);
        let mut joins = Vec::new();
        for idx in 0..8u64 {
            let path_cloned = path.clone();
            joins.push(std::thread::spawn(move || {
                for iter in 0..32u64 {
                    let body = format!("{{\"idx\":{},\"iter\":{}}}", idx, iter);
                    durable_write_string(&path_cloned, &body).expect("durable write should succeed");
                }
            }));
        }
        for join in joins {
            join.join().expect("writer thread");
        }
        let final_body = std::fs::read_to_string(&path).expect("final body");
        assert!(final_body.contains("\"idx\""));
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[test]
    fn durable_write_bytes_cleans_tmp_file_on_rename_error() {
        let data_dir = temp_datadir("durable-write-cleanup");
        let path = format!("{}/peers.txt", data_dir);
        std::fs::create_dir_all(&path).expect("create conflicting dir");

        let err = durable_write_string(&path, "hello").unwrap_err();
        assert!(err.contains("replace_remove_failed") || err.contains("rename_failed"));

        let entries: Vec<String> = std::fs::read_dir(&data_dir)
            .expect("read dir")
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.file_name().to_string_lossy().to_string())
            .collect();
        assert!(
            entries
                .iter()
                .all(|name| !name.starts_with("peers.txt.tmp.")),
            "unexpected tmp leak: {:?}",
            entries
        );

        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[cfg(unix)]
    #[test]
    fn durable_write_bytes_on_unix_does_not_attempt_remove_replace_fallback() {
        let data_dir = temp_datadir("durable-write-unix-rename-only");
        let path = format!("{}/peers.txt", data_dir);
        std::fs::create_dir_all(&path).expect("create conflicting dir");

        let err = durable_write_string(&path, "hello").unwrap_err();
        assert!(err.contains("rename_failed"));
        assert!(!err.contains("replace_remove_failed"));

        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[test]
    fn rollback_to_genesis_and_reopen_preserves_canonical_next_block_state() {
        let data_dir = temp_datadir("rollback-reopen-accept");
        ensure_datadir_meta(&data_dir, "mainnet").unwrap();
        bootstrap(&data_dir).unwrap();

        let db = open_db(&data_dir).unwrap();
        let meta = tree_meta(&db).unwrap();
        let blocks = tree_blocks(&db).unwrap();
        let undo = tree_undo(&db).unwrap();

        let block1 = ChainBlock {
            height: 1,
            hash32: "11".repeat(32),
            bits: pow_start_bits(Network::Mainnet),
            chainwork: work_from_bits(pow_start_bits(Network::Mainnet)),
            timestamp: Some(1),
            prevhash32: Some(genesis_hash(Network::Mainnet).to_string()),
            merkle32: Some("22".repeat(32)),
            nonce: Some(0),
            miner: Some("miner-a".to_string()),
            pow_digest32: Some("33".repeat(32)),
            txs: None,
        };
        put_block_db(&blocks, &block1).unwrap();
        undo.insert(
            undo_key(1),
            serde_json::to_vec(&json!({"created":[],"spent":{}})).unwrap(),
        )
        .unwrap();
        set_tip_fields_db(
            &meta,
            1,
            block1.hash32.clone(),
            block1.chainwork,
            block1.bits,
        )
        .unwrap();
        db.flush().unwrap();
        drop(undo);
        drop(blocks);
        drop(meta);
        drop(db);

        rollback_to_height(&data_dir, 0).unwrap();

        let (tip_h, tip_hash, tip_cw, tip_bits) = tip_fields(&data_dir).unwrap();
        assert_eq!(tip_h, 0);
        assert_eq!(tip_hash, genesis_hash(Network::Mainnet));
        assert_eq!(tip_cw, 0);
        assert_eq!(tip_bits, pow_start_bits(Network::Mainnet));

        let db = open_db(&data_dir).unwrap();
        db.flush().unwrap();
        drop(db);

        bootstrap(&data_dir).unwrap();

        let (tip_h, tip_hash, tip_cw, _tip_bits) = tip_fields(&data_dir).unwrap();
        assert_eq!(tip_h, 0);
        assert_eq!(tip_hash, genesis_hash(Network::Mainnet));
        assert_eq!(tip_cw, 0);
        assert!(block_at(&data_dir, 1).is_none());
        assert_eq!(
            expected_bits_next(&data_dir).unwrap(),
            pow_start_bits(Network::Mainnet)
        );
    }

    #[test]
    fn tip_fields_repairs_meta_tip_hash_from_block_store() {
        let data_dir = temp_datadir("tip-fields-repair");
        ensure_datadir_meta(&data_dir, "mainnet").unwrap();
        bootstrap(&data_dir).unwrap();

        let db = open_db(&data_dir).unwrap();
        let meta = tree_meta(&db).unwrap();
        let blocks = tree_blocks(&db).unwrap();

        let block1 = ChainBlock {
            height: 1,
            hash32: "11".repeat(32),
            bits: pow_start_bits(Network::Mainnet),
            chainwork: work_from_bits(pow_start_bits(Network::Mainnet)),
            timestamp: Some(1),
            prevhash32: Some(genesis_hash(Network::Mainnet).to_string()),
            merkle32: Some("22".repeat(32)),
            nonce: Some(0),
            miner: Some("miner-a".to_string()),
            pow_digest32: Some("11".repeat(32)),
            txs: None,
        };
        put_block_db(&blocks, &block1).unwrap();
        set_tip_fields_db(
            &meta,
            1,
            "ff".repeat(32),
            block1.chainwork,
            block1.bits,
        )
        .unwrap();
        db.flush().unwrap();
        drop(blocks);
        drop(meta);
        drop(db);

        let (tip_h, tip_hash, _tip_cw, _tip_bits) = tip_fields(&data_dir).unwrap();
        assert_eq!(tip_h, 1);
        assert_eq!(tip_hash, block1.hash32);
    }

    #[test]
    fn mandatory_recovery_forces_bits_19_at_recovery_height() {
        let data_dir = temp_datadir("mandatory-recovery-r");
        ensure_datadir_meta(&data_dir, "mainnet").unwrap();
        bootstrap(&data_dir).unwrap();

        let recovery_h = duta_core::netparams::pow_mandatory_recovery_height(Network::Mainnet)
            .expect("mainnet recovery height");
        let db = open_db(&data_dir).unwrap();
        let meta = tree_meta(&db).unwrap();
        let blocks = tree_blocks(&db).unwrap();

        let mut prev_hash = genesis_hash(Network::Mainnet).to_string();
        let mut cw = 0u64;
        for h in 1..recovery_h {
            let bits = 34u64;
            cw = cw.saturating_add(work_from_bits(bits));
            let block = ChainBlock {
                height: h,
                hash32: format!("{:064x}", h),
                bits,
                chainwork: cw,
                timestamp: Some(h.saturating_mul(pow_target_secs(Network::Mainnet))),
                prevhash32: Some(prev_hash.clone()),
                merkle32: Some(format!("{:064x}", h + 10_000)),
                nonce: Some(0),
                miner: Some("miner-r".to_string()),
                pow_digest32: Some(format!("{:064x}", h + 20_000)),
                txs: None,
            };
            put_block_db(&blocks, &block).unwrap();
            prev_hash = block.hash32.clone();
        }
        set_tip_fields_db(&meta, recovery_h - 1, prev_hash, cw, 34).unwrap();
        db.flush().unwrap();

        assert_eq!(expected_bits_next(&data_dir).unwrap(), 19);
    }

    #[test]
    fn mandatory_recovery_returns_to_normal_after_r_plus_19() {
        let data_dir = temp_datadir("mandatory-recovery-exit");
        ensure_datadir_meta(&data_dir, "mainnet").unwrap();
        bootstrap(&data_dir).unwrap();

        let recovery_h = duta_core::netparams::pow_mandatory_recovery_height(Network::Mainnet)
            .expect("mainnet recovery height");
        let recovery_end = recovery_h + 19;
        let db = open_db(&data_dir).unwrap();
        let meta = tree_meta(&db).unwrap();
        let blocks = tree_blocks(&db).unwrap();

        let mut prev_hash = genesis_hash(Network::Mainnet).to_string();
        let mut cw = 0u64;
        for h in 1..=recovery_end {
            let bits = if h < recovery_h {
                34
            } else if h < recovery_h + 5 {
                19
            } else if h < recovery_h + 10 {
                20
            } else if h < recovery_h + 15 {
                21
            } else {
                22
            };
            cw = cw.saturating_add(work_from_bits(bits));
            let block = ChainBlock {
                height: h,
                hash32: format!("{:064x}", 100_000 + h),
                bits,
                chainwork: cw,
                timestamp: Some(h.saturating_mul(pow_target_secs(Network::Mainnet))),
                prevhash32: Some(prev_hash.clone()),
                merkle32: Some(format!("{:064x}", 200_000 + h)),
                nonce: Some(0),
                miner: Some("miner-exit".to_string()),
                pow_digest32: Some(format!("{:064x}", 300_000 + h)),
                txs: None,
            };
            put_block_db(&blocks, &block).unwrap();
            prev_hash = block.hash32.clone();
        }
        set_tip_fields_db(&meta, recovery_end, prev_hash, cw, 21).unwrap();
        db.flush().unwrap();

        assert_eq!(expected_bits_next(&data_dir).unwrap(), 22);
    }

    #[test]
    fn mandatory_recovery_stages_bits_through_window() {
        let data_dir = temp_datadir("mandatory-recovery-hold");
        ensure_datadir_meta(&data_dir, "mainnet").unwrap();
        bootstrap(&data_dir).unwrap();

        let recovery_h = duta_core::netparams::pow_mandatory_recovery_height(Network::Mainnet)
            .expect("mainnet recovery height");
        let db = open_db(&data_dir).unwrap();
        let meta = tree_meta(&db).unwrap();
        let blocks = tree_blocks(&db).unwrap();

        let mut prev_hash = genesis_hash(Network::Mainnet).to_string();
        let mut cw = 0u64;
        for h in 1..=recovery_h {
            let bits = if h < recovery_h { 34 } else { 19 };
            cw = cw.saturating_add(work_from_bits(bits));
            let block = ChainBlock {
                height: h,
                hash32: format!("{:064x}", 400_000 + h),
                bits,
                chainwork: cw,
                timestamp: Some(h.saturating_mul(pow_target_secs(Network::Mainnet))),
                prevhash32: Some(prev_hash.clone()),
                merkle32: Some(format!("{:064x}", 500_000 + h)),
                nonce: Some(0),
                miner: Some("miner-hold".to_string()),
                pow_digest32: Some(format!("{:064x}", 600_000 + h)),
                txs: None,
            };
            put_block_db(&blocks, &block).unwrap();
            prev_hash = block.hash32.clone();
        }
        set_tip_fields_db(&meta, recovery_h, prev_hash, cw, 19).unwrap();
        db.flush().unwrap();

        assert_eq!(expected_bits_next(&data_dir).unwrap(), 19);
    }

    #[test]
    fn mandatory_recovery_advances_stage_floors() {
        let data_dir = temp_datadir("mandatory-recovery-stage-floors");
        ensure_datadir_meta(&data_dir, "mainnet").unwrap();
        bootstrap(&data_dir).unwrap();

        let recovery_h = duta_core::netparams::pow_mandatory_recovery_height(Network::Mainnet)
            .expect("mainnet recovery height");
        let db = open_db(&data_dir).unwrap();
        let meta = tree_meta(&db).unwrap();
        let blocks = tree_blocks(&db).unwrap();

        let mut prev_hash = genesis_hash(Network::Mainnet).to_string();
        let mut cw = 0u64;
        for h in 1..=(recovery_h + 14) {
            let bits = if h < recovery_h {
                34
            } else if h < recovery_h + 5 {
                19
            } else if h < recovery_h + 10 {
                20
            } else {
                21
            };
            cw = cw.saturating_add(work_from_bits(bits));
            let block = ChainBlock {
                height: h,
                hash32: format!("{:064x}", 700_000 + h),
                bits,
                chainwork: cw,
                timestamp: Some(h.saturating_mul(pow_target_secs(Network::Mainnet))),
                prevhash32: Some(prev_hash.clone()),
                merkle32: Some(format!("{:064x}", 800_000 + h)),
                nonce: Some(0),
                miner: Some("miner-stage".to_string()),
                pow_digest32: Some(format!("{:064x}", 900_000 + h)),
                txs: None,
            };
            put_block_db(&blocks, &block).unwrap();
            prev_hash = block.hash32.clone();
        }
        set_tip_fields_db(&meta, recovery_h + 14, prev_hash, cw, 21).unwrap();
        db.flush().unwrap();

        assert_eq!(expected_bits_next(&data_dir).unwrap(), 22);
    }

    #[test]
    fn mandatory_recovery_stage_is_floor_not_exact_override() {
        let data_dir = temp_datadir("mandatory-recovery-stage-floor-only");
        ensure_datadir_meta(&data_dir, "mainnet").unwrap();
        bootstrap(&data_dir).unwrap();

        let recovery_h = duta_core::netparams::pow_mandatory_recovery_height(Network::Mainnet)
            .expect("mainnet recovery height");
        let db = open_db(&data_dir).unwrap();
        let meta = tree_meta(&db).unwrap();
        let blocks = tree_blocks(&db).unwrap();

        let tip_h = recovery_h + 6;
        let mut prev_hash = genesis_hash(Network::Mainnet).to_string();
        let mut cw = 0u64;
        for h in 1..=tip_h {
            let bits = if h < recovery_h { 34 } else { 21 };
            cw = cw.saturating_add(work_from_bits(bits));
            let block = ChainBlock {
                height: h,
                hash32: format!("{:064x}", 1_000_000 + h),
                bits,
                chainwork: cw,
                timestamp: Some(h.saturating_mul(pow_target_secs(Network::Mainnet))),
                prevhash32: Some(prev_hash.clone()),
                merkle32: Some(format!("{:064x}", 1_100_000 + h)),
                nonce: Some(0),
                miner: Some("miner-stage-floor".to_string()),
                pow_digest32: Some(format!("{:064x}", 1_200_000 + h)),
                txs: None,
            };
            put_block_db(&blocks, &block).unwrap();
            prev_hash = block.hash32.clone();
        }
        set_tip_fields_db(&meta, tip_h, prev_hash, cw, 21).unwrap();
        db.flush().unwrap();

        assert_eq!(expected_bits_next(&data_dir).unwrap(), 21);
    }

    #[test]
    fn validate_candidate_block_repairs_meta_tip_before_prevhash_check() {
        let data_dir = temp_datadir("validate-candidate-block-repair");
        ensure_datadir_meta(&data_dir, "mainnet").unwrap();
        bootstrap(&data_dir).unwrap();

        let db = open_db(&data_dir).unwrap();
        let meta = tree_meta(&db).unwrap();
        let blocks = tree_blocks(&db).unwrap();

        let bits = pow_start_bits(Network::Mainnet);
        let work = work_from_bits(bits);
        let block1 = ChainBlock {
            height: 1,
            hash32: "11".repeat(32),
            bits,
            chainwork: work,
            timestamp: Some(1),
            prevhash32: Some(genesis_hash(Network::Mainnet).to_string()),
            merkle32: Some("22".repeat(32)),
            nonce: Some(0),
            miner: Some("miner-a".to_string()),
            pow_digest32: Some("11".repeat(32)),
            txs: None,
        };
        let block2 = ChainBlock {
            height: 2,
            hash32: "33".repeat(32),
            bits,
            chainwork: work.saturating_add(work),
            timestamp: Some(2),
            prevhash32: Some(block1.hash32.clone()),
            merkle32: Some("44".repeat(32)),
            nonce: Some(0),
            miner: Some("miner-b".to_string()),
            pow_digest32: Some("33".repeat(32)),
            txs: None,
        };
        put_block_db(&blocks, &block1).unwrap();
        set_tip_fields_db(&meta, 1, "ff".repeat(32), work, bits).unwrap();
        db.flush().unwrap();
        drop(blocks);
        drop(meta);
        drop(db);

        let err = validate_candidate_block(&data_dir, &block2).unwrap_err();
        assert_eq!(err, "pow_mismatch");

        let (tip_h, tip_hash, _tip_cw, _tip_bits) = tip_fields(&data_dir).unwrap();
        assert_eq!(tip_h, 1);
        assert_eq!(tip_hash, block1.hash32);
    }

    #[test]
    fn classify_pow_outcome_rejects_legacy_after_activation_lightweight() {
        let provided = H32([0u8; 32]);
        let canonical = H32([0xff; 32]);
        let legacy = H32([0u8; 32]);
        let err = classify_pow_outcome(
            duta_core::dutahash::POW_VERSION_V4,
            8,
            provided,
            canonical,
            0,
            Some(legacy),
        )
        .unwrap_err();
        assert_eq!(err, "legacy_pow_after_activation");
    }

    #[test]
    fn classify_pow_outcome_accepts_matching_canonical_digest_lightweight() {
        let provided = H32([0u8; 32]);
        let canonical = H32([0u8; 32]);
        classify_pow_outcome(
            duta_core::dutahash::POW_VERSION_V4,
            8,
            provided,
            canonical,
            256,
            Some(H32([0xff; 32])),
        )
        .unwrap();
    }
}
