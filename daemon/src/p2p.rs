use crate::store;
use crate::submit_tx;
use crate::{blocks_from_json, ChainBlock};
use duta_core::netparams::{self, Network};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{IpAddr, TcpListener, TcpStream, ToSocketAddrs, UdpSocket};
use std::sync::{
    atomic::{AtomicU64, AtomicUsize, Ordering},
    Mutex, MutexGuard, OnceLock,
};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Msg {
    Hello {
        net: String,
        version: String,
        genesis_hash: String,
    },
    GetTip {},
    Tip {
        height: u64,
        hash32: String,
        chainwork: u64,
        bits: u64,
    },
    GetBlocksFrom {
        from: usize,
        limit: usize,
    },
    Blocks {
        blocks: Vec<ChainBlock>,
    },
    Block {
        block: ChainBlock,
    },
    Tx {
        txid: String,
        tx: serde_json::Value,
    },
    Error {
        error: String,
        detail: String,
    },
}

#[derive(Clone, Debug)]
struct P2pConfig {
    port: String,
    net: String,
    local_ip: String,
    seeds: Vec<String>,
    data_dir: String,
}

#[derive(Clone, Debug, Default)]
struct BootstrapRuntime {
    source: String,
    configured_seed_count: usize,
    seeds_file_count: usize,
    persisted_peer_count: usize,
    active_candidates: Vec<String>,
    last_refresh_at: Option<Instant>,
    last_outbound_ok_peer: Option<String>,
    last_outbound_ok_at: Option<Instant>,
    last_outbound_error_peer: Option<String>,
    last_outbound_error: Option<String>,
    last_outbound_error_at: Option<Instant>,
}

#[derive(Clone, Debug)]
struct BootstrapCandidates {
    source: String,
    configured_seed_count: usize,
    seeds_file_count: usize,
    persisted_peer_count: usize,
    candidates: Vec<String>,
}

static P2P_CFG: OnceLock<P2pConfig> = OnceLock::new();
static BOOTSTRAP_RUNTIME: OnceLock<Mutex<BootstrapRuntime>> = OnceLock::new();

// Highest tip height observed from any peer (best-effort).
static BEST_SEEN_HEIGHT: AtomicU64 = AtomicU64::new(0);

static DIAL_FAIL_LOG_STATE: OnceLock<Mutex<HashMap<String, (Instant, u64, String)>>> =
    OnceLock::new();

static LOG_THROTTLE_STATE: OnceLock<Mutex<HashMap<String, (Instant, u64)>>> = OnceLock::new();

fn hello_genesis_hash(net: &str) -> String {
    let network = Network::parse_name(net).unwrap_or(Network::Mainnet);
    netparams::genesis_hash(network).to_string()
}

fn log_throttle_state() -> &'static Mutex<HashMap<String, (Instant, u64)>> {
    LOG_THROTTLE_STATE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn log_throttled_error(key: &str, interval_secs: u64, msg: impl FnOnce(u64) -> String) {
    let now = Instant::now();
    let mut state = match log_throttle_state().lock() {
        Ok(g) => g,
        Err(_) => {
            edlog!("{}", msg(0));
            return;
        }
    };
    let entry = state.entry(key.to_string()).or_insert((now, 0));
    if now.duration_since(entry.0) >= Duration::from_secs(interval_secs) {
        let suppressed = entry.1;
        *entry = (now, 0);
        edlog!("{}", msg(suppressed));
    } else {
        entry.1 = entry.1.saturating_add(1);
    }
}

fn log_throttled_warn(key: &str, interval_secs: u64, msg: impl FnOnce(u64) -> String) {
    let now = Instant::now();
    let mut state = match log_throttle_state().lock() {
        Ok(g) => g,
        Err(_) => {
            wlog!("{}", msg(0));
            return;
        }
    };
    let entry = state.entry(key.to_string()).or_insert((now, 0));
    if now.duration_since(entry.0) >= Duration::from_secs(interval_secs) {
        let suppressed = entry.1;
        *entry = (now, 0);
        wlog!("{}", msg(suppressed));
    } else {
        entry.1 = entry.1.saturating_add(1);
    }
}

fn dial_fail_log_state() -> &'static Mutex<HashMap<String, (Instant, u64, String)>> {
    DIAL_FAIL_LOG_STATE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn is_transient_dial_error(err: &str) -> bool {
    let lower = err.to_ascii_lowercase();
    lower.contains("resource temporarily unavailable")
        || lower.contains("operation would block")
        || lower.contains("timed out")
}

pub(crate) fn bootstrap_has_healthy_peer() -> bool {
    let now = Instant::now();
    if let Ok(peers) = state().outbound_recent.lock() {
        if peers.values().any(|peer| {
            peer.success_count > 0
                && now.duration_since(peer.last_seen_at) <= Duration::from_secs(300)
        }) {
            return true;
        }
    }
    if let Ok(peers) = state().inbound_live.lock() {
        if !peers.is_empty() {
            return true;
        }
    }
    false
}

fn sync_gate_backbone_targets(net: Network) -> Vec<String> {
    let port = net.default_p2p_port().to_string();
    let mut out: Vec<String> = net
        .default_seed_hosts()
        .iter()
        .map(|host: &&str| format!("{}:{}", host.to_ascii_lowercase(), port))
        .collect();
    if let Some(cfg) = cfg() {
        for seed in cfg.seeds.iter() {
            let addr = candidate_addr(seed.trim(), &port).to_ascii_lowercase();
            if !addr.is_empty() && !out.iter().any(|existing| existing == &addr) {
                out.push(addr);
            }
        }
    }
    out
}

const BACKBONE_FRESHNESS_SECS: u64 = 30;
const BACKBONE_RESYNC_INTERVAL_MS: u64 = 500;
const BACKBONE_DIAL_INTERVAL_SECS: u64 = 1;
const HEALTH_SYNC_PRIORITY_HEIGHT: u64 = 16;

pub fn sync_gate_user_detail(detail: &str) -> &'static str {
    if detail.starts_with("sync_gate_syncing") {
        "waiting_for_chain_sync"
    } else {
        "waiting_for_backbone_sync"
    }
}

fn sync_gate_backbone_views(net: Network) -> Vec<(u64, String)> {
    let expected: HashSet<String> = sync_gate_backbone_targets(net).into_iter().collect();
    if expected.is_empty() {
        return Vec::new();
    }
    let now = Instant::now();
    let freshness = Duration::from_secs(BACKBONE_FRESHNESS_SECS);
    let mut merged: HashMap<String, (Instant, u64, String)> = HashMap::new();

    let collect_peer =
        |peer: &PeerSnapshot,
         require_success: bool,
         merged: &mut HashMap<String, (Instant, u64, String)>| {
            if require_success && peer.success_count == 0 {
                return;
            }
            if now.duration_since(peer.last_seen_at) > freshness {
                return;
            }
            let addr = peer.addr.to_ascii_lowercase();
            if !expected.contains(&addr) {
                return;
            }
            let Some(hash32) = peer.last_tip_hash32.as_ref() else {
                return;
            };
            match merged.get(&addr) {
                Some((seen_at, _, _)) if *seen_at >= peer.last_seen_at => {}
                _ => {
                    merged.insert(
                        addr,
                        (peer.last_seen_at, peer.last_tip_height, hash32.clone()),
                    );
                }
            }
        };

    match state().outbound_recent.lock() {
        Ok(peers) => {
            for peer in peers.values() {
                collect_peer(peer, true, &mut merged);
            }
        }
        Err(poisoned) => {
            for peer in poisoned.into_inner().values() {
                collect_peer(peer, true, &mut merged);
            }
        }
    }
    match state().inbound_live.lock() {
        Ok(peers) => {
            for peer in peers.values() {
                collect_peer(peer, false, &mut merged);
            }
        }
        Err(poisoned) => {
            for peer in poisoned.into_inner().values() {
                collect_peer(peer, false, &mut merged);
            }
        }
    }

    merged
        .into_values()
        .map(|(_, height, hash32)| (height, hash32))
        .collect()
}

fn sync_gate_unhealthy(net: Network, tip_height: u64, tip_hash32: &str) -> bool {
    let best_h = best_seen_height();
    if best_h > tip_height.saturating_add(1) {
        return true;
    }
    let backbone_views = sync_gate_backbone_views(net);
    let backbone_peers = backbone_views.len();
    if backbone_peers < sync_gate_min_backbone_peers(net) {
        return true;
    }
    backbone_views
        .iter()
        .any(|(height, hash32)| *height != tip_height || hash32 != tip_hash32)
}

fn sync_gate_active_now() -> bool {
    let Some(cfg) = cfg() else {
        return false;
    };
    let net = Network::parse_name(&cfg.net).unwrap_or(Network::Mainnet);
    let (tip_height, tip_hash32, tip_bits, _) = tip_fields(&cfg.data_dir);
    netparams::pow_bootstrap_sync_enabled(net, tip_height.saturating_add(1), tip_bits)
        || sync_gate_unhealthy(net, tip_height, &tip_hash32)
}

fn network_health_priority_active() -> bool {
    let Some(cfg) = cfg() else {
        return false;
    };
    let net = Network::parse_name(&cfg.net).unwrap_or(Network::Mainnet);
    if net != Network::Mainnet {
        return false;
    }
    let (tip_height, tip_hash32, _tip_bits, _tip_cw) = tip_fields(&cfg.data_dir);
    tip_height <= HEALTH_SYNC_PRIORITY_HEIGHT || sync_gate_unhealthy(net, tip_height, &tip_hash32)
}

fn is_launch_backbone_peer_for_net(net: Network, addr: &str) -> bool {
    let addr = addr.trim().to_ascii_lowercase();
    sync_gate_backbone_targets(net)
        .into_iter()
        .any(|candidate| candidate == addr)
}

fn sync_gate_backbone_peer_count(net: Network) -> usize {
    sync_gate_backbone_views(net).len()
}

fn sync_gate_min_backbone_peers(net: Network) -> usize {
    match net {
        Network::Mainnet => 2,
        Network::Testnet | Network::Stagenet => 0,
    }
}

fn sync_gate_backbone_heights(net: Network) -> Vec<u64> {
    sync_gate_backbone_views(net)
        .into_iter()
        .map(|(height, _)| height)
        .collect()
}

fn sync_gate_backbone_summary(
    net: Network,
    tip_height: u64,
    tip_hash32: &str,
) -> (usize, usize, usize, Option<u64>, Option<u64>) {
    let views = sync_gate_backbone_views(net);
    let exact_matches = views
        .iter()
        .filter(|(height, hash32)| *height == tip_height && hash32 == tip_hash32)
        .count();
    let one_behind = views
        .iter()
        .filter(|(height, _)| height.saturating_add(1) == tip_height)
        .count();
    let mismatches = views
        .iter()
        .filter(|(height, hash32)| *height != tip_height || hash32 != tip_hash32)
        .count();
    let min_height = views.iter().map(|(height, _)| *height).min();
    let max_height = views.iter().map(|(height, _)| *height).max();
    (
        exact_matches,
        one_behind,
        mismatches,
        min_height,
        max_height,
    )
}

pub fn sync_gate_mining_ready(
    net: Network,
    tip_height: u64,
    tip_hash32: &str,
    current_bits: u64,
) -> Result<(), String> {
    if net != Network::Mainnet {
        return Ok(());
    }
    let hard_sync_window =
        netparams::pow_bootstrap_sync_enabled(net, tip_height.saturating_add(1), current_bits);
    let best_h = best_seen_height();
    let syncing = best_h > tip_height.saturating_add(1);
    let backbone_views = sync_gate_backbone_views(net);
    let backbone_peers = backbone_views.len();
    let min_backbone_peers = sync_gate_min_backbone_peers(net);
    let insufficient_backbone = backbone_peers < min_backbone_peers;
    let backbone_heights: Vec<u64> = backbone_views.iter().map(|(height, _)| *height).collect();
    let backbone_hash_mismatches = backbone_views
        .iter()
        .filter(|(_, hash32)| hash32 != tip_hash32)
        .count();
    let matching_backbone_peers = backbone_views
        .iter()
        .filter(|(height, hash32)| *height == tip_height && hash32 == tip_hash32)
        .count();
    let one_behind_backbone_peers = backbone_views
        .iter()
        .filter(|(height, _)| *height + 1 == tip_height)
        .count();
    let conflicting_tip_hash = backbone_views
        .iter()
        .any(|(height, hash32)| *height == tip_height && hash32 != tip_hash32);
    let ahead_backbone = backbone_views
        .iter()
        .any(|(height, _)| *height > tip_height);
    let far_lagging_backbone = backbone_views
        .iter()
        .any(|(height, _)| height.saturating_add(1) < tip_height);
    let backbone_has_nearby_consensus = if hard_sync_window && tip_height > 1 {
        matching_backbone_peers > 0
    } else {
        matching_backbone_peers > 0 || one_behind_backbone_peers == backbone_peers
    };
    let backbone_mismatch = conflicting_tip_hash
        || ahead_backbone
        || far_lagging_backbone
        || !backbone_has_nearby_consensus;

    if !(hard_sync_window || syncing || insufficient_backbone || backbone_mismatch) {
        return Ok(());
    }
    if syncing {
        return Err(format!(
            "sync_gate_syncing tip_height={} best_seen_height={}",
            tip_height, best_h
        ));
    }
    if insufficient_backbone {
        return Err(format!(
            "sync_gate_official_peer_insufficient tip_height={} best_seen_height={} official_backbone_peers={} required_backbone_peers={}",
            tip_height, best_h, backbone_peers, min_backbone_peers
        ));
    }
    if backbone_mismatch {
        let min_height = backbone_heights.iter().copied().min().unwrap_or(0);
        let max_height = backbone_heights.iter().copied().max().unwrap_or(0);
        return Err(format!(
            "sync_gate_official_tip_mismatch tip_height={} official_min_height={} official_max_height={} official_backbone_peers={} official_hash_mismatches={} official_tip_matches={} official_one_behind_matches={}",
            tip_height, min_height, max_height, backbone_peers, backbone_hash_mismatches, matching_backbone_peers, one_behind_backbone_peers
        ));
    }
    Ok(())
}

#[allow(dead_code)]
pub fn sync_gate_local_submit_ready(
    net: Network,
    block_height: u64,
    block_hash32: &str,
    _current_bits: u64,
) -> Result<(), String> {
    if net != Network::Mainnet {
        return Ok(());
    }
    let backbone_views = sync_gate_backbone_views(net);
    let backbone_peers = backbone_views.len();
    let min_backbone_peers = sync_gate_min_backbone_peers(net);
    if backbone_peers < min_backbone_peers {
        return Err(format!(
            "sync_gate_official_peer_insufficient block_height={} official_backbone_peers={} required_backbone_peers={}",
            block_height, backbone_peers, min_backbone_peers
        ));
    }
    let official_ahead = backbone_views
        .iter()
        .any(|(height, _)| *height > block_height);
    if official_ahead {
        let max_height = backbone_views
            .iter()
            .map(|(height, _)| *height)
            .max()
            .unwrap_or(0);
        return Err(format!(
            "sync_gate_official_ahead block_height={} official_max_height={} official_backbone_peers={}",
            block_height, max_height, backbone_peers
        ));
    }
    let competing_same_height = backbone_views
        .iter()
        .any(|(height, hash32)| *height == block_height && hash32 != block_hash32);
    if competing_same_height {
        return Err(format!(
            "sync_gate_competing_official_tip block_height={} official_backbone_peers={}",
            block_height, backbone_peers
        ));
    }
    Ok(())
}

fn log_dial_failed(addr: &str, err: &str) {
    let healthy = bootstrap_has_healthy_peer();
    let transient = is_transient_dial_error(err);
    let interval_secs = if is_transient_dial_error(err) {
        if healthy {
            1800
        } else {
            300
        }
    } else {
        60
    };
    let now = Instant::now();
    let mut state = match dial_fail_log_state().lock() {
        Ok(g) => g,
        Err(_) => {
            if transient && healthy {
                dlog!(
                    "p2p: bootstrap_retry_transient addr={} err={} bootstrap_health=healthy action=backoff",
                    addr,
                    err
                );
            } else if transient {
                wlog!(
                    "p2p: bootstrap_retry_transient addr={} err={} bootstrap_health=degraded action=backoff",
                    addr,
                    err
                );
            } else {
                wlog!("p2p: bootstrap_retry addr={} err={}", addr, err);
            }
            return;
        }
    };
    let entry = state
        .entry(addr.to_string())
        .or_insert_with(|| (now, 0, String::new()));
    if entry.2 != err {
        let suppressed = entry.1;
        *entry = (now, 0, err.to_string());
        if suppressed > 0 {
            if transient && healthy {
                dlog!(
                    "p2p: bootstrap_retry_transient addr={} err={} bootstrap_health=healthy suppressed={} window_secs={} action=backoff",
                    addr,
                    err,
                    suppressed,
                    interval_secs
                );
            } else if transient {
                wlog!(
                    "p2p: bootstrap_retry_transient addr={} err={} bootstrap_health=degraded suppressed={} window_secs={} action=backoff",
                    addr,
                    err,
                    suppressed,
                    interval_secs
                );
            } else {
                wlog!(
                    "p2p: bootstrap_retry addr={} err={} suppressed={} window_secs={}",
                    addr,
                    err,
                    suppressed,
                    interval_secs
                );
            }
        } else {
            if transient && healthy {
                dlog!(
                    "p2p: bootstrap_retry_transient addr={} err={} bootstrap_health=healthy action=backoff",
                    addr,
                    err
                );
            } else if transient {
                wlog!(
                    "p2p: bootstrap_retry_transient addr={} err={} bootstrap_health=degraded action=backoff",
                    addr,
                    err
                );
            } else {
                wlog!("p2p: bootstrap_retry addr={} err={}", addr, err);
            }
        }
        return;
    }
    if now.duration_since(entry.0) >= Duration::from_secs(interval_secs) {
        let suppressed = entry.1;
        entry.0 = now;
        entry.1 = 0;
        if suppressed > 0 {
            if transient && healthy {
                dlog!(
                    "p2p: bootstrap_retry_transient addr={} err={} bootstrap_health=healthy suppressed={} window_secs={} action=backoff",
                    addr,
                    err,
                    suppressed,
                    interval_secs
                );
            } else if transient {
                wlog!(
                    "p2p: bootstrap_retry_transient addr={} err={} bootstrap_health=degraded suppressed={} window_secs={} action=backoff",
                    addr,
                    err,
                    suppressed,
                    interval_secs
                );
            } else {
                wlog!(
                    "p2p: bootstrap_retry addr={} err={} suppressed={} window_secs={}",
                    addr,
                    err,
                    suppressed,
                    interval_secs
                );
            }
        } else {
            if transient && healthy {
                dlog!(
                    "p2p: bootstrap_retry_transient addr={} err={} bootstrap_health=healthy action=backoff",
                    addr,
                    err
                );
            } else if transient {
                wlog!(
                    "p2p: bootstrap_retry_transient addr={} err={} bootstrap_health=degraded action=backoff",
                    addr,
                    err
                );
            } else {
                wlog!("p2p: bootstrap_retry addr={} err={}", addr, err);
            }
        }
    } else {
        entry.1 = entry.1.saturating_add(1);
    }
}

fn cfg() -> Option<&'static P2pConfig> {
    P2P_CFG.get()
}

fn lock_or_recover<'a, T>(mutex: &'a Mutex<T>, name: &str) -> MutexGuard<'a, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            edlog!("p2p: mutex_poison_recovered name={}", name);
            poisoned.into_inner()
        }
    }
}

fn bootstrap_runtime() -> &'static Mutex<BootstrapRuntime> {
    BOOTSTRAP_RUNTIME.get_or_init(|| Mutex::new(BootstrapRuntime::default()))
}

fn seconds_since(when: Option<Instant>) -> Option<u64> {
    when.map(|ts| ts.elapsed().as_secs())
}

pub fn best_seen_height() -> u64 {
    BEST_SEEN_HEIGHT.load(Ordering::Relaxed)
}

pub fn note_local_tip_height(h: u64) {
    note_seen_height(h);
}

/// Public operator-facing P2P counters (non-consensus). Intended for admin RPC /rpc getpeerinfo/getnetworkinfo.
pub fn p2p_public_info() -> serde_json::Value {
    let st = state();
    let connections = st.inbound_total.load(Ordering::Relaxed) as u64;
    let best_h = best_seen_height();
    let launch_cfg = cfg()
        .map(|c| Network::parse_name(&c.net).unwrap_or(Network::Mainnet))
        .unwrap_or(Network::Mainnet);
    let (tip_h, tip_hash32, tip_bits, _) =
        cfg()
            .map(|c| tip_fields(&c.data_dir))
            .unwrap_or((0, "0".repeat(64), 0, 0));
    let launch_backbone_peers = sync_gate_backbone_peer_count(launch_cfg) as u64;
    let launch_backbone_heights = sync_gate_backbone_heights(launch_cfg);
    let launch_backbone_hashes: Vec<String> = sync_gate_backbone_views(launch_cfg)
        .into_iter()
        .map(|(_, hash32)| hash32)
        .collect();
    let (
        launch_backbone_exact_matches,
        launch_backbone_one_behind,
        launch_backbone_mismatches,
        launch_backbone_min_height,
        launch_backbone_max_height,
    ) = sync_gate_backbone_summary(launch_cfg, tip_h, &tip_hash32);
    let launch_required_backbone_peers = sync_gate_min_backbone_peers(launch_cfg) as u64;
    let sync_gate_hard_active =
        netparams::pow_bootstrap_sync_enabled(launch_cfg, tip_h.saturating_add(1), tip_bits);
    let sync_gate_unhealthy_now = sync_gate_unhealthy(launch_cfg, tip_h, &tip_hash32);
    let sync_gate_active = sync_gate_hard_active || sync_gate_unhealthy_now;
    let sync_gate_detail = sync_gate_mining_ready(launch_cfg, tip_h, &tip_hash32, tip_bits).err();
    let network_health_priority = network_health_priority_active();
    let backbone_tip_lag = launch_backbone_max_height
        .unwrap_or(0)
        .saturating_sub(tip_h);
    let total_resync_requests = st.total_resync_requests.load(Ordering::Relaxed);
    let total_competing_tip_events = st.total_competing_tip_events.load(Ordering::Relaxed);
    let total_official_tip_syncs = st.total_official_tip_syncs.load(Ordering::Relaxed);
    let total_reorg_accepts = st.total_reorg_accepts.load(Ordering::Relaxed);
    let total_backbone_tie_reorgs = st.total_backbone_tie_reorgs.load(Ordering::Relaxed);
    let bootstrap_counters = json!({
        "network_health_priority_active": network_health_priority,
        "backbone_tip_lag": backbone_tip_lag,
        "total_resync_requests": total_resync_requests,
        "total_competing_tip_events": total_competing_tip_events,
        "total_official_tip_syncs": total_official_tip_syncs,
        "total_reorg_accepts": total_reorg_accepts,
        "total_backbone_tie_reorgs": total_backbone_tie_reorgs
    });

    let bans = list_banned_entries();
    let ban_count = bans.len() as u64;

    let top_ips = st
        .inbound_per_ip
        .lock()
        .map(|m| {
            let mut v: Vec<(String, u64)> = m
                .iter()
                .map(|(ip, n)| (ip.to_string(), *n as u64))
                .collect();
            v.sort_by_key(|(_, n)| std::cmp::Reverse(*n));
            v.truncate(25);
            v
        })
        .unwrap_or_default();

    let top_subnets = st
        .inbound_per_subnet24
        .lock()
        .map(|m| {
            let mut v: Vec<(String, u64)> = m
                .iter()
                .map(|(k, n)| {
                    (
                        format!("{}.{}.{}.0/24", (k >> 16) & 0xff, (k >> 8) & 0xff, k & 0xff),
                        *n as u64,
                    )
                })
                .collect();
            v.sort_by_key(|(_, n)| std::cmp::Reverse(*n));
            v.truncate(25);
            v
        })
        .unwrap_or_default();

    let (seed_count, configured_seeds, local_ip, p2p_port) = cfg()
        .map(|c| {
            (
                c.seeds.len() as u64,
                c.seeds.clone(),
                c.local_ip.clone(),
                c.port.clone(),
            )
        })
        .unwrap_or_else(|| (0, Vec::new(), String::new(), String::new()));

    let bootstrap = bootstrap_runtime()
        .lock()
        .map(|s| s.clone())
        .unwrap_or_default();

    let inbound_live = st
        .inbound_live
        .lock()
        .map(|m| {
            let mut v: Vec<PeerSnapshot> = m.values().cloned().collect();
            v.sort_by_key(|p| p.connected_at);
            v.into_iter()
                .map(|p| peer_snapshot_json(&p))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let outbound_recent = st
        .outbound_recent
        .lock()
        .map(|m| {
            let mut v: Vec<PeerSnapshot> = m.values().cloned().collect();
            v.sort_by_key(|p| std::cmp::Reverse(p.last_seen_at));
            v.truncate(64);
            v.into_iter()
                .map(|p| peer_snapshot_json(&p))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let outbound_quarantined = outbound_quarantined_peers_json();

    json!({
        "connections": connections,
        "best_seen_height": best_h,
        "sync_gate_active": sync_gate_active,
        "sync_gate_hard_active": sync_gate_hard_active,
        "sync_gate_unhealthy": sync_gate_unhealthy_now,
        "sync_gate_tip_height": tip_h,
        "sync_gate_required_backbone_peers": launch_required_backbone_peers,
        "sync_gate_backbone_peers": launch_backbone_peers,
        "sync_gate_backbone_heights": launch_backbone_heights,
        "sync_gate_backbone_hashes": launch_backbone_hashes,
        "sync_gate_backbone_exact_matches": launch_backbone_exact_matches as u64,
        "sync_gate_backbone_one_behind": launch_backbone_one_behind as u64,
        "sync_gate_backbone_mismatches": launch_backbone_mismatches as u64,
        "sync_gate_backbone_min_height": launch_backbone_min_height,
        "sync_gate_backbone_max_height": launch_backbone_max_height,
        "sync_gate_detail": sync_gate_detail,
        "bootstrap_counters": bootstrap_counters,
        "ban_count": ban_count,
        "bans": bans,
        "seed_count": seed_count,
        "configured_seeds": configured_seeds,
        "local_ip": local_ip,
        "p2p_port": p2p_port,
        "bootstrap_source": bootstrap.source,
        "configured_seed_count": bootstrap.configured_seed_count as u64,
        "seeds_file_count": bootstrap.seeds_file_count as u64,
        "persisted_peer_count": bootstrap.persisted_peer_count as u64,
        "outbound_candidate_count": bootstrap.active_candidates.len() as u64,
        "outbound_candidates": bootstrap.active_candidates,
        "last_bootstrap_refresh_secs": seconds_since(bootstrap.last_refresh_at),
        "last_outbound_ok_peer": bootstrap.last_outbound_ok_peer,
        "last_outbound_ok_secs": seconds_since(bootstrap.last_outbound_ok_at),
        "last_outbound_error_peer": bootstrap.last_outbound_error_peer,
        "last_outbound_error": bootstrap.last_outbound_error,
        "last_outbound_error_secs": seconds_since(bootstrap.last_outbound_error_at),
        "inbound_live_peers": inbound_live,
        "outbound_recent_peers": outbound_recent,
        "outbound_quarantined_peer_count": outbound_quarantined.len() as u64,
        "outbound_quarantined_peers": outbound_quarantined,
        "top_inbound_ips": top_ips,
        "top_inbound_subnets": top_subnets
    })
}

fn note_seen_height(h: u64) {
    // Keep a monotonic best-seen height (best-effort).
    let _ = BEST_SEEN_HEIGHT.fetch_max(h, Ordering::Relaxed);
}

const MAX_INBOUND: usize = 128;
const MAX_INBOUND_PER_IP: usize = 8;
const MAX_INBOUND_PER_SUBNET24: usize = 12;
const MAX_LINE_BYTES: usize = 512 * 1024;
const MAX_MSGS_PER_SEC: u32 = 200;
const MAX_BYTES_PER_SEC: usize = 512 * 1024;
const BAN_THRESHOLD: u32 = 100;
const BAN_TTL_SECS: u64 = 10 * 60;
const IP_SCORE_DECAY_SECS: u64 = 10 * 60;

struct P2pState {
    inbound_total: AtomicUsize,
    inbound_per_ip: Mutex<HashMap<IpAddr, usize>>,
    inbound_per_subnet24: Mutex<HashMap<u32, usize>>,
    bans: Mutex<HashMap<IpAddr, Instant>>,
    ip_scores: Mutex<HashMap<IpAddr, (u32, Instant)>>,
    inbound_live: Mutex<HashMap<String, PeerSnapshot>>,
    outbound_recent: Mutex<HashMap<String, PeerSnapshot>>,
    total_resync_requests: AtomicU64,
    total_competing_tip_events: AtomicU64,
    total_official_tip_syncs: AtomicU64,
    total_reorg_accepts: AtomicU64,
    total_backbone_tie_reorgs: AtomicU64,
}

#[derive(Clone, Debug)]
struct PeerSnapshot {
    addr: String,
    inbound: bool,
    connected_at: Instant,
    last_seen_at: Instant,
    last_tip_height: u64,
    last_tip_hash32: Option<String>,
    last_error: Option<String>,
    success_count: u64,
    failure_count: u64,
    resync_requests: u64,
    competing_tip_events: u64,
    official_tip_syncs: u64,
}

impl PeerSnapshot {
    fn new(addr: &str, inbound: bool) -> Self {
        let now = Instant::now();
        Self {
            addr: addr.to_string(),
            inbound,
            connected_at: now,
            last_seen_at: now,
            last_tip_height: 0,
            last_tip_hash32: None,
            last_error: None,
            success_count: 0,
            failure_count: 0,
            resync_requests: 0,
            competing_tip_events: 0,
            official_tip_syncs: 0,
        }
    }
}

static P2P_STATE: OnceLock<P2pState> = OnceLock::new();
static RESYNC_BACKOFF: OnceLock<Mutex<HashMap<String, Instant>>> = OnceLock::new();
static DIAL_BACKOFF: OnceLock<Mutex<HashMap<String, (u32, Instant)>>> = OnceLock::new();
static KNOWN_PEERS: OnceLock<Mutex<HashMap<String, Instant>>> = OnceLock::new();
static PEERS_LAST_FLUSH: OnceLock<Mutex<Instant>> = OnceLock::new();
static BLOCK_BROADCAST_STATE: OnceLock<Mutex<HashMap<String, Instant>>> = OnceLock::new();

const SEEDS_FILE: &str = "seeds.txt";
const PEERS_FILE: &str = "peers.txt";
const PEERS_MAX: usize = 512;
const PEERS_FLUSH_INTERVAL_SECS: u64 = 15;
// A larger batch window helps peers discover older fork points and converge
// without needing a full datadir wipe after short-lived network splits.
const MAX_BLOCKS_PER_MSG: usize = 128;
const MAX_PEER_TOKEN_LEN: usize = 255;
const OUTBOUND_BAD_PEER_COOLDOWN_SECS: u64 = 30 * 60;
const OUTBOUND_BAD_PEER_MIN_FAILS: u64 = 6;
const OUTBOUND_BAD_PEER_DOMINANT_FAILS: u64 = 12;
const OUTBOUND_BAD_PEER_MAX_EXPOSED: usize = 32;
const MAX_OUTBOUND_DIALS_PER_TICK: usize = 32;
const BLOCK_BROADCAST_FANOUT: usize = 16;
const BLOCK_BROADCAST_MIN_INTERVAL_MS: u64 = 500;
const TX_BROADCAST_FANOUT: usize = 8;
const CONNECT_TIMEOUT_SECS: u64 = 5;
const HEALTH_PRIORITY_BROADCAST_WAIT_MS: u64 = 500;
const CONNECT_TRANSIENT_RETRIES: usize = 3;
const CONNECT_TRANSIENT_RETRY_DELAY_MS: u64 = 200;

fn should_resync(peer: &str, min_interval: Duration) -> bool {
    let m = RESYNC_BACKOFF.get_or_init(|| Mutex::new(HashMap::new()));
    let mut g = lock_or_recover(m, "resync_backoff");
    let now = Instant::now();
    match g.get(peer).copied() {
        Some(last) if now.duration_since(last) < min_interval => false,
        _ => {
            g.insert(peer.to_string(), now);
            true
        }
    }
}

fn dial_should_try(addr: &str) -> bool {
    let m = DIAL_BACKOFF.get_or_init(|| Mutex::new(HashMap::new()));
    let g = lock_or_recover(m, "dial_backoff");
    let now = Instant::now();
    match g.get(addr).copied() {
        Some((_fails, next)) if now < next => false,
        _ => true,
    }
}

fn dial_note_result(addr: &str, ok: bool) {
    let m = DIAL_BACKOFF.get_or_init(|| Mutex::new(HashMap::new()));
    let mut g = lock_or_recover(m, "dial_backoff");
    let now = Instant::now();
    if ok {
        g.remove(addr);
        return;
    }
    let (fails, _) = g.get(addr).copied().unwrap_or((0, now));
    let fails = fails.saturating_add(1);
    // Exponential-ish backoff, capped.
    let mut wait = 1u64 << fails.min(6); // 2..64s
    if wait < 2 {
        wait = 2;
    }
    if wait > 60 {
        wait = 60;
    }
    g.insert(addr.to_string(), (fails, now + Duration::from_secs(wait)));
}

fn bounded_peer_map_insert(
    map: &mut HashMap<String, PeerSnapshot>,
    key: String,
    value: PeerSnapshot,
    max_entries: usize,
) {
    map.insert(key, value);
    if map.len() <= max_entries {
        return;
    }
    let mut oldest_key: Option<String> = None;
    let mut oldest_seen = Instant::now();
    for (k, v) in map.iter() {
        if oldest_key.is_none() || v.last_seen_at < oldest_seen {
            oldest_seen = v.last_seen_at;
            oldest_key = Some(k.clone());
        }
    }
    if let Some(k) = oldest_key {
        map.remove(&k);
    }
}

fn is_valid_peer_host(host: &str) -> bool {
    if host.is_empty() || host.len() > MAX_PEER_TOKEN_LEN {
        return false;
    }
    if host.starts_with('.') || host.ends_with('.') || host.starts_with('-') || host.ends_with('-')
    {
        return false;
    }
    host.chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | ':'))
}

fn canonicalize_peer_token(peer: &str) -> Option<String> {
    let token = peer.trim().trim_matches('/');
    if token.is_empty() || token.len() > MAX_PEER_TOKEN_LEN {
        return None;
    }
    if token.contains("://")
        || token.contains('/')
        || token.contains('\\')
        || token.contains('?')
        || token.contains('#')
        || token.chars().any(|c| c.is_whitespace())
    {
        return None;
    }

    if token.starts_with('[') {
        return token
            .parse::<std::net::SocketAddr>()
            .ok()
            .map(|addr| addr.to_string());
    }

    if let Ok(addr) = token.parse::<std::net::SocketAddr>() {
        return Some(addr.to_string());
    }

    if token.matches(':').count() > 1 {
        return token
            .parse::<std::net::IpAddr>()
            .ok()
            .map(|ip| ip.to_string());
    }

    if let Some((host, port)) = token.rsplit_once(':') {
        if !host.is_empty()
            && port.parse::<u16>().ok().filter(|p| *p > 0).is_some()
            && is_valid_peer_host(host)
        {
            return Some(format!("{}:{}", host.to_ascii_lowercase(), port));
        }
    }

    if token.parse::<std::net::IpAddr>().is_ok() || is_valid_peer_host(token) {
        return Some(token.to_ascii_lowercase());
    }

    None
}

fn parse_peers_text(s: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut seen = HashSet::new();
    for line in s.lines() {
        let mut x = line.trim();
        if x.is_empty() {
            continue;
        }
        if let Some((a, _)) = x.split_once('#') {
            x = a.trim();
        }
        if x.is_empty() {
            continue;
        }
        // Allow comma/space separated lists too.
        for part in x.split(|c: char| c == ',' || c.is_whitespace()) {
            let p = part.trim();
            if p.is_empty() {
                continue;
            }
            if let Some(canonical) = canonicalize_peer_token(p) {
                let key = canonical.to_ascii_lowercase();
                if seen.insert(key) {
                    out.push(canonical);
                }
            }
        }
    }
    out
}

fn built_in_seed_hosts(net: &str) -> Vec<String> {
    duta_core::netparams::Network::parse_name(net)
        .unwrap_or(duta_core::netparams::Network::Mainnet)
        .default_seed_hosts()
        .iter()
        .map(|host| host.to_string())
        .collect()
}

fn resolve_peer_sockets(peer: &str, default_port: &str) -> Vec<String> {
    let addr = candidate_addr(peer, default_port);
    match addr.to_socket_addrs() {
        Ok(resolved) => {
            let mut seen = HashSet::new();
            let mut out = Vec::new();
            for socket in resolved {
                let key = socket.to_string();
                if seen.insert(key.clone()) {
                    out.push(key);
                }
            }
            out
        }
        Err(_) => Vec::new(),
    }
}

fn normalize_bootstrap_candidates(raw: Vec<String>, default_port: &str) -> Vec<String> {
    let mut token_dedup = HashSet::new();
    let mut resolved_dedup = HashSet::new();
    let mut out = Vec::new();
    for item in raw {
        let Some(v) = canonicalize_peer_token(&item) else {
            continue;
        };
        let token_key = v.to_ascii_lowercase();
        if !token_dedup.insert(token_key) {
            continue;
        }
        let resolved = resolve_peer_sockets(&v, default_port);
        if !resolved.is_empty() {
            let mut duplicate = false;
            for socket in resolved {
                if !resolved_dedup.insert(socket) {
                    duplicate = true;
                }
            }
            if duplicate {
                continue;
            }
        }
        out.push(v);
    }
    out
}

fn load_bootstrap_candidates(
    data_dir: &str,
    net: &str,
    default_port: &str,
    configured_seeds: &[String],
) -> BootstrapCandidates {
    let seeds_file = load_list_file(data_dir, SEEDS_FILE);
    let persisted = load_list_file(data_dir, PEERS_FILE);
    let mut source_parts: Vec<&str> = Vec::new();

    let mut base = if !configured_seeds.is_empty() {
        source_parts.push("configured");
        configured_seeds.to_vec()
    } else if !seeds_file.is_empty() {
        source_parts.push("seeds_file");
        seeds_file.clone()
    } else {
        source_parts.push("built_in");
        built_in_seed_hosts(net)
    };

    if !persisted.is_empty() {
        source_parts.push("persisted_peers");
        base.extend(persisted.iter().cloned());
    }

    BootstrapCandidates {
        source: source_parts.join("+"),
        configured_seed_count: configured_seeds.len(),
        seeds_file_count: seeds_file.len(),
        persisted_peer_count: persisted.len(),
        candidates: normalize_bootstrap_candidates(base, default_port),
    }
}

fn note_bootstrap_refresh(snapshot: &BootstrapCandidates) {
    let now = Instant::now();
    let mut runtime = match bootstrap_runtime().lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    };
    let changed = runtime.source != snapshot.source
        || runtime.configured_seed_count != snapshot.configured_seed_count
        || runtime.seeds_file_count != snapshot.seeds_file_count
        || runtime.persisted_peer_count != snapshot.persisted_peer_count
        || runtime.active_candidates != snapshot.candidates;

    runtime.source = snapshot.source.clone();
    runtime.configured_seed_count = snapshot.configured_seed_count;
    runtime.seeds_file_count = snapshot.seeds_file_count;
    runtime.persisted_peer_count = snapshot.persisted_peer_count;
    runtime.active_candidates = snapshot.candidates.clone();
    runtime.last_refresh_at = Some(now);

    if changed {
        dlog!(
            "p2p: bootstrap_refresh source={} candidates={} configured={} seeds_file={} persisted={}",
            runtime.source,
            runtime.active_candidates.len(),
            runtime.configured_seed_count,
            runtime.seeds_file_count,
            runtime.persisted_peer_count
        );
    }
}

fn note_outbound_result(addr: &str, ok: bool, err: Option<&str>) {
    let now = Instant::now();
    let mut runtime = match bootstrap_runtime().lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    };
    if ok {
        runtime.last_outbound_ok_peer = Some(addr.to_string());
        runtime.last_outbound_ok_at = Some(now);
        runtime.last_outbound_error_peer = None;
        runtime.last_outbound_error = None;
        runtime.last_outbound_error_at = None;
    } else if let Some(err) = err {
        runtime.last_outbound_error_peer = Some(addr.to_string());
        runtime.last_outbound_error = Some(err.to_string());
        runtime.last_outbound_error_at = Some(now);
    }
}

fn outbound_candidates() -> Vec<String> {
    bootstrap_runtime()
        .lock()
        .map(|s| s.active_candidates.clone())
        .unwrap_or_else(|_| cfg().map(|c| c.seeds.clone()).unwrap_or_default())
}

fn load_list_file(data_dir: &str, filename: &str) -> Vec<String> {
    let path = format!("{}/{}", data_dir.trim_end_matches('/'), filename);
    match fs::read_to_string(&path) {
        Ok(s) => parse_peers_text(&s),
        Err(_) => Vec::new(),
    }
}

fn peers_path(data_dir: &str) -> String {
    format!("{}/{}", data_dir.trim_end_matches('/'), PEERS_FILE)
}

fn note_peer(data_dir: &str, peer: &str) {
    let Some(peer) = canonicalize_peer_token(peer) else {
        return;
    };
    let m = KNOWN_PEERS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut g = lock_or_recover(m, "known_peers");
    let now = Instant::now();
    g.insert(peer, now);
    // Prune to a max size by removing oldest entries.
    if g.len() > PEERS_MAX {
        let mut v: Vec<(String, Instant)> = g.iter().map(|(k, t)| (k.clone(), *t)).collect();
        v.sort_by_key(|(_, t)| *t);
        let drop_n = g.len().saturating_sub(PEERS_MAX);
        for (k, _) in v.into_iter().take(drop_n) {
            g.remove(&k);
        }
    }

    // Throttled flush.
    let last_m = PEERS_LAST_FLUSH.get_or_init(|| Mutex::new(Instant::now()));
    if let Ok(mut last) = last_m.lock() {
        if now.duration_since(*last) >= Duration::from_secs(PEERS_FLUSH_INTERVAL_SECS) {
            *last = now;
            drop(g);
            flush_peers(data_dir);
        }
    }
}

fn flush_peers(data_dir: &str) {
    let m = match KNOWN_PEERS.get() {
        Some(m) => m,
        None => return,
    };
    let g = match m.lock() {
        Ok(g) => g,
        Err(_) => return,
    };

    // Sort by most-recent.
    let mut v: Vec<(String, Instant)> = g.iter().map(|(k, t)| (k.clone(), *t)).collect();
    v.sort_by_key(|(_, t)| std::cmp::Reverse(*t));

    let mut out = String::new();
    out.push_str("# Known peers (auto-updated). One IP/host per line.\n");
    for (k, _) in v.into_iter().take(PEERS_MAX) {
        out.push_str(&k);
        out.push('\n');
    }

    let path = peers_path(data_dir);
    let _ = store::durable_write_string(&path, &out);
}

/// Manually add a peer/seed to the persisted peers list (peers.txt).
/// This is intended for operator/GUI use (bitcoin-like "addnode").
/// It does not guarantee an immediate connection, but it will be picked up by the outbound dial loop.
pub fn add_peer_manual(data_dir: &str, peer: &str) -> Result<(), String> {
    let peer = canonicalize_peer_token(peer).ok_or_else(|| "invalid_peer".to_string())?;
    // Record and flush immediately so it persists across restarts.
    note_peer(data_dir, &peer);
    flush_peers(data_dir);

    // Clear dial backoff for this peer so the outbound loop can try immediately.
    if let Some(m) = DIAL_BACKOFF.get() {
        if let Ok(mut g) = m.lock() {
            g.remove(&peer);
        }
    }
    Ok(())
}

fn note_inbound_peer_connected(addr: &str) {
    let mut peers = match state().inbound_live.lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    };
    peers.insert(addr.to_string(), PeerSnapshot::new(addr, true));
}

fn note_inbound_peer_tip(addr: &str, height: u64, hash32: &str) {
    let mut peers = match state().inbound_live.lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    };
    let now = Instant::now();
    let entry = peers
        .entry(addr.to_string())
        .or_insert_with(|| PeerSnapshot::new(addr, true));
    entry.last_seen_at = now;
    entry.last_tip_height = height;
    entry.last_tip_hash32 = Some(hash32.to_string());
    entry.last_error = None;
}

fn note_inbound_peer_error(addr: &str, err: &str) {
    let mut peers = match state().inbound_live.lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    };
    let entry = peers
        .entry(addr.to_string())
        .or_insert_with(|| PeerSnapshot::new(addr, true));
    entry.last_seen_at = Instant::now();
    entry.last_error = Some(err.to_string());
}

fn note_inbound_peer_disconnected(addr: &str) {
    if let Ok(mut peers) = state().inbound_live.lock() {
        peers.remove(addr);
    }
}

fn note_outbound_peer_result(
    addr: &str,
    ok: bool,
    height: Option<u64>,
    hash32: Option<&str>,
    err: Option<&str>,
) {
    const OUTBOUND_RECENT_MAX: usize = 128;
    let mut peers = match state().outbound_recent.lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    };
    let now = Instant::now();
    let mut entry = peers
        .remove(addr)
        .unwrap_or_else(|| PeerSnapshot::new(addr, false));
    entry.last_seen_at = now;
    if let Some(h) = height {
        entry.last_tip_height = h;
    }
    if let Some(hash32) = hash32 {
        entry.last_tip_hash32 = Some(hash32.to_string());
    }
    if ok {
        entry.success_count = entry.success_count.saturating_add(1);
        entry.last_error = None;
    } else {
        entry.failure_count = entry.failure_count.saturating_add(1);
        if let Some(err) = err {
            entry.last_error = Some(err.to_string());
        }
    }
    bounded_peer_map_insert(&mut peers, addr.to_string(), entry, OUTBOUND_RECENT_MAX);
}

fn note_peer_resync(addr: &str, competing_tip: bool, official_tip_sync: bool) {
    let st = state();
    st.total_resync_requests.fetch_add(1, Ordering::Relaxed);
    if competing_tip {
        st.total_competing_tip_events
            .fetch_add(1, Ordering::Relaxed);
    }
    if official_tip_sync {
        st.total_official_tip_syncs.fetch_add(1, Ordering::Relaxed);
    }
    if let Ok(mut peers) = state().outbound_recent.lock() {
        if let Some(peer) = peers.get_mut(addr) {
            peer.resync_requests = peer.resync_requests.saturating_add(1);
            if competing_tip {
                peer.competing_tip_events = peer.competing_tip_events.saturating_add(1);
            }
            if official_tip_sync {
                peer.official_tip_syncs = peer.official_tip_syncs.saturating_add(1);
            }
        }
    }
    if let Ok(mut peers) = state().inbound_live.lock() {
        if let Some(peer) = peers.get_mut(addr) {
            peer.resync_requests = peer.resync_requests.saturating_add(1);
            if competing_tip {
                peer.competing_tip_events = peer.competing_tip_events.saturating_add(1);
            }
            if official_tip_sync {
                peer.official_tip_syncs = peer.official_tip_syncs.saturating_add(1);
            }
        }
    }
}

fn note_reorg_accept(backbone_tie: bool) {
    let st = state();
    st.total_reorg_accepts.fetch_add(1, Ordering::Relaxed);
    if backbone_tie {
        st.total_backbone_tie_reorgs.fetch_add(1, Ordering::Relaxed);
    }
}

fn peer_snapshot_json(peer: &PeerSnapshot) -> serde_json::Value {
    json!({
        "addr": peer.addr,
        "inbound": peer.inbound,
        "connected_secs": peer.connected_at.elapsed().as_secs(),
        "last_seen_secs": peer.last_seen_at.elapsed().as_secs(),
        "last_tip_height": peer.last_tip_height,
        "last_tip_hash32": peer.last_tip_hash32,
        "last_error": peer.last_error,
        "success_count": peer.success_count,
        "failure_count": peer.failure_count,
        "resync_requests": peer.resync_requests,
        "competing_tip_events": peer.competing_tip_events,
        "official_tip_syncs": peer.official_tip_syncs
    })
}

fn outbound_peer_skip_reason(peer: &PeerSnapshot) -> Option<&'static str> {
    if peer.last_seen_at.elapsed() > Duration::from_secs(OUTBOUND_BAD_PEER_COOLDOWN_SECS) {
        return None;
    }
    if peer.success_count == 0 && peer.failure_count >= OUTBOUND_BAD_PEER_MIN_FAILS {
        return Some("recent_failures");
    }
    if peer.failure_count >= OUTBOUND_BAD_PEER_DOMINANT_FAILS
        && peer.failure_count >= peer.success_count.saturating_mul(4).saturating_add(4)
    {
        return Some("failure_dominant");
    }
    None
}

fn outbound_peer_should_skip(addr: &str) -> Option<String> {
    if let Some(cfg) = cfg() {
        let net = Network::parse_name(&cfg.net).unwrap_or(Network::Mainnet);
        if sync_gate_active_now() && is_launch_backbone_peer_for_net(net, addr) {
            return None;
        }
    }
    let peers = match state().outbound_recent.lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    };
    peers
        .get(addr)
        .and_then(outbound_peer_skip_reason)
        .map(|s| s.to_string())
}

fn candidate_addr(peer: &str, port: &str) -> String {
    if peer.contains(':') {
        peer.to_string()
    } else {
        format!("{}:{}", peer, port)
    }
}

fn outbound_peer_quality(addr: &str) -> i64 {
    let peers = match state().outbound_recent.lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    };
    let official_backbone = cfg()
        .and_then(|cfg| Network::parse_name(&cfg.net))
        .map(|net| is_launch_backbone_peer_for_net(net, addr))
        .unwrap_or(false);
    let Some(peer) = peers.get(addr) else {
        return if official_backbone { 1_000_000 } else { 0 };
    };
    let recency_bonus = if peer.last_seen_at.elapsed() <= Duration::from_secs(15 * 60) {
        3
    } else if peer.last_seen_at.elapsed() <= Duration::from_secs(60 * 60) {
        1
    } else {
        0
    };
    let backbone_bonus = if official_backbone { 1_000_000 } else { 0 };
    backbone_bonus + peer.success_count as i64 * 4
        - peer.failure_count as i64 * 3
        - peer.competing_tip_events as i64 * 8
        - peer.resync_requests as i64
        + peer.official_tip_syncs as i64 * 6
        + peer.last_tip_height as i64 / 1024
        + recency_bonus
}

fn sync_gate_resync_interval(net: Network, peer: &str) -> Duration {
    if net == Network::Mainnet
        && (network_health_priority_active() || is_launch_backbone_peer_for_net(net, peer))
    {
        Duration::from_millis(BACKBONE_RESYNC_INTERVAL_MS)
    } else {
        Duration::from_secs(2)
    }
}

fn official_tip_sync_from(
    net: Network,
    peer: &str,
    local_h: u64,
    local_hash32: &str,
    remote_h: u64,
    remote_hash32: &str,
) -> usize {
    if net == Network::Mainnet
        && is_launch_backbone_peer_for_net(net, peer)
        && (remote_h > local_h || (remote_h == local_h && remote_hash32 != local_hash32))
    {
        if network_health_priority_active() {
            health_priority_sync_from(local_h)
        } else {
            reorg_overlap_from(local_h)
        }
    } else {
        (local_h + 1) as usize
    }
}

fn official_sync_request_from(
    net: Network,
    peer: &str,
    local_h: u64,
    local_hash32: &str,
    remote_h: u64,
    remote_hash32: &str,
) -> Option<usize> {
    if net == Network::Mainnet
        && is_launch_backbone_peer_for_net(net, peer)
        && (remote_h > local_h || (remote_h == local_h && remote_hash32 != local_hash32))
    {
        Some(official_tip_sync_from(
            net,
            peer,
            local_h,
            local_hash32,
            remote_h,
            remote_hash32,
        ))
    } else {
        None
    }
}

fn health_priority_sync_from(local_h: u64) -> usize {
    let overlap_from = reorg_overlap_from(local_h);
    let earliest_full_window = local_h
        .saturating_add(2)
        .saturating_sub(MAX_BLOCKS_PER_MSG as u64) as usize;
    overlap_from.min(earliest_full_window)
}

fn outbound_tick_interval() -> Duration {
    if network_health_priority_active() {
        Duration::from_millis(500)
    } else if sync_gate_active_now() {
        Duration::from_secs(BACKBONE_DIAL_INTERVAL_SECS)
    } else {
        Duration::from_secs(2)
    }
}

fn block_broadcast_fanout() -> usize {
    if network_health_priority_active() {
        BLOCK_BROADCAST_FANOUT.saturating_mul(2)
    } else {
        BLOCK_BROADCAST_FANOUT
    }
}

fn broadcast_followup_wait() -> Duration {
    if network_health_priority_active() {
        Duration::from_millis(HEALTH_PRIORITY_BROADCAST_WAIT_MS)
    } else {
        Duration::from_millis(250)
    }
}

fn select_outbound_targets(
    peers: &[String],
    port: &str,
    local_ip: &str,
    limit: usize,
) -> Vec<String> {
    let mut scored: Vec<(i64, String)> = peers
        .iter()
        .filter_map(|peer| {
            if !local_ip.is_empty() && peer == local_ip {
                return None;
            }
            let addr = candidate_addr(peer, port);
            if outbound_peer_should_skip(&addr).is_some() {
                return None;
            }
            Some((outbound_peer_quality(&addr), addr))
        })
        .collect();
    scored.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(&b.1)));
    let official_net = cfg()
        .and_then(|cfg| Network::parse_name(&cfg.net))
        .unwrap_or(Network::Mainnet);
    let mut official = Vec::new();
    let mut other = Vec::new();
    for (_, addr) in scored.into_iter() {
        if is_launch_backbone_peer_for_net(official_net, &addr) {
            official.push(addr);
        } else {
            other.push(addr);
        }
    }
    let mut out = Vec::new();
    for addr in official.into_iter() {
        if out.len() >= limit {
            break;
        }
        out.push(addr);
    }
    if out.len() < limit {
        for addr in other.into_iter() {
            if out.len() >= limit {
                break;
            }
            out.push(addr);
        }
    }
    out
}

fn select_block_broadcast_targets(peers: &[String], port: &str, local_ip: &str) -> Vec<String> {
    let limit = block_broadcast_fanout();
    let mut out = select_outbound_targets(peers, port, local_ip, limit);
    if !network_health_priority_active() {
        return out;
    }
    let net = cfg()
        .and_then(|cfg| Network::parse_name(&cfg.net))
        .unwrap_or(Network::Mainnet);
    let mut seen: HashSet<String> = out.iter().cloned().collect();
    for addr in peers.iter().map(|peer| candidate_addr(peer, port)) {
        if out.len() >= limit {
            break;
        }
        if is_launch_backbone_peer_for_net(net, &addr) && seen.insert(addr.clone()) {
            out.push(addr);
        }
    }
    out
}

fn outbound_quarantined_peers_json() -> Vec<serde_json::Value> {
    let bootstrap_net = cfg()
        .and_then(|cfg| Network::parse_name(&cfg.net))
        .filter(|_| sync_gate_active_now());
    let peers = match state().outbound_recent.lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    };
    let mut out: Vec<(PeerSnapshot, &'static str)> = peers
        .values()
        .filter(|peer| {
            bootstrap_net
                .map(|net| !is_launch_backbone_peer_for_net(net, &peer.addr))
                .unwrap_or(true)
        })
        .filter_map(|peer| outbound_peer_skip_reason(peer).map(|reason| (peer.clone(), reason)))
        .collect();
    out.sort_by_key(|(peer, _)| std::cmp::Reverse(peer.last_seen_at));
    out.truncate(OUTBOUND_BAD_PEER_MAX_EXPOSED);
    out.into_iter()
        .map(|(peer, reason)| {
            json!({
                "addr": peer.addr,
                "reason": reason,
                "last_seen_secs": peer.last_seen_at.elapsed().as_secs(),
                "success_count": peer.success_count,
                "failure_count": peer.failure_count,
                "last_error": peer.last_error
            })
        })
        .collect()
}

fn state() -> &'static P2pState {
    P2P_STATE.get_or_init(|| P2pState {
        inbound_total: AtomicUsize::new(0),
        inbound_per_ip: Mutex::new(HashMap::new()),
        inbound_per_subnet24: Mutex::new(HashMap::new()),
        bans: Mutex::new(HashMap::new()),
        ip_scores: Mutex::new(HashMap::new()),
        inbound_live: Mutex::new(HashMap::new()),
        outbound_recent: Mutex::new(HashMap::new()),
        total_resync_requests: AtomicU64::new(0),
        total_competing_tip_events: AtomicU64::new(0),
        total_official_tip_syncs: AtomicU64::new(0),
        total_reorg_accepts: AtomicU64::new(0),
        total_backbone_tie_reorgs: AtomicU64::new(0),
    })
}

struct PeerGuard {
    ip: IpAddr,
    subnet24: Option<u32>,
}

impl Drop for PeerGuard {
    fn drop(&mut self) {
        let st = state();
        st.inbound_total.fetch_sub(1, Ordering::Relaxed);
        if let Ok(mut m) = st.inbound_per_ip.lock() {
            if let Some(v) = m.get_mut(&self.ip) {
                *v = v.saturating_sub(1);
                if *v == 0 {
                    m.remove(&self.ip);
                }
            }
        }
        if let Some(k) = self.subnet24 {
            if let Ok(mut m) = st.inbound_per_subnet24.lock() {
                if let Some(v) = m.get_mut(&k) {
                    *v = v.saturating_sub(1);
                    if *v == 0 {
                        m.remove(&k);
                    }
                }
            }
        }
    }
}

fn ban_ip(ip: IpAddr, reason: &str) {
    let until = Instant::now() + Duration::from_secs(BAN_TTL_SECS);
    if let Ok(mut b) = state().bans.lock() {
        b.insert(ip, until);
    }
    edlog!(
        "p2p: ban ip={} ttl_secs={} reason={}",
        ip,
        BAN_TTL_SECS,
        reason
    );
}

fn is_banned(ip: IpAddr) -> bool {
    let now = Instant::now();
    if let Ok(mut b) = state().bans.lock() {
        b.retain(|_, &mut until| until > now);
        return b.contains_key(&ip);
    }
    false
}

fn list_banned_entries() -> Vec<serde_json::Value> {
    let now = Instant::now();
    state()
        .bans
        .lock()
        .map(|mut bans| {
            bans.retain(|_, until| *until > now);
            let mut out: Vec<(IpAddr, u64)> = bans
                .iter()
                .map(|(ip, until)| (*ip, until.saturating_duration_since(now).as_secs()))
                .collect();
            out.sort_by(|a, b| a.0.to_string().cmp(&b.0.to_string()));
            out.into_iter()
                .map(|(ip, ttl_secs)| json!({"ip": ip.to_string(), "ttl_secs": ttl_secs}))
                .collect()
        })
        .unwrap_or_default()
}

pub fn list_banned_json() -> serde_json::Value {
    serde_json::Value::Array(list_banned_entries())
}

pub fn ban_peer_manual(ip: &str, reason: Option<&str>) -> Result<serde_json::Value, String> {
    let ip = ip
        .trim()
        .parse::<IpAddr>()
        .map_err(|_| "invalid_ip".to_string())?;
    let reason = reason
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or("manual_operator_ban");
    ban_ip(ip, reason);
    if let Ok(mut scores) = state().ip_scores.lock() {
        scores.remove(&ip);
    }
    Ok(json!({
        "ok": true,
        "ip": ip.to_string(),
        "reason": reason,
        "ttl_secs": BAN_TTL_SECS
    }))
}

pub fn unban_peer_manual(ip: &str) -> Result<serde_json::Value, String> {
    let ip = ip
        .trim()
        .parse::<IpAddr>()
        .map_err(|_| "invalid_ip".to_string())?;
    let removed = state()
        .bans
        .lock()
        .map(|mut bans| bans.remove(&ip).is_some())
        .unwrap_or(false);
    if let Ok(mut scores) = state().ip_scores.lock() {
        scores.remove(&ip);
    }
    dlog!("p2p: unban ip={} removed={}", ip, removed);
    Ok(json!({
        "ok": true,
        "ip": ip.to_string(),
        "removed": removed
    }))
}

fn subnet24_key(ip: IpAddr) -> Option<u32> {
    match ip {
        IpAddr::V4(v4) => {
            let o = v4.octets();
            // For private/loopback ranges, don't apply /24 caps. These are commonly used for trusted LAN/mesh.
            let is_private = (o[0] == 10)
                || (o[0] == 172 && (16..=31).contains(&o[1]))
                || (o[0] == 192 && o[1] == 168)
                || (o[0] == 127);
            if is_private {
                None
            } else {
                Some(((o[0] as u32) << 16) | ((o[1] as u32) << 8) | (o[2] as u32))
            }
        }
        IpAddr::V6(_) => None,
    }
}

fn ip_is_loopback_or_private(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => v4.is_loopback() || v4.is_private(),
        IpAddr::V6(v6) => v6.is_loopback() || v6.is_unique_local(),
    }
}

fn add_ip_score(ip: IpAddr, delta: u32, reason: &str) -> u32 {
    let now = Instant::now();
    let st = state();
    let mut score: u32 = 0;

    if let Ok(mut m) = st.ip_scores.lock() {
        // Drop stale scores.
        m.retain(|_, (_, ts)| ts.elapsed() < Duration::from_secs(IP_SCORE_DECAY_SECS));

        if let Some((s, _)) = m.get(&ip).copied() {
            score = s;
        }
        score = score.saturating_add(delta);

        if score >= BAN_THRESHOLD {
            // Escalate to temp-ban and clear score.
            m.remove(&ip);
            ban_ip(ip, reason);
        } else {
            m.insert(ip, (score, now));
        }
    }
    score
}

fn try_accept_peer(ip: IpAddr) -> Option<PeerGuard> {
    if is_banned(ip) {
        wlog!("p2p: inbound_reject ip={} reason=banned", ip);
        return None;
    }
    let st = state();

    if st.inbound_total.load(Ordering::Relaxed) >= MAX_INBOUND {
        wlog!("p2p: inbound_reject ip={} reason=global_cap", ip);
        return None;
    }

    // Subnet (/24) cap (IPv4 only). This mitigates simple eclipsing with many IPs on the same subnet.
    let subnet24 = subnet24_key(ip);
    if let Some(k) = subnet24 {
        if let Ok(mut m) = st.inbound_per_subnet24.lock() {
            let n = m.get(&k).copied().unwrap_or(0);
            if n >= MAX_INBOUND_PER_SUBNET24 {
                wlog!("p2p: inbound_reject ip={} reason=subnet24_cap", ip);
                return None;
            }
            m.insert(k, n + 1);
        } else {
            edlog!("p2p: inbound_reject ip={} reason=state_lock_failed", ip);
            return None;
        }
    }

    if let Ok(mut m) = st.inbound_per_ip.lock() {
        let n = m.get(&ip).copied().unwrap_or(0);
        if n >= MAX_INBOUND_PER_IP {
            // roll back subnet counter if we incremented it
            if let Some(k) = subnet24 {
                if let Ok(mut sm) = st.inbound_per_subnet24.lock() {
                    if let Some(v) = sm.get_mut(&k) {
                        *v = v.saturating_sub(1);
                        if *v == 0 {
                            sm.remove(&k);
                        }
                    }
                }
            }
            wlog!("p2p: inbound_reject ip={} reason=per_ip_cap", ip);
            return None;
        }
        m.insert(ip, n + 1);
    } else {
        // roll back subnet counter if we incremented it
        if let Some(k) = subnet24 {
            if let Ok(mut sm) = st.inbound_per_subnet24.lock() {
                if let Some(v) = sm.get_mut(&k) {
                    *v = v.saturating_sub(1);
                    if *v == 0 {
                        sm.remove(&k);
                    }
                }
            }
        }
        edlog!("p2p: inbound_reject ip={} reason=state_lock_failed", ip);
        return None;
    }

    st.inbound_total.fetch_add(1, Ordering::Relaxed);
    Some(PeerGuard { ip, subnet24 })
}

fn read_line_limited<R: BufRead>(reader: &mut R, buf: &mut Vec<u8>) -> std::io::Result<usize> {
    buf.clear();
    let n = reader
        .by_ref()
        .take((MAX_LINE_BYTES + 1) as u64)
        .read_until(b'\n', buf)?;
    if n == 0 {
        return Ok(0);
    }
    if buf.len() > MAX_LINE_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "line_too_large",
        ));
    }
    Ok(n)
}

fn detect_local_ip(seed_ip: &str) -> Option<String> {
    // Best-effort local IP detection by "connecting" a UDP socket.
    let sock = UdpSocket::bind("0.0.0.0:0").ok()?;
    let _ = sock.connect((seed_ip, 1));
    sock.local_addr().ok().map(|a| a.ip().to_string())
}

fn reorg_overlap_from(tip_h: u64) -> usize {
    // Keep enough overlap to include a recent fork point, but not so much that a
    // single batch ends before the competing peer's current tip. The overlap should
    // leave room in the same response for blocks above our local tip.
    let overlap = (MAX_BLOCKS_PER_MSG / 2) as u64;
    tip_h.saturating_sub(overlap) as usize
}

fn deeper_overlap_from(window_start_h: u64) -> usize {
    let step = (MAX_BLOCKS_PER_MSG / 2) as u64;
    window_start_h.saturating_sub(step) as usize
}

fn should_accept_reorg_candidate(current_tip_cw: u64, incoming_cw: u64) -> bool {
    incoming_cw > current_tip_cw
}

fn backbone_exact_match_count(net: Network, height: u64, hash32: &str) -> usize {
    sync_gate_backbone_views(net)
        .into_iter()
        .filter(|(backbone_height, backbone_hash32)| {
            *backbone_height == height && backbone_hash32 == hash32
        })
        .count()
}

fn should_prefer_backbone_tie_candidate(
    net: Network,
    peer_addr: &str,
    current_tip_height: u64,
    current_tip_hash32: &str,
    candidate_height: u64,
    candidate_hash32: &str,
    current_tip_cw: u64,
    incoming_cw: u64,
) -> bool {
    if incoming_cw != current_tip_cw {
        return false;
    }
    if net != Network::Mainnet {
        return false;
    }
    if !is_launch_backbone_peer_for_net(net, peer_addr) {
        return false;
    }
    let launch_relevant =
        netparams::pow_bootstrap_sync_enabled(net, candidate_height.saturating_add(1), 0)
            || sync_gate_unhealthy(net, candidate_height, candidate_hash32);
    if !launch_relevant {
        return false;
    }
    let candidate_exact = backbone_exact_match_count(net, candidate_height, candidate_hash32);
    if candidate_exact == 0 {
        return false;
    }
    let current_exact = backbone_exact_match_count(net, current_tip_height, current_tip_hash32);
    candidate_exact > current_exact
        || (candidate_exact > 0 && current_tip_height != candidate_height)
}

fn connect_peer(addr: &str) -> Result<TcpStream, String> {
    let timeout = Duration::from_secs(CONNECT_TIMEOUT_SECS);
    let resolved = addr
        .to_socket_addrs()
        .map_err(|e| format!("resolve_failed:{e}"))?;
    let mut last_err: Option<String> = None;
    for socket in resolved {
        for attempt in 0..=CONNECT_TRANSIENT_RETRIES {
            match TcpStream::connect_timeout(&socket, timeout) {
                Ok(stream) => return Ok(stream),
                Err(e) => {
                    let retryable =
                        matches!(
                            e.kind(),
                            std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                        ) || matches!(e.raw_os_error(), Some(11) | Some(35) | Some(10035));
                    last_err = Some(e.to_string());
                    if retryable && attempt < CONNECT_TRANSIENT_RETRIES {
                        thread::sleep(Duration::from_millis(CONNECT_TRANSIENT_RETRY_DELAY_MS));
                        continue;
                    }
                    break;
                }
            }
        }
    }
    Err(last_err.unwrap_or_else(|| "no_socket_address".to_string()))
}

fn send_msg(stream: &mut TcpStream, msg: &Msg) -> std::io::Result<()> {
    let line = serde_json::to_string(msg).unwrap_or_else(|_| {
        "{\"type\":\"error\",\"error\":\"encode_failed\",\"detail\":\"\"}".to_string()
    });
    stream.write_all(line.as_bytes())?;
    stream.write_all(b"\n")?;
    stream.flush()
}

fn serve_broadcast_followups(
    stream: &mut TcpStream,
    data_dir: &str,
    max_wait: Duration,
) -> std::io::Result<()> {
    stream.set_read_timeout(Some(max_wait)).ok();
    let reader_stream = stream.try_clone()?;
    let mut reader = BufReader::new(reader_stream);
    let mut raw = Vec::<u8>::new();
    loop {
        match read_line_limited(&mut reader, &mut raw) {
            Ok(0) => break,
            Ok(_) => {}
            Err(e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                break;
            }
            Err(e) => return Err(e),
        }

        let line = String::from_utf8_lossy(&raw).trim().to_string();
        if line.is_empty() {
            continue;
        }
        let Some(msg) = read_msg_line(&line) else {
            continue;
        };
        match msg {
            Msg::GetTip {} => {
                let (h, hash32, cw, bits) = tip_fields(data_dir);
                let _ = send_msg(
                    stream,
                    &Msg::Tip {
                        height: h,
                        hash32,
                        chainwork: cw,
                        bits,
                    },
                );
            }
            Msg::GetBlocksFrom { from, limit } => {
                let limit = limit.min(MAX_BLOCKS_PER_MSG);
                if let Some(body) = blocks_from_json(data_dir, from, limit) {
                    if let Ok(blocks) = serde_json::from_str::<Vec<ChainBlock>>(&body) {
                        let _ = send_msg(stream, &Msg::Blocks { blocks });
                    }
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn read_msg_line(line: &str) -> Option<Msg> {
    serde_json::from_str::<Msg>(line).ok()
}

fn try_append_blocks(data_dir: &str, blocks: &[ChainBlock]) -> Result<usize, String> {
    if blocks.is_empty() {
        return Ok(0);
    }
    // Source of truth for sync is DB tip, not chain.json (chain.json may be absent on fresh DB-only nodes).
    let (tip_h, tip_hash, _tip_bits, _tip_chainwork) = tip_fields(data_dir);
    let mut expected_h = tip_h.saturating_add(1);
    let mut expected_prev = tip_hash;

    let mut appended = 0usize;
    for b in blocks {
        if let Err(e) = validate_block_basic(expected_h, &expected_prev, b) {
            // allow no-op if already present
            if store::block_at(data_dir, b.height)
                .map(|x| x.hash32 == b.hash32)
                .unwrap_or(false)
            {
                expected_h = expected_h.saturating_add(1);
                expected_prev = b.hash32.clone();
                continue;
            }
            return Err(e);
        }

        appended += 1;

        // DB is the source of truth.
        store::note_accepted_block(data_dir, b)?;

        expected_h = expected_h.saturating_add(1);
        expected_prev = b.hash32.clone();
    }
    Ok(appended)
}
fn tip_fields(data_dir: &str) -> (u64, String, u64, u64) {
    store::tip_fields(data_dir).unwrap_or((0, "0".repeat(64), 0, 0))
}

fn validate_block_basic(
    expected_height: u64,
    expected_prevhash32: &str,
    block: &ChainBlock,
) -> Result<(), String> {
    if block.height != expected_height {
        return Err("bad_height".to_string());
    }
    // Require linkage for non-genesis blocks
    let prev = block
        .prevhash32
        .as_deref()
        .ok_or_else(|| "missing_prevhash32".to_string())?;
    if prev != expected_prevhash32 {
        return Err("bad_prevhash32".to_string());
    }
    if block.height > 0 && block.txs.is_none() {
        return Err("missing_block_txs".to_string());
    }
    Ok(())
}

fn validate_blocks_from(
    _data_dir: &str,
    expected_next_height: u64,
    expected_prev_hash32: String,
    blocks: &[ChainBlock],
) -> Result<usize, String> {
    if blocks.is_empty() {
        return Ok(0);
    }

    // current tip height is inclusive, next expected is tip+1
    let mut expected_h = expected_next_height;
    let mut expected_prev = expected_prev_hash32;
    let mut ok = 0usize;
    for b in blocks {
        if let Err(e) = validate_block_basic(expected_h, &expected_prev, b) {
            return Err(format!("block_invalid_at_index_{}: {}", ok, e));
        }
        ok += 1;
        expected_h = expected_h.saturating_add(1);
        expected_prev = b.hash32.clone();
    }
    Ok(ok)
}

fn penalize_bad_blocks(peer: &str, peer_ip: IpAddr, err: &str, net: &str, data_dir: &str) {
    // Private mesh peers can legitimately race during catch-up and briefly send
    // stale/out-of-order blocks after we have already requested a resync.
    // Treat those as recovery noise, not hostile misbehavior, or clustered
    // launch nodes will end up temp-banning each other under burst mining.
    let bootstrap_seed_recovery = err.contains("stale_or_out_of_order_block")
        && Network::parse_name(net)
            .map(|network| {
                let (tip_height, _, tip_bits, _) = tip_fields(data_dir);
                netparams::pow_bootstrap_sync_enabled(
                    network,
                    tip_height.saturating_add(1),
                    tip_bits,
                ) && is_launch_backbone_peer_for_net(network, peer)
            })
            .unwrap_or(false);
    if (ip_is_loopback_or_private(peer_ip) || bootstrap_seed_recovery)
        && err.contains("stale_or_out_of_order_block")
    {
        dlog!(
            "p2p: reject blocks peer={} ip={} err={} action=ignore_penalty_launch_recovery",
            peer,
            peer_ip,
            err
        );
        return;
    }

    // Treat invalid blocks (including PoW/bits/consensus failures) as misbehavior.
    // This is required for public-go hardening: don't silently burn CPU on spam.
    let ip_score = add_ip_score(peer_ip, 50, "invalid_block");
    edlog!(
        "p2p: reject blocks peer={} ip={} err={} ip_score={}",
        peer,
        peer_ip,
        err,
        ip_score
    );
}

fn handle_peer(
    mut stream: TcpStream,
    peer_ip: IpAddr,
    _guard: PeerGuard,
    data_dir: String,
    net: String,
) {
    let network = Network::parse_name(&net).unwrap_or(Network::Mainnet);
    let peer = stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "?".to_string());
    note_inbound_peer_connected(&peer);
    stream.set_read_timeout(Some(Duration::from_secs(10))).ok();
    stream.set_write_timeout(Some(Duration::from_secs(10))).ok();

    // greet
    let _ = send_msg(
        &mut stream,
        &Msg::Hello {
            net: net.clone(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            genesis_hash: hello_genesis_hash(&net),
        },
    );

    // share tip
    let (h, hash32, cw, bits) = tip_fields(&data_dir);
    let _ = send_msg(
        &mut stream,
        &Msg::Tip {
            height: h,
            hash32,
            chainwork: cw,
            bits,
        },
    );

    let reader_stream = match stream.try_clone() {
        Ok(cloned) => cloned,
        Err(e) => {
            note_inbound_peer_error(&peer, &format!("stream_clone_failed:{}", e));
            edlog!("p2p: stream_clone_failed peer={} err={}", peer, e);
            note_inbound_peer_disconnected(&peer);
            return;
        }
    };
    let mut reader = BufReader::new(reader_stream);

    let mut score: u32 = 0;
    let mut win_start = Instant::now();
    let mut win_count: u32 = 0;
    let mut win_bytes: usize = 0;
    let mut peer_tip_view: Option<(u64, String)> = None;

    let mut raw = Vec::<u8>::new();
    loop {
        if is_banned(peer_ip) {
            note_inbound_peer_error(&peer, "manually_banned");
            edlog!("p2p: disconnect peer={} ip={} reason=banned", peer, peer_ip);
            break;
        }
        let now = Instant::now();
        if now.duration_since(win_start) >= Duration::from_secs(1) {
            win_start = now;
            win_count = 0;
            win_bytes = 0;
        }
        win_count = win_count.saturating_add(1);
        if win_count > MAX_MSGS_PER_SEC {
            score = score.saturating_add(25);
            let ip_score = if ip_is_loopback_or_private(peer_ip) {
                0
            } else {
                add_ip_score(peer_ip, 25, "rate_limited")
            };
            edlog!(
                "p2p: rate_limited peer={} ip={} score={} ip_score={}",
                peer,
                peer_ip,
                score,
                ip_score
            );
            break;
        }

        match read_line_limited(&mut reader, &mut raw) {
            Ok(0) => break,
            Ok(_) => {
                win_bytes = win_bytes.saturating_add(raw.len());
                if win_bytes > MAX_BYTES_PER_SEC {
                    score = score.saturating_add(25);
                    let ip_score = if ip_is_loopback_or_private(peer_ip) {
                        0
                    } else {
                        add_ip_score(peer_ip, 25, "rate_limited_bytes")
                    };
                    edlog!(
                        "p2p: rate_limited_bytes peer={} ip={} bytes={} score={} ip_score={}",
                        peer,
                        peer_ip,
                        win_bytes,
                        score,
                        ip_score
                    );
                    break;
                }
            }
            Err(e) => {
                if e.kind() == std::io::ErrorKind::InvalidData {
                    score = score.saturating_add(50);
                    note_inbound_peer_error(&peer, &e.to_string());
                    let ip_score = add_ip_score(peer_ip, 50, "invalid_data");
                    edlog!(
                        "p2p: reject peer={} ip={} err={} score={} ip_score={}",
                        peer,
                        peer_ip,
                        e,
                        score,
                        ip_score
                    );
                }
                break;
            }
        }

        let mut line = String::from_utf8_lossy(&raw).to_string();
        line = line.trim().to_string();
        if line.is_empty() {
            continue;
        }
        let msg = match read_msg_line(&line) {
            Some(m) => m,
            None => {
                score = score.saturating_add(10);
                note_inbound_peer_error(&peer, "invalid_json");
                let ip_score = add_ip_score(peer_ip, 10, "invalid_json");
                edlog!(
                    "p2p: invalid_json peer={} ip={} score={} ip_score={}",
                    peer,
                    peer_ip,
                    score,
                    ip_score
                );
                let _ = send_msg(
                    &mut stream,
                    &Msg::Error {
                        error: "invalid_json".to_string(),
                        detail: "bad_message".to_string(),
                    },
                );
                if is_banned(peer_ip) {
                    break;
                }
                continue;
            }
        };

        match msg {
            Msg::Hello {
                net: peer_net,
                genesis_hash: peer_genesis_hash,
                ..
            } => {
                if peer_net != net {
                    note_inbound_peer_error(&peer, "wrong_network");
                    let _ = send_msg(
                        &mut stream,
                        &Msg::Error {
                            error: "wrong_network".to_string(),
                            detail: format!("peer_net={}", peer_net),
                        },
                    );
                    ban_ip(peer_ip, "wrong_network");
                    break;
                }
                let expected_genesis_hash = hello_genesis_hash(&net);
                if !peer_genesis_hash.eq_ignore_ascii_case(&expected_genesis_hash) {
                    note_inbound_peer_error(&peer, "wrong_genesis");
                    let _ = send_msg(
                        &mut stream,
                        &Msg::Error {
                            error: "wrong_genesis".to_string(),
                            detail: format!(
                                "peer_genesis_hash={} local_genesis_hash={}",
                                peer_genesis_hash, expected_genesis_hash
                            ),
                        },
                    );
                    ban_ip(peer_ip, "wrong_genesis");
                    break;
                }

                // Phase 4: learn peers from inbound connections.
                note_peer(&data_dir, &peer_ip.to_string());
            }
            Msg::GetTip {} => {
                let (h, hash32, cw, bits) = tip_fields(&data_dir);
                let _ = send_msg(
                    &mut stream,
                    &Msg::Tip {
                        height: h,
                        hash32,
                        chainwork: cw,
                        bits,
                    },
                );
            }
            Msg::Tip { height, hash32, .. } => {
                note_seen_height(height);
                note_inbound_peer_tip(&peer, height, &hash32);
                peer_tip_view = Some((height, hash32.clone()));
                let (local_h, local_hash32, _bb, _cw) = tip_fields(&data_dir);
                if let Some(from) = official_sync_request_from(
                    network,
                    &peer,
                    local_h,
                    &local_hash32,
                    height,
                    &hash32,
                ) {
                    let _ = send_msg(
                        &mut stream,
                        &Msg::GetBlocksFrom {
                            from,
                            limit: MAX_BLOCKS_PER_MSG,
                        },
                    );
                    note_peer_resync(&peer, false, true);
                }
            }
            Msg::GetBlocksFrom { from, limit } => {
                let limit = limit.min(MAX_BLOCKS_PER_MSG);
                match blocks_from_json(&data_dir, from, limit) {
                    Some(body) => match serde_json::from_str::<Vec<ChainBlock>>(&body) {
                        Ok(blocks) => {
                            let _ = send_msg(&mut stream, &Msg::Blocks { blocks });
                        }
                        Err(e) => {
                            let _ = send_msg(
                                &mut stream,
                                &Msg::Error {
                                    error: "blocks_encode_failed".to_string(),
                                    detail: format!("invalid_blocks_json: {}", e),
                                },
                            );
                        }
                    },
                    None => {
                        let _ = send_msg(
                            &mut stream,
                            &Msg::Error {
                                error: "blocks_encode_failed".to_string(),
                                detail: "blocks_from_unavailable".to_string(),
                            },
                        );
                    }
                }
            }
            Msg::Blocks { blocks } => {
                let (tip_h, tip_hash, tip_cw, _bits) = tip_fields(&data_dir);

                // Fast-path: normal extension of tip
                let mut mode: &str = "append";
                let mut fork_point: Option<u64> = None;
                // If peer sends an overlapping window (starting below our next expected height),
                // try to find a common ancestor inside the window and treat the rest as a reorg candidate.
                let mut blocks_view: &[ChainBlock] = &blocks;
                if let Some(first0) = blocks_view.first() {
                    if first0.height < tip_h.saturating_add(1) {
                        let mut common: Option<u64> = None;
                        for b in blocks_view.iter().rev() {
                            if b.height > tip_h {
                                continue;
                            }
                            if let Some(local) = store::block_at(&data_dir, b.height) {
                                if local.hash32 == b.hash32 {
                                    common = Some(b.height);
                                    break;
                                }
                            }
                        }
                        if let Some(fp_h) = common {
                            if let Some(i) = blocks_view
                                .iter()
                                .position(|b| b.height == fp_h.saturating_add(1))
                            {
                                blocks_view = &blocks_view[i..];
                                mode = "reorg_candidate";
                                fork_point = Some(fp_h);
                            } else {
                                // Window doesn't include anything after the common ancestor.
                                if should_resync(&peer, sync_gate_resync_interval(network, &peer)) {
                                    let from = deeper_overlap_from(first0.height);
                                    let limit = MAX_BLOCKS_PER_MSG;
                                    let _ =
                                        send_msg(&mut stream, &Msg::GetBlocksFrom { from, limit });
                                    note_peer_resync(&peer, false, true);
                                    dlog!(
                                        "p2p: resync_request peer={} from={} limit={} reason=common_ancestor_window_exhausted first_height={} fork_point={}",
                                        peer,
                                        from,
                                        limit,
                                        first0.height,
                                        fp_h
                                    );
                                }
                                continue;
                            }
                        } else {
                            // No common ancestor in this window; ask for an older overlap
                            // instead of stalling on the same slice forever.
                            if should_resync(&peer, sync_gate_resync_interval(network, &peer)) {
                                let from = deeper_overlap_from(first0.height);
                                let limit = MAX_BLOCKS_PER_MSG;
                                let _ = send_msg(&mut stream, &Msg::GetBlocksFrom { from, limit });
                                note_peer_resync(&peer, false, true);
                                dlog!(
                                    "p2p: resync_request peer={} from={} limit={} reason=no_common_ancestor first_height={}",
                                    peer,
                                    from,
                                    limit,
                                    first0.height
                                );
                            }
                            continue;
                        }
                    }
                }

                if let Some(first) = blocks_view.first() {
                    let prev = first.prevhash32.clone().unwrap_or_default();
                    if first.height == tip_h + 1 && prev == tip_hash {
                        // ok
                    } else {
                        // Potential fork: does this connect to a known block at height-1?
                        if first.height > 0 && !prev.is_empty() {
                            let want_h = first.height.saturating_sub(1);
                            if let Some(prevb) = store::block_at(&data_dir, want_h) {
                                if prevb.hash32 == prev {
                                    mode = "reorg_candidate";
                                    fork_point = Some(want_h);
                                }
                            }
                        }
                        if mode != "reorg_candidate" {
                            if first.height == tip_h + 1 {
                                // Likely fork: peer is sending blocks that don't connect to our tip.
                                // Ask for an overlapping window so we can find the common ancestor and reorg.
                                if should_resync(&peer, sync_gate_resync_interval(network, &peer)) {
                                    let from = reorg_overlap_from(tip_h);
                                    let limit = MAX_BLOCKS_PER_MSG;
                                    let _ =
                                        send_msg(&mut stream, &Msg::GetBlocksFrom { from, limit });
                                    note_peer_resync(&peer, false, false);
                                    dlog!("p2p: resync_request peer={} from={} limit={} reason=bad_prevhash", peer, from, limit);
                                }
                                continue;
                            }
                            if first.height != tip_h + 1 {
                                // Out-of-order or different-tip blocks. Don't treat as misbehavior;
                                // request the expected range from our current tip.
                                if first.height > tip_h + 1 {
                                    if should_resync(
                                        &peer,
                                        sync_gate_resync_interval(network, &peer),
                                    ) {
                                        let from = (tip_h + 1) as usize;
                                        let limit = 128usize;
                                        let _ = send_msg(
                                            &mut stream,
                                            &Msg::GetBlocksFrom { from, limit },
                                        );
                                        note_peer_resync(&peer, false, false);
                                        dlog!("p2p: resync_request peer={} from={} limit={} reason=out_of_order", peer, from, limit);
                                    }
                                    dlog!("p2p: out_of_order blocks peer={} first_height={} expected={}", peer, first.height, tip_h + 1);
                                } else {
                                    // old/duplicate blocks
                                    dlog!(
                                        "p2p: ignore blocks peer={} first_height={} tip={}",
                                        peer,
                                        first.height,
                                        tip_h
                                    );
                                }
                            } else {
                                dlog!(
                                    "p2p: reject blocks peer={} first_height={} err=bad_prevhash",
                                    peer,
                                    first.height
                                );
                            }
                            continue;
                        }
                    }
                }
                let (base_h, base_prev) = if mode == "append" {
                    (tip_h.saturating_add(1), tip_hash.clone())
                } else {
                    let fp = fork_point.unwrap_or(0);
                    let prev_hash = if fp == 0 {
                        "0".repeat(64)
                    } else {
                        store::block_at(&data_dir, fp)
                            .map(|b| b.hash32)
                            .unwrap_or_else(|| tip_hash.clone())
                    };
                    (fp.saturating_add(1), prev_hash)
                };

                match validate_blocks_from(&data_dir, base_h, base_prev, blocks_view) {
                    Ok(n_ok) => {
                        if n_ok == 0 {
                            continue;
                        }
                        let slice = &blocks_view[..n_ok];

                        let appended = if mode == "append" {
                            match try_append_blocks(&data_dir, slice) {
                                Ok(n) => n,
                                Err(e) => {
                                    penalize_bad_blocks(&peer, peer_ip, &e, &net, &data_dir);
                                    if is_banned(peer_ip) {
                                        break;
                                    }
                                    0
                                }
                            }
                        } else {
                            // Reorg candidate: switch only if incoming chainwork is higher than current tip.
                            let fp = fork_point.unwrap_or(0);
                            let incoming_cw =
                                store::compute_chainwork_for_candidate(&data_dir, fp, slice)
                                    .unwrap_or(0);
                            let candidate_tip_height = slice.last().map(|b| b.height).unwrap_or(fp);
                            let candidate_tip_hash =
                                slice.last().map(|b| b.hash32.as_str()).unwrap_or_default();
                            if !should_accept_reorg_candidate(tip_cw, incoming_cw)
                                && !should_prefer_backbone_tie_candidate(
                                    network,
                                    &peer,
                                    tip_h,
                                    &tip_hash,
                                    candidate_tip_height,
                                    candidate_tip_hash,
                                    tip_cw,
                                    incoming_cw,
                                )
                            {
                                edlog!(
                                    "p2p: ignore fork blocks peer={} first_height={} incoming_cw={} tip_cw={}",
                                    peer,
                                    slice.first().map(|b| b.height).unwrap_or(0),
                                    incoming_cw,
                                    tip_cw
                                );
                                0
                            } else {
                                if let Err(e) = store::rollback_to_height(&data_dir, fp) {
                                    edlog!("p2p: reorg rollback_failed peer={} err={}", peer, e);
                                    0
                                } else {
                                    let n = match try_append_blocks(&data_dir, slice) {
                                        Ok(n) => n,
                                        Err(e) => {
                                            penalize_bad_blocks(
                                                &peer, peer_ip, &e, &net, &data_dir,
                                            );
                                            if is_banned(peer_ip) {
                                                break;
                                            }
                                            0
                                        }
                                    };
                                    if n > 0 {
                                        let backbone_tie = incoming_cw == tip_cw;
                                        note_reorg_accept(backbone_tie);
                                        if let Some(last) = slice.last() {
                                            dlog!(
                                                "p2p: reorg accepted blocks peer={} appended={} tip={} {}",
                                                peer, n, last.height, last.hash32
                                            );
                                        }
                                    }
                                    n
                                }
                            }
                        };
                        if appended > 0 {
                            if let Some(last) = slice.last() {
                                dlog!(
                                    "p2p: accepted blocks peer={} appended={} tip={} {}",
                                    peer,
                                    appended,
                                    last.height,
                                    last.hash32
                                );
                                if slice.len() >= MAX_BLOCKS_PER_MSG {
                                    let from = last.height.saturating_add(1) as usize;
                                    let limit = MAX_BLOCKS_PER_MSG;
                                    let _ =
                                        send_msg(&mut stream, &Msg::GetBlocksFrom { from, limit });
                                    note_peer_resync(&peer, false, false);
                                    dlog!(
                                        "p2p: resync_request peer={} from={} limit={} reason=continue_sync",
                                        peer,
                                        from,
                                        limit
                                    );
                                } else if let Some((remote_h, remote_hash32)) =
                                    peer_tip_view.as_ref()
                                {
                                    let (new_h, new_hash32, _new_bits, _new_cw) =
                                        tip_fields(&data_dir);
                                    if let Some(from) = official_sync_request_from(
                                        network,
                                        &peer,
                                        new_h,
                                        &new_hash32,
                                        *remote_h,
                                        remote_hash32,
                                    ) {
                                        let limit = MAX_BLOCKS_PER_MSG;
                                        let _ = send_msg(
                                            &mut stream,
                                            &Msg::GetBlocksFrom { from, limit },
                                        );
                                        note_peer_resync(&peer, false, true);
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        note_inbound_peer_error(&peer, &e);
                        edlog!("p2p: reject blocks peer={} err={}", peer, e);
                    }
                }
            }
            Msg::Block { block } => {
                let (tip_h, tip_hash, tip_cw, _bits) = tip_fields(&data_dir);

                let prev = block.prevhash32.clone().unwrap_or_default();
                if block.height == tip_h + 1 && prev == tip_hash {
                    // ok normal extension
                } else {
                    if block.height == tip_h + 1 {
                        if should_resync(&peer, sync_gate_resync_interval(network, &peer)) {
                            let from = reorg_overlap_from(tip_h);
                            let limit = MAX_BLOCKS_PER_MSG;
                            let _ = send_msg(&mut stream, &Msg::GetBlocksFrom { from, limit });
                            note_peer_resync(&peer, false, false);
                            dlog!(
                                "p2p: resync_request peer={} from={} limit={} reason=bad_prevhash",
                                peer,
                                from,
                                limit
                            );
                        }
                        continue;
                    }
                    // Reorg candidate if connects to known height-1 and has higher chainwork.
                    if block.height > 0 && !prev.is_empty() {
                        let fp = block.height.saturating_sub(1);
                        if let Some(prevb) = store::block_at(&data_dir, fp) {
                            let cand_cw = store::compute_chainwork_for_candidate(
                                &data_dir,
                                fp,
                                std::slice::from_ref(&block),
                            )
                            .unwrap_or(0);
                            if prevb.hash32 == prev
                                && (should_accept_reorg_candidate(tip_cw, cand_cw)
                                    || should_prefer_backbone_tie_candidate(
                                        network,
                                        &peer,
                                        tip_h,
                                        &tip_hash,
                                        block.height,
                                        &block.hash32,
                                        tip_cw,
                                        cand_cw,
                                    ))
                            {
                                if let Err(e) = store::rollback_to_height(&data_dir, fp) {
                                    edlog!("p2p: reorg rollback_failed peer={} err={}", peer, e);
                                    continue;
                                }
                                let one = vec![block.clone()];
                                let appended = match try_append_blocks(&data_dir, &one) {
                                    Ok(n) => n,
                                    Err(e) => {
                                        penalize_bad_blocks(&peer, peer_ip, &e, &net, &data_dir);
                                        if is_banned(peer_ip) {
                                            break;
                                        }
                                        0
                                    }
                                };
                                if appended > 0 {
                                    let backbone_tie = cand_cw == tip_cw;
                                    note_reorg_accept(backbone_tie);
                                    dlog!(
                                        "p2p: reorg accepted block peer={} tip={} {}",
                                        peer,
                                        block.height,
                                        block.hash32
                                    );
                                }
                                continue;
                            }
                        }
                    }

                    if block.height != tip_h + 1 {
                        // Out-of-order / future block. Don't penalize; ask peer for the missing range.
                        if block.height > tip_h + 1 {
                            if should_resync(&peer, sync_gate_resync_interval(network, &peer)) {
                                let from = (tip_h + 1) as usize;
                                let limit = 128usize;
                                let _ = send_msg(&mut stream, &Msg::GetBlocksFrom { from, limit });
                                note_peer_resync(&peer, false, false);
                                dlog!("p2p: resync_request peer={} from={} limit={} reason=out_of_order", peer, from, limit);
                            }
                            dlog!(
                                "p2p: out_of_order block peer={} height={} expected={} hash={}",
                                peer,
                                block.height,
                                tip_h + 1,
                                block.hash32
                            );
                        } else {
                            // Competing tip block at the same height:
                            // - If peers race and produce different blocks at the same height, simply
                            //   ignoring the loser can cause persistent splits (no trigger to learn the
                            //   other branch's descendants).
                            // - Request a small overlap range so our existing forkpoint+chainwork logic
                            //   (in Msg::Blocks) can decide when the competing branch gains more work.
                            if block.height == tip_h {
                                if let Some(local_tip) = store::block_at(&data_dir, tip_h) {
                                    if local_tip.hash32 != block.hash32 {
                                        if should_resync(
                                            &peer,
                                            sync_gate_resync_interval(network, &peer),
                                        ) {
                                            let from = tip_h.saturating_sub(1) as usize;
                                            let limit = MAX_BLOCKS_PER_MSG;
                                            let _ = send_msg(
                                                &mut stream,
                                                &Msg::GetBlocksFrom { from, limit },
                                            );
                                            note_peer_resync(&peer, true, false);
                                            dlog!(
                                                "p2p: resync_request peer={} from={} limit={} reason=competing_tip height={} local_tip={} peer_tip={}",
                                                peer,
                                                from,
                                                limit,
                                                tip_h,
                                                local_tip.hash32,
                                                block.hash32
                                            );
                                        }
                                    }
                                }
                            }
                            dlog!(
                                "p2p: ignore block peer={} height={} tip={} hash={}",
                                peer,
                                block.height,
                                tip_h,
                                block.hash32
                            );
                        }
                    } else {
                        dlog!(
                            "p2p: reject block peer={} height={} hash={} err=bad_prevhash",
                            peer,
                            block.height,
                            block.hash32
                        );
                    }
                    continue;
                }
                let (tip_h, tip_hash, _cw, _bits) = tip_fields(&data_dir);
                let expected_h = tip_h.saturating_add(1);
                match validate_block_basic(expected_h, &tip_hash, &block) {
                    Ok(()) => {
                        let appended =
                            match try_append_blocks(&data_dir, std::slice::from_ref(&block)) {
                                Ok(n) => n,
                                Err(e) => {
                                    penalize_bad_blocks(&peer, peer_ip, &e, &net, &data_dir);
                                    if is_banned(peer_ip) {
                                        break;
                                    }
                                    0
                                }
                            };
                        if appended > 0 {
                            dlog!(
                                "p2p: accepted block peer={} height={} hash={}",
                                peer,
                                block.height,
                                block.hash32
                            );
                        }
                    }
                    Err(e) => {
                        edlog!(
                            "p2p: reject block peer={} height={} hash={} err={}",
                            peer,
                            block.height,
                            block.hash32,
                            e
                        );
                    }
                }
            }
            Msg::Tx { txid, tx } => match submit_tx::ingest_tx_p2p(&data_dir, &txid, &tx) {
                Ok(outcome) => {
                    if outcome.accepted_new {
                        broadcast_tx(&txid, &tx);
                    }
                    for (promoted_txid, promoted_tx) in outcome.promoted {
                        broadcast_tx(&promoted_txid, &promoted_tx);
                    }
                }
                Err(e) => {
                    note_inbound_peer_error(&peer, &e);
                    edlog!("p2p ingest_tx failed peer={} txid={} err={}", peer, txid, e);
                }
            },
            Msg::Error { error, detail } => {
                note_inbound_peer_error(&peer, &format!("{}:{}", error, detail));
                dlog!(
                    "p2p: peer_error peer={} error={} detail={}",
                    peer,
                    error,
                    detail
                );
                // informational only
            }
        }

        if score >= BAN_THRESHOLD {
            let ip_score = add_ip_score(peer_ip, score, "score_threshold");
            edlog!(
                "p2p: score_threshold peer={} ip={} score={} ip_score={}",
                peer,
                peer_ip,
                score,
                ip_score
            );
            break;
        }
    }
    note_inbound_peer_disconnected(&peer);
}

fn dial_once(addr: &str, data_dir: &str, net: &str) -> Result<(), String> {
    let network = Network::parse_name(net).unwrap_or(Network::Mainnet);
    let mut stream = connect_peer(addr)?;
    stream.set_read_timeout(Some(Duration::from_secs(2))).ok();
    stream.set_write_timeout(Some(Duration::from_secs(2))).ok();

    send_msg(
        &mut stream,
        &Msg::Hello {
            net: net.to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            genesis_hash: hello_genesis_hash(net),
        },
    )
    .map_err(|e| format!("hello_send_failed:{e}"))?;
    send_msg(&mut stream, &Msg::GetTip {}).map_err(|e| format!("gettip_send_failed:{e}"))?;

    // Record reachable peer (best-effort) so new nodes can bootstrap without manual seeds.
    // Keep the exact dial addr (host:port) in backoff, but store host only for persistence.
    if let Some((host, _p)) = addr.rsplit_once(':') {
        note_peer(data_dir, host);
    }

    let reader_stream = stream
        .try_clone()
        .map_err(|e| format!("stream_clone_failed:{e}"))?;
    let mut reader = BufReader::new(reader_stream);
    let mut raw = Vec::<u8>::new();
    let mut peer_tip_view: Option<(u64, String)> = None;
    for _ in 0..16 {
        match read_line_limited(&mut reader, &mut raw) {
            Ok(0) => break,
            Ok(_) => {
                let line = String::from_utf8_lossy(&raw);
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                if let Some(msg) = read_msg_line(line) {
                    match msg {
                        Msg::Hello {
                            net: peer_net,
                            genesis_hash: peer_genesis_hash,
                            ..
                        } => {
                            if peer_net != net {
                                return Err(format!("wrong_network:peer_net={peer_net}"));
                            }
                            let expected_genesis_hash = hello_genesis_hash(net);
                            if !peer_genesis_hash.eq_ignore_ascii_case(&expected_genesis_hash) {
                                return Err(format!(
                                    "wrong_genesis:peer_genesis_hash={peer_genesis_hash} local_genesis_hash={expected_genesis_hash}"
                                ));
                            }
                        }
                        Msg::Tip { height, hash32, .. } => {
                            note_seen_height(height);
                            peer_tip_view = Some((height, hash32.clone()));
                            note_outbound_peer_result(
                                addr,
                                true,
                                Some(height),
                                Some(&hash32),
                                None,
                            );
                            let (local_h, local_hash32, _bb, _cw) = tip_fields(data_dir);
                            if let Some(from) = official_sync_request_from(
                                network,
                                addr,
                                local_h,
                                &local_hash32,
                                height,
                                &hash32,
                            ) {
                                let _ = send_msg(
                                    &mut stream,
                                    &Msg::GetBlocksFrom {
                                        from,
                                        limit: MAX_BLOCKS_PER_MSG,
                                    },
                                );
                                note_peer_resync(addr, false, true);
                            }
                        }
                        Msg::Blocks { blocks } => {
                            let (tip_h, tip_hash, tip_cw, tip_bits) = tip_fields(&data_dir);
                            if blocks.is_empty() {
                                continue;
                            }

                            // Fast-path: extends our tip.
                            let first = &blocks[0];
                            let first_prev = first.prevhash32.clone().unwrap_or_default();
                            if first.height == tip_h.saturating_add(1) && first_prev == tip_hash {
                                let appended = match try_append_blocks(data_dir, &blocks) {
                                    Ok(n) => n,
                                    Err(e) => {
                                        edlog!(
                                            "p2p: append_failed peer={} first_height={} err={}",
                                            addr,
                                            first.height,
                                            e
                                        );
                                        0
                                    }
                                };
                                if appended > 0 && blocks.len() >= MAX_BLOCKS_PER_MSG {
                                    let from = blocks
                                        .last()
                                        .map(|b| b.height.saturating_add(1) as usize)
                                        .unwrap_or((tip_h + 1) as usize);
                                    let _ = send_msg(
                                        &mut stream,
                                        &Msg::GetBlocksFrom {
                                            from,
                                            limit: MAX_BLOCKS_PER_MSG,
                                        },
                                    );
                                } else if let Some((remote_h, remote_hash32)) =
                                    peer_tip_view.as_ref()
                                {
                                    let (new_h, new_hash32, _new_bits, _new_cw) =
                                        tip_fields(data_dir);
                                    if let Some(from) = official_sync_request_from(
                                        network,
                                        addr,
                                        new_h,
                                        &new_hash32,
                                        *remote_h,
                                        remote_hash32,
                                    ) {
                                        let _ = send_msg(
                                            &mut stream,
                                            &Msg::GetBlocksFrom {
                                                from,
                                                limit: MAX_BLOCKS_PER_MSG,
                                            },
                                        );
                                        note_peer_resync(addr, false, true);
                                    }
                                }
                                continue;
                            }

                            // Try forkpoint discovery within the received batch:
                            // Choose the *closest* forkpoint (highest height) within our undo window.
                            let window_start = tip_h.saturating_sub(store::REORG_UNDO_WINDOW);
                            let mut fp_height: Option<u64> = None;
                            let mut fp_index: Option<usize> = None;
                            for (i, b) in blocks.iter().enumerate() {
                                if b.height == 0 {
                                    continue;
                                }
                                let want_prev = b.prevhash32.clone().unwrap_or_default();
                                if want_prev.is_empty() {
                                    continue;
                                }
                                let prev_h = b.height.saturating_sub(1);

                                // Enforce reorg depth window: never consider forkpoints older than window_start.
                                if prev_h < window_start {
                                    continue;
                                }

                                let local_prev_hash = if prev_h == 0 {
                                    "0".repeat(64)
                                } else {
                                    match store::block_at(data_dir, prev_h) {
                                        Some(pb) => pb.hash32,
                                        None => continue,
                                    }
                                };

                                if local_prev_hash == want_prev {
                                    // Prefer the closest forkpoint to our tip.
                                    if fp_height.map(|h| prev_h > h).unwrap_or(true) {
                                        fp_height = Some(prev_h);
                                        fp_index = Some(i);
                                    }
                                }
                            }

                            if let (Some(fp_h), Some(i0)) = (fp_height, fp_index) {
                                let slice = &blocks[i0..];
                                let incoming_cw =
                                    store::compute_chainwork_for_candidate(&data_dir, fp_h, slice)
                                        .unwrap_or(0);
                                let candidate_tip_height =
                                    slice.last().map(|b| b.height).unwrap_or(fp_h);
                                let candidate_tip_hash =
                                    slice.last().map(|b| b.hash32.as_str()).unwrap_or_default();
                                if should_accept_reorg_candidate(tip_cw, incoming_cw)
                                    || should_prefer_backbone_tie_candidate(
                                        network,
                                        addr,
                                        tip_h,
                                        &tip_hash,
                                        candidate_tip_height,
                                        candidate_tip_hash,
                                        tip_cw,
                                        incoming_cw,
                                    )
                                {
                                    match store::rollback_to_height(data_dir, fp_h) {
                                        Ok(()) => {
                                            let appended = match try_append_blocks(data_dir, slice)
                                            {
                                                Ok(n) => n,
                                                Err(e) => {
                                                    edlog!(
                                                        "p2p: reorg append_failed peer={} rollback_to={} err={}",
                                                        addr, fp_h, e
                                                    );
                                                    0
                                                }
                                            };
                                            if appended > 0 {
                                                let backbone_tie = incoming_cw == tip_cw;
                                                note_reorg_accept(backbone_tie);
                                                if let Some(last) = slice.last() {
                                                    dlog!(
                                                    "p2p: reorg peer={} rollback_to={} new_tip_height={} new_tip_hash={} new_tip_cw={}",
                                                    addr, fp_h, last.height, last.hash32, incoming_cw
                                                );
                                                    if slice.len() >= MAX_BLOCKS_PER_MSG {
                                                        let from =
                                                            last.height.saturating_add(1) as usize;
                                                        let _ = send_msg(
                                                            &mut stream,
                                                            &Msg::GetBlocksFrom {
                                                                from,
                                                                limit: MAX_BLOCKS_PER_MSG,
                                                            },
                                                        );
                                                    } else if let Some((remote_h, remote_hash32)) =
                                                        peer_tip_view.as_ref()
                                                    {
                                                        let (new_h, new_hash32, _new_bits, _new_cw) =
                                                            tip_fields(data_dir);
                                                        if let Some(from) =
                                                            official_sync_request_from(
                                                                network,
                                                                addr,
                                                                new_h,
                                                                &new_hash32,
                                                                *remote_h,
                                                                remote_hash32,
                                                            )
                                                        {
                                                            let _ = send_msg(
                                                                &mut stream,
                                                                &Msg::GetBlocksFrom {
                                                                    from,
                                                                    limit: MAX_BLOCKS_PER_MSG,
                                                                },
                                                            );
                                                            note_peer_resync(addr, false, true);
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            edlog!(
                                            "p2p: reorg rollback_failed peer={} rollback_to={} err={}",
                                            addr, fp_h, e
                                        );
                                        }
                                    }
                                }
                                continue;
                            }

                            // Still doesn't connect: request overlap resync (with backoff) and avoid spam.
                            let min_interval = sync_gate_resync_interval(network, addr);
                            if should_resync(addr, min_interval) {
                                let from = reorg_overlap_from(tip_h);
                                // Keep limit modest; dial loop runs often.
                                let limit = MAX_BLOCKS_PER_MSG;
                                let _ = send_msg(&mut stream, &Msg::GetBlocksFrom { from, limit });
                                note_peer_resync(addr, false, false);
                                dlog!(
                                "p2p: resync_request peer={} from={} limit={} reason=bad_prevhash tip_h={} tip_bits={} tip_cw={}",
                                addr, from, limit, tip_h, tip_bits, tip_cw
                            );
                            }
                        }
                        _ => {}
                    }
                } else {
                    note_outbound_peer_result(addr, false, None, None, Some("invalid_json"));
                    return Err("invalid_json".to_string());
                }
            }
            Err(e) => {
                let err = e.to_string();
                note_outbound_peer_result(addr, false, None, None, Some(&err));
                return Err(err);
            }
        }
    }
    Ok(())
}

pub fn broadcast_block(block: &ChainBlock) {
    let cfg = match cfg() {
        Some(c) => c,
        None => return,
    };
    let tip_msg = Msg::Tip {
        height: block.height,
        hash32: block.hash32.clone(),
        chainwork: block.chainwork,
        bits: block.bits,
    };
    let mut sent = 0usize;
    let targets = select_block_broadcast_targets(&outbound_candidates(), &cfg.port, &cfg.local_ip);
    let now = Instant::now();
    for addr in targets.iter() {
        let state = BLOCK_BROADCAST_STATE.get_or_init(|| Mutex::new(HashMap::new()));
        let should_send = {
            let mut g = lock_or_recover(state, "block_broadcast_state");
            match g.get(addr).copied() {
                Some(last)
                    if now.duration_since(last)
                        < Duration::from_millis(BLOCK_BROADCAST_MIN_INTERVAL_MS) =>
                {
                    false
                }
                _ => {
                    g.insert(addr.clone(), now);
                    true
                }
            }
        };
        if !should_send {
            continue;
        }
        if let Ok(mut stream) = connect_peer(addr) {
            stream.set_read_timeout(Some(Duration::from_secs(2))).ok();
            stream.set_write_timeout(Some(Duration::from_secs(2))).ok();
            let _ = send_msg(
                &mut stream,
                &Msg::Hello {
                    net: cfg.net.clone(),
                    version: env!("CARGO_PKG_VERSION").to_string(),
                    genesis_hash: hello_genesis_hash(&cfg.net),
                },
            );
            let _ = send_msg(&mut stream, &tip_msg);
            let _ = send_msg(
                &mut stream,
                &Msg::Block {
                    block: block.clone(),
                },
            );
            let _ =
                serve_broadcast_followups(&mut stream, &cfg.data_dir, broadcast_followup_wait());
            sent += 1;
        }
    }
    if sent > 0 {
        dlog!(
            "p2p: broadcast block height={} hash={} peers_sent={}",
            block.height,
            block.hash32,
            sent
        );
    }
}

pub fn broadcast_tx(txid: &str, tx: &serde_json::Value) {
    let cfg = match cfg() {
        Some(c) => c,
        None => return,
    };
    let targets = select_outbound_targets(
        &outbound_candidates(),
        &cfg.port,
        &cfg.local_ip,
        TX_BROADCAST_FANOUT,
    );
    for addr in targets.iter() {
        if let Ok(mut stream) = connect_peer(addr) {
            stream.set_read_timeout(Some(Duration::from_secs(2))).ok();
            stream.set_write_timeout(Some(Duration::from_secs(2))).ok();
            let _ = send_msg(
                &mut stream,
                &Msg::Hello {
                    net: cfg.net.clone(),
                    version: env!("CARGO_PKG_VERSION").to_string(),
                    genesis_hash: hello_genesis_hash(&cfg.net),
                },
            );
            let _ = send_msg(
                &mut stream,
                &Msg::Tx {
                    txid: txid.to_string(),
                    tx: tx.clone(),
                },
            );
        }
    }
}

pub fn start_p2p(bind_addr: String, data_dir: String, net: String, configured_seeds: Vec<String>) {
    let port = bind_addr.split(':').last().unwrap_or("0").to_string();

    // Phase 4: bootstrap/discovery
    // 1) Prefer explicit CLI/env seeds.
    // 2) Then a local seeds file in the data dir (so public packages can ship/update seeds without recompiling).
    // 3) Finally, fall back to the public DNS seeds only (no internal mesh defaults in public bootstrap).
    let bootstrap = load_bootstrap_candidates(&data_dir, &net, &port, &configured_seeds);
    note_bootstrap_refresh(&bootstrap);
    let mut seeds = bootstrap.candidates.clone();
    if seeds.is_empty() {
        // Public DNS seeds (preferred for fresh bootstrap on public networks).
        // These may be empty/unresolved during early setup; that's OK—other sources (seeds file, persisted peers)
        // and direct connect can still work.
        seeds = built_in_seed_hosts(&net);
    }

    // Load persisted peers (best-effort) and normalize all candidates.
    let persisted = load_list_file(&data_dir, PEERS_FILE);
    let persisted_count = persisted.len();
    let mut merged = Vec::new();
    merged.extend(seeds.into_iter());
    merged.extend(persisted.into_iter());

    seeds = normalize_bootstrap_candidates(merged, &port);

    let configured_seed_count = bootstrap.configured_seed_count;
    let file_seed_count = bootstrap.seeds_file_count;
    let bootstrap_source = if configured_seed_count > 0 {
        "configured"
    } else if file_seed_count > 0 {
        "seeds_file"
    } else {
        "built_in"
    };

    // Store config for broadcast helpers (best-effort).
    let seed_hint = seeds.get(0).map(|s| s.as_str()).unwrap_or("1.1.1.1");
    let local_ip = detect_local_ip(seed_hint).unwrap_or_default();
    let _ = P2P_CFG.set(P2pConfig {
        port: port.clone(),
        net: net.clone(),
        local_ip: local_ip.clone(),
        seeds: seeds.clone(),
        data_dir: data_dir.clone(),
    });
    dlog!(
        "p2p: bootstrap source={} seeds={} persisted_peers={} local_ip={} port={}",
        bootstrap_source,
        seeds.len(),
        persisted_count,
        local_ip,
        port
    );

    // Listener for inbound requests.
    let listener = match TcpListener::bind(&bind_addr) {
        Ok(l) => l,
        Err(e) => {
            edlog!("p2p: failed to bind {}: {}", bind_addr, e);
            return;
        }
    };
    dlog!("p2p: listening on {}", bind_addr);

    let dd_in = data_dir.clone();
    let net_in = net.clone();
    thread::spawn(move || {
        for inbound in listener.incoming() {
            match inbound {
                Ok(stream) => {
                    let peer_ip = match stream.peer_addr().map(|a| a.ip()) {
                        Ok(ip) => ip,
                        Err(_) => {
                            continue;
                        }
                    };
                    let guard = match try_accept_peer(peer_ip) {
                        Some(g) => g,
                        None => {
                            continue;
                        }
                    };
                    let dd = dd_in.clone();
                    let n = net_in.clone();
                    thread::spawn(move || handle_peer(stream, peer_ip, guard, dd, n));
                }
                Err(e) => {
                    log_throttled_error("accept_failed", 60, |suppressed| {
                        if suppressed > 0 {
                            format!(
                                "p2p: accept_failed err={} suppressed={} window_secs=60",
                                e, suppressed
                            )
                        } else {
                            format!("p2p: accept_failed err={}", e)
                        }
                    });
                    thread::sleep(Duration::from_millis(200));
                }
            }
        }
    });

    // Pull-based sync loop (dial seeds, request blocks).
    loop {
        let bootstrap = load_bootstrap_candidates(&data_dir, &net, &port, &configured_seeds);
        note_bootstrap_refresh(&bootstrap);
        let targets = select_outbound_targets(
            &bootstrap.candidates,
            &port,
            &local_ip,
            MAX_OUTBOUND_DIALS_PER_TICK,
        );
        for addr in targets.iter() {
            if let Some(reason) = outbound_peer_should_skip(&addr) {
                log_throttled_warn(&format!("outbound_skip:{addr}"), 300, |suppressed| {
                    if suppressed > 0 {
                        format!(
                            "p2p: outbound_skip peer={} reason={} suppressed={} window_secs=300",
                            addr, reason, suppressed
                        )
                    } else {
                        format!("p2p: outbound_skip peer={} reason={}", addr, reason)
                    }
                });
                continue;
            }
            if !dial_should_try(&addr) {
                continue;
            }
            let ok = match dial_once(addr, &data_dir, &net) {
                Ok(()) => {
                    note_outbound_result(addr, true, None);
                    true
                }
                Err(err) => {
                    log_dial_failed(addr, &err);
                    note_outbound_result(addr, false, Some(&err));
                    false
                }
            };
            dial_note_result(addr, ok);
        }
        thread::sleep(outbound_tick_interval());
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ban_peer_manual, canonicalize_peer_token, deeper_overlap_from, health_priority_sync_from,
        is_transient_dial_error, list_banned_json, normalize_bootstrap_candidates,
        official_tip_sync_from, parse_peers_text, read_line_limited, reorg_overlap_from,
        should_accept_reorg_candidate, should_prefer_backbone_tie_candidate, subnet24_key,
        sync_gate_local_submit_ready, sync_gate_mining_ready, unban_peer_manual,
        validate_block_basic, MAX_BLOCKS_PER_MSG, MAX_LINE_BYTES,
    };
    use crate::ChainBlock;
    use duta_core::netparams::{self, Network};
    use std::io::{BufReader, Cursor};
    use std::sync::atomic::Ordering;
    use std::sync::{Mutex, OnceLock};

    fn sync_gate_test_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn skip_when_sync_gate_disabled() -> bool {
        !netparams::pow_bootstrap_sync_enabled(Network::Mainnet, 1, 0)
    }

    fn clear_test_peer_state() {
        super::BEST_SEEN_HEIGHT.store(0, Ordering::Relaxed);
        if let Ok(mut peers) = super::state().outbound_recent.lock() {
            peers.clear();
        }
        if let Ok(mut peers) = super::state().inbound_live.lock() {
            peers.clear();
        }
    }
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn reorg_candidate_requires_strictly_higher_chainwork() {
        assert!(!should_accept_reorg_candidate(100, 100));
        assert!(!should_accept_reorg_candidate(100, 99));
        assert!(should_accept_reorg_candidate(100, 101));
    }

    #[test]
    fn sync_gate_prefers_official_backbone_tie_candidate() {
        if skip_when_sync_gate_disabled() {
            return;
        }
        let _guard = sync_gate_test_lock().lock().unwrap();
        clear_test_peer_state();
        super::BEST_SEEN_HEIGHT.store(2, Ordering::Relaxed);
        super::note_outbound_peer_result(
            "seed1.dutago.xyz:19082",
            true,
            Some(2),
            Some(&"22".repeat(32)),
            None,
        );
        super::note_outbound_peer_result(
            "seed2.dutago.xyz:19082",
            true,
            Some(2),
            Some(&"22".repeat(32)),
            None,
        );
        assert!(should_prefer_backbone_tie_candidate(
            Network::Mainnet,
            "seed1.dutago.xyz:19082",
            2,
            &"11".repeat(32),
            2,
            &"22".repeat(32),
            100,
            100,
        ));
        assert!(!should_prefer_backbone_tie_candidate(
            Network::Mainnet,
            "198.51.100.10:19082",
            2,
            &"11".repeat(32),
            2,
            &"22".repeat(32),
            100,
            100,
        ));
        assert!(!should_prefer_backbone_tie_candidate(
            Network::Mainnet,
            "seed1.dutago.xyz:19082",
            2,
            &"11".repeat(32),
            2,
            &"33".repeat(32),
            100,
            100,
        ));
    }

    #[test]
    fn sync_gate_rejects_local_submit_when_official_tip_competes_at_same_height() {
        if skip_when_sync_gate_disabled() {
            return;
        }
        let _guard = sync_gate_test_lock().lock().unwrap();
        clear_test_peer_state();
        super::note_outbound_peer_result(
            "seed1.dutago.xyz:19082",
            true,
            Some(26),
            Some(&"22".repeat(32)),
            None,
        );
        super::note_outbound_peer_result(
            "seed2.dutago.xyz:19082",
            true,
            Some(26),
            Some(&"22".repeat(32)),
            None,
        );
        let err =
            sync_gate_local_submit_ready(Network::Mainnet, 26, &"33".repeat(32), 22).unwrap_err();
        assert!(err.starts_with("sync_gate_competing_official_tip"));
    }

    #[test]
    fn sync_gate_allows_local_submit_when_official_tip_matches() {
        if skip_when_sync_gate_disabled() {
            return;
        }
        let _guard = sync_gate_test_lock().lock().unwrap();
        clear_test_peer_state();
        super::note_outbound_peer_result(
            "seed1.dutago.xyz:19082",
            true,
            Some(26),
            Some(&"22".repeat(32)),
            None,
        );
        super::note_outbound_peer_result(
            "seed2.dutago.xyz:19082",
            true,
            Some(26),
            Some(&"22".repeat(32)),
            None,
        );
        let ready = sync_gate_local_submit_ready(Network::Mainnet, 26, &"22".repeat(32), 22);
        assert!(ready.is_ok(), "unexpected error: {:?}", ready.err());
    }

    #[test]
    fn reorg_overlap_leaves_room_for_competing_tip_blocks() {
        assert_eq!(reorg_overlap_from(10), 0);
        assert_eq!(reorg_overlap_from(50), 0);
        assert_eq!(reorg_overlap_from((MAX_BLOCKS_PER_MSG / 2) as u64), 0);
        assert_eq!(
            reorg_overlap_from(MAX_BLOCKS_PER_MSG as u64),
            MAX_BLOCKS_PER_MSG / 2
        );
        assert_eq!(reorg_overlap_from(128), 64);
        assert_eq!(reorg_overlap_from(500), 500usize - (MAX_BLOCKS_PER_MSG / 2));
    }

    #[test]
    fn deeper_overlap_steps_back_by_half_window() {
        assert_eq!(deeper_overlap_from(0), 0);
        assert_eq!(deeper_overlap_from(22), 0);
        assert_eq!(deeper_overlap_from(64), 0);
        assert_eq!(deeper_overlap_from(128), 64);
        assert_eq!(deeper_overlap_from(200), 136);
    }

    #[test]
    fn read_line_limited_rejects_oversized_peer_line() {
        let payload = vec![b'x'; MAX_LINE_BYTES + 32];
        let mut reader = BufReader::new(Cursor::new(payload));
        let mut buf = Vec::new();
        let err = read_line_limited(&mut reader, &mut buf).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert_eq!(err.to_string(), "line_too_large");
    }

    #[test]
    fn sync_gate_allows_mining_during_hard_window_when_backbone_is_healthy() {
        if skip_when_sync_gate_disabled() {
            return;
        }
        let _guard = sync_gate_test_lock().lock().unwrap();
        clear_test_peer_state();

        let now = std::time::Instant::now();
        super::BEST_SEEN_HEIGHT.store(50, Ordering::Relaxed);
        if let Ok(mut peers) = super::state().outbound_recent.lock() {
            for addr in ["seed1.dutago.xyz:19082", "seed2.dutago.xyz:19082"] {
                peers.insert(
                    addr.to_string(),
                    super::PeerSnapshot {
                        addr: addr.to_string(),
                        inbound: false,
                        connected_at: now,
                        last_seen_at: now,
                        last_tip_height: 50,
                        last_tip_hash32: Some("11".repeat(32)),
                        last_error: None,
                        success_count: 1,
                        failure_count: 0,
                        resync_requests: 0,
                        competing_tip_events: 0,
                        official_tip_syncs: 0,
                    },
                );
            }
        }

        assert_eq!(
            sync_gate_mining_ready(Network::Mainnet, 50, &"11".repeat(32), 12),
            Ok(())
        );
    }

    #[test]
    fn transient_dial_error_classification_matches_expected_strings() {
        assert!(is_transient_dial_error(
            "Resource temporarily unavailable (os error 11)"
        ));
        assert!(is_transient_dial_error("operation would block"));
        assert!(is_transient_dial_error("timed out"));
        assert!(!is_transient_dial_error("connection refused"));
    }

    #[test]
    fn canonicalize_peer_token_rejects_urls_and_paths() {
        assert!(canonicalize_peer_token("http://seed.example.com:18082").is_none());
        assert!(canonicalize_peer_token("seed.example.com/path").is_none());
        assert!(canonicalize_peer_token("seed.example.com?x=1").is_none());
    }

    #[test]
    fn canonicalize_peer_token_normalizes_hosts_and_socket_addrs() {
        assert_eq!(
            canonicalize_peer_token("Seed.Example.Com:18082"),
            Some("seed.example.com:18082".to_string())
        );
        assert_eq!(
            canonicalize_peer_token("127.0.0.1:18082"),
            Some("127.0.0.1:18082".to_string())
        );
        assert_eq!(
            canonicalize_peer_token("2001:db8::1"),
            Some("2001:db8::1".to_string())
        );
    }

    #[test]
    fn parse_peers_text_deduplicates_and_ignores_comments() {
        let peers = parse_peers_text(
            "seed.example.com:18082\n\
             seed.example.com:18082 # dup\n\
             127.0.0.1:18082, 127.0.0.1:18082\n\
             invalid/path\n",
        );
        assert_eq!(
            peers,
            vec![
                "seed.example.com:18082".to_string(),
                "127.0.0.1:18082".to_string()
            ]
        );
    }

    #[test]
    fn normalize_bootstrap_candidates_deduplicates_equivalent_inputs() {
        let out = normalize_bootstrap_candidates(
            vec![
                "localhost:18082".to_string(),
                "LOCALHOST:18082".to_string(),
                "127.0.0.1:18082".to_string(),
                "invalid/path".to_string(),
            ],
            "18082",
        );
        assert_eq!(out, vec!["localhost:18082".to_string()]);
    }

    #[test]
    fn select_outbound_targets_prioritizes_official_backbone_peers() {
        let _guard = sync_gate_test_lock().lock().unwrap();
        clear_test_peer_state();
        let targets = super::select_outbound_targets(
            &[
                "198.51.100.10:19082".to_string(),
                "seed1.dutago.xyz:19082".to_string(),
                "seed2.dutago.xyz:19082".to_string(),
            ],
            "19082",
            "",
            2,
        );
        assert_eq!(targets.len(), 2);
        assert!(targets.iter().all(|addr| addr.starts_with("seed")));
    }

    #[test]
    fn block_broadcast_targets_include_official_backbone_first() {
        let _guard = sync_gate_test_lock().lock().unwrap();
        clear_test_peer_state();
        let targets = super::select_block_broadcast_targets(
            &[
                "198.51.100.10:19082".to_string(),
                "seed1.dutago.xyz:19082".to_string(),
                "seed2.dutago.xyz:19082".to_string(),
            ],
            "19082",
            "",
        );
        assert!(!targets.is_empty());
        assert!(targets[0].starts_with("seed"));
    }

    #[test]
    fn subnet24_key_ignores_private_and_loopback_ranges() {
        assert_eq!(subnet24_key(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))), None);
        assert_eq!(subnet24_key(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))), None);
        assert_eq!(
            subnet24_key(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 10))),
            None
        );
    }

    #[test]
    fn subnet24_key_returns_public_prefix_key() {
        assert_eq!(
            subnet24_key(IpAddr::V4(Ipv4Addr::new(8, 8, 4, 4))),
            Some((8u32 << 16) | (8u32 << 8) | 4u32)
        );
    }

    #[test]
    fn validate_block_basic_rejects_non_genesis_block_without_txs() {
        let block = ChainBlock {
            height: 1,
            hash32: "11".repeat(32),
            bits: 12,
            chainwork: 0,
            timestamp: Some(1),
            prevhash32: Some("22".repeat(32)),
            merkle32: Some("33".repeat(32)),
            nonce: Some(1),
            miner: Some("test1111111111111111111111111111111111111111".to_string()),
            pow_digest32: Some("44".repeat(32)),
            txs: None,
        };
        assert_eq!(
            validate_block_basic(1, &"22".repeat(32), &block).unwrap_err(),
            "missing_block_txs"
        );
    }

    #[test]
    fn manual_ban_and_unban_round_trip() {
        let banned = ban_peer_manual("203.0.113.9", Some("test")).unwrap();
        assert_eq!(
            banned.get("ip").and_then(|v| v.as_str()),
            Some("203.0.113.9")
        );
        let list = list_banned_json();
        let arr = list.as_array().cloned().unwrap_or_default();
        assert!(arr
            .iter()
            .any(|v| v.get("ip").and_then(|x| x.as_str()) == Some("203.0.113.9")));

        let unbanned = unban_peer_manual("203.0.113.9").unwrap();
        assert_eq!(
            unbanned.get("removed").and_then(|v| v.as_bool()),
            Some(true)
        );
        let list = list_banned_json();
        let arr = list.as_array().cloned().unwrap_or_default();
        assert!(!arr
            .iter()
            .any(|v| v.get("ip").and_then(|x| x.as_str()) == Some("203.0.113.9")));
    }

    #[test]
    fn sync_gate_requires_official_backbone_peer_on_mainnet() {
        if skip_when_sync_gate_disabled() {
            return;
        }
        let _guard = sync_gate_test_lock().lock().unwrap();
        clear_test_peer_state();
        let err = sync_gate_mining_ready(Network::Mainnet, 50, &"11".repeat(32), 12).unwrap_err();
        assert!(err.starts_with("sync_gate_official_peer_insufficient"));
    }

    #[test]
    fn sync_gate_accepts_recent_official_backbone_peer_on_mainnet() {
        if skip_when_sync_gate_disabled() {
            return;
        }
        let _guard = sync_gate_test_lock().lock().unwrap();
        clear_test_peer_state();
        super::note_outbound_peer_result(
            "seed1.dutago.xyz:19082",
            true,
            Some(50),
            Some(&"11".repeat(32)),
            None,
        );
        let err = sync_gate_mining_ready(Network::Mainnet, 50, &"11".repeat(32), 12).unwrap_err();
        assert!(err.contains("official_backbone_peers=1"));
        super::note_outbound_peer_result(
            "seed2.dutago.xyz:19082",
            true,
            Some(50),
            Some(&"11".repeat(32)),
            None,
        );
        let ready = sync_gate_mining_ready(Network::Mainnet, 50, &"11".repeat(32), 12);
        assert!(ready.is_ok(), "unexpected error: {:?}", ready.err());
    }

    #[test]
    fn sync_gate_accepts_recent_inbound_official_backbone_peer_on_mainnet() {
        if skip_when_sync_gate_disabled() {
            return;
        }
        let _guard = sync_gate_test_lock().lock().unwrap();
        clear_test_peer_state();
        super::note_inbound_peer_connected("seed1.dutago.xyz:19082");
        super::note_inbound_peer_tip("seed1.dutago.xyz:19082", 50, &"11".repeat(32));
        super::note_inbound_peer_connected("seed2.dutago.xyz:19082");
        super::note_inbound_peer_tip("seed2.dutago.xyz:19082", 50, &"11".repeat(32));
        let ready = sync_gate_mining_ready(Network::Mainnet, 50, &"11".repeat(32), 12);
        assert!(ready.is_ok(), "unexpected error: {:?}", ready.err());
    }

    #[test]
    fn sync_gate_rejects_official_backbone_height_mismatch() {
        if skip_when_sync_gate_disabled() {
            return;
        }
        let _guard = sync_gate_test_lock().lock().unwrap();
        clear_test_peer_state();
        super::note_outbound_peer_result(
            "seed1.dutago.xyz:19082",
            true,
            Some(50),
            Some(&"11".repeat(32)),
            None,
        );
        super::note_outbound_peer_result(
            "seed2.dutago.xyz:19082",
            true,
            Some(51),
            Some(&"22".repeat(32)),
            None,
        );
        let err = sync_gate_mining_ready(Network::Mainnet, 50, &"11".repeat(32), 12).unwrap_err();
        assert!(err.starts_with("sync_gate_official_tip_mismatch"));
    }

    #[test]
    fn sync_gate_allows_one_block_of_backbone_lag_when_one_seed_matches_tip() {
        if skip_when_sync_gate_disabled() {
            return;
        }
        let _guard = sync_gate_test_lock().lock().unwrap();
        clear_test_peer_state();
        super::note_outbound_peer_result(
            "seed1.dutago.xyz:19082",
            true,
            Some(50),
            Some(&"11".repeat(32)),
            None,
        );
        super::note_outbound_peer_result(
            "seed2.dutago.xyz:19082",
            true,
            Some(49),
            Some(&"99".repeat(32)),
            None,
        );
        let ready = sync_gate_mining_ready(Network::Mainnet, 50, &"11".repeat(32), 12);
        assert!(ready.is_ok(), "unexpected error: {:?}", ready.err());
    }

    #[test]
    fn sync_gate_allows_when_all_backbone_peers_are_exactly_one_block_behind() {
        if skip_when_sync_gate_disabled() {
            return;
        }
        let _guard = sync_gate_test_lock().lock().unwrap();
        clear_test_peer_state();
        super::note_outbound_peer_result(
            "seed1.dutago.xyz:19082",
            true,
            Some(49),
            Some(&"aa".repeat(32)),
            None,
        );
        super::note_outbound_peer_result(
            "seed2.dutago.xyz:19082",
            true,
            Some(49),
            Some(&"bb".repeat(32)),
            None,
        );
        let ready = sync_gate_mining_ready(Network::Mainnet, 50, &"11".repeat(32), 12);
        assert!(ready.is_err(), "unexpected ready state");
    }

    #[test]
    fn sync_gate_allows_all_backbone_peers_one_block_behind_only_at_bootstrap_start() {
        if skip_when_sync_gate_disabled() {
            return;
        }
        let _guard = sync_gate_test_lock().lock().unwrap();
        clear_test_peer_state();
        super::note_outbound_peer_result(
            "seed1.dutago.xyz:19082",
            true,
            Some(0),
            Some(&"aa".repeat(32)),
            None,
        );
        super::note_outbound_peer_result(
            "seed2.dutago.xyz:19082",
            true,
            Some(0),
            Some(&"bb".repeat(32)),
            None,
        );
        let ready = sync_gate_mining_ready(Network::Mainnet, 1, &"11".repeat(32), 12);
        assert!(ready.is_ok(), "unexpected error: {:?}", ready.err());
    }

    #[test]
    fn official_tip_sync_uses_overlap_for_official_ahead_or_conflicting_tip() {
        clear_test_peer_state();
        assert_eq!(
            official_tip_sync_from(
                Network::Mainnet,
                "seed1.dutago.xyz:19082",
                10,
                &"aa".repeat(32),
                11,
                &"bb".repeat(32)
            ),
            0usize
        );
        super::note_outbound_peer_result(
            "seed1.dutago.xyz:19082",
            true,
            Some(21),
            Some(&"aa".repeat(32)),
            None,
        );
        super::note_outbound_peer_result(
            "seed2.dutago.xyz:19082",
            true,
            Some(21),
            Some(&"aa".repeat(32)),
            None,
        );
        assert_eq!(
            official_tip_sync_from(
                Network::Mainnet,
                "seed1.dutago.xyz:19082",
                21,
                &"aa".repeat(32),
                22,
                &"bb".repeat(32)
            ),
            reorg_overlap_from(21)
        );
        assert_eq!(
            official_tip_sync_from(
                Network::Mainnet,
                "seed1.dutago.xyz:19082",
                21,
                &"aa".repeat(32),
                21,
                &"bb".repeat(32)
            ),
            reorg_overlap_from(21)
        );
        assert_eq!(
            official_tip_sync_from(
                Network::Mainnet,
                "198.51.100.10:19082",
                21,
                &"aa".repeat(32),
                22,
                &"bb".repeat(32)
            ),
            22usize
        );
        assert_eq!(health_priority_sync_from(128), 2usize);
    }

    #[test]
    fn sync_gate_rejects_backbone_that_lags_more_than_one_block() {
        if skip_when_sync_gate_disabled() {
            return;
        }
        let _guard = sync_gate_test_lock().lock().unwrap();
        clear_test_peer_state();
        super::note_outbound_peer_result(
            "seed1.dutago.xyz:19082",
            true,
            Some(50),
            Some(&"11".repeat(32)),
            None,
        );
        super::note_outbound_peer_result(
            "seed2.dutago.xyz:19082",
            true,
            Some(48),
            Some(&"88".repeat(32)),
            None,
        );
        let err = sync_gate_mining_ready(Network::Mainnet, 50, &"11".repeat(32), 12).unwrap_err();
        assert!(err.starts_with("sync_gate_official_tip_mismatch"));
    }

    #[test]
    fn sync_gate_rejects_conflicting_hash_at_same_height() {
        if skip_when_sync_gate_disabled() {
            return;
        }
        let _guard = sync_gate_test_lock().lock().unwrap();
        clear_test_peer_state();
        super::note_outbound_peer_result(
            "seed1.dutago.xyz:19082",
            true,
            Some(50),
            Some(&"11".repeat(32)),
            None,
        );
        super::note_outbound_peer_result(
            "seed2.dutago.xyz:19082",
            true,
            Some(50),
            Some(&"22".repeat(32)),
            None,
        );
        let err = sync_gate_mining_ready(Network::Mainnet, 50, &"11".repeat(32), 12).unwrap_err();
        assert!(err.starts_with("sync_gate_official_tip_mismatch"));
    }

    #[test]
    fn sync_gate_is_inactive_after_bootstrap_window() {
        if skip_when_sync_gate_disabled() {
            return;
        }
        let _guard = sync_gate_test_lock().lock().unwrap();
        clear_test_peer_state();
        super::BEST_SEEN_HEIGHT.store(2000, Ordering::Relaxed);
        super::note_outbound_peer_result(
            "seed1.dutago.xyz:19082",
            true,
            Some(2000),
            Some(&"11".repeat(32)),
            None,
        );
        super::note_outbound_peer_result(
            "seed2.dutago.xyz:19082",
            true,
            Some(2000),
            Some(&"11".repeat(32)),
            None,
        );
        let ready = sync_gate_mining_ready(Network::Mainnet, 2000, &"11".repeat(32), 22);
        assert!(ready.is_ok(), "unexpected error: {:?}", ready.err());
    }

    #[test]
    fn sync_gate_stays_active_after_bootstrap_window_if_network_is_unhealthy() {
        if skip_when_sync_gate_disabled() {
            return;
        }
        let _guard = sync_gate_test_lock().lock().unwrap();
        clear_test_peer_state();
        super::BEST_SEEN_HEIGHT.store(2050, Ordering::Relaxed);
        super::note_outbound_peer_result(
            "seed1.dutago.xyz:19082",
            true,
            Some(2050),
            Some(&"11".repeat(32)),
            None,
        );
        let err = sync_gate_mining_ready(Network::Mainnet, 2000, &"11".repeat(32), 22).unwrap_err();
        assert!(
            err.starts_with("sync_gate_syncing")
                || err.starts_with("sync_gate_official_peer_insufficient")
        );
    }
}
