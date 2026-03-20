#[macro_use]
mod debuglog;
mod amount_display;
mod canon_json;
use clap::{Parser, Subcommand};
use duta_core::netparams::Network;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::fs;
use std::io::Read;
use std::net::{IpAddr, SocketAddr, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::thread;
use std::time::Duration;

mod btc_rpc;
mod info;
mod p2p;
mod rpc_json;
mod store;
mod submit_tx;
mod submit_work;
mod utxo_rpc;
mod work;

const ANSI_RESET: &str = "\x1b[0m";
const ANSI_BOLD: &str = "\x1b[1m";
const ANSI_DIM: &str = "\x1b[2m";
const ANSI_GREEN: &str = "\x1b[32m";
const ANSI_YELLOW: &str = "\x1b[33m";
const ANSI_BLUE: &str = "\x1b[34m";
const ANSI_CYAN: &str = "\x1b[36m";
const ANSI_WHITE: &str = "\x1b[37m";

fn console_tag(tag: &str, color: &str) -> String {
    format!("{}{}{: <8}{}", ANSI_BOLD, color, tag, ANSI_RESET)
}

fn console_kv(tag: &str, color: &str, key: &str, value: impl AsRef<str>) {
    println!(
        "{} {}{}{}",
        console_tag(tag, color),
        key,
        format!("{}:{}", ANSI_DIM, ANSI_RESET),
        value.as_ref()
    );
}

fn console_line(tag: &str, color: &str, value: impl AsRef<str>) {
    println!("{} {}", console_tag(tag, color), value.as_ref());
}

#[derive(Parser, Debug)]
#[command(
    name = "dutad",
    version,
    about = "DUTA full node daemon",
    after_help = "Examples:\n  dutad --daemon\n  dutad status\n  dutad stop\n  duta-cli getpeerinfo\n  duta-cli listbanned\n  duta-cli banpeer 203.0.113.10 launch_abuse\n  duta-cli unbanpeer 203.0.113.10\n  dutad --testnet --daemon"
)]
struct Args {
    /// Data directory (default mainnet: ~/.duta, testnet: ~/.duta/testnet, stagenet: ~/.duta/stagenet). Overrides config datadir=.
    #[arg(long)]
    datadir: Option<String>,

    /// Path to config file (default: <datadir>/duta.conf)
    #[arg(long)]
    conf: Option<String>,

    /// Run in background (non-systemd): spawn child, write pid/log to <datadir>
    #[arg(long)]
    daemon: bool,

    /// Internal: set when running as the spawned child process
    #[arg(long, hide = true)]
    foreground: bool,

    /// Stop background daemon (reads <datadir>/dutad.pid and sends SIGTERM). Use without --daemon.
    #[arg(long)]
    stop: bool,

    #[command(subcommand)]
    command: Option<Cmd>,

    /// Use testnet ports and data directory
    #[arg(long)]
    testnet: bool,

    /// Use stagenet ports and data directory
    #[arg(long)]
    stagenet: bool,

    /// Bind address for daemon RPC (default: 127.0.0.1). Admin RPC is loopback-only in release builds.
    #[arg(long)]
    rpc_bind: Option<String>,

    /// Bind address for P2P (IP only; port is LOCKED by network)
    #[arg(long)]
    bind: Option<String>,

    /// Bind address for public mining HTTP (Phase 3). If set, /work and /submit_work move here
    /// and are removed from admin RPC.
    /// Example testnet: 172.16.20.22:18085 (operator-explicit public/LAN exposure)
    #[arg(long)]
    mining_bind: Option<String>,

    /// Comma-separated bootstrap peers (IP or host). If empty, uses built-in cluster seeds.
    #[arg(long, value_delimiter = ',', num_args = 1..)]
    seeds: Vec<String>,

    /// Add a bootstrap peer (bitcoin-like alias for seeds)
    #[arg(long, num_args = 1..)]
    addnode: Vec<String>,
}

#[derive(Subcommand, Debug, Clone)]
enum Cmd {
    /// Stop background daemon
    Stop,
    /// Show daemon status
    Status,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ChainBlock {
    height: u64,
    hash32: String,
    bits: u64,
    chainwork: u64,
    #[serde(default)]
    timestamp: Option<u64>,
    #[serde(default)]
    prevhash32: Option<String>,
    #[serde(default)]
    merkle32: Option<String>,
    #[serde(default)]
    nonce: Option<u64>,
    #[serde(default)]
    miner: Option<String>,
    #[serde(default)]
    pow_digest32: Option<String>,
    #[serde(default)]
    txs: Option<serde_json::Value>,
}

pub(crate) fn parse_query(url: &str) -> (String, Vec<(String, String)>) {
    let mut path = url;
    let mut query = "";
    if let Some((p, q)) = url.split_once('?') {
        path = p;
        query = q;
    }

    let mut out = Vec::new();
    if !query.is_empty() {
        for part in query.split('&') {
            if part.is_empty() {
                continue;
            }
            let (k, v) = part.split_once('=').unwrap_or((part, ""));
            out.push((percent_decode(k), percent_decode(v)));
        }
    }
    (path.to_string(), out)
}

fn percent_decode(s: &str) -> String {
    // Minimal percent-decoder (good enough for our numeric params)
    let mut out = String::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let h1 = bytes[i + 1];
            let h2 = bytes[i + 2];
            let hex = |c: u8| -> Option<u8> {
                match c {
                    b'0'..=b'9' => Some(c - b'0'),
                    b'a'..=b'f' => Some(c - b'a' + 10),
                    b'A'..=b'F' => Some(c - b'A' + 10),
                    _ => None,
                }
            };
            if let (Some(a), Some(b)) = (hex(h1), hex(h2)) {
                out.push((a * 16 + b) as char);
                i += 3;
                continue;
            }
        }
        if bytes[i] == b'+' {
            out.push(' ');
        } else {
            out.push(bytes[i] as char);
        }
        i += 1;
    }
    out
}

// --- Mining hashrate aggregation (per wallet address) ---
// We keep this intentionally simple: miners report their local hashrate via /work?hs=<H/s>.
// The server aggregates per wallet and exposes /minerstats?address=<WALLET> on the mining listener.
// This is for operator visibility only (NOT consensus-critical).

const MINER_ACTIVE_SECS: u64 = 90; // if no update within this window, miner is considered inactive
const MAX_MINING_ADDRESS_BYTES: usize = 160;
const MAX_MINER_HASHRATE_HS: f64 = 1_000_000_000_000_000.0;

#[derive(Clone, Debug, Default, Serialize)]
struct MinerEntry {
    ip: String,
    #[serde(rename = "hs")]
    hs: f64,
    last_seen: u64,
}

#[derive(Clone, Debug, Serialize)]
struct PublicMinerEntry {
    #[serde(rename = "hs")]
    hs: f64,
    last_seen: u64,
}

#[derive(Default)]
struct MinerStats {
    // wallet -> ip -> entry
    wallets: HashMap<String, HashMap<String, MinerEntry>>,
}

static MINER_STATS: OnceLock<Mutex<MinerStats>> = OnceLock::new();

fn miner_stats() -> &'static Mutex<MinerStats> {
    MINER_STATS.get_or_init(|| Mutex::new(MinerStats::default()))
}

fn remote_ip_string(req: &tiny_http::Request) -> String {
    req.remote_addr()
        .map(|a: &SocketAddr| a.ip().to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn print_startup_banner(
    net: Network,
    data_dir: &str,
    p2p_addr: &str,
    rpc_addr: &str,
    mining_addr: Option<&str>,
) {
    println!();
    console_line(
        "DAEMON",
        ANSI_WHITE,
        format!(
            "DUTA daemon {} starting on {}",
            env!("CARGO_PKG_VERSION"),
            net.as_str()
        ),
    );
    console_kv("PATH", ANSI_YELLOW, "data dir", data_dir);
    console_kv("P2P", ANSI_CYAN, "bind", p2p_addr);
    console_kv("RPC", ANSI_BLUE, "bind", rpc_addr);
    console_kv(
        "MINING",
        ANSI_GREEN,
        "bind",
        mining_addr.unwrap_or("disabled"),
    );
    println!();
}

fn print_chain_status(data_dir: &str) {
    let (tip_height, tip_hash, _tip_chainwork, bits) =
        store::tip_fields(data_dir).unwrap_or((0, "0".repeat(64), 0, 0));
    let best_seen_height = p2p::best_seen_height().max(tip_height);
    let has_healthy_peer = p2p::bootstrap_has_healthy_peer();
    let (_is_ready, health_status, sync_progress) =
        daemon_health_state(tip_height, best_seen_height, has_healthy_peer);
    console_line(
        "CHAIN",
        ANSI_YELLOW,
        format!("tip={} bits={} hash={}", tip_height, bits, tip_hash),
    );
    match health_status {
        "no_peers" => {
            console_line("SYNC", ANSI_YELLOW, "waiting_for_peers");
        }
        "syncing" => {
            console_line(
                "SYNC",
                ANSI_YELLOW,
                format!(
                    "syncing ({:.2}%) best_seen_height={}",
                    sync_progress * 100.0,
                    best_seen_height
                ),
            );
        }
        _ => {
            console_line("SYNC", ANSI_GREEN, "ready");
        }
    }
}

fn print_startup_guidance(rpc_addr: &str) {
    console_kv(
        "RPC",
        ANSI_BLUE,
        "health",
        format!("http://{}/health", rpc_addr),
    );
    console_kv("RPC", ANSI_BLUE, "tip", format!("http://{}/tip", rpc_addr));
    console_line(
        "STATUS",
        ANSI_GREEN,
        "daemon started, waiting for peer sync and block activity",
    );
    println!();
}

fn print_runtime_status_tick(data_dir: &str) {
    let (tip_height, tip_hash, _tip_chainwork, bits) =
        store::tip_fields(data_dir).unwrap_or((0, "0".repeat(64), 0, 0));
    let best_seen_height = p2p::best_seen_height().max(tip_height);
    let has_healthy_peer = p2p::bootstrap_has_healthy_peer();
    let (_is_ready, health_status, sync_progress) =
        daemon_health_state(tip_height, best_seen_height, has_healthy_peer);
    console_line(
        "STATUS",
        ANSI_CYAN,
        format!(
            "tip={} best_seen={} bits={} sync={:.2}% peer_ready={} status={} hash={}",
            tip_height,
            best_seen_height,
            bits,
            sync_progress * 100.0,
            if has_healthy_peer { "yes" } else { "no" },
            health_status,
            tip_hash
        ),
    );
}

fn admin_rpc_bind_is_allowed(bind: &str) -> bool {
    bind.trim() == "127.0.0.1"
}

fn validate_conf_admin_rpc_bind(conf: &duta_core::netparams::Conf) -> Result<(), String> {
    if let Some(raw) = conf
        .get_last("rpcconnect")
        .or_else(|| conf.get_last("rpcbind"))
    {
        let trimmed = raw.trim();
        if !trimmed.is_empty() && trimmed.contains(':') {
            return Err(format!("invalid_admin_rpc_bind: {}", trimmed));
        }
    }
    Ok(())
}

fn validate_locked_bind_override(raw: &str, expected_port: u16, what: &str) -> Result<(), String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(());
    }
    if let Some((host, port)) = trimmed.split_once(':') {
        if host.trim().is_empty() {
            return Err(format!("invalid_{}: {}", what, trimmed));
        }
        let parsed = port
            .trim()
            .parse::<u16>()
            .map_err(|_| format!("invalid_{}: {}", what, trimmed))?;
        if parsed != expected_port {
            return Err(format!("invalid_{}: {}", what, trimmed));
        }
    }
    Ok(())
}

fn validate_conf_listener_binds(
    net: Network,
    conf: &duta_core::netparams::Conf,
) -> Result<(), String> {
    if let Some(raw) = conf.get_last("bind") {
        validate_locked_bind_override(&raw, net.default_p2p_port(), "p2p_bind")?;
    }
    if let Some(raw) = conf
        .get_last("mining_bind")
        .or_else(|| conf.get_last("miningbind"))
        .or_else(|| conf.get_last("miningaddr"))
    {
        validate_locked_bind_override(&raw, net.default_p2p_port().saturating_add(3), "mining_bind")?;
    }
    Ok(())
}

fn load_runtime_conf(conf_path: &str, required: bool) -> Result<duta_core::netparams::Conf, String> {
    match fs::read_to_string(conf_path) {
        Ok(s) => Ok(duta_core::netparams::Conf::parse(&s)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound && !required => {
            Ok(duta_core::netparams::Conf::default())
        }
        Err(e) => Err(format!("config_read_failed: path={} err={}", conf_path, e)),
    }
}

fn lock_or_recover<'a, T>(mutex: &'a Mutex<T>, name: &str) -> MutexGuard<'a, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            edlog!("daemon: mutex_poison_recovered name={}", name);
            poisoned.into_inner()
        }
    }
}

fn update_miner_stats(wallet: &str, ip: &str, hs: f64) {
    if wallet.is_empty() {
        return;
    }
    let now = now_unix();
    let mut st = lock_or_recover(miner_stats(), "miner_stats");

    let w = st.wallets.entry(wallet.to_string()).or_default();

    w.insert(
        ip.to_string(),
        MinerEntry {
            ip: ip.to_string(),
            hs,
            last_seen: now,
        },
    );

    // prune inactive miners for all wallets (bounded memory)
    st.wallets.retain(|_, miners| {
        miners.retain(|_, ent| now.saturating_sub(ent.last_seen) <= MINER_ACTIVE_SECS);
        !miners.is_empty()
    });
}

fn normalize_mining_wallet(raw: &str) -> Option<String> {
    let wallet = raw.trim();
    if wallet.is_empty() || wallet.len() > MAX_MINING_ADDRESS_BYTES {
        return None;
    }
    if wallet.contains(char::is_whitespace) {
        return None;
    }
    Some(wallet.to_string())
}

fn parse_reported_hashrate(params: &[(String, String)]) -> f64 {
    let hs = get_param(params, "hs")
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(0.0);
    if !hs.is_finite() || hs.is_sign_negative() {
        return 0.0;
    }
    hs.min(MAX_MINER_HASHRATE_HS)
}

fn handle_minerstats(
    request: tiny_http::Request,
    params: &[(String, String)],
    respond_json: fn(tiny_http::Request, tiny_http::StatusCode, String),
) {
    if request.method() != &tiny_http::Method::Get {
        respond_error(request, tiny_http::StatusCode(405), "method_not_allowed");
        return;
    }

    let wallet = get_param(params, "address")
        .or_else(|| get_param(params, "wallet"))
        .unwrap_or("");

    if wallet.is_empty() {
        respond_error(request, tiny_http::StatusCode(400), "missing_address");
        return;
    }

    let now = now_unix();
    let st = lock_or_recover(miner_stats(), "miner_stats");
    let mut miners_out: Vec<PublicMinerEntry> = Vec::new();
    let mut total_hs: f64 = 0.0;

    if let Some(mm) = st.wallets.get(wallet) {
        for (_ip, ent) in mm.iter() {
            if now.saturating_sub(ent.last_seen) <= MINER_ACTIVE_SECS {
                total_hs += ent.hs.max(0.0);
                miners_out.push(PublicMinerEntry {
                    hs: ent.hs,
                    last_seen: ent.last_seen,
                });
            }
        }
    }

    miners_out.sort_by(|a, b| b.hs.partial_cmp(&a.hs).unwrap_or(std::cmp::Ordering::Equal));

    respond_json(
        request,
        tiny_http::StatusCode(200),
        json!({
            "address": wallet,
            "miners": miners_out.len(),
            "total_hs": total_hs,
            "per_miner": miners_out,
            "active_window_secs": MINER_ACTIVE_SECS,
        })
        .to_string(),
    );
}

pub(crate) fn get_param<'a>(params: &'a [(String, String)], key: &str) -> Option<&'a str> {
    params
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.as_str())
}

pub(crate) const MAX_RPC_BODY_BYTES: usize = 128 * 1024;
const MAX_RPC_URL_BYTES: usize = 8 * 1024;
use std::time::{SystemTime, UNIX_EPOCH};

const WORK_MAX_PER_SEC: u32 = 60;
const SUBMIT_MAX_PER_SEC: u32 = 120;
const RPC_MAX_PER_SEC: u32 = 120;
const QUERY_MAX_PER_SEC: u32 = 600;
const WORK_MAX_PER_IP_PER_SEC: u32 = 8;
const WORK_MAX_PER_PRIVATE_IP_PER_SEC: u32 = 64;
const SUBMIT_MAX_PER_IP_PER_SEC: u32 = 16;
const RPC_MAX_PER_IP_PER_SEC: u32 = 20;
const QUERY_MAX_PER_IP_PER_SEC: u32 = 30;
const BLOCKS_FROM_MAX_LIMIT: usize = 1024;

#[derive(Debug, Default)]
struct RateCounters {
    work: u32,
    submit: u32,
    rpc: u32,
    query: u32,
}

#[derive(Debug, Default)]
struct RateState {
    sec: u64,
    global: RateCounters,
    per_ip: HashMap<String, RateCounters>,
}

static RATE_STATE: OnceLock<Mutex<RateState>> = OnceLock::new();

fn now_sec() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn request_ip_key(request: &tiny_http::Request) -> String {
    request
        .remote_addr()
        .map(|a: &SocketAddr| a.ip().to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

fn ip_is_loopback_or_private(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => v4.is_loopback() || v4.is_private(),
        IpAddr::V6(v6) => v6.is_loopback() || v6.is_unique_local(),
    }
}

fn request_allows_private_work_burst(request: &tiny_http::Request, kind: &str) -> bool {
    if kind != "work" {
        return false;
    }
    request
        .remote_addr()
        .map(|a: &SocketAddr| ip_is_loopback_or_private(a.ip()))
        .unwrap_or(false)
}

fn counters_for_kind_mut<'a>(c: &'a mut RateCounters, kind: &str) -> Option<(&'a mut u32, u32)> {
    match kind {
        "work" => Some((&mut c.work, WORK_MAX_PER_SEC)),
        "submit" => Some((&mut c.submit, SUBMIT_MAX_PER_SEC)),
        "rpc" => Some((&mut c.rpc, RPC_MAX_PER_SEC)),
        "query" => Some((&mut c.query, QUERY_MAX_PER_SEC)),
        _ => None,
    }
}

fn counters_for_kind_ip_mut<'a>(c: &'a mut RateCounters, kind: &str) -> Option<(&'a mut u32, u32)> {
    match kind {
        "work" => Some((&mut c.work, WORK_MAX_PER_IP_PER_SEC)),
        "submit" => Some((&mut c.submit, SUBMIT_MAX_PER_IP_PER_SEC)),
        "rpc" => Some((&mut c.rpc, RPC_MAX_PER_IP_PER_SEC)),
        "query" => Some((&mut c.query, QUERY_MAX_PER_IP_PER_SEC)),
        _ => None,
    }
}

fn per_ip_rate_limit(kind: &str, private_work_burst: bool) -> Option<u32> {
    let (_, ip_max) = counters_for_kind_ip_mut(&mut RateCounters::default(), kind)?;
    Some(if kind == "work" && private_work_burst {
        WORK_MAX_PER_PRIVATE_IP_PER_SEC
    } else {
        ip_max
    })
}

fn rate_allow(request: &tiny_http::Request, kind: &str) -> bool {
    let sec = now_sec();
    let ip = request_ip_key(request);
    let m = RATE_STATE.get_or_init(|| Mutex::new(RateState::default()));
    let mut st = lock_or_recover(m, "rate_state");
    if st.sec != sec {
        st.sec = sec;
        st.global = RateCounters::default();
        st.per_ip.clear();
    }

    {
        let Some((global_ctr, global_max)) = counters_for_kind_mut(&mut st.global, kind) else {
            return true;
        };
        if *global_ctr >= global_max {
            return false;
        }
    }

    {
        let ip_state = st.per_ip.entry(ip).or_default();
        let Some((ip_ctr, ip_max)) = counters_for_kind_ip_mut(ip_state, kind) else {
            return true;
        };
        let ip_max = per_ip_rate_limit(kind, request_allows_private_work_burst(request, kind))
            .unwrap_or(ip_max);
        if *ip_ctr >= ip_max {
            return false;
        }
        *ip_ctr += 1;
    }

    if let Some((global_ctr, _)) = counters_for_kind_mut(&mut st.global, kind) {
        *global_ctr += 1;
    }
    true
}

fn respond_429(req: tiny_http::Request) {
    respond_error_detail(
        req,
        tiny_http::StatusCode(429),
        "rate_limited",
        json!({"retry_after_secs":1}),
    );
}

fn normalize_path_maybe_home(s: &str) -> String {
    let mut out = s.trim().to_string();
    if out.is_empty() {
        return out;
    }

    let home_dir = || -> Option<String> {
        std::env::var("HOME")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .or_else(|| {
                std::env::var("USERPROFILE")
                    .ok()
                    .filter(|v| !v.trim().is_empty())
            })
            .or_else(|| {
                let drive = std::env::var("HOMEDRIVE").ok()?;
                let path = std::env::var("HOMEPATH").ok()?;
                let joined = format!("{}{}", drive.trim(), path.trim());
                if joined.trim().is_empty() {
                    None
                } else {
                    Some(joined)
                }
            })
    };

    if out == "~" || out.starts_with("~/") || out.starts_with("~\\") {
        if let Some(home) = home_dir() {
            let home = home.trim_end_matches(['/', '\\']);
            if out == "~" {
                return home.to_string();
            }
            let suffix = out[1..].trim_start_matches(['/', '\\']);
            out = format!("{}/{}", home, suffix);
        }
    } else if out.starts_with(".duta/") || out.starts_with(".duta\\") {
        if let Some(home) = home_dir() {
            let home = home.trim_end_matches(['/', '\\']);
            out = format!("{}/{}", home, out);
        }
    }
    out
}

fn validate_conf_network_name(conf: &duta_core::netparams::Conf) -> Result<(), String> {
    if let Some(raw) = conf.get_last("network").or_else(|| conf.get_last("chain")) {
        let trimmed = raw.trim();
        if !trimmed.is_empty() && Network::parse_name(trimmed).is_none() {
            return Err(format!("invalid_network_name: {}", trimmed));
        }
    }
    Ok(())
}

fn process_exists(pid: i32) -> bool {
    if pid <= 1 {
        return false;
    }

    #[cfg(windows)]
    {
        std::process::Command::new("tasklist")
            .args(["/FI", &format!("PID eq {}", pid), "/FO", "CSV", "/NH"])
            .output()
            .ok()
            .and_then(|o| String::from_utf8(o.stdout).ok())
            .map(|s| s.lines().any(|line| line.starts_with('\"')))
            .unwrap_or(false)
    }

    #[cfg(not(windows))]
    {
        let proc_path = format!("/proc/{}", pid);
        if std::path::Path::new(&proc_path).exists() {
            return true;
        }
        std::process::Command::new("kill")
            .args(["-0", &pid.to_string()])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
    }
}

fn pid_matches_dutad_process(pid: i32) -> bool {
    if pid <= 1 {
        return false;
    }

    #[cfg(windows)]
    {
        return std::process::Command::new("tasklist")
            .args(["/FI", &format!("PID eq {}", pid), "/FO", "CSV", "/NH"])
            .output()
            .ok()
            .and_then(|o| String::from_utf8(o.stdout).ok())
            .map(|s| s.to_ascii_lowercase().contains("dutad"))
            .unwrap_or(false);
    }

    #[cfg(not(windows))]
    {
        let cmdline_path = format!("/proc/{}/cmdline", pid);
        fs::read(&cmdline_path)
            .ok()
            .map(|bytes| String::from_utf8_lossy(&bytes).to_ascii_lowercase().contains("dutad"))
            .unwrap_or(false)
    }
}

fn http_health_status(rpc_addr: &str) -> Result<u16, String> {
    let mut stream =
        TcpStream::connect(rpc_addr).map_err(|e| format!("connect {} failed: {}", rpc_addr, e))?;
    stream.set_read_timeout(Some(Duration::from_secs(2))).ok();
    stream.set_write_timeout(Some(Duration::from_secs(2))).ok();
    let req = format!(
        "GET /health HTTP/1.1\r\nHost: {host}\r\nUser-Agent: dutad-daemonize\r\nAccept: application/json\r\nConnection: close\r\n\r\n",
        host = rpc_addr
    );
    use std::io::Write;
    stream
        .write_all(req.as_bytes())
        .map_err(|e| format!("health_write_failed: {}", e))?;
    let mut resp = Vec::new();
    stream
        .read_to_end(&mut resp)
        .map_err(|e| format!("health_read_failed: {}", e))?;
    let resp_s = String::from_utf8_lossy(&resp);
    let status = resp_s
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse::<u16>().ok())
        .ok_or_else(|| "health_status_missing".to_string())?;
    Ok(status)
}

fn http_health_reachable(rpc_addr: &str) -> bool {
    http_health_status(rpc_addr).map(|status| status > 0).unwrap_or(false)
}

fn daemon_health_state(
    tip_height: u64,
    best_seen_height: u64,
    has_healthy_peer: bool,
) -> (bool, &'static str, f64) {
    let sync_progress = if best_seen_height == 0 || tip_height >= best_seen_height {
        1.0
    } else {
        tip_height as f64 / best_seen_height as f64
    };
    if !has_healthy_peer {
        return (false, "no_peers", sync_progress);
    }
    if tip_height >= best_seen_height {
        (true, "ready", sync_progress)
    } else {
        (false, "syncing", sync_progress)
    }
}

fn terminate_process(pid: i32) -> bool {
    if pid <= 1 {
        return false;
    }

    #[cfg(windows)]
    {
        let soft = std::process::Command::new("taskkill")
            .args(["/PID", &pid.to_string(), "/T"])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false);
        if soft {
            return true;
        }
        std::process::Command::new("taskkill")
            .args(["/PID", &pid.to_string(), "/T", "/F"])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
    }

    #[cfg(not(windows))]
    {
        std::process::Command::new("kill")
            .args(["-TERM", &pid.to_string()])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
    }
}

fn write_pid_file(pid_path: &str, pid: u32) -> Result<(), String> {
    store::durable_write_string(pid_path, &format!("{}\n", pid))
        .map_err(|e| format!("pid_write_failed: {}", e))
}

fn stop_daemon(data_dir: &str) -> i32 {
    let pid_path = format!("{}/dutad.pid", data_dir.trim_end_matches('/'));
    let pid_s = match fs::read_to_string(&pid_path) {
        Ok(s) => s,
        Err(_) => {
            eprintln!("dutad: not running (missing pid file: {})", pid_path);
            return 1;
        }
    };
    let pid: i32 = match pid_s.trim().parse() {
        Ok(p) if p > 1 => p,
        _ => {
            eprintln!("dutad: invalid pid file: {}", pid_path);
            return 1;
        }
    };

    if !process_exists(pid) || !pid_matches_dutad_process(pid) {
        // Stale pid file: remove it so next start works cleanly.
        let _ = fs::remove_file(&pid_path);
        eprintln!(
            "dutad: not running (stale pid={}, removed {})",
            pid, pid_path
        );
        return 1;
    }

    let sent = terminate_process(pid);

    if !sent {
        eprintln!("dutad: failed to send SIGTERM to pid={}", pid);
        return 1;
    }

    // Wait briefly for exit; if it stops, clean pid file.
    for _ in 0..50 {
        if !process_exists(pid) {
            let _ = fs::remove_file(&pid_path);
            println!("dutad: stopped (pid={})", pid);
            return 0;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    eprintln!("dutad: SIGTERM sent but still running (pid={})", pid);
    1
}

fn daemon_status(data_dir: &str, rpc_addr: &str) -> i32 {
    let pid_path = format!("{}/dutad.pid", data_dir.trim_end_matches('/'));
    let rpc_reachable = http_health_reachable(rpc_addr);
    let pid_s = match fs::read_to_string(&pid_path) {
        Ok(s) => s,
        Err(_) => {
            if rpc_reachable {
                println!("dutad: running");
                println!("pid: externally_managed_or_missing");
                println!("rpc: {}", rpc_addr);
                println!("rpc_reachable: yes");
                return 0;
            } else {
                println!("dutad: stopped");
                println!("rpc: {}", rpc_addr);
                println!("rpc_reachable: no");
                return 1;
            }
        }
    };
    let pid: i32 = match pid_s.trim().parse() {
        Ok(p) if p > 1 => p,
        _ => {
            let _ = fs::remove_file(&pid_path);
            if rpc_reachable {
                println!("dutad: running");
                println!("stale_pid_removed: invalid");
                println!("pid: externally_managed_or_missing");
                println!("rpc: {}", rpc_addr);
                println!("rpc_reachable: yes");
                return 0;
            } else {
                println!("dutad: stopped");
                println!("stale_pid_removed: invalid");
                println!("rpc: {}", rpc_addr);
                println!("rpc_reachable: no");
                return 1;
            }
        }
    };
    if process_exists(pid) && pid_matches_dutad_process(pid) {
        println!("dutad: running");
        println!("pid: {}", pid);
        println!("rpc: {}", rpc_addr);
        println!(
            "rpc_reachable: {}",
            if rpc_reachable { "yes" } else { "no" }
        );
        return if rpc_reachable { 0 } else { 1 };
    }
    let _ = fs::remove_file(&pid_path);
    if rpc_reachable {
        println!("dutad: running");
        println!("stale_pid_removed: {}", pid);
        println!("pid: externally_managed_or_missing");
        println!("rpc: {}", rpc_addr);
        println!("rpc_reachable: yes");
        0
    } else {
        println!("dutad: stopped");
        println!("stale_pid_removed: {}", pid);
        println!("rpc: {}", rpc_addr);
        println!("rpc_reachable: no");
        1
    }
}

fn wait_for_daemon_rpc_ready(
    rpc_addr: &str,
    child_pid: u32,
    timeout_ms: u64,
) -> Result<(), String> {
    let deadline = std::time::Instant::now() + Duration::from_millis(timeout_ms);
    let mut last_err = String::new();
    while std::time::Instant::now() < deadline {
        if !process_exists(child_pid as i32) {
            return Err(format!("daemon_exited_early: pid={}", child_pid));
        }
        match http_health_status(rpc_addr) {
            Ok(status) if status > 0 => return Ok(()),
            Ok(_) => last_err = "health_not_reachable".to_string(),
            Err(e) => last_err = e,
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    Err(format!(
        "daemon_not_ready: rpc={} timeout_ms={} last_err={}",
        rpc_addr, timeout_ms, last_err
    ))
}

fn maybe_daemonize(args: &Args, data_dir: &str, rpc_addr: &str) {
    if !args.daemon || args.foreground {
        return;
    }

    let exe = match std::env::current_exe() {
        Ok(p) => p,
        Err(e) => {
            eprintln!("dutad: current_exe_failed: {}", e);
            std::process::exit(1);
        }
    };

    let debug_log_path = format!("{}/dutad.stdout.log", data_dir.trim_end_matches('/'));
    let error_log_path = format!("{}/dutad.stderr.log", data_dir.trim_end_matches('/'));
    let pid_path = format!("{}/dutad.pid", data_dir.trim_end_matches('/'));

    if let Err(e) = fs::create_dir_all(data_dir) {
        eprintln!("dutad: datadir_create_failed path={} err={}", data_dir, e);
        std::process::exit(1);
    }

    if let Ok(pid_s) = fs::read_to_string(&pid_path) {
        if let Ok(pid) = pid_s.trim().parse::<i32>() {
            if pid > 1 && process_exists(pid) && pid_matches_dutad_process(pid) {
                eprintln!("dutad: already running (pid={})", pid);
                std::process::exit(1);
            }
        }
        let _ = fs::remove_file(&pid_path);
    }

    let log_file = match fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&debug_log_path)
    {
        Ok(f) => f,
        Err(e) => {
            eprintln!("dutad: log_open_failed: {}", e);
            std::process::exit(1);
        }
    };
    let log_file_err = match fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&error_log_path)
    {
        Ok(f) => f,
        Err(e) => {
            eprintln!("dutad: log_open_failed: {}", e);
            std::process::exit(1);
        }
    };

    let mut child_args: Vec<std::ffi::OsString> = Vec::new();
    for a in std::env::args_os().skip(1) {
        if a == "--daemon" {
            continue;
        }
        child_args.push(a);
    }
    child_args.push(std::ffi::OsString::from("--foreground"));

    let mut cmd = std::process::Command::new(exe);
    cmd.args(child_args);
    cmd.stdin(std::process::Stdio::null());
    cmd.stdout(std::process::Stdio::from(log_file));
    cmd.stderr(std::process::Stdio::from(log_file_err));

    match cmd.spawn() {
        Ok(mut child) => {
            if let Err(e) = write_pid_file(&pid_path, child.id()) {
                eprintln!("dutad: {}", e);
                let _ = terminate_process(child.id() as i32);
                std::process::exit(1);
            }
            if let Err(e) = wait_for_daemon_rpc_ready(rpc_addr, child.id(), 5000) {
                let _ = terminate_process(child.id() as i32);
                let _ = child.wait();
                let _ = fs::remove_file(&pid_path);
                eprintln!("dutad: {}", e);
                std::process::exit(1);
            }
            println!("dutad: started (pid={})", child.id());
            std::process::exit(0);
        }
        Err(e) => {
            eprintln!("dutad: spawn_failed: {}", e);
            std::process::exit(1);
        }
    }
}

fn respond_413(request: tiny_http::Request) {
    respond_error_detail(
        request,
        tiny_http::StatusCode(413),
        "payload_too_large",
        json!({"max_body_bytes": MAX_RPC_BODY_BYTES}),
    );
}

fn respond_414(request: tiny_http::Request) {
    respond_error_detail(
        request,
        tiny_http::StatusCode(414),
        "uri_too_long",
        json!({"max_url_bytes": MAX_RPC_URL_BYTES}),
    );
}

pub(crate) fn respond_415(request: tiny_http::Request) {
    respond_error_detail(
        request,
        tiny_http::StatusCode(415),
        "unsupported_media_type",
        json!({"expected":"application/json"}),
    );
}

pub(crate) fn request_content_type_is_json(request: &tiny_http::Request) -> bool {
    for h in request.headers() {
        if h.field.equiv("Content-Type") {
            let v = h.value.as_str().to_ascii_lowercase();
            return v.contains("application/json");
        }
    }
    true
}

pub(crate) fn read_body_limited(request: &mut tiny_http::Request) -> Result<Vec<u8>, String> {
    let mut buf = Vec::new();
    // Read at most MAX_RPC_BODY_BYTES + 1 so we can detect overflow.
    let reader = request.as_reader();
    let mut limited = reader.take((MAX_RPC_BODY_BYTES + 1) as u64);
    limited
        .read_to_end(&mut buf)
        .map_err(|e| format!("read_body_failed: {}", e))?;
    if buf.len() > MAX_RPC_BODY_BYTES {
        return Err("payload_too_large".to_string());
    }
    Ok(buf)
}

fn append_static_header(
    response: tiny_http::Response<std::io::Cursor<Vec<u8>>>,
    name: &'static [u8],
    value: &'static [u8],
) -> tiny_http::Response<std::io::Cursor<Vec<u8>>> {
    match tiny_http::Header::from_bytes(name, value) {
        Ok(header) => response.with_header(header),
        Err(_) => response,
    }
}

fn respond_json(request: tiny_http::Request, status: tiny_http::StatusCode, body: String) {
    let resp = append_static_header(
        append_static_header(
            append_static_header(
                tiny_http::Response::from_string(body).with_status_code(status),
                b"Content-Type",
                b"application/json",
            ),
            b"Cache-Control",
            b"no-store",
        ),
        b"X-Content-Type-Options",
        b"nosniff",
    );
    let _ = request.respond(resp);
}

pub(crate) fn respond_error(
    request: tiny_http::Request,
    status: tiny_http::StatusCode,
    code: &str,
) {
    respond_json(
        request,
        status,
        json!({"ok":false,"error":code}).to_string(),
    );
}

pub(crate) fn respond_error_detail(
    request: tiny_http::Request,
    status: tiny_http::StatusCode,
    code: &str,
    detail: serde_json::Value,
) {
    let mut body = json!({"ok":false,"error":code});
    if let Some(obj) = body.as_object_mut() {
        if let Some(extra) = detail.as_object() {
            for (k, v) in extra {
                obj.insert(k.clone(), v.clone());
            }
        } else {
            obj.insert("detail".to_string(), detail);
        }
    }
    respond_json(request, status, body.to_string());
}

fn respond_method_not_allowed(request: tiny_http::Request) {
    respond_error(request, tiny_http::StatusCode(405), "method_not_allowed");
}

fn request_is_loopback(request: &tiny_http::Request) -> bool {
    request
        .remote_addr()
        .map(|a: &SocketAddr| a.ip().is_loopback())
        .unwrap_or(false)
}

fn admin_path_requires_loopback(path: &str) -> bool {
    matches!(
        path,
        "/rpc"
            | "/submit_tx"
            | "/rollback_to"
            | "/prune"
            | "/utxo"
            | "/wallet_utxos"
            | "/work"
            | "/submit_work"
    ) || path.starts_with("/work/")
}

pub(crate) fn now_ts() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn expected_hashes(bits: u32) -> Option<u128> {
    if bits >= 128 {
        return None;
    }
    Some(1u128 << bits)
}

fn format_hash_units(n: u128) -> String {
    // Format counts in hash-style units: H, KH, MH, GH, TH, PH, EH (base 1000).
    let suffixes = ["H", "KH", "MH", "GH", "TH", "PH", "EH"];
    if n < 1000 {
        return format!("{}{}", n, suffixes[0]);
    }
    let mut v = n as f64;
    let mut i: usize = 0;
    while v >= 1000.0 && i + 1 < suffixes.len() {
        v /= 1000.0;
        i += 1;
    }
    if v >= 100.0 {
        format!("{:.0}{}", v, suffixes[i])
    } else if v >= 10.0 {
        format!("{:.1}{}", v, suffixes[i])
    } else {
        format!("{:.2}{}", v, suffixes[i])
    }
}

fn format_hash_rate(hs: u64) -> String {
    // Format rates in hash-style units: H/s, KH/s, MH/s, GH/s, TH/s, PH/s, EH/s (base 1000).
    let suffixes = ["H/s", "KH/s", "MH/s", "GH/s", "TH/s", "PH/s", "EH/s"];
    let mut v = hs as f64;
    let mut i: usize = 0;
    while v >= 1000.0 && i + 1 < suffixes.len() {
        v /= 1000.0;
        i += 1;
    }
    if v >= 100.0 {
        format!("{:.0}{}", v, suffixes[i])
    } else if v >= 10.0 {
        format!("{:.1}{}", v, suffixes[i])
    } else {
        format!("{:.2}{}", v, suffixes[i])
    }
}

fn avg_block_time_secs_from_db(data_dir: &str, sample: usize) -> Option<u64> {
    let (tip_h, _tip_hash, _tip_cw, _tip_bits) = store::tip_fields(data_dir)?;
    if tip_h < 2 {
        return None;
    }

    // Collect up to (sample + 1) timestamps from the most recent blocks.
    let need = sample.saturating_add(1);
    let mut ts: Vec<u64> = Vec::new();
    let mut h = tip_h;
    while ts.len() < need {
        let b = store::block_at(data_dir, h)?;
        if let Some(t) = b.timestamp {
            ts.push(t);
        }
        if h == 0 {
            break;
        }
        h = h.saturating_sub(1);
        if h == 0 {
            break;
        }
    }

    if ts.len() < 2 {
        return None;
    }
    // ts is newest->older; reverse to oldest->newest for deltas
    ts.reverse();

    let mut deltas: Vec<u64> = Vec::new();
    for w in ts.windows(2) {
        if let [a, b] = *w {
            if b > a {
                deltas.push(b - a);
            }
        }
    }
    if deltas.is_empty() {
        return None;
    }
    Some(deltas.iter().sum::<u64>() / (deltas.len() as u64))
}

fn tip_json(data_dir: &str) -> String {
    let (height, hash32, chainwork, bits) = store::tip_fields(data_dir).unwrap_or((
        0,
        "0000000000000000000000000000000000000000000000000000000000000000".to_string(),
        0,
        0,
    ));

    let target_block_time_secs = duta_core::netparams::pow_target_secs(
        crate::store::read_datadir_network(data_dir).unwrap_or_else(|| {
            if data_dir.contains("testnet") {
                Network::Testnet
            } else if data_dir.contains("stagenet") {
                Network::Stagenet
            } else {
                Network::Mainnet
            }
        }),
    );

    let avg_bt = avg_block_time_secs_from_db(data_dir, 30).unwrap_or(target_block_time_secs);
    let difficulty: u128 = expected_hashes(bits as u32).unwrap_or(0);
    let nethash: u64 = if avg_bt > 0 {
        (difficulty / (avg_bt as u128)) as u64
    } else {
        0
    };

    json!({
    "height": height,
    "hash32": hash32,
    "chainwork": chainwork,
    "bits": bits,
    "difficulty": format_hash_units(difficulty),
    "nethash": format_hash_rate(nethash),
    "avg_block_time_secs": avg_bt,
    "target_block_time_secs": target_block_time_secs
    })
    .to_string()
}

fn blocks_from_json(data_dir: &str, from: usize, limit: usize) -> Option<String> {
    let mut start = from as u64;

    // Genesis (height=0) is not stored in DB; if the caller asks from=0 and DB has blocks,
    // serve from height=1.
    if start == 0
        && store::block_at(data_dir, 0).is_none()
        && store::block_at(data_dir, 1).is_some()
    {
        start = 1;
    }

    let out = store::blocks_from_range(data_dir, start, limit);
    serde_json::to_string(&out).ok()
}

fn mempool_json(data_dir: &str) -> String {
    let path = format!("{}/mempool.json", data_dir.trim_end_matches('/'));
    if let Ok(s) = fs::read_to_string(&path) {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&s) {
            if let Some(obj) = v.as_object() {
                if let Some(txids) = obj.get("txids") {
                    if txids.is_array() {
                        return v.to_string();
                    }
                }
            }
        }
    }
    json!({"txids": []}).to_string()
}

fn start_rpc_servers(
    rpc_addr: String,
    mining_addr: Option<String>,
    data_dir: String,
    net_str: String,
    rollback_enabled: bool,
) {
    let server = match tiny_http::Server::http(&rpc_addr) {
        Ok(s) => s,
        Err(e) => {
            edlog!("daemon: RPC_BIND_FAIL addr={} err={}", rpc_addr, e);
            return;
        }
    };

    dlog!("daemon: RPC_LISTEN addr=http://{}", rpc_addr);

    let mining_enabled = mining_addr.is_some();

    // Phase 3: optional public mining interface on a separate listener.
    let mining_listener = if let Some(mining_addr) = mining_addr.clone() {
        let mining_server = match tiny_http::Server::http(&mining_addr) {
            Ok(s) => s,
            Err(e) => {
                edlog!("daemon: MINING_BIND_FAIL addr={} err={}", mining_addr, e);
                return;
            }
        };
        Some((mining_addr, mining_server))
    } else {
        None
    };
    if let Some((mining_addr, mining_server)) = mining_listener {
        let data_dir2 = data_dir.clone();
        let net_str2 = net_str.clone();
        thread::spawn(move || {
            dlog!("daemon: MINING_LISTEN addr=http://{} phase=3", mining_addr);

            for request in mining_server.incoming_requests() {
                if request.url().len() > MAX_RPC_URL_BYTES {
                    respond_414(request);
                    continue;
                }

                let (path, params) = parse_query(request.url());
                if path == "/minerstats" {
                    handle_minerstats(request, &params, respond_json);
                    continue;
                }

                // Support /work/<address> in addition to /work?address=...
                if path.starts_with("/work/") || path == "/work" {
                    if request.method() != &tiny_http::Method::Get {
                        respond_method_not_allowed(request);
                        continue;
                    }
                    // Update per-wallet hashrate aggregation (best-effort)
                    {
                        let ip = remote_ip_string(&request);
                        let wallet_raw = if path.starts_with("/work/") {
                            path.trim_start_matches("/work/")
                                .trim_start_matches("/")
                                .to_string()
                        } else {
                            get_param(&params, "address").unwrap_or("").to_string()
                        };
                        if let Some(wallet) = normalize_mining_wallet(&wallet_raw) {
                            let hs = parse_reported_hashrate(&params);
                            update_miner_stats(&wallet, &ip, hs);
                        }
                    }

                    if !rate_allow(&request, "work") {
                        respond_429(request);
                        continue;
                    }
                    work::handle_work(request, &data_dir2, respond_json);
                    continue;
                }

                if path == "/submit_work" {
                    if request.method() != &tiny_http::Method::Post {
                        respond_method_not_allowed(request);
                        continue;
                    }
                    if !request_content_type_is_json(&request) {
                        respond_415(request);
                        continue;
                    }
                    if !rate_allow(&request, "submit") {
                        respond_429(request);
                        continue;
                    }
                    submit_work::handle_submit_work(request, &data_dir2, respond_json);
                    continue;
                }

                if path == "/getmininginfo" {
                    if request.method() != &tiny_http::Method::Get {
                        respond_method_not_allowed(request);
                        continue;
                    }
                    if !rate_allow(&request, "query") {
                        respond_429(request);
                        continue;
                    }
                    btc_rpc::handle_getmininginfo(request, &data_dir2, &net_str2, respond_json);
                    continue;
                }

                // Everything else is NOT available on the public mining listener.
                respond_error(request, tiny_http::StatusCode(404), "not_found");
            }
        });
    }

    for mut request in server.incoming_requests() {
        if request.url().len() > MAX_RPC_URL_BYTES {
            respond_414(request);
            continue;
        }

        let (path, params) = parse_query(request.url());

        if admin_path_requires_loopback(&path) && !request_is_loopback(&request) {
            respond_error(request, tiny_http::StatusCode(404), "not_found");
            continue;
        }

        // Support /work/<address> in addition to /work?address=... (query can be stripped by some clients).
        if path.starts_with("/work/") {
            if mining_enabled {
                respond_error(request, tiny_http::StatusCode(404), "not_found");
                continue;
            }
            if !request_is_loopback(&request) {
                respond_error(request, tiny_http::StatusCode(404), "not_found");
                continue;
            }
            if !rate_allow(&request, "work") {
                respond_429(request);
                continue;
            }
            work::handle_work(request, &data_dir, respond_json);
            continue;
        }

        match path.as_str() {
            "/health" => {
                if request.method() != &tiny_http::Method::Get {
                    respond_method_not_allowed(request);
                } else {
                    if !rate_allow(&request, "query") {
                        respond_429(request);
                        continue;
                    }
                    let (tip_height, _tip_hash, _tip_cw, _tip_bits) =
                        store::tip_fields(&data_dir).unwrap_or((0, "0".repeat(64), 0, 0));
                    let best_seen_height = p2p::best_seen_height().max(tip_height);
                    let has_healthy_peer = p2p::bootstrap_has_healthy_peer();
                    let (is_ready, health_status, sync_progress) =
                        daemon_health_state(tip_height, best_seen_height, has_healthy_peer);
                    let status = if is_ready {
                        tiny_http::StatusCode(200)
                    } else {
                        tiny_http::StatusCode(503)
                    };
                    respond_json(
                        request,
                        status,
                        json!({
                            "ok": is_ready,
                            "status": health_status,
                            "net": net_str.as_str(),
                            "version": env!("CARGO_PKG_VERSION"),
                            "tip_height": tip_height,
                            "sync_progress": sync_progress,
                            "peer_ready": has_healthy_peer
                        })
                        .to_string(),
                    );
                }
            }

            "/rpc" => {
                if request.method() != &tiny_http::Method::Post {
                    respond_method_not_allowed(request);
                    continue;
                }
                if !request_content_type_is_json(&request) {
                    respond_415(request);
                    continue;
                }
                if !rate_allow(&request, "rpc") {
                    respond_429(request);
                    continue;
                }

                let body_bytes = match read_body_limited(&mut request) {
                    Ok(b) => b,
                    Err(e) if e == "payload_too_large" => {
                        respond_413(request);
                        continue;
                    }
                    Err(e) => {
                        respond_error_detail(
                            request,
                            tiny_http::StatusCode(400),
                            "bad_request",
                            json!({"detail":e}),
                        );
                        continue;
                    }
                };

                match rpc_json::handle_rpc(&body_bytes, &data_dir, &net_str) {
                    Ok(resp) => respond_json(request, tiny_http::StatusCode(200), resp),
                    Err(e) => respond_error_detail(
                        request,
                        tiny_http::StatusCode(400),
                        "bad_request",
                        json!({"detail":e}),
                    ),
                }
            }

            "/tip" => {
                if request.method() != &tiny_http::Method::Get {
                    respond_method_not_allowed(request);
                } else {
                    if !rate_allow(&request, "query") {
                        respond_429(request);
                        continue;
                    }
                    respond_json(request, tiny_http::StatusCode(200), tip_json(&data_dir));
                }
            }
            p if p == "/blocks_from" || p.starts_with("/blocks_from/") => {
                if request.method() != &tiny_http::Method::Get {
                    respond_method_not_allowed(request);
                    continue;
                }
                if !rate_allow(&request, "query") {
                    respond_429(request);
                    continue;
                }

                // Query params (preferred): /blocks_from?from=77&limit=3 or /blocks_from?start=77&limit=3
                let from_str = get_param(&params, "from").or_else(|| get_param(&params, "start"));
                let limit_str = get_param(&params, "limit");

                // Path params fallback (in case the HTTP server strips query): /blocks_from/77/3
                let (from, limit) = if from_str.is_none() && limit_str.is_none() {
                    let parts: Vec<&str> = p.split('/').filter(|s| !s.is_empty()).collect();
                    // parts: ["blocks_from", "<from>", "<limit?>"]
                    if parts.len() < 2 {
                        respond_error(request, tiny_http::StatusCode(400), "bad_request");
                        continue;
                    }
                    let from = parts[1].parse::<usize>().unwrap_or(0);
                    let limit = if parts.len() >= 3 {
                        parts[2].parse::<usize>().unwrap_or(4096)
                    } else {
                        4096
                    };
                    (from, limit)
                } else {
                    let from = from_str
                        .and_then(|s: &str| s.parse::<usize>().ok())
                        .unwrap_or(0);
                    let limit = limit_str
                        .and_then(|s: &str| s.parse::<usize>().ok())
                        .unwrap_or(4096);
                    (from, limit)
                };

                if limit > BLOCKS_FROM_MAX_LIMIT {
                    respond_error_detail(
                        request,
                        tiny_http::StatusCode(400),
                        "limit_too_large",
                        json!({"max_limit":BLOCKS_FROM_MAX_LIMIT}),
                    );
                    continue;
                }

                let prune_below = store::prune_below(&data_dir);
                if prune_below > 0 && (from as u64) < prune_below {
                    respond_error_detail(
                        request,
                        tiny_http::StatusCode(410),
                        "pruned",
                        json!({"prune_below":prune_below}),
                    );
                    continue;
                }

                match blocks_from_json(&data_dir, from, limit) {
                    Some(body) => respond_json(request, tiny_http::StatusCode(200), body),
                    None => respond_error(request, tiny_http::StatusCode(500), "chain_unavailable"),
                }
            }
            "/mempool" => {
                if request.method() != &tiny_http::Method::Get {
                    respond_method_not_allowed(request);
                } else {
                    if !rate_allow(&request, "query") {
                        respond_429(request);
                        continue;
                    }
                    respond_json(request, tiny_http::StatusCode(200), mempool_json(&data_dir));
                }
            }

            "/info" => {
                if request.method() != &tiny_http::Method::Get {
                    respond_method_not_allowed(request);
                } else {
                    if !rate_allow(&request, "query") {
                        respond_429(request);
                        continue;
                    }
                    respond_json(
                        request,
                        tiny_http::StatusCode(200),
                        info::info_json(&data_dir, net_str.as_str(), tip_json, mempool_json),
                    );
                }
            }

            "/getblockcount" => {
                if request.method() != &tiny_http::Method::Get {
                    respond_method_not_allowed(request);
                } else {
                    if !rate_allow(&request, "query") {
                        respond_429(request);
                        continue;
                    }
                    btc_rpc::handle_getblockcount(request, &data_dir, respond_json);
                }
            }

            "/getbestblockhash" => {
                if request.method() != &tiny_http::Method::Get {
                    respond_method_not_allowed(request);
                } else {
                    if !rate_allow(&request, "query") {
                        respond_429(request);
                        continue;
                    }
                    btc_rpc::handle_getbestblockhash(request, &data_dir, respond_json);
                }
            }

            p if p == "/getblockhash" || p.starts_with("/getblockhash/") => {
                if request.method() != &tiny_http::Method::Get {
                    respond_method_not_allowed(request);
                } else {
                    if !rate_allow(&request, "query") {
                        respond_429(request);
                        continue;
                    }
                    btc_rpc::handle_getblockhash(request, &data_dir, p, &params, respond_json);
                }
            }

            p if p == "/getblock" || p.starts_with("/getblock/") => {
                if request.method() != &tiny_http::Method::Get {
                    respond_method_not_allowed(request);
                } else {
                    if !rate_allow(&request, "query") {
                        respond_429(request);
                        continue;
                    }
                    btc_rpc::handle_getblock(request, &data_dir, p, &params, respond_json);
                }
            }

            "/getmininginfo" => {
                if request.method() != &tiny_http::Method::Get {
                    respond_method_not_allowed(request);
                } else {
                    if !rate_allow(&request, "query") {
                        respond_429(request);
                        continue;
                    }
                    btc_rpc::handle_getmininginfo(request, &data_dir, &net_str, respond_json);
                }
            }

            "/getinfo" => {
                if request.method() != &tiny_http::Method::Get {
                    respond_method_not_allowed(request);
                } else {
                    if !rate_allow(&request, "query") {
                        respond_429(request);
                        continue;
                    }
                    btc_rpc::handle_getinfo(request, &data_dir, &net_str, respond_json);
                }
            }
            "/utxo" => {
                if !request_is_loopback(&request) {
                    respond_error(request, tiny_http::StatusCode(404), "not_found");
                    continue;
                }
                if !rate_allow(&request, "query") {
                    respond_429(request);
                    continue;
                }
                utxo_rpc::handle_utxo(request, &data_dir, respond_json);
            }
            "/wallet_utxos" => {
                if !request_is_loopback(&request) {
                    respond_error(request, tiny_http::StatusCode(404), "not_found");
                    continue;
                }
                if request.method() != &tiny_http::Method::Post {
                    respond_method_not_allowed(request);
                    continue;
                }
                if !rate_allow(&request, "query") {
                    respond_429(request);
                    continue;
                }
                utxo_rpc::handle_wallet_utxos(request, &data_dir, respond_json);
            }
            "/submit_tx" => {
                if !request_is_loopback(&request) {
                    respond_error(request, tiny_http::StatusCode(404), "not_found");
                    continue;
                }
                if !rate_allow(&request, "rpc") {
                    respond_429(request);
                    continue;
                }
                submit_tx::handle_submit_tx(request, &data_dir, respond_json);
            }

            "/rollback_to" => {
                if !rollback_enabled || !request_is_loopback(&request) {
                    respond_error(request, tiny_http::StatusCode(404), "not_found");
                    continue;
                }
                if request.method() != &tiny_http::Method::Post {
                    respond_method_not_allowed(request);
                    continue;
                }
                if !request_content_type_is_json(&request) {
                    respond_415(request);
                    continue;
                }

                let body_bytes = match read_body_limited(&mut request) {
                    Ok(b) => b,
                    Err(e) if e == "payload_too_large" => {
                        respond_413(request);
                        continue;
                    }
                    Err(e) => {
                        respond_error_detail(
                            request,
                            tiny_http::StatusCode(400),
                            "bad_request",
                            json!({"detail":e}),
                        );
                        continue;
                    }
                };

                let v: serde_json::Value = match serde_json::from_slice(&body_bytes) {
                    Ok(v) => v,
                    Err(_) => {
                        respond_error_detail(
                            request,
                            tiny_http::StatusCode(400),
                            "bad_request",
                            json!({"detail":"invalid_json"}),
                        );
                        continue;
                    }
                };
                let target_h = v.get("height").and_then(|x| x.as_u64()).unwrap_or(0);

                match store::rollback_to_height(&data_dir, target_h) {
                    Ok(()) => {
                        let tip = serde_json::from_str::<serde_json::Value>(&tip_json(&data_dir))
                            .unwrap_or(json!({}));
                        dlog!(
                            "daemon: ROLLBACK_OK target_height={} new_tip={}",
                            target_h,
                            tip
                        );
                        respond_json(
                            request,
                            tiny_http::StatusCode(200),
                            json!({"ok":true,"tip":tip}).to_string(),
                        )
                    }
                    Err(e) => {
                        edlog!("daemon: ROLLBACK_FAIL target_height={} err={}", target_h, e);
                        respond_error_detail(
                            request,
                            tiny_http::StatusCode(500),
                            "rollback_failed",
                            json!({"detail":e}),
                        )
                    }
                }
            }

            "/prune" => {
                if !request_is_loopback(&request) {
                    respond_error(request, tiny_http::StatusCode(404), "not_found");
                    continue;
                }
                if request.method() != &tiny_http::Method::Post {
                    respond_method_not_allowed(request);
                    continue;
                }
                if !request_content_type_is_json(&request) {
                    respond_415(request);
                    continue;
                }

                let body_bytes = match read_body_limited(&mut request) {
                    Ok(b) => b,
                    Err(e) if e == "payload_too_large" => {
                        respond_413(request);
                        continue;
                    }
                    Err(e) => {
                        respond_error_detail(
                            request,
                            tiny_http::StatusCode(400),
                            "bad_request",
                            json!({"detail":e}),
                        );
                        continue;
                    }
                };

                let v: serde_json::Value = match serde_json::from_slice(&body_bytes) {
                    Ok(v) => v,
                    Err(_) => {
                        respond_error_detail(
                            request,
                            tiny_http::StatusCode(400),
                            "bad_request",
                            json!({"detail":"invalid_json"}),
                        );
                        continue;
                    }
                };

                let keep_last = v.get("keep_last").and_then(|x| x.as_u64()).unwrap_or(2000);

                match store::prune(&data_dir, keep_last) {
                    Ok(()) => {
                        let prune_below = store::prune_below(&data_dir);
                        dlog!(
                            "daemon: PRUNE_OK keep_last={} prune_below={}",
                            keep_last,
                            prune_below
                        );
                        respond_json(
                            request,
                            tiny_http::StatusCode(200),
                            json!({"ok":true,"keep_last":keep_last,"prune_below": prune_below})
                                .to_string(),
                        )
                    }
                    Err(e) => {
                        edlog!("daemon: PRUNE_FAIL keep_last={} err={}", keep_last, e);
                        respond_error_detail(
                            request,
                            tiny_http::StatusCode(500),
                            "prune_failed",
                            json!({"detail":e}),
                        )
                    }
                }
            }

            "/work" => {
                if mining_enabled {
                    respond_error(request, tiny_http::StatusCode(404), "not_found");
                    continue;
                }
                if !rate_allow(&request, "work") {
                    respond_429(request);
                    continue;
                }
                work::handle_work(request, &data_dir, respond_json);
            }

            "/submit_work" => {
                if mining_enabled {
                    respond_error(request, tiny_http::StatusCode(404), "not_found");
                    continue;
                }
                if !rate_allow(&request, "submit") {
                    respond_429(request);
                    continue;
                }
                submit_work::handle_submit_work(request, &data_dir, respond_json);
            }
            _ => respond_error(request, tiny_http::StatusCode(404), "not_found"),
        }
    }
}

fn main() {
    std::panic::set_hook(Box::new(|info| {
        edlog!("daemon: PANIC {}", info);
    }));

    let args = Args::parse();
    let stop_requested = args.stop || matches!(args.command, Some(Cmd::Stop));
    let cli_net = if args.stagenet {
        Some(Network::Stagenet)
    } else if args.testnet {
        Some(Network::Testnet)
    } else {
        None
    };

    // Resolve data dir early so we can load duta.conf, then resolve network again
    // using config/datadir hints when CLI flags are absent.
    let mut net = cli_net.unwrap_or(Network::Mainnet);
    let mut data_dir = net.default_data_dir_unix().to_string();
    data_dir = normalize_path_maybe_home(&data_dir);
    if let Some(dd) = args.datadir.as_deref() {
        let dd2 = normalize_path_maybe_home(dd);
        if !dd2.is_empty() {
            data_dir = dd2;
        }
    }

    // Optional config file (bitcoin.conf style): default $DATADIR/duta.conf, or CLI --conf
    let mut conf = duta_core::netparams::Conf::default();
    let mut conf_path = format!("{}/duta.conf", data_dir.trim_end_matches('/'));
    if let Some(cp) = args.conf.as_deref() {
        let cp2 = normalize_path_maybe_home(cp);
        if !cp2.is_empty() {
            conf_path = cp2;
        }
    }
    match load_runtime_conf(&conf_path, args.conf.is_some()) {
        Ok(parsed) => conf = parsed,
        Err(e) => {
            eprintln!("dutad: {}", e);
            std::process::exit(1);
        }
    }
    if let Err(e) = validate_conf_network_name(&conf) {
        eprintln!("dutad: {}", e);
        std::process::exit(1);
    }
    if let Err(e) = validate_conf_admin_rpc_bind(&conf) {
        eprintln!("dutad: {}", e);
        std::process::exit(1);
    }
    if let Err(e) = validate_conf_listener_binds(net, &conf) {
        eprintln!("dutad: {}", e);
        std::process::exit(1);
    }

    if cli_net.is_none() {
        if let Some(conf_net) = conf
            .get_last("network")
            .or_else(|| conf.get_last("chain"))
            .or_else(|| {
                if matches!(
                    conf.get_last("stagenet").as_deref(),
                    Some("1" | "true" | "yes" | "on")
                ) {
                    Some("stagenet".to_string())
                } else if matches!(
                    conf.get_last("testnet").as_deref(),
                    Some("1" | "true" | "yes" | "on")
                ) {
                    Some("testnet".to_string())
                } else {
                    None
                }
            })
            .and_then(|v| Network::parse_name(&v))
            .or_else(|| {
                let d = data_dir.replace('\\', "/").to_ascii_lowercase();
                if d.contains("/stagenet") {
                    Some(Network::Stagenet)
                } else if d.contains("/testnet") {
                    Some(Network::Testnet)
                } else {
                    None
                }
            })
        {
            if conf_net != net {
                net = conf_net;
                if args.datadir.is_none() {
                    data_dir = normalize_path_maybe_home(net.default_data_dir_unix());
                }
                if args.conf.is_none() {
                    conf_path = format!("{}/duta.conf", data_dir.trim_end_matches('/'));
                    match load_runtime_conf(&conf_path, args.conf.is_some()) {
                        Ok(parsed) => conf = parsed,
                        Err(e) => {
                            eprintln!("dutad: {}", e);
                            std::process::exit(1);
                        }
                    }
                    if let Err(e) = validate_conf_network_name(&conf) {
                        eprintln!("dutad: {}", e);
                        std::process::exit(1);
                    }
                    if let Err(e) = validate_conf_admin_rpc_bind(&conf) {
                        eprintln!("dutad: {}", e);
                        std::process::exit(1);
                    }
                    if let Err(e) = validate_conf_listener_binds(net, &conf) {
                        eprintln!("dutad: {}", e);
                        std::process::exit(1);
                    }
                    if let Some(dd) = conf.get_last("datadir") {
                        let dd2 = normalize_path_maybe_home(&dd);
                        if !dd2.is_empty() {
                            data_dir = dd2;
                        }
                    }
                }
            }
        }
    }
    let net_str = net.as_str().to_string();

    // datadir= override (optional). If present, re-load config from that dir.
    // NOTE: CLI --datadir always wins.
    if args.datadir.is_none() {
        if let Some(dd) = conf.get_last("datadir") {
            let dd2 = normalize_path_maybe_home(&dd);
            if !dd2.is_empty() && dd2 != data_dir {
                data_dir = dd2;
                if args.conf.is_none() {
                    conf_path = format!("{}/duta.conf", data_dir.trim_end_matches('/'));
                }
                if let Ok(s) = fs::read_to_string(&conf_path) {
                    conf = duta_core::netparams::Conf::parse(&s);
                }
                if let Err(e) = validate_conf_network_name(&conf) {
                    eprintln!("dutad: {}", e);
                    std::process::exit(1);
                }
                if let Err(e) = validate_conf_admin_rpc_bind(&conf) {
                    eprintln!("dutad: {}", e);
                    std::process::exit(1);
                }
                if let Err(e) = validate_conf_listener_binds(net, &conf) {
                    eprintln!("dutad: {}", e);
                    std::process::exit(1);
                }
            }
        }
    }

    if stop_requested {
        if args.daemon {
            eprintln!("dutad: --stop cannot be combined with --daemon");
            std::process::exit(1);
        }
        let rc = stop_daemon(&data_dir);
        std::process::exit(rc);
    }

    // Bootstrap peers precedence: CLI --seeds > env DUTA_P2P_SEEDS > config (seeds/addnode) > built-in cluster.
    let mut seeds: Vec<String> = args
        .seeds
        .iter()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    seeds.extend(
        args.addnode
            .iter()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty()),
    );
    if seeds.is_empty() {
        if let Ok(s) = std::env::var("DUTA_P2P_SEEDS") {
            seeds = s
                .split(|c: char| c == ',' || c.is_whitespace())
                .map(|x| x.trim().to_string())
                .filter(|x| !x.is_empty())
                .collect();
        }
    }
    if seeds.is_empty() {
        let mut cfg_seeds: Vec<String> = Vec::new();
        for k in ["seeds", "seed", "addnode"].iter() {
            for v in conf.get_all(k) {
                cfg_seeds.extend(
                    v.split(|c: char| c == ',' || c.is_whitespace())
                        .map(|x| x.trim().to_string())
                        .filter(|x| !x.is_empty()),
                );
            }
        }
        if !cfg_seeds.is_empty() {
            seeds = cfg_seeds;
        }
    }

    // P2P bind (locked port). Config may override IP only.
    let mut p2p_bind_ip = net.p2p_bind_default().to_string();
    if let Some(b) = args.bind.as_deref() {
        if !b.trim().is_empty() {
            p2p_bind_ip = b.trim().to_string();
        }
    }
    if let Some(b) = conf.get_last("bind") {
        // Accept either "IP" or "IP:PORT" but ignore if port mismatches.
        if let Some((ip, port)) = b.split_once(':') {
            if port.trim() == net.default_p2p_port().to_string() {
                p2p_bind_ip = ip.trim().to_string();
            }
        } else if !b.trim().is_empty() {
            p2p_bind_ip = b.trim().to_string();
        }
    }
    let p2p_addr = format!("{}:{}", p2p_bind_ip, net.default_p2p_port());

    // Admin RPC bind/port (policy: localhost-only; locked port).
    // Invalid operator input must be explicit here. Silent fallback makes it look like
    // the requested bind succeeded when it did not.
    let mut rpc_bind = "127.0.0.1".to_string();
    if let Some(b) = conf
        .get_last("rpcconnect")
        .or_else(|| conf.get_last("rpcbind"))
    {
        if admin_rpc_bind_is_allowed(&b) {
            rpc_bind = "127.0.0.1".to_string();
        } else if !b.trim().is_empty() {
            eprintln!(
                "daemon: RPC_BIND_POLICY_IGNORE requested={} allowed=127.0.0.1 reason=admin_rpc_must_stay_loopback",
                b.trim()
            );
        }
    }
    if let Some(b) = args.rpc_bind.as_deref() {
        if admin_rpc_bind_is_allowed(b) {
            rpc_bind = "127.0.0.1".to_string();
        } else {
            eprintln!(
                "daemon: RPC_BIND_POLICY_REJECT requested={} allowed=127.0.0.1 reason=admin_rpc_must_stay_loopback",
                b.trim()
            );
            std::process::exit(2);
        }
    }
    let rpc_addr = format!("{}:{}", rpc_bind, net.default_daemon_rpc_port());

    // Public mining bind (locked port). CLI overrides config.
    let mut mining_addr = args.mining_bind.clone();
    if mining_addr.is_none() {
        if let Some(b) = conf
            .get_last("mining_bind")
            .or_else(|| conf.get_last("miningbind"))
            .or_else(|| conf.get_last("miningaddr"))
        {
            let want_port = net.default_p2p_port().saturating_add(3);
            if let Some((ip, port)) = b.split_once(':') {
                if port.trim() == want_port.to_string() {
                    mining_addr = Some(format!("{}:{}", ip.trim(), want_port));
                }
            } else if let Some(port_s) = conf.get_last("miningport") {
                if port_s.trim() == want_port.to_string() {
                    mining_addr = Some(format!("{}:{}", b.trim(), want_port));
                }
            }
        }
    }
    if matches!(args.command, Some(Cmd::Status)) {
        if args.daemon {
            eprintln!("dutad: status cannot be combined with --daemon");
            std::process::exit(1);
        }
        let rc = daemon_status(&data_dir, &rpc_addr);
        std::process::exit(rc);
    }
    maybe_daemonize(&args, &data_dir, &rpc_addr);

    // Ensure data dir has an explicit schema/network marker before touching DB.
    if let Err(e) = store::ensure_datadir_meta(&data_dir, &net_str) {
        eprintln!(
            "daemon: DATADIR_META_FAIL data={} net={} err={}",
            data_dir, net_str, e
        );
        std::process::exit(1);
    }
    // Initialize stdout/stderr log files in datadir.
    if let Err(e) = debuglog::init(&data_dir) {
        eprintln!("daemon: LOG_INIT_FAIL data={} err={}", data_dir, e);
    }
    rpc_json::mark_process_start();

    // DB bootstrap must fail closed for release builds. If legacy import or DB init
    // returns an error, continuing with listeners up would mislead operators about
    // chain readiness and can mask on-disk corruption or invalid bootstrap state.
    if let Err(e) = store::bootstrap(&data_dir) {
        edlog!("daemon: BOOTSTRAP_FAIL data={} err={}", data_dir, e);
        eprintln!("daemon: BOOTSTRAP_FAIL data={} err={}", data_dir, e);
        std::process::exit(1);
    }

    let mining_log = mining_addr.as_deref().unwrap_or("-");
    print_startup_banner(net, &data_dir, &p2p_addr, &rpc_addr, mining_addr.as_deref());
    println!("Step       : data directory ready");
    println!("Step       : chain bootstrap completed");
    print_chain_status(&data_dir);
    println!("Step       : starting RPC and P2P listeners");
    dlog!(
        "daemon: START net={} data={} p2p={} rpc={} mining={} stdout_log={}/dutad.stdout.log stderr_log={}/dutad.stderr.log",
        net.as_str(),
        data_dir,
        p2p_addr,
        rpc_addr,
        mining_log,
        data_dir,
        data_dir
    );
    dlog!("daemon: POW mode=CPUCOIN_POW_V3");

    // Start RPC server.
    let data_for_rpc = data_dir.clone();
    let rpc_addr_for_thread = rpc_addr.clone();
    let mining_addr_for_thread = mining_addr.clone();
    let net_for_rpc = net_str.clone();
    let rollback_enabled = conf
        .get_last("enable_rollback")
        .or_else(|| conf.get_last("unsaferpcrollback"))
        .map(|v| matches!(v.trim(), "1" | "true" | "yes" | "on"))
        .unwrap_or(false)
        || std::env::var("DUTA_ENABLE_ROLLBACK")
            .map(|v| matches!(v.trim(), "1" | "true" | "yes" | "on"))
            .unwrap_or(false);
    let rpc_thread = thread::spawn(move || {
        start_rpc_servers(
            rpc_addr_for_thread,
            mining_addr_for_thread,
            data_for_rpc,
            net_for_rpc,
            rollback_enabled,
        )
    });

    // Start P2P (pull-sync + tx broadcast).
    let p2p_addr_for_thread = p2p_addr.clone();
    let data_for_p2p = data_dir.clone();
    let net_for_p2p = net_str.clone();
    let p2p_thread = thread::spawn(move || {
        p2p::start_p2p(p2p_addr_for_thread, data_for_p2p, net_for_p2p, seeds)
    });
    print_startup_guidance(&rpc_addr);

    let running = Arc::new(AtomicBool::new(true));
    {
        let running = Arc::clone(&running);
        let _ = ctrlc::set_handler(move || {
            running.store(false, Ordering::SeqCst);
        });
    }

    let data_for_status = data_dir.clone();
    let status_running = Arc::clone(&running);
    thread::spawn(move || {
        while status_running.load(Ordering::SeqCst) {
            thread::sleep(Duration::from_secs(30));
            if !status_running.load(Ordering::SeqCst) {
                break;
            }
            print_runtime_status_tick(&data_for_status);
        }
    });

    // Keep process alive, but fail closed if a critical service thread exits.
    while running.load(Ordering::SeqCst) {
        thread::sleep(Duration::from_secs(1));
        if rpc_thread.is_finished() {
            let outcome = rpc_thread.join();
            match outcome {
                Ok(()) => edlog!("daemon: rpc thread exited unexpectedly"),
                Err(_) => edlog!("daemon: rpc thread panicked"),
            }
            std::process::exit(1);
        }
        if p2p_thread.is_finished() {
            let outcome = p2p_thread.join();
            match outcome {
                Ok(()) => edlog!("daemon: p2p thread exited unexpectedly"),
                Err(_) => edlog!("daemon: p2p thread panicked"),
            }
            std::process::exit(1);
        }
    }

    console_line(
        "STATUS",
        ANSI_YELLOW,
        "shutdown signal received, flushing database and stopping",
    );
    if let Err(e) = store::flush_db(&data_dir) {
        edlog!("daemon: SHUTDOWN_FLUSH_FAIL data={} err={}", data_dir, e);
        eprintln!("daemon: SHUTDOWN_FLUSH_FAIL data={} err={}", data_dir, e);
        std::process::exit(1);
    }
    edlog!("daemon: SHUTDOWN_OK data={}", data_dir);
    println!("dutad: shutdown complete");
}

#[cfg(test)]
mod tests {
    use super::{
        admin_path_requires_loopback, blocks_from_json, ip_is_loopback_or_private,
        daemon_health_state, load_runtime_conf,
        per_ip_rate_limit, Args, ChainBlock, SUBMIT_MAX_PER_IP_PER_SEC,
        WORK_MAX_PER_PRIVATE_IP_PER_SEC,
    };
    use crate::store;
    use clap::Parser;
    use duta_core::Network;
    use serde_json::json;
    use std::fs;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    #[test]
    fn admin_paths_stay_loopback_only() {
        for path in [
            "/rpc",
            "/submit_tx",
            "/rollback_to",
            "/prune",
            "/utxo",
            "/wallet_utxos",
            "/work",
            "/submit_work",
            "/work/duta1example",
        ] {
            assert!(
                admin_path_requires_loopback(path),
                "path should stay private: {path}"
            );
        }
    }

    #[test]
    fn public_query_paths_are_not_marked_private() {
        for path in ["/", "/tip", "/getinfo", "/minerstats", "/getblockhash/7"] {
            assert!(
                !admin_path_requires_loopback(path),
                "path should stay public/query-only: {path}"
            );
        }
    }

    #[test]
    fn daemon_health_requires_healthy_peer_before_ready() {
        let (ok, status, sync) = daemon_health_state(0, 0, false);
        assert!(!ok);
        assert_eq!(status, "no_peers");
        assert_eq!(sync, 1.0);
    }

    #[test]
    fn daemon_health_reports_ready_once_peer_is_healthy_and_synced() {
        let (ok, status, sync) = daemon_health_state(100, 100, true);
        assert!(ok);
        assert_eq!(status, "ready");
        assert_eq!(sync, 1.0);
    }

    #[test]
    fn daemon_health_reports_syncing_when_peer_is_ahead() {
        let (ok, status, sync) = daemon_health_state(80, 100, true);
        assert!(!ok);
        assert_eq!(status, "syncing");
        assert!(sync < 1.0);
    }

    #[test]
    fn daemon_health_stays_syncing_when_only_slightly_behind_best_seen_tip() {
        let (ok, status, sync) = daemon_health_state(268, 270, true);
        assert!(!ok);
        assert_eq!(status, "syncing");
        assert!(sync < 1.0);
    }

    #[test]
    fn private_and_loopback_ips_get_private_work_burst_policy() {
        assert!(ip_is_loopback_or_private(IpAddr::V4(Ipv4Addr::LOCALHOST)));
        assert!(ip_is_loopback_or_private(IpAddr::V4(Ipv4Addr::new(
            172, 16, 20, 22
        ))));
        assert!(ip_is_loopback_or_private(IpAddr::V6(Ipv6Addr::LOCALHOST)));
        assert!(ip_is_loopback_or_private(IpAddr::V6(
            "fd00::22".parse().unwrap()
        )));
        assert!(!ip_is_loopback_or_private(IpAddr::V4(Ipv4Addr::new(
            8, 8, 8, 8
        ))));
    }

    #[test]
    fn private_work_burst_does_not_relax_submit_limit() {
        assert_eq!(
            per_ip_rate_limit("work", true),
            Some(WORK_MAX_PER_PRIVATE_IP_PER_SEC)
        );
        assert_eq!(
            per_ip_rate_limit("submit", true),
            Some(SUBMIT_MAX_PER_IP_PER_SEC)
        );
        assert_eq!(
            per_ip_rate_limit("submit", false),
            Some(SUBMIT_MAX_PER_IP_PER_SEC)
        );
    }

    fn temp_datadir(tag: &str) -> String {
        let mut p = std::env::temp_dir();
        let uniq = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        p.push(format!("duta-main-test-{}-{}", tag, uniq));
        std::fs::create_dir_all(&p).unwrap();
        p.to_string_lossy().to_string()
    }

    #[test]
    fn blocks_from_json_ignores_legacy_chain_file_without_db_blocks() {
        let data_dir = temp_datadir("blocks-from");
        let fake_chain = vec![ChainBlock {
            height: 7,
            hash32: "77".repeat(32),
            bits: 1,
            chainwork: 14,
            timestamp: Some(7),
            prevhash32: Some("66".repeat(32)),
            merkle32: Some("88".repeat(32)),
            nonce: Some(0),
            miner: Some("miner".to_string()),
            pow_digest32: Some("99".repeat(32)),
            txs: Some(json!({})),
        }];
        let body = serde_json::to_string(&fake_chain).unwrap();
        std::fs::write(format!("{}/chain.json", data_dir), body).unwrap();

        let out = blocks_from_json(&data_dir, 0, 10).expect("json body");
        assert_eq!(out, "[]");
    }

    #[test]
    fn blocks_from_json_returns_empty_after_db_bootstrap_without_blocks() {
        let data_dir = temp_datadir("blocks-from-bootstrap");
        store::ensure_datadir_meta(&data_dir, "mainnet").unwrap();
        store::bootstrap(&data_dir).unwrap();

        let out = blocks_from_json(&data_dir, 0, 10).expect("json body");
        assert_eq!(out, "[]");
    }

    #[test]
    fn stop_subcommand_is_parsed_for_operator_ux() {
        let args = Args::parse_from(["dutad", "stop"]);
        assert!(matches!(args.command, Some(super::Cmd::Stop)));
        assert!(!args.stop);
    }

    #[test]
    fn status_subcommand_is_parsed_for_operator_ux() {
        let args = Args::parse_from(["dutad", "status"]);
        assert!(matches!(args.command, Some(super::Cmd::Status)));
        assert!(!args.stop);
    }

    #[test]
    fn daemon_status_treats_reachable_rpc_without_pid_file_as_running() {
        use std::io::{Read, Write};
        use std::net::TcpListener;
        use std::thread;

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let _ = stream.set_read_timeout(Some(std::time::Duration::from_secs(1)));
                let mut buf = [0u8; 256];
                let _ = stream.read(&mut buf);
                let _ = stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n");
                let _ = stream.flush();
            }
        });

        let data_dir = temp_datadir("daemon-status-rpc-reachable");
        let rc = super::daemon_status(&data_dir, &addr.to_string());
        assert_eq!(rc, 0);

        let _ = handle.join();
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[test]
    fn daemon_status_treats_503_health_without_pid_file_as_running() {
        use std::io::{Read, Write};
        use std::net::TcpListener;
        use std::thread;

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let _ = stream.set_read_timeout(Some(std::time::Duration::from_secs(1)));
                let mut buf = [0u8; 256];
                let _ = stream.read(&mut buf);
                let _ = stream.write_all(
                    b"HTTP/1.1 503 Service Unavailable\r\nContent-Type: application/json\r\nContent-Length: 33\r\n\r\n{\"ok\":false,\"status\":\"no_peers\"}",
                );
                let _ = stream.flush();
            }
        });

        let data_dir = temp_datadir("daemon-status-rpc-503");
        let rc = super::daemon_status(&data_dir, &addr.to_string());
        assert_eq!(rc, 0);

        let _ = handle.join();
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[cfg(unix)]
    #[test]
    fn pid_matches_dutad_process_rejects_foreign_live_pid() {
        let mut child = std::process::Command::new("sleep")
            .arg("5")
            .spawn()
            .expect("spawn sleep");
        let pid = child.id() as i32;
        assert!(super::process_exists(pid));
        assert!(!super::pid_matches_dutad_process(pid));
        let _ = child.kill();
        let _ = child.wait();
    }

    #[test]
    fn runtime_config_rejects_unknown_network_name() {
        let conf = duta_core::netparams::Conf::parse("network=bogus\n");
        assert_eq!(
            super::validate_conf_network_name(&conf).unwrap_err(),
            "invalid_network_name: bogus"
        );
        let ok = duta_core::netparams::Conf::parse("network=testnet\n");
        assert!(super::validate_conf_network_name(&ok).is_ok());
    }

    #[test]
    fn runtime_config_rejects_admin_rpc_bind_with_port() {
        let conf = duta_core::netparams::Conf::parse("rpcbind=127.0.0.1:notaport\n");
        assert_eq!(
            super::validate_conf_admin_rpc_bind(&conf).unwrap_err(),
            "invalid_admin_rpc_bind: 127.0.0.1:notaport"
        );
        let ok = duta_core::netparams::Conf::parse("rpcbind=127.0.0.1\n");
        assert!(super::validate_conf_admin_rpc_bind(&ok).is_ok());
    }

    #[test]
    fn runtime_config_rejects_invalid_p2p_and_mining_bind_ports() {
        let bad_p2p = duta_core::netparams::Conf::parse("bind=0.0.0.0:notaport\n");
        assert_eq!(
            super::validate_conf_listener_binds(Network::Testnet, &bad_p2p).unwrap_err(),
            "invalid_p2p_bind: 0.0.0.0:notaport"
        );
        let bad_mining = duta_core::netparams::Conf::parse("miningbind=0.0.0.0:notaport\n");
        assert_eq!(
            super::validate_conf_listener_binds(Network::Testnet, &bad_mining).unwrap_err(),
            "invalid_mining_bind: 0.0.0.0:notaport"
        );
        let ok = duta_core::netparams::Conf::parse("bind=0.0.0.0:18082\nminingbind=0.0.0.0:18085\n");
        assert!(super::validate_conf_listener_binds(Network::Testnet, &ok).is_ok());
    }

    #[test]
    fn load_runtime_conf_returns_default_when_missing() {
        let dir = std::env::temp_dir().join(format!(
            "dutad-load-runtime-conf-missing-{}",
            std::process::id()
        ));
        let _ = fs::create_dir_all(&dir);
        let path = dir.join("missing.conf");
        let conf = load_runtime_conf(path.to_str().unwrap(), false).unwrap();
        assert!(conf.get_last("network").is_none());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    #[cfg(unix)]
    fn load_runtime_conf_fails_when_existing_file_is_unreadable() {
        use std::io::Write;
        use std::os::unix::fs::PermissionsExt;

        let dir = std::env::temp_dir().join(format!(
            "dutad-load-runtime-conf-unreadable-{}",
            std::process::id()
        ));
        let _ = fs::create_dir_all(&dir);
        let path = dir.join("duta.conf");
        {
            let mut f = fs::File::create(&path).unwrap();
            writeln!(f, "network=testnet").unwrap();
        }
        let mut perms = fs::metadata(&path).unwrap().permissions();
        perms.set_mode(0o000);
        fs::set_permissions(&path, perms).unwrap();
        let err = load_runtime_conf(path.to_str().unwrap(), false).unwrap_err();
        assert!(err.contains("config_read_failed"));
        let mut cleanup_perms = fs::metadata(&path).unwrap().permissions();
        cleanup_perms.set_mode(0o644);
        let _ = fs::set_permissions(&path, cleanup_perms);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn load_runtime_conf_fails_when_explicit_path_is_missing() {
        let dir = std::env::temp_dir().join(format!(
            "dutad-load-runtime-conf-required-{}",
            std::process::id()
        ));
        let _ = fs::create_dir_all(&dir);
        let path = dir.join("missing-required.conf");
        let err = load_runtime_conf(path.to_str().unwrap(), true).unwrap_err();
        assert!(err.contains("config_read_failed"));
        let _ = fs::remove_dir_all(&dir);
    }
}
