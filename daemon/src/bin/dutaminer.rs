// Minimal external miner for dutad remote-mining endpoints (/work + /submit_work).
// - No extra Rust deps (uses existing workspace deps only).
// - Uses native TCP HTTP requests (no external curl dependency).
// - Heavy PoW brute-force happens here; daemon only verifies+commits.
//
// Build gate (must pass):
//   cd /root/duta && RUSTFLAGS="-Dwarnings" cargo build --release --workspace --all-targets
//
// Run:
//   target/release/dutaminer --rpc http://127.0.0.1:19085 --address miner1 --threads 4
//
// Notes:
// - bits is "leading zero bits" threshold (same as mine_once.rs).
// - Work expires; miner stops when expires_at reached and fetches fresh work.

use std::io::{Read, Write};
use std::net::{Shutdown, TcpStream};
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex, OnceLock,
};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::Parser;
use duta_core::{dutahash, types::H32};
use serde::Deserialize;
use serde_json::json;

#[derive(Parser, Debug)]
#[command(
    name = "dutaminer",
    version,
    about = "External PoW miner for dutad /work + /submit_work"
)]
struct Args {
    /// Mining RPC base URL, e.g. http://127.0.0.1:19085
    #[arg(long)]
    rpc: Option<String>,

    /// Miner address (daemon requires this parameter)
    #[arg(long)]
    address: Option<String>,

    /// Number of mining threads
    #[arg(long)]
    threads: Option<usize>,

    /// Sleep between work polls (ms)
    #[arg(long)]
    poll_ms: Option<u64>,

    /// Optional JSON config file
    #[arg(long)]
    config: Option<PathBuf>,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct MinerConfigFile {
    rpc: Option<String>,
    address: Option<String>,
    threads: Option<usize>,
    poll_ms: Option<u64>,
}

#[derive(Debug, Clone)]
struct RuntimeArgs {
    rpc: String,
    address: String,
    threads: usize,
    poll_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
struct WorkResp {
    work_id: String,
    height: u64,
    bits: u32,
    epoch: u64,
    mem_mb: usize,

    prevhash32: String,
    merkle32: String,
    anchor_hash32: String,

    timestamp: u64,
    expires_at: u64,
}

static WORK_REJECT_LOG_STATE: OnceLock<Mutex<Option<(String, Instant)>>> = OnceLock::new();
const WORK_REJECT_LOG_INTERVAL_SECS: u64 = 15;

fn work_reject_log_state() -> &'static Mutex<Option<(String, Instant)>> {
    WORK_REJECT_LOG_STATE.get_or_init(|| Mutex::new(None))
}

fn log_work_reject_once(message: &str) {
    let now = Instant::now();
    let mut state = match work_reject_log_state().lock() {
        Ok(g) => g,
        Err(_) => {
            eprintln!("{message}");
            return;
        }
    };
    match state.as_ref() {
        Some((last_msg, last_at))
            if last_msg == message
                && now.duration_since(*last_at) < Duration::from_secs(WORK_REJECT_LOG_INTERVAL_SECS) =>
        {
            return;
        }
        _ => {
            *state = Some((message.to_string(), now));
            eprintln!("{message}");
        }
    }
}

const MIN_POLL_MS: u64 = 50;
const GUARDED_BACKOFF_MS: u64 = 2_000;
const BUSY_BACKOFF_MS: u64 = 1_000;
const RPC_TIMEOUT_SECS: u64 = 5;

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::from_secs(0))
        .as_secs()
}

fn leading_zero_bits(h: &H32) -> u32 {
    let mut n: u32 = 0;
    for &b in h.as_bytes().iter() {
        if b == 0 {
            n += 8;
            continue;
        }
        n += b.leading_zeros();
        break;
    }
    n
}

fn parse_http_base(url: &str) -> Result<(String, u16), String> {
    let trimmed = url.trim().trim_end_matches('/');
    let rest = trimmed
        .strip_prefix("http://")
        .ok_or_else(|| "only_http_scheme_supported".to_string())?;
    let (host, port) = rest
        .split_once(':')
        .ok_or_else(|| "rpc_url_missing_port".to_string())?;
    let port = port
        .parse::<u16>()
        .map_err(|_| "rpc_url_invalid_port".to_string())?;
    if host.trim().is_empty() {
        return Err("rpc_url_missing_host".to_string());
    }
    Ok((host.trim().to_string(), port))
}

fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

fn parse_content_length(head: &[u8]) -> Option<usize> {
    let s = std::str::from_utf8(head).ok()?;
    for line in s.lines() {
        let mut parts = line.splitn(2, ':');
        let name = parts.next()?.trim();
        let value = parts.next()?.trim();
        if name.eq_ignore_ascii_case("Content-Length") {
            return value.parse::<usize>().ok();
        }
    }
    None
}

fn split_http_bytes(resp: &[u8]) -> Result<(u16, String), String> {
    let hend = find_header_end(resp).ok_or("bad_http_response")?;
    let head =
        std::str::from_utf8(&resp[..hend]).map_err(|e| format!("bad_http_header_utf8: {e}"))?;
    let body_bytes = &resp[hend + 4..];
    let body =
        String::from_utf8(body_bytes.to_vec()).map_err(|e| format!("bad_http_body_utf8: {e}"))?;
    let status = head
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .ok_or("missing_http_status")?
        .parse::<u16>()
        .map_err(|e| format!("bad_http_status: {e}"))?;
    Ok((status, body))
}

fn http_request(
    host: &str,
    port: u16,
    req: &[u8],
    timeout: Duration,
) -> Result<(String, Option<u16>), String> {
    let mut stream =
        TcpStream::connect((host, port)).map_err(|e| {
            format!(
                "rpc_connection_failed_check_ip_firewall host={host} port={port} err={e}"
            )
        })?;
    let _ = stream.set_read_timeout(Some(timeout));
    let _ = stream.set_write_timeout(Some(timeout));
    let _ = stream.set_nonblocking(false);
    stream
        .write_all(req)
        .map_err(|e| format!("write_request: {e}"))?;
    stream.flush().map_err(|e| format!("flush_request: {e}"))?;
    let _ = stream.shutdown(Shutdown::Write);

    let mut buf = Vec::with_capacity(8192);
    let mut tmp = [0u8; 4096];
    let deadline = std::time::Instant::now() + timeout;
    let mut header_end = None;
    let mut content_length: Option<usize> = None;

    loop {
        match stream.read(&mut tmp) {
            Ok(0) => break,
            Ok(n) => {
                buf.extend_from_slice(&tmp[..n]);
                if header_end.is_none() {
                    header_end = find_header_end(&buf);
                    if let Some(hend) = header_end {
                        content_length = parse_content_length(&buf[..hend]);
                    }
                }
                if let (Some(hend), Some(clen)) = (header_end, content_length) {
                    if buf.len() >= hend + 4 + clen {
                        break;
                    }
                }
            }
            Err(e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                if let (Some(hend), Some(clen)) = (header_end, content_length) {
                    if buf.len() >= hend + 4 + clen {
                        break;
                    }
                }
                if std::time::Instant::now() >= deadline {
                    return Err(format!(
                        "rpc_connection_timeout_check_ip_firewall timeout_ms={} host={} port={}",
                        timeout.as_millis(),
                        host,
                        port
                    ));
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(e) => return Err(format!("read_response: {e}")),
        }
    }

    let (status, body) = split_http_bytes(&buf)?;
    Ok((body, Some(status)))
}

fn http_get(url: &str, path: &str) -> Result<(String, Option<u16>), String> {
    let (host, port) = parse_http_base(url)?;
    let req = format!(
        "GET {path} HTTP/1.1\r\nHost: {host}:{port}\r\nUser-Agent: dutaminer\r\nAccept: application/json\r\nConnection: close\r\n\r\n"
    );
    http_request(&host, port, req.as_bytes(), Duration::from_secs(RPC_TIMEOUT_SECS))
}

fn http_post_json(url: &str, path: &str, body: &str) -> Result<(String, Option<u16>), String> {
    let (host, port) = parse_http_base(url)?;
    let req = format!(
        "POST {path} HTTP/1.1\r\nHost: {host}:{port}\r\nUser-Agent: dutaminer\r\nContent-Type: application/json\r\nAccept: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    http_request(&host, port, req.as_bytes(), Duration::from_secs(RPC_TIMEOUT_SECS))
}

fn guess_mining_port_base(rpc_base: &str) -> Option<String> {
    // If user passes admin RPC port by mistake (18083/19083), try the mining port (18085/19085).
    // Keep this conservative: only rewrite when the string ends with the known admin ports.
    if rpc_base.ends_with(":18083") {
        return Some(rpc_base.trim_end_matches(":18083").to_string() + ":18085");
    }
    if rpc_base.ends_with(":19083") {
        return Some(rpc_base.trim_end_matches(":19083").to_string() + ":19085");
    }
    None
}

fn fmt_secs(s: u64) -> String {
    if s < 60 {
        return format!("{}s", s);
    }
    let m = s / 60;
    let r = s % 60;
    if m < 60 {
        return format!("{}m{}s", m, r);
    }
    let h = m / 60;
    let rm = m % 60;
    format!("{}h{}m", h, rm)
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

fn format_hashrate(hs: f64) -> String {
    // SI (base-1000) using H/s units: H/s, KH/s, MH/s, GH/s, TH/s, PH/s, EH/s
    if !hs.is_finite() || hs <= 0.0 {
        return "0 H/s".to_string();
    }
    let units = ["H/s", "KH/s", "MH/s", "GH/s", "TH/s", "PH/s", "EH/s"];
    let mut v = hs;
    let mut i = 0usize;
    while v >= 1000.0 && i + 1 < units.len() {
        v /= 1000.0;
        i += 1;
    }
    if v >= 100.0 {
        format!("{:.0} {}", v, units[i])
    } else if v >= 10.0 {
        format!("{:.1} {}", v, units[i])
    } else {
        format!("{:.2} {}", v, units[i])
    }
}

fn explain_submit_result(http_code: Option<u16>, body: &str, mined_hash32: &str) {
    // Supports both legacy and newer submit responses.
    let code = http_code.unwrap_or(0);
    let v: serde_json::Value = match serde_json::from_str(body) {
        Ok(v) => v,
        Err(_) => {
            println!(
                "REJECTED share: invalid response (http_code={}) hash={}",
                code, mined_hash32
            );
            println!("reason=invalid");
            return;
        }
    };

    // New-style
    if let Some(status) = v.get("status").and_then(|x| x.as_str()) {
        if status == "accepted" {
            let h = v.get("height").and_then(|x| x.as_u64()).unwrap_or(0);
            let hash = v.get("hash32").and_then(|x| x.as_str()).unwrap_or("");
            if !hash.is_empty() {
                println!("ACCEPTED block: height={} hash={}", h, hash);
            } else {
                println!("ACCEPTED share: height={}", h);
            }
            return;
        }
        if status == "rejected" {
            let reason = v
                .get("reason")
                .and_then(|x| x.as_str())
                .unwrap_or("invalid");
            let detail = v.get("detail").and_then(|x| x.as_str()).unwrap_or("");
            let msg = v
                .get("message")
                .and_then(|x| x.as_str())
                .unwrap_or("Share rejected.");
            if is_sync_submit_reject(reason, detail, msg) {
                println!(
                    "share update pending: waiting for chain sync; miner will retry automatically. hash={}",
                    mined_hash32
                );
                return;
            }
            println!("REJECTED share: {} hash={}", msg, mined_hash32);
            println!("reason={}", reason);
            println!("raw={}", body);
            return;
        }
    }

    // Legacy-style
    if code == 200 && v.get("ok").and_then(|x| x.as_bool()) == Some(true) {
        let h = v.get("height").and_then(|x| x.as_u64()).unwrap_or(0);
        let hash = v.get("hash32").and_then(|x| x.as_str()).unwrap_or("");
        println!("ACCEPTED block: height={} hash={}", h, hash);
        return;
    }

    let err = v
        .get("error")
        .and_then(|x| x.as_str())
        .unwrap_or("unknown_error");
    let detail = v.get("detail").and_then(|x| x.as_str()).unwrap_or("");

    match err {
        "stale_work" => {
            println!("REJECTED share: Share rejected: stale share (submitted too late). Please fetch new work and try again. hash={}", mined_hash32);
            println!("reason=stale");
            println!("raw={}", body);
        }
        "pow_invalid" => {
            println!(
                "REJECTED share: Share rejected: low difficulty (does not meet target). hash={}",
                mined_hash32
            );
            println!("reason=low_difficulty");
            println!("raw={}", body);
        }
        "accept_failed" => {
            if is_sync_submit_reject(err, detail, detail) {
                println!(
                    "share update pending: waiting for chain sync; miner will retry automatically. hash={}",
                    mined_hash32
                );
            } else if detail.contains("stale_or_out_of_order_block") {
                println!("REJECTED share: Share rejected: stale share (submitted too late). Please fetch new work and try again. hash={}", mined_hash32);
                println!("reason=stale");
            } else if detail.contains("bad_prevhash") || detail.contains("bits_invalid") {
                println!("REJECTED share: Share rejected: work mismatch (chain tip changed). Please fetch new work. hash={}", mined_hash32);
                println!("reason=work_mismatch");
            } else {
                if detail.is_empty() {
                    println!(
                        "REJECTED share: Share rejected: invalid share. hash={}",
                        mined_hash32
                    );
                } else {
                    println!(
                        "REJECTED share: Share rejected: invalid share ({}). hash={}",
                        detail, mined_hash32
                    );
                }
                println!("reason=invalid");
            }
            println!("raw={}", body);
        }
        _ => {
            if detail.is_empty() {
                println!(
                    "REJECTED share: Share rejected: invalid share ({}). hash={}",
                    err, mined_hash32
                );
            } else {
                println!(
                    "REJECTED share: Share rejected: invalid share ({}: {}). hash={}",
                    err, detail, mined_hash32
                );
            }
            println!("reason=invalid");
            println!("raw={}", body);
        }
    }
}

fn is_sync_submit_reject(reason: &str, detail: &str, message: &str) -> bool {
    reason.contains("launch_guard")
        || detail.contains("launch_guard_")
        || detail.contains("waiting_for_official_chain_sync")
        || message.contains("waiting for official chain sync")
        || message.contains("waiting for launch sync")
}

fn work_retry_delay_ms(http_code: Option<u16>, body: &str, poll_ms: u64) -> u64 {
    let Ok(v) = serde_json::from_str::<serde_json::Value>(body) else {
        return poll_ms;
    };
    let err = v.get("error").and_then(|x| x.as_str()).unwrap_or("");
    let detail = v.get("detail").map(|x| x.to_string()).unwrap_or_default();
    if err == "launch_guard_not_ready" || detail.contains("launch_guard_") {
        return poll_ms.max(GUARDED_BACKOFF_MS);
    }
    if err == "syncing" || detail.contains("syncing") {
        return poll_ms.max(GUARDED_BACKOFF_MS);
    }
    if err == "busy" || err == "too_many_outstanding_work" {
        return poll_ms.max(BUSY_BACKOFF_MS);
    }
    if http_code == Some(429) {
        return poll_ms.max(BUSY_BACKOFF_MS);
    }
    poll_ms
}

fn explain_work_fetch_reject(http_code: Option<u16>, body: &str) {
    let code = http_code.unwrap_or(0);
    let Ok(v) = serde_json::from_str::<serde_json::Value>(body) else {
        eprintln!("work fetch http_code={} raw={}", code, body);
        return;
    };
    let err = v.get("error").and_then(|x| x.as_str()).unwrap_or("unknown_error");
    let detail = v.get("detail").map(|x| x.to_string()).unwrap_or_default();
    match err {
        "launch_guard_not_ready" => {
            let msg = if detail.contains("launch_guard_solo_bootstrap") {
                "solo mining is temporarily limited during early launch"
            } else if detail.contains("launch_guard_official_tip_mismatch")
                || detail.contains("launch_guard_competing_official_tip")
                || detail.contains("launch_guard_official_ahead")
            {
                "work update pending: waiting for official chain sync"
            } else {
                "work update pending: waiting for launch sync"
            };
            log_work_reject_once(msg);
        }
        "syncing" => {
            log_work_reject_once("work update pending: waiting for node sync");
        }
        "busy" => {
            log_work_reject_once("work update pending: node busy, retrying");
        }
        "too_many_outstanding_work" => {
            log_work_reject_once("work update pending: miner queue is full, retrying");
        }
        _ => {
            eprintln!("work fetch http_code={} error={} detail={}", code, err, detail);
        }
    }
}

fn normalized_poll_ms(poll_ms: u64) -> u64 {
    poll_ms.max(MIN_POLL_MS)
}

fn load_config_file(path: &PathBuf) -> Result<MinerConfigFile, String> {
    let raw = std::fs::read_to_string(path)
        .map_err(|e| format!("read_config {}: {e}", path.display()))?;
    serde_json::from_str::<MinerConfigFile>(&raw)
        .map_err(|e| format!("parse_config {}: {e}", path.display()))
}

fn resolve_runtime_args(args: &Args) -> Result<RuntimeArgs, String> {
    let cfg = match args.config.as_ref() {
        Some(path) => load_config_file(path)?,
        None => MinerConfigFile::default(),
    };
    let rpc = args
        .rpc
        .clone()
        .or(cfg.rpc)
        .ok_or_else(|| "--rpc is required".to_string())?;
    let address = args
        .address
        .clone()
        .or(cfg.address)
        .ok_or_else(|| "--address is required".to_string())?;
    let threads = args.threads.or(cfg.threads).unwrap_or(1);
    if threads == 0 {
        return Err("--threads must be >= 1".to_string());
    }
    Ok(RuntimeArgs {
        rpc,
        address,
        threads,
        poll_ms: args.poll_ms.or(cfg.poll_ms).unwrap_or(200),
    })
}

fn main() -> Result<(), String> {
    let args = resolve_runtime_args(&Args::parse())?;
    let poll_ms = normalized_poll_ms(args.poll_ms);
    if poll_ms != args.poll_ms {
        eprintln!(
            "dutaminer: poll_ms={} too low, using {}ms minimum",
            args.poll_ms, poll_ms
        );
    }

    // Dataset cache keyed by (epoch, anchor_hash32, mem_mb)
    let mut cache_key: Option<(u64, String, usize)> = None;
    let mut cached_dataset: Arc<Vec<u8>> = Arc::new(Vec::new());

    let mut rpc = args.rpc.trim_end_matches('/').to_string();
    println!(
        "dutaminer: rpc={} address={} threads={} poll_ms={}",
        rpc, args.address, args.threads, poll_ms
    );

    let mut last_hs: f64 = 0.0;

    loop {
        let (w_body, w_code) = match http_get(&rpc, &format!("/work?address={}&hs={}", args.address, last_hs)) {
            Ok(t) => t,
            Err(e) => {
                eprintln!("work fetch error: {e}");
                if e.contains("rpc_connection_failed_check_ip_firewall")
                    || e.contains("rpc_connection_timeout_check_ip_firewall")
                {
                    eprintln!(
                        "RPC Connection Failed - Check IP/Firewall (rpc={} timeout={}s)",
                        rpc, RPC_TIMEOUT_SECS
                    );
                }
                thread::sleep(Duration::from_millis(poll_ms));
                continue;
            }
        };
        if let Some(c) = w_code {
            if c != 200 {
                // Common operator mistake after Phase 3 split: pointing at admin RPC (18083/19083)
                // instead of mining port (18085/19085). If we see 404, try the known mining port once.
                if c == 404 {
                    if let Some(alt) = guess_mining_port_base(&rpc) {
                        let alt_work_url =
                            format!("/work?address={}&hs={}", args.address, last_hs);
                        if let Ok((_alt_body, alt_code)) = http_get(&alt, &alt_work_url) {
                            if alt_code == Some(200) {
                                eprintln!(
                                    "work got 404 on {}; switching to mining port {}",
                                    rpc, alt
                                );
                                rpc = alt;
                                // Retry immediately on the corrected port
                                continue;
                            } else {
                                eprintln!(
                                    "work fetch http_code={} raw={} (also tried {} http_code={:?})",
                                    c, w_body, alt, alt_code
                                );
                                thread::sleep(Duration::from_millis(poll_ms));
                                continue;
                            }
                        }
                    }
                }

                explain_work_fetch_reject(Some(c), &w_body);
                thread::sleep(Duration::from_millis(work_retry_delay_ms(
                    Some(c),
                    &w_body,
                    poll_ms,
                )));
                continue;
            }
        }

        let w: WorkResp = match serde_json::from_str(&w_body) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("work json parse error: {e}; raw={w_body}");
                thread::sleep(Duration::from_millis(poll_ms));
                continue;
            }
        };

        let ttl = w.expires_at.saturating_sub(now_unix());
        {
            let diff = expected_hashes(w.bits).unwrap_or(0);
            let diff_s = format_hash_units(diff);
            println!(
                "work: height={} bits={} difficulty={} epoch={} mem_mb={} ttl={}s work_id={}",
                w.height, w.bits, diff_s, w.epoch, w.mem_mb, ttl, w.work_id
            );
        }
        let key = (w.epoch, w.anchor_hash32.clone(), w.mem_mb);
        if cache_key.as_ref() != Some(&key) {
            let anchor = H32::from_hex(&w.anchor_hash32).ok_or("bad anchor_hash32 hex")?;
            let ds = dutahash::build_dataset_for_epoch(w.epoch, anchor, w.mem_mb);
            cached_dataset = Arc::new(ds);
            cache_key = Some(key);
            println!("dataset built: epoch={} mem_mb={}", w.epoch, w.mem_mb);
        }

        // Assemble canonical header bytes used by daemon's remote-mining implementation:
        // [prev(32) | merkle(32) | timestamp(u64 LE) | bits(u32 LE)]
        let prev = H32::from_hex(&w.prevhash32).ok_or("bad prevhash32 hex")?;
        let merkle = H32::from_hex(&w.merkle32).ok_or("bad merkle32 hex")?;

        let mut header = [0u8; 80];
        header[0..32].copy_from_slice(prev.as_bytes());
        header[32..64].copy_from_slice(merkle.as_bytes());
        header[64..72].copy_from_slice(&w.timestamp.to_le_bytes());
        header[72..80].copy_from_slice(&(w.bits as u64).to_le_bytes());

        let anchor = H32::from_hex(&w.anchor_hash32).ok_or("bad anchor_hash32 hex")?;
        let mine_start = std::time::Instant::now();
        let hashes_total: Arc<std::sync::atomic::AtomicU64> =
            Arc::new(std::sync::atomic::AtomicU64::new(0));
        let found = Arc::new(AtomicBool::new(false));
        let found_nonce: Arc<Mutex<Option<u64>>> = Arc::new(Mutex::new(None));
        let dataset = cached_dataset.clone();
        let threads = args.threads;
        let expires_at = w.expires_at;
        let bits = w.bits;

        let mut handles = Vec::with_capacity(threads);
        for tid in 0..threads {
            let found = found.clone();
            let found_nonce = found_nonce.clone();
            let hashes_total = hashes_total.clone();
            let dataset = dataset.clone();
            let header = header; // Copy (80 bytes)
            handles.push(thread::spawn(move || {
                let mut nonce: u64 = tid as u64;
                let step: u64 = threads as u64;
                let mut local_hashes: u64 = 0;
                while !found.load(Ordering::Relaxed) {
                    if now_unix() >= expires_at {
                        break;
                    }
                    let d = dutahash::pow_digest(&header, nonce, w.height, anchor, &dataset);
                    local_hashes = local_hashes.wrapping_add(1);
                    if leading_zero_bits(&d) >= bits {
                        found.store(true, Ordering::Relaxed);
                        *found_nonce.lock().unwrap() = Some(nonce);
                        break;
                    }
                    nonce = nonce.wrapping_add(step);
                }
                hashes_total.fetch_add(local_hashes, Ordering::Relaxed);
            }));
        }

        for h in handles {
            let _ = h.join();
        }

        let nonce = match *found_nonce.lock().unwrap() {
            Some(n) => n,
            None => {
                let ttl = w.expires_at.saturating_sub(now_unix());
                eprintln!(
                    "work window ended before a valid block was found (bits={}, ttl={}); requesting fresh work...",
                    w.bits,
                    fmt_secs(ttl)
                );
                thread::sleep(Duration::from_millis(poll_ms));
                continue;
            }
        };

        let total_hashes = hashes_total.load(Ordering::Relaxed) as f64;
        let elapsed = mine_start.elapsed().as_secs_f64().max(0.000_001);
        let hs = total_hashes / elapsed;
        let hs_str = format_hashrate(hs);

        let mined_digest = dutahash::pow_digest(&header, nonce, w.height, anchor, &dataset);
        let mined_hash32 = mined_digest.to_hex();

        println!(
            "hashrate: {} (hashes={} elapsed={:.2}s)",
            hs_str, total_hashes as u64, elapsed
        );
        last_hs = hs;

        // Best-effort: query aggregated wallet hashrate from server
        if let Ok((st_body, st_code)) = http_get(&rpc, &format!("/minerstats?address={}", args.address)) {
            if st_code == Some(200) {
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(&st_body) {
                    let miners = v.get("miners").and_then(|x| x.as_u64()).unwrap_or(0);
                    let total_hs = v.get("total_hs").and_then(|x| x.as_f64()).unwrap_or(0.0);
                    println!(
                        "wallet_total_hashrate: {} (miners={})",
                        format_hashrate(total_hs),
                        miners
                    );
                }
            }
        }

        let body = json!({"work_id": w.work_id, "nonce": nonce}).to_string();
        match http_post_json(&rpc, "/submit_work", &body) {
            Ok((resp_body, resp_code)) => {
                println!("submit: nonce={} work_id={}", nonce, w.work_id);
                explain_submit_result(resp_code, &resp_body, &mined_hash32);
            }
            Err(e) => eprintln!("submit error: {e}"),
        }

        thread::sleep(Duration::from_millis(20));
    }
}

#[cfg(test)]
mod tests {
    use super::{
        normalized_poll_ms, resolve_runtime_args, work_retry_delay_ms, Args, BUSY_BACKOFF_MS,
        GUARDED_BACKOFF_MS, MIN_POLL_MS,
    };
    use clap::Parser;
    use std::fs;

    #[test]
    fn poll_ms_is_clamped_to_release_floor() {
        assert_eq!(normalized_poll_ms(0), MIN_POLL_MS);
        assert_eq!(normalized_poll_ms(1), MIN_POLL_MS);
        assert_eq!(normalized_poll_ms(49), MIN_POLL_MS);
        assert_eq!(normalized_poll_ms(50), 50);
        assert_eq!(normalized_poll_ms(200), 200);
    }

    #[test]
    fn work_reject_backoff_escalates_for_launch_guard_and_busy_conditions() {
        assert_eq!(
            work_retry_delay_ms(
                Some(503),
                r#"{"error":"launch_guard_not_ready","detail":"launch_guard_official_peer_insufficient"}"#,
                200
            ),
            GUARDED_BACKOFF_MS
        );
        assert_eq!(
            work_retry_delay_ms(Some(503), r#"{"error":"syncing","detail":"syncing tip_height=5"}"#, 200),
            GUARDED_BACKOFF_MS
        );
        assert_eq!(
            work_retry_delay_ms(Some(429), r#"{"error":"too_many_outstanding_work"}"#, 200),
            BUSY_BACKOFF_MS
        );
    }

    #[test]
    fn config_file_is_loaded_and_cli_overrides_it() {
        let path = std::env::temp_dir().join(format!(
            "dutaminer-test-{}.json",
            std::process::id()
        ));
        fs::write(
            &path,
            r#"{"rpc":"http://127.0.0.1:19085","address":"dutcfg","threads":3,"poll_ms":450}"#,
        )
        .unwrap();
        let args = Args::parse_from([
            "dutaminer",
            "--config",
            path.to_str().unwrap(),
            "--threads",
            "2",
        ]);
        let runtime = resolve_runtime_args(&args).unwrap();
        assert_eq!(runtime.rpc, "http://127.0.0.1:19085");
        assert_eq!(runtime.address, "dutcfg");
        assert_eq!(runtime.threads, 2);
        assert_eq!(runtime.poll_ms, 450);
        let _ = fs::remove_file(path);
    }
}
