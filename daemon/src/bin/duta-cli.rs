//! duta-cli: daemon RPC client

use clap::{Parser, Subcommand};
use duta_core::netparams::{Conf, Network};
use serde_json::json;
use std::fs;
use std::io::{Read, Write};
use std::net::{Shutdown, TcpStream};
use std::time::{Duration, Instant};

const RELEASE_VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Parser, Debug)]
#[command(
    name = "duta-cli",
    version = RELEASE_VERSION,
    about = "DUTA daemon CLI (RPC client)",
    after_help = "Examples:\n  duta-cli getpeerinfo\n  duta-cli listbanned\n  duta-cli banpeer 203.0.113.10 launch_abuse\n  duta-cli unbanpeer 203.0.113.10"
)]
struct Args {
    #[arg(long)]
    testnet: bool,
    #[arg(long)]
    stagenet: bool,
    #[arg(long)]
    datadir: Option<String>,
    #[arg(long)]
    conf: Option<String>,
    #[arg(long, default_value = "")]
    rpc: String,
    #[arg(long)]
    pretty: bool,
    #[arg(long)]
    raw: bool,
    #[arg(long, default_value_t = 5000)]
    rpc_timeout_ms: u64,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    Health,
    Tip,
    Blocksfrom {
        from: u64,
        #[arg(default_value_t = 200)]
        limit: u64,
    },
    Utxo {
        txid: String,
        vout: u64,
    },
    Getblockcount,
    Getbestblockhash,
    Getblockhash {
        height: u64,
    },
    Getblock {
        hash_or_height: String,
        #[arg(default_value_t = 1)]
        verbosity: u64,
    },
    Getblockchaininfo,
    Getblockheader {
        hash_or_height: String,
        #[arg(long, num_args = 0..=1, default_missing_value = "true")]
        verbose: Option<bool>,
    },
    Getrawmempool {
        #[arg(long, num_args = 0..=1, default_missing_value = "true")]
        verbose: Option<bool>,
    },
    Getmempoolinfo,
    Gettxout {
        txid: String,
        vout: u64,
        #[arg(long, num_args = 0..=1, default_missing_value = "true")]
        include_mempool: Option<bool>,
    },
    Gettxoutsetinfo,
    Decoderawtransaction {
        tx_hex: String,
    },
    Sendrawtransaction {
        tx_hex: String,
    },
    Getrawtransaction {
        txid: String,
        #[arg(long, num_args = 0..=1, default_missing_value = "true")]
        verbose: Option<bool>,
    },
    Getchaintips,
    Getdifficulty,
    Getnetworkhashps {
        #[arg(default_value_t = 120)]
        lookup: u64,
    },
    Getblocktemplate {
        address: String,
    },
    Submitblock {
        block_hex: String,
    },
    Getmininginfo,
    Validateaddress {
        address: String,
    },
    Getinfo,
    Getnetworkinfo,
    Getpeerinfo,
    Listbanned,
    Getconnectioncount,
    Getrpcinfo,
    Ping,
    Uptime,
    Addpeer {
        peer: String,
    },
    Addnode {
        peer: String,
    },
    Banpeer {
        ip: String,
        reason: Option<String>,
    },
    Unbanpeer {
        ip: String,
    },
    Call {
        method: String,
        params: Vec<String>,
    },
}

fn main() {
    let args = Args::parse();
    let timeout = Duration::from_millis(args.rpc_timeout_ms.max(1));
    let (host, port) = resolve_rpc(&args);
    let out = match &args.cmd {
        Cmd::Health => http_get_json_allow_error_body(&host, port, "/health", timeout),
        Cmd::Tip => http_get_json(&host, port, "/tip", timeout),
        Cmd::Blocksfrom { from, limit } => http_get_json(
            &host,
            port,
            &format!("/blocks_from?from={from}&limit={limit}"),
            timeout,
        ),
        Cmd::Utxo { txid, vout } => http_get_json(
            &host,
            port,
            &format!("/utxo?txid={}&vout={}", url_enc(txid), vout),
            timeout,
        ),
        Cmd::Getblockcount => rpc_call(&host, port, "getblockcount", vec![], timeout),
        Cmd::Getbestblockhash => rpc_call(&host, port, "getbestblockhash", vec![], timeout),
        Cmd::Getblockhash { height } => {
            rpc_call(&host, port, "getblockhash", vec![json!(height)], timeout)
        }
        Cmd::Getblock {
            hash_or_height,
            verbosity,
        } => rpc_call(
            &host,
            port,
            "getblock",
            vec![parse_hash_or_height(hash_or_height), json!(verbosity)],
            timeout,
        ),
        Cmd::Getblockchaininfo => rpc_call(&host, port, "getblockchaininfo", vec![], timeout),
        Cmd::Getblockheader {
            hash_or_height,
            verbose,
        } => {
            let mut params = vec![parse_hash_or_height(hash_or_height)];
            if let Some(v) = verbose {
                params.push(json!(v));
            }
            rpc_call(&host, port, "getblockheader", params, timeout)
        }
        Cmd::Getrawmempool { verbose } => {
            let mut params = Vec::new();
            if let Some(v) = verbose {
                params.push(json!(v));
            }
            rpc_call(&host, port, "getrawmempool", params, timeout)
        }
        Cmd::Getmempoolinfo => rpc_call(&host, port, "getmempoolinfo", vec![], timeout),
        Cmd::Gettxout {
            txid,
            vout,
            include_mempool,
        } => {
            let mut params = vec![json!(txid), json!(vout)];
            if let Some(v) = include_mempool {
                params.push(json!(v));
            }
            rpc_call(&host, port, "gettxout", params, timeout)
        }
        Cmd::Gettxoutsetinfo => rpc_call(&host, port, "gettxoutsetinfo", vec![], timeout),
        Cmd::Decoderawtransaction { tx_hex } => rpc_call(
            &host,
            port,
            "decoderawtransaction",
            vec![json!(tx_hex)],
            timeout,
        ),
        Cmd::Sendrawtransaction { tx_hex } => rpc_call(
            &host,
            port,
            "sendrawtransaction",
            vec![json!(tx_hex)],
            timeout,
        ),
        Cmd::Getrawtransaction { txid, verbose } => {
            let mut params = vec![json!(txid)];
            if let Some(v) = verbose {
                params.push(json!(v));
            }
            rpc_call(&host, port, "getrawtransaction", params, timeout)
        }
        Cmd::Getchaintips => rpc_call(&host, port, "getchaintips", vec![], timeout),
        Cmd::Getdifficulty => rpc_call(&host, port, "getdifficulty", vec![], timeout),
        Cmd::Getnetworkhashps { lookup } => rpc_call(
            &host,
            port,
            "getnetworkhashps",
            vec![json!(lookup)],
            timeout,
        ),
        Cmd::Getblocktemplate { address } => rpc_call(
            &host,
            port,
            "getblocktemplate",
            vec![json!({"address": address})],
            timeout,
        ),
        Cmd::Submitblock { block_hex } => {
            rpc_call(&host, port, "submitblock", vec![json!(block_hex)], timeout)
        }
        Cmd::Getmininginfo => rpc_call(&host, port, "getmininginfo", vec![], timeout),
        Cmd::Validateaddress { address } => rpc_call(
            &host,
            port,
            "validateaddress",
            vec![json!(address)],
            timeout,
        ),
        Cmd::Getinfo => rpc_call(&host, port, "getinfo", vec![], timeout),
        Cmd::Getnetworkinfo => rpc_call(&host, port, "getnetworkinfo", vec![], timeout),
        Cmd::Getpeerinfo => rpc_call(&host, port, "getpeerinfo", vec![], timeout),
        Cmd::Listbanned => rpc_call(&host, port, "listbanned", vec![], timeout),
        Cmd::Getconnectioncount => rpc_call(&host, port, "getconnectioncount", vec![], timeout),
        Cmd::Getrpcinfo => rpc_call(&host, port, "getrpcinfo", vec![], timeout),
        Cmd::Ping => rpc_call(&host, port, "ping", vec![], timeout),
        Cmd::Uptime => rpc_call(&host, port, "uptime", vec![], timeout),
        Cmd::Addpeer { peer } => rpc_call(&host, port, "addpeer", vec![json!(peer)], timeout),
        Cmd::Addnode { peer } => rpc_call(&host, port, "addnode", vec![json!(peer)], timeout),
        Cmd::Banpeer { ip, reason } => {
            let mut params = vec![json!(ip)];
            if let Some(reason) = reason {
                params.push(json!(reason));
            }
            rpc_call(&host, port, "banpeer", params, timeout)
        }
        Cmd::Unbanpeer { ip } => rpc_call(&host, port, "unbanpeer", vec![json!(ip)], timeout),
        Cmd::Call { method, params } => rpc_call(
            &host,
            port,
            method,
            params.iter().map(|p| parse_param(p)).collect(),
            timeout,
        ),
    };

    match out {
        Ok(body) => {
            let exit_code = print_rpc_output(&args.cmd, &body, args.pretty && !args.raw, args.raw);
            if exit_code != 0 {
                std::process::exit(exit_code);
            }
        }
        Err(e) => {
            eprintln!("duta-cli error: {e}");
            std::process::exit(1);
        }
    }
}

fn print_rpc_output(cmd: &Cmd, body: &str, pretty: bool, raw: bool) -> i32 {
    if raw {
        print_raw(body);
        return command_exit_code(cmd, body);
    }
    if pretty {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(body) {
            if let Ok(s) = serde_json::to_string_pretty(&v) {
                println!("{s}");
                return command_exit_code_from_value(cmd, &v);
            }
        }
    }
    print_raw(&format_response(cmd, body));
    command_exit_code(cmd, body)
}

fn json_field<'a>(v: &'a serde_json::Value, path: &[&str]) -> Option<&'a serde_json::Value> {
    let mut cur = v;
    for part in path {
        cur = cur.get(*part)?;
    }
    Some(cur)
}

fn json_bool(v: &serde_json::Value, path: &[&str]) -> Option<bool> {
    json_field(v, path)?.as_bool()
}

fn json_i64(v: &serde_json::Value, path: &[&str]) -> Option<i64> {
    json_field(v, path)?.as_i64()
}

fn json_f64(v: &serde_json::Value, path: &[&str]) -> Option<f64> {
    json_field(v, path)?.as_f64()
}

fn json_str<'a>(v: &'a serde_json::Value, path: &[&str]) -> Option<&'a str> {
    json_field(v, path)?.as_str()
}

fn format_health_response(v: &serde_json::Value, _body: &str) -> String {
    let status = json_str(v, &["status"]).unwrap_or("unknown");
    let ready = json_bool(v, &["ok"]).unwrap_or(false);
    let peer_ready = json_bool(v, &["peer_ready"]).unwrap_or(false);
    let tip_height = json_i64(v, &["tip_height"]).unwrap_or(0);
    let best_seen_height = json_i64(v, &["best_seen_height"]).unwrap_or(tip_height);
    let peer_count = json_i64(v, &["peer_count"]).unwrap_or(0);
    let persisted_peer_count = json_i64(v, &["persisted_peer_count"]).unwrap_or(0);
    let ban_count = json_i64(v, &["ban_count"]).unwrap_or(0);
    let sync_gate_detail = json_field(v, &["sync_gate_detail"])
        .map(|value| match value {
            serde_json::Value::String(s) => s.clone(),
            other => other.to_string(),
        })
        .unwrap_or_else(|| "-".to_string());
    let sync_progress = json_f64(v, &["sync_progress"]).unwrap_or(0.0) * 100.0;
    let header = if ready {
        "daemon ready"
    } else {
        "daemon not ready"
    };
    format!(
        "{header}\nstatus: {status}\npeer ready: {}\npeer count: {peer_count} (persisted: {persisted_peer_count})\ntip height: {tip_height}\nbest seen: {best_seen_height}\nsync progress: {:.2}%\nban count: {ban_count}\nsync gate detail: {sync_gate_detail}",
        if peer_ready { "yes" } else { "no" },
        sync_progress
    )
}

fn format_response(cmd: &Cmd, body: &str) -> String {
    let v: serde_json::Value = match serde_json::from_str(body) {
        Ok(v) => v,
        Err(_) => return body.to_string(),
    };
    match cmd {
        Cmd::Health => format_health_response(&v, body),
        _ => body.to_string(),
    }
}

fn print_raw(body: &str) {
    print!("{body}");
    if !body.ends_with('\n') {
        println!();
    }
}

fn command_exit_code(cmd: &Cmd, body: &str) -> i32 {
    match serde_json::from_str::<serde_json::Value>(body) {
        Ok(v) => command_exit_code_from_value(cmd, &v),
        Err(_) => 0,
    }
}

fn command_exit_code_from_value(cmd: &Cmd, v: &serde_json::Value) -> i32 {
    if matches!(cmd, Cmd::Health) {
        return if v.get("ok").and_then(|x| x.as_bool()) == Some(false) {
            1
        } else {
            0
        };
    }
    rpc_error_exit_code_from_value(v)
}

fn rpc_error_exit_code_from_value(v: &serde_json::Value) -> i32 {
    match v.get("error") {
        Some(err) if !err.is_null() => 1,
        _ => 0,
    }
}

fn parse_hash_or_height(s: &str) -> serde_json::Value {
    match s.parse::<u64>() {
        Ok(h) => json!(h),
        Err(_) => json!(s),
    }
}

fn parse_param(s: &str) -> serde_json::Value {
    serde_json::from_str::<serde_json::Value>(s).unwrap_or_else(|_| {
        if let Ok(v) = s.parse::<i64>() {
            json!(v)
        } else if let Ok(v) = s.parse::<u64>() {
            json!(v)
        } else if let Ok(v) = s.parse::<f64>() {
            json!(v)
        } else if s.eq_ignore_ascii_case("true") {
            json!(true)
        } else if s.eq_ignore_ascii_case("false") {
            json!(false)
        } else if s.eq_ignore_ascii_case("null") {
            serde_json::Value::Null
        } else {
            json!(s)
        }
    })
}

fn resolve_rpc(args: &Args) -> (String, u16) {
    if !args.rpc.trim().is_empty() {
        return split_host_port(&args.rpc).unwrap_or_else(|| {
            eprintln!("Invalid --rpc, expected host:port");
            std::process::exit(2);
        });
    }

    let net = if args.stagenet {
        Network::Stagenet
    } else if args.testnet {
        Network::Testnet
    } else {
        Network::Mainnet
    };

    let mut datadir = normalize_path(net.default_data_dir_unix());
    if let Some(dd) = args.datadir.as_deref() {
        let dd2 = normalize_path(dd);
        if !dd2.is_empty() {
            datadir = dd2;
        }
    }

    let mut conf_path = format!("{}/duta.conf", datadir.trim_end_matches('/'));
    if let Some(cp) = args.conf.as_deref() {
        let cp2 = normalize_path(cp);
        if !cp2.is_empty() {
            conf_path = cp2;
        }
    }

    let mut conf = Conf::default();
    if let Ok(s) = fs::read_to_string(&conf_path) {
        conf = Conf::parse(&s);
    }

    if args.datadir.is_none() {
        if let Some(dd) = conf.get_last("datadir") {
            let dd2 = normalize_path(&dd);
            if !dd2.is_empty() && dd2 != datadir {
                datadir = dd2;
                if args.conf.is_none() {
                    conf_path = format!("{}/duta.conf", datadir.trim_end_matches('/'));
                }
                if let Ok(s) = fs::read_to_string(&conf_path) {
                    conf = Conf::parse(&s);
                }
            }
        }
    }

    let host = conf
        .get_last("rpcconnect")
        .or_else(|| conf.get_last("rpcbind"))
        .filter(|v| v.trim() == "127.0.0.1")
        .unwrap_or_else(|| "127.0.0.1".to_string());

    let default_port = net.default_daemon_rpc_port();
    let port = conf
        .get_last("rpcport")
        .and_then(|s| s.trim().parse::<u16>().ok())
        .unwrap_or(default_port);

    (host, port)
}

fn normalize_path(p: &str) -> String {
    let t = p.trim();
    if let Some(rest) = t.strip_prefix("~/") {
        if let Ok(home) = std::env::var("HOME") {
            return format!("{}/{}", home.trim_end_matches('/'), rest);
        }
    }
    t.to_string()
}

fn split_host_port(s: &str) -> Option<(String, u16)> {
    let mut it = s.split(':');
    let host = it.next()?.trim();
    let port = it.next()?.trim().parse::<u16>().ok()?;
    if host.is_empty() || it.next().is_some() {
        return None;
    }
    Some((host.to_string(), port))
}

fn rpc_call(
    host: &str,
    port: u16,
    method: &str,
    params: Vec<serde_json::Value>,
    timeout: Duration,
) -> Result<String, String> {
    let body = json!({"jsonrpc":"2.0","id":"1","method":method,"params":params}).to_string();
    http_post_json(host, port, "/rpc", &body, timeout)
}

fn http_get_json(host: &str, port: u16, path: &str, timeout: Duration) -> Result<String, String> {
    let req = format!(
        "GET {path} HTTP/1.1\r\nHost: {host}:{port}\r\nUser-Agent: duta-cli\r\nAccept: application/json\r\nConnection: close\r\n\r\n"
    );
    http_request(host, port, req.as_bytes(), timeout, false)
}

fn http_get_json_allow_error_body(
    host: &str,
    port: u16,
    path: &str,
    timeout: Duration,
) -> Result<String, String> {
    let req = format!(
        "GET {path} HTTP/1.1\r\nHost: {host}:{port}\r\nUser-Agent: duta-cli\r\nAccept: application/json\r\nConnection: close\r\n\r\n"
    );
    http_request(host, port, req.as_bytes(), timeout, true)
}

fn http_post_json(
    host: &str,
    port: u16,
    path: &str,
    body: &str,
    timeout: Duration,
) -> Result<String, String> {
    let req = format!(
        "POST {path} HTTP/1.1\r\nHost: {host}:{port}\r\nUser-Agent: duta-cli\r\nContent-Type: application/json\r\nAccept: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(), body
    );
    http_request(host, port, req.as_bytes(), timeout, false)
}

fn http_request(
    host: &str,
    port: u16,
    req: &[u8],
    timeout: Duration,
    allow_error_body: bool,
) -> Result<String, String> {
    let mut stream =
        TcpStream::connect((host, port)).map_err(|e| format!("connect {host}:{port}: {e}"))?;
    let _ = stream.set_read_timeout(Some(timeout));
    let _ = stream.set_write_timeout(Some(timeout));
    let _ = stream.set_nonblocking(false);
    stream
        .write_all(req)
        .map_err(|e| format!("write request: {e}"))?;
    stream.flush().map_err(|e| format!("flush request: {e}"))?;
    let _ = stream.shutdown(Shutdown::Write);

    let mut buf = Vec::with_capacity(8192);
    let mut tmp = [0u8; 4096];
    let deadline = Instant::now() + timeout;
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
                if Instant::now() >= deadline {
                    return Err(format!(
                        "read response timeout after {} ms",
                        timeout.as_millis()
                    ));
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(e) => return Err(format!("read response: {e}")),
        }
    }

    let (status, body) = split_http_bytes(&buf)?;
    if !(200..=299).contains(&status) {
        if allow_error_body {
            return Ok(body);
        }
        return Err(format!("HTTP {status}: {body}"));
    }
    Ok(body)
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
    let hend = find_header_end(resp).ok_or("bad HTTP response")?;
    let head =
        std::str::from_utf8(&resp[..hend]).map_err(|e| format!("bad HTTP header utf8: {e}"))?;
    let body_bytes = &resp[hend + 4..];
    let body =
        String::from_utf8(body_bytes.to_vec()).map_err(|e| format!("bad HTTP body utf8: {e}"))?;
    let status = head
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .ok_or("missing HTTP status")?
        .parse::<u16>()
        .map_err(|e| format!("bad HTTP status: {e}"))?;
    Ok((status, body))
}

fn url_enc(s: &str) -> String {
    s.replace('%', "%25")
        .replace('+', "%2B")
        .replace(' ', "%20")
}

#[cfg(test)]
mod tests {
    use super::{command_exit_code, format_response, Cmd};

    #[test]
    fn health_formats_no_peers_actionably() {
        let out = format_response(
            &Cmd::Health,
            r#"{"ok":false,"status":"no_peers","tip_height":0,"sync_progress":1.0,"peer_ready":false}"#,
        );
        assert!(out.contains("daemon not ready"));
        assert!(out.contains("status: no_peers"));
        assert!(out.contains("peer ready: no"));
        assert!(out.contains("sync progress: 100.00%"));
    }

    #[test]
    fn health_formats_syncing_actionably() {
        let out = format_response(
            &Cmd::Health,
            r#"{"ok":false,"status":"syncing","tip_height":128,"sync_progress":0.4740740740740741,"peer_ready":true}"#,
        );
        assert!(out.contains("daemon not ready"));
        assert!(out.contains("status: syncing"));
        assert!(out.contains("peer ready: yes"));
        assert!(out.contains("tip height: 128"));
        assert!(out.contains("sync progress: 47.41%"));
    }

    #[test]
    fn health_formats_ready_actionably() {
        let out = format_response(
            &Cmd::Health,
            r#"{"ok":true,"status":"ready","tip_height":512,"sync_progress":1.0,"peer_ready":true}"#,
        );
        assert!(out.contains("daemon ready"));
        assert!(out.contains("status: ready"));
        assert!(out.contains("peer ready: yes"));
        assert!(out.contains("tip height: 512"));
    }

    #[test]
    fn health_exit_code_is_nonzero_when_not_ready() {
        let exit_code = command_exit_code(
            &Cmd::Health,
            r#"{"ok":false,"status":"no_peers","tip_height":0,"sync_progress":1.0,"peer_ready":false}"#,
        );
        assert_eq!(exit_code, 1);
    }
}
