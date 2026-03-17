use crate::{p2p, store, ChainBlock};
use duta_core::dutahash;
use duta_core::types::H32;
use serde_json::json;
use std::collections::HashMap;
use std::fs;
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::Instant;

#[derive(Clone)]
struct DatasetCacheEntry {
    epoch: u64,
    anchor_hash: H32,
    mem_mb: usize,
    dataset: Arc<Vec<u8>>,
}

static DATASET_CACHE: OnceLock<Mutex<Option<DatasetCacheEntry>>> = OnceLock::new();

#[derive(Clone)]
struct SubmitCacheEntry {
    expires_at: u64,
    status: u16,
    body: String,
}

static SUBMIT_CACHE: OnceLock<Mutex<HashMap<String, SubmitCacheEntry>>> = OnceLock::new();
const WORK_ID_HEX_LEN: usize = 64;

fn dataset_cache() -> &'static Mutex<Option<DatasetCacheEntry>> {
    DATASET_CACHE.get_or_init(|| Mutex::new(None))
}

fn submit_cache() -> &'static Mutex<HashMap<String, SubmitCacheEntry>> {
    SUBMIT_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn dataset_cache_lock() -> std::sync::MutexGuard<'static, Option<DatasetCacheEntry>> {
    crate::lock_or_recover(dataset_cache(), "submit_work_dataset_cache")
}

fn submit_cache_lock() -> std::sync::MutexGuard<'static, HashMap<String, SubmitCacheEntry>> {
    crate::lock_or_recover(submit_cache(), "submit_work_cache")
}

fn short_id(id: &str) -> &str {
    if id.len() <= 8 {
        id
    } else {
        &id[..8]
    }
}

fn short_opt_id(id: Option<&str>) -> &str {
    match id {
        Some(v) if !v.is_empty() => short_id(v),
        _ => "-",
    }
}

fn short_opt_prefix(id: Option<&str>, n: usize) -> String {
    match id {
        Some(v) if !v.is_empty() => v.chars().take(n).collect::<String>(),
        _ => "-".to_string(),
    }
}

fn now_ts() -> u64 {
    crate::now_ts()
}

fn verbose_mining_log() -> bool {
    std::env::var_os("DUTA_VERBOSE_MINING_LOG").is_some()
}

fn submit_cache_prune(m: &mut HashMap<String, SubmitCacheEntry>, now: u64) {
    m.retain(|_, v| v.expires_at > now);
}

fn submit_cache_key(work_id: &str, nonce: u64) -> String {
    format!("{}:{:016x}", work_id, nonce)
}

fn cache_submit_result(work_id: &str, nonce: u64, status: u16, body: &str) {
    if work_id.is_empty() {
        return;
    }
    // Keep cached results for 10 minutes.
    let now = now_ts();
    let mut m = submit_cache_lock();
    submit_cache_prune(&mut m, now);
    m.insert(
        submit_cache_key(work_id, nonce),
        SubmitCacheEntry {
            expires_at: now.saturating_add(600),
            status,
            body: body.to_string(),
        },
    );
}

fn extract_u64_field(s: &str, key: &str) -> Option<u64> {
    // Parse patterns like "height=134" or "tip=135" inside detail strings.
    for part in s.split(|c: char| c == ' ' || c == ',' || c == ')' || c == '(') {
        if let Some(rest) = part.strip_prefix(key) {
            return rest.parse::<u64>().ok();
        }
    }
    None
}

fn canonical_submit_reason(detail: &str) -> &'static str {
    if detail.contains("stale_work") || detail.contains("stale_or_out_of_order_block") {
        "stale"
    } else if detail.contains("launch_guard") {
        "launch_guard"
    } else if detail.contains("pow_invalid") {
        "low_difficulty"
    } else if detail.contains("bad_prevhash")
        || detail.contains("out_of_order")
        || detail.contains("work_mismatch")
    {
        "work_mismatch"
    } else if detail.contains("syncing") {
        "syncing"
    } else if detail.contains("busy") {
        "busy"
    } else {
        "invalid"
    }
}

fn submit_http_status(reason: &str) -> u16 {
    match reason {
        "stale" => 410,
        "low_difficulty" => 422,
        "syncing" | "busy" | "launch_guard" => 503,
        _ => 400,
    }
}

fn submit_reason_message(reason: &str) -> String {
    match reason {
        "stale" => {
            "Share rejected: stale share (submitted too late). Please fetch new work and try again."
                .to_string()
        }
        "work_mismatch" => {
            "Share rejected: work mismatch (chain tip changed). Please fetch new work.".to_string()
        }
        "launch_guard" => {
            "Share rejected: node is waiting for official chain sync. Please retry after the node is aligned."
                .to_string()
        }
        "syncing" => {
            "Share rejected: node is syncing. Please retry after the node catches up.".to_string()
        }
        "busy" => "Share rejected: node is busy. Please retry shortly.".to_string(),
        "low_difficulty" => "Share rejected: low difficulty (does not meet target).".to_string(),
        _ => "Share rejected: invalid share.".to_string(),
    }
}

fn submit_reject_body(detail: &str) -> (u16, String) {
    let reason = canonical_submit_reason(detail);
    let user_detail = if reason == "launch_guard" {
        crate::p2p::launch_guard_user_detail(detail).to_string()
    } else {
        detail.to_string()
    };
    let mut obj = json!({
        "status":"rejected",
        "reason":reason,
        "reject_reason":reason,
        "message":submit_reason_message(reason),
        "error":"accept_failed",
        "detail":user_detail
    });

    if let Some(hs) = extract_u64_field(detail, "height=") {
        obj["height_submitted"] = json!(hs);
    }
    if let Some(tip) = extract_u64_field(detail, "tip=") {
        obj["tip"] = json!(tip);
    }

    (submit_http_status(reason), obj.to_string())
}

fn work_id_is_valid(work_id: &str) -> bool {
    work_id.len() == WORK_ID_HEX_LEN && work_id.bytes().all(|b| b.is_ascii_hexdigit())
}

fn parse_submit_payload(v: &serde_json::Value) -> Result<(&str, u64), &'static str> {
    let work_id = v
        .get("work_id")
        .and_then(|x| x.as_str())
        .ok_or("missing_work_id")?;
    if !work_id_is_valid(work_id) {
        return Err("invalid_work_id");
    }
    let nonce = v
        .get("nonce")
        .and_then(|x| x.as_u64())
        .ok_or("missing_nonce")?;
    Ok((work_id, nonce))
}

fn get_dataset_cached(epoch: u64, anchor_hash: H32, mem_mb: usize) -> Arc<Vec<u8>> {
    // Fast path: hit.
    {
        let g = dataset_cache_lock();
        if let Some(ent) = g.as_ref() {
            if ent.epoch == epoch && ent.anchor_hash == anchor_hash && ent.mem_mb == mem_mb {
                return ent.dataset.clone();
            }
        }
    }

    // Miss: build without holding the lock (expensive).
    let ds = Arc::new(dutahash::build_dataset_for_epoch(
        epoch,
        anchor_hash,
        mem_mb,
    ));

    // Store (double-check in case another thread built it).
    let mut g = dataset_cache_lock();
    if let Some(ent) = g.as_ref() {
        if ent.epoch == epoch && ent.anchor_hash == anchor_hash && ent.mem_mb == mem_mb {
            return ent.dataset.clone();
        }
    }
    *g = Some(DatasetCacheEntry {
        epoch,
        anchor_hash,
        mem_mb,
        dataset: ds.clone(),
    });
    ds
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

fn write_mempool_value(data_dir: &str, v: &serde_json::Value) -> Result<(), String> {
    let path = format!("{}/mempool.json", data_dir.trim_end_matches('/'));
    let body = serde_json::to_string_pretty(v).map_err(|e| format!("json_encode_failed: {}", e))?;
    store::durable_write_string(&path, &body).map_err(|e| format!("write_failed: {}", e))?;
    Ok(())
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

fn net_from_datadir(data_dir: &str) -> duta_core::netparams::Network {
    if let Some(net) = store::read_datadir_network(data_dir) {
        net
    } else if data_dir.contains("testnet") {
        duta_core::netparams::Network::Testnet
    } else if data_dir.contains("stagenet") {
        duta_core::netparams::Network::Stagenet
    } else {
        duta_core::netparams::Network::Mainnet
    }
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
                obj.remove("fee");
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
            obj.remove("fee");
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

pub(crate) fn accept_mined_block(
    data_dir: &str,
    mined_block: &ChainBlock,
) -> Result<serde_json::Value, String> {
    accept_mined_block_with_source(data_dir, mined_block, false)
}

pub(crate) fn accept_mined_block_with_source(
    data_dir: &str,
    mined_block: &ChainBlock,
    official_stratum_source: bool,
) -> Result<serde_json::Value, String> {
    let net = net_from_datadir(data_dir);
    if !official_stratum_source {
        p2p::launch_guard_local_submit_ready(
            net,
            mined_block.height,
            &mined_block.hash32,
            mined_block.bits,
        )?;
    }
    store::note_accepted_block(data_dir, mined_block)?;
    p2p::note_local_tip_height(mined_block.height);

    let mut mp = read_mempool_value(data_dir);
    if mp.get("txids").and_then(|x| x.as_array()).is_some()
        && mp.get("txs").and_then(|x| x.as_object()).is_some()
    {
        if let Some(txs_val) = mined_block.txs.as_ref() {
            if let Some(txs_obj) = txs_val.as_object() {
                for txid in txs_obj.keys() {
                    mp["txs"].as_object_mut().map(|o| {
                        o.remove(txid);
                    });
                }
                if let Some(txs_left) = mp["txs"].as_object() {
                    let mut keep: Vec<serde_json::Value> = Vec::new();
                    if let Some(old) = mp["txids"].as_array() {
                        for v in old {
                            if let Some(t) = v.as_str() {
                                if txs_left.contains_key(t) {
                                    keep.push(json!(t));
                                }
                            }
                        }
                    }
                    for k in txs_left.keys() {
                        if !keep.iter().any(|v| v.as_str() == Some(k)) {
                            keep.push(json!(k));
                        }
                    }
                    mp["txids"] = serde_json::Value::Array(keep);
                }
            }
        }
        let _ = write_mempool_value(data_dir, &mp);
    }

    edlog!(
        "[dutad] BLOCK_ACCEPTED height={} hash={} prev={} bits={} miner={}",
        mined_block.height,
        short_id(&mined_block.hash32),
        short_opt_id(mined_block.prevhash32.as_deref()),
        mined_block.bits,
        short_opt_prefix(mined_block.miner.as_deref(), 16),
    );
    let block_for_broadcast = mined_block.clone();
    thread::spawn(move || {
        p2p::broadcast_block(&block_for_broadcast);
    });
    Ok(json!({
        "ok":true,
        "status":"accepted",
        "message":"Share accepted.",
        "height": mined_block.height,
        "hash32": mined_block.hash32
    }))
}

fn submit_source_is_official_stratum(request: &tiny_http::Request) -> bool {
    request.headers().iter().any(|h| {
        h.field.equiv("X-DUTA-Work-Source")
            && h.value.as_str().eq_ignore_ascii_case("official-stratum")
    })
}

pub(crate) fn build_mined_block_from_work_nonce(
    work_id: &str,
    nonce: u64,
    consume: bool,
) -> Result<ChainBlock, String> {
    let item = crate::work::peek_work(work_id).ok_or_else(|| "stale_work".to_string())?;

    let anchor_hash = H32::from_hex(&item.anchor_hash32).unwrap_or_else(H32::zero);
    let dataset = get_dataset_cached(item.epoch, anchor_hash, item.mem_mb);
    let d = dutahash::pow_digest(
        &item.header,
        nonce,
        item.height,
        anchor_hash,
        dataset.as_slice(),
    );
    if leading_zero_bits(&d) < (item.bits as u32) {
        return Err("pow_invalid".to_string());
    }

    if consume && crate::work::take_work(work_id).is_none() {
        return Err("stale_work".to_string());
    }

    let pow_digest32 = d.to_hex();
    let hash32 = pow_digest32.clone();
    let block_v = json!({
        "height": item.height,
        "hash32": hash32,
        "bits": item.bits,
        "chainwork": item.chainwork,
        "timestamp": item.timestamp,
        "prevhash32": item.prevhash32,
        "merkle32": item.merkle32,
        "nonce": nonce,
        "miner": item.miner,
        "pow_digest32": pow_digest32,
        "txs": item.txs_obj,
    });

    serde_json::from_value(block_v).map_err(|e| format!("block_decode_failed: {}", e))
}

pub fn handle_submit_work(
    mut request: tiny_http::Request,
    data_dir: &str,
    respond_json: fn(tiny_http::Request, tiny_http::StatusCode, String),
) {
    let official_stratum_source = submit_source_is_official_stratum(&request);
    if request.method() != &tiny_http::Method::Post {
        wlog!("[dutad] SUBMIT_REJECT reason=method_not_allowed");
        crate::respond_error(request, tiny_http::StatusCode(405), "method_not_allowed");
        return;
    }

    if !crate::request_content_type_is_json(&request) {
        crate::respond_415(request);
        return;
    }

    let started = Instant::now();

    let body = match crate::read_body_limited(&mut request) {
        Ok(b) => b,
        Err(e) => {
            wlog!(
                "[dutad] SUBMIT_REJECT reason=payload_too_large detail={}",
                e
            );
            crate::respond_error_detail(
                request,
                tiny_http::StatusCode(413),
                "payload_too_large",
                json!({"detail":e,"max_body_bytes":crate::MAX_RPC_BODY_BYTES}),
            );
            return;
        }
    };

    let v: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            wlog!("[dutad] SUBMIT_REJECT reason=invalid_json detail={}", e);
            crate::respond_error_detail(
                request,
                tiny_http::StatusCode(400),
                "invalid_json",
                json!({"detail":format!("{}", e)}),
            );
            return;
        }
    };

    let (work_id, nonce) = match parse_submit_payload(&v) {
        Ok(parsed) => parsed,
        Err(code) => {
            wlog!("[dutad] SUBMIT_REJECT reason={}", code);
            crate::respond_error(request, tiny_http::StatusCode(400), code);
            return;
        }
    };
    if verbose_mining_log() {
        wlog!("[dutad] SUBMIT work={} nonce={}", short_id(work_id), nonce);
    }

    // Only exact retries of the same work+nonce should hit the idempotency cache.
    {
        let now = now_ts();
        let mut m = submit_cache_lock();
        submit_cache_prune(&mut m, now);
        if let Some(ent) = m.get(&submit_cache_key(work_id, nonce)) {
            wlog!(
                "[dutad] SUBMIT_DUP work={} nonce={} status={}",
                short_id(work_id),
                nonce,
                ent.status
            );
            respond_json(request, tiny_http::StatusCode(ent.status), ent.body.clone());
            return;
        }
    }

    let mined_block = match build_mined_block_from_work_nonce(work_id, nonce, true) {
        Ok(v) => v,
        Err(e) if e == "stale_work" => {
            let body = json!({
                "status":"rejected",
                "reason":"stale",
                "reject_reason":"stale",
                "message":"Share rejected: stale share (submitted too late). Please fetch new work and try again.",
                "error":"stale_work"
            }).to_string();
            wlog!(
                "[dutad] SUBMIT_REJECT work={} reason=stale_work",
                short_id(work_id)
            );
            cache_submit_result(work_id, nonce, 410, &body);
            respond_json(request, tiny_http::StatusCode(410), body);
            return;
        }
        Err(e) if e == "pow_invalid" => {
            let body = json!({
                "status":"rejected",
                "reason":"low_difficulty",
                "reject_reason":"low_difficulty",
                "message":"Share rejected: low difficulty (does not meet target).",
                "error":"pow_invalid"
            })
            .to_string();
            wlog!(
                "[dutad] SUBMIT_REJECT work={} reason=low_difficulty",
                short_id(work_id)
            );
            cache_submit_result(work_id, nonce, 422, &body);
            respond_json(request, tiny_http::StatusCode(422), body);
            return;
        }
        Err(e) => {
            let detail = e.clone();
            let reason = canonical_submit_reason(&detail);
            let (status, body) = submit_reject_body(&detail);
            wlog!(
                "[dutad] SUBMIT_REJECT work={} reason={} detail={}",
                short_id(work_id),
                reason,
                detail
            );
            cache_submit_result(work_id, nonce, status, &body);
            respond_json(request, tiny_http::StatusCode(status), body);
            return;
        }
    };

    let accepted =
        match accept_mined_block_with_source(data_dir, &mined_block, official_stratum_source) {
            Ok(v) => v,
            Err(e) => {
                let detail = e.clone();
                let reason = canonical_submit_reason(&detail);
                let (status, body) = submit_reject_body(&detail);
                edlog!(
                    "[dutad] BLOCK_REJECT work={} reason={} detail={}",
                    short_id(work_id),
                    reason,
                    detail
                );
                cache_submit_result(work_id, nonce, status, &body);
                respond_json(request, tiny_http::StatusCode(status), body);
                return;
            }
        };

    let body = accepted.to_string();

    if verbose_mining_log() {
        wlog!(
            "[dutad] SUBMIT_OK work={} elapsed_ms={}",
            short_id(work_id),
            started.elapsed().as_millis()
        );
    }
    cache_submit_result(work_id, nonce, 200, &body);

    respond_json(request, tiny_http::StatusCode(200), body);
}

#[cfg(test)]
mod tests {
    use super::{
        build_mined_block_from_work_nonce, canonical_submit_reason, parse_submit_payload,
        submit_cache_key, submit_http_status, submit_reason_message, submit_reject_body,
        work_id_is_valid,
    };
    use duta_core::types::H32;
    use serde_json::json;

    #[test]
    fn work_id_validation_requires_fixed_hex_shape() {
        assert!(work_id_is_valid(
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        ));
        assert!(!work_id_is_valid("short"));
        assert!(!work_id_is_valid(
            "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"
        ));
    }

    #[test]
    fn submit_payload_rejects_missing_or_invalid_fields() {
        assert_eq!(parse_submit_payload(&json!({})), Err("missing_work_id"));
        assert_eq!(
            parse_submit_payload(&json!({"work_id":"abcd","nonce":1})),
            Err("invalid_work_id")
        );
        assert_eq!(
            parse_submit_payload(&json!({
                "work_id":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
            })),
            Err("missing_nonce")
        );
    }

    #[test]
    fn submit_payload_accepts_valid_request_shape() {
        let payload = json!({
            "work_id":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            "nonce": 42
        });
        let (work_id, nonce) = parse_submit_payload(&payload).expect("valid payload");
        assert_eq!(
            work_id,
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        );
        assert_eq!(nonce, 42);
    }

    #[test]
    fn submit_cache_key_is_nonce_scoped() {
        let work_id = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        assert_ne!(submit_cache_key(work_id, 1), submit_cache_key(work_id, 2));
    }

    #[test]
    fn submit_reason_mapping_matches_expected_statuses() {
        assert_eq!(canonical_submit_reason("stale_work"), "stale");
        assert_eq!(
            canonical_submit_reason(
                "launch_guard_official_height_mismatch tip_height=50 official_min_height=49"
            ),
            "launch_guard"
        );
        assert_eq!(canonical_submit_reason("pow_invalid"), "low_difficulty");
        assert_eq!(canonical_submit_reason("bad_prevhash"), "work_mismatch");
        assert_eq!(canonical_submit_reason("syncing tip_height=9"), "syncing");
        assert_eq!(canonical_submit_reason("busy"), "busy");
        assert_eq!(submit_http_status("stale"), 410);
        assert_eq!(submit_http_status("low_difficulty"), 422);
        assert_eq!(submit_http_status("syncing"), 503);
        assert_eq!(submit_http_status("launch_guard"), 503);
    }

    #[test]
    fn submit_reject_body_carries_reason_message_and_height_context() {
        let (status, body) = submit_reject_body("stale_or_out_of_order_block height=11 tip=12");
        let parsed: serde_json::Value = serde_json::from_str(&body).expect("valid reject json");
        assert_eq!(status, 410);
        assert_eq!(parsed.get("reason").and_then(|v| v.as_str()), Some("stale"));
        assert_eq!(
            parsed.get("height_submitted").and_then(|v| v.as_u64()),
            Some(11)
        );
        assert_eq!(parsed.get("tip").and_then(|v| v.as_u64()), Some(12));
        assert_eq!(
            parsed.get("message").and_then(|v| v.as_str()),
            Some(submit_reason_message("stale").as_str())
        );
    }

    #[test]
    fn low_difficulty_submit_does_not_consume_work_item() {
        let work_id = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let header = [0u8; 80];
        crate::work::insert_test_work(
            work_id,
            crate::work::WorkItem {
                expires_at: crate::now_ts().saturating_add(60),
                height: 1,
                prevhash32: "00".repeat(32),
                merkle32: "11".repeat(32),
                timestamp: 1,
                bits: 32,
                chainwork: 1,
                miner: "miner".to_string(),
                work_scope: "miner".to_string(),
                txs_obj: json!({}),
                header,
                anchor_hash32: H32::zero().to_hex(),
                epoch: 0,
                mem_mb: 1,
            },
        );

        let err = build_mined_block_from_work_nonce(work_id, 0, true).unwrap_err();
        assert_eq!(err, "pow_invalid");
        assert!(crate::work::peek_work(work_id).is_some());
    }
}
