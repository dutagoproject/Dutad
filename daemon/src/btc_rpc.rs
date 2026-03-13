use crate::get_param;
use crate::{store, ChainBlock};
use duta_core::netparams::{genesis_hash, pow_start_bits, Network};
use serde_json::json;

fn ok(result: serde_json::Value) -> String {
    json!({"result": result, "error": null, "id": null}).to_string()
}

fn err(code: i64, message: &str) -> String {
    json!({
        "result": null,
        "error": {"code": code, "message": message},
        "id": null
    })
    .to_string()
}

fn parse_path_u64(path: &str, prefix: &str) -> Option<u64> {
    let p = path.strip_prefix(prefix)?;
    let p = p.strip_prefix('/')?;
    if p.is_empty() {
        return None;
    }
    let seg = p.split('/').next().unwrap_or("");
    seg.parse::<u64>().ok()
}

fn parse_path_str(path: &str, prefix: &str) -> Option<String> {
    let p = path.strip_prefix(prefix)?;
    let p = p.strip_prefix('/')?;
    if p.is_empty() {
        return None;
    }
    Some(p.split('/').next().unwrap_or("").to_string())
}

fn network_from_data_dir(data_dir: &str) -> Network {
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

fn canonical_genesis_block(data_dir: &str) -> ChainBlock {
    let net = network_from_data_dir(data_dir);
    ChainBlock {
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

fn block_hash_at_height(data_dir: &str, height: u64) -> Option<String> {
    if height == 0 {
        return Some(canonical_genesis_block(data_dir).hash32);
    }
    crate::store::block_at(data_dir, height).map(|b| b.hash32)
}

fn block_by_height(data_dir: &str, height: u64) -> Option<ChainBlock> {
    if height == 0 {
        return Some(canonical_genesis_block(data_dir));
    }
    crate::store::block_at(data_dir, height)
}

pub fn handle_getblockcount(
    request: tiny_http::Request,
    data_dir: &str,
    respond: fn(tiny_http::Request, tiny_http::StatusCode, String),
) {
    let (h, _hash, _cw, _bits) =
        crate::store::tip_fields(data_dir).unwrap_or((0, "0".repeat(64), 0, 0));
    respond(request, tiny_http::StatusCode(200), ok(json!(h)));
}

pub fn handle_getbestblockhash(
    request: tiny_http::Request,
    data_dir: &str,
    respond: fn(tiny_http::Request, tiny_http::StatusCode, String),
) {
    let (_h, hash, _cw, _bits) =
        crate::store::tip_fields(data_dir).unwrap_or((0, "0".repeat(64), 0, 0));
    respond(request, tiny_http::StatusCode(200), ok(json!(hash)));
}

pub fn handle_getblockhash(
    request: tiny_http::Request,
    data_dir: &str,
    path: &str,
    params: &[(String, String)],
    respond: fn(tiny_http::Request, tiny_http::StatusCode, String),
) {
    let height = get_param(params, "height")
        .and_then(|s| s.parse::<u64>().ok())
        .or_else(|| parse_path_u64(path, "/getblockhash"));

    let height = match height {
        Some(h) => h,
        None => {
            respond(
                request,
                tiny_http::StatusCode(400),
                err(-32602, "missing_height"),
            );
            return;
        }
    };

    match block_hash_at_height(data_dir, height) {
        Some(hash32) => respond(request, tiny_http::StatusCode(200), ok(json!(hash32))),
        None => respond(
            request,
            tiny_http::StatusCode(404),
            err(-5, "block_not_found"),
        ),
    }
}

pub fn handle_getblock(
    request: tiny_http::Request,
    data_dir: &str,
    path: &str,
    params: &[(String, String)],
    respond: fn(tiny_http::Request, tiny_http::StatusCode, String),
) {
    let hash = get_param(params, "hash")
        .map(|s| s.to_string())
        .or_else(|| parse_path_str(path, "/getblock"));
    let height = get_param(params, "height").and_then(|s| s.parse::<u64>().ok());
    let verbosity = get_param(params, "verbosity")
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(1);

    let h_opt: Option<u64> = if let Some(h) = height {
        Some(h)
    } else if let Some(ref hash32) = hash {
        crate::store::height_by_hash(data_dir, hash32)
    } else {
        None
    };

    let h = match h_opt {
        Some(h) => h,
        None => {
            respond(
                request,
                tiny_http::StatusCode(400),
                err(-32602, "missing_hash_or_height"),
            );
            return;
        }
    };

    let b = match block_by_height(data_dir, h) {
        Some(b) => b,
        None => {
            respond(
                request,
                tiny_http::StatusCode(404),
                err(-5, "block_not_found"),
            );
            return;
        }
    };

    if verbosity <= 0 {
        respond(request, tiny_http::StatusCode(200), ok(json!(b.hash32)));
        return;
    }

    respond(
        request,
        tiny_http::StatusCode(200),
        ok(serde_json::to_value(b).unwrap_or(json!({}))),
    );
}

#[cfg(test)]
mod tests {
    use super::{block_by_height, block_hash_at_height};
    use crate::store;

    fn temp_datadir(tag: &str) -> String {
        let mut p = std::env::temp_dir();
        let uniq = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        p.push(format!("duta-btc-rpc-test-{}-{}", tag, uniq));
        std::fs::create_dir_all(&p).unwrap();
        p.to_string_lossy().to_string()
    }

    #[test]
    fn genesis_hash_is_served_for_height_zero_on_db_only_node() {
        let data_dir = temp_datadir("genesis-hash");
        store::ensure_datadir_meta(&data_dir, "mainnet").unwrap();
        store::bootstrap(&data_dir).unwrap();

        let hash = block_hash_at_height(&data_dir, 0).expect("genesis hash");
        assert_eq!(hash, duta_core::netparams::genesis_hash(duta_core::netparams::Network::Mainnet));
    }

    #[test]
    fn genesis_block_is_served_for_height_zero_on_db_only_node() {
        let data_dir = temp_datadir("genesis-block");
        store::ensure_datadir_meta(&data_dir, "mainnet").unwrap();
        store::bootstrap(&data_dir).unwrap();

        let block = block_by_height(&data_dir, 0).expect("genesis block");
        assert_eq!(block.height, 0);
        assert_eq!(
            block.hash32,
            duta_core::netparams::genesis_hash(duta_core::netparams::Network::Mainnet)
        );
        assert_eq!(
            block.bits,
            duta_core::netparams::pow_start_bits(duta_core::netparams::Network::Mainnet)
        );
    }
}

pub fn handle_getmininginfo(
    request: tiny_http::Request,
    data_dir: &str,
    net: &str,
    respond: fn(tiny_http::Request, tiny_http::StatusCode, String),
) {
    let (h, _hash, _cw, bits) =
        crate::store::tip_fields(data_dir).unwrap_or((0, "0".repeat(64), 0, 0));
    let next_bits = crate::store::expected_bits_next(data_dir).unwrap_or(bits);

    let v = json!({
        "blocks": h,
        "difficulty_bits": bits,
        "next_bits": next_bits,
        "network": net,
        "errors": "",
        "warnings": ""
    });

    respond(request, tiny_http::StatusCode(200), ok(v));
}

pub fn handle_getinfo(
    request: tiny_http::Request,
    data_dir: &str,
    net: &str,
    respond: fn(tiny_http::Request, tiny_http::StatusCode, String),
) {
    let (h, hash, cw, bits) =
        crate::store::tip_fields(data_dir).unwrap_or((0, "0".repeat(64), 0, 0));
    let next_bits = crate::store::expected_bits_next(data_dir).unwrap_or(bits);

    let v = json!({
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
        "warnings": ""
    });

    respond(request, tiny_http::StatusCode(200), ok(v));
}
