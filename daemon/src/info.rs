use serde_json::json;

pub fn info_json(
    data_dir: &str,
    net: &str,
    tip_json: fn(&str) -> String,
    mempool_json: fn(&str) -> String,
) -> String {
    let tip = serde_json::from_str::<serde_json::Value>(&tip_json(data_dir)).unwrap_or(json!({}));
    let mpv = serde_json::from_str::<serde_json::Value>(&mempool_json(data_dir))
        .unwrap_or(json!({"txids":[]}));
    let mp_n = mpv
        .get("txids")
        .and_then(|x| x.as_array())
        .map(|a| a.len())
        .unwrap_or(0);

    json!({
        "net": net,
        "tip": tip,
        "mempool_txids": mp_n,
        "version": env!("CARGO_PKG_VERSION")
    })
    .to_string()
}
