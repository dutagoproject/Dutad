use duta_core::pow_v4::{
    build_dataset_for_epoch, epoch_number, pow_digest, GLOBAL_DATASET_MEM_MB,
};
use duta_core::types::H32;
use std::fs;
use std::path::PathBuf;
use std::time::Instant;

fn parse_arg(args: &[String], name: &str, default: &str) -> String {
    let key = format!("--{name}");
    args.windows(2)
        .find(|w| w[0] == key)
        .map(|w| w[1].clone())
        .unwrap_or_else(|| default.to_string())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let out_dir = PathBuf::from(parse_arg(&args, "out", "target/bench_v41"));
    let hashes: u64 = parse_arg(&args, "hashes", "32").parse()?;
    let height: u64 = parse_arg(&args, "height", "1").parse()?;

    fs::create_dir_all(&out_dir)?;

    let mut header = [0u8; 80];
    for (i, b) in header.iter_mut().enumerate() {
        *b = (i as u8).wrapping_mul(3).wrapping_add(1);
    }
    let anchor = H32([0x42; 32]);
    let epoch = epoch_number(height);

    let build_start = Instant::now();
    let dataset = build_dataset_for_epoch(epoch, anchor, GLOBAL_DATASET_MEM_MB);
    let build_ms = build_start.elapsed().as_millis();

    fs::write(out_dir.join("dataset.bin"), &dataset)?;
    fs::write(out_dir.join("header80.bin"), header)?;
    fs::write(out_dir.join("anchor32.bin"), anchor.as_bytes())?;

    let verify_start = Instant::now();
    let sample = pow_digest(&header, 42, height, anchor, &dataset);
    let verify_ns = verify_start.elapsed().as_nanos();

    let hash_start = Instant::now();
    let mut last = sample;
    for nonce in 0..hashes {
        last = pow_digest(&header, nonce, height, anchor, &dataset);
    }
    let hash_elapsed = hash_start.elapsed().as_secs_f64();
    let hps = if hash_elapsed > 0.0 {
        hashes as f64 / hash_elapsed
    } else {
        0.0
    };

    let report = serde_json::json!({
        "algo": "dutahash-v4.1",
        "dataset_mb": GLOBAL_DATASET_MEM_MB,
        "scratchpad_bytes": duta_core::pow_v4::SCRATCHPAD_BYTES,
        "steps": duta_core::pow_v4::POW_STEPS,
        "random_dataset_reads_per_step": duta_core::pow_v4::RANDOM_DATASET_READS_PER_STEP,
        "height": height,
        "epoch": epoch,
        "build_dataset_ms": build_ms,
        "single_verify_ns": verify_ns,
        "hashes": hashes,
        "hashes_per_sec": hps,
        "last_digest": last.to_hex(),
    });
    fs::write(
        out_dir.join("cpu_report.json"),
        serde_json::to_vec_pretty(&report)?,
    )?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}
