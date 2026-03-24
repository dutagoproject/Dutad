//! CPUCOIN_POW_V4 mandatory-fork PoW.
//!
//! V4 intentionally changes both dataset derivation and the final digest path so
//! pre-fork miners cannot accidentally remain valid after activation.

use sha3::{Digest, Sha3_256};

use crate::types::H32;

pub const EPOCH_LEN: u64 = 2048;
pub const GLOBAL_DATASET_MEM_MB: usize = 256;
pub const SCRATCHPAD_BYTES: usize = 256 * 1024;
pub const POW_STEPS: u64 = 4096;
pub const RANDOM_DATASET_READS_PER_STEP: usize = 4;

#[inline]
fn sha3_256(data: &[u8]) -> [u8; 32] {
    let mut h = Sha3_256::new();
    h.update(data);
    let out = h.finalize();
    let mut a = [0u8; 32];
    a.copy_from_slice(&out);
    a
}

#[inline]
fn u64le(b: &[u8]) -> u64 {
    let mut a = [0u8; 8];
    a.copy_from_slice(&b[0..8]);
    u64::from_le_bytes(a)
}

#[inline]
fn p64le(x: u64) -> [u8; 8] {
    x.to_le_bytes()
}

#[inline]
fn rotl(x: u64, r: u32) -> u64 {
    x.rotate_left(r)
}

#[inline]
fn mix_u64(a: u64, b: u64, c: u64) -> u64 {
    let z = a
        .wrapping_add(rotl(b ^ c, 19))
        .wrapping_mul(0x9E37_79B9_7F4A_7C15);
    let y = z ^ rotl(a.wrapping_add(c), 27);
    y.wrapping_add(rotl(b, 41))
}

pub fn stage_mem_mb(height: u64) -> usize {
    let _ = height;
    GLOBAL_DATASET_MEM_MB
}

#[inline]
pub fn epoch_number(height: u64) -> u64 {
    height / EPOCH_LEN
}

#[inline]
pub fn anchor_height(height: u64) -> u64 {
    epoch_number(height) * EPOCH_LEN
}

pub fn build_dataset_for_epoch(epoch: u64, anchor_hash32: H32, mem_mb: usize) -> Vec<u8> {
    let m = mem_mb * 1024 * 1024;
    let block_size = 64usize;
    let nblocks = m / block_size;

    let mut seed_in = Vec::with_capacity(16 + 8 + 32);
    seed_in.extend_from_slice(b"CPUCOIN_DATASET_V4|");
    seed_in.extend_from_slice(&p64le(epoch));
    seed_in.extend_from_slice(anchor_hash32.as_bytes());
    let seed = sha3_256(&seed_in);

    let mut s0 = u64le(&seed[0..8]);
    let mut s1 = u64le(&seed[8..16]);
    let mut s2 = u64le(&seed[16..24]);
    let mut s3 = u64le(&seed[24..32]);

    let mut ds = vec![0u8; m];

    for i in 0..nblocks {
        let iu = i as u64;
        s0 = mix_u64(s0 ^ iu, s1, s3);
        s1 = mix_u64(s1, s2 ^ iu.rotate_left(7), s0);
        s2 = mix_u64(s2, s3, s1 ^ iu.rotate_left(13));
        s3 = mix_u64(s3 ^ iu, s0, s2);

        let mut inp = Vec::with_capacity(8 * 6);
        inp.extend_from_slice(&p64le(s0));
        inp.extend_from_slice(&p64le(s1));
        inp.extend_from_slice(&p64le(s2));
        inp.extend_from_slice(&p64le(s3));
        inp.extend_from_slice(&p64le(iu));
        inp.extend_from_slice(anchor_hash32.as_bytes());
        let h1 = sha3_256(&inp);
        let h2 = sha3_256(&h1);
        let off = i * block_size;
        ds[off..off + 32].copy_from_slice(&h1);
        ds[off + 32..off + 64].copy_from_slice(&h2);
    }

    ds
}

pub fn pow_digest(
    header80: &[u8; 80],
    nonce: u64,
    height: u64,
    anchor_hash32: H32,
    dataset: &[u8],
) -> H32 {
    let block_size = 64usize;
    let nblocks = dataset.len() / block_size;
    let ep = epoch_number(height);

    let mut seed_in = Vec::with_capacity(16 + 80 + 8 + 8 + 8 + 32);
    seed_in.extend_from_slice(b"CPUCOIN_POW_V4|");
    seed_in.extend_from_slice(header80);
    seed_in.extend_from_slice(&p64le(nonce));
    seed_in.extend_from_slice(&p64le(height));
    seed_in.extend_from_slice(&p64le(ep));
    seed_in.extend_from_slice(anchor_hash32.as_bytes());
    let seed = sha3_256(&seed_in);

    let mut s0 = u64le(&seed[0..8]);
    let mut s1 = u64le(&seed[8..16]);
    let mut s2 = u64le(&seed[16..24]);
    let mut s3 = u64le(&seed[24..32]);
    let mut lane = s0 ^ s2;
    let scratch_words = (SCRATCHPAD_BYTES / 8).max(1);
    let mut scratch = vec![0u64; scratch_words];

    {
        let mut fill = Vec::with_capacity(16 + 80 + 8 + 8 + 32);
        fill.extend_from_slice(b"CPUCOIN_POW_V4_SCR|");
        fill.extend_from_slice(header80);
        fill.extend_from_slice(&p64le(nonce));
        fill.extend_from_slice(&p64le(height));
        fill.extend_from_slice(anchor_hash32.as_bytes());
        let mut seed_block = sha3_256(&fill);
        for i in 0..scratch_words {
            if i & 3 == 0 {
                let mut next = Vec::with_capacity(32 + 8 + 8);
                next.extend_from_slice(&seed_block);
                next.extend_from_slice(&p64le(i as u64));
                next.extend_from_slice(&p64le(ep));
                seed_block = sha3_256(&next);
            }
            let off = (i & 3) * 8;
            scratch[i] = u64le(&seed_block[off..off + 8]) ^ (i as u64).wrapping_mul(0x9E37_79B9);
        }
    }

    for t in 0..POW_STEPS {
        let idx0 = (lane ^ rotl(s1, 11) ^ t.wrapping_mul(0x9E37_79B9_7F4A_7C15u64)) as u128;
        let j0 = (idx0 % (nblocks as u128)) as usize;
        let off0 = j0 * block_size;
        let x0 = u64le(&dataset[off0..off0 + 8]);
        let x1 = u64le(&dataset[off0 + 16..off0 + 24]);

        let idx1 = (s2 ^ rotl(s3, 29) ^ x0 ^ x1 ^ lane.wrapping_mul(0xD1B5_4A32_D192_ED03u64))
            as u128;
        let j1 = (idx1 % (nblocks as u128)) as usize;
        let off1 = j1 * block_size;
        let y0 = u64le(&dataset[off1 + 8..off1 + 16]);
        let y1 = u64le(&dataset[off1 + 24..off1 + 32]);

        let sp_idx0 = ((lane ^ s0 ^ x0) as usize) % scratch_words;
        let sp_idx1 = ((rotl(s1, 9) ^ y0 ^ t) as usize) % scratch_words;
        let sp_idx2 = ((rotl(s2, 17) ^ x1 ^ lane) as usize) % scratch_words;
        let sp_idx3 = ((rotl(s3, 27) ^ y1 ^ s0) as usize) % scratch_words;

        let sp0 = scratch[sp_idx0];
        let sp1 = scratch[sp_idx1];
        let sp2 = scratch[sp_idx2];
        let sp3 = scratch[sp_idx3];

        let mut extra = 0u64;
        for r in 0..RANDOM_DATASET_READS_PER_STEP {
            let mix = match r {
                0 => lane ^ sp0 ^ s2,
                1 => s0 ^ sp1 ^ y1,
                2 => s1 ^ sp2 ^ x0,
                _ => s3 ^ sp3 ^ x1,
            };
            let idx = (mix
                ^ (t.wrapping_add(r as u64)).wrapping_mul(0x94D0_49BB_1331_11EBu64))
                as u128;
            let j = (idx % (nblocks as u128)) as usize;
            let off = j * block_size;
            let lo = u64le(&dataset[off..off + 8]);
            let hi = u64le(&dataset[off + 32..off + 40]);
            extra ^= mix_u64(lo, hi, mix ^ (r as u64));
        }

        lane = mix_u64(lane ^ x0 ^ sp0, y1 ^ sp1, t ^ s3 ^ extra);
        s0 = mix_u64(s0 ^ x0 ^ extra, y0 ^ sp2, lane);
        s1 = mix_u64(s1 ^ sp1, x1 ^ lane ^ extra, s0);
        s2 = mix_u64(s2 ^ y0 ^ sp3, s0 ^ extra, x1);
        s3 = mix_u64(s3 ^ y1 ^ extra, s1 ^ sp0, lane);

        scratch[sp_idx0] = mix_u64(sp0 ^ x0, lane, extra);
        scratch[sp_idx1] = mix_u64(sp1 ^ y0, s0, lane);
        scratch[sp_idx2] = mix_u64(sp2 ^ x1, s1, extra);
        scratch[sp_idx3] = mix_u64(sp3 ^ y1, s2, s3);
    }

    let mut fin_in = Vec::with_capacity(24 + 80 + 8 + 8 + 8 + 32 + 40);
    fin_in.extend_from_slice(b"CPUCOIN_POW_V4_FIN|");
    fin_in.extend_from_slice(header80);
    fin_in.extend_from_slice(&p64le(nonce));
    fin_in.extend_from_slice(&p64le(height));
    fin_in.extend_from_slice(&p64le(ep));
    fin_in.extend_from_slice(anchor_hash32.as_bytes());
    fin_in.extend_from_slice(&p64le(lane));
    fin_in.extend_from_slice(&p64le(s0));
    fin_in.extend_from_slice(&p64le(s1));
    fin_in.extend_from_slice(&p64le(s2));
    fin_in.extend_from_slice(&p64le(s3));
    fin_in.extend_from_slice(&p64le(scratch[0]));
    fin_in.extend_from_slice(&p64le(scratch[scratch_words / 2]));
    fin_in.extend_from_slice(&p64le(scratch[scratch_words - 1]));

    H32(sha3_256(&fin_in))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pow_v4_smoke() {
        let header = [0u8; 80];
        let h = 0u64;
        let ep = epoch_number(h);
        let anchor = H32([0xaa; 32]);
        let ds = build_dataset_for_epoch(ep, anchor, 1);
        let d = pow_digest(&header, 1, h, anchor, &ds);
        assert_ne!(d.0, [0u8; 32]);
    }

    #[test]
    fn v4_vectors_differ_from_v3() {
        let mut header = [0u8; 80];
        for (i, b) in header.iter_mut().enumerate() {
            *b = i as u8;
        }
        let height = 12_345u64;
        let nonce = 42u64;
        let anchor = H32([0x11; 32]);
        let ds = build_dataset_for_epoch(epoch_number(height), anchor, 1);
        assert_eq!(
            pow_digest(&header, nonce, height, anchor, &ds).to_hex(),
            "758770774ab2bbb9a3ac22fe0c7b93eca3cfcc554d4557a24bd49bb10ce90ae2"
        );
        assert_ne!(
            pow_digest(&header, nonce, height, anchor, &ds).to_hex(),
            crate::pow_v3::pow_digest(&header, nonce, height, anchor, &ds).to_hex()
        );
    }

    #[test]
    fn v4_stage_mem_is_fixed_at_256mb() {
        assert_eq!(stage_mem_mb(0), GLOBAL_DATASET_MEM_MB);
        assert_eq!(stage_mem_mb(50_000), GLOBAL_DATASET_MEM_MB);
        assert_eq!(stage_mem_mb(500_000), GLOBAL_DATASET_MEM_MB);
    }

    #[test]
    fn v4_scratchpad_constants_match_guardrails() {
        assert_eq!(GLOBAL_DATASET_MEM_MB, 256);
        assert_eq!(SCRATCHPAD_BYTES, 256 * 1024);
        assert_eq!(POW_STEPS, 4096);
        assert_eq!(RANDOM_DATASET_READS_PER_STEP, 4);
    }

    #[test]
    #[ignore = "manual benchmark guard for verifier/operator costs"]
    fn bench_v4_build_dataset_256mb_once() {
        let anchor = H32([0x77; 32]);
        let start = std::time::Instant::now();
        let ds = build_dataset_for_epoch(0, anchor, GLOBAL_DATASET_MEM_MB);
        eprintln!(
            "bench_v4_build_dataset_256mb_once bytes={} elapsed_ms={}",
            ds.len(),
            start.elapsed().as_millis()
        );
        assert_eq!(ds.len(), GLOBAL_DATASET_MEM_MB * 1024 * 1024);
    }

    #[test]
    #[ignore = "manual benchmark guard for verifier/operator costs"]
    fn bench_v4_verify_digest_once_with_256mb_dataset() {
        let anchor = H32([0x22; 32]);
        let ds = build_dataset_for_epoch(0, anchor, GLOBAL_DATASET_MEM_MB);
        let header = [0x11; 80];
        let start = std::time::Instant::now();
        let digest = pow_digest(&header, 42, 1, anchor, &ds);
        eprintln!(
            "bench_v4_verify_digest_once_with_256mb_dataset digest={} elapsed_ms={}",
            digest.to_hex(),
            start.elapsed().as_millis()
        );
        assert_ne!(digest.0, [0u8; 32]);
    }
}
