//! CPUCOIN_POW_V3 reference port from `pow_ref_v3.py`.
//!
//! NOTE: This module is intentionally "boring" and close to the Python reference so
//! consensus stays identical.

use sha3::{Digest, Sha3_256};

use crate::types::H32;

pub const EPOCH_LEN: u64 = 2048;

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
    let a = a.wrapping_add(b) ^ rotl(c, 17);
    let a = a.wrapping_add(rotl(b, 31));
    a ^ rotl(a, 27)
}

pub fn stage_mem_mb(height: u64) -> usize {
    if height < 50_000 {
        return 128;
    }
    if height < 150_000 {
        return 192;
    }
    256
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
    seed_in.extend_from_slice(b"CPUCOIN_DATASET_V3|");
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
        s0 = mix_u64(s0, s1, iu);
        s1 = mix_u64(s1, s2, s0);
        s2 = mix_u64(s2, s3, s1);
        s3 = mix_u64(s3, s0, s2);

        let mut inp = Vec::with_capacity(8 * 5);
        inp.extend_from_slice(&p64le(s0));
        inp.extend_from_slice(&p64le(s1));
        inp.extend_from_slice(&p64le(s2));
        inp.extend_from_slice(&p64le(s3));
        inp.extend_from_slice(&p64le(iu));
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
    seed_in.extend_from_slice(b"CPUCOIN_POW_V3|");
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

    let steps: u64 = 768;

    for t in 0..steps {
        let idx0 = (s0 ^ rotl(s1, 13) ^ t.wrapping_mul(0x9E3779B97F4A7C15u64)) as u128;
        let j0 = (idx0 % (nblocks as u128)) as usize;
        let off0 = j0 * block_size;
        let x0 = u64le(&dataset[off0..off0 + 8]);

        let idx1 = (s2 ^ rotl(s3, 29) ^ x0 ^ t.wrapping_mul(0xD1B54A32D192ED03u64)) as u128;
        let j1 = (idx1 % (nblocks as u128)) as usize;
        let off1 = j1 * block_size;
        let y0 = u64le(&dataset[off1 + 8..off1 + 16]);

        s0 = mix_u64(s0, x0, s2);
        s1 = mix_u64(s1, y0, s3);
        s2 = mix_u64(s2, s0, x0);
        s3 = mix_u64(s3, s1, y0);
    }

    let mut fin_in = Vec::with_capacity(24 + 80 + 8 + 8 + 8 + 32 + 32);
    fin_in.extend_from_slice(b"CPUCOIN_POW_V3_FIN|");
    fin_in.extend_from_slice(header80);
    fin_in.extend_from_slice(&p64le(nonce));
    fin_in.extend_from_slice(&p64le(height));
    fin_in.extend_from_slice(&p64le(ep));
    fin_in.extend_from_slice(anchor_hash32.as_bytes());
    fin_in.extend_from_slice(&p64le(s0));
    fin_in.extend_from_slice(&p64le(s1));
    fin_in.extend_from_slice(&p64le(s2));
    fin_in.extend_from_slice(&p64le(s3));

    H32(sha3_256(&fin_in))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pow_smoke() {
        let header = [0u8; 80];
        let h = 0u64;
        let ep = epoch_number(h);
        let anchor = H32([0xaa; 32]);
        let ds = build_dataset_for_epoch(ep, anchor, 64);
        let d = pow_digest(&header, 1, h, anchor, &ds);
        assert_ne!(d.0, [0u8; 32]);
    }

    #[test]
    fn deterministic_vector_small_dataset() {
        let mut header = [0u8; 80];
        for (i, b) in header.iter_mut().enumerate() {
            *b = i as u8;
        }
        let height = 12_345u64;
        let nonce = 42u64;
        let anchor = H32([0x11; 32]);
        let ds = build_dataset_for_epoch(epoch_number(height), anchor, 1);

        assert_eq!(
            hex::encode(&ds[..32]),
            "285c3b1033926f7a771dbfbdcbc6e0fda6141434c181f1cf71d2a24c46138680"
        );
        assert_eq!(
            pow_digest(&header, nonce, height, anchor, &ds).to_hex(),
            "182d2942fd0acb6ec6a69fad3937bedccb8f6ed53174bbe1c773f064a004ad19"
        );
    }

    #[test]
    fn stage_schedule_stays_monotonic() {
        assert_eq!(stage_mem_mb(0), 128);
        assert_eq!(stage_mem_mb(49_999), 128);
        assert_eq!(stage_mem_mb(50_000), 192);
        assert_eq!(stage_mem_mb(149_999), 192);
        assert_eq!(stage_mem_mb(150_000), 256);
    }
}
