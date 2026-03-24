//! Public-facing DUTA PoW namespace.
//!
//! `dutahash` is the preferred public API for consumers.
//! The underlying consensus implementations live in `crate::pow_v3` and `crate::pow_v4`.

pub use crate::pow_v3::{
    anchor_height, build_dataset_for_epoch, epoch_number, pow_digest, stage_mem_mb, EPOCH_LEN,
};

pub const POW_VERSION_V3: u8 = 3;
pub const POW_VERSION_V4: u8 = 4;

pub fn epoch_number_for_version(pow_version: u8, height: u64) -> u64 {
    match pow_version {
        POW_VERSION_V3 => crate::pow_v3::epoch_number(height),
        POW_VERSION_V4 => crate::pow_v4::epoch_number(height),
        _ => panic!("unsupported pow version: {}", pow_version),
    }
}

pub fn anchor_height_for_version(pow_version: u8, height: u64) -> u64 {
    match pow_version {
        POW_VERSION_V3 => crate::pow_v3::anchor_height(height),
        POW_VERSION_V4 => crate::pow_v4::anchor_height(height),
        _ => panic!("unsupported pow version: {}", pow_version),
    }
}

pub fn stage_mem_mb_for_version(pow_version: u8, height: u64) -> usize {
    match pow_version {
        POW_VERSION_V3 => crate::pow_v3::stage_mem_mb(height),
        POW_VERSION_V4 => crate::pow_v4::stage_mem_mb(height),
        _ => panic!("unsupported pow version: {}", pow_version),
    }
}

pub fn build_dataset_for_version(
    pow_version: u8,
    epoch: u64,
    anchor_hash32: crate::types::H32,
    mem_mb: usize,
) -> Vec<u8> {
    match pow_version {
        POW_VERSION_V3 => crate::pow_v3::build_dataset_for_epoch(epoch, anchor_hash32, mem_mb),
        POW_VERSION_V4 => crate::pow_v4::build_dataset_for_epoch(epoch, anchor_hash32, mem_mb),
        _ => panic!("unsupported pow version: {}", pow_version),
    }
}

pub fn pow_digest_for_version(
    pow_version: u8,
    header80: &[u8; 80],
    nonce: u64,
    height: u64,
    anchor_hash32: crate::types::H32,
    dataset: &[u8],
) -> crate::types::H32 {
    match pow_version {
        POW_VERSION_V3 => crate::pow_v3::pow_digest(header80, nonce, height, anchor_hash32, dataset),
        POW_VERSION_V4 => crate::pow_v4::pow_digest(header80, nonce, height, anchor_hash32, dataset),
        _ => panic!("unsupported pow version: {}", pow_version),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::H32;

    #[test]
    fn dutahash_reexports_match_consensus_module() {
        let header = [0x33u8; 80];
        let nonce = 7u64;
        let height = 99u64;
        let anchor = H32([0x55; 32]);
        let ds = build_dataset_for_epoch(epoch_number(height), anchor, 1);
        let digest = pow_digest(&header, nonce, height, anchor, &ds);
        assert_eq!(
            digest.to_hex(),
            crate::pow_v3::pow_digest(&header, nonce, height, anchor, &ds).to_hex()
        );
    }

    #[test]
    fn versioned_pow_helpers_split_v3_and_v4_lightweight() {
        let header = [0x33u8; 80];
        let nonce = 7u64;
        let height = 99u64;
        let anchor = H32([0x55; 32]);
        let ds_v3 = build_dataset_for_version(
            POW_VERSION_V3,
            epoch_number_for_version(POW_VERSION_V3, height),
            anchor,
            1,
        );
        let ds_v4 = build_dataset_for_version(
            POW_VERSION_V4,
            epoch_number_for_version(POW_VERSION_V4, height),
            anchor,
            1,
        );
        let v3 = pow_digest_for_version(POW_VERSION_V3, &header, nonce, height, anchor, &ds_v3);
        let v4 = pow_digest_for_version(POW_VERSION_V4, &header, nonce, height, anchor, &ds_v4);
        assert_ne!(v3, v4);
    }
}
