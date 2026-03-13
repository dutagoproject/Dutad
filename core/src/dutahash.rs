//! Public-facing DUTA PoW namespace.
//!
//! `dutahash` is the preferred public API for consumers.
//! The underlying consensus implementation remains `crate::pow_v3`.

pub use crate::pow_v3::{
    anchor_height, build_dataset_for_epoch, epoch_number, pow_digest, stage_mem_mb, EPOCH_LEN,
};

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
}
