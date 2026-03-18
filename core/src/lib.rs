//! Core primitives for DUTA COIN.
//!
//! Stability intent for go-public hardening:
//! - `dutahash` is the preferred public PoW namespace.
//! - `pow_v3` remains the canonical implementation module.
//! - `pow::v3` is retained as a compatibility namespace and re-exports `pow_v3`.
//! - `address`, `hash`, `netparams`, and `types` provide the compact public surface
//!   used by daemon/miner/wallet today.

pub mod address;
pub mod amount;
pub mod dutahash;
pub mod hash;
pub mod netparams;
pub mod pow;
pub mod pow_v3;
pub mod types;

pub use address::{
    address_prefix, detect_network, parse_address, parse_address_for_network, pkh_from_hex,
    pkh_from_pubkey, pkh_to_address, pkh_to_address_for_network, pkh_to_hex, ADDR_PREFIX,
};
pub use amount::{
    format_dut_i64, format_dut_u64, parse_duta_to_dut_i64, AmountUnits, AMOUNT_UNITS, BASE_UNIT,
    DEFAULT_DUST_CHANGE_DUT, DEFAULT_MAX_WALLET_FEE_DUT, DEFAULT_MIN_OUTPUT_VALUE_DUT,
    DEFAULT_MIN_RELAY_FEE_PER_KB_DUT, DEFAULT_WALLET_FEE_DUT, DISPLAY_UNIT, DUT_PER_DUTA,
    DUT_PER_DUTA_I64, DUTA_DECIMALS,
};
pub use netparams::{dev_fee_address, devfee_addrs, devfee_bps, Conf, Network};
pub use types::H32;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compatibility_namespaces_agree() {
        let height = 12345u64;
        let anchor = H32([0x22; 32]);
        assert_eq!(pow_v3::epoch_number(height), dutahash::epoch_number(height));
        assert_eq!(pow_v3::epoch_number(height), pow::v3::epoch_number(height));
        assert_eq!(
            pow_v3::anchor_height(height),
            dutahash::anchor_height(height)
        );
        assert_eq!(
            pow_v3::anchor_height(height),
            pow::v3::anchor_height(height)
        );
        assert_eq!(
            pow_v3::build_dataset_for_epoch(0, anchor, 1),
            dutahash::build_dataset_for_epoch(0, anchor, 1)
        );
    }
}
