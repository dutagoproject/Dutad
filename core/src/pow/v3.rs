//! Compatibility re-export for the historical `pow::v3` module path.
//!
//! Canonical source-of-truth lives in `crate::pow_v3`.
//! Keeping this as a pure re-export prevents drift between two implementations.

pub use crate::pow_v3::{
    anchor_height, build_dataset_for_epoch, epoch_number, pow_digest, stage_mem_mb, EPOCH_LEN,
};
