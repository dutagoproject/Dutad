//! Compatibility re-export for the historical `pow::v4` module path.
//!
//! Canonical source-of-truth lives in `crate::pow_v4`.

pub use crate::pow_v4::{
    anchor_height, build_dataset_for_epoch, epoch_number, pow_digest, stage_mem_mb, EPOCH_LEN,
};
