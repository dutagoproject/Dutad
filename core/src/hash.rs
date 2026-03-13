use crate::types::H32;
use sha3::{Digest, Sha3_256};

/// Consensus hash primitive: SHA3-256.
///
/// Reused across txid / blockid / merkle leaves.
/// (PoW v3 already depends on SHA3-256, so we avoid new deps.)
pub fn sha3_256(bytes: &[u8]) -> H32 {
    let mut h = Sha3_256::new();
    h.update(bytes);
    let out = h.finalize();
    let mut a = [0u8; 32];
    a.copy_from_slice(&out);
    H32(a)
}

#[inline]
pub fn sha3_256_hex(bytes: &[u8]) -> String {
    sha3_256(bytes).to_hex()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha3_hex_matches_known_vector() {
        assert_eq!(
            sha3_256_hex(b"abc"),
            "3a985da74fe225b2045c172d6bd390bd855f086e3e9d525b46bfe24511431532"
        );
    }
}
