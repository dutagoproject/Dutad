//! Small byte/rotation helpers retained for compatibility with older internal code paths.
//! New public consumers should prefer the stable APIs exposed from `dutahash` / `pow_v3`.

#[inline]
pub fn rotl_u64(x: u64, r: u32) -> u64 {
    x.rotate_left(r)
}

#[inline]
pub fn u64le(b: &[u8]) -> u64 {
    let mut a = [0u8; 8];
    a.copy_from_slice(&b[0..8]);
    u64::from_le_bytes(a)
}

#[inline]
pub fn p64le(x: u64) -> [u8; 8] {
    x.to_le_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn helpers_roundtrip() {
        let x = 0x1122334455667788u64;
        assert_eq!(u64le(&p64le(x)), x);
        assert_eq!(rotl_u64(0x01, 8), 0x100);
    }
}
