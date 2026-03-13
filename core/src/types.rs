use core::fmt;

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct H32(pub [u8; 32]);

impl H32 {
    pub fn zero() -> Self {
        Self([0u8; 32])
    }

    pub fn from_slice_32(b: &[u8]) -> Option<Self> {
        if b.len() != 32 {
            return None;
        }
        let mut a = [0u8; 32];
        a.copy_from_slice(b);
        Some(Self(a))
    }

    pub fn from_hex(s: &str) -> Option<Self> {
        let b = hex::decode(s).ok()?;
        Self::from_slice_32(&b)
    }

    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Display for H32 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

impl fmt::Debug for H32 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "H32({})", self.to_hex())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_roundtrip() {
        let h = H32([0xabu8; 32]);
        assert_eq!(H32::from_hex(&h.to_hex()), Some(h));
    }

    #[test]
    fn invalid_lengths_are_rejected() {
        assert_eq!(H32::from_slice_32(&[0u8; 31]), None);
        assert_eq!(H32::from_hex("abcd"), None);
    }
}
