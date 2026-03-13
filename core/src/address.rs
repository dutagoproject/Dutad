use crate::hash;
use crate::netparams::Network;
use crate::types::H32;

/// Legacy compatibility alias.
///
/// New code should use `address_prefix(Network::...)` or the network-aware helpers.
pub const ADDR_PREFIX: &str = "dut";

pub fn address_prefix(net: Network) -> &'static str {
    match net {
        Network::Mainnet => "dut",
        Network::Testnet => "test",
        Network::Stagenet => "stg",
    }
}

pub fn detect_network(s: &str) -> Option<Network> {
    let s = s.trim();
    for net in [Network::Mainnet, Network::Testnet, Network::Stagenet] {
        if s.starts_with(address_prefix(net)) {
            return Some(net);
        }
    }
    None
}

pub fn parse_address_for_network(net: Network, s: &str) -> Option<[u8; 20]> {
    let s = s.trim();
    let rest = s.strip_prefix(address_prefix(net))?;
    if rest.len() != 40 {
        return None;
    }
    let b = hex::decode(rest).ok()?;
    if b.len() != 20 {
        return None;
    }
    let mut a = [0u8; 20];
    a.copy_from_slice(&b);
    Some(a)
}

pub fn parse_address(s: &str) -> Option<[u8; 20]> {
    let net = detect_network(s)?;
    parse_address_for_network(net, s)
}

pub fn pkh_to_address_for_network(net: Network, pkh: &[u8; 20]) -> String {
    format!("{}{}", address_prefix(net), hex::encode(pkh))
}

pub fn pkh_to_address(pkh: &[u8; 20]) -> String {
    pkh_to_address_for_network(Network::Mainnet, pkh)
}

/// Pubkey-hash = first 20 bytes of SHA3-256(pubkey_bytes).
pub fn pkh_from_pubkey(pubkey_bytes: &[u8]) -> [u8; 20] {
    let H32(h) = hash::sha3_256(pubkey_bytes);
    let mut out = [0u8; 20];
    out.copy_from_slice(&h[0..20]);
    out
}

pub fn pkh_to_hex(pkh: &[u8; 20]) -> String {
    hex::encode(pkh)
}

pub fn pkh_from_hex(s: &str) -> Option<[u8; 20]> {
    let b = hex::decode(s).ok()?;
    if b.len() != 20 {
        return None;
    }
    let mut a = [0u8; 20];
    a.copy_from_slice(&b);
    Some(a)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn address_roundtrip_per_network() {
        let pkh = [0x11u8; 20];
        assert_eq!(
            pkh_to_address_for_network(Network::Mainnet, &pkh),
            "dut1111111111111111111111111111111111111111"
        );
        assert_eq!(
            pkh_to_address_for_network(Network::Testnet, &pkh),
            "test1111111111111111111111111111111111111111"
        );
        assert_eq!(
            pkh_to_address_for_network(Network::Stagenet, &pkh),
            "stg1111111111111111111111111111111111111111"
        );
        assert_eq!(
            parse_address_for_network(
                Network::Mainnet,
                "dut1111111111111111111111111111111111111111"
            ),
            Some(pkh)
        );
        assert_eq!(
            parse_address_for_network(
                Network::Testnet,
                "test1111111111111111111111111111111111111111"
            ),
            Some(pkh)
        );
        assert_eq!(
            parse_address_for_network(
                Network::Stagenet,
                "stg1111111111111111111111111111111111111111"
            ),
            Some(pkh)
        );
    }

    #[test]
    fn reject_wrong_prefix_and_length() {
        assert_eq!(
            parse_address("btc1111111111111111111111111111111111111111"),
            None
        );
        assert_eq!(parse_address("dutabcd"), None);
        assert_eq!(
            parse_address_for_network(
                Network::Mainnet,
                "test1111111111111111111111111111111111111111"
            ),
            None
        );
    }

    #[test]
    fn reject_non_hex_payload() {
        assert_eq!(
            parse_address("dutzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"),
            None
        );
    }

    #[test]
    fn pubkey_hash_matches_sha3_prefix() {
        let pkh = pkh_from_pubkey(b"duta");
        assert_eq!(hex::encode(pkh), "0c9343066fbc45f6340e5a716fcdd6430306613c");
    }
}
