//! Network parameters (baked-in; no env).

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Network {
    Mainnet,
    Testnet,
    Stagenet,
}

impl Network {
    pub fn parse_name(name: &str) -> Option<Self> {
        match name.trim().to_ascii_lowercase().as_str() {
            "mainnet" | "main" => Some(Network::Mainnet),
            "testnet" | "test" => Some(Network::Testnet),
            "stagenet" | "stage" => Some(Network::Stagenet),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Network::Mainnet => "mainnet",
            Network::Testnet => "testnet",
            Network::Stagenet => "stagenet",
        }
    }

    pub fn default_p2p_port(&self) -> u16 {
        match self {
            Network::Mainnet => 19082,
            Network::Testnet => 18082,
            Network::Stagenet => 17082,
        }
    }

    pub fn default_daemon_rpc_port(&self) -> u16 {
        match self {
            Network::Mainnet => 19083,
            Network::Testnet => 18083,
            Network::Stagenet => 17083,
        }
    }

    pub fn default_wallet_rpc_port(&self) -> u16 {
        match self {
            Network::Mainnet => 19084,
            Network::Testnet => 18084,
            Network::Stagenet => 17084,
        }
    }

    pub fn default_data_dir_unix(&self) -> &'static str {
        match self {
            Network::Mainnet => ".duta",
            Network::Testnet => ".duta/testnet",
            Network::Stagenet => ".duta/stagenet",
        }
    }

    pub fn daemon_rpc_bind_default(&self) -> &'static str {
        "127.0.0.1"
    }

    pub fn p2p_bind_default(&self) -> &'static str {
        "0.0.0.0"
    }

    pub fn default_seed_hosts(&self) -> &'static [&'static str] {
        match self {
            Network::Mainnet => &["seed1.dutago.xyz", "seed2.dutago.xyz", "seed3.dutago.xyz"],
            Network::Testnet => &[],
            Network::Stagenet => &[],
        }
    }
}

// --- Dev fee parameters (baked-in consensus schedule) ---
// Change BEFORE public launch. Changing after launch forks consensus.
pub const DEVFEE_BPS_YEAR1: u64 = 800;
pub const DEVFEE_BPS_YEAR2: u64 = 400;
pub const DEVFEE_BPS_STEADY: u64 = 200;
pub const DEVFEE_BPS: u64 = DEVFEE_BPS_YEAR1;
pub const DEV_FEE_BPS: u64 = DEVFEE_BPS;

pub const DEVFEE_ADDRS_MAINNET: [&str; 1] = ["dut3ed5b614170366a0d16242504c12c16cd6537925"];
pub const DEVFEE_ADDRS_TESTNET: [&str; 3] = [
    "test36b504b446742162d52f5d666fc4780c2d4ae740",
    "test46b504b446742162d52f5d666fc4780c2d4ae741",
    "test56b504b446742162d52f5d666fc4780c2d4ae742",
];
pub const DEVFEE_ADDRS_STAGENET: [&str; 1] = ["stg36b504b446742162d52f5d666fc4780c2d4ae740"];

pub const DEV_FEE_ADDRESS_MAINNET: &str = DEVFEE_ADDRS_MAINNET[0];
pub const DEV_FEE_ADDRESS_TESTNET: &str = DEVFEE_ADDRS_TESTNET[0];
pub const DEV_FEE_ADDRESS_STAGENET: &str = DEVFEE_ADDRS_STAGENET[0];

pub fn dev_fee_address(net: Network) -> &'static str {
    devfee_addrs(net)[0]
}

pub fn devfee_addrs(net: Network) -> &'static [&'static str] {
    match net {
        Network::Mainnet => &DEVFEE_ADDRS_MAINNET,
        Network::Testnet => &DEVFEE_ADDRS_TESTNET,
        Network::Stagenet => &DEVFEE_ADDRS_STAGENET,
    }
}

pub fn blocks_per_year(net: Network) -> u64 {
    let target_secs = pow_target_secs(net).max(1);
    (365u64 * 24 * 60 * 60) / target_secs
}

pub fn devfee_bps(net: Network, height: u64) -> u64 {
    let year = blocks_per_year(net);
    if height < year {
        DEVFEE_BPS_YEAR1
    } else if height < year.saturating_mul(2) {
        DEVFEE_BPS_YEAR2
    } else {
        DEVFEE_BPS_STEADY
    }
}

pub const GENESIS_HASH_MAINNET: &str =
    "000ccffedbff23d49e96f0db60122798c0ba46a5839fb6b256e7404972fbf85a";
pub const GENESIS_HASH_TESTNET: &str =
    "005e7ac709497090e6ea26729feeb6e43b55ef79be080620d3f4a2fb25ec771e";
pub const GENESIS_HASH_STAGENET: &str =
    "00095918e8706b1f61780de33763f56912c5f947c2ccd0110cf148ae88750823";

pub fn genesis_hash(net: Network) -> &'static str {
    match net {
        Network::Mainnet => GENESIS_HASH_MAINNET,
        Network::Testnet => GENESIS_HASH_TESTNET,
        Network::Stagenet => GENESIS_HASH_STAGENET,
    }
}

pub fn pow_target_secs(net: Network) -> u64 {
    match net {
        Network::Mainnet => 60,
        Network::Testnet => 30,
        Network::Stagenet => 30,
    }
}

pub fn pow_retarget_window(net: Network) -> u64 {
    match net {
        Network::Mainnet => 60,
        Network::Testnet => 30,
        Network::Stagenet => 30,
    }
}

pub fn pow_start_bits(net: Network) -> u64 {
    match net {
        Network::Mainnet => 16,
        Network::Testnet => 8,
        Network::Stagenet => 10,
    }
}

pub fn pow_v4_activation_height(net: Network) -> u64 {
    match net {
        Network::Mainnet => 5_400,
        Network::Testnet => 1,
        Network::Stagenet => 1,
    }
}

pub fn pow_mandatory_recovery_height(net: Network) -> Option<u64> {
    match net {
        Network::Mainnet => Some(5_650),
        Network::Testnet | Network::Stagenet => None,
    }
}

pub fn pow_mandatory_recovery_bits(net: Network) -> Option<u64> {
    match net {
        Network::Mainnet => Some(19),
        Network::Testnet | Network::Stagenet => None,
    }
}

pub fn pow_mandatory_recovery_window(net: Network) -> u64 {
    match net {
        Network::Mainnet => 20,
        Network::Testnet | Network::Stagenet => 0,
    }
}

pub fn pow_mandatory_recovery_active(net: Network, next_height: u64) -> bool {
    let Some(start) = pow_mandatory_recovery_height(net) else {
        return false;
    };
    let window = pow_mandatory_recovery_window(net);
    window > 0 && next_height >= start && next_height < start.saturating_add(window)
}

pub fn pow_consensus_version(net: Network, height: u64) -> u8 {
    if height >= pow_v4_activation_height(net) {
        4
    } else {
        3
    }
}

pub fn pow_algorithm_name(net: Network, height: u64) -> &'static str {
    match pow_consensus_version(net, height) {
        3 => "duta-pow-v3",
        4 => "duta-pow-v4",
        _ => "duta-pow-unknown",
    }
}

pub fn pow_min_bits(_net: Network) -> u64 {
    8
}

pub fn pow_max_bits(_net: Network) -> u64 {
    60
}

pub fn pow_bootstrap_sync_until_height(net: Network) -> u64 {
    match net {
        Network::Mainnet => 0,
        Network::Testnet | Network::Stagenet => 0,
    }
}

pub fn pow_bootstrap_sync_target_bits(net: Network) -> u64 {
    match net {
        Network::Mainnet => 22,
        Network::Testnet | Network::Stagenet => pow_start_bits(net),
    }
}

pub fn pow_bootstrap_sync_recent_span(net: Network) -> u64 {
    match net {
        Network::Mainnet => 10,
        Network::Testnet | Network::Stagenet => 0,
    }
}

pub fn pow_bootstrap_sync_enabled(net: Network, _next_height: u64, _current_bits: u64) -> bool {
    match net {
        Network::Mainnet => {
            _next_height <= pow_bootstrap_sync_until_height(net)
                && _current_bits < pow_bootstrap_sync_target_bits(net)
        }
        Network::Testnet | Network::Stagenet => false,
    }
}

pub fn max_local_mining_sync_lag_blocks(net: Network) -> u64 {
    match net {
        Network::Mainnet => 50,
        Network::Testnet | Network::Stagenet => 0,
    }
}

pub fn mining_sync_gate_until_height(net: Network) -> u64 {
    match net {
        Network::Mainnet => 600,
        Network::Testnet | Network::Stagenet => 0,
    }
}

pub fn pow_launch_difficulty_hardening_enabled(
    net: Network,
    next_height: u64,
    current_bits: u64,
) -> bool {
    match net {
        Network::Mainnet => pow_bootstrap_sync_enabled(net, next_height, current_bits),
        Network::Testnet | Network::Stagenet => false,
    }
}

use std::collections::BTreeMap;

#[derive(Clone, Debug, Default)]
pub struct Conf {
    map: BTreeMap<String, Vec<String>>,
}

impl Conf {
    pub fn parse(text: &str) -> Self {
        let mut out = Conf::default();
        for raw in text.lines() {
            let mut line = raw.trim();
            if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
                continue;
            }

            if let Some(i) = line.find('#') {
                line = line[..i].trim();
            }
            if let Some(i) = line.find(';') {
                line = line[..i].trim();
            }
            if line.is_empty() {
                continue;
            }

            let (k, v) = if let Some((k, v)) = line.split_once('=') {
                (k.trim(), v.trim())
            } else if let Some((k, v)) = line.split_once(char::is_whitespace) {
                (k.trim(), v.trim())
            } else {
                (line, "1")
            };

            if k.is_empty() {
                continue;
            }

            let key = k.to_ascii_lowercase();
            let val = v.trim().trim_matches('"').trim_matches('\'').to_string();
            out.map.entry(key).or_default().push(val);
        }
        out
    }

    pub fn get_all(&self, key: &str) -> Vec<String> {
        self.map
            .get(&key.to_ascii_lowercase())
            .cloned()
            .unwrap_or_default()
    }

    pub fn get_last(&self, key: &str) -> Option<String> {
        self.map
            .get(&key.to_ascii_lowercase())
            .and_then(|v| v.last().cloned())
    }
}

#[cfg(test)]
mod pow_version_tests {
    use super::{
        pow_algorithm_name, pow_consensus_version, pow_mandatory_recovery_active,
        pow_mandatory_recovery_bits, pow_mandatory_recovery_height,
        pow_mandatory_recovery_window, pow_v4_activation_height, Network,
    };

    #[test]
    fn pow_consensus_switches_by_activation_height() {
        assert_eq!(pow_consensus_version(Network::Mainnet, 179_999), 3);
        assert_eq!(
            pow_consensus_version(
                Network::Mainnet,
                pow_v4_activation_height(Network::Mainnet)
            ),
            4
        );
        assert_eq!(pow_consensus_version(Network::Testnet, 0), 3);
        assert_eq!(pow_consensus_version(Network::Testnet, 1), 4);
    }

    #[test]
    fn pow_algorithm_name_tracks_consensus_version() {
        assert_eq!(pow_algorithm_name(Network::Testnet, 0), "duta-pow-v3");
        assert_eq!(pow_algorithm_name(Network::Testnet, 1), "duta-pow-v4");
    }

    #[test]
    fn mandatory_recovery_window_is_mainnet_only_and_bounded() {
        let start = pow_mandatory_recovery_height(Network::Mainnet).unwrap();
        let bits = pow_mandatory_recovery_bits(Network::Mainnet).unwrap();
        assert_eq!(start, 5_650);
        assert_eq!(bits, 19);
        assert_eq!(pow_mandatory_recovery_window(Network::Mainnet), 20);
        assert!(pow_mandatory_recovery_active(Network::Mainnet, start));
        assert!(pow_mandatory_recovery_active(Network::Mainnet, start + 19));
        assert!(!pow_mandatory_recovery_active(Network::Mainnet, start + 20));
        assert_eq!(pow_mandatory_recovery_height(Network::Testnet), None);
        assert_eq!(pow_mandatory_recovery_height(Network::Stagenet), None);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn devfee_compat_aliases_match() {
        for net in [Network::Mainnet, Network::Testnet, Network::Stagenet] {
            assert_eq!(DEV_FEE_BPS, DEVFEE_BPS);
            assert_eq!(dev_fee_address(net), devfee_addrs(net)[0]);
            assert_eq!(devfee_bps(net, 0), DEVFEE_BPS_YEAR1);
        }
    }

    #[test]
    fn devfee_addresses_are_valid_for_each_network() {
        for net in [Network::Mainnet, Network::Testnet, Network::Stagenet] {
            for addr in devfee_addrs(net) {
                assert!(
                    crate::address::parse_address_for_network(net, addr).is_some(),
                    "invalid devfee addr for {:?}: {}",
                    net,
                    addr
                );
            }
        }
    }

    #[test]
    fn port_defaults_stay_distinct() {
        for net in [Network::Mainnet, Network::Testnet, Network::Stagenet] {
            assert_ne!(net.default_p2p_port(), net.default_daemon_rpc_port());
            assert_ne!(net.default_p2p_port(), net.default_wallet_rpc_port());
            assert_ne!(net.default_daemon_rpc_port(), net.default_wallet_rpc_port());
            assert_eq!(net.daemon_rpc_bind_default(), "127.0.0.1");
            assert_eq!(net.p2p_bind_default(), "0.0.0.0");
        }
    }

    #[test]
    fn network_parse_name_accepts_common_aliases() {
        assert_eq!(Network::parse_name("mainnet"), Some(Network::Mainnet));
        assert_eq!(Network::parse_name("test"), Some(Network::Testnet));
        assert_eq!(Network::parse_name("stage"), Some(Network::Stagenet));
        assert_eq!(Network::parse_name("unknown"), None);
    }

    #[test]
    fn conf_parse_preserves_repeated_keys_and_flags() {
        let cfg = Conf::parse(
            r#"
            # comment
            rpc-bind = 127.0.0.1 ; inline comment
            rpc-bind 0.0.0.0
            txindex
            peers=seed1.dutago.xyz
            peers=seed2.dutago.xyz
            "#,
        );
        assert_eq!(cfg.get_all("rpc-bind"), vec!["127.0.0.1", "0.0.0.0"]);
        assert_eq!(cfg.get_last("txindex"), Some("1".to_string()));
        assert_eq!(
            cfg.get_all("peers"),
            vec![
                "seed1.dutago.xyz".to_string(),
                "seed2.dutago.xyz".to_string()
            ]
        );
    }

    #[test]
    fn difficulty_bounds_are_ordered() {
        for net in [Network::Mainnet, Network::Testnet, Network::Stagenet] {
            assert!(pow_min_bits(net) <= pow_start_bits(net));
            assert!(pow_start_bits(net) <= pow_max_bits(net));
            assert!(pow_target_secs(net) > 0);
            assert!(pow_retarget_window(net) > 0);
        }
    }

    #[test]
    fn sync_gate_is_mainnet_only() {
        assert!(!pow_bootstrap_sync_enabled(Network::Mainnet, 1, 12));
        assert!(!pow_bootstrap_sync_enabled(Network::Mainnet, 500, 12));
        assert!(!pow_bootstrap_sync_enabled(Network::Mainnet, 501, 12));
        assert!(!pow_bootstrap_sync_enabled(Network::Mainnet, 1, 21));
        assert!(!pow_bootstrap_sync_enabled(Network::Testnet, 1, 8));
        assert!(!pow_bootstrap_sync_enabled(Network::Stagenet, 1, 10));
        assert!(!pow_launch_difficulty_hardening_enabled(
            Network::Mainnet,
            1,
            12
        ));
    }

    #[test]
    fn devfee_schedule_steps_down_by_chain_year() {
        let year = blocks_per_year(Network::Mainnet);
        assert_eq!(year, 525_600);
        assert_eq!(devfee_bps(Network::Mainnet, 0), DEVFEE_BPS_YEAR1);
        assert_eq!(devfee_bps(Network::Mainnet, year - 1), DEVFEE_BPS_YEAR1);
        assert_eq!(devfee_bps(Network::Mainnet, year), DEVFEE_BPS_YEAR2);
        assert_eq!(devfee_bps(Network::Mainnet, year * 2), DEVFEE_BPS_STEADY);
    }
}
