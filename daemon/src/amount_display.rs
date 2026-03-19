use duta_core::amount::{DUTA_DECIMALS, DUT_PER_DUTA};

pub fn format_dut_fixed_8(amount_dut: u64) -> String {
    let whole = amount_dut / DUT_PER_DUTA;
    let frac = amount_dut % DUT_PER_DUTA;
    format!("{whole}.{frac:0width$}", width = DUTA_DECIMALS as usize)
}

#[cfg(test)]
mod tests {
    use super::format_dut_fixed_8;

    #[test]
    fn fixed_display_amount_keeps_eight_decimals() {
        assert_eq!(format_dut_fixed_8(0), "0.00000000");
        assert_eq!(format_dut_fixed_8(1), "0.00000001");
        assert_eq!(format_dut_fixed_8(10_000), "0.00010000");
        assert_eq!(format_dut_fixed_8(100_000_000), "1.00000000");
        assert_eq!(format_dut_fixed_8(112_340_000), "1.12340000");
    }
}
