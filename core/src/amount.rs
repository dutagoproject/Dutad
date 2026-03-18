#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AmountUnits {
    pub display_unit: &'static str,
    pub base_unit: &'static str,
    pub decimals: u32,
    pub base_per_display: u64,
}

pub const DUTA_DECIMALS: u32 = 8;
pub const DUT_PER_DUTA: u64 = 100_000_000;
pub const DUT_PER_DUTA_I64: i64 = DUT_PER_DUTA as i64;
pub const DISPLAY_UNIT: &str = "DUTA";
pub const BASE_UNIT: &str = "dut";
pub const AMOUNT_UNITS: AmountUnits = AmountUnits {
    display_unit: DISPLAY_UNIT,
    base_unit: BASE_UNIT,
    decimals: DUTA_DECIMALS,
    base_per_display: DUT_PER_DUTA,
};

pub const DEFAULT_MIN_RELAY_FEE_PER_KB_DUT: u64 = 10_000;
pub const DEFAULT_MIN_OUTPUT_VALUE_DUT: u64 = 1;
pub const DEFAULT_WALLET_FEE_DUT: i64 = DEFAULT_MIN_RELAY_FEE_PER_KB_DUT as i64;
pub const DEFAULT_DUST_CHANGE_DUT: i64 = DEFAULT_MIN_OUTPUT_VALUE_DUT as i64;
pub const DEFAULT_MAX_WALLET_FEE_DUT: i64 = 10 * DUT_PER_DUTA_I64;

pub fn format_dut_u64(amount_dut: u64) -> String {
    format_dut_i128(amount_dut as i128)
}

pub fn format_dut_i64(amount_dut: i64) -> String {
    format_dut_i128(amount_dut as i128)
}

fn format_dut_i128(amount_dut: i128) -> String {
    let sign = if amount_dut < 0 { "-" } else { "" };
    let abs = amount_dut.abs();
    let whole = abs / DUT_PER_DUTA as i128;
    let frac = abs % DUT_PER_DUTA as i128;
    if frac == 0 {
        return format!("{sign}{whole}");
    }
    let mut frac_text = format!("{frac:0width$}", width = DUTA_DECIMALS as usize);
    while frac_text.ends_with('0') {
        frac_text.pop();
    }
    format!("{sign}{whole}.{frac_text}")
}

pub fn parse_duta_to_dut_i64(input: &str) -> Result<i64, String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err("amount_missing".to_string());
    }

    let (negative, rest) = match trimmed.as_bytes()[0] {
        b'+' => (false, &trimmed[1..]),
        b'-' => (true, &trimmed[1..]),
        _ => (false, trimmed),
    };
    if rest.is_empty() {
        return Err("amount_invalid".to_string());
    }

    let parts: Vec<&str> = rest.split('.').collect();
    if parts.len() > 2 {
        return Err("amount_invalid".to_string());
    }
    let whole_part = parts[0];
    let frac_part = if parts.len() == 2 { parts[1] } else { "" };
    if whole_part.is_empty() && frac_part.is_empty() {
        return Err("amount_invalid".to_string());
    }
    if !whole_part.chars().all(|c| c.is_ascii_digit()) {
        return Err("amount_invalid".to_string());
    }
    if !frac_part.chars().all(|c| c.is_ascii_digit()) {
        return Err("amount_invalid".to_string());
    }
    if frac_part.len() > DUTA_DECIMALS as usize {
        return Err("amount_too_precise".to_string());
    }

    let whole = if whole_part.is_empty() {
        0i128
    } else {
        whole_part
            .parse::<i128>()
            .map_err(|_| "amount_invalid".to_string())?
    };
    let mut frac_text = frac_part.to_string();
    while frac_text.len() < DUTA_DECIMALS as usize {
        frac_text.push('0');
    }
    let frac = if frac_text.is_empty() {
        0i128
    } else {
        frac_text
            .parse::<i128>()
            .map_err(|_| "amount_invalid".to_string())?
    };

    let mut amount = whole
        .checked_mul(DUT_PER_DUTA as i128)
        .and_then(|v| v.checked_add(frac))
        .ok_or_else(|| "amount_overflow".to_string())?;
    if negative {
        amount = -amount;
    }
    i64::try_from(amount).map_err(|_| "amount_overflow".to_string())
}

#[cfg(test)]
mod tests {
    use super::{
        format_dut_i64, format_dut_u64, parse_duta_to_dut_i64, AMOUNT_UNITS, BASE_UNIT,
        DISPLAY_UNIT, DUT_PER_DUTA, DUT_PER_DUTA_I64,
    };

    #[test]
    fn format_amounts_drop_trailing_zeroes() {
        assert_eq!(format_dut_u64(0), "0");
        assert_eq!(format_dut_u64(DUT_PER_DUTA), "1");
        assert_eq!(format_dut_u64(12_340_000), "0.1234");
        assert_eq!(format_dut_i64(-1_500_000_000), "-15");
        assert_eq!(format_dut_i64(-(DUT_PER_DUTA_I64 + 10)), "-1.0000001");
    }

    #[test]
    fn parse_decimal_amounts_into_dut() {
        assert_eq!(parse_duta_to_dut_i64("1").unwrap(), DUT_PER_DUTA_I64);
        assert_eq!(parse_duta_to_dut_i64("0.00000001").unwrap(), 1);
        assert_eq!(parse_duta_to_dut_i64("12.34").unwrap(), 1_234_000_000);
        assert_eq!(parse_duta_to_dut_i64("-0.5").unwrap(), -50_000_000);
    }

    #[test]
    fn parse_rejects_invalid_or_too_precise_values() {
        assert_eq!(parse_duta_to_dut_i64("").unwrap_err(), "amount_missing");
        assert_eq!(parse_duta_to_dut_i64("abc").unwrap_err(), "amount_invalid");
        assert_eq!(
            parse_duta_to_dut_i64("0.000000001").unwrap_err(),
            "amount_too_precise"
        );
    }

    #[test]
    fn amount_units_define_consensus_and_display_layers() {
        assert_eq!(AMOUNT_UNITS.display_unit, DISPLAY_UNIT);
        assert_eq!(AMOUNT_UNITS.base_unit, BASE_UNIT);
        assert_eq!(AMOUNT_UNITS.decimals, 8);
        assert_eq!(AMOUNT_UNITS.base_per_display, DUT_PER_DUTA);
    }
}
