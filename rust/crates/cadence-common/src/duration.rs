//! Go time.ParseDuration arithmetic for dynamic duration properties.
/// Parse a signed Go duration without trimming, rounding, or accepting bare
/// numbers other than zero. Invalid values let property readers use defaults.
pub fn parse_nanos(input: &str) -> Option<i64> {
    let (negative, mut rest) = match input.as_bytes().first() {
        Some(b'-') => (true, &input[1..]),
        Some(b'+') => (false, &input[1..]),
        _ => (false, input),
    };
    if rest == "0" {
        return Some(0);
    }
    if rest.is_empty() {
        return None;
    }
    let mut total = 0u64;
    while !rest.is_empty() {
        let start = rest;
        let mut integer = 0u64;
        let digits = rest.bytes().take_while(u8::is_ascii_digit).count();
        for digit in rest.bytes().take(digits) {
            integer = integer
                .checked_mul(10)?
                .checked_add(u64::from(digit - b'0'))?;
            if integer > (1u64 << 63) {
                return None;
            }
        }
        rest = &rest[digits..];
        let mut fraction = 0u64;
        let mut scale = 1f64;
        let mut fraction_digits = 0;
        if let Some(tail) = rest.strip_prefix('.') {
            fraction_digits = tail.bytes().take_while(u8::is_ascii_digit).count();
            let mut overflow = false;
            for digit in tail.bytes().take(fraction_digits) {
                if overflow {
                    continue;
                }
                if fraction > ((1u64 << 63) - 1) / 10 {
                    overflow = true;
                    continue;
                }
                let next = fraction * 10 + u64::from(digit - b'0');
                if next > 1u64 << 63 {
                    overflow = true;
                    continue;
                }
                fraction = next;
                scale *= 10.;
            }
            rest = &tail[fraction_digits..];
        }
        if digits == 0 && fraction_digits == 0 {
            return None;
        }
        let end = rest
            .find(|c: char| c == '.' || c.is_ascii_digit())
            .unwrap_or(rest.len());
        let unit = match &rest[..end] {
            "ns" => 1u64,
            "us" | "µs" | "μs" => 1_000,
            "ms" => 1_000_000,
            "s" => 1_000_000_000,
            "m" => 60_000_000_000,
            "h" => 3_600_000_000_000,
            _ => return None,
        };
        rest = &rest[end..];
        let mut value = integer.checked_mul(unit)?;
        if fraction > 0 {
            value = value.checked_add((fraction as f64 * (unit as f64 / scale)) as u64)?;
        }
        if value > 1u64 << 63 {
            return None;
        }
        total = total.checked_add(value)?;
        if total > 1u64 << 63 || rest.len() == start.len() {
            return None;
        }
    }
    if negative {
        Some((total as i64).wrapping_neg())
    } else {
        i64::try_from(total).ok()
    }
}
