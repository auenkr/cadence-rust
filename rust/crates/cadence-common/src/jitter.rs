//! Jitter helpers ported from `go/common/backoff/jitter.go`.
//!
//! Ranges match Go exactly: `[(1-c)*input, (1+c)*input)`.

use rand::Rng;

fn validate_coefficient(coefficient: f64) {
    assert!(
        (0.0..=1.0).contains(&coefficient),
        "coefficient cannot be < 0 or > 1"
    );
}

/// Random `i64` in `[(1-c)*input, (1+c)*input)`. Mirrors Go `JitInt64`.
pub fn jit_i64<R: Rng + ?Sized>(rng: &mut R, input: i64, coefficient: f64) -> i64 {
    validate_coefficient(coefficient);
    if coefficient == 0.0 || input == 0 {
        return input;
    }
    let base = (input as f64 * (1.0 - coefficient)) as i64;
    let span = 2 * (input - base);
    base + rng.gen_range(0..span)
}

/// Random `f64` in `[(1-c)*input, (1+c)*input)`. Mirrors Go `JitFloat64`.
pub fn jit_f64<R: Rng + ?Sized>(rng: &mut R, input: f64, coefficient: f64) -> f64 {
    validate_coefficient(coefficient);
    if coefficient == 0.0 || input == 0.0 {
        return input;
    }
    let base = input * (1.0 - coefficient);
    base + rng.gen::<f64>() * 2.0 * (input - base)
}

/// Random `Duration` in `[(1-c)*d, (1+c)*d)`. Mirrors Go `JitDuration`.
pub fn jit_duration<R: Rng + ?Sized>(
    rng: &mut R,
    duration: std::time::Duration,
    coefficient: f64,
) -> std::time::Duration {
    std::time::Duration::from_nanos(jit_i64(rng, duration.as_nanos() as i64, coefficient) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;

    #[test]
    fn zero_coefficient_or_input_is_identity() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(1);
        assert_eq!(jit_i64(&mut rng, 0, 0.5), 0);
        assert_eq!(jit_i64(&mut rng, 42, 0.0), 42);
        assert_eq!(jit_f64(&mut rng, 0.0, 0.5), 0.0);
        assert_eq!(jit_f64(&mut rng, 3.0, 0.0), 3.0);
    }

    #[test]
    fn jitter_stays_in_range() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        for _ in 0..1000 {
            let v = jit_i64(&mut rng, 1000, 0.2);
            assert!((800..1200).contains(&v), "out of range: {v}");
            let f = jit_f64(&mut rng, 100.0, 0.1);
            assert!((90.0..110.0).contains(&f), "out of range: {f}");
        }
    }

    #[test]
    #[should_panic(expected = "coefficient cannot be < 0 or > 1")]
    fn invalid_coefficient_panics() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(1);
        let _ = jit_i64(&mut rng, 10, 1.5);
    }
}
