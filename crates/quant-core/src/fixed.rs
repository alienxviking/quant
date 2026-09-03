//! Fixed-point money types.
//!
//! # Why not `f64`?
//!
//! Exchanges quote prices and quantities as *decimal* values ("0.1" BTC,
//! "68123.45" USDT). Binary floating point cannot represent most decimal
//! fractions exactly, so `0.1 + 0.2 != 0.3`. In a trading engine that error
//! does not stay small:
//!
//! - Position tracking accumulates error over thousands of fills, so your
//!   internal position slowly diverges from the exchange's. You then either
//!   flatten a position you do not have, or carry one you think is closed.
//! - Order quantities get rejected for violating lot-size filters because
//!   `0.30000000000000004` is not a multiple of the step size.
//! - Backtest P&L becomes non-reproducible, because summation order changes
//!   the result.
//!
//! So every monetary quantity in the engine is an `i64` scaled by
//! `10^SCALE_DECIMALS`. Integers are exact, associative, and reproducible.
//! Floats are allowed in research code (Python, analytics), never here --
//! see the `clippy::float_arithmetic = "deny"` lint in the root manifest.
//!
//! # Why 8 decimal places?
//!
//! It matches the finest precision crypto exchanges quote (satoshi-level,
//! `1e-8`) and still leaves an `i64` range of roughly ±9.2e10 units, which
//! comfortably covers any price or size we will ever send. If we later add
//! an instrument that needs more precision, that is a data-contract change
//! with a migration -- not something to paper over with a wider float.

use core::fmt;
use core::ops::{Add, AddAssign, Neg, Sub, SubAssign};
use core::str::FromStr;

/// Number of decimal places every fixed-point value is scaled by.
pub const SCALE_DECIMALS: u32 = 8;

/// The scale factor itself: `10^SCALE_DECIMALS`.
pub const SCALE: i64 = 100_000_000;

/// Failure modes when parsing an exchange-supplied decimal string.
///
/// Every variant is a *loud* failure. We never silently round, truncate or
/// fall back to zero: a malformed price is a signal that our understanding
/// of the venue's payload is wrong, and that is exactly the moment to stop
/// rather than trade on a guess.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseFixedError {
    /// Input was empty, or contained a sign with no digits.
    Empty,
    /// Input contained a character that is not a digit, sign or point.
    InvalidChar(char),
    /// More than one decimal point.
    MultipleDecimalPoints,
    /// A significant (non-zero) digit beyond `SCALE_DECIMALS` places.
    ///
    /// Trailing zeros past the scale are accepted, because venues pad their
    /// decimal strings. A non-zero digit means real precision would be lost.
    PrecisionLoss { max_decimals: u32 },
    /// The value does not fit in the fixed-point range.
    Overflow,
}

impl fmt::Display for ParseFixedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => write!(f, "empty numeric string"),
            Self::InvalidChar(c) => write!(f, "invalid character {c:?} in numeric string"),
            Self::MultipleDecimalPoints => write!(f, "multiple decimal points"),
            Self::PrecisionLoss { max_decimals } => {
                write!(f, "more than {max_decimals} significant decimal places")
            }
            Self::Overflow => write!(f, "value out of fixed-point range"),
        }
    }
}

impl std::error::Error for ParseFixedError {}

/// Parse a decimal string into a value scaled by [`SCALE`].
///
/// Deliberately does **not** route through `f64`: the whole point of these
/// types is that the exchange's decimal string survives into our engine
/// bit-for-bit. Exponent notation is rejected rather than handled, because
/// no venue we support emits it and accepting it would mean guessing.
fn parse_scaled(s: &str) -> Result<i64, ParseFixedError> {
    let (negative, digits) = match s.as_bytes().first() {
        None => return Err(ParseFixedError::Empty),
        Some(b'-') => (true, &s[1..]),
        Some(b'+') => (false, &s[1..]),
        Some(_) => (false, s),
    };

    let mut acc: i64 = 0;
    let mut frac_digits: u32 = 0;
    let mut seen_point = false;
    let mut seen_digit = false;

    // Iterate bytes, not chars: the grammar is pure ASCII, and this avoids
    // a lossy `char as u8` cast in the digit conversion.
    for &byte in digits.as_bytes() {
        match byte {
            b'0'..=b'9' => {
                seen_digit = true;
                let d = i64::from(byte - b'0');

                if seen_point && frac_digits >= SCALE_DECIMALS {
                    // Past our precision. Padding zeros are fine to drop;
                    // anything else means we would lose real information.
                    if d == 0 {
                        continue;
                    }
                    return Err(ParseFixedError::PrecisionLoss {
                        max_decimals: SCALE_DECIMALS,
                    });
                }

                if seen_point {
                    frac_digits += 1;
                }
                acc = acc
                    .checked_mul(10)
                    .and_then(|v| v.checked_add(d))
                    .ok_or(ParseFixedError::Overflow)?;
            }
            b'.' => {
                if seen_point {
                    return Err(ParseFixedError::MultipleDecimalPoints);
                }
                seen_point = true;
            }
            other => return Err(ParseFixedError::InvalidChar(char::from(other))),
        }
    }

    if !seen_digit {
        return Err(ParseFixedError::Empty);
    }

    let shortfall = SCALE_DECIMALS - frac_digits;
    let scaled = acc
        .checked_mul(10_i64.pow(shortfall))
        .ok_or(ParseFixedError::Overflow)?;

    Ok(if negative { -scaled } else { scaled })
}

/// Render a scaled integer back to its canonical decimal string.
///
/// Trailing zeros are trimmed so the output round-trips through
/// [`parse_scaled`] and reads naturally in logs.
fn fmt_scaled(raw: i64, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    // i64::MIN has no positive counterpart, so widen before taking abs.
    let magnitude = i128::from(raw).unsigned_abs();
    let scale = u128::from(SCALE.unsigned_abs());
    let integral = magnitude / scale;
    let fractional = magnitude % scale;

    if raw < 0 {
        write!(f, "-")?;
    }
    if fractional == 0 {
        write!(f, "{integral}")
    } else {
        let frac = format!("{fractional:0width$}", width = SCALE_DECIMALS as usize);
        write!(f, "{integral}.{}", frac.trim_end_matches('0'))
    }
}

/// Generates a distinct newtype over a scaled `i64`.
///
/// Each monetary concept gets its own type so the compiler stops us adding a
/// price to a quantity, or passing a notional where a size is expected. This
/// is free at runtime and catches a whole class of mistakes that would
/// otherwise only show up as a wrong order hitting the wire.
macro_rules! fixed_type {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
        #[derive(serde::Serialize, serde::Deserialize)]
        #[serde(transparent)]
        pub struct $name(i64);

        impl $name {
            /// The zero value.
            pub const ZERO: Self = Self(0);
            /// The largest representable value.
            ///
            /// A sentinel for "more than any limit", used where an overflowing
            /// multiplication has to compare as too big rather than wrap. Not a
            /// value any real price or size takes.
            pub const MAX_VALUE: Self = Self(i64::MAX);
            /// Smallest representable positive increment (`1e-8`).
            pub const MIN_TICK: Self = Self(1);

            /// Construct from an already-scaled integer.
            ///
            /// Prefer [`Self::from_str`] for exchange payloads and
            /// [`Self::from_units`] for literals -- this is the raw escape
            /// hatch and it is easy to forget the scale factor.
            #[must_use]
            pub const fn from_raw(raw: i64) -> Self {
                Self(raw)
            }

            /// The underlying scaled integer. Use for serialization and
            /// arithmetic that needs to widen to `i128`.
            #[must_use]
            pub const fn raw(self) -> i64 {
                self.0
            }

            /// Construct from a whole number of units (e.g. `from_units(5)`
            /// is `5.0`). Saturates rather than wrapping on overflow.
            #[must_use]
            pub const fn from_units(units: i64) -> Self {
                Self(units.saturating_mul(SCALE))
            }

            #[must_use]
            pub const fn is_zero(self) -> bool {
                self.0 == 0
            }

            #[must_use]
            pub const fn is_positive(self) -> bool {
                self.0 > 0
            }

            #[must_use]
            pub const fn is_negative(self) -> bool {
                self.0 < 0
            }

            /// Absolute value. Saturating, because `i64::MIN.abs()` panics.
            #[must_use]
            pub const fn abs(self) -> Self {
                Self(self.0.saturating_abs())
            }

            /// Addition that reports overflow instead of panicking.
            ///
            /// The engine should use this on any path that touches
            /// externally-supplied values; a panic in the order path takes
            /// the process down with live positions open.
            #[must_use]
            pub const fn checked_add(self, rhs: Self) -> Option<Self> {
                match self.0.checked_add(rhs.0) {
                    Some(v) => Some(Self(v)),
                    None => None,
                }
            }

            #[must_use]
            pub const fn checked_sub(self, rhs: Self) -> Option<Self> {
                match self.0.checked_sub(rhs.0) {
                    Some(v) => Some(Self(v)),
                    None => None,
                }
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt_scaled(self.0, f)
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, concat!(stringify!($name), "("))?;
                fmt_scaled(self.0, f)?;
                write!(f, ")")
            }
        }

        impl FromStr for $name {
            type Err = ParseFixedError;
            fn from_str(s: &str) -> Result<Self, Self::Err> {
                parse_scaled(s).map(Self)
            }
        }

        // The operators panic on overflow in *both* debug and release, rather
        // than inheriting the default wrap-in-release behaviour. Overflowing
        // an i64 at 1e-8 scale means a value above ~9.2e10 units, which can
        // only happen if something upstream is badly wrong. Crashing is bad;
        // silently wrapping a price and sending the wrapped value to an
        // exchange is much worse. Order-path code that handles untrusted
        // input should use `checked_add` / `checked_sub` and decide for
        // itself.
        impl Add for $name {
            type Output = Self;
            fn add(self, rhs: Self) -> Self {
                self.checked_add(rhs)
                    .expect(concat!(stringify!($name), " overflow in add"))
            }
        }

        impl Sub for $name {
            type Output = Self;
            fn sub(self, rhs: Self) -> Self {
                self.checked_sub(rhs)
                    .expect(concat!(stringify!($name), " overflow in sub"))
            }
        }

        impl AddAssign for $name {
            fn add_assign(&mut self, rhs: Self) {
                *self = *self + rhs;
            }
        }

        impl SubAssign for $name {
            fn sub_assign(&mut self, rhs: Self) {
                *self = *self - rhs;
            }
        }

        impl Neg for $name {
            type Output = Self;
            fn neg(self) -> Self {
                Self(self.0.checked_neg()
                    .expect(concat!(stringify!($name), " overflow in neg")))
            }
        }
    };
}

fixed_type! {
    /// A price, in quote currency per unit of base currency.
    Px
}

fixed_type! {
    /// A quantity, in units of the base currency.
    ///
    /// Signed: a negative quantity is a short position or a sell-side flow.
    Qty
}

fixed_type! {
    /// A monetary value in quote currency: notional, fee, or realized P&L.
    Notional
}

fixed_type! {
    /// A dimensionless fraction: a fee rate, a discount, a percentage of
    /// notional.
    ///
    /// Fixed-point like everything else, so `"0.001"` is ten basis points and
    /// `"0.00075"` is seven and a half. Basis points as an integer would have
    /// been tidier to read and could not express the fractional tiers venues
    /// actually publish.
    ///
    /// Signed, because a maker rebate is a negative rate and a type that could
    /// not hold one would quietly misprice every passive strategy.
    Rate
}

impl Notional {
    /// `notional / quantity`: the price implied by a value and a size.
    ///
    /// The inverse of [`Px::notional`], and it lives beside it so the pair
    /// cannot drift. Used for the size-weighted average price of a fill that
    /// consumed several book levels — the number a P&L must use, and one that
    /// two call sites had previously each derived for themselves.
    ///
    /// `None` for a zero quantity: a price per nothing is not a price, and
    /// returning zero would be a plausible wrong answer.
    #[must_use]
    pub fn per_unit(self, qty: Qty) -> Option<Px> {
        if qty.raw() == 0 {
            return None;
        }
        let scaled = i128::from(self.raw()).checked_mul(i128::from(SCALE))?;
        i64::try_from(scaled / i128::from(qty.raw()))
            .ok()
            .map(Px::from_raw)
    }

    /// `notional * rate`: a fee, a discount, a fraction of a position.
    ///
    /// Truncates toward zero, like every other multiplication here, so a fee is
    /// never rounded *up* into money the venue did not charge.
    #[must_use]
    pub fn scaled_by(self, rate: Rate) -> Option<Self> {
        let product = i128::from(self.raw()).checked_mul(i128::from(rate.raw()))?;
        i64::try_from(product / i128::from(SCALE))
            .ok()
            .map(Self::from_raw)
    }
}

impl Px {
    /// `price * quantity`, in quote currency.
    ///
    /// Widens to `i128` before multiplying because two 8-decimal values
    /// multiply to a 16-decimal intermediate, which overflows `i64` for any
    /// realistic BTC notional. Returns `None` if the *result* does not fit.
    #[must_use]
    pub fn notional(self, qty: Qty) -> Option<Notional> {
        let product = i128::from(self.raw()).checked_mul(i128::from(qty.raw()))?;
        // Truncates toward zero. Sub-satoshi residue is not representable;
        // the venue's own fill report is authoritative for settlement, and
        // any discrepancy must surface in reconciliation rather than be
        // rounded away here.
        let scaled = product / i128::from(SCALE);
        i64::try_from(scaled).ok().map(Notional::from_raw)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_typical_exchange_strings() {
        assert_eq!("68123.45".parse::<Px>().unwrap().raw(), 6_812_345_000_000);
        assert_eq!("0.00000001".parse::<Qty>().unwrap().raw(), 1);
        assert_eq!("0".parse::<Qty>().unwrap().raw(), 0);
        assert_eq!("-1.5".parse::<Qty>().unwrap().raw(), -150_000_000);
        // Binance pads to 8dp; that must not be mistaken for extra precision.
        assert_eq!("12.10000000".parse::<Px>().unwrap().raw(), 1_210_000_000);
        // Padding beyond our scale is dropped only when it is all zeros.
        assert_eq!(
            "12.100000000000".parse::<Px>().unwrap().raw(),
            1_210_000_000
        );
    }

    #[test]
    fn rejects_rather_than_rounds() {
        assert_eq!(
            "0.000000001".parse::<Px>(),
            Err(ParseFixedError::PrecisionLoss { max_decimals: 8 })
        );
        assert_eq!(
            "1.2.3".parse::<Px>(),
            Err(ParseFixedError::MultipleDecimalPoints)
        );
        assert_eq!("1e5".parse::<Px>(), Err(ParseFixedError::InvalidChar('e')));
        assert_eq!("".parse::<Px>(), Err(ParseFixedError::Empty));
        assert_eq!("-".parse::<Px>(), Err(ParseFixedError::Empty));
        assert_eq!("nan".parse::<Px>(), Err(ParseFixedError::InvalidChar('n')));
        assert_eq!(
            "99999999999999999999".parse::<Px>(),
            Err(ParseFixedError::Overflow)
        );
    }

    #[test]
    fn display_round_trips() {
        for s in ["68123.45", "0.00000001", "0", "-1.5", "1000000"] {
            let px: Px = s.parse().unwrap();
            assert_eq!(px.to_string(), s, "round-trip failed for {s}");
        }
        // Trailing zeros are normalized away, which is still a valid parse.
        let px: Px = "12.10000000".parse().unwrap();
        assert_eq!(px.to_string(), "12.1");
        assert_eq!("12.1".parse::<Px>().unwrap(), px);
    }

    #[test]
    fn the_float_problem_does_not_exist_here() {
        let a: Qty = "0.1".parse().unwrap();
        let b: Qty = "0.2".parse().unwrap();
        let c: Qty = "0.3".parse().unwrap();
        assert_eq!(a + b, c);

        // And summation is order-independent, so backtests reproduce.
        let xs: Vec<Qty> = ["0.1", "0.2", "0.3", "0.7"]
            .iter()
            .map(|s| s.parse().unwrap())
            .collect();
        let fwd = xs.iter().copied().fold(Qty::ZERO, |a, b| a + b);
        let rev = xs.iter().rev().copied().fold(Qty::ZERO, |a, b| a + b);
        assert_eq!(fwd, rev);
        assert_eq!(fwd, "1.3".parse::<Qty>().unwrap());
    }

    #[test]
    fn notional_widens_before_multiplying() {
        let px: Px = "68123.45".parse().unwrap();
        let qty: Qty = "1.5".parse().unwrap();
        assert_eq!(px.notional(qty).unwrap().to_string(), "102185.175");

        // A naive i64 multiply of the raw values would have overflowed here.
        let big_px: Px = "68123.45".parse().unwrap();
        let big_qty: Qty = "10000".parse().unwrap();
        assert_eq!(big_px.notional(big_qty).unwrap().to_string(), "681234500");
    }

    #[test]
    fn notional_is_signed_for_short_flow() {
        let px: Px = "100".parse().unwrap();
        let sell: Qty = "-2.5".parse().unwrap();
        assert_eq!(px.notional(sell).unwrap().to_string(), "-250");
    }

    #[test]
    fn checked_ops_do_not_panic_at_the_boundary() {
        let max = Qty::from_raw(i64::MAX);
        assert_eq!(max.checked_add(Qty::MIN_TICK), None);
        assert_eq!(Qty::from_raw(i64::MIN).checked_sub(Qty::MIN_TICK), None);
    }

    #[test]
    fn formats_extreme_values_without_panicking() {
        // i64::MIN.abs() would panic; fmt_scaled widens to i128 first.
        let v = Px::from_raw(i64::MIN);
        assert!(v.to_string().starts_with('-'));
    }
}
