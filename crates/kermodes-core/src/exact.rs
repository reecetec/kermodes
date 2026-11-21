//! Exact sums for `sum`, `mean`, and `std`. Every float64 is an integer times a power of two, so a
//! sum of them is one too: it is kept exactly and rounds to float64 once, when it is read. The
//! result is then the same whatever the order, chunking, or merging of partial sums that made it,
//! and taking values back out restores the earlier sum exactly.
//!
//! A sum is a two's complement integer of `N` 64-bit limbs times `2^scale`, its scale the smallest
//! exponent among its values: a float64 is its 53-bit significand times 2 to an exponent that the
//! values in one binade share. Values spread over `k` binades then need about
//! `53 + k + log2(rows)` bits, so 128 hold the sums of typical data and 256 the sums of their
//! squares. A sum that outgrows its limbs, or meets a NaN or an infinity, moves to a wide integer
//! that holds any sum of float64 values or of their squares, kept apart so a group's state stays
//! small in the common case.

/// The limbs and unit of a wide sum. Its unit is the smallest square of a float64, 2^-2148, and
/// 2^63 squares of the largest float64, below 2^(2048 + 63), fit with their sign:
/// 2148 + 2111 + 1 bits.
const WIDE_LIMBS: usize = 68;
const WIDE_SCALE: i32 = -2148;

const INFINITY_BITS: u64 = 0x7ff << 52;

/// One value of a sum, exactly.
#[derive(Clone, Copy)]
pub(crate) enum Term {
    /// `±m·2^e`, with `m` below 2^64.
    Finite {
        negative: bool,
        m: u128,
        e: i32,
    },
    Nan,
    Infinite {
        negative: bool,
    },
}

impl Term {
    #[inline]
    pub fn of_f64(x: f64) -> Self {
        let bits = x.to_bits();
        let negative = bits >> 63 != 0;
        let biased = ((bits >> 52) & 0x7ff) as i32;
        let fraction = bits & ((1 << 52) - 1);
        if biased == 0x7ff {
            return if fraction == 0 {
                Term::Infinite { negative }
            } else {
                Term::Nan
            };
        }
        // Values within a binade share an exponent, so the sum's scale only moves when a smaller
        // binade arrives.
        let (m, e) = if biased == 0 {
            (fraction, -1074)
        } else {
            (fraction | 1 << 52, biased - 1075)
        };
        Term::Finite {
            negative,
            m: u128::from(m),
            e,
        }
    }

    #[inline]
    pub fn of_i64(value: i64) -> Self {
        Term::Finite {
            negative: value < 0,
            m: u128::from(value.unsigned_abs()),
            e: 0,
        }
    }

    /// The square, exactly: `m` is at most 2^63, so `m²` fits in 128 bits.
    #[inline]
    pub fn square(self) -> Self {
        match self {
            Term::Finite { m, e, .. } => Term::Finite {
                negative: false,
                m: m * m,
                e: 2 * e,
            },
            Term::Nan => Term::Nan,
            Term::Infinite { .. } => Term::Infinite { negative: false },
        }
    }
}

/// An exact sum: `limbs · 2^scale` while it fits, else the wide integer `wide - 1` of the
/// accumulator's `Wides`.
#[derive(Clone, Copy)]
pub(crate) struct Exact<const N: usize> {
    limbs: [u64; N],
    scale: i32,
    wide: u32,
}

impl<const N: usize> Default for Exact<N> {
    fn default() -> Self {
        // The largest scale, so the first value sets it.
        Self {
            limbs: [0; N],
            scale: i32::MAX,
            wide: 0,
        }
    }
}

/// The wide sums of one aggregation's groups, by index; freed indices are reused.
#[derive(Default)]
pub(crate) struct Wides {
    entries: Vec<Wide>,
    free: Vec<u32>,
}

/// A sum at unit 2^WIDE_SCALE, with its NaNs and infinities counted so they can leave too.
#[derive(Clone, Copy)]
struct Wide {
    limbs: [u64; WIDE_LIMBS],
    nan: i64,
    /// Positive, then negative.
    infinite: [i64; 2],
}

impl Wides {
    /// Stores `wide`, returning its index plus one.
    fn insert(&mut self, wide: Wide) -> u32 {
        match self.free.pop() {
            Some(index) => {
                self.entries[index as usize] = wide;
                index + 1
            }
            None => {
                self.entries.push(wide);
                self.entries.len() as u32
            }
        }
    }
}

/// Adds `±m·2^shift` to a 128- or 256-bit two's complement integer in native wide arithmetic, the
/// common case of `Exact::add`. False, leaving `limbs` as they were, if the value or the result
/// would not fit.
#[inline(always)]
fn add_fast(limbs: &mut [u64], m: u128, shift: u32, negative: bool) -> bool {
    let low = u128::from(limbs[0]) | u128::from(limbs[1]) << 64;
    if limbs.len() == 2 {
        // The value stays below 2^126, so a checked add catches any overflow.
        if m.leading_zeros() < shift + 2 {
            return false;
        }
        let (sum, value) = (low as i128, (m << shift) as i128);
        let Some(sum) = (if negative {
            sum.checked_sub(value)
        } else {
            sum.checked_add(value)
        }) else {
            return false;
        };
        limbs[0] = sum as u64;
        limbs[1] = (sum >> 64) as u64;
        return true;
    }
    // The value stays below 2^254, its high half below 2^126.
    if 128 - m.leading_zeros() + shift > 254 {
        return false;
    }
    let high = (u128::from(limbs[2]) | u128::from(limbs[3]) << 64) as i128;
    let (value_low, value_high) = match shift {
        0 => (m, 0),
        1..128 => (m << shift, m >> (128 - shift)),
        _ => (0, m << (shift - 128)),
    };
    let (low, high) = if negative {
        let (low, borrow) = low.overflowing_sub(value_low);
        (
            low,
            high.checked_sub(value_high as i128 + i128::from(borrow)),
        )
    } else {
        let (low, carry) = low.overflowing_add(value_low);
        (
            low,
            high.checked_add(value_high as i128 + i128::from(carry)),
        )
    };
    let Some(high) = high else {
        return false;
    };
    limbs.copy_from_slice(&[
        low as u64,
        (low >> 64) as u64,
        high as u64,
        (high >> 64) as u64,
    ]);
    true
}

/// Adds the two's complement `value·2^shift` to `limbs`, both 128 bits or both 256, in native
/// wide arithmetic, the common case of `Exact::merge`. False, leaving `limbs` as they were, if
/// it would not fit (or, at 256 bits, if `value` needs more than 128).
#[inline]
fn merge_fast(limbs: &mut [u64], value: &[u64], shift: u32) -> bool {
    if limbs.len() == 2 {
        let value = (u128::from(value[0]) | u128::from(value[1]) << 64) as i128;
        let sum = (u128::from(limbs[0]) | u128::from(limbs[1]) << 64) as i128;
        let shifted = value << shift;
        let Some(sum) = (shifted >> shift == value)
            .then(|| sum.checked_add(shifted))
            .flatten()
        else {
            return false;
        };
        limbs[0] = sum as u64;
        limbs[1] = (sum >> 64) as u64;
        return true;
    }
    let (negative, magnitude) = magnitude_of(<[u64; 4]>::try_from(value).expect("256 bits"));
    let low = u128::from(magnitude[0]) | u128::from(magnitude[1]) << 64;
    magnitude[2..] == [0, 0] && add_fast(limbs, low, shift, negative)
}

impl<const N: usize> Exact<N> {
    /// Adds `term`, or takes it back out if `remove`.
    #[inline]
    pub fn add(&mut self, wides: &mut Wides, term: Term, remove: bool) {
        if let Term::Finite { negative, m, e } = term {
            if m == 0 {
                return;
            }
            if self.scale == i32::MAX && self.wide == 0 {
                // An empty sum takes its scale from its first value.
                self.scale = e;
            }
            let shift = i64::from(e) - i64::from(self.scale);
            if self.wide == 0
                && (0..64 * N as i64).contains(&shift)
                && add_fast(&mut self.limbs, m, shift as u32, negative != remove)
            {
                return;
            }
        }
        self.add_slow(wides, term, remove);
    }

    /// `add` for a value below the scale, one that overflows the limbs, or a NaN or infinity.
    #[cold]
    fn add_slow(&mut self, wides: &mut Wides, term: Term, remove: bool) {
        let count = if remove { -1 } else { 1 };
        let (negative, m, e) = match term {
            Term::Finite { negative, m, e } => (negative != remove, m, e),
            Term::Nan => {
                self.wide_mut(wides).nan += count;
                return;
            }
            Term::Infinite { negative } => {
                self.wide_mut(wides).infinite[usize::from(negative)] += count;
                return;
            }
        };
        if self.wide == 0 {
            if let Some((limbs, scale)) = self.plus(&halves(m), negative, e) {
                (self.limbs, self.scale) = (limbs, scale);
                return;
            }
            self.promote(wides);
        }
        let wide = &mut wides.entries[self.wide as usize - 1];
        let fits = accumulate(
            &mut wide.limbs,
            &halves(m),
            (e - WIDE_SCALE) as u32,
            negative,
        );
        debug_assert!(fits, "a wide sum holds every sum");
    }

    /// Adds the sum `other`, whose wide integer, if any, is in `theirs`.
    pub fn merge(&mut self, wides: &mut Wides, other: &Self, theirs: &Wides) {
        if other.wide != 0 {
            let other = &theirs.entries[other.wide as usize - 1];
            let wide = self.wide_mut(wides);
            wide.nan += other.nan;
            wide.infinite[0] += other.infinite[0];
            wide.infinite[1] += other.infinite[1];
            let (negative, magnitude) = magnitude_of(other.limbs);
            let fits = accumulate(&mut wide.limbs, &magnitude, 0, negative);
            debug_assert!(fits, "a wide sum holds every sum");
            return;
        }
        if other.limbs == [0; N] {
            return;
        }
        if self.wide == 0 {
            if self.scale == i32::MAX {
                (self.limbs, self.scale) = (other.limbs, other.scale);
                return;
            }
            // Most merges add a sum at the same or a larger scale that fits.
            let shift = i64::from(other.scale) - i64::from(self.scale);
            if (0..64 * N as i64).contains(&shift)
                && merge_fast(&mut self.limbs, &other.limbs, shift as u32)
            {
                return;
            }
        }
        let (negative, magnitude) = magnitude_of(other.limbs);
        if self.wide == 0 {
            if let Some((limbs, scale)) = self.plus(&magnitude, negative, other.scale) {
                (self.limbs, self.scale) = (limbs, scale);
                return;
            }
            self.promote(wides);
        }
        let wide = &mut wides.entries[self.wide as usize - 1];
        let shift = (other.scale - WIDE_SCALE) as u32;
        let fits = accumulate(&mut wide.limbs, &magnitude, shift, negative);
        debug_assert!(fits, "a wide sum holds every sum");
    }

    /// Empties the sum, freeing its wide integer.
    pub fn clear(&mut self, wides: &mut Wides) {
        if self.wide != 0 {
            wides.free.push(self.wide - 1);
        }
        *self = Self::default();
    }

    /// The sum rounded to float64. NaN if a value is NaN or the values hold both infinities, else
    /// the infinity they hold; a finite sum too large for float64 rounds to an infinity, and an
    /// exact zero is +0.
    pub fn value(&self, wides: &Wides) -> f64 {
        if let Some((negative, top, exp)) = self.narrow() {
            return to_f64(negative, top, false, exp);
        }
        self.read(
            wides,
            |value| value,
            |negative, magnitude, scale| {
                let (top, sticky, exp) = leading(magnitude, scale);
                to_f64(negative, top, sticky, exp)
            },
        )
    }

    /// The sum divided by `n`, rounded once.
    pub fn mean(&self, n: i64, wides: &Wides) -> f64 {
        if let Some((negative, top, exp)) = self.narrow() {
            return quotient(negative, top, false, exp, n as u64);
        }
        self.read(
            wides,
            |value| value,
            |negative, magnitude, scale| {
                let (top, sticky, exp) = leading(magnitude, scale);
                quotient(negative, top, sticky, exp, n as u64)
            },
        )
    }

    /// A sum held in 128 bits as `±top·2^exp`, `top` normalized as `leading`'s (or 0): the common
    /// case of `value` and `mean`, without `leading`'s walk over limbs.
    fn narrow(&self) -> Option<(bool, u128, i64)> {
        if self.wide != 0 || N != 2 {
            return None;
        }
        let sum = (u128::from(self.limbs[0]) | u128::from(self.limbs[1]) << 64) as i128;
        let magnitude = sum.unsigned_abs();
        if magnitude == 0 {
            return Some((false, 0, 0));
        }
        let zeros = magnitude.leading_zeros();
        let exp = i64::from(self.scale) - i64::from(zeros);
        Some((sum < 0, magnitude << zeros, exp))
    }

    /// `finite(negative, magnitude, scale)` for a finite sum, `±magnitude·2^scale`, or
    /// `special` of the NaN or infinity the sum is.
    fn read<R>(
        &self,
        wides: &Wides,
        special: impl FnOnce(f64) -> R,
        finite: impl FnOnce(bool, &[u64], i64) -> R,
    ) -> R {
        if self.wide == 0 {
            let (negative, magnitude) = magnitude_of(self.limbs);
            return finite(negative, &magnitude, i64::from(self.scale));
        }
        let wide = &wides.entries[self.wide as usize - 1];
        let [positive, negative] = wide.infinite;
        if wide.nan > 0 || (positive > 0 && negative > 0) {
            return special(f64::NAN);
        }
        if positive > 0 {
            return special(f64::INFINITY);
        }
        if negative > 0 {
            return special(f64::NEG_INFINITY);
        }
        let (negative, magnitude) = magnitude_of(wide.limbs);
        finite(negative, &magnitude, i64::from(WIDE_SCALE))
    }

    /// This sum plus `±magnitude·2^e`, at the smaller of the two scales, if it fits `N` limbs.
    fn plus(&self, magnitude: &[u64], negative: bool, e: i32) -> Option<([u64; N], i32)> {
        let empty = self.limbs == [0; N];
        let scale = if empty { e } else { self.scale.min(e) };
        let mut limbs = [0; N];
        if !empty {
            let (held_negative, held) = magnitude_of(self.limbs);
            let shift = (self.scale - scale) as u32;
            if !accumulate(&mut limbs, &held, shift, held_negative) {
                return None;
            }
        }
        accumulate(&mut limbs, magnitude, (e - scale) as u32, negative).then_some((limbs, scale))
    }

    fn wide_mut<'a>(&mut self, wides: &'a mut Wides) -> &'a mut Wide {
        if self.wide == 0 {
            self.promote(wides);
        }
        &mut wides.entries[self.wide as usize - 1]
    }

    /// Moves the sum to a wide integer.
    fn promote(&mut self, wides: &mut Wides) {
        let mut wide = Wide {
            limbs: [0; WIDE_LIMBS],
            nan: 0,
            infinite: [0; 2],
        };
        if self.limbs != [0; N] {
            let (negative, magnitude) = magnitude_of(self.limbs);
            let shift = (self.scale - WIDE_SCALE) as u32;
            let fits = accumulate(&mut wide.limbs, &magnitude, shift, negative);
            debug_assert!(fits, "a wide sum holds every sum");
        }
        self.wide = wides.insert(wide);
    }

    /// The finite sum's magnitude, without zero limbs at either end, into `magnitude`, and its
    /// scale; `None` for a NaN or an infinity.
    fn magnitude_into(&self, wides: &Wides, magnitude: &mut Vec<u64>) -> Option<i64> {
        self.read(
            wides,
            |_| None,
            |_, held, scale| {
                let low = held.iter().take_while(|limb| **limb == 0).count();
                let high = held
                    .iter()
                    .rposition(|limb| *limb != 0)
                    .map_or(low, |t| t + 1);
                magnitude.clear();
                magnitude.extend_from_slice(&held[low..high]);
                Some(scale + 64 * low as i64)
            },
        )
    }
}

/// Buffers for `sample_std`, reused across groups.
#[derive(Default)]
pub(crate) struct Scratch {
    sum: Vec<u64>,
    squares: Vec<u64>,
    product: Vec<u64>,
    square: Vec<u64>,
}

/// The sample standard deviation of `n > 1` values from their exact sum and sum of squares:
/// `sqrt((n·Σx² − (Σx)²) / (n(n−1)))`, the variance formed exactly (so it is exactly 0 when the
/// values are equal) and its square root rounded once. NaN if any value is NaN or infinite.
pub(crate) fn sample_std(
    n: i64,
    sum: &Exact<2>,
    squares: &Exact<4>,
    wides: &Wides,
    scratch: &mut Scratch,
) -> f64 {
    let Scratch {
        sum: s,
        squares: q,
        product,
        square,
    } = scratch;
    let (Some(s_scale), Some(q_scale)) = (
        sum.magnitude_into(wides, s),
        squares.magnitude_into(wides, q),
    ) else {
        return f64::NAN;
    };
    let n = n as u64;
    multiply(q, &[n], product);
    multiply(s, s, square);
    let s_scale = 2 * s_scale;
    let unit = q_scale.min(s_scale);
    shift_left(product, (q_scale - unit) as usize);
    shift_left(square, (s_scale - unit) as usize);
    subtract(product, square);
    if product.is_empty() {
        return 0.0;
    }
    // The variance's numerator to four limbs, from its top, which leaves a quotient of over 2^128
    // by a divisor below 2^64; to five, flooring by each factor in turn (as dividing by their
    // product floors), for a larger one. Bits below them, and the remainders, need only be known
    // nonzero.
    let (single, both) = (u64::try_from(u128::from(n) * u128::from(n - 1)), [n, n - 1]);
    let (width, divisors) = match &single {
        Ok(divisor) => (4, std::slice::from_ref(divisor)),
        Err(_) => (5, &both[..]),
    };
    let mut limbs = [0; 5];
    let limbs = &mut limbs[..width];
    let len = product.len();
    let kept = len.min(width);
    limbs[width - kept..].copy_from_slice(&product[len - kept..]);
    let mut sticky = product[..len - kept].iter().any(|limb| *limb != 0);
    for &divisor in divisors {
        sticky |= divide(limbs, divisor) != 0;
    }
    let (top, below, exp) = leading(limbs, unit + 64 * (len as i64 - width as i64));
    square_root(top, sticky || below, exp)
}

/// `sum / n` rounded once to float64, for an integer sum.
pub(crate) fn int_mean(sum: i128, n: i64) -> f64 {
    let (top, sticky, exp) = leading(&halves(sum.unsigned_abs()), 0);
    quotient(sum < 0, top, sticky, exp, n as u64)
}

fn halves(m: u128) -> [u64; 2] {
    [m as u64, (m >> 64) as u64]
}

/// A two's complement integer's sign and magnitude.
fn magnitude_of<const N: usize>(mut limbs: [u64; N]) -> (bool, [u64; N]) {
    let negative = limbs[N - 1] >> 63 != 0;
    if negative {
        let mut carry = true;
        for limb in &mut limbs {
            (*limb, carry) = (!*limb).overflowing_add(u64::from(carry));
        }
    }
    (negative, limbs)
}

/// Adds `±magnitude·2^shift` to the two's complement integer `sum`. False if the value or the
/// result does not fit `sum`'s limbs with its sign, leaving `sum` wrong.
#[inline]
fn accumulate(sum: &mut [u64], magnitude: &[u64], shift: u32, negative: bool) -> bool {
    let Some(top) = magnitude.iter().rposition(|limb| *limb != 0) else {
        return true;
    };
    let len = sum.len();
    let bits = 64 * top as u64 + u64::from(64 - magnitude[top].leading_zeros()) + u64::from(shift);
    if bits >= 64 * len as u64 {
        return false;
    }
    let sign = sum[len - 1] >> 63;
    let (mut at, bit) = ((shift / 64) as usize, shift % 64);
    let (mut carry, mut spill) = (false, 0);
    for &limb in &magnitude[..=top] {
        let part = if bit == 0 { limb } else { limb << bit | spill };
        spill = if bit == 0 { 0 } else { limb >> (64 - bit) };
        carry = step(&mut sum[at], part, carry, negative);
        at += 1;
    }
    if spill != 0 {
        carry = step(&mut sum[at], spill, carry, negative);
        at += 1;
    }
    while carry && at < len {
        carry = step(&mut sum[at], 0, carry, negative);
        at += 1;
    }
    // The value added is below 2^(64·len − 1), so the sum overflowed exactly when adding flipped
    // a nonnegative sum negative, or subtracting a negative sum nonnegative.
    let after = sum[len - 1] >> 63;
    if negative {
        !(sign == 1 && after == 0)
    } else {
        !(sign == 0 && after == 1)
    }
}

/// `limb ± part ± carry`, returning the carry (or borrow) out.
#[inline]
fn step(limb: &mut u64, part: u64, carry: bool, subtract: bool) -> bool {
    let (value, first, second);
    if subtract {
        (value, first) = limb.overflowing_sub(part);
        (*limb, second) = value.overflowing_sub(u64::from(carry));
    } else {
        (value, first) = limb.overflowing_add(part);
        (*limb, second) = value.overflowing_add(u64::from(carry));
    }
    first || second
}

/// The highest 128 bits of `magnitude`, normalized so their high bit is set, whether any bit
/// below them is set, and the exponent of their last bit, given that of the magnitude's last;
/// all zero for a zero magnitude.
fn leading(magnitude: &[u64], scale: i64) -> (u128, bool, i64) {
    let Some(top) = magnitude.iter().rposition(|limb| *limb != 0) else {
        return (0, false, 0);
    };
    let at = |i: usize| top.checked_sub(i).map_or(0, |i| magnitude[i]);
    let zeros = magnitude[top].leading_zeros();
    let window = u128::from(at(0)) << 64 | u128::from(at(1));
    let below = at(2);
    let (high, rest) = if zeros == 0 {
        (window, below)
    } else {
        (
            window << zeros | u128::from(below >> (64 - zeros)),
            below << zeros,
        )
    };
    let sticky = rest != 0 || magnitude[..top.saturating_sub(2)].iter().any(|l| *l != 0);
    (
        high,
        sticky,
        scale + 64 * (top as i64 - 1) - i64::from(zeros),
    )
}

/// `±(top + ε)·2^exp / n` rounded once to float64, for `top` and `ε` as `to_f64`'s.
fn quotient(negative: bool, top: u128, sticky: bool, exp: i64, n: u64) -> f64 {
    if top == 0 {
        return 0.0;
    }
    // `top` is at least 2^127 and `n` below 2^64, so the quotient keeps at least 64 bits, more
    // than rounding reads: the bits `top` dropped and the remainder need only be known nonzero.
    let mut limbs = [top as u64, (top >> 64) as u64];
    let remainder = divide(&mut limbs, n);
    let q = u128::from(limbs[0]) | u128::from(limbs[1]) << 64;
    let zeros = q.leading_zeros();
    to_f64(
        negative,
        q << zeros,
        sticky || remainder != 0,
        exp - i64::from(zeros),
    )
}

/// `sqrt((top + ε)·2^exp)` rounded once to float64, for `top` and `ε` as `to_f64`'s, `top`
/// nonzero.
fn square_root(top: u128, sticky: bool, exp: i64) -> f64 {
    // An even exponent halves exactly. `top` keeps at least 127 bits, so its integer root keeps
    // 64, and the root's bits below them, like `top`'s, need only be known nonzero.
    let (top, sticky, exp) = if exp % 2 == 0 {
        (top, sticky, exp)
    } else {
        (top >> 1, sticky || top & 1 == 1, exp + 1)
    };
    let root = integer_root(top);
    to_f64(
        false,
        root << 64,
        sticky || root * root != top,
        exp / 2 - 64,
    )
}

/// The integer square root of `x`, at least 2^126: a float64 estimate, within a few thousand,
/// one Newton step in float64, within one, then exact comparisons, all without the software
/// 128-bit division `isqrt` uses.
fn integer_root(x: u128) -> u128 {
    let estimate = ((x as f64).sqrt() as u128).min(u128::from(u64::MAX));
    let error = x.wrapping_sub(estimate * estimate) as i128;
    let step = (error as f64 / (2.0 * estimate as f64)).round() as i128;
    let mut root = (estimate as i128 + step) as u128;
    while root.checked_mul(root).is_none_or(|square| square > x) {
        root -= 1;
    }
    while (root + 1)
        .checked_mul(root + 1)
        .is_some_and(|square| square <= x)
    {
        root += 1;
    }
    root
}

/// Divides `limbs` by `d` in place, returning the remainder.
fn divide(limbs: &mut [u64], d: u64) -> u64 {
    let mut remainder = 0;
    for limb in limbs.iter_mut().rev() {
        (*limb, remainder) = divide_step(remainder, *limb, d);
    }
    remainder
}

/// `(high·2^64 + low) / d` and its remainder, for `high < d`. A `d` below 2^32 takes two
/// hardware divisions of 64 bits, each of a remainder below `d` and 32 more bits, where a
/// 128-bit division is a software routine many times slower.
#[inline]
fn divide_step(high: u64, low: u64, d: u64) -> (u64, u64) {
    if d >> 32 == 0 {
        let upper = high << 32 | low >> 32;
        let (q1, r1) = (upper / d, upper % d);
        let lower = r1 << 32 | (low & 0xffff_ffff);
        return ((q1 << 32) | (lower / d), lower % d);
    }
    let x = u128::from(high) << 64 | u128::from(low);
    ((x / u128::from(d)) as u64, (x % u128::from(d)) as u64)
}

/// `±(top + ε)·2^exp` rounded to the nearest float64, ties to even, where `top` is 0 or has its
/// high bit set and `ε`, in (0, 1) if `sticky` and 0 otherwise, stands for the bits below it.
fn to_f64(negative: bool, top: u128, sticky: bool, exp: i64) -> f64 {
    if top == 0 {
        return 0.0;
    }
    let sign = u64::from(negative) << 63;
    let high = exp + 127;
    if high >= 1024 {
        return f64::from_bits(sign | INFINITY_BITS);
    }
    let normal = high >= -1022;
    // The bits below the last one kept: a normal result keeps 53; a subnormal one ends at 2^-1074.
    let dropped = if normal { 75 } else { -1074 - exp };
    if dropped > 128 {
        // Below half the smallest subnormal.
        return f64::from_bits(sign);
    }
    let (kept, rest, half) = if dropped == 128 {
        (0, top, 1 << 127)
    } else {
        let dropped = dropped as u32;
        (
            (top >> dropped) as u64,
            top & ((1 << dropped) - 1),
            1 << (dropped - 1),
        )
    };
    let up = rest > half || (rest == half && (sticky || kept & 1 == 1));
    let kept = kept + u64::from(up);
    let bits = if normal {
        let (kept, unit) = if kept == 1 << 53 {
            (1 << 52, exp + dropped + 1)
        } else {
            (kept, exp + dropped)
        };
        let biased = unit + 52 + 1023;
        if biased >= 2047 {
            INFINITY_BITS
        } else {
            (biased as u64) << 52 | (kept & ((1 << 52) - 1))
        }
    } else {
        // In units of 2^-1074, which a subnormal's bits are; rounding up to 2^52 gives the
        // smallest normal's bits.
        kept
    };
    f64::from_bits(sign | bits)
}

/// `a · b` into `out`, without high zero limbs.
fn multiply(a: &[u64], b: &[u64], out: &mut Vec<u64>) {
    out.clear();
    out.resize(a.len() + b.len(), 0);
    for (i, &x) in a.iter().enumerate() {
        let mut carry = 0u128;
        for (j, &y) in b.iter().enumerate() {
            let product = u128::from(x) * u128::from(y) + u128::from(out[i + j]) + carry;
            out[i + j] = product as u64;
            carry = product >> 64;
        }
        out[i + b.len()] = carry as u64;
    }
    while out.last() == Some(&0) {
        out.pop();
    }
}

fn shift_left(value: &mut Vec<u64>, bits: usize) {
    if value.is_empty() {
        return;
    }
    let (words, bits) = (bits / 64, (bits % 64) as u32);
    if bits > 0 {
        let mut carry = 0;
        for limb in value.iter_mut() {
            let next = *limb >> (64 - bits);
            *limb = *limb << bits | carry;
            carry = next;
        }
        if carry != 0 {
            value.push(carry);
        }
    }
    if words > 0 {
        value.splice(0..0, std::iter::repeat_n(0, words));
    }
}

/// `value -= other`, for `value >= other`, without high zero limbs.
fn subtract(value: &mut Vec<u64>, other: &[u64]) {
    let mut borrow = false;
    for (i, limb) in value.iter_mut().enumerate() {
        if i >= other.len() && !borrow {
            break;
        }
        borrow = step(limb, other.get(i).copied().unwrap_or(0), borrow, true);
    }
    debug_assert!(!borrow, "n·Σx² ≥ (Σx)²");
    while value.last() == Some(&0) {
        value.pop();
    }
}
