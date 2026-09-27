//! Microsecond timestamps with RFC 3339 parsing and formatting.

use serde::{Deserialize, Serialize};
use std::{fmt, str::FromStr};
use thiserror::Error;

const MICROS_PER_SECOND: i64 = 1_000_000;
const SECONDS_PER_DAY: i64 = 86_400;

/// Reasons a timestamp is rejected.
#[derive(Clone, Debug, Eq, PartialEq, Error)]
pub enum TimestampError {
    /// The instant is before year 0000 or after year 9999.
    #[error("timestamp {micros} is out of range; timestamps must fall in years 0000 through 9999")]
    OutOfRange {
        /// The rejected instant, in microseconds since the Unix epoch.
        micros: i64,
    },
    /// A string is not a valid RFC 3339 date-time.
    #[error("'{value}' is not an RFC 3339 timestamp: {reason}")]
    InvalidRfc3339 {
        /// The rejected string.
        value: String,
        /// What is wrong with it.
        reason: &'static str,
    },
}

/// An instant stored as `i64` microseconds since the Unix epoch,
/// 1970-01-01T00:00:00Z.
///
/// Values are limited to 0000-01-01T00:00:00Z through
/// 9999-12-31T23:59:59.999999Z so every timestamp has an RFC 3339 form.
/// Serialized as the integer microsecond count.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(try_from = "i64", into = "i64")]
pub struct Timestamp(i64);

impl Timestamp {
    /// Earliest representable instant, 0000-01-01T00:00:00Z.
    pub const MIN: Self = Self(-62_167_219_200_000_000);
    /// Latest representable instant, 9999-12-31T23:59:59.999999Z.
    pub const MAX: Self = Self(253_402_300_799_999_999);
    /// The Unix epoch.
    pub const UNIX_EPOCH: Self = Self(0);

    /// Build a timestamp from microseconds since the Unix epoch.
    ///
    /// # Errors
    ///
    /// Returns [`TimestampError::OutOfRange`] outside years 0000 to 9999.
    pub fn from_micros(micros: i64) -> Result<Self, TimestampError> {
        if (Self::MIN.0..=Self::MAX.0).contains(&micros) {
            Ok(Self(micros))
        } else {
            Err(TimestampError::OutOfRange { micros })
        }
    }

    /// Microseconds since the Unix epoch.
    #[must_use]
    pub fn as_micros(self) -> i64 {
        self.0
    }

    /// Parse an RFC 3339 date-time such as `2026-09-27T12:30:00.5+02:00`.
    ///
    /// The date and time separator may be `T`, `t`, or a space, and the
    /// offset `Z`, `z`, or `±HH:MM`. Fractions finer than a microsecond are
    /// truncated. Leap seconds (`:60`) are rejected.
    ///
    /// # Errors
    ///
    /// Returns [`TimestampError::InvalidRfc3339`] for malformed input and
    /// [`TimestampError::OutOfRange`] when the offset moves the instant
    /// outside years 0000 to 9999.
    pub fn parse_rfc3339(value: &str) -> Result<Self, TimestampError> {
        let micros = parse_rfc3339_micros(value.as_bytes()).map_err(|reason| {
            TimestampError::InvalidRfc3339 {
                value: value.to_owned(),
                reason,
            }
        })?;
        Self::from_micros(micros)
    }

    /// Format as RFC 3339 in UTC, for example `2026-09-27T10:30:00Z`, with a
    /// six-digit fraction only when the sub-second part is non-zero.
    #[must_use]
    pub fn to_rfc3339(self) -> String {
        let seconds = self.0.div_euclid(MICROS_PER_SECOND);
        let fraction = self.0.rem_euclid(MICROS_PER_SECOND);
        let days = seconds.div_euclid(SECONDS_PER_DAY);
        let second_of_day = seconds.rem_euclid(SECONDS_PER_DAY);
        let (year, month, day) = civil_from_days(days);
        let hour = second_of_day / 3_600;
        let minute = second_of_day % 3_600 / 60;
        let second = second_of_day % 60;
        let mut formatted =
            format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}");
        if fraction != 0 {
            formatted.push_str(&format!(".{fraction:06}"));
        }
        formatted.push('Z');
        formatted
    }
}

impl TryFrom<i64> for Timestamp {
    type Error = TimestampError;

    fn try_from(micros: i64) -> Result<Self, Self::Error> {
        Self::from_micros(micros)
    }
}

impl From<Timestamp> for i64 {
    fn from(timestamp: Timestamp) -> Self {
        timestamp.0
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.to_rfc3339())
    }
}

impl FromStr for Timestamp {
    type Err = TimestampError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse_rfc3339(value)
    }
}

fn digits(bytes: &[u8], start: usize, len: usize) -> Result<i64, &'static str> {
    let slice = bytes
        .get(start..start + len)
        .ok_or("unexpected end of input")?;
    slice.iter().try_fold(0_i64, |acc, byte| {
        if byte.is_ascii_digit() {
            Ok(acc * 10 + i64::from(byte - b'0'))
        } else {
            Err("expected a digit")
        }
    })
}

fn expect(
    bytes: &[u8],
    position: usize,
    allowed: &[u8],
    reason: &'static str,
) -> Result<(), &'static str> {
    match bytes.get(position) {
        Some(byte) if allowed.contains(byte) => Ok(()),
        _ => Err(reason),
    }
}

fn parse_rfc3339_micros(bytes: &[u8]) -> Result<i64, &'static str> {
    let year = digits(bytes, 0, 4)?;
    expect(bytes, 4, b"-", "expected '-' after the year")?;
    let month = digits(bytes, 5, 2)?;
    expect(bytes, 7, b"-", "expected '-' after the month")?;
    let day = digits(bytes, 8, 2)?;
    expect(bytes, 10, b"Tt ", "expected 'T' between date and time")?;
    let hour = digits(bytes, 11, 2)?;
    expect(bytes, 13, b":", "expected ':' after the hour")?;
    let minute = digits(bytes, 14, 2)?;
    expect(bytes, 16, b":", "expected ':' after the minute")?;
    let second = digits(bytes, 17, 2)?;

    let mut position = 19;
    let mut fraction_micros = 0_i64;
    if bytes.get(position) == Some(&b'.') {
        position += 1;
        let start = position;
        while bytes.get(position).is_some_and(u8::is_ascii_digit) {
            position += 1;
        }
        if position == start {
            return Err("expected digits after '.'");
        }
        let kept = (position - start).min(6);
        let value = digits(bytes, start, kept)?;
        let scale = 10_i64.pow(u32::try_from(6 - kept).map_err(|_| "fraction too long")?);
        fraction_micros = value * scale;
    }

    let offset_seconds = match bytes.get(position) {
        Some(b'Z' | b'z') => {
            position += 1;
            0
        }
        Some(sign @ (b'+' | b'-')) => {
            let offset_hour = digits(bytes, position + 1, 2)?;
            expect(bytes, position + 3, b":", "expected ':' in the offset")?;
            let offset_minute = digits(bytes, position + 4, 2)?;
            if offset_hour > 23 || offset_minute > 59 {
                return Err("offset out of range");
            }
            position += 6;
            let magnitude = offset_hour * 3_600 + offset_minute * 60;
            if *sign == b'-' { -magnitude } else { magnitude }
        }
        _ => return Err("expected 'Z' or a '+HH:MM' offset"),
    };
    if position != bytes.len() {
        return Err("unexpected trailing characters");
    }

    if !(1..=12).contains(&month) {
        return Err("month out of range");
    }
    if day < 1 || day > days_in_month(year, month) {
        return Err("day out of range");
    }
    if hour > 23 || minute > 59 || second > 59 {
        return Err("time of day out of range");
    }

    let days = days_from_civil(year, month, day);
    let local_seconds = days * SECONDS_PER_DAY + hour * 3_600 + minute * 60 + second;
    Ok((local_seconds - offset_seconds) * MICROS_PER_SECOND + fraction_micros)
}

fn is_leap_year(year: i64) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

fn days_in_month(year: i64, month: i64) -> i64 {
    match month {
        2 if is_leap_year(year) => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

/// Days since 1970-01-01 for a proleptic Gregorian date (H. Hinnant).
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let year_of_era = year - era * 400;
    let shifted_month = (month + 9) % 12;
    let day_of_year = (153 * shifted_month + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

/// Proleptic Gregorian date for days since 1970-01-01 (H. Hinnant).
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let shifted = days + 719_468;
    let era = shifted.div_euclid(146_097);
    let day_of_era = shifted - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let shifted_month = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * shifted_month + 2) / 5 + 1;
    let month = if shifted_month < 10 {
        shifted_month + 3
    } else {
        shifted_month - 9
    };
    let year = year_of_era + era * 400 + i64::from(month <= 2);
    (year, month, day)
}
