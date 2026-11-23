// SPDX-FileCopyrightText: The futures-stream-ext authors
// SPDX-License-Identifier: MPL-2.0

//! [`Sleep`](crate::Sleep) implementation for the Tokio runtime.

use std::{pin::Pin, time::Duration};

/// [`IntervalThrottler`](crate::IntervalThrottler) using [`tokio::time::Sleep`].
pub type TokioIntervalThrottler = crate::IntervalThrottler<tokio::time::Sleep>;

impl crate::Sleep for tokio::time::Sleep {
    fn sleep(duration: Duration) -> Self {
        tokio::time::sleep(duration)
    }

    fn reset(self: Pin<&mut Self>, duration: Duration) {
        // Reuses the registered timer.
        tokio::time::Sleep::reset(self, tokio::time::Instant::now() + duration);
    }

    fn reset_next_period(self: Pin<&mut Self>, period: Duration) {
        let now = tokio::time::Instant::now();
        let delay =
            crate::next_period_delay(now.saturating_duration_since(self.deadline()), period);
        tokio::time::Sleep::reset(self, now + delay);
    }
}

#[cfg(test)]
mod tests;
