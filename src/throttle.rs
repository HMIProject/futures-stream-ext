// SPDX-FileCopyrightText: The futures-stream-ext authors
// SPDX-License-Identifier: MPL-2.0

use std::{
    num::NonZeroUsize,
    pin::Pin,
    task::{Context, Poll, ready},
    time::Duration,
};

use futures_util::{
    Stream, StreamExt as _,
    stream::{Fuse, FusedStream as _},
};
use pin_project_lite::pin_project;

use crate::{IntervalEdge, Sleep, pending_and_wake_if, poll_latest_ready, reset_or_sleep};

/// Throttling strategy of [`Throttle`].
///
/// Ticks control when the pending item of the throttled stream is yielded.
pub trait Throttler {
    /// Polls for the next tick.
    ///
    /// Returns `Ready` if the pending item of the throttled stream should be
    /// yielded now. Like [`Future::poll()`], it must register the waker of `cx`
    /// when returning `Pending`.
    fn poll_tick(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()>;

    /// A new item has been received from the input stream while idle.
    ///
    /// The throttler is idle initially and after `on_tick` has been
    /// called with `false`. This method is not called again until then.
    ///
    /// After invocation of this method `on_tick` will be called
    /// with `true` when the next tick is ready. The current
    /// item that will be yielded may still change until `on_tick`
    /// is called.
    ///
    /// [`poll_tick()`](Self::poll_tick) will be invoked immediately after this
    /// method returns.
    fn on_first_item(self: Pin<&mut Self>);

    /// A tick has been ready.
    ///
    /// Called after each tick before `poll_tick()` is invoked again.
    /// `item_yielded` is `true` if a pending item of the throttled stream is
    /// about to be yielded and `false` if no item has been received from the
    /// input stream since the last tick. The throttler should start the next
    /// interval only if `item_yielded` is `true`.
    fn on_tick(self: Pin<&mut Self>, item_yielded: bool);
}

pin_project! {
    /// Result of [`StreamExt::throttle()`](crate::StreamExt::throttle).
    #[derive(Debug)]
    #[must_use = "streams do nothing unless polled"]
    pub struct Throttle<St: Stream, Th> {
        #[pin]
        stream: Fuse<St>,
        #[pin]
        throttler: Th,
        max_ready_count: NonZeroUsize,
        pending_item: Option<St::Item>,
        // Set until `on_first_item` has been called and
        // again after `on_tick` has been called with `false`.
        throttler_idle: bool,
    }
}

impl<St, Th> Throttle<St, Th>
where
    St: Stream,
    Th: Throttler,
{
    pub(crate) fn new(stream: St, throttler: Th, max_ready_count: NonZeroUsize) -> Self {
        Self {
            stream: stream.fuse(),
            throttler,
            max_ready_count,
            pending_item: None,
            throttler_idle: true,
        }
    }
}

impl<St, Th> Stream for Throttle<St, Th>
where
    St: Stream,
    Th: Throttler,
{
    type Item = St::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        // We want to receive the most recent item that is ready.
        let (latest_item, stream_maybe_ready) =
            poll_latest_ready(this.stream.as_mut(), cx, *this.max_ready_count);
        if let Some(latest_item) = latest_item {
            if *this.throttler_idle {
                *this.throttler_idle = false;
                this.throttler.as_mut().on_first_item();
            }
            *this.pending_item = Some(latest_item);
        }

        if this.stream.is_terminated() {
            if this.pending_item.is_none() {
                // Stream has finished.
                return Poll::Ready(None);
            }
            // Yield the last pending item when the throttler is ready.
            ready!(this.throttler.as_mut().poll_tick(cx));
            return Poll::Ready(this.pending_item.take());
        }

        if this.throttler.as_mut().poll_tick(cx).is_ready() {
            let yielded_item = this.pending_item.take();
            this.throttler.as_mut().on_tick(yielded_item.is_some());
            if yielded_item.is_none() {
                *this.throttler_idle = true;
            } else {
                return Poll::Ready(yielded_item);
            }
        }
        pending_and_wake_if(cx, stream_maybe_ready)
    }
}

#[derive(Debug, Clone, Copy)]
enum IntervalThrottlerState {
    /// No items are pending.
    Idle,
    /// Items are pending and the next tick is ready immediately.
    TickReady,
    /// Items are pending and the next tick is ready when `sleep` has elapsed.
    Sleeping,
}

pin_project! {
    /// Throttles a stream by using a fixed interval.
    ///
    /// The interval is implemented by the given [`Sleep`] type. While items are
    /// pending, intervals follow a fixed schedule with missed intervals skipped
    /// (see [`Sleep::reset_next_period()`]).
    ///
    /// The `period` is the minimum interval between subsequent items, which
    /// limits the maximum frequency of the throttled stream. The `edge`
    /// controls whether a pending item is yielded at the start or at the end
    /// of an interval.
    ///
    /// In the following marble diagram, time passes from left to right, `|`
    /// marks the end of a stream, and the period spans 4 columns:
    ///
    /// ```text
    /// input:     -ab-c-d-e----------f-g------|
    /// leading:   -a---c---e---------f---g----|
    /// trailing:  -----c---e-------------g----|
    /// ```
    ///
    /// With [`IntervalEdge::Leading`], the first item of a burst is yielded
    /// immediately. With [`IntervalEdge::Trailing`], the first interval
    /// starts when the first item arrives. With both edges, the latest pending
    /// item is yielded at the end of each interval, and all other items are
    /// discarded. Intervals continue on a fixed schedule while items are
    /// pending. After an interval without items the throttler is idle again,
    /// e.g. before `f` arrives.
    ///
    /// The `max_ready_count` of [`StreamExt::throttle()`](crate::StreamExt::throttle)
    /// only affects which items are yielded if a tick is ready while multiple
    /// items are ready at the same time:
    ///
    /// - With [`IntervalEdge::Trailing`], items that are ready at the same time
    ///   fall into the same interval anyway. A greater value only improves the
    ///   efficiency.
    /// - With [`IntervalEdge::Leading`], the first item of a burst is yielded
    ///   immediately. [`NonZeroUsize::MIN`] yields the first ready item, a greater
    ///   value the latest of up to `max_ready_count` items that are
    ///   ready at the same time.
    /// - With a zero period, [`NonZeroUsize::MIN`] passes every item through,
    ///   a greater value skips all but the latest of the items that are ready at
    ///   the same time.
    #[derive(Debug)]
    #[project = IntervalThrottlerProjection]
    pub struct IntervalThrottler<S> {
        period: Duration,
        edge: IntervalEdge,
        // Created on demand and reused afterwards.
        #[pin]
        sleep: Option<S>,
        state: IntervalThrottlerState,
    }
}

impl<S: Sleep> IntervalThrottler<S> {
    #[must_use]
    pub const fn new(period: Duration, edge: IntervalEdge) -> Self {
        Self {
            period,
            edge,
            sleep: None,
            state: IntervalThrottlerState::Idle,
        }
    }
}

/// Starts the first or the next interval.
fn start_interval<S: Sleep>(
    sleep: Pin<&mut Option<S>>,
    period: Duration,
) -> IntervalThrottlerState {
    if period.is_zero() {
        // No throttling.
        return IntervalThrottlerState::TickReady;
    }
    reset_or_sleep(sleep, period);
    IntervalThrottlerState::Sleeping
}

impl<S: Sleep> Throttler for IntervalThrottler<S> {
    fn poll_tick(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let IntervalThrottlerProjection { sleep, state, .. } = self.project();
        match state {
            IntervalThrottlerState::Idle => return Poll::Pending,
            // The first tick or no throttling.
            IntervalThrottlerState::TickReady => (),
            IntervalThrottlerState::Sleeping => {
                let sleep = sleep
                    .as_pin_mut()
                    .expect("sleep must exist while waiting for it");
                ready!(sleep.poll(cx));
            }
        }
        // The next interval is started by `on_tick()`.
        Poll::Ready(())
    }

    fn on_first_item(self: Pin<&mut Self>) {
        let IntervalThrottlerProjection {
            period,
            edge,
            sleep,
            state,
        } = self.project();
        debug_assert!(matches!(state, IntervalThrottlerState::Idle));
        *state = match edge {
            // The first tick is ready immediately.
            IntervalEdge::Leading => IntervalThrottlerState::TickReady,
            IntervalEdge::Trailing => start_interval(sleep, *period),
        };
    }

    fn on_tick(self: Pin<&mut Self>, item_yielded: bool) {
        let IntervalThrottlerProjection {
            period,
            sleep,
            state,
            ..
        } = self.project();
        if !item_yielded {
            // Leave the elapsed sleep unarmed until items are pending again.
            *state = IntervalThrottlerState::Idle;
            return;
        }
        match state {
            IntervalThrottlerState::Idle => panic!("on_tick called while idle"),
            IntervalThrottlerState::TickReady => *state = start_interval(sleep, *period),
            IntervalThrottlerState::Sleeping => {
                // Continue on a fixed schedule.
                sleep
                    .as_pin_mut()
                    .expect("sleep must exist while waiting for it")
                    .reset_next_period(*period);
            }
        }
    }
}
