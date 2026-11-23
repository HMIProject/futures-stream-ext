// SPDX-FileCopyrightText: The futures-stream-ext authors
// SPDX-License-Identifier: MPL-2.0

//! Extensions of the [`Stream`] trait and utilities for
//! transforming or shaping streams.

#![cfg_attr(docsrs, feature(doc_cfg))]

use std::{
    num::NonZeroUsize,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use futures_util::{Stream, stream::Fuse};

mod debounce;
pub use self::debounce::Debounce;

mod dedup;
pub use self::dedup::{Dedup, DedupBy, DedupByKey, DedupErr, DedupOk};

mod default_if_empty;
pub use self::default_if_empty::DefaultIfEmpty;

mod end_with;
pub use self::end_with::EndWith;

mod make_item;
pub use self::make_item::{DefaultItem, MakeItem};

mod switch_map;
pub use self::switch_map::SwitchMap;

mod throttle;
pub use self::throttle::{IntervalThrottler, Throttle, Throttler};

#[cfg(feature = "tokio")]
#[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
pub mod tokio;

/// Interval edge trigger variants
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum IntervalEdge {
    Leading,
    Trailing,
}

pub trait Sleep: Future<Output = ()> + Sized {
    /// Suspends the task for the given duration.
    fn sleep(duration: Duration) -> Self;

    /// Restarts the sleep to elapse after the given duration from now.
    ///
    /// Might be invoked both before and after the sleep has elapsed.
    ///
    /// The default implementation replaces `self` with a new sleep.
    /// Override it if the sleep could be reset more efficiently,
    /// e.g. by reusing an already registered timer.
    fn reset(mut self: Pin<&mut Self>, duration: Duration) {
        self.set(Self::sleep(duration));
    }

    /// Restarts the elapsed sleep for the next period of a fixed schedule.
    ///
    /// The next deadline should be the previous deadline plus `period`,
    /// independent of when the sleep has been polled. Missed periods
    /// should be skipped.
    ///
    /// The default implementation invokes [`reset()`](Self::reset) and
    /// starts the next period from now, i.e. the schedule may drift.
    /// Implementations that know their deadline should override it
    /// by using [`next_period_delay()`].
    fn reset_next_period(self: Pin<&mut Self>, period: Duration) {
        self.reset(period);
    }
}

/// Delay until the next deadline on a fixed schedule with missed periods skipped.
///
/// The `elapsed` argument is the time that has passed since the previous
/// deadline.
///
/// Similar to `tokio::time::MissedTickBehavior::Skip`, but without
/// any tolerance for slightly delayed ticks.
///
/// # Panics
///
/// Panics if `period` is zero.
#[must_use]
pub fn next_period_delay(elapsed: Duration, period: Duration) -> Duration {
    assert!(!period.is_zero());
    if elapsed < period {
        return period.saturating_sub(elapsed);
    }
    // Skip missed periods and align with the schedule.
    let behind_nanos = elapsed.as_nanos() % period.as_nanos();
    let behind =
        Duration::from_nanos(u64::try_from(behind_nanos).expect("remainder is less than period"));
    period.saturating_sub(behind)
}

/// Resets the sleep or creates a new one if none exists yet.
fn reset_or_sleep<S: Sleep>(mut sleep: Pin<&mut Option<S>>, duration: Duration) {
    if let Some(sleep) = sleep.as_mut().as_pin_mut() {
        sleep.reset(duration);
    } else {
        sleep.set(Some(S::sleep(duration)));
    }
}

/// Polls the latest ready item from the stream.
///
/// Continues polling the stream while ready, but at most `max_ready_count`
/// times in a row to prevent endless loops for streams that are always ready.
/// The fused stream is never polled again after it has finished.
///
/// Returns the last ready item, if any, and whether the stream might still
/// be ready.
fn poll_latest_ready<St: Stream>(
    mut stream: Pin<&mut Fuse<St>>,
    cx: &mut Context<'_>,
    max_ready_count: NonZeroUsize,
) -> (Option<St::Item>, bool) {
    let mut latest_item = None;
    let mut ready_count = 0;
    while let Poll::Ready(Some(next_item)) = stream.as_mut().poll_next(cx) {
        latest_item = Some(next_item);
        ready_count += 1;
        if ready_count >= max_ready_count.get() {
            return (latest_item, true);
        }
    }
    (latest_item, false)
}

/// Returns `Pending` and wakes the task if the stream might still be ready.
///
/// The self-wake ensures that polling the stream continues. It is not needed
/// when returning `Ready`, because the consumer will poll again anyway.
fn pending_and_wake_if<T>(cx: &mut Context<'_>, stream_maybe_ready: bool) -> Poll<T> {
    if stream_maybe_ready {
        cx.waker().wake_by_ref();
    }
    Poll::Pending
}

/// Extension trait for [`Stream`].
///
/// Runtime-agnostic. Methods that use the Tokio runtime require the `tokio`
/// feature.
///
/// # Polling ready items
///
/// The streams returned by some combinators have a `yield_after(max_ready_count)`
/// method. It limits how many ready items of the input stream are consumed
/// consecutively during a single invocation of [`Stream::poll_next()`]. After
/// reaching the limit, the stream wakes itself up and continues during the next
/// invocation. This prevents an endless loop for input streams that are always
/// ready.
///
/// - [`Debounce`], [`Throttle`] and [`SwitchMap`] default to
///   [`NonZeroUsize::MIN`]. They consume the items that are ready at the same
///   time, but only act on the latest one.
/// - The `dedup` streams, e.g. [`Dedup`], have no limit by default. Like
///   [`filter()`](futures_util::StreamExt::filter), they never wake themselves
///   up: polling an input stream that is always ready and yields only items
///   that are discarded never returns.
///
/// Choosing the value:
///
/// - [`NonZeroUsize::MIN`] is a safe default.
/// - A greater value skips more items at once and needs fewer self-wakeups.
///   A good choice is the expected burst size, e.g. the capacity of the
///   channel that feeds the input stream.
/// - Use [`NonZeroUsize::MAX`] only if the input stream is guaranteed to become
///   pending eventually. Otherwise, polling an input stream that is always
///   ready never returns.
///
/// Whether the value also affects which items are yielded is documented for
/// each combinator.
pub trait StreamExt: Stream {
    /// Filters out subsequent/adjacent stream items.
    ///
    /// Equivalent to `distinctUntilChanged` in RxJS.
    ///
    /// By default, never yields to the executor while discarding items.
    /// Use [`Dedup::yield_after()`] if the input stream might be always ready,
    /// see [Polling ready items](StreamExt#polling-ready-items).
    fn dedup(self) -> Dedup<Self>
    where
        Self: Sized,
        Self::Item: Clone + PartialEq,
    {
        Dedup::new(self)
    }

    /// Filters out subsequent/adjacent stream items that are equal according to `eq_fn`.
    ///
    /// `eq_fn` is invoked with the last passed through item and the next item,
    /// in this order. The next item is discarded if `eq_fn` returns `true`.
    /// The order matches `Itertools::dedup_by()`, but is the opposite of
    /// [`Vec::dedup_by()`], which passes the next element first.
    /// Each passed through item is cloned once and stored until it is replaced
    /// by the next item that is not equal.
    ///
    /// See also: [`dedup()`](Self::dedup)
    fn dedup_by<F>(self, eq_fn: F) -> DedupBy<Self, F>
    where
        Self: Sized,
        Self::Item: Clone,
        F: FnMut(&Self::Item, &Self::Item) -> bool,
    {
        DedupBy::new(self, eq_fn)
    }

    /// Filters out subsequent/adjacent stream items with the same key.
    ///
    /// Compares the keys of stream items that are extracted by `key_fn`.
    /// The key of each stream item is extracted once and the items are
    /// passed through unmodified.
    /// The last key is stored until a different key occurs.
    /// The first item is always passed through, like [`Vec::dedup_by_key()`].
    ///
    /// See also: [`dedup()`](Self::dedup)
    fn dedup_by_key<K, F>(self, key_fn: F) -> DedupByKey<Self, K, F>
    where
        Self: Sized,
        K: PartialEq,
        F: FnMut(&Self::Item) -> K,
    {
        DedupByKey::new(self, key_fn)
    }

    /// Filters out subsequent/adjacent `Ok` items in a result stream.
    ///
    /// See also: [`dedup()`](Self::dedup)
    fn dedup_ok<T, E>(self) -> DedupOk<Self, T>
    where
        Self: Stream<Item = Result<T, E>> + Sized,
        T: Clone + PartialEq,
    {
        DedupOk::new(self)
    }

    /// Filters out subsequent/adjacent `Err` items in a result stream.
    ///
    /// See also: [`dedup()`](Self::dedup)
    fn dedup_err<T, E>(self) -> DedupErr<Self, E>
    where
        Self: Stream<Item = Result<T, E>> + Sized,
        E: Clone + PartialEq,
    {
        DedupErr::new(self)
    }

    /// Debounces an input stream.
    ///
    /// Delays items until no more new items have arrived during the delay window.
    ///
    /// Filters out items that arrive in quick succession. Only the last item from
    /// each sequence is emitted. All preceding items are discarded.
    ///
    /// The delay is implemented by the given [`Sleep`] type.
    ///
    /// See [Polling ready items](StreamExt#polling-ready-items) for choosing
    /// the `max_ready_count` of [`Debounce::yield_after()`]. Its value usually
    /// does not affect which items are emitted, because items that are ready at the same time fall into the
    /// same delay window anyway. Only with a zero delay, [`NonZeroUsize::MIN`]
    /// emits every item, while a greater value emits only the latest of up to
    /// `max_ready_count` items that are ready at the same time.
    ///
    /// See also: [`throttle()`](Self::throttle)
    fn debounce_with_sleep<S: Sleep>(self, delay: Duration) -> Debounce<Self, S>
    where
        Self: Sized,
    {
        Debounce::new(self, delay)
    }

    /// Debounces an input stream using [`tokio::time::Sleep`](::tokio::time::Sleep).
    ///
    /// See [`debounce_with_sleep()`](Self::debounce_with_sleep) for details.
    #[cfg(feature = "tokio")]
    #[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
    fn debounce(self, delay: Duration) -> Debounce<Self, ::tokio::time::Sleep>
    where
        Self: Sized,
    {
        self.debounce_with_sleep(delay)
    }

    /// Throttles an input stream.
    ///
    /// The throttler defines the throttling strategy.
    ///
    /// See [Polling ready items](StreamExt#polling-ready-items) for choosing
    /// the `max_ready_count` of [`Throttle::yield_after()`]. Its value only
    /// affects which items are yielded if the throttler is ready while multiple items are ready at the
    /// same time: [`NonZeroUsize::MIN`] yields the first one, a greater value the
    /// latest of up to `max_ready_count` items. See [`IntervalThrottler`]
    /// for the impact on interval throttling.
    fn throttle<Th>(self, throttler: Th) -> Throttle<Self, Th>
    where
        Self: Sized,
        Th: Throttler<Self::Item>,
    {
        Throttle::new(self, throttler)
    }

    /// Switches to a new inner stream for each item of the outer stream.
    ///
    /// Maps each item of this outer stream to an inner stream with `switch_to` and
    /// yields the items of the current inner stream. The previous inner stream is
    /// dropped when the outer stream yields the next item, i.e. its pending items
    /// are discarded as stale.
    ///
    /// The outer stream is polled first. Items of an inner stream are only yielded
    /// while no new outer item is ready.
    ///
    /// Ends after both the outer stream and the current inner stream ended. An inner
    /// stream that ended is not considered as stale, i.e. its last item remains valid
    /// until the next outer item arrives.
    ///
    /// Equivalent to `switchMap` in RxJS. In the following marble diagram, time
    /// passes from left to right, `|` marks the end of a stream, and `!` marks
    /// where an inner stream is dropped. The outer items `a`, `b`, and `c` are
    /// mapped to the inner streams in the lines below them, which start in the
    /// same column.
    ///
    /// ```text
    /// outer:   --a---------b-------c--|
    /// inner a:   --1---2---!
    /// inner b:             ----4-|
    /// inner c:                     --5---6-|
    /// result:  ----1---2-------4-----5---6-|
    /// ```
    ///
    /// Inner stream `a` would have yielded `3` later, but is dropped when `b`
    /// arrives. The result does not end when inner stream `b` ends, but only after
    /// both the outer stream and inner stream `c` ended.
    ///
    /// # Choosing `max_ready_count`
    ///
    /// See [Polling ready items](StreamExt#polling-ready-items) and
    /// [`SwitchMap::yield_after()`]. Unlike for the other combinators, the value affects the result whenever the outer stream
    /// yields multiple items at the same time. Only the inner stream of the
    /// latest of up to `max_ready_count` ready outer items is created.
    /// With [`NonZeroUsize::MIN`], an inner stream is created for every outer
    /// item and polled once before switching to the next one. It may yield
    /// items, although a newer outer item is already ready:
    ///
    /// ```
    /// use std::num::NonZeroUsize;
    ///
    /// use futures_stream_ext::StreamExt as _;
    /// use futures_util::{FutureExt as _, StreamExt as _, stream};
    ///
    /// let switch_map = |max_ready_count| {
    ///     stream::iter([1, 2])
    ///         .switch_map(|n| stream::iter([n * 10, n * 10 + 1]))
    ///         .yield_after(max_ready_count)
    ///         .collect::<Vec<_>>()
    ///         .now_or_never()
    ///         .unwrap()
    /// };
    /// assert_eq!(switch_map(NonZeroUsize::MIN), [10, 20, 21]);
    /// assert_eq!(switch_map(NonZeroUsize::new(2).unwrap()), [20, 21]);
    /// ```
    ///
    /// Choose a value that is at least as large as the expected burst size of
    /// the outer stream, so that a burst creates only a single inner stream.
    /// This matters if creating an inner stream is expensive or has side
    /// effects, e.g. when it subscribes to a resource.
    ///
    /// # Difference to `flat_map()`
    ///
    /// [`flat_map()`](futures_util::StreamExt::flat_map) also maps each outer item
    /// to an inner stream, but drains each inner stream completely before it polls
    /// the outer stream for the next item. No item is discarded, but new outer items
    /// wait until the current inner stream ended. With an inner stream `a` that
    /// yields `1`, `2`, `3` and then ends, `flat_map()` yields `1 2 3 4 5 6` instead
    /// of `1 2 4 5 6`, and an inner stream that never ends blocks all further outer
    /// items. This corresponds to `concatMap` in RxJS, not to RxJS' `flatMap`,
    /// which is an alias of `mergeMap` and polls all inner streams concurrently.
    ///
    /// Use `switch_map()` if only the inner stream of the latest outer item is
    /// relevant, e.g. when the outer stream yields the current configuration and
    /// the inner stream observes data that depends on it.
    fn switch_map<U, F>(self, switch_to: F) -> SwitchMap<Self, U, F>
    where
        Self: Sized,
        U: Stream,
        F: FnMut(Self::Item) -> U,
    {
        SwitchMap::new(self, switch_to)
    }

    /// Yields a final item after this stream ended.
    ///
    /// The item is created by `make_item` only after the stream ended.
    /// Equivalent to `endWith` in RxJS.
    ///
    /// Intended for finite streams whose last item becomes invalid when they
    /// end, e.g. by yielding an error as the final item. Consumers consider the
    /// last item of a finite stream as valid indefinitely otherwise.
    ///
    /// `make_item` is usually a closure, but can be any [`MakeItem`], e.g.
    /// [`DefaultItem`].
    ///
    /// See also: [`end_with_if_nonempty()`](Self::end_with_if_nonempty)
    fn end_with<F>(self, make_item: F) -> EndWith<Self, F>
    where
        Self: Sized,
        F: MakeItem<Self::Item>,
    {
        EndWith::new(self, make_item)
    }

    /// Yields a final item after this stream ended, unless it ended without yielding any items.
    ///
    /// Like [`end_with()`](Self::end_with), but an empty stream stays empty, because there
    /// is no last item that could become invalid.
    ///
    /// The item is created by `make_item` only after a non-empty stream ended.
    /// `make_item` is dropped without being invoked if the stream ended empty.
    ///
    /// ```
    /// use futures_stream_ext::StreamExt as _;
    /// use futures_util::{FutureExt as _, StreamExt as _, stream};
    ///
    /// let end_with_if_nonempty = |items: Vec<u32>| {
    ///     stream::iter(items)
    ///         .end_with_if_nonempty(|| 0)
    ///         .collect::<Vec<_>>()
    ///         .now_or_never()
    ///         .unwrap()
    /// };
    /// assert_eq!(end_with_if_nonempty(vec![1, 2]), [1, 2, 0]);
    /// assert_eq!(end_with_if_nonempty(vec![]), []);
    /// ```
    fn end_with_if_nonempty<F>(self, make_item: F) -> EndWith<Self, F>
    where
        Self: Sized,
        F: MakeItem<Self::Item>,
    {
        EndWith::new_if_nonempty(self, make_item)
    }

    /// Yields the default item if this stream ends without yielding any items.
    ///
    /// Yields [`Default::default()`] as a final item if the stream ended before
    /// yielding its first item. Otherwise, the stream is passed through unchanged.
    /// Equivalent to `defaultIfEmpty` in RxJS.
    ///
    /// See also: [`default_if_empty_with()`](Self::default_if_empty_with)
    fn default_if_empty(self) -> DefaultIfEmpty<Self, DefaultItem<Self::Item>>
    where
        Self: Sized,
        Self::Item: Default,
    {
        DefaultIfEmpty::new(self, DefaultItem::new())
    }

    /// Yields a single item if this stream ends without yielding any items.
    ///
    /// Yields a final item created by `make_item` if the stream ended before
    /// yielding its first item. Otherwise, the stream is passed through unchanged.
    /// Equivalent to `defaultIfEmpty` in RxJS.
    ///
    /// `make_item` is usually a closure, but can be any [`MakeItem`].
    ///
    /// See also: [`default_if_empty()`](Self::default_if_empty)
    fn default_if_empty_with<F>(self, make_item: F) -> DefaultIfEmpty<Self, F>
    where
        Self: Sized,
        F: MakeItem<Self::Item>,
    {
        DefaultIfEmpty::new(self, make_item)
    }
}

impl<St: Stream> StreamExt for St {}

#[cfg(test)]
mod tests;
