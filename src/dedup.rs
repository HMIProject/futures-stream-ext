// SPDX-FileCopyrightText: The futures-stream-ext authors
// SPDX-License-Identifier: MPL-2.0

use std::{
    pin::Pin,
    task::{Context, Poll, ready},
};

use futures_util::{Stream, stream::FusedStream};
use pin_project_lite::pin_project;

/// Stateful predicate for filtering stream items synchronously.
trait FilterState<T> {
    /// Updates the state and returns `true` if `next_item` should be passed through.
    fn filter_next(&mut self, next_item: &T) -> bool;
}

pin_project! {
    /// Filters stream items synchronously with a stateful predicate.
    ///
    /// Unlike [`StreamExt::filter()`](futures_util::StreamExt::filter) it
    /// neither buffers the item nor creates a future for each item.
    #[derive(Debug)]
    struct FilterStatefulSync<St, State> {
        #[pin]
        stream: St,
        state: State,
    }
}

impl<St, State> Stream for FilterStatefulSync<St, State>
where
    St: Stream,
    State: FilterState<St::Item>,
{
    type Item = St::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        while let Some(next_item) = ready!(this.stream.as_mut().poll_next(cx)) {
            if this.state.filter_next(&next_item) {
                return Poll::Ready(Some(next_item));
            }
        }
        Poll::Ready(None)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let (_, upper) = self.stream.size_hint();
        (0, upper)
    }
}

impl<St, State> FusedStream for FilterStatefulSync<St, State>
where
    St: FusedStream,
    State: FilterState<St::Item>,
{
    fn is_terminated(&self) -> bool {
        self.stream.is_terminated()
    }
}

/// Implements [`Stream`] for a public wrapper of [`FilterStatefulSync`].
macro_rules! delegate_stream_to_inner {
    () => {
        type Item = St::Item;

        fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            self.project().inner.poll_next(cx)
        }

        fn size_hint(&self) -> (usize, Option<usize>) {
            self.inner.size_hint()
        }
    };
}

/// Filtering of subsequent/adjacent sequence items.
///
/// Each distinct sequence item will be cloned once.
/// All other sequence items are discarded.
///
/// See also: <https://rxmarbles.com/#distinctUntilChanged>
fn filter_dedup<T>(last_item: &mut Option<T>, next_item: &T) -> bool
where
    T: Clone + PartialEq,
{
    filter_dedup_by(last_item, next_item, |last_item, next_item| {
        last_item == next_item
    })
}

/// Like [`filter_dedup()`], but compares items with `eq_fn`.
fn filter_dedup_by<T>(
    last_item: &mut Option<T>,
    next_item: &T,
    eq_fn: impl FnOnce(&T, &T) -> bool,
) -> bool
where
    T: Clone,
{
    match last_item {
        // Discard the next item.
        Some(last_item) if eq_fn(last_item, next_item) => false,
        Some(last_item) => {
            // Reuse the existing allocation.
            last_item.clone_from(next_item);
            true
        }
        None => {
            *last_item = Some(next_item.clone());
            true
        }
    }
}

/// State for filtering distinct values that are interrupted by other items.
///
/// Used by [`filter_dedup_ok()`] and [`filter_dedup_err()`]. Keeps the last
/// value after an interruption to reuse its allocation.
#[derive(Debug, Clone)]
struct DedupState<T> {
    last_value: Option<T>,
    interrupted: bool,
}

impl<T> DedupState<T> {
    const fn new() -> Self {
        Self {
            last_value: None,
            interrupted: false,
        }
    }
}

/// Like [`filter_dedup()`], but passes through and
/// interrupts the sequence if `next_value` is `None`.
fn filter_dedup_some<T>(state: &mut DedupState<T>, next_value: Option<&T>) -> bool
where
    T: Clone + PartialEq,
{
    let Some(next_value) = next_value else {
        state.interrupted = true;
        return true;
    };
    if !state.interrupted {
        return filter_dedup(&mut state.last_value, next_value);
    }
    state.interrupted = false;
    match &mut state.last_value {
        // Reuse the existing allocation.
        Some(last_value) => last_value.clone_from(next_value),
        None => state.last_value = Some(next_value.clone()),
    }
    true
}

impl<T: Clone + PartialEq> FilterState<T> for Option<T> {
    fn filter_next(&mut self, next_item: &T) -> bool {
        filter_dedup(self, next_item)
    }
}

pin_project! {
    /// Result of [`StreamExt::dedup()`](crate::StreamExt::dedup).
    #[derive(Debug)]
    #[must_use = "streams do nothing unless polled"]
    pub struct Dedup<St: Stream> {
        #[pin]
        inner: FilterStatefulSync<St, Option<St::Item>>,
    }
}

impl<St: Stream> Dedup<St> {
    pub(crate) const fn new(stream: St) -> Self {
        Self {
            inner: FilterStatefulSync {
                stream,
                state: None,
            },
        }
    }
}

impl<St> Stream for Dedup<St>
where
    St: Stream,
    St::Item: Clone + PartialEq,
{
    delegate_stream_to_inner!();
}

impl<St> FusedStream for Dedup<St>
where
    St: FusedStream,
    St::Item: Clone + PartialEq,
{
    fn is_terminated(&self) -> bool {
        self.inner.is_terminated()
    }
}

#[derive(Debug)]
struct ByState<T, F> {
    last_item: Option<T>,
    eq_fn: F,
}

impl<T, F> FilterState<T> for ByState<T, F>
where
    T: Clone,
    F: FnMut(&T, &T) -> bool,
{
    fn filter_next(&mut self, next_item: &T) -> bool {
        filter_dedup_by(&mut self.last_item, next_item, &mut self.eq_fn)
    }
}

pin_project! {
    /// Result of [`StreamExt::dedup_by()`](crate::StreamExt::dedup_by).
    #[derive(Debug)]
    #[must_use = "streams do nothing unless polled"]
    pub struct DedupBy<St: Stream, F> {
        #[pin]
        inner: FilterStatefulSync<St, ByState<St::Item, F>>,
    }
}

impl<St: Stream, F> DedupBy<St, F> {
    pub(crate) const fn new(stream: St, eq_fn: F) -> Self {
        Self {
            inner: FilterStatefulSync {
                stream,
                state: ByState {
                    last_item: None,
                    eq_fn,
                },
            },
        }
    }
}

impl<St, F> Stream for DedupBy<St, F>
where
    St: Stream,
    St::Item: Clone,
    F: FnMut(&St::Item, &St::Item) -> bool,
{
    delegate_stream_to_inner!();
}

impl<St, F> FusedStream for DedupBy<St, F>
where
    St: FusedStream,
    St::Item: Clone,
    F: FnMut(&St::Item, &St::Item) -> bool,
{
    fn is_terminated(&self) -> bool {
        self.inner.is_terminated()
    }
}

#[derive(Debug)]
struct ByKeyState<K, F> {
    last_key: Option<K>,
    key_fn: F,
}

impl<T, K, F> FilterState<T> for ByKeyState<K, F>
where
    K: PartialEq,
    F: FnMut(&T) -> K,
{
    fn filter_next(&mut self, next_item: &T) -> bool {
        let next_key = (self.key_fn)(next_item);
        if self.last_key.as_ref() == Some(&next_key) {
            return false;
        }
        self.last_key = Some(next_key);
        true
    }
}

pin_project! {
    /// Result of [`StreamExt::dedup_by_key()`](crate::StreamExt::dedup_by_key).
    #[derive(Debug)]
    #[must_use = "streams do nothing unless polled"]
    pub struct DedupByKey<St, K, F> {
        #[pin]
        inner: FilterStatefulSync<St, ByKeyState<K, F>>,
    }
}

impl<St, K, F> DedupByKey<St, K, F> {
    pub(crate) const fn new(stream: St, key_fn: F) -> Self {
        Self {
            inner: FilterStatefulSync {
                stream,
                state: ByKeyState {
                    last_key: None,
                    key_fn,
                },
            },
        }
    }
}

impl<St, K, F> Stream for DedupByKey<St, K, F>
where
    St: Stream,
    K: PartialEq,
    F: FnMut(&St::Item) -> K,
{
    delegate_stream_to_inner!();
}

impl<St, K, F> FusedStream for DedupByKey<St, K, F>
where
    St: FusedStream,
    K: PartialEq,
    F: FnMut(&St::Item) -> K,
{
    fn is_terminated(&self) -> bool {
        self.inner.is_terminated()
    }
}

/// Filters out subsequent/adjacent `Ok` sequence items.
///
/// Each distinct `Ok` sequence item will be cloned once.
/// All other `Ok` sequence items are discarded.
/// All `Err` sequence items are passed through.
///
/// See also: [`filter_dedup()`]
fn filter_dedup_ok<T, E>(state: &mut DedupState<T>, next_result: &Result<T, E>) -> bool
where
    T: Clone + PartialEq,
{
    filter_dedup_some(state, next_result.as_ref().ok())
}

#[derive(Debug)]
struct OkState<T>(DedupState<T>);

impl<T, E> FilterState<Result<T, E>> for OkState<T>
where
    T: Clone + PartialEq,
{
    fn filter_next(&mut self, next_result: &Result<T, E>) -> bool {
        filter_dedup_ok(&mut self.0, next_result)
    }
}

pin_project! {
    /// Result of [`StreamExt::dedup_ok()`](crate::StreamExt::dedup_ok).
    #[derive(Debug)]
    #[must_use = "streams do nothing unless polled"]
    pub struct DedupOk<St, T> {
        #[pin]
        inner: FilterStatefulSync<St, OkState<T>>,
    }
}

impl<St, T> DedupOk<St, T> {
    pub(crate) const fn new(stream: St) -> Self {
        Self {
            inner: FilterStatefulSync {
                stream,
                state: OkState(DedupState::new()),
            },
        }
    }
}

impl<St, T, E> Stream for DedupOk<St, T>
where
    St: Stream<Item = Result<T, E>>,
    T: Clone + PartialEq,
{
    delegate_stream_to_inner!();
}

impl<St, T, E> FusedStream for DedupOk<St, T>
where
    St: FusedStream<Item = Result<T, E>>,
    T: Clone + PartialEq,
{
    fn is_terminated(&self) -> bool {
        self.inner.is_terminated()
    }
}

/// Filters out subsequent/adjacent `Err` sequence items.
///
/// Each distinct `Err` sequence item will be cloned once.
/// All other `Err` sequence items are discarded.
/// All `Ok` sequence items are passed through.
///
/// See also: [`filter_dedup()`]
fn filter_dedup_err<T, E>(state: &mut DedupState<E>, next_result: &Result<T, E>) -> bool
where
    E: Clone + PartialEq,
{
    filter_dedup_some(state, next_result.as_ref().err())
}

#[derive(Debug)]
struct ErrState<E>(DedupState<E>);

impl<T, E> FilterState<Result<T, E>> for ErrState<E>
where
    E: Clone + PartialEq,
{
    fn filter_next(&mut self, next_result: &Result<T, E>) -> bool {
        filter_dedup_err(&mut self.0, next_result)
    }
}

pin_project! {
    /// Result of [`StreamExt::dedup_err()`](crate::StreamExt::dedup_err).
    #[derive(Debug)]
    #[must_use = "streams do nothing unless polled"]
    pub struct DedupErr<St, E> {
        #[pin]
        inner: FilterStatefulSync<St, ErrState<E>>,
    }
}

impl<St, E> DedupErr<St, E> {
    pub(crate) const fn new(stream: St) -> Self {
        Self {
            inner: FilterStatefulSync {
                stream,
                state: ErrState(DedupState::new()),
            },
        }
    }
}

impl<St, T, E> Stream for DedupErr<St, E>
where
    St: Stream<Item = Result<T, E>>,
    E: Clone + PartialEq,
{
    delegate_stream_to_inner!();
}

impl<St, T, E> FusedStream for DedupErr<St, E>
where
    St: FusedStream<Item = Result<T, E>>,
    E: Clone + PartialEq,
{
    fn is_terminated(&self) -> bool {
        self.inner.is_terminated()
    }
}

#[cfg(test)]
mod tests;
