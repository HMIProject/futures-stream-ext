// SPDX-FileCopyrightText: The futures-stream-ext authors
// SPDX-License-Identifier: MPL-2.0

use std::{
    marker::PhantomData,
    mem,
    num::NonZeroUsize,
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
        // Unlimited if `None`.
        max_ready_count: Option<NonZeroUsize>,
    }
}

impl<St, State> FilterStatefulSync<St, State> {
    const fn new(stream: St, state: State) -> Self {
        Self {
            stream,
            state,
            max_ready_count: None,
        }
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
        let max_ready_count = *this.max_ready_count;
        let mut ready_count = 0;
        loop {
            let Some(next_item) = ready!(this.stream.as_mut().poll_next(cx)) else {
                return Poll::Ready(None);
            };
            if this.state.filter_next(&next_item) {
                return Poll::Ready(Some(next_item));
            }
            ready_count += 1;
            if max_ready_count.is_some_and(|max| ready_count >= max.get()) {
                // Yield to the executor and continue during the next invocation.
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let (_, upper) = self.stream.size_hint();
        (0, upper)
    }
}

impl<St: FusedStream, State> FilterStatefulSync<St, State> {
    fn is_terminated(&self) -> bool {
        self.stream.is_terminated()
    }
}

/// Implements `yield_after()` for a public wrapper of [`FilterStatefulSync`].
macro_rules! impl_yield_after {
    ($name:ident<$($param:ident),*>) => {
        impl<$($param),*> $name<$($param),*>
        where
            St: Stream,
        {
            /// Yields to the executor after discarding `max_ready_count`
            /// consecutive ready items.
            ///
            /// By default, the stream never yields to the executor while
            /// discarding items, like
            /// [`StreamExt::filter()`](futures_util::StreamExt::filter). Use
            /// this method if the input stream might be always ready and yield
            /// only items that are discarded, e.g. a synchronous stream.
            /// Otherwise, polling the stream would never return. Does not
            /// affect which items are yielded.
            ///
            /// See [Polling ready items](crate::StreamExt#polling-ready-items)
            /// for how to choose `max_ready_count`.
            pub const fn yield_after(mut self, max_ready_count: NonZeroUsize) -> Self {
                self.inner.max_ready_count = Some(max_ready_count);
                self
            }
        }
    };
}

/// Implements [`FusedStream`] for a public wrapper of [`FilterStatefulSync`].
macro_rules! impl_fused_stream {
    ($name:ident<$($param:ident),*>) => {
        impl<$($param),*> FusedStream for $name<$($param),*>
        where
            Self: Stream,
            St: FusedStream,
        {
            fn is_terminated(&self) -> bool {
                self.inner.is_terminated()
            }
        }
    };
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

/// Filtering of subsequent/adjacent sequence items that are equal according
/// to `eq_fn`.
///
/// Each passed through item will be cloned once.
/// All other sequence items are discarded.
///
/// See also: <https://rxmarbles.com/#distinctUntilChanged>
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

impl<T: Clone + PartialEq> FilterState<T> for Option<T> {
    fn filter_next(&mut self, next_item: &T) -> bool {
        filter_dedup_by(self, next_item, T::eq)
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
            inner: FilterStatefulSync::new(stream, None),
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
            inner: FilterStatefulSync::new(
                stream,
                ByState {
                    last_item: None,
                    eq_fn,
                },
            ),
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
            inner: FilterStatefulSync::new(
                stream,
                ByKeyState {
                    last_key: None,
                    key_fn,
                },
            ),
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

/// Selects the values of [`Result`] items that are filtered by [`ResultState`].
trait SelectValue<T, E> {
    type Value;

    /// Returns `None` if `result` should be passed through.
    fn select(result: &Result<T, E>) -> Option<&Self::Value>;
}

/// Selects `Ok` values.
#[derive(Debug)]
struct SelectOk;

impl<T, E> SelectValue<T, E> for SelectOk {
    type Value = T;

    fn select(result: &Result<T, E>) -> Option<&T> {
        result.as_ref().ok()
    }
}

/// Selects `Err` values.
#[derive(Debug)]
struct SelectErr;

impl<T, E> SelectValue<T, E> for SelectErr {
    type Value = E;

    fn select(result: &Result<T, E>) -> Option<&E> {
        result.as_ref().err()
    }
}

/// Filters out subsequent/adjacent values selected by `S`.
///
/// All other items are passed through and interrupt the sequence.
/// Keeps the last value after an interruption to reuse its allocation.
#[derive(Debug)]
struct ResultState<V, S> {
    last_value: Option<V>,
    interrupted: bool,
    select: PhantomData<S>,
}

impl<V, S> ResultState<V, S> {
    const fn new() -> Self {
        Self {
            last_value: None,
            interrupted: false,
            select: PhantomData,
        }
    }
}

impl<T, E, V, S> FilterState<Result<T, E>> for ResultState<V, S>
where
    S: SelectValue<T, E, Value = V>,
    V: Clone + PartialEq,
{
    fn filter_next(&mut self, next_result: &Result<T, E>) -> bool {
        let Some(next_value) = S::select(next_result) else {
            self.interrupted = true;
            return true;
        };
        // The first value after an interruption is always passed through.
        let interrupted = mem::take(&mut self.interrupted);
        filter_dedup_by(
            &mut self.last_value,
            next_value,
            |last_value, next_value| !interrupted && last_value == next_value,
        )
    }
}

pin_project! {
    /// Result of [`StreamExt::dedup_ok()`](crate::StreamExt::dedup_ok).
    #[derive(Debug)]
    #[must_use = "streams do nothing unless polled"]
    pub struct DedupOk<St, T> {
        #[pin]
        inner: FilterStatefulSync<St, ResultState<T, SelectOk>>,
    }
}

impl<St, T> DedupOk<St, T> {
    pub(crate) const fn new(stream: St) -> Self {
        Self {
            inner: FilterStatefulSync::new(stream, ResultState::new()),
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

pin_project! {
    /// Result of [`StreamExt::dedup_err()`](crate::StreamExt::dedup_err).
    #[derive(Debug)]
    #[must_use = "streams do nothing unless polled"]
    pub struct DedupErr<St, E> {
        #[pin]
        inner: FilterStatefulSync<St, ResultState<E, SelectErr>>,
    }
}

impl<St, E> DedupErr<St, E> {
    pub(crate) const fn new(stream: St) -> Self {
        Self {
            inner: FilterStatefulSync::new(stream, ResultState::new()),
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

impl_yield_after!(Dedup<St>);
impl_yield_after!(DedupBy<St, F>);
impl_yield_after!(DedupByKey<St, K, F>);
impl_yield_after!(DedupOk<St, T>);
impl_yield_after!(DedupErr<St, E>);

impl_fused_stream!(Dedup<St>);
impl_fused_stream!(DedupBy<St, F>);
impl_fused_stream!(DedupByKey<St, K, F>);
impl_fused_stream!(DedupOk<St, T>);
impl_fused_stream!(DedupErr<St, E>);

#[cfg(test)]
mod tests;
