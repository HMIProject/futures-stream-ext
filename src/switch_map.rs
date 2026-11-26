// SPDX-FileCopyrightText: The futures-stream-ext authors
// SPDX-License-Identifier: MPL-2.0

use std::{
    fmt,
    num::NonZeroUsize,
    pin::Pin,
    task::{Context, Poll},
};

use futures_util::{
    Stream, StreamExt as _,
    stream::{Fuse, FusedStream},
};
use pin_project_lite::pin_project;

use crate::{pending_and_wake_if, poll_latest_ready};

#[cfg(doc)]
use super::StreamExt;

/// When the stream ends.
#[derive(Debug, Clone, Copy)]
enum Mode {
    /// After both the outer stream and the current inner stream ended.
    Exhaustive,
    /// As soon as the outer stream ended, dropping the current inner stream.
    Scoped,
}

pin_project! {
    /// Stream returned by [`StreamExt::switch_map()`] and
    /// [`StreamExt::switch_map_scoped()`].
    #[must_use = "streams do nothing unless polled"]
    pub struct SwitchMap<St, U, F> {
        #[pin]
        outer: Fuse<St>,

        // `None` before the first outer item and after the current inner stream ended.
        // In `Mode::Scoped` also `None` after the outer stream ended, i.e. it never
        // outlives the outer stream.
        #[pin]
        inner: Option<U>,

        // `None` after the outer stream ended. In `Mode::Exhaustive` this drops the
        // captured state early, while the last inner stream might still be active.
        // In `Mode::Scoped` it is dropped together with the inner stream.
        switch_to: Option<F>,

        mode: Mode,

        max_ready_count: NonZeroUsize,
    }
}

// Implemented manually, because closures do not implement `Debug`.
impl<St, U, F> fmt::Debug for SwitchMap<St, U, F>
where
    St: fmt::Debug,
    U: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SwitchMap")
            .field("outer", &self.outer)
            .field("inner", &self.inner)
            .field("mode", &self.mode)
            .field("max_ready_count", &self.max_ready_count)
            .finish_non_exhaustive()
    }
}

impl<St, U, F> SwitchMap<St, U, F> {
    pub(super) fn new(outer: St, switch_to: F) -> Self
    where
        St: Stream,
    {
        Self::with_mode(outer, switch_to, Mode::Exhaustive)
    }

    pub(super) fn new_scoped(outer: St, switch_to: F) -> Self
    where
        St: Stream,
    {
        Self::with_mode(outer, switch_to, Mode::Scoped)
    }

    fn with_mode(outer: St, switch_to: F, mode: Mode) -> Self
    where
        St: Stream,
    {
        Self {
            outer: outer.fuse(),
            inner: None,
            switch_to: Some(switch_to),
            mode,
            max_ready_count: NonZeroUsize::MIN,
        }
    }

    /// Yields to the executor after consuming `max_ready_count` consecutive
    /// ready items of the input stream.
    ///
    /// Defaults to [`NonZeroUsize::MIN`]. See
    /// [Polling ready items](crate::StreamExt#polling-ready-items) for how to
    /// choose `max_ready_count`. It affects which inner streams are created,
    /// see [`StreamExt::switch_map()`](crate::StreamExt::switch_map). For
    /// [`StreamExt::switch_map_scoped()`](crate::StreamExt::switch_map_scoped)
    /// it also affects when the end of the outer stream is observed, and
    /// thereby whether the inner stream of the last outer item is created and
    /// polled at all.
    pub const fn yield_after(mut self, max_ready_count: NonZeroUsize) -> Self {
        self.max_ready_count = max_ready_count;
        self
    }
}

impl<St, U, F> Stream for SwitchMap<St, U, F>
where
    St: Stream,
    U: Stream,
    F: FnMut(St::Item) -> U,
{
    type Item = U::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        // Consume the ready outer items, but at most `max_ready_count`
        // to prevent an endless loop for an outer stream that is always ready.
        // Only the inner stream of the latest one is created and polled.
        let (latest_item, outer_maybe_ready) =
            poll_latest_ready(this.outer.as_mut(), cx, *this.max_ready_count);
        if this.outer.is_terminated() && matches!(*this.mode, Mode::Scoped) {
            // The inner stream must not outlive the outer stream. Neither
            // create the inner stream of an outer item that arrived together
            // with the end of the outer stream, because it would be dropped
            // before being polled.
            this.inner.set(None);
            *this.switch_to = None;
            return Poll::Ready(None);
        }
        if let Some(item) = latest_item {
            // Drop the stale inner stream before creating its replacement.
            // A single `set()` would call `switch_to()` first and drop the
            // stale stream afterwards. Both would coexist, which conflicts if
            // they own the same exclusive resource, e.g. a subscription that
            // is unsubscribed by key when the stale stream is dropped. This
            // might even cause a deadlock. Overlapping is pointless anyway,
            // because the stale items are discarded.
            //
            // TODO: Release the inner stream asynchronously instead of dropping
            // it synchronously, e.g. if the inner stream occupies remote resources
            // on a server.
            this.inner.set(None);
            let switch_to = this
                .switch_to
                .as_mut()
                .expect("switch_to must exist until the outer stream ended");
            this.inner.set(Some(switch_to(item)));
        }
        if this.outer.is_terminated() {
            *this.switch_to = None;
        }

        if let Some(inner_stream) = this.inner.as_mut().as_pin_mut() {
            match inner_stream.poll_next(cx) {
                Poll::Ready(Some(item)) => return Poll::Ready(Some(item)),
                Poll::Ready(None) => this.inner.set(None),
                Poll::Pending => return pending_and_wake_if(cx, outer_maybe_ready),
            }
        }

        if this.outer.is_terminated() {
            return Poll::Ready(None);
        }
        // Woken up by the outer stream, or by ourselves if it might still be ready.
        pending_and_wake_if(cx, outer_maybe_ready)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        if !self.outer.is_terminated() {
            // The current inner stream could be replaced at any time.
            return (0, None);
        }
        self.inner.as_ref().map_or((0, Some(0)), Stream::size_hint)
    }
}

impl<St, U, F> FusedStream for SwitchMap<St, U, F>
where
    St: Stream,
    U: Stream,
    F: FnMut(St::Item) -> U,
{
    fn is_terminated(&self) -> bool {
        self.outer.is_terminated() && self.inner.is_none()
    }
}
