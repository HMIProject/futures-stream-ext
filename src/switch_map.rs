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

pin_project! {
    /// Stream returned by [`StreamExt::switch_map()`].
    #[must_use = "streams do nothing unless polled"]
    pub struct SwitchMap<S, U, F> {
        #[pin]
        outer: Fuse<S>,

        // `None` before the first outer item and after the current inner stream ended.
        #[pin]
        inner: Option<U>,

        // `None` after the outer stream ended. Drops the captured state early,
        // while the last inner stream is still active.
        switch_to: Option<F>,

        max_ready_count: NonZeroUsize,
    }
}

// Implemented manually, because closures do not implement `Debug`.
impl<S, U, F> fmt::Debug for SwitchMap<S, U, F>
where
    S: fmt::Debug,
    U: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SwitchMap")
            .field("outer", &self.outer)
            .field("inner", &self.inner)
            .field("max_ready_count", &self.max_ready_count)
            .finish_non_exhaustive()
    }
}

impl<S, U, F> SwitchMap<S, U, F> {
    pub(super) fn new(outer: S, switch_to: F, max_ready_count: NonZeroUsize) -> Self
    where
        S: Stream,
    {
        Self {
            outer: outer.fuse(),
            inner: None,
            switch_to: Some(switch_to),
            max_ready_count,
        }
    }
}

impl<S, U, F> Stream for SwitchMap<S, U, F>
where
    S: Stream,
    U: Stream,
    F: FnMut(S::Item) -> U,
{
    type Item = U::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        // Consume the ready outer items, but at most `max_ready_count`
        // to prevent an endless loop for an outer stream that is always ready.
        // Only the inner stream of the latest one is created and polled.
        let (latest_item, outer_maybe_ready) =
            poll_latest_ready(this.outer.as_mut(), cx, *this.max_ready_count);
        if let Some(item) = latest_item {
            // Drop the stale inner stream before creating its replacement.
            // A single `set()` would call `switch_to()` first and drop the
            // stale stream afterwards. Both would coexist, which conflicts if
            // they own the same exclusive resource, e.g. a subscription that
            // is unsubscribed by key when the stale stream is dropped. This
            // might even cause a deadlock. Overlapping is pointless anyway,
            // because the stale items are discarded.
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

impl<S, U, F> FusedStream for SwitchMap<S, U, F>
where
    S: Stream,
    U: Stream,
    F: FnMut(S::Item) -> U,
{
    fn is_terminated(&self) -> bool {
        self.outer.is_terminated() && self.inner.is_none()
    }
}
