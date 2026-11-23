// SPDX-FileCopyrightText: The futures-stream-ext authors
// SPDX-License-Identifier: MPL-2.0

use std::{
    num::NonZeroUsize,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use futures_util::{
    Stream, StreamExt as _,
    stream::{Fuse, FusedStream as _},
};
use pin_project_lite::pin_project;

use crate::{Sleep, pending_and_wake_if, poll_latest_ready, reset_or_sleep};

pin_project! {
    /// Result of [`StreamExt::debounce_with_sleep()`](crate::StreamExt::debounce_with_sleep).
    #[cfg_attr(
        feature = "tokio",
        doc = "",
        doc = "Also returned by [`StreamExt::debounce()`](crate::StreamExt::debounce)."
    )]
    #[derive(Debug)]
    #[must_use = "streams do nothing unless polled"]
    #[project = DebounceProjection]
    pub struct Debounce<St: Stream, S> {
        #[pin]
        stream: Fuse<St>,

        delay: Duration,

        max_ready_count: NonZeroUsize,

        pending_item: Option<St::Item>,

        // Created on demand and reused afterwards.
        // Always `Some` while `pending_item` is `Some`.
        #[pin]
        sleep: Option<S>,
    }
}

impl<St: Stream, S: Sleep> Debounce<St, S> {
    pub(crate) fn new(stream: St, delay: Duration) -> Self {
        Self {
            stream: stream.fuse(),
            delay,
            max_ready_count: NonZeroUsize::MIN,
            pending_item: None,
            sleep: None,
        }
    }
}

impl<St: Stream, S> Debounce<St, S> {
    /// Yields to the executor after consuming `max_ready_count` consecutive
    /// ready items of the input stream.
    ///
    /// Defaults to [`NonZeroUsize::MIN`]. See
    /// [Polling ready items](crate::StreamExt#polling-ready-items) for how to
    /// choose `max_ready_count`. See
    /// [`StreamExt::debounce_with_sleep()`](crate::StreamExt::debounce_with_sleep)
    /// for how it affects which items are yielded.
    pub const fn yield_after(mut self, max_ready_count: NonZeroUsize) -> Self {
        self.max_ready_count = max_ready_count;
        self
    }
}

impl<St, S> Stream for Debounce<St, S>
where
    St: Stream,
    S: Sleep,
{
    type Item = St::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let DebounceProjection {
            mut stream,
            delay,
            max_ready_count,
            pending_item,
            mut sleep,
        } = self.project();

        let (latest_item, stream_maybe_ready) =
            poll_latest_ready(stream.as_mut(), cx, *max_ready_count);

        // Replace the pending item and restart the delay.
        if let Some(latest_item) = latest_item {
            *pending_item = Some(latest_item);
            reset_or_sleep(sleep.as_mut(), *delay);
        }

        if stream.is_terminated() {
            // Stream has finished. No newer item can arrive that would replace
            // the pending item, so yield it immediately without waiting for the
            // delay to elapse. Ends the stream if no item is pending.
            return Poll::Ready(pending_item.take());
        }

        if pending_item.is_none() {
            // New stream items may arrive later.
            return Poll::Pending;
        }

        let sleep = sleep
            .as_pin_mut()
            .expect("sleep must exist while an item is pending");
        if sleep.poll(cx).is_ready() {
            return Poll::Ready(pending_item.take());
        }
        pending_and_wake_if(cx, stream_maybe_ready)
    }
}
