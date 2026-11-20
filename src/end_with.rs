// SPDX-FileCopyrightText: The futures-stream-ext authors
// SPDX-License-Identifier: MPL-2.0

use std::{
    fmt,
    pin::Pin,
    task::{Context, Poll, ready},
};

use futures_util::{
    Stream, StreamExt as _,
    stream::{Fuse, FusedStream},
};
use pin_project_lite::pin_project;

#[cfg(doc)]
use super::StreamExt;

pin_project! {
    /// Stream returned by [`StreamExt::end_with()`].
    #[must_use = "streams do nothing unless polled"]
    pub struct EndWith<St, F> {
        #[pin]
        stream: Fuse<St>,

        // `None` after the final item has been created.
        make_item: Option<F>,
    }
}

// Implemented manually, because closures do not implement `Debug`.
impl<St, F> fmt::Debug for EndWith<St, F>
where
    St: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EndWith")
            .field("stream", &self.stream)
            .finish_non_exhaustive()
    }
}

impl<St, F> EndWith<St, F>
where
    St: Stream,
{
    pub(super) fn new(stream: St, make_item: F) -> Self {
        Self {
            stream: stream.fuse(),
            make_item: Some(make_item),
        }
    }
}

impl<St, F> Stream for EndWith<St, F>
where
    St: Stream,
    F: FnOnce() -> St::Item,
{
    type Item = St::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let next = match ready!(this.stream.poll_next(cx)) {
            Some(item) => Some(item),
            None => this.make_item.take().map(|make_item| make_item()),
        };
        Poll::Ready(next)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let pending = usize::from(self.make_item.is_some());
        let (lower, upper) = self.stream.size_hint();
        (
            lower.saturating_add(pending),
            upper.and_then(|upper| upper.checked_add(pending)),
        )
    }
}

impl<St, F> FusedStream for EndWith<St, F>
where
    St: Stream,
    F: FnOnce() -> St::Item,
{
    fn is_terminated(&self) -> bool {
        self.stream.is_terminated() && self.make_item.is_none()
    }
}
