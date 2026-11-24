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

use crate::MakeItem;

#[cfg(doc)]
use super::StreamExt;

pin_project! {
    /// Stream returned by [`StreamExt::default_if_empty()`].
    #[must_use = "streams do nothing unless polled"]
    pub struct DefaultIfEmpty<St, F> {
        #[pin]
        stream: Fuse<St>,

        // `None` after the stream yielded its first item or the default item
        // has been created.
        make_item: Option<F>,
    }
}

// Implemented manually, because closures do not implement `Debug`.
impl<St, F> fmt::Debug for DefaultIfEmpty<St, F>
where
    St: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DefaultIfEmpty")
            .field("stream", &self.stream)
            .finish_non_exhaustive()
    }
}

impl<St, F> DefaultIfEmpty<St, F>
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

impl<St, F> Stream for DefaultIfEmpty<St, F>
where
    St: Stream,
    F: MakeItem<St::Item>,
{
    type Item = St::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let next = match ready!(this.stream.poll_next(cx)) {
            Some(item) => {
                // Drop the closure early, because it is not needed anymore.
                *this.make_item = None;
                Some(item)
            }
            None => this.make_item.take().map(MakeItem::make_item),
        };
        Poll::Ready(next)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let (lower, upper) = self.stream.size_hint();
        if self.make_item.is_none() {
            return (lower, upper);
        }
        // Either the items of the stream or the default item.
        (lower.max(1), upper.map(|upper| upper.max(1)))
    }
}

impl<St, F> FusedStream for DefaultIfEmpty<St, F>
where
    St: Stream,
    F: MakeItem<St::Item>,
{
    fn is_terminated(&self) -> bool {
        self.stream.is_terminated() && self.make_item.is_none()
    }
}
