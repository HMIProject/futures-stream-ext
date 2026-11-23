// SPDX-FileCopyrightText: The futures-stream-ext authors
// SPDX-License-Identifier: MPL-2.0

use std::{
    fmt,
    pin::Pin,
    task::{Context, Poll, ready},
};

use futures_util::{Stream, stream::FusedStream};
use pin_project_lite::pin_project;

use crate::MakeItem;

#[cfg(doc)]
use super::StreamExt;

pin_project! {
    /// Stream returned by [`StreamExt::end_with()`].
    #[must_use = "streams do nothing unless polled"]
    pub struct EndWith<St, F> {
        #[pin]
        stream: St,

        // `None` after the stream ended and the final item has been created.
        // The stream is not polled anymore afterwards.
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

impl<St, F> EndWith<St, F> {
    pub(super) const fn new(stream: St, make_item: F) -> Self {
        Self {
            stream,
            make_item: Some(make_item),
        }
    }
}

impl<St, F> Stream for EndWith<St, F>
where
    St: Stream,
    F: MakeItem<St::Item>,
{
    type Item = St::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        if this.make_item.is_none() {
            return Poll::Ready(None);
        }
        let next = match ready!(this.stream.poll_next(cx)) {
            Some(item) => Some(item),
            None => this.make_item.take().map(MakeItem::make_item),
        };
        Poll::Ready(next)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        if self.make_item.is_none() {
            return (0, Some(0));
        }
        let (lower, upper) = self.stream.size_hint();
        (
            lower.saturating_add(1),
            upper.and_then(|upper| upper.checked_add(1)),
        )
    }
}

impl<St, F> FusedStream for EndWith<St, F>
where
    St: Stream,
    F: MakeItem<St::Item>,
{
    fn is_terminated(&self) -> bool {
        self.make_item.is_none()
    }
}

pin_project! {
    /// Stream returned by [`StreamExt::end_with_if_nonempty()`].
    #[must_use = "streams do nothing unless polled"]
    pub struct EndWithIfNonempty<St, F> {
        #[pin]
        stream: St,

        // `None` after the stream ended. The final item has been created
        // unless the stream was empty. The stream is not polled anymore afterwards.
        make_item: Option<F>,

        // `true` after the stream yielded its first item.
        nonempty: bool,
    }
}

// Implemented manually, because closures do not implement `Debug`.
impl<St, F> fmt::Debug for EndWithIfNonempty<St, F>
where
    St: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EndWithIfNonempty")
            .field("stream", &self.stream)
            .field("nonempty", &self.nonempty)
            .finish_non_exhaustive()
    }
}

impl<St, F> EndWithIfNonempty<St, F> {
    pub(super) const fn new(stream: St, make_item: F) -> Self {
        Self {
            stream,
            make_item: Some(make_item),
            nonempty: false,
        }
    }
}

impl<St, F> Stream for EndWithIfNonempty<St, F>
where
    St: Stream,
    F: MakeItem<St::Item>,
{
    type Item = St::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        if this.make_item.is_none() {
            return Poll::Ready(None);
        }
        let Some(item) = ready!(this.stream.poll_next(cx)) else {
            // Taken unconditionally to drop the closure of an empty stream.
            let make_item = this.make_item.take();
            return Poll::Ready(
                make_item
                    .filter(|_| *this.nonempty)
                    .map(MakeItem::make_item),
            );
        };
        *this.nonempty = true;
        Poll::Ready(Some(item))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        if self.make_item.is_none() {
            return (0, Some(0));
        }
        let (lower, upper) = self.stream.size_hint();
        if self.nonempty {
            return (
                lower.saturating_add(1),
                upper.and_then(|upper| upper.checked_add(1)),
            );
        }
        // The final item follows only if the stream yields at least one item.
        (
            if lower == 0 {
                0
            } else {
                lower.saturating_add(1)
            },
            upper.and_then(|upper| {
                if upper == 0 {
                    Some(0)
                } else {
                    upper.checked_add(1)
                }
            }),
        )
    }
}

impl<St, F> FusedStream for EndWithIfNonempty<St, F>
where
    St: Stream,
    F: MakeItem<St::Item>,
{
    fn is_terminated(&self) -> bool {
        self.make_item.is_none()
    }
}
