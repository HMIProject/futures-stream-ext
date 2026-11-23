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
    /// Stream returned by [`StreamExt::end_with()`] and
    /// [`StreamExt::end_with_if_nonempty()`].
    #[must_use = "streams do nothing unless polled"]
    pub struct EndWith<St, F> {
        #[pin]
        stream: St,

        // `None` after the stream ended. The final item has been created
        // if `emit_item` was set. The stream is not polled anymore afterwards.
        make_item: Option<F>,

        // Whether the final item is created when the stream ends.
        // Initially `false` only for `end_with_if_nonempty()`, set by the first item.
        emit_item: bool,
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
            .field("emit_item", &self.emit_item)
            .finish_non_exhaustive()
    }
}

impl<St, F> EndWith<St, F> {
    pub(super) const fn new(stream: St, make_item: F) -> Self {
        Self::with_emit_item(stream, make_item, true)
    }

    pub(super) const fn new_if_nonempty(stream: St, make_item: F) -> Self {
        Self::with_emit_item(stream, make_item, false)
    }

    const fn with_emit_item(stream: St, make_item: F, emit_item: bool) -> Self {
        Self {
            stream,
            make_item: Some(make_item),
            emit_item,
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
        let Some(item) = ready!(this.stream.poll_next(cx)) else {
            // Taken unconditionally to drop the closure of an empty stream.
            let make_item = this.make_item.take();
            return Poll::Ready(
                make_item
                    .filter(|_| *this.emit_item)
                    .map(MakeItem::make_item),
            );
        };
        *this.emit_item = true;
        Poll::Ready(Some(item))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        if self.make_item.is_none() {
            return (0, Some(0));
        }
        let (lower, upper) = self.stream.size_hint();
        // Unless already set, `emit_item` gets set only if the stream yields an item.
        let add_final_item = |count: usize| {
            if self.emit_item || count > 0 {
                count.checked_add(1)
            } else {
                Some(0)
            }
        };
        (
            add_final_item(lower).unwrap_or(usize::MAX),
            upper.and_then(add_final_item),
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
