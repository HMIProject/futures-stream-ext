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

/// When the final item is yielded.
#[derive(Debug, Clone, Copy)]
enum Mode {
    Always,
    IfEmpty,
    IfNonempty,
}

impl Mode {
    /// Whether the final item is yielded after a stream that was (non-)empty.
    const fn emit_item(self, nonempty: bool) -> bool {
        match self {
            Self::Always => true,
            Self::IfEmpty => !nonempty,
            Self::IfNonempty => nonempty,
        }
    }
}

pin_project! {
    /// Stream returned by [`StreamExt::end_with()`],
    /// [`StreamExt::end_with_if_nonempty()`], [`StreamExt::default_if_empty()`]
    /// and [`StreamExt::default_if_empty_with()`].
    #[must_use = "streams do nothing unless polled"]
    pub struct EndWith<St, F> {
        #[pin]
        stream: Fuse<St>,

        // `None` after the final item has been created or as soon as it is
        // known that the final item will not be yielded.
        make_item: Option<F>,

        mode: Mode,

        // `true` after the stream yielded its first item.
        nonempty: bool,
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
            .field("mode", &self.mode)
            .field("nonempty", &self.nonempty)
            .finish_non_exhaustive()
    }
}

impl<St: Stream, F> EndWith<St, F> {
    pub(super) fn new(stream: St, make_item: F) -> Self {
        Self::with_mode(stream, make_item, Mode::Always)
    }

    pub(super) fn new_if_nonempty(stream: St, make_item: F) -> Self {
        Self::with_mode(stream, make_item, Mode::IfNonempty)
    }

    pub(super) fn new_if_empty(stream: St, make_item: F) -> Self {
        Self::with_mode(stream, make_item, Mode::IfEmpty)
    }

    fn with_mode(stream: St, make_item: F, mode: Mode) -> Self {
        Self {
            stream: stream.fuse(),
            make_item: Some(make_item),
            mode,
            nonempty: false,
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
        let Some(item) = ready!(this.stream.poll_next(cx)) else {
            // Taken unconditionally to drop the closure if not needed.
            let make_item = this.make_item.take();
            let emit_item = this.mode.emit_item(*this.nonempty);
            return Poll::Ready(make_item.filter(|_| emit_item).map(MakeItem::make_item));
        };
        *this.nonempty = true;
        if !this.mode.emit_item(true) {
            // Drop the closure early, because it is not needed anymore.
            *this.make_item = None;
        }
        Poll::Ready(Some(item))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let (lower, upper) = self.stream.size_hint();
        if self.make_item.is_none() {
            return (lower, upper);
        }
        // Adds the final item to `count` remaining items of the stream.
        let add_final_item = |count: usize| {
            let nonempty = self.nonempty || count > 0;
            count.checked_add(self.mode.emit_item(nonempty).into())
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
        self.stream.is_terminated() && self.make_item.is_none()
    }
}
