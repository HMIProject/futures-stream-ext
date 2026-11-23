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
    /// Shared implementation of [`EndWith`] and [`EndWithIfNonempty`].
    struct EndWithInner<St, F> {
        #[pin]
        stream: St,

        // `None` after the stream ended. The final item has been created
        // if `emit_item` was set. The stream is not polled anymore afterwards.
        make_item: Option<F>,

        // Whether the final item is created when the stream ends.
        // Initially `false` only for `EndWithIfNonempty`, set by the first item.
        emit_item: bool,
    }
}

impl<St, F> EndWithInner<St, F> {
    const fn new(stream: St, make_item: F, emit_item: bool) -> Self {
        Self {
            stream,
            make_item: Some(make_item),
            emit_item,
        }
    }
}

impl<St, F> Stream for EndWithInner<St, F>
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

/// Defines a public wrapper of [`EndWithInner`].
macro_rules! end_with_wrapper {
    ($(#[$attr:meta])* $name:ident, emit_item: $emit_item:literal) => {
        pin_project! {
            $(#[$attr])*
            #[must_use = "streams do nothing unless polled"]
            pub struct $name<St, F> {
                #[pin]
                inner: EndWithInner<St, F>,
            }
        }

        impl<St, F> $name<St, F> {
            pub(super) const fn new(stream: St, make_item: F) -> Self {
                Self {
                    inner: EndWithInner::new(stream, make_item, $emit_item),
                }
            }
        }

        impl<St, F> Stream for $name<St, F>
        where
            St: Stream,
            F: MakeItem<St::Item>,
        {
            type Item = St::Item;

            fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
                self.project().inner.poll_next(cx)
            }

            fn size_hint(&self) -> (usize, Option<usize>) {
                self.inner.size_hint()
            }
        }

        impl<St, F> FusedStream for $name<St, F>
        where
            St: Stream,
            F: MakeItem<St::Item>,
        {
            fn is_terminated(&self) -> bool {
                self.inner.make_item.is_none()
            }
        }
    };
}

end_with_wrapper!(
    /// Stream returned by [`StreamExt::end_with()`].
    EndWith,
    emit_item: true
);

end_with_wrapper!(
    /// Stream returned by [`StreamExt::end_with_if_nonempty()`].
    EndWithIfNonempty,
    emit_item: false
);

// Implemented manually, because closures do not implement `Debug`.
impl<St, F> fmt::Debug for EndWith<St, F>
where
    St: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EndWith")
            .field("stream", &self.inner.stream)
            .finish_non_exhaustive()
    }
}

// Implemented manually, because closures do not implement `Debug`.
impl<St, F> fmt::Debug for EndWithIfNonempty<St, F>
where
    St: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EndWithIfNonempty")
            .field("stream", &self.inner.stream)
            .field("nonempty", &self.inner.emit_item)
            .finish_non_exhaustive()
    }
}
