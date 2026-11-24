// SPDX-FileCopyrightText: The futures-stream-ext authors
// SPDX-License-Identifier: MPL-2.0

use std::{fmt, marker::PhantomData};

#[cfg(doc)]
use super::StreamExt;

/// Creates a single item on demand.
///
/// Used by [`StreamExt::end_with()`] and [`StreamExt::default_if_empty()`].
/// Implemented for closures and for [`DefaultItem`].
///
/// This trait is needed, because a closure bound alone would not accept
/// [`DefaultItem`]: custom types cannot implement [`FnOnce`] on stable Rust.
pub trait MakeItem<T> {
    /// Creates the item.
    fn make_item(self) -> T;
}

// Does not conflict with the impl for `DefaultItem`, because `FnOnce` is a
// fundamental trait: the compiler may rely on `DefaultItem` not implementing
// it, because only this crate could add that impl.
impl<T, F> MakeItem<T> for F
where
    F: FnOnce() -> T,
{
    fn make_item(self) -> T {
        self()
    }
}

/// Creates [`Default::default()`].
///
/// Can be passed to [`StreamExt::end_with()`] and
/// [`StreamExt::default_if_empty()`] instead of the function pointer `fn() -> T`. A function pointer would add its size to the stream that
/// stores it and would be invoked indirectly. `DefaultItem` has no size and invokes
/// [`Default::default()`] statically.
// `PhantomData<fn() -> T>` instead of `PhantomData<T>`: `DefaultItem` never
// owns a `T`. It is `Send`, `Sync` and `Unpin` regardless of `T`, and it is
// covariant in `T`.
pub struct DefaultItem<T>(PhantomData<fn() -> T>);

impl<T> DefaultItem<T> {
    /// Creates a `DefaultItem`.
    #[must_use]
    pub const fn new() -> Self {
        Self(PhantomData)
    }
}

// `Clone`, `Copy`, `Default` and `Debug` are implemented manually,
// because the derives would require the same traits for `T`.

impl<T> Clone for DefaultItem<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for DefaultItem<T> {}

impl<T> Default for DefaultItem<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> fmt::Debug for DefaultItem<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("DefaultItem")
    }
}

impl<T: Default> MakeItem<T> for DefaultItem<T> {
    fn make_item(self) -> T {
        T::default()
    }
}
