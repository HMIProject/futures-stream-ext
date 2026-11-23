// SPDX-FileCopyrightText: The futures-stream-ext authors
// SPDX-License-Identifier: MPL-2.0

//! Runtime-agnostic tests that do not require any optional features.

use std::{
    cell::{Cell, RefCell},
    collections::VecDeque,
    num::NonZeroUsize,
    pin::{Pin, pin},
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
    time::Duration,
};

use futures_util::{
    FutureExt as _, Stream, StreamExt as _,
    stream::{self, FusedStream as _},
};

use crate::{Sleep, StreamExt as _, Throttler, next_period_delay};

fn assert_not_unpin<S>(_future_or_stream: &S) {
    struct Check<T: ?Sized>(std::marker::PhantomData<T>);

    #[expect(dead_code)]
    trait AmbiguousIfUnpin {
        fn assert() {}
    }
    impl<T: ?Sized + Unpin> AmbiguousIfUnpin for Check<T> {}

    trait AmbiguousAlways {
        fn assert() {}
    }
    impl<T: ?Sized> AmbiguousAlways for Check<T> {}

    // Triggers a "multiple applicable items in scope" error ONLY if S: Unpin
    <Check<S>>::assert();
}

/// A [`Sleep`] that is ready immediately.
#[derive(Debug)]
struct NoSleep;

impl Future for NoSleep {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(())
    }
}

impl Sleep for NoSleep {
    fn sleep(_duration: Duration) -> Self {
        Self
    }
}

/// A [`Throttler`] that is always ready.
#[derive(Debug)]
struct NoThrottler;

impl Stream for NoThrottler {
    type Item = ();

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(Some(()))
    }
}

impl<T> Throttler<T> for NoThrottler {
    fn on_first_item(self: Pin<&mut Self>) {}

    fn on_tick(self: Pin<&mut Self>, _yielded_item: Option<&T>) {}
}

#[tokio::test]
async fn debounce_with_sleep_should_only_emit_last_of_ready_items() {
    let actual = stream::iter([1, 2, 3])
        .debounce_with_sleep::<NoSleep>(Duration::ZERO)
        .yield_after(NonZeroUsize::MAX)
        .collect::<Vec<_>>()
        .await;
    assert_eq!(vec![3], actual);
}

#[tokio::test]
async fn debounce_with_sleep_should_limit_polling_of_ready_items() {
    // The input stream is always ready and never finishes.
    let actual = stream::iter(0..)
        .debounce_with_sleep::<NoSleep>(Duration::ZERO)
        .yield_after(NonZeroUsize::new(3).unwrap())
        .take(2)
        .collect::<Vec<_>>()
        .await;
    assert_eq!(vec![2, 5], actual);
}

#[tokio::test]
async fn debounce_with_sleep_should_emit_every_item_with_zero_delay_and_min_ready_count() {
    let actual = stream::iter([1, 2, 3])
        .debounce_with_sleep::<NoSleep>(Duration::ZERO)
        .collect::<Vec<_>>()
        .await;
    assert_eq!(vec![1, 2, 3], actual);
}

#[tokio::test]
async fn throttle_should_skip_ready_items_with_greater_ready_count() {
    let actual = stream::iter([1, 2, 3])
        .throttle(NoThrottler)
        .yield_after(NonZeroUsize::new(2).unwrap())
        .collect::<Vec<_>>()
        .await;
    assert_eq!(vec![2, 3], actual);
}

#[tokio::test]
async fn throttle_should_pass_through_all_items_if_always_ready() {
    let actual = stream::iter([1, 2, 3])
        .throttle(NoThrottler)
        .collect::<Vec<_>>()
        .await;
    assert_eq!(vec![1, 2, 3], actual);
}

#[test]
fn next_period_delay_should_stay_on_schedule() {
    let period = Duration::from_millis(10);
    // Polled immediately or late, but within the next period.
    for (elapsed_millis, expected_millis) in [(0, 10), (1, 9), (9, 1)] {
        assert_eq!(
            Duration::from_millis(expected_millis),
            next_period_delay(Duration::from_millis(elapsed_millis), period)
        );
    }
}

#[test]
fn next_period_delay_should_skip_missed_periods() {
    let period = Duration::from_millis(10);
    for (elapsed_millis, expected_millis) in [(10, 10), (15, 5), (25, 5), (30, 10)] {
        assert_eq!(
            Duration::from_millis(expected_millis),
            next_period_delay(Duration::from_millis(elapsed_millis), period)
        );
    }
}

#[test]
fn switch_map_yields_only_inner_items_of_last_ready_outer_item() {
    let mut stream = stream::iter([1, 2])
        .switch_map(|n| stream::iter([n * 10, n * 10 + 1]))
        .yield_after(NonZeroUsize::MAX);
    assert_eq!(poll_next(&mut stream), Poll::Ready(Some(20)));
    assert_eq!(poll_next(&mut stream), Poll::Ready(Some(21)));
    assert_eq!(poll_next(&mut stream), Poll::Ready(None));
}

#[test]
fn switch_map_continues_after_inner_ended_while_outer_continues() {
    let queue = Rc::new(RefCell::new(VecDeque::from([1])));
    let mut stream = queued_stream(&queue)
        .switch_map(|n| stream::iter([n * 10]))
        .yield_after(NonZeroUsize::MAX);
    assert_eq!(poll_next(&mut stream), Poll::Ready(Some(10)));
    // Inner stream ended, outer stream pending: switch_map must not end.
    assert_eq!(poll_next(&mut stream), Poll::Pending);
    assert!(!stream.is_terminated());

    queue.borrow_mut().push_back(2);
    assert_eq!(poll_next(&mut stream), Poll::Ready(Some(20)));
}

#[test]
fn switch_map_discards_pending_items_of_stale_inner_stream() {
    let queue = Rc::new(RefCell::new(VecDeque::from([1])));
    let mut stream = queued_stream(&queue)
        .switch_map(|n| stream::iter([n * 10, n * 10 + 1]).chain(stream::pending()))
        .yield_after(NonZeroUsize::MAX);
    assert_eq!(poll_next(&mut stream), Poll::Ready(Some(10)));

    queue.borrow_mut().push_back(2);
    assert_eq!(poll_next(&mut stream), Poll::Ready(Some(20)));
    assert_eq!(poll_next(&mut stream), Poll::Ready(Some(21)));
    assert_eq!(poll_next(&mut stream), Poll::Pending);
}

#[test]
fn switch_map_accepts_streams_that_are_not_unpin() {
    let outer = stream::once(async { 1 });
    assert_not_unpin(&outer);
    let mut stream = pin!(
        outer
            .switch_map(|n| {
                let inner = stream::once(async move { n * 10 });
                assert_not_unpin(&inner);
                inner
            })
            .yield_after(NonZeroUsize::MAX)
    );
    assert_eq!(poll_next(&mut stream), Poll::Ready(Some(10)));
    assert_eq!(poll_next(&mut stream), Poll::Ready(None));
}

#[tokio::test]
async fn switch_map_should_limit_polling_of_ready_outer_items() {
    // The outer stream is always ready and never finishes.
    let actual = stream::iter(0..)
        .switch_map(|n| stream::iter([n]))
        .yield_after(NonZeroUsize::new(3).unwrap())
        .take(2)
        .collect::<Vec<_>>()
        .await;
    assert_eq!(vec![2, 5], actual);
}

struct WakeFlag(AtomicBool);

impl WakeFlag {
    /// Returns the flag and a waker that sets it.
    fn new_waker() -> (Arc<Self>, Waker) {
        let wake_flag = Arc::new(Self(AtomicBool::new(false)));
        let waker = Waker::from(Arc::clone(&wake_flag));
        (wake_flag, waker)
    }

    /// Returns `true` if the waker has been woken, and resets the flag.
    fn take(&self) -> bool {
        self.0.swap(false, Ordering::Relaxed)
    }
}

impl Wake for WakeFlag {
    fn wake(self: Arc<Self>) {
        self.0.store(true, Ordering::Relaxed);
    }
}

#[test]
fn dedup_should_not_yield_to_the_executor_by_default() {
    // Discards 100 duplicates during a single invocation.
    let mut stream = stream::repeat(1).take(100).chain(stream::iter([2])).dedup();
    assert_eq!(poll_next(&mut stream), Poll::Ready(Some(1)));
    assert_eq!(poll_next(&mut stream), Poll::Ready(Some(2)));
    assert_eq!(poll_next(&mut stream), Poll::Ready(None));
}

#[test]
fn dedup_should_yield_to_the_executor_after_discarding_items() {
    let (wake_flag, waker) = WakeFlag::new_waker();
    let mut cx = Context::from_waker(&waker);

    // Always ready and yields 100 duplicates. Continues after yielding to the
    // executor.
    let mut stream = stream::repeat(1)
        .take(100)
        .chain(stream::iter([2]))
        .dedup()
        .yield_after(NonZeroUsize::new(32).unwrap());
    assert_eq!(stream.poll_next_unpin(&mut cx), Poll::Ready(Some(1)));
    let mut pending_count = 0;
    let next_item = loop {
        match stream.poll_next_unpin(&mut cx) {
            Poll::Ready(next_item) => break next_item,
            Poll::Pending => {
                assert!(wake_flag.take());
                pending_count += 1;
            }
        }
    };
    assert_eq!(next_item, Some(2));
    assert_eq!(pending_count, 3);
}

#[test]
fn dedup_ok_should_yield_to_the_executor_after_discarding_items() {
    let (wake_flag, waker) = WakeFlag::new_waker();
    let mut cx = Context::from_waker(&waker);

    // Always ready and yields only duplicates after the first item.
    let mut stream = stream::repeat(Ok::<_, ()>(1))
        .dedup_ok()
        .yield_after(NonZeroUsize::MIN);
    assert_eq!(stream.poll_next_unpin(&mut cx), Poll::Ready(Some(Ok(1))));
    assert_eq!(stream.poll_next_unpin(&mut cx), Poll::Pending);
    assert!(wake_flag.take());
}

#[test]
fn switch_map_should_wake_itself_if_outer_stream_might_still_be_ready() {
    let (wake_flag, waker) = WakeFlag::new_waker();
    let mut cx = Context::from_waker(&waker);

    // The inner streams are always pending and never wake up the task.
    let mut stream = stream::iter(0..10)
        .switch_map(|_| stream::pending::<u32>())
        .yield_after(NonZeroUsize::new(3).unwrap());

    // Consumes the outer items 0..=8 in chunks of 3. The limit is reached while
    // the outer stream might still be ready.
    for _ in 0..3 {
        assert_eq!(stream.poll_next_unpin(&mut cx), Poll::Pending);
        assert!(wake_flag.take());
    }

    // Consumes the outer item 9 and the end of the outer stream. Only the outer
    // stream could wake up the task, but it has ended.
    assert_eq!(stream.poll_next_unpin(&mut cx), Poll::Pending);
    assert!(!wake_flag.take());
}

#[test]
fn switch_map_should_only_switch_to_latest_ready_outer_item() {
    let mut switched = Vec::new();
    let mut stream = stream::iter(1..=5)
        .switch_map(|n| {
            switched.push(n);
            stream::iter([n * 10])
        })
        .yield_after(NonZeroUsize::new(2).unwrap());
    assert_eq!(poll_next(&mut stream), Poll::Ready(Some(20)));
    assert_eq!(poll_next(&mut stream), Poll::Ready(Some(40)));
    assert_eq!(poll_next(&mut stream), Poll::Ready(Some(50)));
    assert_eq!(poll_next(&mut stream), Poll::Ready(None));
    drop(stream);
    assert_eq!(vec![2, 4, 5], switched);
}

#[test]
fn switch_map_should_drop_switch_to_after_outer_ended() {
    let captured = Rc::new(());
    let mut stream = stream::iter([1])
        .switch_map({
            let captured = Rc::clone(&captured);
            move |n| {
                let _ = &captured;
                stream::iter([n * 10]).chain(stream::pending())
            }
        })
        .yield_after(NonZeroUsize::MAX);
    assert_eq!(Rc::strong_count(&captured), 2);
    assert_eq!(poll_next(&mut stream), Poll::Ready(Some(10)));
    // The outer stream ended, but the inner stream is still active.
    assert_eq!(poll_next(&mut stream), Poll::Pending);
    assert_eq!(Rc::strong_count(&captured), 1);
}

#[test]
fn switch_map_should_be_terminated_after_outer_and_inner_ended() {
    let queue = Rc::new(RefCell::new(VecDeque::from([1])));
    let mut stream = queued_stream(&queue)
        .take(1)
        .switch_map(|n| stream::iter([n * 10]))
        .yield_after(NonZeroUsize::MAX);
    assert!(!stream.is_terminated());
    assert_eq!(poll_next(&mut stream), Poll::Ready(Some(10)));
    // The outer stream ended, but the inner stream has not reported its end yet.
    assert!(!stream.is_terminated());
    assert_eq!(poll_next(&mut stream), Poll::Ready(None));
    assert!(stream.is_terminated());
    // Polling a terminated stream again is safe.
    assert_eq!(poll_next(&mut stream), Poll::Ready(None));
}

#[test]
fn switch_map_should_report_size_hint() {
    let queue = Rc::new(RefCell::new(VecDeque::from([1])));
    let mut stream = queued_stream(&queue)
        .switch_map(|n| stream::iter([n * 10]))
        .yield_after(NonZeroUsize::MAX);
    // The outer stream continues.
    assert_eq!(stream.size_hint(), (0, None));
    assert_eq!(poll_next(&mut stream), Poll::Ready(Some(10)));
    assert_eq!(stream.size_hint(), (0, None));

    let mut stream = stream::iter([1])
        .switch_map(|n| stream::iter([n * 10, n * 10 + 1]))
        .yield_after(NonZeroUsize::MAX);
    assert_eq!(stream.size_hint(), (0, None));
    assert_eq!(poll_next(&mut stream), Poll::Ready(Some(10)));
    // The outer stream ended, so only the remaining items of the inner stream follow.
    assert_eq!(stream.size_hint(), (1, Some(1)));
    assert_eq!(poll_next(&mut stream), Poll::Ready(Some(11)));
    assert_eq!(poll_next(&mut stream), Poll::Ready(None));
    assert_eq!(stream.size_hint(), (0, Some(0)));

    let mut stream = stream::empty::<u32>().switch_map(|n| stream::iter([n]));
    assert_eq!(poll_next(&mut stream), Poll::Ready(None));
    assert_eq!(stream.size_hint(), (0, Some(0)));
}

#[test]
fn switch_map_should_implement_debug_for_closures() {
    let mut stream = stream::iter([1]).switch_map(|n| stream::iter([n * 10]));
    assert_eq!(poll_next(&mut stream), Poll::Ready(Some(10)));
    let debug = format!("{stream:?}");
    assert!(debug.starts_with("SwitchMap { outer: "), "{debug}");
    assert!(debug.contains("inner: Some("), "{debug}");
    assert!(debug.ends_with(", max_ready_count: 1, .. }"), "{debug}");
}

#[test]
fn end_with_yields_final_item_after_stream_ended() {
    let mut stream = stream::iter([1, 2]).end_with(|| 0);
    assert_eq!(stream.size_hint(), (3, Some(3)));
    assert_eq!(poll_next(&mut stream), Poll::Ready(Some(1)));
    assert_eq!(poll_next(&mut stream), Poll::Ready(Some(2)));
    assert!(!stream.is_terminated());
    assert_eq!(poll_next(&mut stream), Poll::Ready(Some(0)));
    assert_eq!(stream.size_hint(), (0, Some(0)));
    assert_eq!(poll_next(&mut stream), Poll::Ready(None));
    assert!(stream.is_terminated());
    assert_eq!(poll_next(&mut stream), Poll::Ready(None));
}

#[test]
fn end_with_yields_final_item_after_empty_stream() {
    let mut stream = stream::empty().end_with(|| 0);
    assert_eq!(poll_next(&mut stream), Poll::Ready(Some(0)));
    assert_eq!(poll_next(&mut stream), Poll::Ready(None));
}

#[test]
fn end_with_accepts_default_item() {
    let mut stream = stream::iter([1]).end_with(crate::DefaultItem::new());
    assert_eq!(poll_next(&mut stream), Poll::Ready(Some(1)));
    assert_eq!(poll_next(&mut stream), Poll::Ready(Some(0)));
    assert_eq!(poll_next(&mut stream), Poll::Ready(None));
}

#[test]
fn end_with_creates_final_item_only_after_stream_ended() {
    let queue = Rc::new(RefCell::new(VecDeque::from([1])));
    let created = Rc::new(Cell::new(false));
    let mut stream = queued_stream(&queue).end_with({
        let created = Rc::clone(&created);
        move || {
            created.set(true);
            0
        }
    });
    assert_eq!(poll_next(&mut stream), Poll::Ready(Some(1)));
    assert_eq!(poll_next(&mut stream), Poll::Pending);
    assert!(!created.get());
}

#[test]
fn end_with_accepts_streams_that_are_not_unpin() {
    let inner = stream::once(async { 1 });
    assert_not_unpin(&inner);
    let mut stream = pin!(inner.end_with(|| 0));
    assert_eq!(poll_next(&mut stream), Poll::Ready(Some(1)));
    assert_eq!(poll_next(&mut stream), Poll::Ready(Some(0)));
    assert_eq!(poll_next(&mut stream), Poll::Ready(None));
}

#[test]
fn end_with_if_nonempty_yields_final_item_after_nonempty_stream_ended() {
    let mut stream = stream::iter([1, 2]).end_with_if_nonempty(|| 0);
    assert_eq!(stream.size_hint(), (3, Some(3)));
    assert_eq!(poll_next(&mut stream), Poll::Ready(Some(1)));
    assert_eq!(stream.size_hint(), (2, Some(2)));
    assert_eq!(poll_next(&mut stream), Poll::Ready(Some(2)));
    assert_eq!(stream.size_hint(), (1, Some(1)));
    assert!(!stream.is_terminated());
    assert_eq!(poll_next(&mut stream), Poll::Ready(Some(0)));
    assert_eq!(stream.size_hint(), (0, Some(0)));
    assert_eq!(poll_next(&mut stream), Poll::Ready(None));
    assert!(stream.is_terminated());
    assert_eq!(poll_next(&mut stream), Poll::Ready(None));
}

#[test]
fn end_with_if_nonempty_keeps_empty_stream_empty() {
    let created = Rc::new(Cell::new(false));
    let mut stream = stream::empty().end_with_if_nonempty({
        let created = Rc::clone(&created);
        move || {
            created.set(true);
            0
        }
    });
    assert_eq!(stream.size_hint(), (0, Some(0)));
    assert!(!stream.is_terminated());
    assert_eq!(poll_next(&mut stream), Poll::Ready(None));
    assert!(stream.is_terminated());
    assert_eq!(poll_next(&mut stream), Poll::Ready(None));
    assert!(!created.get());
}

#[test]
fn end_with_if_nonempty_size_hint_of_unknown_stream() {
    let queue = Rc::new(RefCell::new(VecDeque::from([1])));
    let mut stream = queued_stream(&queue).end_with_if_nonempty(|| 0);
    assert_eq!(stream.size_hint(), (0, None));
    assert_eq!(poll_next(&mut stream), Poll::Ready(Some(1)));
    assert_eq!(stream.size_hint(), (1, None));
}

#[test]
fn end_with_if_nonempty_creates_final_item_only_after_stream_ended() {
    let queue = Rc::new(RefCell::new(VecDeque::from([1])));
    let created = Rc::new(Cell::new(false));
    let mut stream = queued_stream(&queue).end_with_if_nonempty({
        let created = Rc::clone(&created);
        move || {
            created.set(true);
            0
        }
    });
    assert_eq!(poll_next(&mut stream), Poll::Ready(Some(1)));
    assert_eq!(poll_next(&mut stream), Poll::Pending);
    assert!(!created.get());
}

#[test]
fn end_with_if_nonempty_accepts_streams_that_are_not_unpin() {
    let inner = stream::once(async { 1 });
    assert_not_unpin(&inner);
    let mut stream = pin!(inner.end_with_if_nonempty(|| 0));
    assert_eq!(poll_next(&mut stream), Poll::Ready(Some(1)));
    assert_eq!(poll_next(&mut stream), Poll::Ready(Some(0)));
    assert_eq!(poll_next(&mut stream), Poll::Ready(None));
}

#[test]
fn end_with_and_default_if_empty_with_implement_debug_for_closures() {
    let debug = format!("{:?}", stream::iter([1]).end_with(|| 0));
    assert!(debug.starts_with("EndWith { stream: "), "{debug}");
    assert!(debug.ends_with(", emit_item: true, .. }"), "{debug}");
    let debug = format!("{:?}", stream::iter([1]).end_with_if_nonempty(|| 0));
    assert!(debug.starts_with("EndWith { stream: "), "{debug}");
    assert!(debug.ends_with(", emit_item: false, .. }"), "{debug}");
    let debug = format!("{:?}", stream::iter([1]).default_if_empty_with(|| 0));
    assert!(debug.starts_with("DefaultIfEmpty { stream: "), "{debug}");
    assert!(debug.ends_with(", .. }"), "{debug}");
}

#[test]
fn default_if_empty_yields_default_item_after_empty_stream() {
    let mut stream = stream::empty::<u32>().default_if_empty();
    assert_eq!(poll_next(&mut stream), Poll::Ready(Some(0)));
    assert_eq!(poll_next(&mut stream), Poll::Ready(None));
}

#[test]
fn default_if_empty_does_not_store_a_function_pointer() {
    assert_eq!(size_of::<crate::DefaultItem<u32>>(), 0);
}

#[test]
fn default_if_empty_with_accepts_custom_make_item() {
    struct Fallback;

    impl crate::MakeItem<u32> for Fallback {
        fn make_item(self) -> u32 {
            42
        }
    }

    let mut stream = stream::empty().default_if_empty_with(Fallback);
    assert_eq!(poll_next(&mut stream), Poll::Ready(Some(42)));
    assert_eq!(poll_next(&mut stream), Poll::Ready(None));
}

#[test]
fn default_item_can_be_created_and_copied() {
    // `NoDefault` implements neither `Clone` nor `Default`.
    struct NoDefault;

    // Compiles only if `DefaultItem<NoDefault>` is `Copy` and `Default`.
    let default_item = crate::DefaultItem::<NoDefault>::new();
    let default_items = [default_item, default_item, Default::default()];
    assert_eq!(default_items.len(), 3);

    let make_item = crate::DefaultItem::<u32>::default();
    let mut stream = stream::empty().default_if_empty_with(make_item);
    assert_eq!(poll_next(&mut stream), Poll::Ready(Some(0)));
}

#[test]
fn default_if_empty_passes_through_non_empty_stream() {
    let mut stream = stream::iter([1, 2]).default_if_empty();
    assert_eq!(poll_next(&mut stream), Poll::Ready(Some(1)));
    assert_eq!(poll_next(&mut stream), Poll::Ready(Some(2)));
    assert_eq!(poll_next(&mut stream), Poll::Ready(None));
}

#[test]
fn default_if_empty_with_is_terminated_after_empty_stream_ended() {
    let mut stream = stream::empty().default_if_empty_with(|| 0);
    assert_eq!(stream.size_hint(), (1, Some(1)));
    assert!(!stream.is_terminated());
    assert_eq!(poll_next(&mut stream), Poll::Ready(Some(0)));
    assert_eq!(stream.size_hint(), (0, Some(0)));
    assert_eq!(poll_next(&mut stream), Poll::Ready(None));
    assert!(stream.is_terminated());
    assert_eq!(poll_next(&mut stream), Poll::Ready(None));
}

#[test]
fn default_if_empty_with_is_terminated_after_non_empty_stream_ended() {
    let mut stream = stream::iter([1, 2]).default_if_empty_with(|| 0);
    assert_eq!(stream.size_hint(), (2, Some(2)));
    assert_eq!(poll_next(&mut stream), Poll::Ready(Some(1)));
    assert_eq!(stream.size_hint(), (1, Some(1)));
    assert_eq!(poll_next(&mut stream), Poll::Ready(Some(2)));
    assert!(!stream.is_terminated());
    assert_eq!(poll_next(&mut stream), Poll::Ready(None));
    assert!(stream.is_terminated());
}

#[test]
fn default_if_empty_with_stays_pending_while_stream_is_pending() {
    let mut stream = stream::pending::<u32>().default_if_empty_with(|| 0);
    assert_eq!(poll_next(&mut stream), Poll::Pending);
}

#[test]
fn default_if_empty_with_stays_pending_after_items_while_stream_is_pending() {
    let queue = Rc::new(RefCell::new(VecDeque::from([1])));
    let mut stream = queued_stream(&queue).default_if_empty_with(|| 0);
    assert_eq!(poll_next(&mut stream), Poll::Ready(Some(1)));
    assert_eq!(poll_next(&mut stream), Poll::Pending);
}

/// Stream that yields the items pushed into the queue and never ends.
fn queued_stream(queue: &Rc<RefCell<VecDeque<u32>>>) -> impl stream::Stream<Item = u32> + Unpin {
    let queue = Rc::clone(queue);
    stream::poll_fn(move |_cx| match queue.borrow_mut().pop_front() {
        Some(item) => Poll::Ready(Some(item)),
        None => Poll::Pending,
    })
}

/// Polls the stream once without a waker, distinguishing a pending stream
/// (`Poll::Pending`) from an ended stream (`Poll::Ready(None)`).
fn poll_next<S: stream::Stream + Unpin>(stream: &mut S) -> Poll<Option<S::Item>> {
    stream
        .next()
        .now_or_never()
        .map_or(Poll::Pending, Poll::Ready)
}
