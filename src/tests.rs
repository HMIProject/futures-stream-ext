// SPDX-FileCopyrightText: The futures-stream-ext authors
// SPDX-License-Identifier: MPL-2.0

//! Runtime-agnostic tests that do not require any optional features.

use std::{
    cell::RefCell,
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

use futures_util::{FutureExt as _, Stream, StreamExt as _, stream};

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
    fn on_first_item(self: Pin<&mut Self>, _cx: &mut Context<'_>) {}

    fn on_tick(self: Pin<&mut Self>, _cx: &mut Context<'_>, _yielded_item: Option<&T>) {}
}

#[tokio::test]
async fn debounce_with_sleep_should_only_emit_last_of_ready_items() {
    let actual = stream::iter([1, 2, 3])
        .debounce_with_sleep::<NoSleep>(Duration::ZERO, NonZeroUsize::MAX)
        .collect::<Vec<_>>()
        .await;
    assert_eq!(vec![3], actual);
}

#[tokio::test]
async fn debounce_with_sleep_should_limit_polling_of_ready_items() {
    // The input stream is always ready and never finishes.
    let actual = stream::iter(0..)
        .debounce_with_sleep::<NoSleep>(Duration::ZERO, NonZeroUsize::new(3).unwrap())
        .take(2)
        .collect::<Vec<_>>()
        .await;
    assert_eq!(vec![2, 5], actual);
}

#[tokio::test]
async fn throttle_should_pass_through_all_items_if_always_ready() {
    let actual = stream::iter([1, 2, 3])
        .throttle(NoThrottler, NonZeroUsize::MIN)
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
    let mut stream =
        stream::iter([1, 2]).switch_map(|n| stream::iter([n * 10, n * 10 + 1]), NonZeroUsize::MAX);
    assert_eq!(poll_next(&mut stream), Poll::Ready(Some(20)));
    assert_eq!(poll_next(&mut stream), Poll::Ready(Some(21)));
    assert_eq!(poll_next(&mut stream), Poll::Ready(None));
}

#[test]
fn switch_map_ends_after_outer_and_inner_ended() {
    let mut stream =
        stream::iter([1]).switch_map(|n| stream::iter([n * 10, n * 10 + 1]), NonZeroUsize::MAX);
    assert_eq!(poll_next(&mut stream), Poll::Ready(Some(10)));
    assert_eq!(poll_next(&mut stream), Poll::Ready(Some(11)));
    assert_eq!(poll_next(&mut stream), Poll::Ready(None));
}

#[test]
fn switch_map_continues_after_inner_ended_while_outer_continues() {
    let queue = Rc::new(RefCell::new(VecDeque::from([1])));
    let mut stream =
        queued_stream(&queue).switch_map(|n| stream::iter([n * 10]), NonZeroUsize::MAX);
    assert_eq!(poll_next(&mut stream), Poll::Ready(Some(10)));
    // Inner stream ended, outer stream pending: switch_map must not end.
    assert_eq!(poll_next(&mut stream), Poll::Pending);

    queue.borrow_mut().push_back(2);
    assert_eq!(poll_next(&mut stream), Poll::Ready(Some(20)));
}

#[test]
fn switch_map_discards_pending_items_of_stale_inner_stream() {
    let queue = Rc::new(RefCell::new(VecDeque::from([1])));
    let mut stream = queued_stream(&queue).switch_map(
        |n| stream::iter([n * 10, n * 10 + 1]).chain(stream::pending()),
        NonZeroUsize::MAX,
    );
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
    let mut stream = pin!(outer.switch_map(
        |n| {
            let inner = stream::once(async move { n * 10 });
            assert_not_unpin(&inner);
            inner
        },
        NonZeroUsize::MAX,
    ));
    assert_eq!(poll_next(&mut stream), Poll::Ready(Some(10)));
    assert_eq!(poll_next(&mut stream), Poll::Ready(None));
}

#[tokio::test]
async fn switch_map_should_limit_polling_of_ready_outer_items() {
    // The outer stream is always ready and never finishes.
    let actual = stream::iter(0..)
        .switch_map(|n| stream::iter([n]), NonZeroUsize::new(3).unwrap())
        .take(2)
        .collect::<Vec<_>>()
        .await;
    assert_eq!(vec![2, 5], actual);
}

#[test]
fn switch_map_should_wake_itself_if_outer_stream_might_still_be_ready() {
    struct WakeFlag(AtomicBool);

    impl Wake for WakeFlag {
        fn wake(self: Arc<Self>) {
            self.0.store(true, Ordering::Relaxed);
        }
    }

    let wake_flag = Arc::new(WakeFlag(AtomicBool::new(false)));
    let waker = Waker::from(Arc::clone(&wake_flag));
    let mut cx = Context::from_waker(&waker);

    // The inner streams are always pending and never wake up the task.
    let mut stream =
        stream::iter(0..10).switch_map(|_| stream::pending::<u32>(), NonZeroUsize::new(3).unwrap());

    // Consumes the outer items 0..=8 in chunks of 3. The limit is reached while
    // the outer stream might still be ready.
    for _ in 0..3 {
        assert_eq!(stream.poll_next_unpin(&mut cx), Poll::Pending);
        assert!(wake_flag.0.swap(false, Ordering::Relaxed));
    }

    // Consumes the outer item 9 and the end of the outer stream. Only the outer
    // stream could wake up the task, but it has ended.
    assert_eq!(stream.poll_next_unpin(&mut cx), Poll::Pending);
    assert!(!wake_flag.0.load(Ordering::Relaxed));
}

#[test]
fn switch_map_should_only_switch_to_latest_ready_outer_item() {
    let mut switched = Vec::new();
    let mut stream = stream::iter(1..=5).switch_map(
        |n| {
            switched.push(n);
            stream::iter([n * 10])
        },
        NonZeroUsize::new(2).unwrap(),
    );
    assert_eq!(poll_next(&mut stream), Poll::Ready(Some(20)));
    assert_eq!(poll_next(&mut stream), Poll::Ready(Some(40)));
    assert_eq!(poll_next(&mut stream), Poll::Ready(Some(50)));
    assert_eq!(poll_next(&mut stream), Poll::Ready(None));
    drop(stream);
    assert_eq!(vec![2, 4, 5], switched);
}

#[test]
fn default_if_empty_yields_item_after_empty_stream() {
    let mut stream = pin!(stream::empty().default_if_empty(|| 0));
    assert_eq!(poll_next(&mut stream), Poll::Ready(Some(0)));
    assert_eq!(poll_next(&mut stream), Poll::Ready(None));
}

#[test]
fn default_if_empty_passes_through_non_empty_stream() {
    let mut stream = pin!(stream::iter([1, 2]).default_if_empty(|| 0));
    assert_eq!(poll_next(&mut stream), Poll::Ready(Some(1)));
    assert_eq!(poll_next(&mut stream), Poll::Ready(Some(2)));
    assert_eq!(poll_next(&mut stream), Poll::Ready(None));
}

#[test]
fn default_if_empty_stays_pending_while_stream_is_pending() {
    let mut stream = pin!(stream::pending::<u32>().default_if_empty(|| 0));
    assert_eq!(poll_next(&mut stream), Poll::Pending);
}

#[test]
fn default_if_empty_stays_pending_after_items_while_stream_is_pending() {
    let queue = Rc::new(RefCell::new(VecDeque::from([1])));
    let mut stream = pin!(queued_stream(&queue).default_if_empty(|| 0));
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
