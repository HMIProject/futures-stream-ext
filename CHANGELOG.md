<!-- SPDX-FileCopyrightText: The futures-stream-ext authors -->
<!-- SPDX-License-Identifier: MPL-2.0 -->

# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- The `dedup` stream types have a `yield_after(max_ready_count)` method that makes them yield to the executor after discarding `max_ready_count` consecutive ready items.
  By default, they still never yield while discarding items, like `filter()`.
  Use it if the input stream might be always ready and yield only items that are discarded, e.g. a synchronous stream.
  Otherwise, polling the stream never returns.

### Changed

- **Breaking:** `Debounce` yields the pending item immediately when the input stream ends, instead of waiting for the delay to elapse, like `debounceTime` in RxJS.
- **Breaking:** The stream types `Debounced` and `Throttled` were renamed to `Debounce` and `Throttle`, matching the names of the methods that return them.
- **Breaking:** The `distinct_until_changed` family was renamed to `dedup`, following `Vec::dedup()` and `Itertools::dedup()`:
  - The methods `distinct_until_changed()`, `distinct_until_changed_by()` and `distinct_until_changed_by_key()` to `dedup()`, `dedup_by()` and `dedup_by_key()`.
  - The stream types `DistinctUntilChanged*` to `Dedup*`.
  - `distinct_until_changed_ok()` and `distinct_until_changed_err()` were replaced by `dedup_by_some_key()`, which dedups items by a borrowed key and passes through items without a key.
    Replace them with `dedup_by_some_key(|result| result.as_ref().ok())` and `dedup_by_some_key(|result| result.as_ref().err())`.
    The stream types `DistinctUntilChangedOk` and `DistinctUntilChangedErr` were replaced by `DedupBySomeKey`.
  - `dedup_by()` invokes the comparison function with the last passed through item first, like `Itertools::dedup_by()` and unlike `Vec::dedup_by()`.
- **Breaking:** `dedup_by_key()` no longer takes an initial key and always passes through the first item, like `Vec::dedup_by_key()`.
  Replace `distinct_until_changed_by_key(None, |item| Some(key(item)))` with `dedup_by_key(|item| key(item))`.
  Replace an initial key that is not `None`, i.e. `distinct_until_changed_by_key(Some(initial_key), |item| Some(key(item)))`, with `skip_while({ let key = key.clone(); move |item| futures_util::future::ready(key(item) == initial_key) }).dedup_by_key(move |item| key(item))` to keep discarding leading items with the initial key.
  The `clone()` is only needed if `key` is a closure that is not `Copy`.
- **Breaking:** `Throttler::on_first_item()` and `Throttler::on_tick()` no longer take a `Context` argument.
  The throttler is polled right after each callback.
- **Breaking:** `debounce()` moved from `tokio::StreamExt` to `StreamExt`, where it requires the `tokio` feature.
- **Breaking:** `debounce()`, `debounce_with_sleep()`, `throttle()` and `switch_map()` no longer take a `max_ready_count` argument.
  It defaults to `NonZeroUsize::MIN` and can be set with the `yield_after(max_ready_count)` method of the returned stream, like for the `dedup` streams.
  Replace `throttle(throttler, max_ready_count)` with `throttle(throttler).yield_after(max_ready_count)` and omit `yield_after()` for `NonZeroUsize::MIN`.
- **Breaking:** `Throttler` is no longer generic over the item type and no longer a `Stream<Item = ()>`.
  Implement `Throttler::poll_tick()`, which returns `Poll<()>`, instead of `Stream::poll_next()`.
  `Throttler::on_tick()` takes `item_yielded: bool` instead of `Option<&T>`.
  `IntervalThrottler` no longer implements `Stream`.

### Removed

- **Breaking:** The `tokio::StreamExt` trait.
  Its `debounce()` method moved to `StreamExt`, so only one `StreamExt` of this crate needs to be imported.
- **Breaking:** The free functions `filter_distinct_until_changed()`, `filter_distinct_until_changed_ok()` and `filter_distinct_until_changed_err()`.
  Use the `StreamExt` methods `dedup()` and `dedup_by_some_key()` instead.
- **Breaking:** `DistinctState`, which was only needed for the removed free functions.
- **Breaking:** `StreamExt::throttle_interval_with_sleep()`, `tokio::StreamExt::throttle_interval()` and `IntervalThrottlerConfig`.
  Replace `throttle_interval_with_sleep::<S>(IntervalThrottlerConfig { period, edge }, max_ready_count)` with `throttle(IntervalThrottler::<S>::new(period, edge), max_ready_count)`.
  With Tokio, use `tokio::TokioIntervalThrottler::new(period, edge)`.

## [0.6.2] - 2026-11-23

### Added

- `StreamExt::switch_map()` and the stream type `SwitchMap` map each item to an inner stream and yield only the items of the latest inner stream.
  The previous inner stream is dropped when the next outer item arrives, like `switchMap` in RxJS.
- `StreamExt::end_with()` and the stream type `EndWith` yield a final item after the stream ended, like `endWith` in RxJS.
- `StreamExt::default_if_empty()` and the stream type `DefaultIfEmpty` yield a single item if the stream ends without yielding any items, like `defaultIfEmpty` in RxJS.
- The stream types `SwitchMap`, `EndWith` and `DefaultIfEmpty` implement `FusedStream`.

## [0.6.1] - 2026-11-20

### Added

- `StreamExt::distinct_until_changed_by()` and the stream type `DistinctUntilChangedBy` filter out adjacent items that are equal according to a comparison function.

## [0.6.0] - 2026-11-20

This release contains many breaking changes.
See [Migrating from 0.5](#migrating-from-05) for a summary.

### Added

- `StreamExt` can be used without the `tokio` feature.
  It provides the runtime-agnostic `debounce_with_sleep()` and `throttle_interval_with_sleep()`, which are generic over the `Sleep` implementation.
- `IntervalThrottler` is runtime-agnostic and generic over the `Sleep` implementation.
  It can be constructed with `IntervalThrottler::new()` and used with `StreamExt::throttle()`.
- `Sleep::reset()` restarts a sleep.
  The default implementation replaces the sleep with a new one.
  The implementation for `tokio::time::Sleep` reuses the registered timer.
- `Sleep::reset_next_period()` restarts an elapsed sleep on a fixed schedule.
  The default implementation falls back to `reset()`, so existing `Sleep` implementations keep working unchanged.
- `next_period_delay()` calculates the delay until the next deadline on a fixed schedule, with missed periods skipped.
  `Sleep` implementations that know their deadline can use it to override `reset_next_period()`.
- `DistinctState` holds the state of `filter_distinct_until_changed_ok()` and `filter_distinct_until_changed_err()`.
- The stream types `DistinctUntilChanged`, `DistinctUntilChangedByKey`, `DistinctUntilChangedOk` and `DistinctUntilChangedErr`.
  They implement `FusedStream` if the wrapped stream does.

### Changed

- **Breaking:** `debounce()` and `throttle_interval()` moved to the new `tokio::StreamExt` trait.
  The associated types `StreamExt::Sleep` and `StreamExt::IntervalThrottler` were removed.
- **Breaking:** `debounce()` and `debounce_with_sleep()` require a `poll_next_max_ready_count` argument, like `throttle()`.
- **Breaking:** The free functions `distinct_until_changed()`, `distinct_until_changed_map()`, `distinct_until_changed_ok_result()` and `distinct_until_changed_err_result()` were replaced by `StreamExt` methods.
  The methods return named stream types instead of `impl Stream`.
- **Breaking:** `distinct_until_changed_map()` was renamed to `distinct_until_changed_by_key()`.
  It compares the keys of items and passes the items through unmodified.
- **Breaking:** The `_result` suffix was dropped from `distinct_until_changed_ok()`, `distinct_until_changed_err()`, `filter_distinct_until_changed_ok()` and `filter_distinct_until_changed_err()`.
- **Breaking:** `filter_distinct_until_changed_ok()` and `filter_distinct_until_changed_err()` take a `DistinctState` instead of an `Option`.
- **Breaking:** The `Throttler` callbacks were renamed: `throttle_pending()` to `on_first_item()` and `throttle_ready()` to `on_tick()`.
- **Breaking:** `Throttled` calls `Throttler::on_first_item()` only while the throttler is idle, i.e. initially and after `on_tick()` was called with `None`.
  Throttlers should start the next interval in `on_tick()` only if an item is yielded.
- **Breaking:** `ThrottleIntervalConfig` was renamed to `IntervalThrottlerConfig`.
- **Breaking:** `tokio::IntervalThrottler` was replaced by the type alias `tokio::TokioIntervalThrottler` for `IntervalThrottler<tokio::time::Sleep>`.
  It no longer has a type parameter.
- **Breaking:** `Throttled::new()` is no longer public.
  Use `StreamExt::throttle()` instead.
- **Breaking:** The minimum supported Rust version is 1.89.
- Throttle intervals follow a fixed schedule and skip missed periods, instead of drifting by the scheduling latency.
- `Debounced` and `IntervalThrottler` reuse their sleep instead of creating a new one for each item or interval.
- `IntervalThrottler` starts the next interval only while items are pending.
- The distinct filters neither buffer items nor create a future for each item, and they reuse the allocation of the last value.

### Removed

- The dependency on `futures-core`.

### Fixed

- `Debounced` no longer polls an input stream that is always ready forever.
- `Throttled` no longer wakes itself up spuriously when it yields an item.

### Migrating from 0.5

| 0.5                                                           | 0.6                                                            |
| ------------------------------------------------------------- | -------------------------------------------------------------- |
| `StreamExt::debounce(delay)`                                  | `tokio::StreamExt::debounce(delay, poll_next_max_ready_count)` |
| `StreamExt::throttle_interval()`                              | `tokio::StreamExt::throttle_interval()`                        |
| `distinct_until_changed(stream)`                              | `stream.distinct_until_changed()`                              |
| `distinct_until_changed_map(stream, initial, f)`              | `stream.distinct_until_changed_by_key(initial, f)`             |
| `distinct_until_changed_ok_result(stream)`                    | `stream.distinct_until_changed_ok()`                           |
| `distinct_until_changed_err_result(stream)`                   | `stream.distinct_until_changed_err()`                          |
| `filter_distinct_until_changed_ok_result(&mut Option<T>, …)`  | `filter_distinct_until_changed_ok(&mut DistinctState<T>, …)`   |
| `filter_distinct_until_changed_err_result(&mut Option<E>, …)` | `filter_distinct_until_changed_err(&mut DistinctState<E>, …)`  |
| `Throttler::throttle_pending()`                               | `Throttler::on_first_item()`                                   |
| `Throttler::throttle_ready()`                                 | `Throttler::on_tick()`                                         |
| `ThrottleIntervalConfig`                                      | `IntervalThrottlerConfig`                                      |
| `tokio::IntervalThrottler<T>`                                 | `tokio::TokioIntervalThrottler`                                |
| `Throttled::new(stream, throttler, n)`                        | `stream.throttle(throttler, n)`                                |

A value of `NonZeroUsize::MIN` for `poll_next_max_ready_count` is a safe default.

## [0.5.0] and earlier

Changes before 0.6 are not recorded in this file.
See the [commit history](https://github.com/HMIProject/futures-stream-ext/commits/v0.5.0).

[Unreleased]: https://github.com/HMIProject/futures-stream-ext/compare/v0.6.2...HEAD
[0.6.2]: https://github.com/HMIProject/futures-stream-ext/compare/v0.6.1...v0.6.2
[0.6.1]: https://github.com/HMIProject/futures-stream-ext/compare/v0.6.0...v0.6.1
[0.6.0]: https://github.com/HMIProject/futures-stream-ext/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/HMIProject/futures-stream-ext/releases/tag/v0.5.0
