<!-- SPDX-FileCopyrightText: The futures-stream-ext authors -->
<!-- SPDX-License-Identifier: MPL-2.0 -->

# futures-stream-ext

[![Crates.io](https://img.shields.io/crates/v/futures-stream-ext.svg)](https://crates.io/crates/futures-stream-ext)
[![Docs.rs](https://docs.rs/futures-stream-ext/badge.svg)](https://docs.rs/futures-stream-ext)
[![Deps.rs](https://deps.rs/repo/github/HMIProject/futures-stream-ext/status.svg)](https://deps.rs/repo/github/HMIProject/futures-stream-ext)
[![Security audit](https://github.com/HMIProject/futures-stream-ext/actions/workflows/audit-dependencies.yaml/badge.svg)](https://github.com/HMIProject/futures-stream-ext/actions/workflows/audit-dependencies.yaml)
[![Continuous integration](https://github.com/HMIProject/futures-stream-ext/actions/workflows/test.yaml/badge.svg)](https://github.com/HMIProject/futures-stream-ext/actions/workflows/test.yaml)
[![License: MPL 2.0](https://img.shields.io/badge/License-MPL_2.0-brightgreen.svg)](https://opensource.org/licenses/MPL-2.0)

Extensions of the
[`Stream`](https://docs.rs/futures-core/latest/futures_core/stream/trait.Stream.html) trait.

Adds stream combinators that complement [`futures-util`](https://docs.rs/futures-util)
and are modeled after their [RxJS](https://rxjs.dev) counterparts:

- `dedup()`, `dedup_by()`, `dedup_by_key()`, `dedup_by_some_key()`: filter out adjacent
  duplicates (`distinctUntilChanged`)
- `debounce()`: yield an item only after no newer item arrived for a given delay (`debounceTime`)
- `throttle()`: yield at most the latest item per interval (`throttleTime`, `auditTime`)
- `switch_map()`: switch to a new inner stream for each outer item and drop the previous one
  (`switchMap`)
- `end_with()`, `end_with_if_nonempty()`: yield a final item after the stream ended (`endWith`)
- `default_if_empty()`, `default_if_empty_with()`: yield a single item if the stream ended empty
  (`defaultIfEmpty`)

The combinators are runtime-agnostic. Timers are abstracted by the `Sleep` trait. The optional
`tokio` feature provides an implementation for the Tokio runtime and adds `debounce()`, a
shorthand for `debounce_with_sleep()`.

## License

Licensed under the Mozilla Public License 2.0 (MPL-2.0) (see [MPL-2.0.txt](LICENSES/MPL-2.0.txt) or
<https://www.mozilla.org/MPL/2.0/>).

Permissions of this copyleft license are conditioned on making available source code of licensed
files and modifications of those files under the same license (or in certain cases, one of the GNU
licenses). Copyright and license notices must be preserved. Contributors provide an express grant of
patent rights. However, a larger work using the licensed work may be distributed under different
terms and without source code for files added in the larger work.

### Contribution

Any contribution intentionally submitted for inclusion in the work by you shall be licensed under
the Mozilla Public License 2.0 (MPL-2.0).

It is required to add the following header with the corresponding
[SPDX short identifier](https://spdx.dev/ids/) to the top of each file:

```rust
// SPDX-License-Identifier: MPL-2.0
```
