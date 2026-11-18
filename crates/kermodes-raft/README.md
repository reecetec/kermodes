# kermodes-raft: raft-rs, vendored

[raft-rs](https://github.com/tikv/raft-rs) (TiKV's port of etcd's raft), copied into this
repository so weir's replication (architecture.md §7.6) owns its consensus code and its wire
format. Licensed Apache-2.0, as upstream (`LICENSE`); the copyright notices in the files are
upstream's.

**Base: `ad13f3d90780f53aea2488c6a4b76c0d334bf136` (2026-05-13, "try to shrink unstable entries
buffer to avoid memory leak (#589)").** Its last crates.io release is 0.7.0 (2023); TiKV itself
depends on its master branch.

Why vendored rather than a git dependency: raft-rs is a pure state machine (no I/O, no clock, no
network), which is what the simulator drives; but its message types come from protobuf, either
rust-protobuf 2 (RUSTSEC-2024-0437) or prost 0.11 beside our 0.14 with `protoc` at build time.
Here the messages are written out (`proto/`), and the rest of upstream is kept as it is so its
fixes port cleanly.

## Layout

Upstream's crates, with their library names, so its code and tests are unchanged:

| Here | Upstream | Library |
|---|---|---|
| `.` | `raft` (`src/`) | `raft` |
| `proto/` | `raft-proto` | `raft_proto` |
| `harness/` | `harness` (with its integration tests) | `harness` |
| `datadriven/` | `datadriven` | `datadriven` |

Left out: benches and examples (criterion), the failpoint tests (`fail`), and upstream's CI and
build files.

## What differs from upstream

- `proto/src/lib.rs` replaces the generated `eraftpb` code: the same messages, fields, and
  accessors, encoded by hand in protobuf's wire format (the same bytes as upstream's
  rust-protobuf, pinned by a test; unknown fields are read past). `protocompat::PbMessage` is
  that codec's trait (`compute_size`, `write_to_bytes`, `merge_from_bytes`). Bytes fields are
  `bytes::Bytes`; repeated fields are `Vec`.
- Logging still goes through `slog` (its core crate only), so upstream's log statements are
  unchanged; `default_logger` writes warnings and errors to standard error instead of using
  `slog-term`.
- `fxhash` → `rustc-hash` (the same `FxHasher` names), `thiserror` 1 → 2, `getset` → written-out
  getters, `lazy_static` → `std::sync::LazyLock`, `similar-asserts` → `assert_eq!`.
- `tracker::Configuration::to_conf_state` and the majority configuration's `Display` sort their
  IDs, as etcd's do: upstream's order was fxhash's, which a different hasher changes.
- `quorum/majority.rs` builds its small stack array safely instead of with `MaybeUninit`.
- A few lints of a newer clippy are fixed in place; the harness tests allow the ones that come
  from `Vec` fields where upstream had rust-protobuf's containers.

## Porting an upstream fix

Compare upstream's change against the base above; apply it here by hand, with its tests; record
the new base in this file and in the commit message. Keep changes to upstream's code to what this
list describes, so the next port stays easy.
