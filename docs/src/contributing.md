# Contributing

Contributing guidance is defined in the root `CONTRIBUTING.md`.

The canonical testing doctrine for the repository lives in [Testing](./testing.md). Use that chapter as the source of truth for harness design, test placement, and the long-term TigerBeetle-inspired layering strategy.

Enable the tracked pre-push hook once per clone with `git config core.hooksPath .githooks`. That hook runs `cargo deny check`, `cargo audit`, and `cargo machete` before pushes.

Before opening a pull request, run:

```bash
scripts/check.sh
```

The etcd integration tests run only when `LOGPOSE_TEST_ETCD_ENDPOINTS` is set,
for example to `http://127.0.0.1:2379`. When it is set, an unreachable etcd
fails those tests. When it is unset, they pass without running (run with
`--nocapture` to see the skip message), except when `CI` is set, where they
fail so CI cannot silently drop them. The Rust Tests workflow sets it.
`scripts/check.sh` builds the mdBook only when `mdbook` and `mdbook-toc` are
installed.
