# Contributing

Contributing guidance is defined in the root `CONTRIBUTING.md`.

The canonical testing doctrine for the repository lives in [Testing](./testing.md). Use that chapter as the source of truth for harness design, test placement, and the long-term TigerBeetle-inspired layering strategy.

Enable the tracked pre-push hook once per clone with `git config core.hooksPath .githooks`. That hook runs `cargo deny check`, `cargo audit`, and `cargo machete` before pushes.

Before opening a pull request, run:

```bash
scripts/check.sh
```

The full workspace verification flow now expects etcd to be reachable at
`http://127.0.0.1:2379` unless you override `LOGPOSE_TEST_ETCD_ENDPOINTS`.
