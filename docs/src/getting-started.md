# Getting Started

Install Rust `1.94.1` and run:

```bash
cargo metadata --format-version 1 > /dev/null
scripts/check.sh
git config core.hooksPath .githooks
```

Run the CLI:

```bash
cargo run -p logpose-cli -- diagnostics status
```

Run the server:

```bash
cargo run -p logpose-server
```
