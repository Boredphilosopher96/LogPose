# Getting Started

Install Rust `1.94.1` and run:

```bash
cargo metadata --format-version 1 > /dev/null
scripts/check.sh
git config core.hooksPath .githooks
```

Run the beginner-friendly interactive mode:

```bash
cargo run -p logpose-cli -- interactive
```

Interactive mode stays open after each action so you can inspect results, copy the current view to the clipboard, go back to the previous form, or keep working through the next task. Collection-aware workflows open with live collection suggestions, and fuzzy selection is available for searchable fields from the keyboard.

Run direct operator commands:

```bash
cargo run -p logpose-cli -- status
cargo run -p logpose-cli -- collection create colors --dimensions 768 --metric cosine
```

Request machine-readable output when you need exact payloads:

```bash
cargo run -p logpose-cli -- --json status
```

Run the server:

```bash
cargo run -p logpose-server
```
