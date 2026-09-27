#!/usr/bin/env sh
# Resolve a protoc binary for build scripts that cannot use protoc-bin-vendored
# directly, such as the one in etcd-client.
#
# `.cargo/config.toml` points `PROTOC` here, so `cargo build` and `cargo test`
# work without a system protoc. An explicit `PROTOC` in the environment still
# takes precedence over the config. Resolution order:
#
# 1. the protoc-bin-vendored binary for this host from the Cargo registry,
#    which the workspace already downloads for `logpose-api-grpc`
# 2. `protoc` on `PATH`
set -eu

case "$(uname -s)-$(uname -m)" in
  Linux-x86_64 | Linux-amd64) platform="linux-x86_64" ;;
  Linux-aarch64 | Linux-arm64) platform="linux-aarch_64" ;;
  Linux-i386 | Linux-i686) platform="linux-x86_32" ;;
  Linux-ppc64le) platform="linux-ppcle_64" ;;
  Linux-s390x) platform="linux-s390_64" ;;
  Darwin-x86_64) platform="macos-x86_64" ;;
  Darwin-arm64 | Darwin-aarch64) platform="macos-aarch_64" ;;
  *) platform="" ;;
esac

if [ -n "${platform}" ]; then
  vendored=""
  for candidate in "${CARGO_HOME:-${HOME}/.cargo}"/registry/src/*/protoc-bin-vendored-"${platform}"-*/bin/protoc; do
    if [ -x "${candidate}" ]; then
      vendored="${candidate}"
    fi
  done
  if [ -n "${vendored}" ]; then
    exec "${vendored}" "$@"
  fi
fi

if command -v protoc >/dev/null 2>&1; then
  exec protoc "$@"
fi

echo "scripts/protoc.sh: no protoc found." >&2
echo "Run 'cargo fetch' to download the vendored protoc, install protoc on PATH, or set PROTOC." >&2
exit 1
