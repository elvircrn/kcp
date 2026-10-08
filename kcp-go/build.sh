#!/usr/bin/env bash
# build.sh — cross-compile the Go kcp binary for the release matrix.
# Usage: ./build.sh [version]   (version only names the output dir)
set -euo pipefail
cd "$(dirname "$0")"

VERSION=${1:-dev}
OUT=dist/$VERSION
mkdir -p "$OUT"

platforms=(
  darwin/amd64
  darwin/arm64
  linux/amd64
  linux/arm64
)
for p in "${platforms[@]}"; do
  os=${p%/*}; arch=${p#*/}
  echo "building $os/$arch"
  GOOS=$os GOARCH=$arch CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" \
    -o "$OUT/kcp-$os-$arch" .
done

echo "checksums"
( cd "$OUT" && shasum -a 256 kcp-* > SHA256SUMS )
ls -la "$OUT"
