# kcp — resilient `kubectl cp`

`kubectl cp` streams one tar over a single websocket/SPDY connection, so
large pod → local transfers regularly die with:

```
error: error reading from error stream: read message: websocket: close 1006
```

kcp is a drop-in replacement that survives flaky connections. Single
static Go binary using client-go's exec transport directly — one
process, one authenticated connection pool, no kubectl fork per
operation.

## Usage

```sh
kcp -n <namespace> <pod>:/remote/path /local/dest
```

e.g.

```sh
kcp -n elvircrn-kimi-k3 qwen3-30b-pd-disagg:/var/cache/vllm/profile \
  /Users/ecrncevi/new_profile_dump_v6
```

Install from a [release](https://github.com/elvircrn/kcp/releases):

```sh
curl -LO https://github.com/elvircrn/kcp/releases/download/v1.0.0/kcp-darwin-arm64
chmod +x kcp-darwin-arm64 && sudo mv kcp-darwin-arm64 /usr/local/bin/kcp
```

Or build from source: `cd kcp-go && go build -o kcp .`
(`./build.sh` cross-compiles the full release matrix.)

## How it survives bad connections

- **Per-file streams** — one transfer per operation instead of one giant
  tar, so a dropped connection costs one file (or one chunk), not the
  whole transfer.
- **Small-file batching** — files ≤1MiB are fetched in a single
  tar-over-exec batch (≤200 files per batch) instead of one exec each;
  any batch failure falls back to per-file copy for its members.
- **Chunking with resume** — larger files are fetched in size-verified
  MiB-aligned chunks into `<dest>.kcp.part`. An interrupted run leaves
  the partial file; rerun the same command and it continues from where
  it stopped. Chunks stream straight to disk — no per-chunk buffering,
  so parallel transfers stay flat on RAM.
- **Dynamic chunk sizing** — after 4 clean chunks the size doubles (up to
  `--max-chunk-mb`); a failed chunk halves it (down to `--min-chunk-mb`).
  Good connections ramp up, struggling ones back off.
- **Retries with exponential backoff + jitter** — every remote operation
  is retried up to `--retries` times with capped, jittered backoff.
- **Parallel workers** — `--parallel N` concurrent transfers (default 8)
  over one connection pool. Large files are sorted biggest-first and
  dealt round-robin so long transfers start immediately.
- **Verification** — every file is size-checked; `--verify md5` also
  compares checksums on both ends. Verified files are skipped on rerun.
- **SPDY exec transport** — avoids the websocket exec path that produces
  `close 1006`.
- **Hardened extraction** — remote paths from batched tars are
  sanitized; path traversal from a compromised pod is rejected.

## Options

| Flag | Default | Meaning |
|---|---|---|
| `-n <ns>` | | namespace |
| `-c <container>` | | target container |
| `--parallel <N>` | 8 | concurrent transfers |
| `--chunk-mb <N>` | 64 | starting chunk size |
| `--max-chunk-mb <N>` | 1024 | chunk size ceiling |
| `--min-chunk-mb <N>` | 4 | chunk size floor |
| `--retries <N>` | 10 | attempts per operation |
| `--backoff <dur>` | 2s | first retry delay (doubles) |
| `--backoff-cap <dur>` | 60s | max retry delay |
| `--verify <mode>` | size | `size` / `md5` / `off` |

## Requirements

A kubeconfig; remote pods need `sh` plus standard utils (`dd`, `wc`,
`find`, `tar`); `md5sum` on both ends for `--verify md5`.

## Tests

```sh
cd kcp-go && go test
```

Covers argument parsing, chunk-writer bounds, and tar extraction
(including a path-traversal attempt that must be neutralized).
