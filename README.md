# kcp — resilient `kubectl cp`

`kubectl cp` streams one tar over a single websocket/SPDY connection, so
large pod → local transfers regularly die with:

```
error: error reading from error stream: read message: websocket: close 1006
```

kcp is a drop-in replacement that survives flaky connections. Two
implementations, same design:

- **`kcp`** (bash) — zero dependencies beyond bash + kubectl. Start here.
- **`kcp-go/`** (Go, single static binary) — same behavior, ~100× lower
  per-operation overhead: one process, one authenticated connection pool,
  streams chunks straight to disk (no per-chunk buffering).

## Usage

```sh
~/kcp/kcp -n <namespace> <pod>:/remote/path /local/dest
~/kcp/kcp-go/kcp -n <namespace> <pod>:/remote/path /local/dest
```

e.g.

```sh
~/kcp/kcp -n elvircrn-kimi-k3 qwen3-30b-pd-disagg:/var/cache/vllm/profile \
  /Users/ecrncevi/new_profile_dump_v6
```

Build the Go binary: `cd kcp-go && go build -o kcp .`

## How they survive bad connections

- **Per-file streams** — one transfer per operation instead of one giant
  tar, so a dropped connection costs one file (or one chunk), not the
  whole transfer.
- **Small-file batching** — files ≤1MiB are fetched in a single
  tar-over-exec batch (≤200 files per batch) instead of one exec each;
  this is where most per-file latency went. Any batch failure falls back
  to per-file copy for its members.
- **Chunking with resume** — larger files are fetched in size-verified
  MiB-aligned chunks into `<dest>.kcp.part`. An interrupted run leaves
  the partial file; rerun the same command and it continues from where
  it stopped.
- **Dynamic chunk sizing** — after 4 clean chunks the size doubles (up to
  `--max-chunk-mb`); a failed chunk halves it (down to `--min-chunk-mb`).
  Good connections ramp up, struggling ones back off.
- **Retries with exponential backoff + jitter** — every remote operation
  is retried up to `--retries` times with capped, jittered backoff.
- **Parallel workers** — `--parallel N` concurrent transfers (default 8;
  Go: 8 goroutines over one connection pool). Large files are sorted
  biggest-first and dealt round-robin so long transfers start
  immediately.
- **Verification** — every file is size-checked; `--verify md5` also
  compares checksums on both ends. Verified files are skipped on rerun.
- **SPDY preferred** — both avoid the websocket exec path that produces
  `close 1006` (bash passes `--websockets=false` where supported; Go
  uses the SPDY executor directly). Set `KCP_WEBSOCKETS=1` (bash only)
  to keep kubectl's default.

## Options (both versions)

| Flag | Default | Meaning |
|---|---|---|
| `-n <ns>` | | namespace |
| `-c, --container <name>` | | target container |
| `--parallel <N>` | 8 | concurrent transfers |
| `--chunk-mb <N>` | 64 | starting chunk size |
| `--max-chunk-mb <N>` | 1024 | chunk size ceiling |
| `--min-chunk-mb <N>` | 4 | chunk size floor |
| `--retries <N>` | 10 | attempts per operation |
| `--backoff <secs>` | 2 | first retry delay (doubles) |
| `--backoff-cap <secs>` | 60 | max retry delay |
| `--verify <mode>` | size | `size` / `md5` / `off` |

Env equivalents (bash): `KCP_PARALLEL`, `KCP_CHUNK_MB`,
`KCP_MAX_CHUNK_MB`, `KCP_MIN_CHUNK_MB`, `KCP_RETRIES`,
`KCP_BACKOFF_BASE`, `KCP_BACKOFF_CAP`, `KCP_VERIFY`, `KCP_WEBSOCKETS`.

## Requirements

- bash kcp: bash, kubectl; remote pods need `sh` + standard utils
  (`dd`, `wc`, `find`, `tar`); `md5sum` on both ends for `--verify md5`.
- Go kcp: a kubeconfig; same pod-side utils. Build: `cd kcp-go && go build -o kcp .`

## Tests

```sh
test/test_kcp.sh      # bash: end-to-end against a flaky mock kubectl
cd kcp-go && go test  # Go: parse, chunk-writer, tar-extract (incl. traversal)
```

The bash mock kills a configurable fraction of streams mid-transfer and
asserts byte-identical results (baseline, 40% failure, resume, md5,
single-file).
