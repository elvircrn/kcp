#!/usr/bin/env bash
# test_kcp.sh — end-to-end test of kcp against the mock kubectl.
set -u
DIR=$(cd "$(dirname "$0")" && pwd)
KCP=$DIR/../kcp

WORK=$(mktemp -d "${TMPDIR:-/tmp}/kcp-test.XXXXXX")
trap 'rm -rf "$WORK"' EXIT

# fake remote filesystem
FS=$WORK/remote
mkdir -p "$FS/var/cache/vllm/profile/iter1" "$FS/var/cache/vllm/profile/iter2"
head -c $((70 * 1048576)) /dev/urandom > "$FS/var/cache/vllm/profile/big.pb"        # 70 MiB, chunked path
head -c $((5 * 1048576))  /dev/urandom > "$FS/var/cache/vllm/profile/iter1/model.ot" # 5 MiB, small path
echo "config text" > "$FS/var/cache/vllm/profile/iter1/config.yaml"
head -c $((6 * 1048576)) /dev/urandom > "$FS/var/cache/vllm/profile/iter2/tensors.bin"
printf 'x%.0s' $(seq 1 30) > "$FS/var/cache/vllm/profile/iter1/odd_name.txt" 2>/dev/null || true

export MOCK_FS=$FS
export KCP_RETRIES=6 KCP_BACKOFF_BASE=0 KCP_BACKOFF_CAP=0   # no sleeping in tests
export KCP_CHUNK_MB=4 KCP_MIN_CHUNK_MB=2                     # exercise chunk resizing
export KCP_VERIFY=size
unset KCP_WEBSOCKETS

# put mock kubectl first on PATH
mkdir -p "$WORK/bin"
ln -s "$DIR/mock-kubectl" "$WORK/bin/kubectl"
export MOCK_ORIG_PATH=$PATH
export PATH="$WORK/bin:$PATH"

fail() { echo "FAIL: $*"; exit 1; }
pass() { echo "PASS: $*"; }

run() {
  MOCK_FAIL_RATE=${MOCK_FAIL_RATE:-0} "$KCP" -n testns fake-pod:/var/cache/vllm/profile "$WORK/dst" "$@"
}

# --- test 1: happy path, no failures ---
rm -rf "$WORK/dst"; mkdir -p "$WORK/dst"
MOCK_FAIL_RATE=0 run >/dev/null || fail "baseline copy exited nonzero"
[ -f "$WORK/dst/profile/big.pb" ] || fail "big.pb missing"
[ -f "$WORK/dst/profile/iter1/model.ot" ] || fail "model.ot missing"
[ -f "$WORK/dst/profile/iter2/tensors.bin" ] || fail "tensors.bin missing"
diff -r "$FS/var/cache/vllm/profile" "$WORK/dst/profile" >/dev/null || fail "copied tree differs from source"
pass "baseline copy is byte-identical"

# --- test 2: rerun does no work — everything is already present ---
out=$(MOCK_FAIL_RATE=0 run 2>&1)
echo "$out" | grep -q "0 file(s) to copy" || fail "rerun did not skip existing files (output: $(echo "$out" | tail -3 | tr '\n' ' '))"
pass "rerun skips verified files"

# --- test 3: flaky connection, 40% of streams die mid-transfer ---
# A give-up after N consecutive failures is legitimate behavior at this
# rate; the promise is that rerunning completes the job. So: rerun until
# done (max 3), then demand byte-identical content.
rm -rf "$WORK/dst"; mkdir -p "$WORK/dst"
attempt=1
while [ "$attempt" -le 3 ]; do
  MOCK_FAIL_RATE=0.4 KCP_RETRIES=8 run >/dev/null 2>&1 && break
  attempt=$((attempt + 1))
done
diff -r "$FS/var/cache/vllm/profile" "$WORK/dst/profile" >/dev/null \
  || fail "flaky copy differs from source after $attempt run(s)"
pass "flaky copy (40% stream failure) completes byte-identical (run $attempt)"

# --- test 4: resume — interrupt mid-file, rerun, verify it continues ---
rm -rf "$WORK/dst"; mkdir -p "$WORK/dst/profile"
# plant a real 20MiB prefix of the source as the partial file
head -c $((20 * 1048576)) "$FS/var/cache/vllm/profile/big.pb" > "$WORK/dst/profile/big.pb.kcp.part"
MOCK_FAIL_RATE=0 run >/dev/null || fail "resume run exited nonzero"
diff -r "$FS/var/cache/vllm/profile" "$WORK/dst/profile" >/dev/null || fail "resumed copy differs"
pass "partial .part file is picked up and completed"

# --- test 5: verify=md5 end-to-end ---
rm -rf "$WORK/dst"; mkdir -p "$WORK/dst"
KCP_VERIFY=md5 run >/dev/null || fail "md5 verify run exited nonzero"
diff -r "$FS/var/cache/vllm/profile" "$WORK/dst/profile" >/dev/null || fail "md5 copy differs"
pass "md5 verification mode"

# --- test 6: single-file copy ---
rm -rf "$WORK/dst"
mkdir -p "$WORK/dst"
MOCK_FAIL_RATE=0 run --help >/dev/null 2>&1 || true
"$KCP" -n testns fake-pod:/var/cache/vllm/profile/iter1/config.yaml "$WORK/dst" >/dev/null \
  || fail "single-file copy failed"
[ -f "$WORK/dst/config.yaml" ] || fail "single-file copy did not land"
cmp -s "$FS/var/cache/vllm/profile/iter1/config.yaml" "$WORK/dst/config.yaml" || fail "single-file differs"
pass "single-file copy"

echo
echo "all tests passed"
