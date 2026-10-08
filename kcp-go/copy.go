package main

import (
	"archive/tar"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Run orchestrates one remote path -> local dest.
func (r *runner) Run(ctx context.Context, rpath, dst string) error {
	kind, size, err := r.probe(ctx, rpath)
	if err != nil {
		return err
	}

	var target string
	switch kind {
	case "dir":
		if st, err := os.Stat(dst); err == nil && st.IsDir() {
			target = filepath.Join(dst, filepath.Base(rpath))
		} else {
			target = dst
		}
		if err := os.MkdirAll(target, 0o755); err != nil {
			return err
		}
	case "file":
		if st, err := os.Stat(dst); err == nil && st.IsDir() {
			target = filepath.Join(dst, filepath.Base(rpath))
		} else {
			target = dst
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
	default:
		return errors.New("remote path not found: " + rpath)
	}
	fmt.Printf("kcp: pod %s:%s -> %s\n", r.pod, rpath, target)

	if kind == "file" {
		return r.copyOne(ctx, rpath, target, size)
	}

	entries, err := r.list(ctx, rpath)
	if err != nil {
		return err
	}

	// filter out files already present at the right size (cheap resume)
	{
		var pending []entry
		for _, e := range entries {
			if st, err := os.Stat(filepath.Join(target, e.rel)); err != nil || st.Size() != e.size {
				pending = append(pending, e)
			}
		}
		entries = pending
	}

	// small files travel as one tar-over-exec batch
	const batchLimitBytes = MiB
	const batchMaxFiles = 200
	var large []entry
	var small []entry
	for _, e := range entries {
		if e.size <= batchLimitBytes {
			small = append(small, e)
		} else {
			large = append(large, e)
		}
	}
	if len(small) > 0 {
		fmt.Printf("kcp: %d small file(s) — batching\n", len(small))
		for i := 0; i < len(small); i += batchMaxFiles {
			end := i + batchMaxFiles
			if end > len(small) {
				end = len(small)
			}
			if err := r.copyBatch(ctx, rpath, target, small[i:end]); err != nil {
				fmt.Fprintf(os.Stderr, "kcp: %v\n", err)
			}
		}
	}
	entries = large

	// largest first: long transfers start immediately, small files fill gaps
	sort.Slice(entries, func(i, j int) bool { return entries[i].size > entries[j].size })

	n := min(o.parallel, len(entries))
	fmt.Printf("kcp: %d large file(s), %d worker(s)\n", len(entries), n)

	var failed atomic.Bool
	buckets := make([][]entry, n)
	for i, e := range entries {
		buckets[i%n] = append(buckets[i%n], e) // round-robin on sorted list
	}

	var wg sync.WaitGroup
	for _, b := range buckets {
		wg.Add(1)
		go func(b []entry) {
			defer wg.Done()
			for _, e := range b {
				if ctx.Err() != nil {
					return
				}
				if err := r.copyOne(ctx, rpath+"/"+e.rel, filepath.Join(target, e.rel), e.size); err != nil {
					if !errors.Is(err, context.Canceled) {
						fmt.Fprintf(os.Stderr, "kcp: %v\n", err)
						r.noteFailed(e.rel)
						r.fail.Add(1)
					}
					failed.Store(true)
				}
			}
		}(b)
	}
	wg.Wait()

	if ctx.Err() != nil {
		return errors.New("interrupted — rerun the same command to resume")
	}
	if failed.Load() {
		return errors.New("some files failed (rerun to retry)")
	}
	return nil
}

// copyBatch tars a set of small files pod-side into one exec stream,
// extracts locally, verifies sizes; falls back to per-file on any trouble.
func (r *runner) copyBatch(ctx context.Context, rpath, target string, entries []entry) error {
	rels := make([]string, len(entries))
	for i, e := range entries {
		rels[i] = e.rel
	}

	tarball := filepath.Join(os.TempDir(), fmt.Sprintf("kcp-batch-%d.tar", time.Now().UnixNano()))
	err := r.withRetry(ctx, fmt.Sprintf("tar %d file(s)", len(entries)), func() error {
		// script: cd $1; tar -cf - "$2" "$3" ... — argv-borne rel paths
		script := `cd "$1" || exit 9; shift; tar -cf - "$@"`
		full := append([]string{script, rpath}, rels...)
		f, err := os.Create(tarball)
		if err != nil {
			return err
		}
		err = r.execTo(ctx, f, full...)
		cerr := f.Close()
		if err != nil {
			os.Remove(tarball)
			return err
		}
		if cerr != nil {
			os.Remove(tarball)
			return cerr
		}
		return nil
	})
	if err != nil {
		// fall back to per-file for this batch
		fmt.Fprintf(os.Stderr, "kcp: batch tar failed — falling back to per-file: %v\n", err)
		return r.copyEntriesPerFile(ctx, rpath, target, entries)
	}

	// extract and verify each member
	extracted := 0
	if err := extractTar(tarball, target, &extracted); err != nil {
		os.Remove(tarball)
		return r.copyEntriesPerFile(ctx, rpath, target, entries)
	}
	os.Remove(tarball)
	if extracted != len(entries) {
		fmt.Fprintf(os.Stderr, "kcp: batch extract got %d of %d files — recopying missing ones\n", extracted, len(entries))
		return r.copyEntriesPerFile(ctx, rpath, target, entries)
	}
	for _, e := range entries {
		if st, err := os.Stat(filepath.Join(target, e.rel)); err != nil || st.Size() != e.size {
			fmt.Fprintf(os.Stderr, "kcp: batched file bad after extract: %s — recopying\n", e.rel)
			return r.copyEntriesPerFile(ctx, rpath, target, entries)
		}
		r.done.Add(1)
	}
	return nil
}

// copyEntriesPerFile copies entries one at a time (batch fallback).
func (r *runner) copyEntriesPerFile(ctx context.Context, rpath, target string, entries []entry) error {
	var firstErr error
	for _, e := range entries {
		if err := r.copyOne(ctx, rpath+"/"+e.rel, filepath.Join(target, e.rel), e.size); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			fmt.Fprintf(os.Stderr, "kcp: %v\n", err)
		}
	}
	return firstErr
}

// extractTar unpacks a tarball into dst, counting members into n.
func extractTar(tarball, dst string, n *int) error {
	f, err := os.Open(tarball)
	if err != nil {
		return err
	}
	defer f.Close()
	tr := tar.NewReader(f)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		name := filepath.Clean(hdr.Name)
		if strings.HasPrefix(name, "..") || filepath.IsAbs(name) {
			continue // never trust remote paths
		}
		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(filepath.Join(dst, name), 0o755); err != nil {
				return err
			}
		case tar.TypeReg:
			out := filepath.Join(dst, name)
			if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
				return err
			}
			of, err := os.OpenFile(out, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
			if err != nil {
				return err
			}
			if _, err := io.Copy(of, tr); err != nil {
				of.Close()
				return err
			}
			of.Close()
			*n++
		}
	}
}

// probe returns ("dir", 0, nil), ("file", size, nil) or an error.
func (r *runner) probe(ctx context.Context, rpath string) (string, int64, error) {
	out, err := r.execRetry(ctx, "probe "+rpath,
		`p="$1"; if [ -d "$p" ]; then echo dir; elif [ -f "$p" ]; then printf 'file %s\n' "$(wc -c < "$p" | tr -d ' ')"; else echo missing; fi`, rpath)
	if err != nil {
		return "", 0, err
	}
	switch s := strings.TrimSpace(string(out)); {
	case strings.HasPrefix(s, "dir"):
		return "dir", 0, nil
	case strings.HasPrefix(s, "file "):
		var size int64
		fmt.Sscanf(s[5:], "%d", &size)
		return "file", size, nil
	default:
		return "missing", 0, errors.New("remote path not found: " + rpath)
	}
}

// list returns all files under rpath with sizes, relative paths.
func (r *runner) list(ctx context.Context, rpath string) ([]entry, error) {
	out, err := r.execRetry(ctx, "list "+rpath,
		`cd "$1" || exit 9; find . -type f | while IFS= read -r f; do printf '%s\t%s\n' "$(wc -c < "$f" | tr -d ' ')" "${f#./}"; done`, rpath)
	if err != nil {
		return nil, err
	}
	var entries []entry
	for _, line := range strings.Split(string(out), "\n") {
		if line == "" {
			continue
		}
		tab := strings.IndexByte(line, '\t')
		if tab < 0 {
			continue
		}
		var size int64
		fmt.Sscanf(line[:tab], "%d", &size)
		entries = append(entries, entry{size: size, rel: line[tab+1:]})
	}
	return entries, nil
}

// copyOne handles skip-check, small-path, chunked path and verification for a single file.
func (r *runner) copyOne(ctx context.Context, rpath, lpath string, size int64) error {
	fmt.Printf("kcp: %s (%s)\n", filepath.Base(lpath), human(size))

	// skip if already present and verified
	if st, err := os.Stat(lpath); err == nil && st.Size() == size {
		if o.verify != "md5" {
			r.skip.Add(1)
			return nil
		}
		ok, err := r.md5Matches(ctx, rpath, lpath)
		if err == nil && ok {
			r.skip.Add(1)
			return nil
		}
	}

	if err := os.MkdirAll(filepath.Dir(lpath), 0o755); err != nil {
		return err
	}

	if size <= int64(o.chunkMB)*MiB {
		err := r.copySmall(ctx, rpath, lpath, size)
		if err == nil {
			r.done.Add(1)
			return nil
		}
		return err
	}

	if err := r.copyChunked(ctx, rpath, lpath, size); err != nil {
		return err
	}
	r.done.Add(1)
	return r.verifyFile(ctx, rpath, lpath, size)
}

func (r *runner) copySmall(ctx context.Context, rpath, lpath string, size int64) error {
	err := r.withRetry(ctx, "copy "+filepath.Base(lpath), func() error {
		out, err := r.exec(ctx, `cat "$1"`, rpath)
		if err != nil {
			return err
		}
		if int64(len(out)) != size {
			return fmt.Errorf("short read: got %d of %d bytes", len(out), size)
		}
		return writeAtomic(lpath, out)
	})
	if err == nil {
		return r.verifyFile(ctx, rpath, lpath, size)
	}
	return err
}

// copyChunked fetches a large file in MiB-aligned chunks into lpath+".kcp.part",
// resuming if the partial file already exists, then renames on completion.
func (r *runner) copyChunked(ctx context.Context, rpath, lpath string, size int64) error {
	part := lpath + ".kcp.part"
	var offset int64
	chunkMB := o.chunkMB
	consecFail, okStreak := 0, 0

	if st, err := os.Stat(part); err == nil {
		offset = st.Size()
		if offset > size || offset%MiB != 0 {
			fmt.Fprintf(os.Stderr, "kcp: discarding unusable partial %s\n", part)
			os.Remove(part)
			offset = 0
		} else if offset == size {
			return os.Rename(part, lpath)
		} else {
			fmt.Printf("kcp: resuming %s at %s / %s\n", filepath.Base(lpath), human(offset), human(size))
		}
	}

	f, err := os.OpenFile(part, os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	for offset < size {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		remaining := size - offset
		mbLen := chunkMB
		if int64(mbLen)*MiB > remaining {
			mbLen = int((remaining + MiB - 1) / MiB)
		}
		expected := int64(mbLen) * MiB
		if expected > remaining {
			expected = remaining
		}

		chunkErr := r.fetchChunk(ctx, rpath, int64(offset/MiB), int64(mbLen), expected, f, offset)
		if chunkErr == nil {
			offset += expected
			okStreak++
			consecFail = 0
			if okStreak >= 4 && chunkMB < o.maxChunkMB {
				chunkMB *= 2
				if chunkMB > o.maxChunkMB {
					chunkMB = o.maxChunkMB
				}
				okStreak = 0
				fmt.Printf("kcp: %s: connection stable — chunk size raised to %dMiB\n", filepath.Base(lpath), chunkMB)
			}
			fmt.Printf("kcp:   %s: %s / %s (%d%%)\n", filepath.Base(lpath), human(offset), human(size), offset*100/size)
		} else {
			consecFail++
			okStreak = 0
			if consecFail >= o.retries {
				return fmt.Errorf("giving up on %s after %d consecutive chunk failures; partial kept for resume", rpath, consecFail)
			}
			r.retries.Add(1)
			if chunkMB > o.minChunkMB {
				chunkMB /= 2
				if chunkMB < o.minChunkMB {
					chunkMB = o.minChunkMB
				}
				fmt.Fprintf(os.Stderr, "kcp: %s: connection struggling — chunk size lowered to %dMiB (%v)\n", filepath.Base(lpath), chunkMB, chunkErr)
			}
			if serr := r.sleepBackoff(ctx, consecFail); serr != nil {
				return serr
			}
		}
	}
	return os.Rename(part, lpath)
}

// fetchChunk streams one MiB-aligned slice via dd directly into f at
// offset, verifying the byte count as it lands (no full-chunk buffering).
func (r *runner) fetchChunk(ctx context.Context, rpath string, mbOff, mbLen, expected int64, f *os.File, offset int64) error {
	cw := &countingWriterAt{f: f, off: offset, want: expected}
	err := r.execTo(ctx, cw,
		fmt.Sprintf(`dd if="$1" bs=1M skip=%d count=%d 2>/dev/null`, mbOff, mbLen), rpath)
	if err != nil {
		return err
	}
	if cw.got != expected {
		return fmt.Errorf("chunk short: got %d of %d bytes", cw.got, expected)
	}
	return nil
}

// countingWriterAt wraps WriteAt on an *os.File, counting bytes written.
type countingWriterAt struct {
	f    *os.File
	off  int64
	want int64
	got  int64
}

func (c *countingWriterAt) Write(p []byte) (int, error) {
	n, err := c.f.WriteAt(p, c.off+c.got)
	c.got += int64(n)
	if err != nil {
		return n, err
	}
	if c.got > c.want {
		return n, fmt.Errorf("chunk overlong: %d > %d bytes", c.got, c.want)
	}
	return n, nil
}

func (r *runner) verifyFile(ctx context.Context, rpath, lpath string, size int64) error {
	st, err := os.Stat(lpath)
	if err != nil {
		return err
	}
	if st.Size() != size {
		return fmt.Errorf("size mismatch: %s is %d bytes, expected %d", lpath, st.Size(), size)
	}
	if o.verify == "md5" {
		ok, err := r.md5Matches(ctx, rpath, lpath)
		if err == nil && !ok {
			return errors.New("md5 mismatch on " + lpath + " — will recopy on next run")
		}
	}
	return nil
}

func (r *runner) md5Matches(ctx context.Context, rpath, lpath string) (bool, error) {
	out, err := r.execRetry(ctx, "md5sum "+rpath, `md5sum "$1" | awk '{print $1}'`, rpath)
	if err != nil {
		return false, err
	}
	remote := strings.TrimSpace(string(out))
	if remote == "" {
		return false, errors.New("empty md5 reply")
	}
	local, err := localMD5(lpath)
	if err != nil {
		return false, err
	}
	return remote == local, nil
}

func localMD5(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := md5.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// ---------- shared helpers ----------

func writeAtomic(lpath string, data []byte) error {
	tmp := lpath + ".kcp.tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, lpath)
}

func human(n int64) string {
	const u = "KMGT"
	f := float64(n)
	for i := 0; i < 4; i++ {
		if f < 1024 {
			if i == 0 {
				return fmt.Sprintf("%dB", int64(f))
			}
			return fmt.Sprintf("%.1f%ciB", f, u[i-1])
		}
		f /= 1024
	}
	return fmt.Sprintf("%.1fTiB", f)
}

// execRetry / withRetry / sleepBackoff live in main.go.
