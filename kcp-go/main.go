// kcp — resilient, parallel kubectl-cp replacement (Go).
//
// Per-file streams with chunking, size-verified chunks, dynamic chunk
// sizing, exponential backoff, resume, and parallel workers — the same
// design as the bash kcp, in one static binary using client-go's exec
// transport directly (no kubectl process per operation).
package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/remotecommand"
)

const MiB = 1024 * 1024

// ---------- options ----------
type opts struct {
	namespace  string
	container  string
	parallel   int
	chunkMB    int
	maxChunkMB int
	minChunkMB int
	retries    int
	backoff    time.Duration
	cap        time.Duration
	verify     string
}

var o opts

func main() {
	flag.StringVar(&o.namespace, "n", "", "namespace")
	flag.StringVar(&o.container, "c", "", "container")
	flag.IntVar(&o.parallel, "parallel", 8, "concurrent file transfers")
	flag.IntVar(&o.chunkMB, "chunk-mb", 64, "starting chunk size (MiB)")
	flag.IntVar(&o.maxChunkMB, "max-chunk-mb", 1024, "chunk size ceiling (MiB)")
	flag.IntVar(&o.minChunkMB, "min-chunk-mb", 4, "chunk size floor (MiB)")
	flag.IntVar(&o.retries, "retries", 10, "max attempts per operation")
	flag.DurationVar(&o.backoff, "backoff", 2*time.Second, "first retry delay")
	flag.DurationVar(&o.cap, "backoff-cap", 60*time.Second, "max retry delay")
	flag.StringVar(&o.verify, "verify", "size", "size | md5 | off")
	flag.Parse()

	args := flag.Args()
	if len(args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: kcp [options] <pod>[:<remote-path>] <local-dest>")
		flag.Usage()
		os.Exit(2)
	}

	// validate
	switch o.verify {
	case "size", "md5", "off":
	default:
		fatal("--verify must be size, md5 or off")
	}
	if o.minChunkMB > o.chunkMB || o.chunkMB > o.maxChunkMB {
		fatal("require min-chunk-mb <= chunk-mb <= max-chunk-mb")
	}

	podSpec, rpath, err := parseSource(args[0])
	if err != nil {
		fatal(err)
	}
	if o.namespace == "" {
		o.namespace = podSpec.namespace
	}
	dst := args[1]

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		clientcmd.NewDefaultClientConfigLoadingRules(), nil).ClientConfig()
	if err != nil {
		fatal("kubeconfig: ", err)
	}
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		fatal("client: ", err)
	}

	r := &runner{client: client, cfg: cfg, pod: podSpec.pod}
	start := time.Now()

	if err := r.Run(ctx, rpath, dst); err != nil {
		fmt.Fprintf(os.Stderr, "kcp: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("kcp: summary: %d copied, %d skipped, %d failed, %d retries, %s\n",
		r.done.Load(), r.skip.Load(),
		r.fail.Load(), r.retries.Load(),
		time.Since(start).Round(time.Millisecond))
	if n := len(r.failedList()); n > 0 {
		fmt.Fprintln(os.Stderr, "kcp: failed files (rerun to retry):")
		for _, f := range r.failedList() {
			fmt.Fprintf(os.Stderr, "kcp:   %s\n", f)
		}
		os.Exit(1)
	}
}

func fatal(v ...any) {
	fmt.Fprintln(os.Stderr, append([]any{"kcp: "}, v...)...)
	os.Exit(2)
}

// parseSource splits "pod", "pod:/path" or "ns/pod:/path".
func parseSource(s string) (spec struct{ pod, namespace string }, rpath string, err error) {
	pod := s
	if i := strings.Index(s, ":"); i >= 0 {
		pod, rpath = s[:i], s[i+1:]
	}
	rpath = strings.TrimRight(rpath, "/")
	if rpath == "" {
		return spec, "", errors.New("please give an explicit remote path: pod:/path")
	}
	if i := strings.Index(pod, "/"); i >= 0 {
		spec.namespace, spec.pod = pod[:i], pod[i+1:]
	} else {
		spec.pod = pod
	}
	if spec.pod == "" {
		return spec, "", errors.New("could not parse pod from: " + s)
	}
	return spec, rpath, nil
}

// ---------- remote file entry ----------
type entry struct {
	size int64
	rel  string
}

// ---------- runner ----------
type runner struct {
	client *kubernetes.Clientset
	cfg    *rest.Config
	pod    string

	done    atomic.Int64
	skip    atomic.Int64
	fail    atomic.Int64
	retries atomic.Int64

	failedMu sync.Mutex
	failed   []string
}

func (r *runner) noteFailed(rel string) {
	r.failedMu.Lock()
	r.failed = append(r.failed, rel)
	r.failedMu.Unlock()
}

func (r *runner) failedList() []string {
	r.failedMu.Lock()
	defer r.failedMu.Unlock()
	return append([]string(nil), r.failed...)
}

// exec runs a command in the pod, capturing stdout to a writer. err
// carries stderr text for diagnostics.
func (r *runner) exec(ctx context.Context, args ...string) ([]byte, error) {
	var buf bytesBuf
	err := r.execTo(ctx, &buf.b, args...)
	return buf.b.Bytes(), err
}

// execTo streams the command's stdout into w (no full buffering unless w
// itself buffers) — used for chunk fetches to keep memory flat.
func (r *runner) execTo(ctx context.Context, w io.Writer, args ...string) error {
	req := r.client.CoreV1().RESTClient().Post().
		Resource("pods").Namespace(o.namespace).Name(r.pod).
		SubResource("exec").
		Param("container", o.container).
		Param("stdout", "true").Param("stderr", "true").
		Param("command", "sh").Param("command", "-c").Param("command", args[0]).
		Param("command", "kcp")
	for _, a := range args[1:] {
		req.Param("command", a)
	}

	exec, err := remotecommand.NewSPDYExecutor(r.cfg, "POST", req.URL())
	if err != nil {
		return err
	}
	var stderr bytesBuf
	err = exec.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdout: w, Stderr: &stderr,
	})
	if err != nil {
		return fmt.Errorf("%v: %s", err, strings.TrimSpace(stderr.String()))
	}
	return nil
}

type bytesBuf struct {
	b bytes.Buffer
}

func (w *bytesBuf) Write(p []byte) (int, error) { return w.b.Write(p) }

func (w *bytesBuf) String() string { return w.b.String() }

// withRetry retries fn until success or o.retries attempts, with capped
// exponential backoff + jitter. ctx cancellation aborts.
func (r *runner) withRetry(ctx context.Context, desc string, fn func() error) error {
	var last error
	for n := 1; n <= o.retries; n++ {
		if err := fn(); err == nil {
			return nil
		} else {
			last = err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if n == o.retries {
			break
		}
		r.retries.Add(1)
		fmt.Fprintf(os.Stderr, "kcp: warn: %s failed (attempt %d/%d): %v\n", desc, n, o.retries, last)
		if serr := r.sleepBackoff(ctx, n); serr != nil {
			return serr
		}
	}
	return fmt.Errorf("giving up: %s (after %d attempts): %w", desc, o.retries, last)
}

// execRetry = withRetry around a single exec.
func (r *runner) execRetry(ctx context.Context, desc, script string, args ...string) ([]byte, error) {
	var out []byte
	err := r.withRetry(ctx, desc, func() error {
		var err error
		out, err = r.exec(ctx, append([]string{script}, args...)...)
		return err
	})
	return out, err
}

// sleepBackoff sleeps min(cap, backoff * 2^(n-1)) + jitter.
func (r *runner) sleepBackoff(ctx context.Context, n int) error {
	d := o.backoff
	for i := 1; i < n && d < o.cap; i++ {
		d *= 2
	}
	if d > o.cap {
		d = o.cap
	}
	d += time.Duration(rand.Int63n(int64(300 * time.Millisecond))) // jitter
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}
