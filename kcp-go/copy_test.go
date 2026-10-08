package main

import (
	"archive/tar"
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestParseSource(t *testing.T) {
	cases := []struct {
		in             string
		pod, ns, rpath string
		wantErr        bool
	}{
		{"pod:/path", "pod", "", "/path", false},
		{"ns/pod:/path", "pod", "ns", "/path", false},
		{"pod:/path/", "pod", "", "/path", false},
		{"pod", "pod", "", "", true}, // no path -> error
		{"", "", "", "", true},
	}
	for _, c := range cases {
		spec, rpath, err := parseSource(c.in)
		if c.wantErr {
			if err == nil {
				t.Errorf("%q: want error, got nil", c.in)
			}
			continue
		}
		if err != nil {
			t.Errorf("%q: %v", c.in, err)
			continue
		}
		if spec.pod != c.pod || spec.namespace != c.ns || rpath != c.rpath {
			t.Errorf("%q: got (%q,%q,%q) want (%q,%q,%q)", c.in, spec.pod, spec.namespace, rpath, c.pod, c.ns, c.rpath)
		}
	}
}

func TestHuman(t *testing.T) {
	cases := []struct {
		n    int64
		want string
	}{
		{0, "0B"}, {512, "512B"},
		{1024, "1.0KiB"}, {1048576, "1.0MiB"}, {1572864, "1.5MiB"},
		{1073741824, "1.0GiB"}, {1099511627776, "1.0TiB"},
	}
	for _, c := range cases {
		if got := human(c.n); got != c.want {
			t.Errorf("human(%d) = %q, want %q", c.n, got, c.want)
		}
	}
}

func TestExtractTar(t *testing.T) {
	// build a tarball with a dir, a file, and a path-traversal attempt
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	must := func(err error) {
		if err != nil {
			t.Fatal(err)
		}
	}
	must(tw.WriteHeader(&tar.Header{Name: "sub", Typeflag: tar.TypeDir}))
	must(tw.WriteHeader(&tar.Header{Name: "sub/a.txt", Typeflag: tar.TypeReg, Size: 3, Mode: 0o644}))
	_, err := tw.Write([]byte("abc"))
	must(err)
	must(tw.WriteHeader(&tar.Header{Name: "top.txt", Typeflag: tar.TypeReg, Size: 2, Mode: 0o644}))
	_, err = tw.Write([]byte("hi"))
	must(err)
	// traversal attempt must be ignored
	must(tw.WriteHeader(&tar.Header{Name: "../../etc/passwd", Typeflag: tar.TypeReg, Size: 1, Mode: 0o644}))
	_, err = tw.Write([]byte("x"))
	must(err)
	must(tw.Close())

	dir := t.TempDir()
	tarball := filepath.Join(dir, "b.tar")
	must(os.WriteFile(tarball, buf.Bytes(), 0o644))

	n := 0
	if err := extractTar(tarball, dir, &n); err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("extracted %d files, want 2 (traversal excluded)", n)
	}
	if b, _ := os.ReadFile(filepath.Join(dir, "sub/a.txt")); string(b) != "abc" {
		t.Error("sub/a.txt content wrong")
	}
	if b, _ := os.ReadFile(filepath.Join(dir, "top.txt")); string(b) != "hi" {
		t.Error("top.txt content wrong")
	}
	// traversal target must not exist outside dst
	if _, err := os.Stat(filepath.Join(dir, "..", "etc", "passwd")); err == nil {
		t.Error("path traversal was not neutralized")
	}
}

func TestCountingWriterAt(t *testing.T) {
	dir := t.TempDir()
	f, err := os.Create(filepath.Join(dir, "p"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	c := &countingWriterAt{f: f, off: 1024, want: 3}
	if _, err := c.Write([]byte("abc")); err != nil {
		t.Fatal(err)
	}
	if c.got != 3 {
		t.Fatalf("got %d, want 3", c.got)
	}
	if _, err := c.Write([]byte("d")); err == nil {
		t.Fatal("expected overlong error")
	}
	f.Seek(1024, 0)
	buf := make([]byte, 3)
	if _, err := f.Read(buf); err != nil || string(buf) != "abc" {
		t.Fatalf("read back %q err %v", buf, err)
	}
}
