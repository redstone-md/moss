package webui

import (
	"bytes"
	"compress/gzip"
	"io/fs"
	"net/http"
	"path"
	"strings"
	"sync"
)

// Serve the embedded bundle compressed.
//
// go:embed stores files verbatim, and Fly does not compress for you, so without
// this a first visit pulls the JavaScript bundle at its full size over whatever
// connection the reader has. Compressing on the fly per request would spend CPU
// on a shared VM that is also relaying for the mesh — so each file is compressed
// ONCE, on first request, and kept.
//
// That cache is safe to hold forever precisely because the files are embedded:
// they cannot change without a new binary, and a new binary starts with an empty
// cache. Bounded by the bundle, not by traffic.
type gzipCache struct {
	mu    sync.RWMutex
	files map[string][]byte
}

func newGzipCache() *gzipCache {
	return &gzipCache{files: make(map[string][]byte)}
}

// compressible reports whether squeezing this file is worth the CPU. Images,
// fonts and wasm are already compressed; running deflate over them costs time
// and gives back a few bytes at best.
func compressible(name string) bool {
	switch strings.ToLower(path.Ext(name)) {
	case ".js", ".css", ".html", ".json", ".svg", ".txt", ".map", ".xml":
		return true
	default:
		return false
	}
}

func (c *gzipCache) get(sub fs.FS, name string) []byte {
	c.mu.RLock()
	if b, ok := c.files[name]; ok {
		c.mu.RUnlock()
		return b
	}
	c.mu.RUnlock()

	raw, err := fs.ReadFile(sub, name)
	if err != nil {
		return nil
	}
	var buf bytes.Buffer
	zw, err := gzip.NewWriterLevel(&buf, gzip.BestCompression)
	if err != nil {
		return nil
	}
	if _, err := zw.Write(raw); err != nil || zw.Close() != nil {
		return nil
	}
	// Compression that does not pay for itself is worse than none: the browser
	// spends time inflating for nothing.
	if buf.Len() >= len(raw) {
		return nil
	}

	out := buf.Bytes()
	c.mu.Lock()
	c.files[name] = out
	c.mu.Unlock()
	return out
}

// gzipHandler wraps a file server, serving a compressed copy when the client
// accepts one and the file is worth compressing.
func gzipHandler(sub fs.FS, plain http.Handler) http.Handler {
	cache := newGzipCache()

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(r.URL.Path, "/")
		if name == "" || strings.HasSuffix(name, "/") {
			name += "scope.html"
		}
		if !compressible(name) || !acceptsGzip(r) {
			plain.ServeHTTP(w, r)
			return
		}
		body := cache.get(sub, name)
		if body == nil {
			plain.ServeHTTP(w, r)
			return
		}

		// Vary matters even behind a CDN that already knows: a cache that stores
		// the compressed body under a key shared with a client that cannot read
		// it serves garbage to that client.
		w.Header().Set("vary", "accept-encoding")
		w.Header().Set("content-encoding", "gzip")
		w.Header().Set("content-type", contentType(name))
		w.Header().Set("content-length", itoa(len(body)))
		if r.Method == http.MethodHead {
			return
		}
		_, _ = w.Write(body)
	})
}

func acceptsGzip(r *http.Request) bool {
	for _, part := range strings.Split(r.Header.Get("Accept-Encoding"), ",") {
		if strings.EqualFold(strings.TrimSpace(strings.Split(part, ";")[0]), "gzip") {
			return true
		}
	}
	return false
}

func contentType(name string) string {
	switch strings.ToLower(path.Ext(name)) {
	case ".js", ".map":
		return "text/javascript; charset=utf-8"
	case ".css":
		return "text/css; charset=utf-8"
	case ".html":
		return "text/html; charset=utf-8"
	case ".json":
		return "application/json; charset=utf-8"
	case ".svg":
		return "image/svg+xml"
	case ".xml":
		return "application/xml"
	default:
		return "text/plain; charset=utf-8"
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
