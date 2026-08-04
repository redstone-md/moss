// Package webui embeds the built MossScope bundle so a single binary can serve
// the whole interface with no files on disk next to it.
//
// `dist` is filled by `make scope` (Vite output staged here) and is gitignored
// apart from a .gitkeep, because go:embed fails at COMPILE time on a missing
// directory: a contributor fixing a transport bug must be able to run
// `go build ./...` without installing Node. When the directory holds nothing,
// the handler serves the committed placeholder instead.
package webui

import (
	"embed"
	_ "embed"
	"io/fs"
	"net/http"
	"strings"
)

//go:embed all:dist
var dist embed.FS

//go:embed placeholder.html
var placeholder []byte

// Available reports whether a real bundle was compiled into this binary.
func Available() bool {
	_, err := fs.Stat(dist, "dist/scope.html")
	return err == nil
}

// Handler serves the bundle. Hashed asset filenames from Vite are immutable and
// cached for a year; entry documents are not cached at all, so a redeploy shows
// up on the next reload rather than after a cache expiry nobody can predict.
func Handler() http.Handler {
	if !Available() {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("content-type", "text/html; charset=utf-8")
			w.Header().Set("cache-control", "no-store")
			_, _ = w.Write(placeholder)
		})
	}

	sub, err := fs.Sub(dist, "dist")
	if err != nil {
		return http.NotFoundHandler()
	}
	files := http.FileServer(http.FS(sub))
	// Cache-control and the entry rewrite have to happen before compression
	// chooses a body, so the chain is: headers → gzip → file server.
	compressed := gzipHandler(sub, files)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/")
		if path == "" {
			// The scope IS the app here; the marketing landing lives on the site.
			r = cloneWithPath(r, "/scope.html")
			path = "scope.html"
		}
		if strings.HasPrefix(path, "assets/") {
			w.Header().Set("cache-control", "public, max-age=31536000, immutable")
		} else {
			w.Header().Set("cache-control", "no-cache")
		}
		// Serving the debugger means serving something that talks to a loopback
		// socket; keep it out of other people's frames.
		w.Header().Set("x-frame-options", "SAMEORIGIN")
		w.Header().Set("x-content-type-options", "nosniff")
		compressed.ServeHTTP(w, r)
	})
}

func cloneWithPath(r *http.Request, path string) *http.Request {
	c := r.Clone(r.Context())
	c.URL.Path = path
	return c
}
