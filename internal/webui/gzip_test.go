package webui

import (
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// The bundle must go out compressed to clients that ask for it, and verbatim to
// those that do not. Getting the second half wrong is the worse failure: a
// client that cannot inflate receives bytes it will render as noise.
func TestBundleIsServedCompressedOnlyWhenAccepted(t *testing.T) {
	h := Handler()

	req := httptest.NewRequest(http.MethodGet, "/scope.html", nil)
	req.Header.Set("Accept-Encoding", "gzip, deflate, br")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if enc := rec.Header().Get("content-encoding"); enc != "gzip" && Available() {
		t.Fatalf("content-encoding = %q, ожидался gzip", enc)
	}
	if rec.Header().Get("content-encoding") == "gzip" {
		if v := rec.Header().Get("vary"); !strings.Contains(strings.ToLower(v), "accept-encoding") {
			t.Fatalf("vary = %q — без него кеш отдаст сжатое тело клиенту, который его не примет", v)
		}
		zr, err := gzip.NewReader(rec.Body)
		if err != nil {
			t.Fatalf("тело не разжимается: %v", err)
		}
		body, err := io.ReadAll(zr)
		if err != nil {
			t.Fatalf("тело повреждено: %v", err)
		}
		if !strings.Contains(string(body), "<") {
			t.Fatal("разжатое тело не похоже на HTML")
		}
	}

	plain := httptest.NewRecorder()
	h.ServeHTTP(plain, httptest.NewRequest(http.MethodGet, "/scope.html", nil))
	if enc := plain.Header().Get("content-encoding"); enc != "" {
		t.Fatalf("клиенту без Accept-Encoding отдано %q", enc)
	}
}

// Already-compressed formats must be left alone: re-compressing wasm or a png
// spends CPU on a shared machine and returns nothing.
func TestPrecompressedFormatsAreNotRecompressed(t *testing.T) {
	for _, name := range []string{"moss.wasm", "favicon.png", "font.woff2"} {
		if compressible(name) {
			t.Errorf("%s помечен как сжимаемый — это трата процессора впустую", name)
		}
	}
	for _, name := range []string{"scope.html", "assets/app.js", "assets/app.css", "icon.svg"} {
		if !compressible(name) {
			t.Errorf("%s не сжимается, хотя должен", name)
		}
	}
}
