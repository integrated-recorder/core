package server

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/integrated-recorder/core/internal/applog"
)

// requestLogMiddleware records only bounded request metadata. It never stores
// the URL, query, headers, body, recording title, or adapter-provided values.
func requestLogMiddleware(store *applog.Store, next http.Handler) http.Handler {
	if store == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" || r.URL.Path == "/" || r.URL.Path == "/api/logs" || strings.HasPrefix(r.URL.Path, "/static/") {
			next.ServeHTTP(w, r)
			return
		}
		started := time.Now()
		wrapped := &requestStatusWriter{ResponseWriter: w}
		defer func() {
			status := wrapped.status
			if status == 0 {
				status = http.StatusOK
			}
			level := "info"
			if status >= http.StatusInternalServerError {
				level = "error"
			} else if status >= http.StatusBadRequest {
				level = "warn"
			}
			method := safeLogMethod(r.Method)
			store.Add(level, "http", fmt.Sprintf("%s completed with status %d after %dms", method, status, time.Since(started).Milliseconds()))
		}()
		next.ServeHTTP(wrapped, r)
	})
}

type requestStatusWriter struct {
	http.ResponseWriter
	status int
}

func (w *requestStatusWriter) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *requestStatusWriter) Write(payload []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(payload)
}

func (w *requestStatusWriter) Flush() {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (w *requestStatusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func safeLogMethod(method string) string {
	if len(method) == 0 || len(method) > 16 {
		return "OTHER"
	}
	for _, r := range method {
		if r < 'A' || r > 'Z' {
			return "OTHER"
		}
	}
	return method
}
