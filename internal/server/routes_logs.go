package server

import (
	"net/http"
	"net/url"

	"github.com/integrated-recorder/core/internal/applog"
)

type logPageResponse struct {
	Items      []applog.Entry `json:"items"`
	NextCursor string         `json:"next_cursor"`
}

// NewLogHandler returns a bounded read-only handler for GET /api/logs. The
// handler queries only the supplied in-memory store; it never opens files.
func NewLogHandler(store *applog.Store) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		if r.URL.Path != "/api/logs" {
			http.NotFound(w, r)
			return
		}
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		if store == nil {
			writeError(w, http.StatusServiceUnavailable, "logs are unavailable")
			return
		}
		values, err := url.ParseQuery(r.URL.RawQuery)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid log query")
			return
		}
		query, err := applog.ParseQuery(values)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid log query")
			return
		}
		page, err := store.Query(query)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid log query")
			return
		}
		writeJSON(w, http.StatusOK, logPageResponse{Items: page.Items, NextCursor: page.NextCursor})
	})
}
