package server

import "strings"

// isSPARoute only admits routes that the embedded client application owns.
// Unknown paths must remain ordinary 404s instead of receiving index.html.
func isSPARoute(path string) bool {
	switch path {
	case "/", "/setup", "/login", "/recordings", "/new", "/adapters", "/workflows", "/settings", "/watches", "/watches/new", "/storage",
		"/sources", "/plugins", "/account/preferences", "/account/security", "/account/sessions",
		"/system", "/system/users", "/system/updates", "/system/diagnostics", "/system/audit",
		"/storage/locations", "/storage/retention", "/storage/advanced":
		return true
	}
	for _, prefix := range []string{"/recordings/", "/adapters/", "/sources/", "/workflows/", "/watches/", "/storage/"} {
		if !strings.HasPrefix(path, prefix) {
			continue
		}
		id := strings.TrimPrefix(path, prefix)
		return id != "" && id != "." && id != ".." && !strings.Contains(id, "/")
	}
	return false
}
