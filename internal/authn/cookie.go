package authn

import (
	"net/http"
	"time"
)

// SessionCookieName is the HttpOnly cookie used for authenticated requests.
const SessionCookieName = "ir_session"

// SetSessionCookie writes the session token as an HttpOnly, strict-same-site
// cookie. Secure is enabled for TLS requests or when the deployment is known
// to terminate TLS at a trusted reverse proxy.
func SetSessionCookie(w http.ResponseWriter, r *http.Request, session Session, forceSecure bool) {
	SetSessionCookieSecure(w, r, session, (r != nil && r.TLS != nil) || forceSecure)
}

// SetSessionCookieSecure writes the session cookie with an explicit transport
// policy. It is used only when the server deliberately transitions an
// authenticated browser between its HTTPS and HTTP endpoints.
func SetSessionCookieSecure(w http.ResponseWriter, _ *http.Request, session Session, secure bool) {
	remaining := time.Until(session.ExpiresAt)
	maxAge := int(remaining / time.Second)
	if remaining > 0 && remaining%time.Second != 0 {
		maxAge++
	}
	if maxAge < 1 {
		maxAge = 1
	}
	http.SetCookie(w, &http.Cookie{
		Name:     SessionCookieName,
		Value:    session.Token,
		Path:     "/",
		Expires:  session.ExpiresAt,
		MaxAge:   maxAge,
		HttpOnly: true,
		Secure:   secure,
		SameSite: http.SameSiteStrictMode,
	})
}

// ClearSessionCookie expires the session cookie using the same scope and
// security attributes as SetSessionCookie.
func ClearSessionCookie(w http.ResponseWriter, r *http.Request, forceSecure bool) {
	http.SetCookie(w, &http.Cookie{
		Name:     SessionCookieName,
		Value:    "",
		Path:     "/",
		Expires:  time.Unix(1, 0).UTC(),
		MaxAge:   -1,
		HttpOnly: true,
		Secure:   (r != nil && r.TLS != nil) || forceSecure,
		SameSite: http.SameSiteStrictMode,
	})
}

// SessionToken extracts the session cookie value. It returns an empty string
// when the request has no valid cookie.
func SessionToken(r *http.Request) string {
	if r == nil {
		return ""
	}
	cookie, err := r.Cookie(SessionCookieName)
	if err != nil {
		return ""
	}
	return cookie.Value
}
