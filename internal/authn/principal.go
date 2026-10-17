package authn

import (
	"context"
	"encoding/json"
	"net/http"
)

// Principal is reconstructed from the latest durable account and role state
// for every authenticated request. Role is a deprecated display aid only;
// authorization uses the explicit permission union.
type Principal struct {
	UserID      string
	Login       string
	Roles       []string
	Permissions []Permission
	Role        string
}

type principalContextKey struct{}

func WithPrincipal(ctx context.Context, principal Principal) context.Context {
	return context.WithValue(ctx, principalContextKey{}, principal)
}

func PrincipalFromContext(ctx context.Context) (Principal, bool) {
	principal, ok := ctx.Value(principalContextKey{}).(Principal)
	if !ok || !validUserID(principal.UserID) || principal.Login == "" || len(principal.Permissions) == 0 {
		return Principal{}, false
	}
	return principal, true
}

func HasPermission(principal Principal, permission Permission) bool {
	if !validUserID(principal.UserID) || len(principal.Permissions) == 0 {
		return false
	}
	switch permission {
	case PermissionAccountProfileSelf, PermissionAccountPasswordSelf, PermissionAccountSessionsSelf, PermissionUserPreferences:
		return true
	}
	for _, candidate := range principal.Permissions {
		if candidate == permission {
			return true
		}
	}
	return false
}

func HasRole(principal Principal, roleID string) bool {
	return validUserID(principal.UserID) && hasRole(principal.Roles, roleID)
}

// RequirePermission is shared by product and Runtime Host APIs. Unknown
// permission identifiers always fail closed.
func RequirePermission(permission Permission, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, ok := PrincipalFromContext(r.Context())
		if !ok {
			http.Error(w, "authentication required", http.StatusUnauthorized)
			return
		}
		if !HasPermission(principal, permission) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			_ = json.NewEncoder(w).Encode(struct {
				ErrorCode string `json:"error_code"`
				Message   string `json:"message"`
			}{ErrorCode: "permission_denied", Message: "Permission denied."})
			return
		}
		next.ServeHTTP(w, r)
	})
}
