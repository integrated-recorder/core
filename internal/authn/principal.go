package authn

import (
	"context"
	"net/http"
)

type Permission string

const (
	PermissionRecordingRead    Permission = "recording.read"
	PermissionRecordingControl Permission = "recording.control"
	PermissionRecordingDelete  Permission = "recording.delete"
	PermissionStorageManage    Permission = "storage.manage"
	PermissionPluginManage     Permission = "plugin.manage"
	PermissionSettingsManage   Permission = "settings.manage"
	PermissionAuditRead        Permission = "audit.read"
	PermissionUpdateManage     Permission = "update.manage"
)

// Principal binds request identity to server-validated session state.
// No request header can create or replace this value.
type Principal struct {
	UserID string
	Login  string
	Role   string
}

type principalContextKey struct{}

func WithPrincipal(ctx context.Context, principal Principal) context.Context {
	return context.WithValue(ctx, principalContextKey{}, principal)
}

func PrincipalFromContext(ctx context.Context) (Principal, bool) {
	principal, ok := ctx.Value(principalContextKey{}).(Principal)
	if !ok || !validUserID(principal.UserID) || principal.Login == "" || principal.Role == "" {
		return Principal{}, false
	}
	return principal, true
}

func HasPermission(principal Principal, permission Permission) bool {
	if !validUserID(principal.UserID) || principal.Role != RoleOwner {
		return false
	}
	switch permission {
	case PermissionRecordingRead, PermissionRecordingControl, PermissionRecordingDelete,
		PermissionStorageManage, PermissionPluginManage, PermissionSettingsManage,
		PermissionAuditRead, PermissionUpdateManage:
		return true
	default:
		return false
	}
}

// RequirePermission is the shared authorization middleware seam. Current
// owner role grants every declared control-plane permission.
func RequirePermission(permission Permission, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, ok := PrincipalFromContext(r.Context())
		if !ok {
			http.Error(w, "authentication required", http.StatusUnauthorized)
			return
		}
		if !HasPermission(principal, permission) {
			http.Error(w, "permission denied", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}
