package server

import (
	"net/http"
	"strings"

	"github.com/integrated-recorder/core/internal/authn"
)

type routePermissionRule struct {
	method     string
	pattern    string
	permission authn.Permission
}

// protectedRoutePermissions is the authoritative product API authorization
// table. New ServeMux API registrations are checked against this table by a
// source-level test; unmatched method/path pairs fail closed.
var protectedRoutePermissions = []routePermissionRule{
	{http.MethodPost, "/api/auth/logout", authn.PermissionAccountSessionsSelf},
	{http.MethodGet, "/api/user", authn.PermissionAccountProfileSelf},
	{http.MethodPatch, "/api/user", authn.PermissionAccountProfileSelf},
	{http.MethodGet, "/api/user/authorization", authn.PermissionAccountProfileSelf},
	{http.MethodPost, "/api/user/password", authn.PermissionAccountPasswordSelf},
	{http.MethodGet, "/api/user/preferences", authn.PermissionUserPreferences},
	{http.MethodPut, "/api/user/preferences", authn.PermissionUserPreferences},
	{http.MethodGet, "/api/user/sessions", authn.PermissionAccountSessionsSelf},
	{http.MethodDelete, "/api/user/sessions/{sessionID}", authn.PermissionAccountSessionsSelf},
	{http.MethodPost, "/api/user/sessions/revoke-others", authn.PermissionAccountSessionsSelf},
	{http.MethodGet, "/api/users", authn.PermissionUserRead},
	{http.MethodPost, "/api/users", authn.PermissionUserCreate},
	{http.MethodPatch, "/api/users/{userID}/roles", authn.PermissionUserManage},
	{http.MethodPost, "/api/users/{userID}/disable", authn.PermissionUserManage},
	{http.MethodPost, "/api/users/{userID}/enable", authn.PermissionUserManage},
	{http.MethodDelete, "/api/users/{userID}", authn.PermissionUserManage},
	{http.MethodGet, "/api/roles", authn.PermissionRoleRead},
	{http.MethodPost, "/api/roles", authn.PermissionRoleManage},
	{http.MethodPut, "/api/roles/{roleID}", authn.PermissionRoleManage},
	{http.MethodPatch, "/api/roles/{roleID}", authn.PermissionRoleManage},
	{http.MethodDelete, "/api/roles/{roleID}", authn.PermissionRoleManage},
	{http.MethodPost, "/api/setup/storage-test", authn.PermissionSystemSettings},
	{http.MethodPost, "/api/recordings", authn.PermissionRecordingCreate},
	{http.MethodGet, "/api/recordings", authn.PermissionRecordingRead},
	{http.MethodGet, "/api/recordings/{id}", authn.PermissionRecordingRead},
	{http.MethodDelete, "/api/recordings/{id}", authn.PermissionRecordingDelete},
	{http.MethodGet, "/api/recordings/{id}/diagnostics/storage", authn.PermissionRecordingRead},
	{http.MethodPost, "/api/recordings/{id}/stop", authn.PermissionRecordingStop},
	{http.MethodPost, "/api/recordings/{id}/complete", authn.PermissionRecordingStop},
	{http.MethodPost, "/api/recordings/{id}/seal", authn.PermissionRecordingStop},
	{http.MethodGet, "/api/recordings/{id}/lifecycle", authn.PermissionRecordingRead},
	{http.MethodGet, "/api/recordings/{id}/acquisition-flow", authn.PermissionRecordingRead},
	{http.MethodGet, "/api/recordings/{id}/play/master.m3u8", authn.PermissionRecordingRead},
	{http.MethodGet, "/api/recordings/{id}/play/tracks/{track}/playlist.m3u8", authn.PermissionRecordingRead},
	{http.MethodGet, "/api/recordings/{id}/play/live/master.m3u8", authn.PermissionRecordingRead},
	{http.MethodGet, "/api/recordings/{id}/play/live/tracks/{track}/playlist.m3u8", authn.PermissionRecordingRead},
	{http.MethodGet, "/api/recordings/{id}/play/segments/{segmentID}", authn.PermissionRecordingRead},
	{http.MethodGet, "/api/recordings/{id}/tags", authn.PermissionRecordingRead},
	{http.MethodPut, "/api/recordings/{id}/tags", authn.PermissionRecordingAnnotate},
	{http.MethodGet, "/api/recordings/{id}/archive/index", authn.PermissionRecordingRead},
	{http.MethodGet, "/api/recordings/{id}/integrity", authn.PermissionRecordingRead},
	{http.MethodPost, "/api/recordings/{id}/integrity/verify", authn.PermissionRecordingIntegrity},
	{http.MethodGet, "/api/integrity/jobs/{job_id}", authn.PermissionRecordingRead},
	{http.MethodPost, "/api/integrity/jobs/{job_id}/cancel", authn.PermissionRecordingIntegrity},
	{http.MethodPost, "/api/recordings/{id}/exports", authn.PermissionRecordingExport},
	{http.MethodGet, "/api/recordings/{id}/exports", authn.PermissionRecordingRead},
	{http.MethodGet, "/api/exports/{job_id}", authn.PermissionRecordingRead},
	{http.MethodGet, "/api/exports/{job_id}/download", authn.PermissionRecordingExport},
	{http.MethodDelete, "/api/exports/{job_id}", authn.PermissionRecordingExport},
	{http.MethodGet, "/api/recordings/{id}/thumbnail", authn.PermissionRecordingRead},
	{http.MethodPost, "/api/recordings/{id}/thumbnail/regenerate", authn.PermissionRecordingPreview},
	{http.MethodGet, "/api/recordings/{id}/previews", authn.PermissionRecordingRead},
	{http.MethodGet, "/api/recordings/{id}/previews/{ordinal}", authn.PermissionRecordingRead},
	{http.MethodPost, "/api/recordings/{id}/previews", authn.PermissionRecordingPreview},
	{http.MethodGet, "/api/v2/recordings", authn.PermissionRecordingRead},
	{http.MethodGet, "/api/dashboard", authn.PermissionRecordingRead},
	{http.MethodGet, "/api/search", authn.PermissionRecordingRead},
	{http.MethodGet, "/api/system/storage", authn.PermissionStorageRead},
	{http.MethodGet, "/api/storage/pools", authn.PermissionStorageRead},
	{http.MethodGet, "/api/storage/pools/{pool_id}/metrics", authn.PermissionStorageRead},
	{http.MethodGet, "/api/system/info", authn.PermissionSystemRead},
	{http.MethodGet, "/api/resolve-workflows", authn.PermissionRecordingWorkflow},
	{http.MethodGet, "/api/resolve-workflows/{id}", authn.PermissionRecordingWorkflow},
	{http.MethodDelete, "/api/resolve-workflows/{id}", authn.PermissionRecordingWorkflow},
	{http.MethodPost, "/api/resolve-workflows/{id}/continue", authn.PermissionRecordingWorkflow},
	{http.MethodGet, "/api/workflow-history", authn.PermissionRecordingWorkflow},
	{http.MethodGet, "/api/workflow-history/{id}", authn.PermissionRecordingWorkflow},
	{http.MethodGet, "/api/adapters", authn.PermissionRecordingCreate},
	{http.MethodGet, "/api/adapters/{id}", authn.PermissionRecordingCreate},
	{http.MethodGet, "/api/adapters/{id}/icon", authn.PermissionPluginRead},
	{http.MethodGet, "/api/adapters/{id}/schema", authn.PermissionRecordingCreate},
	{http.MethodGet, "/api/adapters/{id}/config", authn.PermissionPluginConfigure},
	{http.MethodPut, "/api/adapters/{id}/config", authn.PermissionPluginConfigure},
	{http.MethodGet, "/api/adapters/{id}/resources", authn.PermissionRecordingCreate},
	{http.MethodGet, "/api/adapters/{id}/resources/search", authn.PermissionRecordingCreate},
	{http.MethodPost, "/api/adapters/{id}/restart", authn.PermissionPluginManage},
	{http.MethodPost, "/api/adapters/{id}/enable", authn.PermissionPluginManage},
	{http.MethodPost, "/api/adapters/{id}/disable", authn.PermissionPluginManage},
	{http.MethodPost, "/api/watches", authn.PermissionWatchManage},
	{http.MethodGet, "/api/watches", authn.PermissionWatchRead},
	{http.MethodGet, "/api/watches/{id}", authn.PermissionWatchRead},
	{http.MethodPut, "/api/watches/{id}", authn.PermissionWatchManage},
	{http.MethodDelete, "/api/watches/{id}", authn.PermissionWatchManage},
	{http.MethodPost, "/api/watches/{id}/enable", authn.PermissionWatchManage},
	{http.MethodPost, "/api/watches/{id}/disable", authn.PermissionWatchManage},
	{http.MethodPost, "/api/watches/{id}/check", authn.PermissionWatchCheck},
	{http.MethodGet, "/api/watches/{id}/recordings", authn.PermissionWatchRead},
	{http.MethodGet, "/api/watches/{id}/events", authn.PermissionWatchRead},
	{http.MethodGet, "/api/recordings/{id}/events", authn.PermissionRecordingRead},
	{http.MethodGet, "/api/recordings/{id}/timed-events", authn.PermissionRecordingRead},
	{http.MethodGet, "/api/recordings/{id}/timed-events/export", authn.PermissionRecordingExport},
	{http.MethodGet, "/api/recordings/{id}/metadata", authn.PermissionRecordingRead},
	{http.MethodGet, "/api/audit", authn.PermissionAuditRead},
	{http.MethodGet, "/api/notifications", authn.PermissionSystemDiagnostics},
	{http.MethodPost, "/api/notifications/sync", authn.PermissionSystemDiagnostics},
	{http.MethodPost, "/api/notifications/{id}/read", authn.PermissionSystemDiagnostics},
	{http.MethodPost, "/api/notifications/read-all", authn.PermissionSystemDiagnostics},
	{http.MethodGet, "/api/logs", authn.PermissionSystemDiagnostics},
	{http.MethodGet, "/api/settings", authn.PermissionSystemRead},
	{http.MethodPut, "/api/settings", authn.PermissionSystemSettings},
	{http.MethodGet, "/api/retention/candidates", authn.PermissionRecordingRead},
	{http.MethodPost, "/api/retention/run", authn.PermissionRecordingDelete},
	{http.MethodGet, "/api/runtime/update", authn.PermissionSystemUpdate},
	{http.MethodPost, "/api/runtime/update/check", authn.PermissionSystemUpdate},
	{http.MethodPost, "/api/runtime/update/stage", authn.PermissionSystemUpdate},
	{http.MethodPost, "/api/runtime/update/activate", authn.PermissionSystemUpdate},
	{http.MethodPost, "/api/runtime/update/rollback", authn.PermissionSystemUpdate},
	{http.MethodPost, "/api/runtime/adapters/reconcile", authn.PermissionPluginManage},
}

func routePermission(method, path string) authn.Permission {
	if method == http.MethodHead {
		method = http.MethodGet
	}
	for _, rule := range protectedRoutePermissions {
		if rule.method == method && routePatternMatches(rule.pattern, path) {
			return rule.permission
		}
	}
	return authn.Permission("permission.unclassified")
}

// recordingPermission is retained as a narrow test helper for recorder route
// contracts; the global route table remains the single authorization source.
func recordingPermission(method, path string) (authn.Permission, bool) {
	permission := routePermission(method, path)
	return permission, permission != authn.Permission("permission.unclassified")
}

func routePatternMatches(pattern, path string) bool {
	left, right := strings.Split(strings.TrimPrefix(pattern, "/"), "/"), strings.Split(strings.TrimPrefix(path, "/"), "/")
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if strings.HasPrefix(left[i], "{") && strings.HasSuffix(left[i], "}") {
			if right[i] == "" {
				return false
			}
			continue
		}
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
