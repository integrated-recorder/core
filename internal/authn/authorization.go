package authn

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"
)

// Permission identifiers are stable API values. Built-in role definitions are
// compiled into the application; only custom role definitions are persisted.
const (
	RoleOwner         = "owner"
	RoleAdministrator = "administrator"
	RoleOperator      = "operator"
	RoleViewer        = "viewer"
	// RoleMember remains a source compatibility alias for the old member role.
	RoleMember         = RoleOperator
	UserStatusActive   = "active"
	UserStatusDisabled = "disabled"
)

type Permission string

const (
	PermissionRecordingRead       Permission = "recording.read"
	PermissionRecordingCreate     Permission = "recording.create"
	PermissionRecordingStop       Permission = "recording.stop"
	PermissionRecordingDelete     Permission = "recording.delete"
	PermissionRecordingExport     Permission = "recording.export"
	PermissionRecordingIntegrity  Permission = "recording.integrity.run"
	PermissionRecordingAnnotate   Permission = "recording.annotate"
	PermissionRecordingPreview    Permission = "recording.preview.manage"
	PermissionRecordingWorkflow   Permission = "recording.workflow.manage"
	PermissionWatchRead           Permission = "watch.read"
	PermissionWatchManage         Permission = "watch.manage"
	PermissionWatchCheck          Permission = "watch.check"
	PermissionPluginRead          Permission = "plugin.read"
	PermissionPluginManage        Permission = "plugin.manage"
	PermissionPluginConfigure     Permission = "plugin.configure"
	PermissionStorageRead         Permission = "storage.read"
	PermissionStorageManage       Permission = "storage.manage"
	PermissionSystemRead          Permission = "system.read"
	PermissionSystemSettings      Permission = "system.settings.manage"
	PermissionSystemUpdate        Permission = "system.update.manage"
	PermissionSystemDiagnostics   Permission = "system.diagnostics.read"
	PermissionUserRead            Permission = "user.read"
	PermissionUserCreate          Permission = "user.create"
	PermissionUserManage          Permission = "user.manage"
	PermissionRoleRead            Permission = "role.read"
	PermissionRoleManage          Permission = "role.manage"
	PermissionAuditRead           Permission = "audit.read"
	PermissionAccountProfileSelf  Permission = "account.profile.self"
	PermissionAccountPasswordSelf Permission = "account.password.self"
	PermissionAccountSessionsSelf Permission = "account.sessions.self"
	PermissionUserPreferences     Permission = "user.preferences.self"
	// Deprecated source aliases retained for existing in-package integrations.
	PermissionRecordingControl Permission = "recording.control"
	PermissionSettingsManage   Permission = "settings.manage"
	PermissionUpdateManage     Permission = "update.manage"
)

var permissionCatalog = []Permission{
	PermissionRecordingRead, PermissionRecordingCreate, PermissionRecordingStop,
	PermissionRecordingDelete, PermissionRecordingExport, PermissionRecordingIntegrity,
	PermissionRecordingAnnotate, PermissionRecordingPreview, PermissionRecordingWorkflow,
	PermissionWatchRead, PermissionWatchManage, PermissionWatchCheck,
	PermissionPluginRead, PermissionPluginManage, PermissionPluginConfigure,
	PermissionStorageRead, PermissionStorageManage, PermissionSystemRead,
	PermissionSystemSettings, PermissionSystemUpdate, PermissionSystemDiagnostics,
	PermissionUserRead, PermissionUserCreate, PermissionUserManage,
	PermissionRoleRead, PermissionRoleManage, PermissionAuditRead,
	PermissionAccountProfileSelf, PermissionAccountPasswordSelf,
	PermissionAccountSessionsSelf, PermissionUserPreferences,
}

type Role struct {
	ID          string       `json:"id"`
	Name        string       `json:"name"`
	BuiltIn     bool         `json:"built_in"`
	Permissions []Permission `json:"permissions"`
	CreatedAt   *time.Time   `json:"created_at,omitempty"`
}

type Authorization struct {
	UserID      string       `json:"user_id"`
	Login       string       `json:"login"`
	Roles       []string     `json:"roles"`
	Permissions []Permission `json:"permissions"`
}

var roleIDRE = regexp.MustCompile(`^[a-z][a-z0-9-]{0,63}$`)

func PermissionCatalog() []Permission {
	return append([]Permission(nil), permissionCatalog...)
}

func BuiltInRoles() []Role {
	all := nonSelfPermissions()
	operator := []Permission{
		PermissionRecordingRead, PermissionRecordingCreate, PermissionRecordingStop,
		PermissionRecordingIntegrity, PermissionRecordingExport, PermissionRecordingAnnotate,
		PermissionRecordingPreview, PermissionRecordingWorkflow, PermissionWatchCheck,
	}
	viewer := []Permission{PermissionRecordingRead, PermissionWatchRead, PermissionPluginRead, PermissionStorageRead, PermissionSystemRead}
	admin := append([]Permission(nil), all...)
	return []Role{
		{ID: RoleOwner, Name: "Owner", BuiltIn: true, Permissions: all},
		{ID: RoleAdministrator, Name: "Administrator", BuiltIn: true, Permissions: admin},
		{ID: RoleOperator, Name: "Operator", BuiltIn: true, Permissions: operator},
		{ID: RoleViewer, Name: "Viewer", BuiltIn: true, Permissions: viewer},
	}
}

func nonSelfPermissions() []Permission {
	result := make([]Permission, 0, len(permissionCatalog))
	for _, permission := range permissionCatalog {
		switch permission {
		case PermissionAccountProfileSelf, PermissionAccountPasswordSelf, PermissionAccountSessionsSelf, PermissionUserPreferences:
			continue
		default:
			result = append(result, permission)
		}
	}
	return result
}

func builtinRole(id string) (Role, bool) {
	for _, role := range BuiltInRoles() {
		if role.ID == id {
			return role, true
		}
	}
	return Role{}, false
}

func permissionsForRoles(roles []string, custom []Role) []Permission {
	customByID := make(map[string]Role, len(custom))
	for _, role := range custom {
		customByID[role.ID] = role
	}
	set := make(map[Permission]struct{})
	for _, roleID := range roles {
		role, ok := builtinRole(roleID)
		if !ok {
			role, ok = customByID[roleID]
		}
		if !ok {
			continue
		}
		for _, permission := range role.Permissions {
			set[permission] = struct{}{}
		}
	}
	result := make([]Permission, 0, len(set))
	for permission := range set {
		result = append(result, permission)
	}
	result = append(result, PermissionAccountProfileSelf, PermissionAccountPasswordSelf, PermissionAccountSessionsSelf, PermissionUserPreferences)
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}

func allRoleIDsKnown(roles []string, custom []Role) bool {
	if len(roles) == 0 {
		return false
	}
	customSet := make(map[string]struct{}, len(custom))
	for _, role := range custom {
		customSet[role.ID] = struct{}{}
	}
	seen := make(map[string]struct{}, len(roles))
	for _, id := range roles {
		if _, exists := seen[id]; exists {
			return false
		}
		seen[id] = struct{}{}
		if _, ok := builtinRole(id); ok {
			continue
		}
		if _, ok := customSet[id]; !ok {
			return false
		}
	}
	return true
}

func validateRolePermissions(permissions []Permission) ([]Permission, error) {
	known := make(map[Permission]struct{}, len(permissionCatalog))
	for _, permission := range permissionCatalog {
		known[permission] = struct{}{}
	}
	if len(permissions) == 0 || len(permissions) > len(permissionCatalog) {
		return nil, ErrInvalidRole
	}
	set := make(map[Permission]struct{}, len(permissions))
	for _, permission := range permissions {
		if _, ok := known[permission]; !ok {
			return nil, ErrInvalidRole
		}
		if _, duplicate := set[permission]; duplicate {
			return nil, ErrInvalidRole
		}
		set[permission] = struct{}{}
	}
	result := append([]Permission(nil), permissions...)
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result, nil
}

func randomRoleID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("%w: generate role identity", ErrStorage)
	}
	return "role-" + hex.EncodeToString(raw[:]), nil
}

func validRoleID(id string) bool {
	if _, ok := builtinRole(id); ok {
		return true
	}
	return roleIDRE.MatchString(id) && !strings.HasPrefix(id, "owner") && !strings.HasPrefix(id, "administrator")
}

func hasRole(roles []string, id string) bool {
	for _, candidate := range roles {
		if candidate == id {
			return true
		}
	}
	return false
}

var (
	ErrInvalidRole     = errors.New("role definition is invalid")
	ErrRoleNotFound    = errors.New("role not found")
	ErrBuiltinRole     = errors.New("built-in role cannot be changed")
	ErrRoleInUse       = errors.New("role is assigned to users")
	ErrOwnerAssignment = errors.New("owner assignment requires an owner")
)

func validateRoleAssignments(roles []string, custom []Role) error {
	if !allRoleIDsKnown(roles, custom) {
		return ErrInvalidRole
	}
	return nil
}
