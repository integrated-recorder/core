package server

import (
	"errors"
	"net/http"
	"strings"

	"github.com/integrated-recorder/core/internal/authn"
)

func (s *Server) registerRoleRoutes() {
	s.mux.HandleFunc("GET /api/roles", s.rolesList)
	s.mux.HandleFunc("POST /api/roles", s.roleCreate)
	s.mux.HandleFunc("PUT /api/roles/{roleID}", s.roleUpdate)
	s.mux.HandleFunc("PATCH /api/roles/{roleID}", s.roleUpdate)
	s.mux.HandleFunc("DELETE /api/roles/{roleID}", s.roleDelete)
}

func (s *Server) rolesList(w http.ResponseWriter, r *http.Request) {
	if s.auth == nil {
		writeCodedError(w, http.StatusServiceUnavailable, "roles_unavailable", "Roles could not be loaded.")
		return
	}
	roles, permissions, err := s.auth.ListRoles()
	if err != nil {
		writeCodedError(w, http.StatusServiceUnavailable, "roles_unavailable", "Roles could not be loaded.")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"roles": roles, "permissions": permissions})
}

func (s *Server) roleCreate(w http.ResponseWriter, r *http.Request) {
	if s.auth == nil {
		writeCodedError(w, http.StatusServiceUnavailable, "roles_unavailable", "The role could not be created.")
		return
	}
	var request struct {
		Name        string             `json:"name"`
		Permissions []authn.Permission `json:"permissions"`
	}
	if err := decodeJSONBody(w, r, 16<<10, &request); err != nil {
		writeCodedError(w, http.StatusBadRequest, "role_invalid", "The role definition is invalid.")
		return
	}
	role, err := s.auth.CreateCustomRole(request.Name, request.Permissions)
	if err != nil {
		writeCodedError(w, http.StatusBadRequest, "role_invalid", "The role definition is invalid.")
		return
	}
	s.auditMutation(w, r, "role_created", role.ID)
	writeJSON(w, http.StatusCreated, role)
}

func (s *Server) roleUpdate(w http.ResponseWriter, r *http.Request) {
	if s.auth == nil {
		writeCodedError(w, http.StatusServiceUnavailable, "roles_unavailable", "The role could not be updated.")
		return
	}
	var request struct {
		Name        string             `json:"name"`
		Permissions []authn.Permission `json:"permissions"`
	}
	if err := decodeJSONBody(w, r, 16<<10, &request); err != nil {
		writeCodedError(w, http.StatusBadRequest, "role_invalid", "The role definition is invalid.")
		return
	}
	id := strings.TrimSpace(r.PathValue("roleID"))
	role, err := s.auth.UpdateCustomRole(id, request.Name, request.Permissions)
	switch {
	case errors.Is(err, authn.ErrBuiltinRole):
		writeCodedError(w, http.StatusConflict, "role_builtin", "Built-in roles cannot be edited.")
	case errors.Is(err, authn.ErrRoleNotFound):
		writeCodedError(w, http.StatusNotFound, "role_not_found", "The role could not be found.")
	case errors.Is(err, authn.ErrInvalidRole):
		writeCodedError(w, http.StatusBadRequest, "role_invalid", "The role definition is invalid.")
	case err != nil:
		writeCodedError(w, http.StatusServiceUnavailable, "roles_unavailable", "The role could not be updated.")
	default:
		s.auditMutation(w, r, "role_changed", role.ID)
		writeJSON(w, http.StatusOK, role)
	}
}

func (s *Server) roleDelete(w http.ResponseWriter, r *http.Request) {
	if s.auth == nil {
		writeCodedError(w, http.StatusServiceUnavailable, "roles_unavailable", "The role could not be deleted.")
		return
	}
	id := strings.TrimSpace(r.PathValue("roleID"))
	err := s.auth.DeleteCustomRole(id)
	switch {
	case errors.Is(err, authn.ErrBuiltinRole):
		writeCodedError(w, http.StatusConflict, "role_builtin", "Built-in roles cannot be deleted.")
	case errors.Is(err, authn.ErrRoleNotFound):
		writeCodedError(w, http.StatusNotFound, "role_not_found", "The role could not be found.")
	case errors.Is(err, authn.ErrRoleInUse):
		writeCodedError(w, http.StatusConflict, "role_in_use", "Remove this role from all users before deleting it.")
	case err != nil:
		writeCodedError(w, http.StatusServiceUnavailable, "roles_unavailable", "The role could not be deleted.")
	default:
		s.auditMutation(w, r, "role_deleted", id)
		w.WriteHeader(http.StatusNoContent)
	}
}
