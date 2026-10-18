package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/integrated-recorder/core/internal/authn"
	"github.com/integrated-recorder/core/internal/management"
)

func newAccountTestHandler(t *testing.T) (*authn.Service, *management.Store, http.Handler, authn.Session) {
	t.Helper()
	root := t.TempDir()
	auth, err := authn.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := auth.Bootstrap("owner", "account-owner-password"); err != nil {
		t.Fatal(err)
	}
	ownerSession, err := auth.Login("account-owner-password")
	if err != nil {
		t.Fatal(err)
	}
	products, err := management.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	handler := NewWithOptions(nil, nil, nil, Options{Auth: auth, Management: products})
	return auth, products, handler, ownerSession
}

func accountRequest(handler http.Handler, method, path, body string, session authn.Session, includeCSRF bool) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.AddCookie(&http.Cookie{Name: authn.SessionCookieName, Value: session.Token})
	if includeCSRF {
		request.Header.Set("X-CSRF-Token", session.CSRFToken)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func TestAccountPasswordChangeCSRFAndSecretSafeAudit(t *testing.T) {
	auth, products, handler, session := newAccountTestHandler(t)
	owner, err := auth.GetUser(session.UserID)
	if err != nil {
		t.Fatal(err)
	}
	account := accountRequest(handler, http.MethodGet, "/api/user", "", session, false)
	if account.Code != http.StatusOK || strings.Contains(account.Body.String(), "password_hash") || strings.Contains(account.Body.String(), "credential_epoch") {
		t.Fatalf("self account response=%d %s", account.Code, account.Body.String())
	}
	if noCSRF := accountRequest(handler, http.MethodPost, "/api/user/password", `{"current_password":"account-owner-password","new_password":"account-new-password"}`, session, false); noCSRF.Code != http.StatusForbidden {
		t.Fatalf("password change without CSRF status=%d", noCSRF.Code)
	}
	wrong := accountRequest(handler, http.MethodPost, "/api/user/password", `{"current_password":"wrong-account-password","new_password":"account-new-password"}`, session, true)
	if wrong.Code != http.StatusUnauthorized || strings.Contains(wrong.Body.String(), "wrong-account-password") {
		t.Fatalf("wrong current password response=%d %s", wrong.Code, wrong.Body.String())
	}
	invalid := accountRequest(handler, http.MethodPost, "/api/user/password", `{"current_password":"account-owner-password","new_password":"short"}`, session, true)
	if invalid.Code != http.StatusBadRequest {
		t.Fatalf("invalid new password status=%d body=%s", invalid.Code, invalid.Body.String())
	}
	changed := accountRequest(handler, http.MethodPost, "/api/user/password", `{"current_password":"account-owner-password","new_password":"account-new-password"}`, session, true)
	if changed.Code != http.StatusNoContent {
		t.Fatalf("password change status=%d body=%s", changed.Code, changed.Body.String())
	}
	cleared := false
	for _, cookie := range changed.Result().Cookies() {
		if (cookie.Name == authn.SessionCookieName || cookie.Name == csrfCookieName) && cookie.MaxAge < 0 {
			cleared = true
		}
	}
	if !cleared {
		t.Fatal("password change did not clear session cookies")
	}
	if _, err := auth.Authenticate(session.Token); err == nil {
		t.Fatal("password change left the current session valid")
	}
	if _, err := auth.Login("account-owner-password"); err == nil {
		t.Fatal("old password remained valid")
	}
	if _, err := auth.Login("account-new-password"); err != nil {
		t.Fatalf("new password login: %v", err)
	}
	events, _, err := products.AuditPage(20, "")
	if err != nil || len(events) != 1 || events[0].Type != "password_changed" || events[0].Actor == nil || events[0].Actor.Type != management.AuditActorUser || events[0].Actor.UserID != owner.ID {
		t.Fatalf("password audit=%+v err=%v", events, err)
	}
	data, err := json.Marshal(events)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"account-owner-password", "account-new-password", session.Token, session.CSRFToken} {
		if strings.Contains(string(data), secret) {
			t.Fatal("audit event disclosed credential material")
		}
	}
}

func TestUserAndSessionManagementIsolationAndAudit(t *testing.T) {
	auth, products, handler, ownerSession := newAccountTestHandler(t)
	secondOwner, err := auth.Login("account-owner-password")
	if err != nil {
		t.Fatal(err)
	}
	listed := accountRequest(handler, http.MethodGet, "/api/user/sessions", "", ownerSession, false)
	if listed.Code != http.StatusOK || strings.Contains(listed.Body.String(), ownerSession.Token) || strings.Contains(listed.Body.String(), ownerSession.CSRFToken) {
		t.Fatalf("session list response=%d %s", listed.Code, listed.Body.String())
	}
	var sessionPage struct {
		Items []authn.SessionInfo `json:"items"`
	}
	if err := json.Unmarshal(listed.Body.Bytes(), &sessionPage); err != nil || len(sessionPage.Items) != 2 {
		t.Fatalf("session page=%+v err=%v", sessionPage, err)
	}
	var otherID string
	for _, item := range sessionPage.Items {
		if !item.Current {
			otherID = item.ID
		}
	}
	if otherID == "" {
		t.Fatal("current session was not identified")
	}
	revoked := accountRequest(handler, http.MethodDelete, "/api/user/sessions/"+otherID, "", ownerSession, true)
	if revoked.Code != http.StatusNoContent {
		t.Fatalf("session revoke status=%d body=%s", revoked.Code, revoked.Body.String())
	}
	if _, err := auth.Authenticate(secondOwner.Token); err == nil {
		t.Fatal("revoked session remained valid")
	}
	third, err := auth.Login("account-owner-password")
	if err != nil {
		t.Fatal(err)
	}
	revokeOthers := accountRequest(handler, http.MethodPost, "/api/user/sessions/revoke-others", `{}`, ownerSession, true)
	if revokeOthers.Code != http.StatusOK || !strings.Contains(revokeOthers.Body.String(), `"revoked_count":1`) {
		t.Fatalf("revoke others status=%d body=%s", revokeOthers.Code, revokeOthers.Body.String())
	}
	if _, err := auth.Authenticate(ownerSession.Token); err != nil {
		t.Fatalf("current session was revoked with others: %v", err)
	}
	if _, err := auth.Authenticate(third.Token); err == nil {
		t.Fatal("other session survived revoke-others")
	}
	created := accountRequest(handler, http.MethodPost, "/api/users", `{"login":"new-member","password":"new-member-password"}`, ownerSession, true)
	if created.Code != http.StatusCreated || strings.Contains(created.Body.String(), "password_hash") || strings.Contains(created.Body.String(), "new-member-password") {
		t.Fatalf("user create status=%d body=%s", created.Code, created.Body.String())
	}
	var newUser authn.User
	if err := json.Unmarshal(created.Body.Bytes(), &newUser); err != nil || newUser.Role != authn.RoleMember || newUser.Status != authn.UserStatusActive {
		t.Fatalf("created user=%+v err=%v", newUser, err)
	}
	memberSession, err := auth.LoginAs(newUser.Login, "new-member-password")
	if err != nil {
		t.Fatal(err)
	}
	memberList := accountRequest(handler, http.MethodGet, "/api/users", "", memberSession, false)
	if memberList.Code != http.StatusForbidden {
		t.Fatalf("member user-list status=%d body=%s", memberList.Code, memberList.Body.String())
	}
	memberSelf := accountRequest(handler, http.MethodGet, "/api/user", "", memberSession, false)
	if memberSelf.Code != http.StatusOK {
		t.Fatalf("member self account status=%d", memberSelf.Code)
	}
	selfDisable := accountRequest(handler, http.MethodPost, "/api/users/"+newUser.ID+"/disable", `{}`, memberSession, true)
	if selfDisable.Code != http.StatusForbidden && selfDisable.Code != http.StatusConflict {
		t.Fatalf("member self-disable status=%d", selfDisable.Code)
	}
	disabled := accountRequest(handler, http.MethodPost, "/api/users/"+newUser.ID+"/disable", `{}`, ownerSession, true)
	if disabled.Code != http.StatusOK {
		t.Fatalf("disable user status=%d body=%s", disabled.Code, disabled.Body.String())
	}
	if _, err := auth.Authenticate(memberSession.Token); err == nil {
		t.Fatal("disabled user session remained valid")
	}
	if _, err := auth.LoginAs(newUser.Login, "new-member-password"); err == nil {
		t.Fatal("disabled user logged in")
	}
	enabled := accountRequest(handler, http.MethodPost, "/api/users/"+newUser.ID+"/enable", `{}`, ownerSession, true)
	if enabled.Code != http.StatusOK {
		t.Fatalf("enable user status=%d body=%s", enabled.Code, enabled.Body.String())
	}
	if _, err := auth.LoginAs(newUser.Login, "new-member-password"); err != nil {
		t.Fatalf("enabled user login: %v", err)
	}
	ownerDisable := accountRequest(handler, http.MethodPost, "/api/users/"+ownerSession.UserID+"/disable", `{}`, ownerSession, true)
	if ownerDisable.Code != http.StatusConflict {
		t.Fatalf("self owner disable status=%d body=%s", ownerDisable.Code, ownerDisable.Body.String())
	}
	events, _, err := products.AuditPage(50, "")
	if err != nil {
		t.Fatal(err)
	}
	wanted := map[string]struct{ actorID, objectID string }{
		"session_revoked":  {actorID: ownerSession.UserID, objectID: otherID},
		"sessions_revoked": {actorID: ownerSession.UserID, objectID: ownerSession.UserID},
		"user_created":     {actorID: ownerSession.UserID, objectID: newUser.ID},
		"user_disabled":    {actorID: ownerSession.UserID, objectID: newUser.ID},
		"user_enabled":     {actorID: ownerSession.UserID, objectID: newUser.ID},
	}
	for action, expected := range wanted {
		found := false
		for _, event := range events {
			if event.Type == action && event.ObjectID == expected.objectID && event.Actor != nil && event.Actor.Type == management.AuditActorUser && event.Actor.UserID == expected.actorID {
				found = true
			}
		}
		if !found {
			t.Errorf("missing %s audit with actor %s", action, expected.actorID)
		}
	}
	encoded, _ := json.Marshal(events)
	for _, secret := range []string{ownerSession.Token, ownerSession.CSRFToken, "new-member-password"} {
		if strings.Contains(string(encoded), secret) {
			t.Fatal("audit response exposed secret material")
		}
	}
}

func TestUserManagementProtectsLastActiveOwnerAtHTTPBoundary(t *testing.T) {
	auth, _, handler, ownerSession := newAccountTestHandler(t)
	owner, err := auth.GetUser(ownerSession.UserID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := auth.CreateUser("second-owner", "second-owner-password", authn.RoleOwner); err != nil {
		t.Fatal(err)
	}
	second, err := auth.FindByLogin("second-owner")
	if err != nil {
		t.Fatal(err)
	}
	if response := accountRequest(handler, http.MethodPost, "/api/users/"+owner.ID+"/disable", `{}`, ownerSession, true); response.Code != http.StatusConflict {
		t.Fatalf("owner self-disable status=%d body=%s", response.Code, response.Body.String())
	}
	otherSession, err := auth.LoginAs(second.Login, "second-owner-password")
	if err != nil {
		t.Fatal(err)
	}
	if response := accountRequest(handler, http.MethodPost, "/api/users/"+owner.ID+"/disable", `{}`, otherSession, true); response.Code != http.StatusOK {
		t.Fatalf("disable other active owner status=%d body=%s", response.Code, response.Body.String())
	}
	if response := accountRequest(handler, http.MethodPost, "/api/users/"+second.ID+"/disable", `{}`, otherSession, true); response.Code != http.StatusConflict {
		t.Fatalf("last owner protection status=%d body=%s", response.Code, response.Body.String())
	}
	if _, err := auth.Authenticate(ownerSession.Token); err == nil {
		t.Fatal("disabled owner session remained valid")
	}
}
