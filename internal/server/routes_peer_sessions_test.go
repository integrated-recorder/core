package server

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/integrated-recorder/core/internal/authn"
	"github.com/integrated-recorder/core/internal/peerapi"
)

type peerSessionManagerFixture struct {
	sessions      []peerapi.SessionInfo
	identity      string
	node          string
	connectErr    error
	disconnectErr error
}

func (m *peerSessionManagerFixture) List() []peerapi.SessionInfo {
	return append([]peerapi.SessionInfo(nil), m.sessions...)
}
func (m *peerSessionManagerFixture) Connect(_ context.Context, identity, node string) error {
	m.identity, m.node = identity, node
	if m.connectErr != nil {
		return m.connectErr
	}
	m.sessions = append(m.sessions, peerapi.SessionInfo{IdentityID: identity, NodeID: node})
	return nil
}
func (m *peerSessionManagerFixture) Disconnect(node string) error {
	m.node = node
	if m.disconnectErr != nil {
		return m.disconnectErr
	}
	m.sessions = nil
	return nil
}

func TestPeerSessionAPIRequiresPermissionAndCSRFAndConnectsExactTarget(t *testing.T) {
	root := t.TempDir()
	auth, err := authn.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := auth.Bootstrap("peer-owner", "peer-owner-password"); err != nil {
		t.Fatal(err)
	}
	owner, err := auth.LoginAs("peer-owner", "peer-owner-password")
	if err != nil {
		t.Fatal(err)
	}
	member, err := auth.CreateUser("peer-member", "peer-member-password", authn.RoleMember)
	if err != nil {
		t.Fatal(err)
	}
	memberSession, err := auth.LoginAs(member.Login, "peer-member-password")
	if err != nil {
		t.Fatal(err)
	}
	manager := &peerSessionManagerFixture{}
	handler := NewWithOptions(nil, nil, nil, Options{Auth: auth, PeerSessions: manager})
	body := `{"identity_id":"gi_aaaaaaaaaaaaaaaaaaaaaa","node_id":"nd_BBBBBBBBBBBBBBBBBBBBBB"}`

	listedByReader := httptest.NewRecorder()
	handler.ServeHTTP(listedByReader, authenticatedIdentityRequest(http.MethodGet, "https://recorder.example.com/api/peer-sessions", "", memberSession))
	if listedByReader.Code != http.StatusOK {
		t.Fatalf("recording reader session listing status=%d body=%s", listedByReader.Code, listedByReader.Body.String())
	}

	noCSRFRequest := authenticatedIdentityRequest(http.MethodPost, "https://recorder.example.com/api/peer-sessions", body, owner)
	noCSRFRequest.Header.Del("X-CSRF-Token")
	noCSRF := httptest.NewRecorder()
	handler.ServeHTTP(noCSRF, noCSRFRequest)
	if noCSRF.Code != http.StatusForbidden {
		t.Fatalf("peer connect without CSRF status=%d body=%s", noCSRF.Code, noCSRF.Body.String())
	}

	connected := httptest.NewRecorder()
	handler.ServeHTTP(connected, authenticatedIdentityRequest(http.MethodPost, "https://recorder.example.com/api/peer-sessions", body, owner))
	if connected.Code != http.StatusNoContent || manager.identity != "gi_aaaaaaaaaaaaaaaaaaaaaa" || manager.node != "nd_BBBBBBBBBBBBBBBBBBBBBB" {
		t.Fatalf("connect status=%d identity=%q node=%q body=%s", connected.Code, manager.identity, manager.node, connected.Body.String())
	}

	listed := httptest.NewRecorder()
	handler.ServeHTTP(listed, authenticatedIdentityRequest(http.MethodGet, "https://recorder.example.com/api/peer-sessions", "", owner))
	if listed.Code != http.StatusOK || !strings.Contains(listed.Body.String(), manager.identity) || !strings.Contains(listed.Body.String(), manager.node) {
		t.Fatalf("session list status=%d body=%s", listed.Code, listed.Body.String())
	}

	disconnected := httptest.NewRecorder()
	handler.ServeHTTP(disconnected, authenticatedIdentityRequest(http.MethodDelete, "https://recorder.example.com/api/peer-sessions/nd_BBBBBBBBBBBBBBBBBBBBBB", "", owner))
	if disconnected.Code != http.StatusNoContent || manager.node != "nd_BBBBBBBBBBBBBBBBBBBBBB" {
		t.Fatalf("disconnect status=%d node=%q body=%s", disconnected.Code, manager.node, disconnected.Body.String())
	}
}

func TestPeerSessionAPIMapsConnectionErrorsWithoutLeakingTransportDetails(t *testing.T) {
	root := t.TempDir()
	auth, err := authn.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := auth.Bootstrap("peer-owner", "peer-owner-password"); err != nil {
		t.Fatal(err)
	}
	owner, err := auth.LoginAs("peer-owner", "peer-owner-password")
	if err != nil {
		t.Fatal(err)
	}
	manager := &peerSessionManagerFixture{connectErr: errors.New("private endpoint path and credential leaked")}
	handler := NewWithOptions(nil, nil, nil, Options{Auth: auth, PeerSessions: manager})
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, authenticatedIdentityRequest(http.MethodPost, "https://recorder.example.com/api/peer-sessions", `{"identity_id":"gi_aaaaaaaaaaaaaaaaaaaaaa","node_id":"nd_BBBBBBBBBBBBBBBBBBBBBB"}`, owner))
	if response.Code != http.StatusBadGateway || response.Body.String() == "" || strings.Contains(response.Body.String(), "private endpoint") || strings.Contains(response.Body.String(), "credential leaked") {
		t.Fatalf("unsafe transport error mapping status=%d body=%s", response.Code, response.Body.String())
	}
}
