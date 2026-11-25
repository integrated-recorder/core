package main

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/globalidentity"
	"github.com/integrated-recorder/core/internal/nodeidentity"
	"github.com/integrated-recorder/core/internal/peerapi"
)

type fakeIdentitySnapshotReader struct {
	snapshot globalidentity.Snapshot
	found    bool
	err      error
}

func (f fakeIdentitySnapshotReader) Snapshot() (globalidentity.Snapshot, bool, error) {
	return f.snapshot, f.found, f.err
}

type fakeNodeInfoReader struct {
	info nodeidentity.Info
	err  error
}

func (f fakeNodeInfoReader) Info() (nodeidentity.Info, error) { return f.info, f.err }

type fakePeerConnectionManager struct {
	mu          sync.Mutex
	sessions    []peerapi.SessionInfo
	connects    []peerapi.SessionInfo
	disconnects []string
	connectErr  map[string]error
}

func (f *fakePeerConnectionManager) List() []peerapi.SessionInfo {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]peerapi.SessionInfo(nil), f.sessions...)
}

func (f *fakePeerConnectionManager) Connect(_ context.Context, identityID, nodeID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.connects = append(f.connects, peerapi.SessionInfo{IdentityID: identityID, NodeID: nodeID})
	return f.connectErr[nodeID]
}

func (f *fakePeerConnectionManager) Disconnect(nodeID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.disconnects = append(f.disconnects, nodeID)
	return nil
}

func (f *fakePeerConnectionManager) results() ([]peerapi.SessionInfo, []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]peerapi.SessionInfo(nil), f.connects...), append([]string(nil), f.disconnects...)
}

func TestPeerConnectionReconcilerDialsOnlyDelegatedRemoteNodesAndRotatesBatch(t *testing.T) {
	identityID := "gi_aaaaaaaaaaaaaaaaaaaaaa"
	localNodeID := "nd_0000000000000000000000"
	nodes := []string{
		localNodeID,
		"nd_0000000000000000000001",
		"nd_0000000000000000000002",
		"nd_0000000000000000000003",
		"nd_0000000000000000000004",
		"nd_0000000000000000000005",
		"nd_0000000000000000000006",
		"nd_0000000000000000000007",
		"nd_0000000000000000000008",
	}
	peers := &fakePeerConnectionManager{connectErr: map[string]error{}}
	reconciler := &peerConnectionReconciler{
		identity: fakeIdentitySnapshotReader{snapshot: globalidentity.Snapshot{IdentityID: identityID, NodeIDs: nodes}, found: true},
		node:     fakeNodeInfoReader{info: nodeidentity.Info{NodeID: localNodeID}},
		peers:    peers,
	}
	if err := reconciler.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	first, _ := peers.results()
	if len(first) != peerConnectionBatch {
		t.Fatalf("first pass dial count=%d, want bounded batch %d", len(first), peerConnectionBatch)
	}
	firstIDs := make([]string, len(first))
	for i, session := range first {
		if session.IdentityID != identityID || session.NodeID == localNodeID {
			t.Fatalf("unexpected dial target: %+v", session)
		}
		firstIDs[i] = session.NodeID
	}
	sort.Strings(firstIDs)
	if err := reconciler.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	all, _ := peers.results()
	secondIDs := make([]string, 0, len(all)-len(first))
	for _, session := range all[len(first):] {
		secondIDs = append(secondIDs, session.NodeID)
	}
	if len(secondIDs) != peerConnectionBatch {
		t.Fatalf("second pass dial count=%d, want %d", len(secondIDs), peerConnectionBatch)
	}
	for _, id := range secondIDs {
		if containsString(firstIDs, id) {
			t.Fatalf("batch cursor retried %q before visiting remaining peers; first=%v second=%v", id, firstIDs, secondIDs)
		}
	}
}

func TestPeerConnectionReconcilerDisconnectsRevokedAndWrongIdentitySessions(t *testing.T) {
	identityID := "gi_aaaaaaaaaaaaaaaaaaaaaa"
	localNodeID := "nd_local0000000000000000"
	authorized := "nd_authorized000000000000"
	peers := &fakePeerConnectionManager{
		sessions: []peerapi.SessionInfo{
			{IdentityID: identityID, NodeID: authorized},
			{IdentityID: identityID, NodeID: "nd_revoked00000000000000"},
			{IdentityID: "gi_bbbbbbbbbbbbbbbbbbbbbb", NodeID: "nd_otheridentity0000000"},
		},
		connectErr: map[string]error{},
	}
	reconciler := &peerConnectionReconciler{
		identity: fakeIdentitySnapshotReader{snapshot: globalidentity.Snapshot{IdentityID: identityID, NodeIDs: []string{localNodeID, authorized}}, found: true},
		node:     fakeNodeInfoReader{info: nodeidentity.Info{NodeID: localNodeID}},
		peers:    peers,
	}
	if err := reconciler.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	connects, disconnects := peers.results()
	if len(connects) != 0 {
		t.Fatalf("already-connected authorized peer was dialed again: %+v", connects)
	}
	sort.Strings(disconnects)
	want := []string{"nd_otheridentity0000000", "nd_revoked00000000000000"}
	if !reflect.DeepEqual(disconnects, want) {
		t.Fatalf("disconnected sessions=%v, want %v", disconnects, want)
	}
}

func TestPeerConnectionReconcilerFrozenIdentityDisconnectsAllAndDoesNotDial(t *testing.T) {
	peers := &fakePeerConnectionManager{
		sessions:   []peerapi.SessionInfo{{IdentityID: "gi_aaaaaaaaaaaaaaaaaaaaaa", NodeID: "nd_peer0000000000000000"}},
		connectErr: map[string]error{},
	}
	reconciler := &peerConnectionReconciler{
		identity: fakeIdentitySnapshotReader{snapshot: globalidentity.Snapshot{IdentityID: "gi_aaaaaaaaaaaaaaaaaaaaaa", Frozen: true}, found: true},
		node:     fakeNodeInfoReader{info: nodeidentity.Info{NodeID: "nd_local0000000000000000"}},
		peers:    peers,
	}
	if err := reconciler.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	connects, disconnects := peers.results()
	if len(connects) != 0 || !reflect.DeepEqual(disconnects, []string{"nd_peer0000000000000000"}) {
		t.Fatalf("frozen identity reconciliation connects=%v disconnects=%v", connects, disconnects)
	}
}

func TestPeerConnectionReconcilerUsesOneDeterministicDialerPerPair(t *testing.T) {
	identityID := "gi_aaaaaaaaaaaaaaaaaaaaaa"
	localNodeID := "nd_zzzzzzzzzzzzzzzzzzzzzz"
	remoteNodeID := "nd_aaaaaaaaaaaaaaaaaaaaaa"
	peers := &fakePeerConnectionManager{connectErr: map[string]error{}}
	reconciler := &peerConnectionReconciler{
		identity: fakeIdentitySnapshotReader{snapshot: globalidentity.Snapshot{IdentityID: identityID, NodeIDs: []string{localNodeID, remoteNodeID}}, found: true},
		node:     fakeNodeInfoReader{info: nodeidentity.Info{NodeID: localNodeID}},
		peers:    peers,
	}
	if err := reconciler.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	connects, _ := peers.results()
	if len(connects) != 0 {
		t.Fatalf("higher Node ID initiated a competing dial: %+v", connects)
	}
}

func TestPeerConnectionReconcilerRetriesFailedDialOnNextPass(t *testing.T) {
	identityID := "gi_aaaaaaaaaaaaaaaaaaaaaa"
	localNodeID := "nd_local0000000000000000"
	remoteNodeID := "nd_remote00000000000000"
	peers := &fakePeerConnectionManager{connectErr: map[string]error{remoteNodeID: errors.New("temporary signaling failure")}}
	reconciler := &peerConnectionReconciler{
		identity: fakeIdentitySnapshotReader{snapshot: globalidentity.Snapshot{IdentityID: identityID, NodeIDs: []string{localNodeID, remoteNodeID}}, found: true},
		node:     fakeNodeInfoReader{info: nodeidentity.Info{NodeID: localNodeID}},
		peers:    peers,
	}
	if err := reconciler.Reconcile(context.Background()); err == nil {
		t.Fatal("failed connection attempt was hidden")
	}
	if err := reconciler.Reconcile(context.Background()); err == nil {
		t.Fatal("subsequent connection failure was hidden")
	}
	connects, _ := peers.results()
	if len(connects) != 2 {
		t.Fatalf("connection retries=%d, want 2", len(connects))
	}
}

func TestPeerConnectionLoopJoinsActivePassOnStop(t *testing.T) {
	started := make(chan struct{})
	pass := func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}
	app := &controlApplication{peerConnectionPass: pass}
	if err := app.startPeerConnectionLoop(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("peer reconciliation did not start")
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := app.stopPeerConnectionLoop(stopCtx); err != nil {
		t.Fatal(err)
	}
	app.mu.Lock()
	defer app.mu.Unlock()
	if app.peerConnectionCancel != nil || app.peerConnectionDone != nil {
		t.Fatal("peer connection loop was not fully joined")
	}
}

func containsString(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}
