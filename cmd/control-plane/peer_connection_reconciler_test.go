package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
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

func TestPeerConnectionReconcilerResumesFairBatchAfterRestart(t *testing.T) {
	identityID := "gi_AAAAAAAAAAAAAAAAAAAAAA"
	localNodeID := "nd_0000000000000000000000"
	nodes := []string{localNodeID}
	connectErr := make(map[string]error)
	for i := 1; i <= 9; i++ {
		nodeID := fmt.Sprintf("nd_%022d", i)
		nodes = append(nodes, nodeID)
		connectErr[nodeID] = errors.New("simulated unavailable peer")
	}
	cursorRoot := filepath.Join(t.TempDir(), "peer-connection")
	store, err := openPeerConnectionCursorStore(cursorRoot)
	if err != nil {
		t.Fatal(err)
	}
	identity := fakeIdentitySnapshotReader{snapshot: globalidentity.Snapshot{IdentityID: identityID, NodeIDs: nodes}, found: true}
	localNode := fakeNodeInfoReader{info: nodeidentity.Info{NodeID: localNodeID}}

	// A new reconciler and reopened store model a process restart on every pass.
	// The first eight peers fail, the one-peer tail is planned, then wraparound
	// must resume at Node 1 rather than retrying Node 9 forever.
	windows := [][2]int{{1, 5}, {5, 9}, {9, 10}, {1, 5}, {5, 9}}
	for pass, window := range windows {
		store, err = openPeerConnectionCursorStore(cursorRoot)
		if err != nil {
			t.Fatal(err)
		}
		peers := &fakePeerConnectionManager{connectErr: connectErr}
		reconciler := &peerConnectionReconciler{identity: identity, node: localNode, peers: peers, cursorStore: store}
		if err := reconciler.Reconcile(context.Background()); err == nil {
			t.Fatalf("pass %d: unavailable peers were not reported", pass+1)
		}
		calls, _ := peers.results()
		if len(calls) != window[1]-window[0] {
			t.Fatalf("pass %d planned %d peers, want %d", pass+1, len(calls), window[1]-window[0])
		}
		got := make(map[string]struct{}, len(calls))
		for _, call := range calls {
			got[call.NodeID] = struct{}{}
		}
		for expected := window[0]; expected < window[1]; expected++ {
			want := fmt.Sprintf("nd_%022d", expected)
			if _, ok := got[want]; !ok {
				t.Fatalf("pass %d did not plan Node %s; got %v", pass+1, want, got)
			}
		}
	}
}

func TestPeerConnectionCursorStoreReopensAndScopesProgress(t *testing.T) {
	root := filepath.Join(t.TempDir(), "peer-connection")
	store, err := openPeerConnectionCursorStore(root)
	if err != nil {
		t.Fatal(err)
	}
	identityID := "gi_AAAAAAAAAAAAAAAAAAAAAA"
	desiredSet := peerConnectionDesiredSetHash("nd_0000000000000000000000", map[string]struct{}{
		"nd_1111111111111111111111": {},
		"nd_2222222222222222222222": {},
	})
	last := "nd_2222222222222222222222"
	if err := store.SaveLastPlanned(identityID, desiredSet, last); err != nil {
		t.Fatal(err)
	}
	reopened, err := openPeerConnectionCursorStore(root)
	if err != nil {
		t.Fatal(err)
	}
	got, err := reopened.LastPlanned(identityID, desiredSet)
	if err != nil || got != last {
		t.Fatalf("reopened cursor=%q, err=%v; want %q", got, err, last)
	}
	if got, err := reopened.LastPlanned("gi_BBBBBBBBBBBBBBBBBBBBBB", desiredSet); err != nil || got != "" {
		t.Fatalf("different identity inherited cursor=%q, err=%v", got, err)
	}
	if got, err := reopened.LastPlanned(identityID, peerConnectionDesiredSetHash("nd_0000000000000000000000", map[string]struct{}{"nd_3333333333333333333333": {}})); err != nil || got != "" {
		t.Fatalf("different delegated set inherited cursor=%q, err=%v", got, err)
	}
}

func TestPeerConnectionCursorStoreFailsClosedOnMalformedState(t *testing.T) {
	root := filepath.Join(t.TempDir(), "peer-connection")
	store, err := openPeerConnectionCursorStore(root)
	if err != nil {
		t.Fatal(err)
	}
	identityID := "gi_AAAAAAAAAAAAAAAAAAAAAA"
	desiredSet := peerConnectionDesiredSetHash("nd_0000000000000000000000", map[string]struct{}{"nd_1111111111111111111111": {}})
	if err := store.SaveLastPlanned(identityID, desiredSet, "nd_1111111111111111111111"); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, peerConnectionCursorName)
	malformed := []byte(`{"state_version":"1","state_version":"1","identity_id":"gi_AAAAAAAAAAAAAAAAAAAAAA","desired_set_hash":"` + desiredSet + `","last_planned_node_id":"nd_1111111111111111111111"}` + "\n")
	if err := os.WriteFile(path, malformed, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.LastPlanned(identityID, desiredSet); err == nil {
		t.Fatal("duplicate-field cursor state was accepted")
	}
	if _, err := openPeerConnectionCursorStore(root); err == nil {
		t.Fatal("store open accepted malformed durable state")
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
