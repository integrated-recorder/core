package main

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/integrated-recorder/core/internal/authority"
	"github.com/integrated-recorder/core/internal/globalidentity"
	"github.com/integrated-recorder/core/internal/nodeidentity"
	"github.com/integrated-recorder/core/internal/peerapi"
)

const (
	peerConnectionReconcileInterval = 30 * time.Second
	peerConnectionDialTimeout       = 20 * time.Second
	peerConnectionWorkers           = 4
	peerConnectionBatch             = 4
)

type identitySnapshotReader interface {
	Snapshot() (globalidentity.Snapshot, bool, error)
}

type nodeInfoReader interface {
	Info() (nodeidentity.Info, error)
}

type peerConnectionManager interface {
	List() []peerapi.SessionInfo
	Connect(context.Context, string, string) error
	Disconnect(string) error
}

// peerConnectionReconciler maintains signaling sessions only with the Node
// Keys present in the current, unfrozen Global Identity history. A session
// grants no archive authority; each peer command still checks its current
// OwnerSubject capability. Reconciliation is bounded so a large identity or
// an offline peer cannot block Control lifecycle work indefinitely.
type peerConnectionReconciler struct {
	identity    identitySnapshotReader
	node        nodeInfoReader
	peers       peerConnectionManager
	resolve     authority.AuthorityIdentityResolver
	owners      *authority.Store
	cursorStore peerConnectionCursorStore

	mu     sync.Mutex
	cursor int // in-memory fallback for unit fixtures without a durable cursor store
}

func (r *peerConnectionReconciler) Reconcile(ctx context.Context) error {
	if r == nil || ctx == nil || r.identity == nil || r.node == nil || r.peers == nil {
		return errors.New("peer connection reconciliation is unavailable")
	}
	snapshot, initialized, err := r.identity.Snapshot()
	if err != nil {
		return errors.New("global identity state is unavailable")
	}
	if !initialized || snapshot.Frozen || snapshot.IdentityID == "" {
		return r.disconnectAll()
	}
	local, nodeErr := r.node.Info()
	if nodeErr != nil {
		return errors.New("local Node identity is unavailable")
	}

	desired := make(map[string]struct{}, len(snapshot.NodeIDs))
	for _, nodeID := range snapshot.NodeIDs {
		if nodeID != "" && nodeID != local.NodeID {
			desired[nodeID] = struct{}{}
		}
	}
	if !containsNodeID(snapshot.NodeIDs, local.NodeID) {
		// An undelegated local key must not initiate or retain peer sessions.
		desired = map[string]struct{}{}
	}

	connected := make(map[string]struct{})
	var errorMu sync.Mutex
	var cleanupErrors []error
	addError := func(err error) {
		if err == nil {
			return
		}
		errorMu.Lock()
		cleanupErrors = append(cleanupErrors, err)
		errorMu.Unlock()
	}
	joinedErrors := func() error {
		errorMu.Lock()
		defer errorMu.Unlock()
		return errors.Join(cleanupErrors...)
	}
	for _, session := range r.peers.List() {
		_, delegated := desired[session.NodeID]
		if delegated && session.IdentityID == snapshot.IdentityID {
			connected[session.NodeID] = struct{}{}
			continue
		}
		if session.IdentityID != snapshot.IdentityID && r.resolve != nil && r.owners != nil &&
			authorizePeerSession(snapshot.IdentityID, local.NodeID, session.IdentityID, session.NodeID, r.resolve, r.owners, time.Now().UTC()) == nil {
			// Cross-identity sessions are always explicit. Preserve an established
			// session only while the current signed OwnerSubject capability still
			// authorizes this pair; the session does not authorize any command.
			continue
		}
		if err := r.peers.Disconnect(session.NodeID); err != nil && !errors.Is(err, peerapi.ErrPeerNotConnected) {
			addError(err)
		}
	}
	if len(desired) == 0 {
		return joinedErrors()
	}

	targets := make([]string, 0, len(desired))
	for nodeID := range desired {
		// Each pair has one deterministic signaling initiator. This prevents
		// simultaneous outgoing negotiations from installing different sessions
		// at opposite ends; the higher Node ID waits for the lower Node to dial.
		if local.NodeID < nodeID {
			if _, ok := connected[nodeID]; !ok {
				targets = append(targets, nodeID)
			}
		}
	}
	sort.Strings(targets)
	if len(targets) == 0 {
		return joinedErrors()
	}
	if r.cursorStore != nil {
		desiredSetHash := peerConnectionDesiredSetHash(local.NodeID, desired)
		lastPlanned, err := r.cursorStore.LastPlanned(snapshot.IdentityID, desiredSetHash)
		if err != nil {
			return errors.New("peer connection scheduler state is unavailable")
		}
		start := peerConnectionCursorStart(targets, lastPlanned)
		end := start + peerConnectionBatch
		if end > len(targets) {
			end = len(targets)
		}
		planned := append([]string(nil), targets[start:end]...)
		// Persist every nonempty planned batch endpoint before any network
		// attempt, including a short tail. Otherwise the cursor can stall on
		// that tail forever. The cursor is scheduling metadata only; every
		// Connect still performs its normal authorization.
		if err := r.cursorStore.SaveLastPlanned(snapshot.IdentityID, desiredSetHash, planned[len(planned)-1]); err != nil {
			return errors.New("peer connection scheduler state could not be persisted")
		}
		targets = planned
	} else {
		start := r.nextCursor(len(targets))
		if len(targets) > peerConnectionBatch {
			rotated := append(append([]string(nil), targets[start:]...), targets[:start]...)
			targets = rotated[:peerConnectionBatch]
		} else if start > 0 {
			targets = append(append([]string(nil), targets[start:]...), targets[:start]...)
		}
	}

	var wait sync.WaitGroup
	for index := 0; index < len(targets); {
		if err := ctx.Err(); err != nil {
			addError(err)
			break
		}
		end := index + peerConnectionWorkers
		if end > len(targets) {
			end = len(targets)
		}
		for _, nodeID := range targets[index:end] {
			nodeID := nodeID
			wait.Add(1)
			go func() {
				defer wait.Done()
				dialCtx, cancel := context.WithTimeout(ctx, peerConnectionDialTimeout)
				defer cancel()
				if err := r.peers.Connect(dialCtx, snapshot.IdentityID, nodeID); err != nil && !errors.Is(err, peerapi.ErrPeerAlreadyConnected) {
					addError(err)
				}
			}()
		}
		index = end
	}
	wait.Wait()
	return joinedErrors()
}

func (r *peerConnectionReconciler) disconnectAll() error {
	var joined error
	for _, session := range r.peers.List() {
		if err := r.peers.Disconnect(session.NodeID); err != nil && !errors.Is(err, peerapi.ErrPeerNotConnected) {
			joined = errors.Join(joined, err)
		}
	}
	return joined
}

func (r *peerConnectionReconciler) nextCursor(targetCount int) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	if targetCount <= 0 {
		r.cursor = 0
		return 0
	}
	start := r.cursor % targetCount
	r.cursor = (start + peerConnectionBatch) % targetCount
	return start
}

func containsNodeID(nodeIDs []string, wanted string) bool {
	for _, nodeID := range nodeIDs {
		if nodeID == wanted {
			return true
		}
	}
	return false
}

func peerConnectionCursorStart(targets []string, lastPlanned string) int {
	if len(targets) == 0 || lastPlanned == "" {
		return 0
	}
	start := sort.SearchStrings(targets, lastPlanned)
	if start < len(targets) && targets[start] <= lastPlanned {
		start++
	}
	if start == len(targets) {
		return 0
	}
	return start
}
