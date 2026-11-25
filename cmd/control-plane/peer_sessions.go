package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/integrated-recorder/core/internal/archivecatalog"
	"github.com/integrated-recorder/core/internal/authority"
	"github.com/integrated-recorder/core/internal/discovery"
	"github.com/integrated-recorder/core/internal/globalidentity"
	"github.com/integrated-recorder/core/internal/identityprotocol"
	"github.com/integrated-recorder/core/internal/lease"
	"github.com/integrated-recorder/core/internal/nodeidentity"
	"github.com/integrated-recorder/core/internal/peerapi"
	"github.com/integrated-recorder/core/internal/peerprotocol"
	"github.com/integrated-recorder/core/internal/peertransport"
	"github.com/integrated-recorder/core/internal/replication"
	"github.com/integrated-recorder/core/internal/signalingprotocol"
)

// newPeerSessionSlot wires the distributed read/replication transport only
// when its durable identity prerequisites exist. All state is private under
// /data, and a failure here remains isolated from local recording startup.
func newPeerSessionSlot(dataDir string, identity *globalidentity.Store, registry *globalidentity.HistoryRegistry, federation *discovery.FederationClient, authorityStore *authority.Store, catalog *archivecatalog.Store, nodeStore *nodeidentity.Store, leases *lease.Store, ciphertexts *replication.Store, turnAuth *peertransport.TURNAuthConfig) *peerapi.SessionManagerSlot {
	slot, err := peerapi.NewSessionManagerSlot(func(ctx context.Context) (*peerapi.SessionManager, error) {
		if ctx == nil || identity == nil || registry == nil || federation == nil || authorityStore == nil || catalog == nil || nodeStore == nil || leases == nil {
			return nil, peerapi.ErrSessionNetworkingUnavailable
		}
		local, initialized, err := identity.Snapshot()
		if err != nil || !initialized || local.IdentityID == "" {
			return nil, peerapi.ErrSessionNetworkingUnavailable
		}
		info, err := nodeStore.Info()
		if err != nil {
			return nil, peerapi.ErrSessionNetworkingUnavailable
		}
		publicKey, err := base64.RawURLEncoding.DecodeString(info.PublicKey)
		if err != nil {
			return nil, peerapi.ErrSessionNetworkingUnavailable
		}
		derivedNodeID, err := identityprotocol.NodeID(publicKey)
		if err != nil || derivedNodeID != info.NodeID {
			return nil, peerapi.ErrSessionNetworkingUnavailable
		}

		remoteResolver := authority.NewFederatedIdentityResolver(registry, federation)
		resolveAuthority := authority.IdentityStateResolverFromFederated(identity, remoteResolver)
		now := time.Now().UTC()
		localState, err := resolveAuthority(local.IdentityID, "", now)
		if err != nil || localState.Frozen || !equalPublicKey(localState.CurrentKeys[authority.PrincipalNode+":"+info.NodeID], publicKey) {
			// A generated Node Key cannot participate until the signed identity
			// history currently delegates it. Never enroll implicitly here.
			return nil, peerapi.ErrSessionNetworkingUnavailable
		}
		resolvePeer := peerprotocol.IdentityResolver(resolveAuthority)
		resolveSignal := func(identityID, headHash string, at time.Time) (signalingprotocol.IdentityState, error) {
			state, err := resolveAuthority(identityID, headHash, at)
			if err != nil {
				return signalingprotocol.IdentityState{}, err
			}
			return signalingprotocol.IdentityState{
				IdentityID: state.IdentityID, HeadHash: state.HeadHash, Frozen: state.Frozen,
				VerifiedAt: state.VerifiedAt, FreshUntil: state.FreshUntil, CurrentKeys: state.CurrentKeys,
			}, nil
		}
		if ciphertexts == nil {
			return nil, peerapi.ErrSessionNetworkingUnavailable
		}
		iceAuth := peertransport.TURNAuthConfig{}
		if turnAuth != nil {
			iceAuth = *turnAuth
		}
		authorizer := lease.FederatedAuthority{Store: authorityStore, Federation: federation, Resolve: resolveAuthority}
		service := &peerapi.Service{
			Catalog: catalog, ReplicaSnapshots: ciphertexts, Ciphertexts: ciphertexts, Leases: leases, Authorizer: authorizer,
			SessionKeys: controlPlaneSessionDEKProvider{store: ciphertexts, protector: nodeStore, nodeID: info.NodeID},
			IdentityID:  local.IdentityID, NodeID: info.NodeID, Resolve: resolvePeer, Signer: nodeStore,
			Replay: nodeidentity.PeerCommandReplayConsumer{NodeStore: nodeStore},
		}
		transport := peerapi.SignalingTransport{ResolveOptions: func() (peertransport.SignalingOptions, error) {
			at := time.Now().UTC()
			current, err := resolveAuthority(local.IdentityID, "", at)
			if err != nil || current.Frozen || current.HeadHash == "" || !equalPublicKey(current.CurrentKeys[authority.PrincipalNode+":"+info.NodeID], publicKey) {
				return peertransport.SignalingOptions{}, peerapi.ErrSessionNetworkingUnavailable
			}
			ice, err := iceAuth.ICE(info.NodeID, at)
			if err != nil {
				return peertransport.SignalingOptions{}, peerapi.ErrSessionNetworkingUnavailable
			}
			return peertransport.SignalingOptions{
				Broker: federation, SignalSigner: nodeStore,
				SignalReplay:           nodeidentity.PeerSignalReplayConsumer{NodeStore: nodeStore},
				SignalIdentityResolver: resolveSignal, HelloSigner: nodeStore,
				PeerIdentityResolver: resolvePeer, PeerReplay: nodeidentity.PeerReplayConsumer{NodeStore: nodeStore},
				IdentityID: local.IdentityID, IdentityHead: current.HeadHash, NodeID: info.NodeID,
				ICE: ice,
				AuthorizePeer: func(targetIdentityID, targetNodeID string, checkAt time.Time) error {
					return authorizePeerSession(local.IdentityID, info.NodeID, targetIdentityID, targetNodeID, resolveAuthority, authorityStore, checkAt)
				},
			}, nil
		}}
		manager, err := peerapi.NewSessionManager(transport, service)
		if err != nil {
			return nil, err
		}
		return manager, nil
	})
	if err != nil {
		return nil
	}
	return slot
}

const maxPeerTURNSecretBytes = 1024

// loadPeerTURNAuthConfig reads optional ICE configuration. TURN shared-secret
// material is accepted only from a private regular file; malformed settings
// disable peer networking but do not prevent local recording from starting.
func loadPeerTURNAuthConfig() (*peertransport.TURNAuthConfig, error) {
	if strings.TrimSpace(os.Getenv("IR_PEER_ICE_CONFIGURATION_INVALID")) != "" {
		return nil, peertransport.ErrConfig
	}
	turnURLs, err := parsePeerICEURLs(os.Getenv("IR_PEER_TURN_URLS"))
	if err != nil {
		return nil, peertransport.ErrConfig
	}
	stunURLs, err := parsePeerICEURLs(os.Getenv("IR_PEER_STUN_URLS"))
	if err != nil {
		return nil, peertransport.ErrConfig
	}
	secretPath := strings.TrimSpace(os.Getenv("IR_PEER_TURN_SHARED_SECRET_FILE"))
	relayOnly := false
	if raw := strings.TrimSpace(os.Getenv("IR_PEER_ICE_RELAY_ONLY")); raw != "" {
		relayOnly, err = strconv.ParseBool(raw)
		if err != nil {
			return nil, peertransport.ErrConfig
		}
	}
	ttl := time.Duration(0)
	if raw := strings.TrimSpace(os.Getenv("IR_PEER_TURN_CREDENTIAL_TTL")); raw != "" {
		ttl, err = time.ParseDuration(raw)
		if err != nil {
			return nil, peertransport.ErrConfig
		}
	}
	if len(turnURLs) == 0 && len(stunURLs) == 0 && secretPath == "" && !relayOnly && ttl == 0 {
		return nil, nil
	}
	var secret []byte
	if secretPath != "" {
		secret, err = readPrivatePeerTURNSecret(secretPath)
		if err != nil {
			return nil, peertransport.ErrConfig
		}
	}
	config, err := peertransport.NewTURNAuthConfig(turnURLs, stunURLs, secret, ttl, relayOnly)
	if err != nil {
		return nil, peertransport.ErrConfig
	}
	return config, nil
}

func parsePeerICEURLs(raw string) ([]string, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	parts := strings.Split(raw, ",")
	if len(parts) > 8 {
		return nil, peertransport.ErrConfig
	}
	values := make([]string, len(parts))
	for i, part := range parts {
		values[i] = strings.TrimSpace(part)
		if values[i] == "" {
			return nil, peertransport.ErrConfig
		}
	}
	return values, nil
}

func readPrivatePeerTURNSecret(path string) ([]byte, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, peertransport.ErrConfig
	}
	before, err := os.Lstat(path)
	if err != nil || !before.Mode().IsRegular() || before.Mode().Perm()&0o077 != 0 {
		return nil, peertransport.ErrConfig
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, peertransport.ErrConfig
	}
	defer file.Close()
	after, err := file.Stat()
	if err != nil || !after.Mode().IsRegular() || after.Mode().Perm()&0o077 != 0 || !os.SameFile(before, after) {
		return nil, peertransport.ErrConfig
	}
	data, err := io.ReadAll(io.LimitReader(file, maxPeerTURNSecretBytes+1))
	if err != nil || len(data) > maxPeerTURNSecretBytes {
		return nil, peertransport.ErrConfig
	}
	secret := bytes.TrimSpace(data)
	if len(secret) < 32 {
		return nil, peertransport.ErrConfig
	}
	return append([]byte(nil), secret...), nil
}

func openReplicaLeaseStore(dataDir string, identity *globalidentity.Store, registry *globalidentity.HistoryRegistry, federation *discovery.FederationClient, authorities *authority.Store) (*lease.Store, error) {
	if dataDir == "" || identity == nil || authorities == nil {
		return nil, errors.New("replica lease prerequisites are unavailable")
	}
	remote := authority.NewFederatedIdentityResolver(registry, federation)
	resolve := authority.IdentityStateResolverFromFederated(identity, remote)
	return lease.Open(lease.Options{
		Root:       filepath.Join(dataDir, "replication", "leases"),
		Authorizer: lease.FederatedAuthority{Store: authorities, Federation: federation, Resolve: resolve},
		Keys:       localLeaseKeyResolver{identity: identity, registry: registry},
	})
}

// authorizePeerSession preserves automatic same-identity node links and also
// permits an explicitly delegated cross-identity transport when a current,
// unforked OwnerSubject capability grants archive access to the other Node.
// The connection itself conveys no authority: every peer command still
// revalidates its exact current capability against the durable authority log.
func authorizePeerSession(localIdentityID, localNodeID, targetIdentityID, targetNodeID string, resolve authority.AuthorityIdentityResolver, authorities *authority.Store, now time.Time) error {
	if resolve == nil || now.IsZero() || localIdentityID == "" || localNodeID == "" ||
		targetIdentityID == "" || targetNodeID == "" || targetNodeID == localNodeID {
		return peerprotocol.ErrCommandDenied
	}
	local, err := resolve(localIdentityID, "", now)
	if err != nil || local.Frozen || local.IdentityID != localIdentityID || !peerIdentityHasNode(local, localNodeID) {
		return peerprotocol.ErrCommandDenied
	}
	target, err := resolve(targetIdentityID, "", now)
	if err != nil || target.Frozen || target.IdentityID != targetIdentityID || !peerIdentityHasNode(target, targetNodeID) {
		return peerprotocol.ErrCommandDenied
	}
	if targetIdentityID == localIdentityID {
		return nil
	}
	if authorities == nil {
		return peerprotocol.ErrCommandDenied
	}
	// The caller must either control a subject that grants the target Node
	// access, or have imported a subject controlled by that target identity
	// that grants this local Node access. This symmetric rule lets both peers
	// independently authorize the same connection without trusting Discovery
	// or TURN as an authority source.
	subjects, err := authorities.List(resolve, now)
	if err != nil {
		return peerprotocol.ErrCommandDenied
	}
	localNode := authority.Principal{Kind: authority.PrincipalNode, ID: localNodeID}
	targetNode := authority.Principal{Kind: authority.PrincipalNode, ID: targetNodeID}
	for _, subject := range subjects {
		if subject.Frozen {
			continue
		}
		var audience authority.Principal
		switch subject.Subject.Controller {
		case authority.Principal{Kind: authority.PrincipalIdentity, ID: localIdentityID}:
			audience = targetNode
		case authority.Principal{Kind: authority.PrincipalIdentity, ID: targetIdentityID}:
			audience = localNode
		default:
			continue
		}
		for _, chain := range subject.Grants {
			if len(chain) == 0 || chain[len(chain)-1].Grant.Audience != audience {
				continue
			}
			grant := chain[len(chain)-1].Grant
			for _, action := range []string{"archive.view", "archive.replicate", "archive.delete"} {
				if err := authorities.AuthorizeChain(subject.Subject.ID, chain, resolve, now, audience, grant.ArchiveID, grant.SnapshotID, action); err == nil {
					return nil
				}
			}
		}
	}
	return peerprotocol.ErrCommandDenied
}

func peerIdentityHasNode(state authority.IdentityState, nodeID string) bool {
	key := ed25519.PublicKey(state.CurrentKeys[authority.PrincipalNode+":"+nodeID])
	derived, err := identityprotocol.NodeID(key)
	return err == nil && derived == nodeID
}

func equalPublicKey(a, b []byte) bool {
	return len(a) == len(b) && len(a) > 0 && bytes.Equal(a, b)
}
