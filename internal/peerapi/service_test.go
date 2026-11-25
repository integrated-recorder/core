package peerapi

import (
	"bytes"
	"context"
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/archivecatalog"
	"github.com/integrated-recorder/core/internal/archiveindex"
	"github.com/integrated-recorder/core/internal/archivesnapshot"
	"github.com/integrated-recorder/core/internal/authority"
	"github.com/integrated-recorder/core/internal/cryptoarchive"
	"github.com/integrated-recorder/core/internal/identityprotocol"
	"github.com/integrated-recorder/core/internal/lease"
	"github.com/integrated-recorder/core/internal/peerprotocol"
	"github.com/integrated-recorder/core/internal/peertransport"
	"github.com/integrated-recorder/core/internal/replication"
)

func TestResponseFramesBoundChunksAndDetectReorderingOrMutation(t *testing.T) {
	requestID, _, err := peerprotocol.NewCommand(bytes.NewReader(bytes.Repeat([]byte{0x23}, 48)))
	if err != nil {
		t.Fatal(err)
	}
	body := bytes.Repeat([]byte("ciphertext-range-"), peertransport.MaxDataFrame)
	frames, err := EncodeResponse(requestID, responseKindCiphertext, body)
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) < 2 {
		t.Fatalf("expected multiple bounded frames, got %d", len(frames))
	}
	for _, frame := range frames {
		if len(frame) > peertransport.MaxDataFrame {
			t.Fatalf("response frame size=%d exceeds limit %d", len(frame), peertransport.MaxDataFrame)
		}
	}
	session := &memorySession{received: frames}
	response, err := ReceiveResponse(context.Background(), session, requestID, MaxPeerResponseBytes)
	if err != nil || response.RequestID != requestID || response.Kind != responseKindCiphertext || !bytes.Equal(response.Body, body) {
		t.Fatalf("response round trip kind=%d bytes=%d err=%v", response.Kind, len(response.Body), err)
	}

	reordered := append([][]byte(nil), frames...)
	reordered[0], reordered[1] = reordered[1], reordered[0]
	if _, err := ReceiveResponse(context.Background(), &memorySession{received: reordered}, requestID, MaxPeerResponseBytes); !errors.Is(err, ErrResponseInvalid) {
		t.Fatalf("reordered chunks accepted: %v", err)
	}
	changed := append([][]byte(nil), frames...)
	changed[0] = append([]byte(nil), changed[0]...)
	changed[0][len(changed[0])-1] ^= 0x80
	if _, err := ReceiveResponse(context.Background(), &memorySession{received: changed}, requestID, MaxPeerResponseBytes); !errors.Is(err, ErrResponseInvalid) {
		t.Fatalf("corrupted response accepted: %v", err)
	}
	otherID, _, err := peerprotocol.NewCommand(bytes.NewReader(bytes.Repeat([]byte{0x24}, 48)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ReceiveResponse(context.Background(), &memorySession{received: frames}, otherID, MaxPeerResponseBytes); !errors.Is(err, ErrResponseInvalid) {
		t.Fatalf("cross-request frames accepted: %v", err)
	}
}

func TestClientRoundTripBindsResponseToSignedCommandAndRemoteNode(t *testing.T) {
	command := peerAPIClientCommand(t, "nd_bbbbbbbbbbbbbbbbbbbbbb")
	body := []byte(`{"ok":true}`)
	frames, err := EncodeResponse(command.RequestID, responseKindSnapshot, body)
	if err != nil {
		t.Fatal(err)
	}
	session := &memorySession{command: command, remote: authority.Principal{Kind: authority.PrincipalNode, ID: command.TargetNodeID}, received: frames}
	client, err := NewClient(session, 1024)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Do(context.Background(), command)
	if err != nil || response.Kind != responseKindSnapshot || !bytes.Equal(response.Body, body) {
		t.Fatalf("response=%q kind=%d err=%v", response.Body, response.Kind, err)
	}
	if session.sentCommand.RequestID != command.RequestID {
		t.Fatalf("sent command ID=%q want %q", session.sentCommand.RequestID, command.RequestID)
	}
}

func TestClientRejectsWrongTargetAndUncorrelatedRemoteError(t *testing.T) {
	command := peerAPIClientCommand(t, "nd_bbbbbbbbbbbbbbbbbbbbbb")
	session := &memorySession{command: command, remote: authority.Principal{Kind: authority.PrincipalNode, ID: "nd_cccccccccccccccccccccc"}}
	client, err := NewClient(session, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Do(context.Background(), command); !errors.Is(err, peerprotocol.ErrCommandDenied) || session.sentCommand.RequestID != "" {
		t.Fatalf("wrong target result err=%v sent=%q", err, session.sentCommand.RequestID)
	}

	remoteBody, _ := json.Marshal(errorPayload{Version: ResponseVersion, RequestID: "pc_BBBBBBBBBBBBBBBBBBBBBw", Code: "unavailable"})
	frames, err := EncodeResponse(command.RequestID, responseKindError, remoteBody)
	if err != nil {
		t.Fatal(err)
	}
	session.remote = authority.Principal{Kind: authority.PrincipalNode, ID: command.TargetNodeID}
	session.received = frames
	_, err = client.Do(context.Background(), command)
	if !errors.Is(err, ErrResponseInvalid) {
		t.Fatalf("uncorrelated remote error accepted: %v", err)
	}
}

func TestReplicaOfferPagingCrossesDurableReceiverValidation(t *testing.T) {
	ctx := context.Background()
	ownerPublic, ownerPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	identityID, err := identityprotocol.IdentityID(ownerPublic)
	if err != nil {
		t.Fatal(err)
	}
	serverPrivate := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x44}, ed25519.SeedSize))
	providerPrivate := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x55}, ed25519.SeedSize))
	serverNodeID, err := identityprotocol.NodeID(serverPrivate.Public().(ed25519.PublicKey))
	if err != nil {
		t.Fatal(err)
	}
	providerNodeID, err := identityprotocol.NodeID(providerPrivate.Public().(ed25519.PublicKey))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	ownerSubjectID := "os_" + strings.Repeat("Q", 22)
	archiveID := "arc_" + strings.Repeat("R", 22)
	snapshotID := "snap_" + strings.Repeat("a", 64)
	head := "sha256:" + strings.Repeat("b", 64)
	identityKeyID, err := identityprotocol.KeyID(ownerPublic)
	if err != nil {
		t.Fatal(err)
	}
	grantFor := func(audience string, action string, fill byte) authority.SignedGrant {
		grant, signErr := authority.SignGrant(authority.Grant{
			Version: authority.ProfileVersion, SubjectID: ownerSubjectID,
			Issuer:    authority.Principal{Kind: authority.PrincipalIdentity, ID: identityID},
			Audience:  authority.Principal{Kind: authority.PrincipalNode, ID: audience},
			ArchiveID: archiveID, SnapshotID: snapshotID, Actions: []string{action},
			NotBefore: now.Add(-time.Minute).Format(time.RFC3339), ExpiresAt: now.Add(time.Hour).Format(time.RFC3339),
			DelegationDepth: "0", Nonce: base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{fill}, 32)),
			PolicyEpoch: "1", IssuerKeyID: identityKeyID,
		}, ownerPrivate)
		if signErr != nil {
			t.Fatal(signErr)
		}
		return grant
	}
	offerGrant := grantFor(serverNodeID, "archive.replicate", 0x61)
	commandGrant := grantFor(providerNodeID, "archive.replicate", 0x62)
	objects := make([]lease.EncryptedObject, MaxOfferPageObjects+1)
	for i := range objects {
		objects[i] = lease.EncryptedObject{
			ObjectID: fmt.Sprintf("ob_%020d", i+1), CiphertextSize: "1",
			CiphertextSHA256: "sha256:" + strings.Repeat("c", 64),
		}
	}
	offer := lease.Offer{
		Version: lease.ProtocolVersion, LeaseID: "rl_" + strings.Repeat("D", 22),
		ArchiveID: archiveID, SnapshotID: snapshotID, KeyEpoch: "1",
		PlaintextManifestHash: "sha256:" + strings.Repeat("d", 64), ReplicaManifestHash: "sha256:" + strings.Repeat("e", 64),
		EncryptedObjectSet: objects, ProviderNodeID: providerNodeID, OwnerSubjectID: ownerSubjectID,
		ReservedBytes: fmt.Sprint(len(objects)), IssuedAt: now.Format(time.RFC3339),
		ExpiresAt: now.Add(time.Hour).Format(time.RFC3339), RenewalPolicy: "manual",
	}
	signed, err := lease.SignOffer(offer, authority.Principal{Kind: authority.PrincipalNode, ID: serverNodeID}, []authority.SignedGrant{offerGrant}, serverPrivate)
	if err != nil {
		t.Fatalf("sign test offer: %v", err)
	}
	offerHash, err := lease.OfferHash(signed.Offer)
	if err != nil {
		t.Fatal(err)
	}
	keys := peerAPITestKeys{
		authority.PrincipalNode + ":" + serverNodeID:   serverPrivate.Public().(ed25519.PublicKey),
		authority.PrincipalNode + ":" + providerNodeID: providerPrivate.Public().(ed25519.PublicKey),
	}
	openStore := func(name string) *lease.Store {
		store, openErr := lease.Open(lease.Options{
			Root: filepath.Join(t.TempDir(), name), Authorizer: peerOfferTestAuthorizer{}, Keys: keys,
			Now: func() time.Time { return now },
		})
		if openErr != nil {
			t.Fatal(openErr)
		}
		return store
	}
	sourceStore, destinationStore := openStore("source-leases"), openStore("provider-leases")
	if _, err := sourceStore.IssueOffer(ctx, signed); err != nil {
		t.Fatalf("persist source offer before delivery: %v", err)
	}
	authorizer := &peerAPIAuthProbe{}
	service := &Service{
		Leases: sourceStore, Authorizer: authorizer, IdentityID: identityID, NodeID: serverNodeID,
		Now: func() time.Time { return now },
	}
	session := &offerLoopbackSession{
		remote:    authority.Principal{Kind: authority.PrincipalNode, ID: serverNodeID},
		actor:     authority.Principal{Kind: authority.PrincipalNode, ID: providerNodeID},
		sessionID: "ps_aaaaaaaaaaaaaaaaaaaaaa", service: service, dropAcceptanceResponse: true,
	}
	client, err := NewScopedClient(session, MaxPeerResponseBytes, RequestScope{
		IdentityID: identityID, IdentityHead: head, NodeID: providerNodeID,
		TargetIdentityID: identityID, TargetNodeID: serverNodeID, OwnerSubjectID: ownerSubjectID,
		ArchiveID: archiveID, SnapshotID: snapshotID, Action: "archive.replicate",
		GrantChain: []authority.SignedGrant{commandGrant}, Resolve: func(string, string, time.Time) (authority.IdentityState, error) {
			return authority.IdentityState{}, nil
		}, Signer: peerAPIClientSigner{privateKey: providerPrivate}, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("create replica client: %v", err)
	}
	inbox, err := client.ReplicaOffers(ctx)
	if err != nil || len(inbox) != 1 || inbox[0].LeaseID != offer.LeaseID || inbox[0].OfferHash != offerHash || inbox[0].ObjectCount != len(objects) {
		t.Fatalf("provider inbox=%+v err=%v", inbox, err)
	}
	signer := peerOfferAcceptanceSigner{privateKey: providerPrivate}
	received, err := client.AcceptReplicaOffer(ctx, destinationStore, signer, archiveID, snapshotID, offer.LeaseID)
	if err == nil || received.Status != lease.StatusAccepted || len(received.Offer.Offer.EncryptedObjectSet) != len(objects) {
		t.Fatalf("lost acceptance response should preserve local accepted lease: status=%q objects=%d err=%v", received.Status, len(received.Offer.Offer.EncryptedObjectSet), err)
	}
	session.dropAcceptanceResponse = false
	received, err = client.AcceptReplicaOffer(ctx, destinationStore, signer, archiveID, snapshotID, offer.LeaseID)
	if err != nil || received.Status != lease.StatusTransferring || len(received.Offer.Offer.EncryptedObjectSet) != len(objects) {
		t.Fatalf("retry durable acceptance status=%q objects=%d err=%v", received.Status, len(received.Offer.Offer.EncryptedObjectSet), err)
	}
	if session.commands != 6 {
		t.Fatalf("inbox, offer pages, acceptance retries, and transfer begin requests=%d, want inbox, two pages, two receipts, and one begin", session.commands)
	}
	if received.OfferHash != offerHash || received.Offer.Signature != signed.Signature {
		t.Fatal("delivered offer identity or original signature changed")
	}
	sourceAccepted, err := sourceStore.Get(ctx, offer.LeaseID)
	if err != nil || sourceAccepted.Status != lease.StatusTransferring || sourceAccepted.Acceptance.Acceptance.ProviderNodeID != providerNodeID {
		t.Fatalf("source acceptance status=%q err=%v", sourceAccepted.Status, err)
	}
	providerAccepted, err := destinationStore.Get(ctx, offer.LeaseID)
	if err != nil || providerAccepted.Status != lease.StatusTransferring || providerAccepted.OfferHash != sourceAccepted.OfferHash {
		t.Fatalf("provider/source transfer state diverged: provider=%q source=%q err=%v", providerAccepted.Status, sourceAccepted.Status, err)
	}
	checkpoint, err := lease.SignCheckpoint(lease.Checkpoint{
		Version: lease.ProtocolVersion, LeaseID: offer.LeaseID, OfferHash: offerHash,
		ObjectID: objects[0].ObjectID, CommittedOffset: "1", ProviderNodeID: providerNodeID,
		RecordedAt: now.Add(time.Second).Format(time.RFC3339),
	}, providerPrivate)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := destinationStore.Checkpoint(ctx, checkpoint); err != nil {
		t.Fatalf("persist provider checkpoint before forwarding: %v", err)
	}
	if err := client.SubmitReplicaCheckpoint(ctx, archiveID, snapshotID, offer.LeaseID, checkpoint); err != nil {
		t.Fatalf("forward signed durable checkpoint to source: %v", err)
	}
	sourceProgress, err := sourceStore.Get(ctx, offer.LeaseID)
	if err != nil || len(sourceProgress.Progress) != len(objects) || sourceProgress.Progress[0].ObjectID != objects[0].ObjectID || sourceProgress.Progress[0].CommittedOffset != "1" {
		t.Fatalf("source did not persist the exact provider checkpoint: progress=%+v err=%v", sourceProgress.Progress, err)
	}
	forged := checkpoint
	forged.Checkpoint.CommittedOffset = "0"
	if err := client.SubmitReplicaCheckpoint(ctx, archiveID, snapshotID, offer.LeaseID, forged); err == nil {
		t.Fatal("source accepted a checkpoint whose signature no longer matches its body")
	}
	sourceAfterForgery, err := sourceStore.Get(ctx, offer.LeaseID)
	if err != nil || sourceAfterForgery.Progress[0].CommittedOffset != "1" {
		t.Fatalf("forged checkpoint changed source progress: progress=%+v err=%v", sourceAfterForgery.Progress, err)
	}

	// Even a self-consistent page hash cannot replace the original signed
	// object set: the final durable receiver boundary checks the signature.
	tampered := signed
	tampered.Offer.EncryptedObjectSet = append([]lease.EncryptedObject(nil), signed.Offer.EncryptedObjectSet...)
	tampered.Offer.EncryptedObjectSet[0].CiphertextSHA256 = "sha256:" + strings.Repeat("f", 64)
	tamperedHash, err := lease.OfferHash(tampered.Offer)
	if err != nil {
		t.Fatal(err)
	}
	tamperedStore := &peerOfferReaderFixture{signed: tampered, hash: tamperedHash}
	tamperedService := &Service{Leases: tamperedStore, Authorizer: &peerAPIAuthProbe{}, IdentityID: identityID, NodeID: serverNodeID, Now: func() time.Time { return now }}
	tamperedSession := &offerLoopbackSession{
		remote:    authority.Principal{Kind: authority.PrincipalNode, ID: serverNodeID},
		actor:     authority.Principal{Kind: authority.PrincipalNode, ID: providerNodeID},
		sessionID: "ps_bbbbbbbbbbbbbbbbbbbbbb", service: tamperedService,
	}
	tamperedClient, err := NewScopedClient(tamperedSession, MaxPeerResponseBytes, RequestScope{
		IdentityID: identityID, IdentityHead: head, NodeID: providerNodeID,
		TargetIdentityID: identityID, TargetNodeID: serverNodeID, OwnerSubjectID: ownerSubjectID,
		ArchiveID: archiveID, SnapshotID: snapshotID, Action: "archive.replicate", GrantChain: []authority.SignedGrant{commandGrant},
		Resolve: func(string, string, time.Time) (authority.IdentityState, error) {
			return authority.IdentityState{}, nil
		},
		Signer: peerAPIClientSigner{privateKey: providerPrivate}, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tamperedClient.ReceiveReplicaOffer(ctx, openStore("tampered-provider"), archiveID, snapshotID, offer.LeaseID); !errors.Is(err, lease.ErrDenied) {
		t.Fatalf("tampered signed offer crossed receiving Store validation: %v", err)
	}
}

type peerOfferTestAuthorizer struct{}

type peerOfferAcceptanceSigner struct{ privateKey ed25519.PrivateKey }

func (s peerOfferAcceptanceSigner) SignReplicaAcceptance(value lease.Acceptance) (lease.SignedAcceptance, error) {
	return lease.SignAcceptance(value, s.privateKey)
}

func (peerOfferTestAuthorizer) Authorize(_ context.Context, subjectID, archiveID, snapshotID, action string, actor authority.Principal, chain []authority.SignedGrant, _ time.Time) error {
	if subjectID == "" || archiveID == "" || snapshotID == "" || action != "archive.replicate" || actor.ID == "" || len(chain) != 1 {
		return authority.ErrDenied
	}
	return nil
}

type offerLoopbackSession struct {
	remote                    authority.Principal
	actor                     authority.Principal
	sessionID                 string
	service                   *Service
	command                   peerprotocol.Command
	frames                    [][]byte
	commands                  int
	dropAcceptanceResponse    bool
	acceptanceResponseDropped bool
	wrongTombstoneReceipt     bool
}

func (s *offerLoopbackSession) Remote() authority.Principal { return s.remote }
func (s *offerLoopbackSession) SessionID() string           { return s.sessionID }
func (s *offerLoopbackSession) SendCommand(_ context.Context, command peerprotocol.Command) error {
	s.command = command
	s.frames = nil
	s.commands++
	return nil
}
func (s *offerLoopbackSession) Receive(ctx context.Context) ([]byte, error) {
	if len(s.frames) == 0 {
		kind, body, err := s.service.execute(ctx, s.command, s.actor)
		if err != nil {
			kind = responseKindError
			body, _ = json.Marshal(errorPayload{Version: ResponseVersion, RequestID: s.command.RequestID, Code: publicErrorCode(err)})
		}
		if s.wrongTombstoneReceipt && kind == responseKindTombstone {
			var receipt tombstoneReceiptPayload
			if json.Unmarshal(body, &receipt) == nil {
				receipt.TombstoneHash = "sha256:" + strings.Repeat("0", 64)
				body, _ = json.Marshal(receipt)
			}
		}
		frames, frameErr := EncodeResponse(s.command.RequestID, kind, body)
		if frameErr != nil {
			return nil, frameErr
		}
		if s.dropAcceptanceResponse && !s.acceptanceResponseDropped {
			var request requestPayload
			if json.Unmarshal(s.command.Payload, &request) == nil && request.Operation == "replica.acceptance" {
				s.acceptanceResponseDropped = true
				return nil, io.ErrUnexpectedEOF
			}
		}
		s.frames = frames
	}
	frame := s.frames[0]
	s.frames = s.frames[1:]
	return frame, nil
}

type peerOfferReaderFixture struct {
	signed lease.SignedOffer
	hash   string
}

func (r *peerOfferReaderFixture) OfferForProvider(_ context.Context, leaseID, providerID string) (lease.SignedOffer, string, error) {
	if leaseID != r.signed.Offer.LeaseID || providerID != r.signed.Offer.ProviderNodeID {
		return lease.SignedOffer{}, "", lease.ErrDenied
	}
	return r.signed, r.hash, nil
}

func peerAPIClientCommand(t *testing.T, targetNodeID string) peerprotocol.Command {
	t.Helper()
	requestID, nonce, err := peerprotocol.NewCommand(bytes.NewReader(bytes.Repeat([]byte{0x31}, 48)))
	if err != nil {
		t.Fatal(err)
	}
	payload := json.RawMessage(`{"operation":"snapshot.head"}`)
	payloadHash, err := peerprotocol.PayloadDigest(payload)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	privateKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x71}, ed25519.SeedSize))
	grant, err := authority.SignGrant(authority.Grant{
		Version: authority.ProfileVersion, SubjectID: "os_dddddddddddddddddddddd",
		Issuer:    authority.Principal{Kind: authority.PrincipalIdentity, ID: "gi_aaaaaaaaaaaaaaaaaaaaaa"},
		Audience:  authority.Principal{Kind: authority.PrincipalNode, ID: targetNodeID},
		ArchiveID: "arc_eeeeeeeeeeeeeeeeeeeeee", Actions: []string{"archive.view"},
		NotBefore: now.Add(-time.Minute).Format(time.RFC3339), ExpiresAt: now.Add(time.Minute).Format(time.RFC3339),
		DelegationDepth: "0", Nonce: base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x72}, 32)),
		PolicyEpoch: "0", IssuerKeyID: "ik_" + strings.Repeat("A", 43),
	}, privateKey)
	if err != nil {
		t.Fatalf("sign test grant: %v", err)
	}
	return peerprotocol.Command{
		Version: peerprotocol.CommandVersion, RequestID: requestID, SessionID: "ps_aaaaaaaaaaaaaaaaaaaaaa",
		SourceIdentityID: "gi_aaaaaaaaaaaaaaaaaaaaaa", SourceHeadHash: "sha256:" + strings.Repeat("a", 64),
		SourceNodeID: "nd_cccccccccccccccccccccc", TargetIdentityID: "gi_bbbbbbbbbbbbbbbbbbbbbb", TargetNodeID: targetNodeID,
		OwnerSubjectID: "os_dddddddddddddddddddddd", ArchiveID: "arc_eeeeeeeeeeeeeeeeeeeeee",
		Action: "archive.view", IssuedAt: now.Format(time.RFC3339), ExpiresAt: now.Add(time.Minute).Format(time.RFC3339),
		Nonce: nonce, PayloadHash: payloadHash, GrantChain: []authority.SignedGrant{grant}, Payload: payload,
		Signature: base64.RawURLEncoding.EncodeToString(make([]byte, 64)),
	}
}

func TestServeOneRechecksCapabilityAndReturnsOnlyVerifiedSnapshot(t *testing.T) {
	snapshot := peerAPITestSnapshot(t)
	requestID, nonce, err := peerprotocol.NewCommand(bytes.NewReader(bytes.Repeat([]byte{0x42}, 48)))
	if err != nil {
		t.Fatal(err)
	}
	payload := json.RawMessage(`{"operation":"snapshot.get"}`)
	payloadHash, err := peerprotocol.PayloadDigest(payload)
	if err != nil {
		t.Fatal(err)
	}
	command := peerprotocol.Command{
		Version: peerprotocol.CommandVersion, RequestID: requestID, SessionID: "ps_aaaaaaaaaaaaaaaaaaaaaa",
		SourceIdentityID: "gi_aaaaaaaaaaaaaaaaaaaaaa", SourceHeadHash: "sha256:" + string(bytes.Repeat([]byte{'a'}, 64)),
		SourceNodeID: "nd_bbbbbbbbbbbbbbbbbbbbbb", TargetIdentityID: "gi_aaaaaaaaaaaaaaaaaaaaaa", TargetNodeID: "nd_cccccccccccccccccccccc",
		OwnerSubjectID: "os_dddddddddddddddddddddd", ArchiveID: snapshot.Content.ArchiveID, SnapshotID: snapshot.SnapshotID,
		Action: "archive.view", IssuedAt: time.Now().UTC().Truncate(time.Second).Format(time.RFC3339),
		ExpiresAt: time.Now().UTC().Truncate(time.Second).Add(time.Minute).Format(time.RFC3339),
		Nonce:     nonce, PayloadHash: payloadHash, GrantChain: []authority.SignedGrant{{}}, Payload: payload,
	}
	authorizer := &peerAPIAuthProbe{}
	replay := &peerAPIReplayProbe{}
	resolver := func(string, string, time.Time) (authority.IdentityState, error) {
		return authority.IdentityState{}, nil
	}
	session := &memorySession{command: command, remote: authority.Principal{Kind: authority.PrincipalNode, ID: command.SourceNodeID}}
	service := Service{
		Catalog: snapshotCatalog{snapshot: snapshot}, Authorizer: authorizer,
		IdentityID: command.TargetIdentityID, NodeID: command.TargetNodeID,
		Resolve: resolver, Replay: replay, Now: func() time.Time { return time.Now().UTC() },
	}
	if err := service.ServeOne(context.Background(), session); err != nil {
		t.Fatalf("serve snapshot request: %v", err)
	}
	if authorizer.calls.Load() != 2 || replay.calls != 1 {
		t.Fatalf("authorization calls=%d replay consumes=%d, want 2 and 1", authorizer.calls.Load(), replay.calls)
	}
	response, err := ReceiveResponse(context.Background(), &memorySession{received: session.sent}, requestID, MaxPeerResponseBytes)
	if err != nil || response.Kind != responseKindSnapshot {
		t.Fatalf("snapshot response kind=%d err=%v", response.Kind, err)
	}
	decoded, err := archivesnapshot.Parse(response.Body)
	if err != nil || archivesnapshot.Verify(decoded) != nil || decoded.SnapshotID != snapshot.SnapshotID {
		t.Fatalf("returned snapshot identity=%q err=%v", decoded.SnapshotID, err)
	}
	if bytes.Contains(response.Body, []byte("recordings/")) {
		t.Fatal("peer snapshot response exposed a local archive path")
	}
}

func TestScopedClientFetchesSnapshotHeadAndExactSnapshot(t *testing.T) {
	snapshot := peerAPITestSnapshot(t)
	encoded, err := archivesnapshot.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	remoteNodeID := "nd_cccccccccccccccccccccc"
	session := &snapshotClientSession{memorySession: memorySession{
		remote: authority.Principal{Kind: authority.PrincipalNode, ID: remoteNodeID}, sessionID: "ps_aaaaaaaaaaaaaaaaaaaaaa",
	}, snapshot: encoded}
	privateKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x29}, ed25519.SeedSize))
	sourceNodeID, err := identityprotocol.NodeID(privateKey.Public().(ed25519.PublicKey))
	if err != nil {
		t.Fatal(err)
	}
	grantChain := peerAPIClientCommand(t, remoteNodeID).GrantChain
	client, err := NewScopedClient(session, MaxPeerResponseBytes, RequestScope{
		IdentityID: "gi_aaaaaaaaaaaaaaaaaaaaaa", IdentityHead: "sha256:" + strings.Repeat("a", 64),
		NodeID: sourceNodeID, TargetIdentityID: "gi_aaaaaaaaaaaaaaaaaaaaaa", TargetNodeID: remoteNodeID,
		OwnerSubjectID: "os_dddddddddddddddddddddd", ArchiveID: snapshot.Content.ArchiveID,
		Action: "archive.view", GrantChain: grantChain,
		Resolve: func(string, string, time.Time) (authority.IdentityState, error) {
			return authority.IdentityState{}, nil
		},
		Signer: peerAPIClientSigner{privateKey: privateKey}, Now: func() time.Time { return time.Now().UTC() },
	})
	if err != nil {
		t.Fatalf("create archive-wide scoped client: %v", err)
	}
	head, err := client.HeadSnapshot(context.Background())
	if err != nil || head.SnapshotID != snapshot.SnapshotID || session.sentCommand.SnapshotID != "" {
		t.Fatalf("snapshot head=%+v command snapshot=%q err=%v", head, session.sentCommand.SnapshotID, err)
	}
	got, err := client.GetSnapshot(context.Background(), snapshot.SnapshotID)
	if err != nil || got.SnapshotID != snapshot.SnapshotID || session.sentCommand.SnapshotID != snapshot.SnapshotID {
		t.Fatalf("snapshot=%+v command snapshot=%q err=%v", got, session.sentCommand.SnapshotID, err)
	}
	pinned, err := NewScopedClient(session, MaxPeerResponseBytes, RequestScope{
		IdentityID: "gi_aaaaaaaaaaaaaaaaaaaaaa", IdentityHead: "sha256:" + strings.Repeat("a", 64), NodeID: sourceNodeID,
		TargetIdentityID: "gi_aaaaaaaaaaaaaaaaaaaaaa", TargetNodeID: remoteNodeID, OwnerSubjectID: "os_dddddddddddddddddddddd",
		ArchiveID: snapshot.Content.ArchiveID, SnapshotID: snapshot.SnapshotID, Action: "archive.view", GrantChain: grantChain,
		Resolve: func(string, string, time.Time) (authority.IdentityState, error) {
			return authority.IdentityState{}, nil
		},
		Signer: peerAPIClientSigner{privateKey: privateKey},
	})
	if err != nil {
		t.Fatalf("create snapshot-pinned client: %v", err)
	}
	if _, err := pinned.GetSnapshot(context.Background(), "snap_"+strings.Repeat("b", 64)); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("snapshot-pinned client widened scope: %v", err)
	}
}

type peerAPIClientSigner struct{ privateKey ed25519.PrivateKey }

func (s peerAPIClientSigner) SignPeerCommand(command peerprotocol.Command, _ peerprotocol.IdentityResolver, _ time.Time) (peerprotocol.Command, error) {
	return peerprotocol.SignCommand(command, s.privateKey)
}

type snapshotClientSession struct {
	memorySession
	snapshot []byte
}

func (s *snapshotClientSession) SendCommand(_ context.Context, command peerprotocol.Command) error {
	s.sentCommand = command
	frames, err := EncodeResponse(command.RequestID, responseKindSnapshot, s.snapshot)
	if err != nil {
		return err
	}
	s.received = frames
	return nil
}

func TestServeOneFailsClosedWhenCapabilityChangesBeforeArchiveRead(t *testing.T) {
	snapshot := peerAPITestSnapshot(t)
	requestID, nonce, _ := peerprotocol.NewCommand(bytes.NewReader(bytes.Repeat([]byte{0x55}, 48)))
	payload := json.RawMessage(`{"operation":"snapshot.get"}`)
	payloadHash, _ := peerprotocol.PayloadDigest(payload)
	now := time.Now().UTC().Truncate(time.Second)
	command := peerprotocol.Command{
		Version: peerprotocol.CommandVersion, RequestID: requestID, SessionID: "ps_aaaaaaaaaaaaaaaaaaaaaa",
		SourceIdentityID: "gi_aaaaaaaaaaaaaaaaaaaaaa", SourceHeadHash: "sha256:" + string(bytes.Repeat([]byte{'a'}, 64)),
		SourceNodeID: "nd_bbbbbbbbbbbbbbbbbbbbbb", TargetIdentityID: "gi_aaaaaaaaaaaaaaaaaaaaaa", TargetNodeID: "nd_cccccccccccccccccccccc",
		OwnerSubjectID: "os_dddddddddddddddddddddd", ArchiveID: snapshot.Content.ArchiveID, SnapshotID: snapshot.SnapshotID,
		Action: "archive.view", IssuedAt: now.Format(time.RFC3339), ExpiresAt: now.Add(time.Minute).Format(time.RFC3339),
		Nonce: nonce, PayloadHash: payloadHash, GrantChain: []authority.SignedGrant{{}}, Payload: payload,
	}
	authorizer := &peerAPIAuthProbe{denyAt: 2}
	session := &memorySession{command: command, remote: authority.Principal{Kind: authority.PrincipalNode, ID: command.SourceNodeID}}
	service := Service{Catalog: snapshotCatalog{snapshot: snapshot}, Authorizer: authorizer,
		IdentityID: command.TargetIdentityID, NodeID: command.TargetNodeID,
		Resolve: func(string, string, time.Time) (authority.IdentityState, error) {
			return authority.IdentityState{}, nil
		},
		Replay: &peerAPIReplayProbe{}, Now: func() time.Time { return now }}
	if err := service.ServeOne(context.Background(), session); err != nil {
		t.Fatalf("safe denial response failed: %v", err)
	}
	if authorizer.calls.Load() != 2 {
		t.Fatalf("authorizer calls=%d, want initial and pre-read checks", authorizer.calls.Load())
	}
	response, err := ReceiveResponse(context.Background(), &memorySession{received: session.sent}, requestID, 1024)
	if err != nil || response.Kind != responseKindError {
		t.Fatalf("denial response kind=%d err=%v", response.Kind, err)
	}
	var payloadResponse errorPayload
	if json.Unmarshal(response.Body, &payloadResponse) != nil || payloadResponse.Code != "permission_denied" {
		t.Fatalf("unsafe or unexpected public error body=%q", response.Body)
	}
}

func TestPeerServiceLoadsPersistedReplicaSnapshotOnlyAfterCanonicalMiss(t *testing.T) {
	snapshot := peerAPITestSnapshot(t)
	store, err := replication.Open(filepath.Join(t.TempDir(), "replica"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PersistSnapshot(snapshot); err != nil {
		t.Fatal(err)
	}
	service := Service{Catalog: missingSnapshotCatalog{err: archivecatalog.ErrNotFound}, ReplicaSnapshots: store}
	loaded, err := service.snapshot(context.Background(), snapshot.Content.ArchiveID, snapshot.SnapshotID)
	if err != nil || loaded.SnapshotID != snapshot.SnapshotID {
		t.Fatalf("replica snapshot fallback failed: id=%q err=%v", loaded.SnapshotID, err)
	}

	service.Catalog = missingSnapshotCatalog{err: archivecatalog.ErrUnavailable}
	if _, err := service.snapshot(context.Background(), snapshot.Content.ArchiveID, snapshot.SnapshotID); !errors.Is(err, archivecatalog.ErrUnavailable) {
		t.Fatalf("catalog corruption/unavailability was hidden by replica fallback: %v", err)
	}
}

func TestServeOneReturnsOnlyRequestedCiphertextRange(t *testing.T) {
	snapshot := peerAPITestSnapshot(t)
	requestID, nonce, err := peerprotocol.NewCommand(bytes.NewReader(bytes.Repeat([]byte{0x33}, 48)))
	if err != nil {
		t.Fatal(err)
	}
	objectID := "ob_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	payload := json.RawMessage(`{"operation":"ciphertext.range","object_id":"` + objectID + `","key_epoch":"1","offset":"3","length":"5"}`)
	payloadHash, err := peerprotocol.PayloadDigest(payload)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	command := peerprotocol.Command{
		Version: peerprotocol.CommandVersion, RequestID: requestID, SessionID: "ps_aaaaaaaaaaaaaaaaaaaaaa",
		SourceIdentityID: "gi_aaaaaaaaaaaaaaaaaaaaaa", SourceHeadHash: "sha256:" + strings.Repeat("a", 64),
		SourceNodeID: "nd_bbbbbbbbbbbbbbbbbbba", TargetIdentityID: "gi_cccccccccccccccccccccc", TargetNodeID: "nd_dddddddddddddddddddddd",
		OwnerSubjectID: "os_eeeeeeeeeeeeeeeeeeee", ArchiveID: snapshot.Content.ArchiveID, SnapshotID: snapshot.SnapshotID,
		Action: "archive.view", IssuedAt: now.Format(time.RFC3339), ExpiresAt: now.Add(time.Minute).Format(time.RFC3339),
		Nonce: nonce, PayloadHash: payloadHash, GrantChain: []authority.SignedGrant{{}}, Payload: payload,
	}
	ciphertexts := &peerAPICiphertexts{data: []byte("opaque-ciphertext-bytes"), objectID: objectID}
	authorizer := &peerAPIAuthProbe{}
	session := &memorySession{command: command, remote: authority.Principal{Kind: authority.PrincipalNode, ID: command.SourceNodeID}}
	service := Service{
		Catalog: snapshotCatalog{snapshot: snapshot}, Ciphertexts: ciphertexts, Authorizer: authorizer,
		IdentityID: command.TargetIdentityID, NodeID: command.TargetNodeID,
		Resolve: func(string, string, time.Time) (authority.IdentityState, error) {
			return authority.IdentityState{}, nil
		},
		Replay: &peerAPIReplayProbe{}, Now: func() time.Time { return now },
	}
	if err := service.ServeOne(context.Background(), session); err != nil {
		t.Fatalf("serve ciphertext range: %v", err)
	}
	response, err := ReceiveResponse(context.Background(), &memorySession{received: session.sent}, requestID, 1024)
	if err != nil || response.Kind != responseKindCiphertext || string(response.Body) != "que-c" {
		t.Fatalf("ciphertext response=%q kind=%d err=%v", response.Body, response.Kind, err)
	}
	if ciphertexts.bundleCalls != 1 || ciphertexts.rangeCalls != 1 || ciphertexts.offset != 3 || ciphertexts.length != 5 {
		t.Fatalf("bundle=%d range=%d offset=%d length=%d", ciphertexts.bundleCalls, ciphertexts.rangeCalls, ciphertexts.offset, ciphertexts.length)
	}
}

func TestServeOneReturnsValidatedPublicReplicaManifestForReplicateCapability(t *testing.T) {
	snapshot := peerAPITestSnapshot(t)
	requestID, nonce, err := peerprotocol.NewCommand(bytes.NewReader(bytes.Repeat([]byte{0x39}, 48)))
	if err != nil {
		t.Fatal(err)
	}
	payload := json.RawMessage(`{"operation":"replica.manifest","key_epoch":"1"}`)
	payloadHash, err := peerprotocol.PayloadDigest(payload)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	command := peerprotocol.Command{
		Version: peerprotocol.CommandVersion, RequestID: requestID, SessionID: "ps_aaaaaaaaaaaaaaaaaaaaaa",
		SourceIdentityID: "gi_aaaaaaaaaaaaaaaaaaaaaa", SourceHeadHash: "sha256:" + strings.Repeat("a", 64),
		SourceNodeID: "nd_bbbbbbbbbbbbbbbbbbba", TargetIdentityID: "gi_cccccccccccccccccccccc", TargetNodeID: "nd_dddddddddddddddddddddd",
		OwnerSubjectID: "os_eeeeeeeeeeeeeeeeeeee", ArchiveID: snapshot.Content.ArchiveID, SnapshotID: snapshot.SnapshotID,
		Action: "archive.replicate", IssuedAt: now.Format(time.RFC3339), ExpiresAt: now.Add(time.Minute).Format(time.RFC3339),
		Nonce: nonce, PayloadHash: payloadHash, GrantChain: []authority.SignedGrant{{}}, Payload: payload,
	}
	archiveObject := snapshot.Content.Objects[0]
	ciphertexts := &peerAPICiphertexts{
		objectID: "ob_" + base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x3a}, 32)),
		bundle: replication.Bundle{
			Version: replication.BundleVersion, ArchiveID: snapshot.Content.ArchiveID,
			SnapshotID: snapshot.SnapshotID, KeyEpoch: "1",
			Objects: []replication.ObjectBinding{{
				ObjectID:   "ob_" + base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x3a}, 32)),
				Coordinate: archiveObject.Coordinate, PlaintextSize: archiveObject.PlaintextSize,
				PlaintextSHA256: archiveObject.PlaintextSHA256, CiphertextSize: 65,
				CiphertextSHA256: "sha256:" + strings.Repeat("b", 64),
			}},
		},
	}
	authorizer := &peerAPIAuthProbe{}
	session := &memorySession{command: command, remote: authority.Principal{Kind: authority.PrincipalNode, ID: command.SourceNodeID}}
	service := Service{
		Catalog: snapshotCatalog{snapshot: snapshot}, Ciphertexts: ciphertexts, Authorizer: authorizer,
		IdentityID: command.TargetIdentityID, NodeID: command.TargetNodeID,
		Resolve: func(string, string, time.Time) (authority.IdentityState, error) {
			return authority.IdentityState{}, nil
		},
		Replay: &peerAPIReplayProbe{}, Now: func() time.Time { return now },
	}
	if err := service.ServeOne(context.Background(), session); err != nil {
		t.Fatalf("serve public replica manifest: %v", err)
	}
	response, err := ReceiveResponse(context.Background(), &memorySession{received: session.sent}, requestID, 1024*1024)
	if err != nil || response.Kind != responseKindManifest {
		t.Fatalf("manifest response kind=%d err=%v", response.Kind, err)
	}
	var manifest replication.ReplicaManifest
	if err := json.Unmarshal(response.Body, &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.Version != replication.ReplicaManifestVersion || manifest.ArchiveID != snapshot.Content.ArchiveID ||
		manifest.SnapshotID != snapshot.SnapshotID || manifest.KeyEpoch != "1" || len(manifest.Objects) != 1 {
		t.Fatalf("unexpected public manifest: %+v", manifest)
	}
	if _, err := replication.BundleFromPublicReplicaManifest(snapshot, manifest); err != nil {
		t.Fatalf("manifest did not bind to verified snapshot: %v", err)
	}
	for _, forbidden := range []string{"plaintext_sha256", archiveObject.PlaintextSHA256, "private", "path", "source_uri"} {
		if strings.Contains(string(response.Body), forbidden) {
			t.Fatalf("public replica manifest exposed %q: %s", forbidden, response.Body)
		}
	}
	if ciphertexts.bundleCalls != 1 || authorizer.calls.Load() != 2 {
		t.Fatalf("bundle loads=%d authorization checks=%d, want 1 and 2", ciphertexts.bundleCalls, authorizer.calls.Load())
	}
}

func TestReplicaManifestOperationRequiresReplicationScope(t *testing.T) {
	tests := []struct {
		name       string
		action     string
		snapshotID string
		want       bool
	}{
		{name: "replicate exact snapshot", action: "archive.replicate", snapshotID: "snap_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", want: true},
		{name: "view can read public archive manifest", action: "archive.view", snapshotID: "snap_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", want: true},
		{name: "export cannot fetch replication manifest", action: "archive.export", snapshotID: "snap_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
		{name: "archive-wide grant cannot fetch replication manifest", action: "archive.replicate"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			command := peerprotocol.Command{Action: test.action, SnapshotID: test.snapshotID}
			if got := operationAllowed("replica.manifest", command); got != test.want {
				t.Fatalf("operationAllowed(replica.manifest, action=%q, snapshot=%q)=%t, want %t", test.action, test.snapshotID, got, test.want)
			}
		})
	}
}

func TestReplicaKeyEpochOperationRequiresArchiveReadScope(t *testing.T) {
	for _, test := range []struct {
		name       string
		action     string
		snapshotID string
		want       bool
	}{
		{name: "archive view", action: "archive.view", snapshotID: "snap_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", want: true},
		{name: "archive replicate", action: "archive.replicate", snapshotID: "snap_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", want: true},
		{name: "archive export denied", action: "archive.export", snapshotID: "snap_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
		{name: "archive wide scope denied", action: "archive.view"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := operationAllowed("replica.key-epochs", peerprotocol.Command{Action: test.action, SnapshotID: test.snapshotID}); got != test.want {
				t.Fatalf("operationAllowed(replica.key-epochs, action=%q, snapshot=%q)=%t, want %t", test.action, test.snapshotID, got, test.want)
			}
		})
	}
}

func TestKeyEpochResponseParserRequiresExactBindingsAndDescendingUniqueEpochs(t *testing.T) {
	snapshot := peerAPITestSnapshot(t)
	valid := `{"response_version":"ir.peer-key-epochs/1","archive_id":"` + snapshot.Content.ArchiveID + `","snapshot_id":"` + snapshot.SnapshotID + `","key_epochs":["9","2"]}`
	epochs, err := parseKeyEpochsPayload([]byte(valid), snapshot)
	if err != nil || len(epochs) != 2 || epochs[0] != 9 || epochs[1] != 2 {
		t.Fatalf("valid epoch response=%v err=%v", epochs, err)
	}
	for _, invalid := range []string{
		`{"response_version":"ir.peer-key-epochs/1","archive_id":"` + snapshot.Content.ArchiveID + `","snapshot_id":"` + snapshot.SnapshotID + `","key_epochs":["2","9"]}`,
		`{"response_version":"ir.peer-key-epochs/1","archive_id":"` + snapshot.Content.ArchiveID + `","snapshot_id":"` + snapshot.SnapshotID + `","key_epochs":["9","9"]}`,
		`{"response_version":"ir.peer-key-epochs/1","archive_id":"` + snapshot.Content.ArchiveID + `","snapshot_id":"` + snapshot.SnapshotID + `","key_epochs":["09"]}`,
		`{"response_version":"ir.peer-key-epochs/1","archive_id":"` + snapshot.Content.ArchiveID + `","snapshot_id":"snap_` + strings.Repeat("f", 64) + `","key_epochs":["1"]}`,
		valid[:len(valid)-1] + `,"unexpected":true}`,
	} {
		if _, err := parseKeyEpochsPayload([]byte(invalid), snapshot); !errors.Is(err, ErrResponseInvalid) {
			t.Fatalf("invalid key epoch response accepted: err=%v body=%s", err, invalid)
		}
	}
}

func TestReplicaVerifyOperationRequiresExactReplicationScope(t *testing.T) {
	tests := []struct {
		name       string
		action     string
		snapshotID string
		want       bool
	}{
		{name: "replicate exact snapshot", action: "archive.replicate", snapshotID: "snap_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", want: true},
		{name: "view cannot trigger source lease activation", action: "archive.view", snapshotID: "snap_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
		{name: "archive wide grant cannot verify a lease", action: "archive.replicate"},
		{name: "export cannot trigger source lease activation", action: "archive.export", snapshotID: "snap_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			command := peerprotocol.Command{Action: test.action, SnapshotID: test.snapshotID}
			if got := operationAllowed("replica.verify", command); got != test.want {
				t.Fatalf("operationAllowed(replica.verify, action=%q, snapshot=%q)=%t, want %t", test.action, test.snapshotID, got, test.want)
			}
		})
	}
}

func TestSubmitTombstoneUsesExactPeerScopeAndReturnsStorageReceipt(t *testing.T) {
	ctx := context.Background()
	ownerPublic, ownerPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	identityID, err := identityprotocol.IdentityID(ownerPublic)
	if err != nil {
		t.Fatal(err)
	}
	owner := authority.Principal{Kind: authority.PrincipalIdentity, ID: identityID}
	sourcePrivate := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x35}, ed25519.SeedSize))
	sourceNodeID, err := identityprotocol.NodeID(sourcePrivate.Public().(ed25519.PublicKey))
	if err != nil {
		t.Fatal(err)
	}
	targetPrivate := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x37}, ed25519.SeedSize))
	targetNodeID, err := identityprotocol.NodeID(targetPrivate.Public().(ed25519.PublicKey))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	ownerSubjectID := "os_" + strings.Repeat("c", 22)
	archiveID := "arc_" + base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x44}, 16))
	ownerKeyID, err := identityprotocol.KeyID(ownerPublic)
	if err != nil {
		t.Fatal(err)
	}
	signGrant := func(action string, audience authority.Principal, fill byte) authority.SignedGrant {
		grant, signErr := authority.SignGrant(authority.Grant{
			Version: authority.ProfileVersion, SubjectID: ownerSubjectID, Issuer: owner,
			Audience:  audience,
			ArchiveID: archiveID, Actions: []string{action},
			NotBefore: now.Add(-time.Minute).Format(time.RFC3339), ExpiresAt: now.Add(time.Hour).Format(time.RFC3339),
			DelegationDepth: "0", Nonce: base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{fill}, 32)),
			PolicyEpoch: "1", IssuerKeyID: ownerKeyID,
		}, ownerPrivate)
		if signErr != nil {
			t.Fatal(signErr)
		}
		return grant
	}
	deleteChain := []authority.SignedGrant{signGrant("archive.delete", owner, 0x41)}
	replicateChain := []authority.SignedGrant{signGrant("archive.replicate", authority.Principal{Kind: authority.PrincipalNode, ID: sourceNodeID}, 0x42)}
	grantBytes, err := json.Marshal(deleteChain)
	if err != nil {
		t.Fatal(err)
	}
	canonicalGrantBytes, err := identityprotocol.Canonicalize(grantBytes)
	if err != nil {
		t.Fatal(err)
	}
	evidenceHash := sha256.Sum256(append([]byte("IR/replica-lease/v1/authorization\x00"), canonicalGrantBytes...))
	tombstone := lease.Tombstone{
		Version: lease.TombstoneVersion, ArchiveID: archiveID, OwnerSubjectID: ownerSubjectID,
		DeletionEpoch: "1", ReasonCode: "owner_requested", IssuedAt: now.Format(time.RFC3339),
		AuthorizedEvidence: "sha256:" + hex.EncodeToString(evidenceHash[:]),
	}
	signed, err := lease.SignTombstone(tombstone, ownerSubjectID, owner, deleteChain, ownerPrivate)
	if err != nil {
		t.Fatalf("sign tombstone: %v", err)
	}
	store, err := lease.Open(lease.Options{
		Root:       filepath.Join(t.TempDir(), "peer-lease-store"),
		Authorizer: peerTombstoneScopeAuthorizer{ownerSubjectID: ownerSubjectID, archiveID: archiveID},
		Keys:       peerAPITestKeys{authority.PrincipalIdentity + ":" + identityID: ownerPublic},
		Now:        func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	replicaStore, err := replication.Open(filepath.Join(t.TempDir(), "replica-store"))
	if err != nil {
		t.Fatal(err)
	}
	service := &Service{
		Leases: store, Ciphertexts: replicaStore, Authorizer: peerTombstoneScopeAuthorizer{ownerSubjectID: ownerSubjectID, archiveID: archiveID},
		IdentityID: identityID, NodeID: targetNodeID, Now: func() time.Time { return now },
	}
	session := &offerLoopbackSession{
		remote:    authority.Principal{Kind: authority.PrincipalNode, ID: targetNodeID},
		actor:     authority.Principal{Kind: authority.PrincipalNode, ID: sourceNodeID},
		sessionID: "ps_aaaaaaaaaaaaaaaaaaaaaa", service: service,
	}
	client, err := NewScopedClient(session, MaxPeerResponseBytes, RequestScope{
		IdentityID: identityID, IdentityHead: "sha256:" + strings.Repeat("3", 64), NodeID: sourceNodeID,
		TargetIdentityID: identityID, TargetNodeID: targetNodeID, OwnerSubjectID: ownerSubjectID,
		ArchiveID: archiveID, Action: "archive.replicate",
		GrantChain: replicateChain,
		Resolve: func(string, string, time.Time) (authority.IdentityState, error) {
			return authority.IdentityState{}, nil
		},
		Signer: peerAPIClientSigner{privateKey: sourcePrivate}, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("create scoped peer client: %v", err)
	}
	snapshotScopedScope := RequestScope{
		IdentityID: identityID, IdentityHead: "sha256:" + strings.Repeat("3", 64), NodeID: sourceNodeID,
		TargetIdentityID: identityID, TargetNodeID: targetNodeID, OwnerSubjectID: ownerSubjectID,
		ArchiveID: archiveID, SnapshotID: "snap_" + strings.Repeat("e", 64), Action: "archive.replicate",
		GrantChain: replicateChain,
		Resolve: func(string, string, time.Time) (authority.IdentityState, error) {
			return authority.IdentityState{}, nil
		},
		Signer: peerAPIClientSigner{privateKey: sourcePrivate}, Now: func() time.Time { return now },
	}
	snapshotScopedClient, err := NewScopedClient(session, MaxPeerResponseBytes, snapshotScopedScope)
	if err != nil {
		t.Fatalf("create snapshot-scoped peer client: %v", err)
	}
	if err := snapshotScopedClient.SubmitTombstone(ctx, signed); !errors.Is(err, ErrInvalidRequest) || session.commands != 0 {
		t.Fatalf("snapshot-scoped capability was accepted for archive tombstone: commands=%d err=%v", session.commands, err)
	}
	receipt, err := client.SubmitTombstoneWithReceipt(ctx, signed)
	if err != nil || receipt.Erasure == nil || receipt.Erasure.ArchiveID != archiveID {
		t.Fatalf("submit owner tombstone and erase provider replica: receipt=%+v err=%v", receipt, err)
	}
	session.wrongTombstoneReceipt = true
	if err := client.SubmitTombstone(ctx, signed); !errors.Is(err, ErrResponseInvalid) {
		t.Fatalf("mismatched provider receipt was accepted: %v", err)
	}
	session.wrongTombstoneReceipt = false
	leases, tombstones, err := store.Snapshot(ctx)
	if err != nil || len(leases) != 0 || len(tombstones) != 1 || tombstones[0].Signed.Signature != signed.Signature ||
		session.command.Action != "archive.replicate" || session.command.SnapshotID != "" {
		t.Fatalf("durable tombstone state leases=%d tombstones=%d action=%q snapshot=%q err=%v", len(leases), len(tombstones), session.command.Action, session.command.SnapshotID, err)
	}
	var request requestPayload
	if json.Unmarshal(session.command.Payload, &request) != nil || request.Operation != "replica.tombstone" || request.Tombstone == nil || request.Tombstone.Signature != signed.Signature {
		t.Fatalf("wire command did not carry the unchanged owner-signed decision: %+v", request)
	}
	if err := client.SubmitTombstone(ctx, signed); err != nil || session.commands != 3 {
		t.Fatalf("exact peer replay is not idempotent: commands=%d err=%v", session.commands, err)
	}

	wrongScope := signed
	wrongScope.Tombstone.OwnerSubjectID = "os_" + strings.Repeat("9", 22)
	if err := client.SubmitTombstone(ctx, wrongScope); !errors.Is(err, ErrInvalidRequest) || session.commands != 3 {
		t.Fatalf("mismatched OwnerSubject escaped client scope: commands=%d err=%v", session.commands, err)
	}

	forged := signed
	forged.Signature = strings.Repeat("A", 86)
	if err := client.SubmitTombstone(ctx, forged); err == nil || session.commands != 4 {
		t.Fatal("forged owner signature was acknowledged")
	}
	_, tombstonesAfterForgery, err := store.Snapshot(ctx)
	if err != nil || len(tombstonesAfterForgery) != 1 || tombstonesAfterForgery[0].Signed.Signature != signed.Signature {
		t.Fatalf("forged event changed durable tombstone state: %+v err=%v", tombstonesAfterForgery, err)
	}
}

func TestReplicaTombstoneOperationRequiresArchiveWideReplication(t *testing.T) {
	tests := []struct {
		name       string
		action     string
		snapshotID string
		want       bool
	}{
		{name: "archive-wide replication grant", action: "archive.replicate", want: true},
		{name: "snapshot-scoped command cannot delete archive", action: "archive.replicate", snapshotID: "snap_" + strings.Repeat("a", 64)},
		{name: "view cannot mutate lease state", action: "archive.view"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			command := peerprotocol.Command{Action: test.action, SnapshotID: test.snapshotID}
			if got := operationAllowed("replica.tombstone", command); got != test.want {
				t.Fatalf("operationAllowed(replica.tombstone)=%t, want %t", got, test.want)
			}
		})
	}
}

type peerTombstoneScopeAuthorizer struct {
	ownerSubjectID string
	archiveID      string
}

func (a peerTombstoneScopeAuthorizer) Authorize(_ context.Context, subjectID, archiveID, _ string, action string, actor authority.Principal, chain []authority.SignedGrant, _ time.Time) error {
	if subjectID != a.ownerSubjectID || archiveID != a.archiveID || len(chain) != 1 || chain[0].Grant.Audience != actor {
		return authority.ErrDenied
	}
	switch action {
	case "archive.delete":
		if actor.Kind == authority.PrincipalIdentity && actor.ID != "" && containsTestAction(chain[0].Grant.Actions, action) {
			return nil
		}
	case "archive.replicate":
		if actor.Kind == authority.PrincipalNode && actor.ID != "" && containsTestAction(chain[0].Grant.Actions, action) {
			return nil
		}
	}
	return authority.ErrDenied
}

func containsTestAction(actions []string, wanted string) bool {
	for _, action := range actions {
		if action == wanted {
			return true
		}
	}
	return false
}

func TestServeOneReturnsOnlyCurrentOwnerSignedRecipientKeyEnvelope(t *testing.T) {
	snapshot := peerAPITestSnapshot(t)
	ownerPublic, ownerPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ownerID, err := identityprotocol.IdentityID(ownerPublic)
	if err != nil {
		t.Fatal(err)
	}
	ownerKeyID, err := identityprotocol.KeyID(ownerPublic)
	if err != nil {
		t.Fatal(err)
	}
	recipient, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	dek, err := cryptoarchive.NewDEK()
	if err != nil {
		t.Fatal(err)
	}
	defer dek.Destroy()
	envelope, err := cryptoarchive.WrapDEK(dek, snapshot.Content.ArchiveID, 1, recipient.PublicKey().Bytes(), ownerKeyID, ownerPrivate)
	if err != nil {
		t.Fatal(err)
	}
	requestID, nonce, err := peerprotocol.NewCommand(bytes.NewReader(bytes.Repeat([]byte{0x41}, 48)))
	if err != nil {
		t.Fatal(err)
	}
	payload := json.RawMessage(`{"operation":"replica.key-envelope","key_epoch":"1","recipient_key_id":"` + envelope.RecipientKeyID + `"}`)
	payloadHash, err := peerprotocol.PayloadDigest(payload)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	command := peerprotocol.Command{
		Version: peerprotocol.CommandVersion, RequestID: requestID, SessionID: "ps_aaaaaaaaaaaaaaaaaaaaaa",
		SourceIdentityID: "gi_aaaaaaaaaaaaaaaaaaaaaa", SourceHeadHash: "sha256:" + strings.Repeat("a", 64),
		SourceNodeID: "nd_bbbbbbbbbbbbbbbbbbba", TargetIdentityID: "gi_cccccccccccccccccccccc", TargetNodeID: "nd_dddddddddddddddddddddd",
		OwnerSubjectID: "os_eeeeeeeeeeeeeeeeeeee", ArchiveID: snapshot.Content.ArchiveID, SnapshotID: snapshot.SnapshotID,
		Action: "archive.view", IssuedAt: now.Format(time.RFC3339), ExpiresAt: now.Add(time.Minute).Format(time.RFC3339),
		Nonce: nonce, PayloadHash: payloadHash,
		GrantChain: []authority.SignedGrant{{Grant: authority.Grant{Issuer: authority.Principal{Kind: authority.PrincipalIdentity, ID: ownerID}}}}, Payload: payload,
	}
	ciphertexts := &peerAPICiphertexts{envelope: envelope}
	session := &memorySession{command: command, remote: authority.Principal{Kind: authority.PrincipalNode, ID: command.SourceNodeID}}
	service := Service{
		Catalog: snapshotCatalog{snapshot: snapshot}, Ciphertexts: ciphertexts, Authorizer: &peerAPIAuthProbe{},
		IdentityID: command.TargetIdentityID, NodeID: command.TargetNodeID,
		Resolve: func(identityID, head string, _ time.Time) (authority.IdentityState, error) {
			if identityID != ownerID || head != "" {
				return authority.IdentityState{}, peerprotocol.ErrStale
			}
			return authority.IdentityState{
				IdentityID: ownerID, VerifiedAt: now.Add(-time.Second), FreshUntil: now.Add(time.Minute),
				CurrentKeys: map[string]ed25519.PublicKey{authority.PrincipalIdentity + ":" + ownerID: ownerPublic},
			}, nil
		},
		Replay: &peerAPIReplayProbe{}, Now: func() time.Time { return now },
	}
	if err := service.ServeOne(context.Background(), session); err != nil {
		t.Fatalf("serve key envelope: %v", err)
	}
	response, err := ReceiveResponse(context.Background(), &memorySession{received: session.sent}, requestID, MaxPeerResponseBytes)
	if err != nil || response.Kind != responseKindKeyEnvelope {
		t.Fatalf("envelope response kind=%d err=%v", response.Kind, err)
	}
	returned, err := cryptoarchive.ParseKeyEnvelope(response.Body)
	if err != nil || returned != envelope {
		t.Fatalf("returned envelope mismatch: err=%v", err)
	}
	if err := cryptoarchive.VerifyKeyEnvelopeSignature(returned, ownerPublic); err != nil {
		t.Fatalf("returned envelope signature invalid: %v", err)
	}
	if strings.Contains(string(response.Body), "private_key") || strings.Contains(string(response.Body), "dek_bytes") {
		t.Fatalf("response exposed private key material: %s", response.Body)
	}
}

func TestSessionDEKEnvelopeRequiresArchiveViewAndBindsCurrentGrant(t *testing.T) {
	snapshot := peerAPITestSnapshot(t)
	ownerPublic, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ownerID, err := identityprotocol.IdentityID(ownerPublic)
	if err != nil {
		t.Fatal(err)
	}
	ownerKeyID, err := identityprotocol.KeyID(ownerPublic)
	if err != nil {
		t.Fatal(err)
	}
	keyPair, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	grant := authority.Grant{
		Version: authority.ProfileVersion, SubjectID: "os_eeeeeeeeeeeeeeeeeeeeee",
		Issuer:    authority.Principal{Kind: authority.PrincipalIdentity, ID: ownerID},
		Audience:  authority.Principal{Kind: authority.PrincipalNode, ID: "nd_dddddddddddddddddddddd"},
		ArchiveID: snapshot.Content.ArchiveID, Actions: []string{"archive.view"},
		NotBefore: now.Add(-time.Minute).Format(time.RFC3339), ExpiresAt: now.Add(time.Minute).Format(time.RFC3339),
		DelegationDepth: "0", Nonce: base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x72}, 32)),
		PolicyEpoch: "0", IssuerKeyID: ownerKeyID,
	}
	grantHash, err := authority.GrantHash(grant)
	if err != nil {
		t.Fatal(err)
	}
	publicKeyText := base64.RawURLEncoding.EncodeToString(keyPair.PublicKey().Bytes())
	payload := json.RawMessage(`{"operation":"replica.session-key-envelope","key_epoch":"7","recipient_public_key":"` + publicKeyText + `"}`)
	command := peerAPIClientCommand(t, "nd_dddddddddddddddddddddd")
	command.SourceNodeID = "nd_cccccccccccccccccccccc"
	command.TargetNodeID = "nd_dddddddddddddddddddddd"
	command.TargetIdentityID = "gi_bbbbbbbbbbbbbbbbbbbbbb"
	command.OwnerSubjectID = grant.SubjectID
	command.ArchiveID = snapshot.Content.ArchiveID
	command.SnapshotID = snapshot.SnapshotID
	command.GrantChain = []authority.SignedGrant{{Grant: grant}}
	command.Payload = payload
	command.Action = "archive.view"
	command.PayloadHash, err = peerprotocol.PayloadDigest(payload)
	if err != nil {
		t.Fatal(err)
	}
	provider := testSessionDEKProvider{dataKey: bytes.Repeat([]byte{0x6a}, 32)}
	service := Service{
		Catalog: snapshotCatalog{snapshot: snapshot}, Ciphertexts: &peerAPICiphertexts{}, SessionKeys: provider,
		Authorizer: &peerAPIAuthProbe{}, IdentityID: command.TargetIdentityID, NodeID: command.TargetNodeID,
		Resolve: func(identityID, head string, _ time.Time) (authority.IdentityState, error) {
			if identityID != ownerID || head != "" {
				return authority.IdentityState{}, peerprotocol.ErrStale
			}
			return authority.IdentityState{IdentityID: ownerID, VerifiedAt: now.Add(-time.Second), FreshUntil: now.Add(time.Minute)}, nil
		},
		Now: func() time.Time { return now },
	}
	kind, body, err := service.execute(context.Background(), command, authority.Principal{Kind: authority.PrincipalNode, ID: command.SourceNodeID})
	if err != nil || kind != responseKindSessionDEK {
		t.Fatalf("session DEK response kind=%d err=%v", kind, err)
	}
	envelope, err := cryptoarchive.ParseSessionDEKEnvelope(body)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := cryptoarchive.OpenSessionDEK(envelope, keyPair.Bytes(), cryptoarchive.SessionDEKBinding{
		ArchiveID: snapshot.Content.ArchiveID, SnapshotID: snapshot.SnapshotID, KeyEpoch: 7,
		OwnerSubjectID: grant.SubjectID, GrantHash: grantHash, SupplierNodeID: service.NodeID,
	})
	if err != nil || !bytes.Equal(opened, provider.dataKey) {
		clear(opened)
		t.Fatalf("session key opened=%t err=%v", bytes.Equal(opened, provider.dataKey), err)
	}
	clear(opened)

	command.Action = "archive.replicate"
	if _, _, err := service.execute(context.Background(), command, authority.Principal{Kind: authority.PrincipalNode, ID: command.SourceNodeID}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("non-view command action accepted session key request: %v", err)
	}
}

func TestReplicaKeyEpochsOnlyListsCurrentOwnerSignedBundlesForRecipient(t *testing.T) {
	snapshot := peerAPITestSnapshot(t)
	ownerPublic, ownerPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ownerID, err := identityprotocol.IdentityID(ownerPublic)
	if err != nil {
		t.Fatal(err)
	}
	ownerKeyID, err := identityprotocol.KeyID(ownerPublic)
	if err != nil {
		t.Fatal(err)
	}
	recipient, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	dek, err := cryptoarchive.NewDEK()
	if err != nil {
		t.Fatal(err)
	}
	defer dek.Destroy()
	validEnvelope, err := cryptoarchive.WrapDEK(dek, snapshot.Content.ArchiveID, 1, recipient.PublicKey().Bytes(), ownerKeyID, ownerPrivate)
	if err != nil {
		t.Fatal(err)
	}
	wrongRecipient, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	wrongRecipientEnvelope, err := cryptoarchive.WrapDEK(dek, snapshot.Content.ArchiveID, 3, wrongRecipient.PublicKey().Bytes(), ownerKeyID, ownerPrivate)
	if err != nil {
		t.Fatal(err)
	}
	tamperedEnvelope, err := cryptoarchive.WrapDEK(dek, snapshot.Content.ArchiveID, 2, recipient.PublicKey().Bytes(), ownerKeyID, ownerPrivate)
	if err != nil {
		t.Fatal(err)
	}
	tamperedEnvelope.Ciphertext = strings.Repeat("A", len(tamperedEnvelope.Ciphertext))
	object := snapshot.Content.Objects[0]
	ciphertexts := &peerAPICiphertexts{
		epochs: []uint64{3, 2, 1},
		bundle: replication.Bundle{Version: replication.BundleVersion, ArchiveID: snapshot.Content.ArchiveID, SnapshotID: snapshot.SnapshotID,
			Objects: []replication.ObjectBinding{{ObjectID: "ob_" + base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x45}, 32)),
				Coordinate: object.Coordinate, PlaintextSize: object.PlaintextSize, PlaintextSHA256: object.PlaintextSHA256,
				CiphertextSize: 65, CiphertextSHA256: "sha256:" + strings.Repeat("b", 64)}}},
		envelopes: map[uint64]cryptoarchive.KeyEnvelope{1: validEnvelope, 2: tamperedEnvelope, 3: wrongRecipientEnvelope},
	}
	command := peerprotocol.Command{
		SessionID: "ps_aaaaaaaaaaaaaaaaaaaaaa", TargetIdentityID: "gi_cccccccccccccccccccccc", TargetNodeID: "nd_dddddddddddddddddddddd",
		SourceNodeID: "nd_bbbbbbbbbbbbbbbbbbba", OwnerSubjectID: "os_eeeeeeeeeeeeeeeeeeee",
		ArchiveID: snapshot.Content.ArchiveID, SnapshotID: snapshot.SnapshotID, Action: "archive.view",
		GrantChain: []authority.SignedGrant{{Grant: authority.Grant{Issuer: authority.Principal{Kind: authority.PrincipalIdentity, ID: ownerID}}}},
		Payload:    json.RawMessage(`{"operation":"replica.key-epochs","recipient_key_id":"` + validEnvelope.RecipientKeyID + `"}`),
	}
	now := time.Now().UTC()
	service := Service{
		Catalog: snapshotCatalog{snapshot: snapshot}, Ciphertexts: ciphertexts, Authorizer: &peerAPIAuthProbe{},
		IdentityID: command.TargetIdentityID, NodeID: command.TargetNodeID,
		Resolve: func(identityID, head string, _ time.Time) (authority.IdentityState, error) {
			if identityID != ownerID || head != "" {
				return authority.IdentityState{}, peerprotocol.ErrStale
			}
			return authority.IdentityState{IdentityID: ownerID, VerifiedAt: now.Add(-time.Second), FreshUntil: now.Add(time.Minute),
				CurrentKeys: map[string]ed25519.PublicKey{authority.PrincipalIdentity + ":" + ownerID: ownerPublic}}, nil
		},
		Now: func() time.Time { return now },
	}
	kind, body, err := service.execute(context.Background(), command, authority.Principal{Kind: authority.PrincipalNode, ID: command.SourceNodeID})
	if err != nil || kind != responseKindKeyEpochs {
		t.Fatalf("key epoch response kind=%d err=%v", kind, err)
	}
	var response keyEpochsPayload
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatal(err)
	}
	if response.Version != "ir.peer-key-epochs/1" || response.ArchiveID != snapshot.Content.ArchiveID || response.SnapshotID != snapshot.SnapshotID ||
		len(response.KeyEpochs) != 1 || response.KeyEpochs[0] != "1" {
		t.Fatalf("key epoch discovery response=%+v", response)
	}
	for _, forbidden := range []string{"private_key", "dek_bytes", "plaintext_sha256", "/Users/", "source_uri"} {
		if strings.Contains(string(body), forbidden) {
			t.Fatalf("key epoch response exposed %q: %s", forbidden, body)
		}
	}
}

func TestServeOneRejectsKeyEnvelopeNotSignedByCurrentOwnerKey(t *testing.T) {
	snapshot := peerAPITestSnapshot(t)
	ownerPublic, ownerPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ownerID, _ := identityprotocol.IdentityID(ownerPublic)
	ownerKeyID, _ := identityprotocol.KeyID(ownerPublic)
	recipient, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	dek, err := cryptoarchive.NewDEK()
	if err != nil {
		t.Fatal(err)
	}
	defer dek.Destroy()
	envelope, err := cryptoarchive.WrapDEK(dek, snapshot.Content.ArchiveID, 1, recipient.PublicKey().Bytes(), ownerKeyID, ownerPrivate)
	if err != nil {
		t.Fatal(err)
	}
	envelope.Ciphertext = strings.Repeat("A", len(envelope.Ciphertext)) // invalidates the signed binding.
	requestID, nonce, _ := peerprotocol.NewCommand(bytes.NewReader(bytes.Repeat([]byte{0x42}, 48)))
	payload := json.RawMessage(`{"operation":"replica.key-envelope","key_epoch":"1","recipient_key_id":"` + envelope.RecipientKeyID + `"}`)
	payloadHash, _ := peerprotocol.PayloadDigest(payload)
	now := time.Now().UTC().Truncate(time.Second)
	command := peerprotocol.Command{
		Version: peerprotocol.CommandVersion, RequestID: requestID, SessionID: "ps_aaaaaaaaaaaaaaaaaaaaaa",
		SourceIdentityID: "gi_aaaaaaaaaaaaaaaaaaaaaa", SourceHeadHash: "sha256:" + strings.Repeat("a", 64),
		SourceNodeID: "nd_bbbbbbbbbbbbbbbbbbba", TargetIdentityID: "gi_cccccccccccccccccccccc", TargetNodeID: "nd_dddddddddddddddddddddd",
		OwnerSubjectID: "os_eeeeeeeeeeeeeeeeeeee", ArchiveID: snapshot.Content.ArchiveID, SnapshotID: snapshot.SnapshotID,
		Action: "archive.view", IssuedAt: now.Format(time.RFC3339), ExpiresAt: now.Add(time.Minute).Format(time.RFC3339),
		Nonce: nonce, PayloadHash: payloadHash,
		GrantChain: []authority.SignedGrant{{Grant: authority.Grant{Issuer: authority.Principal{Kind: authority.PrincipalIdentity, ID: ownerID}}}}, Payload: payload,
	}
	session := &memorySession{command: command, remote: authority.Principal{Kind: authority.PrincipalNode, ID: command.SourceNodeID}}
	service := Service{
		Catalog: snapshotCatalog{snapshot: snapshot}, Ciphertexts: &peerAPICiphertexts{envelope: envelope}, Authorizer: &peerAPIAuthProbe{},
		IdentityID: command.TargetIdentityID, NodeID: command.TargetNodeID,
		Resolve: func(string, string, time.Time) (authority.IdentityState, error) {
			return authority.IdentityState{IdentityID: ownerID, VerifiedAt: now.Add(-time.Second), FreshUntil: now.Add(time.Minute), CurrentKeys: map[string]ed25519.PublicKey{authority.PrincipalIdentity + ":" + ownerID: ownerPublic}}, nil
		}, Replay: &peerAPIReplayProbe{}, Now: func() time.Time { return now },
	}
	if err := service.ServeOne(context.Background(), session); err != nil {
		t.Fatalf("serve signed error envelope: %v", err)
	}
	response, err := ReceiveResponse(context.Background(), &memorySession{received: session.sent}, requestID, MaxPeerResponseBytes)
	if err != nil || response.Kind != responseKindError {
		t.Fatalf("tampered envelope response kind=%d err=%v", response.Kind, err)
	}
}

func TestServeOneRejectsMismatchedSessionBeforeReadingArchive(t *testing.T) {
	snapshot := peerAPITestSnapshot(t)
	requestID, nonce, _ := peerprotocol.NewCommand(bytes.NewReader(bytes.Repeat([]byte{0x34}, 48)))
	payload := json.RawMessage(`{"operation":"snapshot.get"}`)
	payloadHash, _ := peerprotocol.PayloadDigest(payload)
	now := time.Now().UTC().Truncate(time.Second)
	command := peerprotocol.Command{
		Version: peerprotocol.CommandVersion, RequestID: requestID, SessionID: "ps_aaaaaaaaaaaaaaaaaaaaaa",
		SourceIdentityID: "gi_aaaaaaaaaaaaaaaaaaaaaa", SourceHeadHash: "sha256:" + strings.Repeat("a", 64),
		SourceNodeID: "nd_bbbbbbbbbbbbbbbbbbba", TargetIdentityID: "gi_cccccccccccccccccccccc", TargetNodeID: "nd_dddddddddddddddddddddd",
		OwnerSubjectID: "os_eeeeeeeeeeeeeeeeeeee", ArchiveID: snapshot.Content.ArchiveID, SnapshotID: snapshot.SnapshotID,
		Action: "archive.view", IssuedAt: now.Format(time.RFC3339), ExpiresAt: now.Add(time.Minute).Format(time.RFC3339),
		Nonce: nonce, PayloadHash: payloadHash, GrantChain: []authority.SignedGrant{{}}, Payload: payload,
	}
	catalog := &countingSnapshotCatalog{snapshot: snapshot}
	session := &memorySession{command: command, remote: authority.Principal{Kind: authority.PrincipalNode, ID: command.SourceNodeID}, sessionID: "ps_zzzzzzzzzzzzzzzzzzzzzz"}
	service := Service{Catalog: catalog, Authorizer: &peerAPIAuthProbe{}, IdentityID: command.TargetIdentityID, NodeID: command.TargetNodeID,
		Resolve: func(string, string, time.Time) (authority.IdentityState, error) {
			return authority.IdentityState{}, nil
		}, Replay: &peerAPIReplayProbe{}}
	if err := service.ServeOne(context.Background(), session); !errors.Is(err, peerprotocol.ErrCommandDenied) {
		t.Fatalf("session mismatch error=%v", err)
	}
	if catalog.calls != 0 || len(session.sent) != 0 {
		t.Fatalf("archive read calls=%d response frames=%d", catalog.calls, len(session.sent))
	}
}

func peerAPITestSnapshot(t *testing.T) archivesnapshot.Snapshot {
	t.Helper()
	session, err := archiveindex.NewSessionIdentity("recording-peerapi-test", "hls", nil, "broadcast")
	if err != nil {
		t.Fatal(err)
	}
	inventory, err := archiveindex.NewInventory("recording-peerapi-test", session)
	if err != nil {
		t.Fatal(err)
	}
	archiveID, err := archivesnapshot.ArchiveIDForRecording(inventory.RecordingID)
	if err != nil {
		t.Fatal(err)
	}
	inventory.ArchiveID = archiveID
	payload := []byte("canonical media bytes")
	digest := sha256.Sum256(payload)
	_, err = archiveindex.ApplyClaim(&inventory, archiveindex.ClaimInput{
		Coordinate: archiveindex.Coordinate{SessionID: inventory.Session.ID, TrackID: "main", SourceEpoch: 1, DiscontinuitySequence: 1, Sequence: 1, Kind: archiveindex.ObjectMedia},
		ClaimID:    "claim-peerapi", Source: archiveindex.ClaimLiveOrigin, AcquiredAt: time.Now().UTC(),
		PayloadPath: "private/payload.bin", Size: int64(len(payload)), SHA256: hex.EncodeToString(digest[:]),
		Verification: archiveindex.VerificationVerified, Duration: 2.5,
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := archivesnapshot.Build(inventory, 1, "")
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

type snapshotCatalog struct{ snapshot archivesnapshot.Snapshot }

type missingSnapshotCatalog struct{ err error }

func (c missingSnapshotCatalog) Head(context.Context, string) (archivesnapshot.Snapshot, error) {
	return archivesnapshot.Snapshot{}, c.err
}

func (c missingSnapshotCatalog) Snapshot(context.Context, string, string) (archivesnapshot.Snapshot, error) {
	return archivesnapshot.Snapshot{}, c.err
}

func (c snapshotCatalog) Head(context.Context, string) (archivesnapshot.Snapshot, error) {
	return c.snapshot, nil
}

type countingSnapshotCatalog struct {
	snapshot archivesnapshot.Snapshot
	calls    int
}

func (c *countingSnapshotCatalog) Head(context.Context, string) (archivesnapshot.Snapshot, error) {
	c.calls++
	return c.snapshot, nil
}

func (c *countingSnapshotCatalog) Snapshot(context.Context, string, string) (archivesnapshot.Snapshot, error) {
	c.calls++
	return c.snapshot, nil
}

type peerAPICiphertexts struct {
	data        []byte
	objectID    string
	bundle      replication.Bundle
	bundleCalls int
	rangeCalls  int
	offset      int64
	length      int64
	envelope    cryptoarchive.KeyEnvelope
	epochs      []uint64
	envelopes   map[uint64]cryptoarchive.KeyEnvelope
}

type testSessionDEKProvider struct{ dataKey []byte }

func (p testSessionDEKProvider) HasSessionDEK(_ archivesnapshot.Snapshot, _ uint64) bool {
	return len(p.dataKey) == 32
}

func (p testSessionDEKProvider) SealSessionDEK(_ archivesnapshot.Snapshot, _ uint64, recipientPublicKey []byte, binding cryptoarchive.SessionDEKBinding) (cryptoarchive.SessionDEKEnvelope, error) {
	return cryptoarchive.SealSessionDEK(p.dataKey, recipientPublicKey, binding)
}

func (c *peerAPICiphertexts) KeyEpochs(_ archivesnapshot.Snapshot) ([]uint64, error) {
	return append([]uint64(nil), c.epochs...), nil
}

func (c *peerAPICiphertexts) LoadKeyEnvelope(_ archivesnapshot.Snapshot, epoch uint64, _ string) (cryptoarchive.KeyEnvelope, error) {
	if envelope, ok := c.envelopes[epoch]; ok {
		return envelope, nil
	}
	if c.envelope.EnvelopeVersion == "" {
		return cryptoarchive.KeyEnvelope{}, replication.ErrObjectUnavailable
	}
	return c.envelope, nil
}

func (c *peerAPICiphertexts) LoadBundle(snapshot archivesnapshot.Snapshot, keyEpoch uint64) (replication.Bundle, error) {
	c.bundleCalls++
	if c.bundle.Version != "" {
		bundle := c.bundle
		bundle.KeyEpoch = strconv.FormatUint(keyEpoch, 10)
		return bundle, nil
	}
	if keyEpoch != 1 || snapshot.Content.ArchiveID == "" {
		return replication.Bundle{}, replication.ErrInvalid
	}
	return replication.Bundle{Version: "ir.encrypted-snapshot/1", ArchiveID: snapshot.Content.ArchiveID, SnapshotID: snapshot.SnapshotID,
		KeyEpoch: "1", Objects: []replication.ObjectBinding{{ObjectID: c.objectID, CiphertextSize: int64(len(c.data))}}}, nil
}

func (c *peerAPICiphertexts) OpenCiphertextRangeForPeer(_ context.Context, _ archivesnapshot.Snapshot, _ replication.Bundle, _ replication.Authorization, action, objectID string, offset, length int64) (io.ReadCloser, error) {
	c.rangeCalls++
	c.offset, c.length = offset, length
	if action != "archive.view" || objectID != c.objectID || offset < 0 || length <= 0 || offset+length > int64(len(c.data)) {
		return nil, replication.ErrObjectUnavailable
	}
	return io.NopCloser(bytes.NewReader(c.data[offset : offset+length])), nil
}
func (c snapshotCatalog) Snapshot(_ context.Context, archiveID, snapshotID string) (archivesnapshot.Snapshot, error) {
	if c.snapshot.Content.ArchiveID != archiveID || c.snapshot.SnapshotID != snapshotID {
		return archivesnapshot.Snapshot{}, archivecatalog.ErrNotFound
	}
	return c.snapshot, nil
}

type peerAPIAuthProbe struct {
	calls  atomic.Int32
	denyAt int
}

func (a *peerAPIAuthProbe) Authorize(_ context.Context, _, _, _, _ string, _ authority.Principal, chain []authority.SignedGrant, _ time.Time) error {
	calls := a.calls.Add(1)
	if a.denyAt == int(calls) || len(chain) != 1 {
		return authority.ErrDenied
	}
	return nil
}

type peerAPIReplayProbe struct{ calls int }

func (r *peerAPIReplayProbe) ConsumeCommand(_ peerprotocol.Command, _ peerprotocol.IdentityResolver, _ time.Time, _ string, _ authority.Principal, _, _ string) error {
	r.calls++
	return nil
}

type memorySession struct {
	command     peerprotocol.Command
	remote      authority.Principal
	sessionID   string
	sentCommand peerprotocol.Command
	sent        [][]byte
	received    [][]byte
}

func (s *memorySession) Remote() authority.Principal { return s.remote }
func (s *memorySession) SessionID() string {
	if s.sessionID != "" {
		return s.sessionID
	}
	return s.command.SessionID
}
func (s *memorySession) SendCommand(_ context.Context, command peerprotocol.Command) error {
	s.sentCommand = command
	return nil
}
func (s *memorySession) Send(_ context.Context, payload []byte) error {
	s.sent = append(s.sent, append([]byte(nil), payload...))
	return nil
}
func (s *memorySession) Receive(_ context.Context) ([]byte, error) {
	if len(s.received) == 0 {
		return nil, io.EOF
	}
	frame := s.received[0]
	s.received = s.received[1:]
	return append([]byte(nil), frame...), nil
}
func (s *memorySession) ReceiveAuthorizedCommand(ctx context.Context, _ peerprotocol.IdentityResolver, authorizer peerprotocol.CommandAuthorizer, replay peertransport.PeerCommandReplayConsumer, localIdentityID, localNodeID string) (peerprotocol.Command, error) {
	if s.command.TargetIdentityID != localIdentityID || s.command.TargetNodeID != localNodeID || s.command.SessionID != s.SessionID() {
		return peerprotocol.Command{}, peerprotocol.ErrCommandDenied
	}
	if err := authorizer.Authorize(ctx, s.command.OwnerSubjectID, s.command.ArchiveID, s.command.SnapshotID, s.command.Action, s.remote, s.command.GrantChain, time.Now().UTC()); err != nil {
		return peerprotocol.Command{}, peerprotocol.ErrCommandDenied
	}
	if err := replay.ConsumeCommand(s.command, nil, time.Now().UTC(), s.command.SessionID, s.remote, localIdentityID, localNodeID); err != nil {
		return peerprotocol.Command{}, err
	}
	return s.command, nil
}
