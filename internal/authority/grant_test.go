package authority

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/identityprotocol"
)

func TestCapabilityChainAllowsOnlyAttenuatedDelegation(t *testing.T) {
	fixture := newAuthorityFixture(t)
	rootGrant := fixture.rootGrant()
	rootSigned, err := SignGrant(rootGrant, fixture.rootPrivate)
	if err != nil {
		t.Fatal(err)
	}
	parentHash, err := GrantHash(rootGrant)
	if err != nil {
		t.Fatal(err)
	}
	child := fixture.grant(fixture.delegate, fixture.recipient, []string{"archive.view"}, "0")
	child.ParentGrantHash = parentHash
	child.NotBefore = fixture.now.Add(time.Minute).UTC().Format(time.RFC3339)
	child.ExpiresAt = fixture.now.Add(time.Hour).UTC().Format(time.RFC3339)
	childSigned, err := SignGrant(child, fixture.delegatePrivate)
	if err != nil {
		t.Fatal(err)
	}
	ctx := fixture.context()
	ctx.Now = fixture.now.Add(2 * time.Minute)
	if err := VerifyGrantChain(ctx, []SignedGrant{rootSigned, childSigned}, fixture.recipient, fixture.archiveID, fixture.snapshotID, "archive.view"); err != nil {
		t.Fatalf("valid attenuated chain rejected: %v", err)
	}
	if err := VerifyGrantChain(ctx, []SignedGrant{rootSigned, childSigned}, fixture.recipient, fixture.archiveID, fixture.snapshotID, "archive.export"); !errors.Is(err, ErrDenied) {
		t.Fatalf("ungranted action result = %v, want denied", err)
	}
	other := Principal{Kind: PrincipalIdentity, ID: fixture.otherIdentity}
	if err := VerifyGrantChain(ctx, []SignedGrant{rootSigned, childSigned}, other, fixture.archiveID, fixture.snapshotID, "archive.view"); !errors.Is(err, ErrDenied) {
		t.Fatalf("wrong audience result = %v, want denied", err)
	}
}

func TestCapabilityChainRejectsScopeTimeActionAndDepthEscalation(t *testing.T) {
	cases := []struct {
		name         string
		mutateParent func(*authorityFixture, *Grant)
		mutateChild  func(*authorityFixture, Grant) Grant
	}{
		{
			name: "action expansion",
			mutateChild: func(f *authorityFixture, grant Grant) Grant {
				grant.Actions = []string{"archive.export"}
				return grant
			},
		},
		{
			name: "archive expansion",
			mutateChild: func(f *authorityFixture, grant Grant) Grant {
				grant.ArchiveID = f.otherArchive
				return grant
			},
		},
		{
			name:         "snapshot expansion",
			mutateParent: func(f *authorityFixture, grant *Grant) { grant.SnapshotID = f.snapshotID },
			mutateChild: func(f *authorityFixture, grant Grant) Grant {
				grant.SnapshotID = f.otherSnapshot
				return grant
			},
		},
		{
			name: "expiration expansion",
			mutateChild: func(f *authorityFixture, grant Grant) Grant {
				grant.ExpiresAt = f.now.Add(3 * time.Hour).UTC().Format(time.RFC3339)
				return grant
			},
		},
		{
			name: "not-before expansion",
			mutateChild: func(f *authorityFixture, grant Grant) Grant {
				grant.NotBefore = f.now.Add(-time.Minute).UTC().Format(time.RFC3339)
				return grant
			},
		},
		{
			name: "delegation depth not reduced",
			mutateChild: func(f *authorityFixture, grant Grant) Grant {
				grant.DelegationDepth = "1"
				return grant
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newAuthorityFixture(t)
			parent := f.rootGrant()
			if tc.mutateParent != nil {
				tc.mutateParent(f, &parent)
			}
			rootSigned, err := SignGrant(parent, f.rootPrivate)
			if err != nil {
				t.Fatal(err)
			}
			parentHash, _ := GrantHash(parent)
			child := f.grant(f.delegate, f.recipient, []string{"archive.view"}, "0")
			child.ParentGrantHash = parentHash
			if tc.mutateChild != nil {
				child = tc.mutateChild(f, child)
			}
			childSigned, err := SignGrant(child, f.delegatePrivate)
			if err != nil {
				t.Fatal(err)
			}
			ctx := f.context()
			ctx.Now = f.now.Add(2 * time.Minute)
			if err := VerifyGrantChain(ctx, []SignedGrant{rootSigned, childSigned}, f.recipient, f.archiveID, f.snapshotID, "archive.view"); !errors.Is(err, ErrDenied) {
				t.Fatalf("escalated chain result = %v, want denied", err)
			}
		})
	}
}

func TestCapabilityChainFailsClosedOnStaleFrozenRevokedOrMismatchedState(t *testing.T) {
	fixture := newAuthorityFixture(t)
	grant := fixture.rootGrant()
	signed, err := SignGrant(grant, fixture.rootPrivate)
	if err != nil {
		t.Fatal(err)
	}
	base := fixture.context()
	tests := []struct {
		name   string
		mutate func(*VerificationContext)
		want   error
	}{
		{name: "stale identity", mutate: func(ctx *VerificationContext) { ctx.Identity.FreshUntil = ctx.Now }, want: ErrStaleIdentity},
		{name: "identity cache too old", mutate: func(ctx *VerificationContext) { ctx.Identity.VerifiedAt = ctx.Now.Add(-6 * time.Minute) }, want: ErrStaleIdentity},
		{name: "identity freshness claim too long", mutate: func(ctx *VerificationContext) { ctx.Identity.FreshUntil = ctx.Identity.VerifiedAt.Add(6 * time.Minute) }, want: ErrStaleIdentity},
		{name: "frozen identity", mutate: func(ctx *VerificationContext) { ctx.Identity.Frozen = true }, want: ErrFrozenIdentity},
		{name: "policy epoch mismatch", mutate: func(ctx *VerificationContext) { ctx.SubjectPolicyEpoch = "2" }, want: ErrStaleIdentity},
		{name: "revoked grant", mutate: func(ctx *VerificationContext) {
			ctx.RevokedGrantHashes[grantHashUnchecked(grant)] = struct{}{}
		}, want: ErrRevoked},
		{name: "controller key replaced", mutate: func(ctx *VerificationContext) {
			ctx.Identity.CurrentKeys[fixture.owner.key()] = ed25519.PublicKey(make([]byte, ed25519.PublicKeySize))
		}, want: ErrDenied},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := cloneContext(base)
			tc.mutate(&ctx)
			err := VerifyGrantChain(ctx, []SignedGrant{signed}, fixture.delegate, fixture.archiveID, "", "archive.view")
			if !errors.Is(err, tc.want) {
				t.Fatalf("result = %v, want errors.Is(_, %v)", err, tc.want)
			}
		})
	}
}

func TestCapabilityGrantRejectsInvalidSignatureAndUnsupportedGroupSubject(t *testing.T) {
	f := newAuthorityFixture(t)
	grant := f.rootGrant()
	signed, err := SignGrant(grant, f.rootPrivate)
	if err != nil {
		t.Fatal(err)
	}
	signed.Signature[0] ^= 1
	if err := VerifyGrantChain(f.context(), []SignedGrant{signed}, f.delegate, f.archiveID, "", "archive.view"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("modified signature result = %v, want invalid", err)
	}

	ctx := f.context()
	ctx.Subject.Type = SubjectGroup
	if err := VerifyGrantChain(ctx, []SignedGrant{mustSign(t, grant, f.rootPrivate)}, f.delegate, f.archiveID, "", "archive.view"); !errors.Is(err, ErrUnsupportedSubject) {
		t.Fatalf("group subject result = %v, want unsupported", err)
	}
}

func TestCapabilityChainRejectsReusedNonce(t *testing.T) {
	f := newAuthorityFixture(t)
	parent := f.rootGrant()
	rootSigned := mustSign(t, parent, f.rootPrivate)
	parentHash, _ := GrantHash(parent)
	child := f.grant(f.delegate, f.recipient, []string{"archive.view"}, "0")
	child.ParentGrantHash = parentHash
	child.Nonce = parent.Nonce
	childSigned := mustSign(t, child, f.delegatePrivate)
	if err := VerifyGrantChain(f.context(), []SignedGrant{rootSigned, childSigned}, f.recipient, f.archiveID, "", "archive.view"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("reused nonce result = %v, want invalid", err)
	}
}

func TestCapabilityLeafCannotDelegateAtZeroDepth(t *testing.T) {
	f := newAuthorityFixture(t)
	parent := f.rootGrant()
	rootSigned := mustSign(t, parent, f.rootPrivate)
	parentHash, _ := GrantHash(parent)
	child := f.grant(f.delegate, f.recipient, []string{"archive.delegate", "archive.view"}, "0")
	child.ParentGrantHash = parentHash
	childSigned := mustSign(t, child, f.delegatePrivate)
	if err := VerifyGrantChain(f.context(), []SignedGrant{rootSigned, childSigned}, f.recipient, f.archiveID, "", "archive.delegate"); !errors.Is(err, ErrDenied) {
		t.Fatalf("zero-depth delegate result = %v, want denied", err)
	}
}

func TestParseSignedGrantRejectsAmbiguousAndOversizedWireInput(t *testing.T) {
	f := newAuthorityFixture(t)
	grant := f.rootGrant()
	signed := mustSign(t, grant, f.rootPrivate)
	wire := struct {
		Grant     Grant  `json:"grant"`
		Signature string `json:"signature"`
	}{Grant: grant, Signature: base64.RawURLEncoding.EncodeToString(signed.Signature)}
	valid, err := json.Marshal(wire)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseSignedGrant(valid)
	if err != nil || string(parsed.Signature) != string(signed.Signature) {
		t.Fatalf("valid signed grant parse: err=%v", err)
	}
	if _, err := ParseSignedGrant(append(append([]byte(nil), valid...), []byte(` {}`)...)); !errors.Is(err, ErrInvalid) {
		t.Fatalf("trailing JSON result = %v, want invalid", err)
	}
	duplicate := strings.Replace(string(valid), `"signature":`, `"grant":{},"signature":`, 1)
	if _, err := ParseSignedGrant([]byte(duplicate)); !errors.Is(err, ErrInvalid) {
		t.Fatalf("duplicate property result = %v, want invalid", err)
	}
	if _, err := ParseSignedGrant(make([]byte, maxSignedGrantBytes+1)); !errors.Is(err, ErrInvalid) {
		t.Fatalf("oversize result = %v, want invalid", err)
	}
}

func TestPersonOwnerSubjectUsesStableDistinctIdentifier(t *testing.T) {
	f := newAuthorityFixture(t)
	first, err := NewPersonOwnerSubject(f.owner.ID, "1", f.now, stringsReader("0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewPersonOwnerSubject(f.owner.ID, "1", f.now, stringsReader("fedcba9876543210"))
	if err != nil {
		t.Fatal(err)
	}
	if first.ID == second.ID || first.ID == f.owner.ID || first.Type != SubjectPerson || first.Controller != f.owner {
		t.Fatalf("OwnerSubject identity separation failed: first=%+v second=%+v", first, second)
	}
}

func TestOwnerTransferRequiresBothFreshIdentitiesAndBumpsPolicyEpoch(t *testing.T) {
	f := newAuthorityFixture(t)
	ctx := f.context()
	recipientPublic := f.recipientPrivate.Public().(ed25519.PublicKey)
	recipientState := IdentityState{
		IdentityID: f.recipient.ID, HeadHash: "sha256:" + strings.Repeat("1", 64),
		VerifiedAt: f.now, FreshUntil: f.now.Add(5 * time.Minute),
		CurrentKeys: map[string]ed25519.PublicKey{f.recipient.key(): recipientPublic},
	}
	entropy := append([]byte("transfer-id-1234"), bytes.Repeat([]byte{0x77}, 32)...)
	offer, err := NewTransferOffer(ctx, f.recipient, f.now.Add(time.Hour), bytes.NewReader(entropy))
	if err != nil {
		t.Fatal(err)
	}
	offer, err = BindRecipientHead(offer, recipientState, f.now)
	if err != nil {
		t.Fatal(err)
	}
	signed, err := SignTransferOffer(offer, f.rootPrivate)
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := AcceptTransferOffer(signed, f.recipientPrivate)
	if err != nil {
		t.Fatal(err)
	}
	result, err := VerifyTransferAcceptance(ctx, recipientState, accepted)
	if err != nil {
		t.Fatalf("valid dual-signed transfer rejected: %v", err)
	}
	if result.Subject.ID != f.subject.ID || result.Subject.Controller != f.recipient || result.Subject.PolicyEpoch != "2" || result.InvalidatedPolicyEpoch != "1" || result.State != "rekey_required" || !result.RekeyRequired {
		t.Fatalf("unexpected transfer result: %+v", result)
	}

	changed := accepted
	changed.Offer.ExpiresAt = f.now.Add(25 * time.Hour).UTC().Format(time.RFC3339)
	if _, err := VerifyTransferAcceptance(ctx, recipientState, changed); !errors.Is(err, ErrInvalid) {
		t.Fatalf("tampered transfer offer result = %v, want invalid", err)
	}
	badOldProof := accepted
	oldSignature, _ := base64.RawURLEncoding.DecodeString(accepted.CurrentControllerProof)
	oldSignature[0] ^= 1
	badOldProof.CurrentControllerProof = base64.RawURLEncoding.EncodeToString(oldSignature)
	if _, err := VerifyTransferAcceptance(ctx, recipientState, badOldProof); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid old-controller consent result = %v, want invalid", err)
	}
	badNewProof := accepted
	newSignature, _ := base64.RawURLEncoding.DecodeString(accepted.RecipientProof)
	newSignature[0] ^= 1
	badNewProof.RecipientProof = base64.RawURLEncoding.EncodeToString(newSignature)
	if _, err := VerifyTransferAcceptance(ctx, recipientState, badNewProof); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid recipient consent result = %v, want invalid", err)
	}
	staleRecipient := recipientState
	staleRecipient.FreshUntil = f.now
	if _, err := VerifyTransferAcceptance(ctx, staleRecipient, accepted); !errors.Is(err, ErrStaleIdentity) {
		t.Fatalf("stale recipient result = %v, want stale", err)
	}
}

type authorityFixture struct {
	now                                                                           time.Time
	owner, delegate, recipient                                                    Principal
	rootPrivate, delegatePrivate, recipientPrivate                                ed25519.PrivateKey
	identityID, otherIdentity, archiveID, otherArchive, snapshotID, otherSnapshot string
	subject                                                                       OwnerSubject
}

func newAuthorityFixture(t *testing.T) *authorityFixture {
	t.Helper()
	rootPublic, rootPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	delegatePublic, delegatePrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	recipientPublic, recipientPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	otherPublic, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ownerID, _ := identityprotocol.IdentityID(rootPublic)
	delegateID, _ := identityprotocol.NodeID(delegatePublic)
	recipientID, _ := identityprotocol.IdentityID(recipientPublic)
	otherID, _ := identityprotocol.IdentityID(otherPublic)
	now := time.Date(2026, 11, 2, 1, 2, 3, 0, time.UTC)
	subject, err := NewPersonOwnerSubject(ownerID, "1", now, stringsReader("0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	return &authorityFixture{
		now:         now,
		owner:       Principal{Kind: PrincipalIdentity, ID: ownerID},
		delegate:    Principal{Kind: PrincipalNode, ID: delegateID},
		recipient:   Principal{Kind: PrincipalIdentity, ID: recipientID},
		rootPrivate: rootPrivate, delegatePrivate: delegatePrivate, recipientPrivate: recipientPrivate,
		identityID: ownerID, otherIdentity: otherID,
		archiveID:    "arc_" + base64.RawURLEncoding.EncodeToString([]byte("0123456789abcdef")),
		otherArchive: "arc_" + base64.RawURLEncoding.EncodeToString([]byte("fedcba9876543210")),
		snapshotID:   "snap_" + strings.Repeat("a", 64), otherSnapshot: "snap_" + strings.Repeat("b", 64),
		subject: subject,
	}
}

func (f *authorityFixture) context() VerificationContext {
	rootPublic := f.rootPrivate.Public().(ed25519.PublicKey)
	delegatePublic := f.delegatePrivate.Public().(ed25519.PublicKey)
	return VerificationContext{
		Subject: f.subject, SubjectPolicyEpoch: f.subject.PolicyEpoch,
		RevokedGrantHashes: map[string]struct{}{},
		Identity: IdentityState{
			IdentityID: f.identityID, HeadHash: "sha256:" + strings.Repeat("0", 64),
			VerifiedAt:  f.now,
			FreshUntil:  f.now.Add(5 * time.Minute),
			CurrentKeys: map[string]ed25519.PublicKey{f.owner.key(): rootPublic, f.delegate.key(): delegatePublic},
		},
		Now: f.now,
	}
}

func (f *authorityFixture) grant(issuer, audience Principal, actions []string, depth string) Grant {
	key := f.rootPrivate
	if issuer == f.delegate {
		key = f.delegatePrivate
	}
	keyID, _ := identityprotocol.KeyID(key.Public().(ed25519.PublicKey))
	nonce := sha256.Sum256([]byte(issuer.key() + "\x00" + audience.key()))
	return Grant{
		Version: ProfileVersion, SubjectID: f.subject.ID, Issuer: issuer, Audience: audience,
		ArchiveID: f.archiveID, Actions: actions,
		NotBefore: f.now.UTC().Format(time.RFC3339), ExpiresAt: f.now.Add(2 * time.Hour).UTC().Format(time.RFC3339),
		DelegationDepth: depth, Nonce: base64.RawURLEncoding.EncodeToString(nonce[:]),
		PolicyEpoch: f.subject.PolicyEpoch, IssuerKeyID: keyID,
	}
}

func (f *authorityFixture) rootGrant() Grant {
	return f.grant(f.owner, f.delegate, []string{"archive.delegate", "archive.view"}, "1")
}

func cloneContext(input VerificationContext) VerificationContext {
	output := input
	output.Identity.CurrentKeys = make(map[string]ed25519.PublicKey, len(input.Identity.CurrentKeys))
	for key, value := range input.Identity.CurrentKeys {
		output.Identity.CurrentKeys[key] = append(ed25519.PublicKey(nil), value...)
	}
	output.RevokedGrantHashes = make(map[string]struct{}, len(input.RevokedGrantHashes))
	for hash := range input.RevokedGrantHashes {
		output.RevokedGrantHashes[hash] = struct{}{}
	}
	return output
}

func mustSign(t *testing.T, grant Grant, key ed25519.PrivateKey) SignedGrant {
	t.Helper()
	signed, err := SignGrant(grant, key)
	if err != nil {
		t.Fatal(err)
	}
	return signed
}

func stringsReader(value string) *fixedReader { return &fixedReader{value: []byte(value)} }

type fixedReader struct{ value []byte }

func (r *fixedReader) Read(dst []byte) (int, error) {
	if len(r.value) == 0 {
		return 0, fmt.Errorf("entropy exhausted")
	}
	n := copy(dst, r.value)
	r.value = r.value[n:]
	return n, nil
}
