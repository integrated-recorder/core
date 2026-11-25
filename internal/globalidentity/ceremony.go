package globalidentity

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/go-webauthn/webauthn/protocol"
	"github.com/go-webauthn/webauthn/webauthn"
	"github.com/integrated-recorder/core/internal/identityprotocol"
)

const (
	ceremonyLifetime        = 5 * time.Minute
	identitySessionLifetime = 12 * time.Hour
	maxPendingCeremonies    = 256
)

var (
	ErrCeremonyExpired  = errors.New("global identity ceremony expired or unavailable")
	ErrNoCredentials    = errors.New("global identity has no passkeys")
	ErrCeremonyCapacity = errors.New("global identity ceremony capacity reached")
)

type ceremony struct {
	kind              string
	webAuthn          *webauthn.WebAuthn
	session           *webauthn.SessionData
	user              webauthn.User
	origin            string
	identityID        string
	playbackPublicKey []byte
	initial           bool
	expiresAt         time.Time
}

type identitySession struct {
	identityID        string
	historyHead       string
	csrfToken         string
	playbackPublicKey []byte
	expiresAt         time.Time
}

// CeremonyManager holds only short-lived WebAuthn challenge state and
// process-local global-identity sessions. The recovery phrase and all
// recovery-derived private keys remain in the user's browser.
type CeremonyManager struct {
	store    *Store
	mu       sync.Mutex
	pending  map[string]ceremony
	sessions map[string]identitySession
	now      func() time.Time
	random   io.Reader
}

func NewCeremonyManager(store *Store) *CeremonyManager {
	return &CeremonyManager{
		store:    store,
		pending:  make(map[string]ceremony),
		sessions: make(map[string]identitySession),
		now:      time.Now,
		random:   rand.Reader,
	}
}

func (m *CeremonyManager) BeginRegistration(rpID, origin, identityID string) (string, *protocol.CredentialCreation, error) {
	if m == nil || m.store == nil || !strings.HasPrefix(identityID, "gi_") || len(identityID) > 64 {
		return "", nil, ErrInvalidGenesis
	}
	user, err := m.store.WebAuthnUser(identityID)
	initial := false
	if errors.Is(err, ErrNotInitialized) {
		if _, exists, snapshotErr := m.store.Snapshot(); snapshotErr != nil {
			return "", nil, snapshotErr
		} else if exists {
			return "", nil, ErrInvalidGenesis
		}
		initial = true
		handle := make([]byte, 32)
		if _, err := io.ReadFull(m.random, handle); err != nil {
			return "", nil, ErrCeremonyCapacity
		}
		user = webauthnUser{handle: handle, id: identityID}
	} else if err != nil {
		return "", nil, err
	} else {
		snapshot, exists, snapshotErr := m.store.Snapshot()
		if snapshotErr != nil {
			return "", nil, snapshotErr
		}
		if !exists {
			return "", nil, ErrNotInitialized
		}
		if snapshot.Frozen {
			return "", nil, ErrIdentityFrozen
		}
	}
	wa, err := newWebAuthn(rpID, origin)
	if err != nil {
		return "", nil, err
	}
	options, session, err := wa.BeginRegistration(user,
		webauthn.WithResidentKeyRequirement(protocol.ResidentKeyRequirementRequired),
		webauthn.WithExclusions(webauthn.Credentials(user.WebAuthnCredentials()).CredentialDescriptors()),
	)
	if err != nil {
		return "", nil, err
	}
	token, err := m.add(ceremony{kind: "registration", webAuthn: wa, session: session, user: user, origin: origin, identityID: identityID, initial: initial, expiresAt: m.now().Add(ceremonyLifetime)})
	if err != nil {
		return "", nil, err
	}
	return token, options, nil
}

func (m *CeremonyManager) FinishRegistration(token, origin string, request *http.Request, canonicalEvent, signature []byte) (Snapshot, error) {
	ceremony, err := m.take(token, "registration", origin)
	if err != nil {
		return Snapshot{}, err
	}
	credential, err := ceremony.webAuthn.FinishRegistration(ceremony.user, *ceremony.session, request)
	if err != nil {
		return Snapshot{}, ErrInvalidGenesis
	}
	verified, err := parseEvent(canonicalEvent, signature)
	if err != nil || verified.IdentityID != ceremony.identityID {
		return Snapshot{}, ErrInvalidGenesis
	}
	if ceremony.initial {
		err = m.store.CreateGenesis(canonicalEvent, signature, ceremony.user.WebAuthnID(), *credential, ceremony.session.Challenge)
	} else {
		err = m.store.AddCredential(canonicalEvent, signature, ceremony.session.Challenge, *credential)
	}
	if err != nil {
		return Snapshot{}, err
	}
	snapshot, exists, err := m.store.Snapshot()
	if err != nil || !exists {
		return Snapshot{}, ErrCorruptStore
	}
	return snapshot, nil
}

func (m *CeremonyManager) BeginLogin(rpID, origin string, playbackPublicKey ...[]byte) (string, *protocol.CredentialAssertion, error) {
	if m == nil || m.store == nil {
		return "", nil, ErrNotInitialized
	}
	snapshot, exists, err := m.store.Snapshot()
	if err != nil {
		return "", nil, err
	}
	if !exists {
		return "", nil, ErrNotInitialized
	}
	if snapshot.Frozen {
		return "", nil, ErrIdentityFrozen
	}
	user, err := m.store.WebAuthnUser("")
	if err != nil {
		return "", nil, err
	}
	if len(user.WebAuthnCredentials()) == 0 {
		return "", nil, ErrNoCredentials
	}
	wa, err := newWebAuthn(rpID, origin)
	if err != nil {
		return "", nil, err
	}
	if len(playbackPublicKey) > 1 {
		return "", nil, ErrInvalidGenesis
	}
	var ephemeralKey []byte
	loginOptions := []webauthn.LoginOption{webauthn.WithUserVerification(protocol.VerificationRequired)}
	if len(playbackPublicKey) == 1 {
		if len(playbackPublicKey[0]) != 32 {
			return "", nil, ErrInvalidGenesis
		}
		ephemeralKey = append([]byte(nil), playbackPublicKey[0]...)
		var nonce [32]byte
		if _, err := io.ReadFull(m.random, nonce[:]); err != nil {
			clear(ephemeralKey)
			return "", nil, ErrCeremonyCapacity
		}
		challengeInput := append([]byte("IR/passkey-playback-key/v1\x00"), nonce[:]...)
		challengeInput = append(challengeInput, ephemeralKey...)
		challenge := sha256.Sum256(challengeInput)
		clear(nonce[:])
		clear(challengeInput)
		loginOptions = append(loginOptions, webauthn.WithChallenge(challenge[:]))
	}
	options, session, err := wa.BeginDiscoverableLogin(loginOptions...)
	if err != nil {
		clear(ephemeralKey)
		return "", nil, err
	}
	token, err := m.add(ceremony{kind: "login", webAuthn: wa, session: session, user: user, origin: origin, identityID: user.WebAuthnName(), playbackPublicKey: ephemeralKey, expiresAt: m.now().Add(ceremonyLifetime)})
	if err != nil {
		clear(ephemeralKey)
		return "", nil, err
	}
	return token, options, nil
}

func (m *CeremonyManager) FinishLogin(token, origin string, request *http.Request) (string, string, string, error) {
	ceremony, err := m.take(token, "login", origin)
	if err != nil {
		return "", "", "", err
	}
	user, credential, err := ceremony.webAuthn.FinishPasskeyLogin(func(rawID, userHandle []byte) (webauthn.User, error) {
		storedUser, err := m.store.WebAuthnUser("")
		if err != nil {
			return nil, err
		}
		if !bytes.Equal(storedUser.WebAuthnID(), userHandle) {
			return nil, ErrNotInitialized
		}
		for _, storedCredential := range storedUser.WebAuthnCredentials() {
			if bytes.Equal(storedCredential.ID, rawID) {
				return storedUser, nil
			}
		}
		return nil, ErrCredentialNotFound
	}, *ceremony.session, request)
	if err != nil || credential == nil || user == nil || user.WebAuthnName() != ceremony.identityID {
		return "", "", "", ErrCeremonyExpired
	}
	if err := m.store.ReplaceCredential(*credential); err != nil {
		return "", "", "", err
	}
	snapshot, exists, err := m.store.Snapshot()
	if err != nil || !exists || snapshot.Frozen {
		return "", "", "", ErrIdentityFrozen
	}
	credentialID := base64.RawURLEncoding.EncodeToString(credential.ID)
	credentialActive := false
	for _, activeCredentialID := range snapshot.CredentialIDs {
		if activeCredentialID == credentialID {
			credentialActive = true
			break
		}
	}
	if !credentialActive {
		return "", "", "", ErrCredentialNotFound
	}
	tokenValue, err := randomToken(m.random)
	if err != nil {
		return "", "", "", err
	}
	csrfValue, err := randomToken(m.random)
	if err != nil {
		return "", "", "", err
	}
	m.mu.Lock()
	m.sessions[tokenValue] = identitySession{
		identityID: user.WebAuthnName(), historyHead: snapshot.HeadHash, csrfToken: csrfValue,
		playbackPublicKey: append([]byte(nil), ceremony.playbackPublicKey...), expiresAt: m.now().Add(identitySessionLifetime),
	}
	m.mu.Unlock()
	clear(ceremony.playbackPublicKey)
	return user.WebAuthnName(), tokenValue, csrfValue, nil
}

func (m *CeremonyManager) Session(token string) (string, string, bool) {
	if m == nil || m.store == nil || token == "" {
		return "", "", false
	}
	identityID, csrfToken := "", ""
	authenticated := false
	err := m.store.withSnapshot(func(snapshot Snapshot, exists bool) {
		m.mu.Lock()
		defer m.mu.Unlock()
		if !exists || snapshot.Frozen {
			clear(m.sessions)
			return
		}
		session, ok := m.sessions[token]
		if !ok {
			return
		}
		if !session.expiresAt.After(m.now()) || session.historyHead == "" || session.historyHead != snapshot.HeadHash {
			delete(m.sessions, token)
			return
		}
		identityID, csrfToken, authenticated = session.identityID, session.csrfToken, true
	})
	if err != nil {
		m.mu.Lock()
		clear(m.sessions)
		m.mu.Unlock()
		return "", "", false
	}
	return identityID, csrfToken, authenticated
}

// SessionPlaybackPublicKey returns the public half of a short-lived browser
// playback key only for a currently valid Passkey session that bound that key
// into its signed WebAuthn challenge. The private half never leaves the
// browser. Callers must still enforce the current OwnerSubject capability.
func (m *CeremonyManager) SessionPlaybackPublicKey(token string) ([]byte, bool) {
	return m.sessionPlaybackPublicKey(token, false)
}

// TakeSessionPlaybackPublicKey atomically returns and consumes the ephemeral
// recipient key bound into a Passkey assertion. A session can request at most
// one online archive-key envelope for that browser key; a new envelope needs
// a new Passkey ceremony and a fresh in-memory key pair.
func (m *CeremonyManager) TakeSessionPlaybackPublicKey(token string) ([]byte, bool) {
	return m.sessionPlaybackPublicKey(token, true)
}

func (m *CeremonyManager) sessionPlaybackPublicKey(token string, consume bool) ([]byte, bool) {
	if m == nil || m.store == nil || token == "" {
		return nil, false
	}
	var publicKey []byte
	valid := false
	err := m.store.withSnapshot(func(snapshot Snapshot, exists bool) {
		m.mu.Lock()
		defer m.mu.Unlock()
		if !exists || snapshot.Frozen {
			clear(m.sessions)
			return
		}
		session, ok := m.sessions[token]
		if !ok {
			return
		}
		if !session.expiresAt.After(m.now()) || session.historyHead == "" || session.historyHead != snapshot.HeadHash {
			delete(m.sessions, token)
			return
		}
		if len(session.playbackPublicKey) != 32 {
			return
		}
		publicKey = append([]byte(nil), session.playbackPublicKey...)
		if consume {
			clear(session.playbackPublicKey)
			session.playbackPublicKey = nil
			m.sessions[token] = session
		}
		valid = true
	})
	if err != nil || !valid {
		clear(publicKey)
		return nil, false
	}
	return publicKey, true
}

// RevokeCredential commits the recovery-root-signed history event and
// invalidates all local global-identity sessions. A stale authorization
// session must not outlive a credential-history change.
func (m *CeremonyManager) RevokeCredential(canonicalEvent, signature []byte, credentialID string) error {
	if m == nil || m.store == nil {
		return ErrNotInitialized
	}
	if err := m.store.RevokeCredential(canonicalEvent, signature, credentialID); err != nil {
		return err
	}
	m.mu.Lock()
	clear(m.sessions)
	m.mu.Unlock()
	return nil
}

func (m *CeremonyManager) ValidCSRF(sessionToken, supplied string) bool {
	if m == nil || sessionToken == "" || supplied == "" {
		return false
	}
	_, expected, ok := m.Session(sessionToken)
	return ok && subtle.ConstantTimeCompare([]byte(expected), []byte(supplied)) == 1
}

func (m *CeremonyManager) Logout(token string) {
	if m == nil || token == "" {
		return
	}
	m.mu.Lock()
	delete(m.sessions, token)
	m.mu.Unlock()
}

func (m *CeremonyManager) add(value ceremony) (string, error) {
	token, err := randomToken(m.random)
	if err != nil {
		return "", err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	for key, pending := range m.pending {
		if !pending.expiresAt.After(now) {
			delete(m.pending, key)
		}
	}
	if len(m.pending) >= maxPendingCeremonies {
		return "", ErrCeremonyCapacity
	}
	m.pending[token] = value
	return token, nil
}

func (m *CeremonyManager) take(token, kind, origin string) (ceremony, error) {
	if m == nil || token == "" {
		return ceremony{}, ErrCeremonyExpired
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	value, ok := m.pending[token]
	delete(m.pending, token)
	if !ok || value.kind != kind || value.origin != origin || !value.expiresAt.After(m.now()) {
		return ceremony{}, ErrCeremonyExpired
	}
	return value, nil
}

func newWebAuthn(rpID, origin string) (*webauthn.WebAuthn, error) {
	return webauthn.New(&webauthn.Config{
		RPDisplayName:         "Integrated Recorder",
		RPID:                  rpID,
		RPOrigins:             []string{origin},
		AttestationPreference: protocol.PreferNoAttestation,
		AuthenticatorSelection: protocol.AuthenticatorSelection{
			ResidentKey:      protocol.ResidentKeyRequirementRequired,
			UserVerification: protocol.VerificationRequired,
		},
		Timeouts: webauthn.TimeoutsConfig{
			Registration: webauthn.TimeoutConfig{Enforce: true, Timeout: ceremonyLifetime},
			Login:        webauthn.TimeoutConfig{Enforce: true, Timeout: ceremonyLifetime},
		},
	})
}

func randomToken(source io.Reader) (string, error) {
	bytes := make([]byte, 32)
	if _, err := io.ReadFull(source, bytes); err != nil {
		return "", ErrCeremonyCapacity
	}
	return base64.RawURLEncoding.EncodeToString(bytes), nil
}

func parseEvent(canonicalEvent, signature []byte) (identityEventView, error) {
	verified, err := identityprotocol.VerifyEvent(canonicalEvent, signature)
	if err != nil {
		return identityEventView{}, err
	}
	return identityEventView{IdentityID: verified.IdentityID}, nil
}

type identityEventView struct {
	IdentityID string
}
