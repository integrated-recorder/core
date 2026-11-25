package domain

import (
	"crypto/rand"
	"encoding/base64"
	"io"
	"strings"
)

// NewArchiveID creates a random, installation-independent logical archive
// identity. It is written into the recording root at creation and must not be
// replaced when the archive is copied to another node with a different local
// Recording ID.
func NewArchiveID() (string, error) {
	var entropy [16]byte
	if _, err := io.ReadFull(rand.Reader, entropy[:]); err != nil {
		return "", err
	}
	return "arc_" + base64.RawURLEncoding.EncodeToString(entropy[:]), nil
}

// ValidArchiveID reports whether value has the canonical v1 logical archive
// identifier form.
func ValidArchiveID(value string) bool {
	const prefix = "arc_"
	if len(value) != len(prefix)+22 || !strings.HasPrefix(value, prefix) {
		return false
	}
	encoded := value[len(prefix):]
	decoded, err := base64.RawURLEncoding.DecodeString(encoded)
	return err == nil && len(decoded) == 16 && base64.RawURLEncoding.EncodeToString(decoded) == encoded
}
