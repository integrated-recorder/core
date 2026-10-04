package storage

import (
	"context"
	"errors"
	"io"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	// MaxObjectBytes is the largest physical archive object accepted by Core.
	MaxObjectBytes       int64 = 1 << 30
	maxObjectPageSize          = 1000
	maxObjectEnumeration       = 200_000
	objectCallTimeout          = 5 * time.Minute
)

var ErrObjectNotFound = errors.New("storage object not found")

// PhysicalObjectInfo is provider-neutral metadata for one complete object.
// A successful Put must report the exact key, size, and lowercase SHA-256 of
// the bytes it atomically published.
type PhysicalObjectInfo struct {
	Key        string
	Size       int64
	SHA256     string
	ModifiedAt time.Time
}

// PhysicalObjectPage is sorted by key. NextCursor is the last key in this
// page, or empty when there are no more objects.
type PhysicalObjectPage struct {
	Items      []PhysicalObjectInfo
	NextCursor string
}

// PhysicalObjectStore is the lower-level byte placement boundary. Put must
// consume exactly size bytes and publish the whole object atomically: after a
// failure, the prior complete value or no value is visible; after success, the
// complete new value is visible. Implementations must not expose partial
// canonical objects under key.
type PhysicalObjectStore interface {
	Put(context.Context, string, io.Reader, int64) (PhysicalObjectInfo, error)
	Open(context.Context, string) (io.ReadCloser, PhysicalObjectInfo, error)
	OpenRange(context.Context, string, int64, int64) (io.ReadCloser, PhysicalObjectInfo, error)
	Stat(context.Context, string) (PhysicalObjectInfo, error)
	List(context.Context, string, string, int) (PhysicalObjectPage, error)
	Delete(context.Context, string) error
}

// PhysicalObjectStoreIdentity is optional process metadata used only for the
// bounded primary-storage telemetry projection. It does not grant provider
// authority over archive semantics.
type PhysicalObjectStoreIdentity interface {
	StorageProviderIdentity() (id, name string)
}

// ValidateObjectKey applies the protocol's logical slash-key rules before a
// key reaches a physical provider. Keys are opaque UTF-8 identifiers; no
// filesystem path interpretation is performed.
func ValidateObjectKey(key string) error {
	if key == "" || len(key) > 1024 || !utf8.ValidString(key) || strings.HasPrefix(key, "/") || strings.ContainsAny(key, "\\\x00") {
		return errors.New("invalid storage object key")
	}
	for _, r := range key {
		if r < 0x20 || r == 0x7f {
			return errors.New("invalid storage object key")
		}
	}
	for _, component := range strings.Split(key, "/") {
		if component == "" || component == "." || component == ".." {
			return errors.New("invalid storage object key")
		}
	}
	return nil
}

func validateObjectPrefix(prefix string) error {
	if prefix == "" {
		return nil
	}
	key := prefix
	// Prefixes may name a complete logical directory namespace. The trailing
	// slash is not an object-key component and is needed for safe boundary
	// matching such as recordings/<id>/.
	if strings.HasSuffix(key, "/") {
		key = strings.TrimSuffix(key, "/")
	}
	if err := ValidateObjectKey(key); err != nil {
		return err
	}
	return nil
}
