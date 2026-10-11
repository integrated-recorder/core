// Package storageproto defines the process boundary for trusted storage
// provider executables. It contains physical object I/O only; archive meaning
// and canonical write authority remain owned by Core.
package storageproto

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"
)

const (
	Version                    = 1
	MaxControlFrameBytes       = 64 << 10
	MaxConfigBytes             = 1 << 20
	MaxObjectBytes       int64 = 1 << 30
	MaxListLimit               = 1000
)

var (
	ErrUnsupported  = errors.New("storage provider operation is unsupported")
	ErrNotFound     = errors.New("storage object not found")
	ErrInvalidKey   = errors.New("storage object key is invalid")
	ErrInvalidRange = errors.New("storage object range is invalid")
	ErrProtocol     = errors.New("storage provider protocol error")
	ErrClosed       = errors.New("storage provider process is closed")
	// ErrUnframedResponse identifies a legacy provider's plain-text 502 response.
	// LIST callers may retry it with a smaller page while still validating every
	// returned key and cursor.
	ErrUnframedResponse = errors.New("storage provider returned an unframed error response")
)

var (
	idPattern        = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)
	configKeyPattern = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,63}$`)
	versionPattern   = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9.+_-]{0,127}$`)
	keyControl       = regexp.MustCompile(`[\x00-\x1f\x7f]`)
)

const (
	CapabilityRead          = "object.read"
	CapabilityWrite         = "object.write"
	CapabilityStat          = "object.stat"
	CapabilityList          = "object.list"
	CapabilityDelete        = "object.delete"
	CapabilityRangeRead     = "object.range_read"
	CapabilityAtomicReplace = "atomic_replace"
)

var requiredCapabilities = []string{
	CapabilityRead, CapabilityWrite, CapabilityStat, CapabilityList,
	CapabilityDelete, CapabilityRangeRead, CapabilityAtomicReplace,
}

type Descriptor struct {
	ProtocolVersion     int      `json:"protocol_version"`
	ID                  string   `json:"id"`
	Name                string   `json:"name"`
	Version             string   `json:"version"`
	ConfigurationSchema Schema   `json:"configuration_schema"`
	Capabilities        []string `json:"capabilities"`
}

type Schema struct {
	Fields []Field `json:"fields"`
}

type Field struct {
	Key         string          `json:"key"`
	Control     string          `json:"control"`
	Label       string          `json:"label"`
	Description string          `json:"description,omitempty"`
	Required    bool            `json:"required,omitempty"`
	Default     json.RawMessage `json:"default,omitempty"`
	Options     []Option        `json:"options,omitempty"`
	Constraints *Constraints    `json:"constraints,omitempty"`
}

type Option struct {
	Value any    `json:"value"`
	Label string `json:"label"`
}

type Constraints struct {
	Min       *float64 `json:"min,omitempty"`
	Max       *float64 `json:"max,omitempty"`
	MinLength *int     `json:"min_length,omitempty"`
	MaxLength *int     `json:"max_length,omitempty"`
}

type Config struct {
	Values  map[string]json.RawMessage `json:"values,omitempty"`
	Secrets map[string]string          `json:"secrets,omitempty"`
}

type ObjectInfo struct {
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

type ObjectEntry struct {
	Key    string `json:"key"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256,omitempty"`
}

type ListPage struct {
	Items      []ObjectEntry `json:"items"`
	NextCursor string        `json:"next_cursor,omitempty"`
}

// Provider is implemented by an external storage executable. Put must publish
// a complete object atomically: it must never expose partial bytes at key.
// Provider errors returned by an implementation are translated to bounded
// protocol error codes at the process boundary.
type Provider interface {
	Descriptor() Descriptor
	Configure(context.Context, Config) error
	Probe(context.Context) error
	Put(context.Context, string, io.Reader, int64) (ObjectInfo, error)
	Open(context.Context, string) (io.ReadCloser, ObjectInfo, error)
	OpenRange(context.Context, string, int64, int64) (io.ReadCloser, ObjectInfo, error)
	Stat(context.Context, string) (ObjectInfo, error)
	List(context.Context, string, string, int) (ListPage, error)
	Delete(context.Context, string) error
}

func ValidateDescriptor(d Descriptor) error {
	if d.ProtocolVersion != Version || !idPattern.MatchString(d.ID) || !validText(d.Name, 1, 128) || !versionPattern.MatchString(d.Version) {
		return fmt.Errorf("%w: invalid descriptor identity", ErrProtocol)
	}
	if len(d.Capabilities) != len(requiredCapabilities) {
		return fmt.Errorf("%w: required capability set is incomplete", ErrProtocol)
	}
	seen := map[string]bool{}
	for _, capability := range d.Capabilities {
		if seen[capability] {
			return fmt.Errorf("%w: duplicate capability", ErrProtocol)
		}
		seen[capability] = true
	}
	for _, capability := range requiredCapabilities {
		if !seen[capability] {
			return fmt.Errorf("%w: required capability missing", ErrProtocol)
		}
	}
	if err := ValidateSchema(d.ConfigurationSchema); err != nil {
		return err
	}
	encoded, err := json.Marshal(d)
	if err != nil || len(encoded) > MaxControlFrameBytes {
		return fmt.Errorf("%w: descriptor exceeds frame limit", ErrProtocol)
	}
	return nil
}

func ValidateSchema(s Schema) error {
	if s.Fields == nil || len(s.Fields) > 64 {
		return fmt.Errorf("%w: too many configuration fields", ErrProtocol)
	}
	seen := map[string]bool{}
	for _, field := range s.Fields {
		if !configKeyPattern.MatchString(field.Key) || !validText(field.Label, 1, 128) || len(field.Description) > 512 || !utf8.ValidString(field.Description) {
			return fmt.Errorf("%w: invalid configuration field", ErrProtocol)
		}
		if seen[field.Key] {
			return fmt.Errorf("%w: duplicate configuration field", ErrProtocol)
		}
		seen[field.Key] = true
		switch field.Control {
		case "text", "secret", "boolean", "number", "select":
		default:
			return fmt.Errorf("%w: unsupported configuration control", ErrProtocol)
		}
		if field.Control == "secret" && len(field.Default) != 0 {
			return fmt.Errorf("%w: secret defaults are forbidden", ErrProtocol)
		}
		if len(field.Default) > 4096 || (len(field.Default) != 0 && !json.Valid(field.Default)) {
			return fmt.Errorf("%w: invalid configuration default", ErrProtocol)
		}
		if field.Control == "select" && len(field.Options) == 0 || field.Control != "select" && len(field.Options) != 0 {
			return fmt.Errorf("%w: invalid configuration options", ErrProtocol)
		}
		if len(field.Options) > 128 {
			return fmt.Errorf("%w: too many configuration options", ErrProtocol)
		}
		seenOptions := map[string]bool{}
		for _, option := range field.Options {
			if !validText(option.Label, 1, 128) || option.Value == nil {
				return fmt.Errorf("%w: invalid configuration option", ErrProtocol)
			}
			encoded, err := json.Marshal(option.Value)
			if err != nil || len(encoded) > 1024 {
				return fmt.Errorf("%w: invalid configuration option", ErrProtocol)
			}
			var scalar any
			decoder := json.NewDecoder(bytes.NewReader(encoded))
			decoder.UseNumber()
			if decoder.Decode(&scalar) != nil {
				return fmt.Errorf("%w: invalid configuration option", ErrProtocol)
			}
			switch scalar.(type) {
			case string, json.Number, bool:
			default:
				return fmt.Errorf("%w: invalid configuration option", ErrProtocol)
			}
			canonical, _ := json.Marshal(scalar)
			if seenOptions[string(canonical)] {
				return fmt.Errorf("%w: duplicate configuration option", ErrProtocol)
			}
			seenOptions[string(canonical)] = true
		}
		if c := field.Constraints; c != nil {
			if c.Min != nil && c.Max != nil && *c.Min > *c.Max || c.MinLength != nil && *c.MinLength < 0 || c.MaxLength != nil && *c.MaxLength < 0 || c.MinLength != nil && c.MaxLength != nil && *c.MinLength > *c.MaxLength {
				return fmt.Errorf("%w: invalid configuration constraints", ErrProtocol)
			}
		}
		if len(field.Default) != 0 && !validSchemaValue(field, field.Default) {
			return fmt.Errorf("%w: invalid configuration default", ErrProtocol)
		}
	}
	return nil
}

func Fingerprint(d Descriptor) (string, error) {
	if err := ValidateDescriptor(d); err != nil {
		return "", err
	}
	copy := d
	copy.Capabilities = append([]string(nil), d.Capabilities...)
	sort.Strings(copy.Capabilities)
	b, err := json.Marshal(copy)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

func ValidateKey(key string) error {
	if len(key) == 0 || len(key) > 1024 || !utf8.ValidString(key) || strings.HasPrefix(key, "/") || strings.Contains(key, `\`) || keyControl.MatchString(key) {
		return ErrInvalidKey
	}
	for _, component := range strings.Split(key, "/") {
		if component == "" || component == "." || component == ".." {
			return ErrInvalidKey
		}
	}
	return nil
}

func ValidatePrefix(prefix string) error {
	if prefix == "" {
		return nil
	}
	key := prefix
	if strings.HasSuffix(key, "/") {
		key = strings.TrimSuffix(key, "/")
	}
	if err := ValidateKey(key); err != nil {
		return err
	}
	return nil
}

func validText(value string, min, max int) bool {
	return len(value) >= min && len(value) <= max && utf8.ValidString(value) && !strings.ContainsRune(value, 0) && !keyControl.MatchString(value)
}

func validSchemaValue(field Field, raw json.RawMessage) bool {
	var value any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if decoder.Decode(&value) != nil {
		return false
	}
	switch field.Control {
	case "text":
		text, ok := value.(string)
		if !ok || !validText(text, 0, 16<<10) {
			return false
		}
		if field.Constraints != nil {
			if field.Constraints.MinLength != nil && len(text) < *field.Constraints.MinLength || field.Constraints.MaxLength != nil && len(text) > *field.Constraints.MaxLength {
				return false
			}
		}
		return true
	case "boolean":
		_, ok := value.(bool)
		return ok
	case "number":
		number, ok := value.(json.Number)
		if !ok {
			return false
		}
		parsed, err := number.Float64()
		if err != nil {
			return false
		}
		if field.Constraints != nil {
			if field.Constraints.Min != nil && parsed < *field.Constraints.Min || field.Constraints.Max != nil && parsed > *field.Constraints.Max {
				return false
			}
		}
		return true
	case "select":
		for _, option := range field.Options {
			encoded, _ := json.Marshal(option.Value)
			var candidate any
			d := json.NewDecoder(bytes.NewReader(encoded))
			d.UseNumber()
			if d.Decode(&candidate) == nil && reflectValueEqual(value, candidate) {
				return true
			}
		}
		return false
	default:
		return false
	}
}

func reflectValueEqual(left, right any) bool {
	leftJSON, leftErr := json.Marshal(left)
	rightJSON, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftJSON, rightJSON)
}
