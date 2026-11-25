package identityprotocol

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sort"
	"unicode/utf8"
)

const maxCanonicalDocumentBytes = 1 << 20

var schemaKey = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

// Canonicalize applies the bounded ir.identity.event/1 JSON profile and RFC
// 8785-compatible serialization to a single JSON value. The profile rejects
// duplicate keys, numbers, invalid Unicode, non-schema property names, and
// trailing values before producing bytes that may be signed.
func Canonicalize(input []byte) ([]byte, error) {
	if len(input) == 0 || len(input) > maxCanonicalDocumentBytes {
		return nil, errors.New("identity protocol document size is invalid")
	}
	value, err := decodeStrictJSON(input)
	if err != nil {
		return nil, err
	}
	return canonicalJSON(value)
}

func decodeStrictJSON(data []byte) (any, error) {
	if !utf8.Valid(data) || !validUnicodeEscapes(data) {
		return nil, errors.New("invalid UTF-8 or Unicode escape")
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	value, err := decodeValue(dec, 0)
	if err != nil {
		return nil, err
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, errors.New("trailing JSON value")
	}
	return value, nil
}

func decodeValue(dec *json.Decoder, depth int) (any, error) {
	if depth > 64 {
		return nil, errors.New("identity protocol JSON nesting is too deep")
	}
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	switch value := tok.(type) {
	case json.Delim:
		switch value {
		case '{':
			object := make(map[string]any)
			for dec.More() {
				keyToken, err := dec.Token()
				if err != nil {
					return nil, err
				}
				key, ok := keyToken.(string)
				if !ok {
					return nil, errors.New("object key is not a string")
				}
				if _, exists := object[key]; exists {
					return nil, errors.New("duplicate object property")
				}
				item, err := decodeValue(dec, depth+1)
				if err != nil {
					return nil, err
				}
				object[key] = item
			}
			_, err := dec.Token()
			return object, err
		case '[':
			var array []any
			for dec.More() {
				item, err := decodeValue(dec, depth+1)
				if err != nil {
					return nil, err
				}
				array = append(array, item)
			}
			_, err := dec.Token()
			return array, err
		default:
			return nil, errors.New("unexpected JSON delimiter")
		}
	case json.Number:
		return nil, errors.New("numbers are forbidden by this profile")
	case string, bool, nil:
		return value, nil
	default:
		return nil, fmt.Errorf("unsupported protocol JSON token %T", tok)
	}
}

func canonicalJSON(value any) ([]byte, error) {
	var out bytes.Buffer
	if err := appendCanonical(&out, value); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

func appendCanonical(out *bytes.Buffer, value any) error {
	switch v := value.(type) {
	case nil:
		out.WriteString("null")
	case bool:
		if v {
			out.WriteString("true")
		} else {
			out.WriteString("false")
		}
	case string:
		return appendJSONString(out, v)
	case []any:
		out.WriteByte('[')
		for i, item := range v {
			if i > 0 {
				out.WriteByte(',')
			}
			if err := appendCanonical(out, item); err != nil {
				return err
			}
		}
		out.WriteByte(']')
	case map[string]any:
		keys := make([]string, 0, len(v))
		for key := range v {
			if !schemaKey.MatchString(key) {
				return fmt.Errorf("property name %q is outside the protocol profile", key)
			}
			keys = append(keys, key)
		}
		sort.Strings(keys)
		out.WriteByte('{')
		for i, key := range keys {
			if i > 0 {
				out.WriteByte(',')
			}
			if err := appendJSONString(out, key); err != nil {
				return err
			}
			out.WriteByte(':')
			if err := appendCanonical(out, v[key]); err != nil {
				return err
			}
		}
		out.WriteByte('}')
	case json.Number, float64, float32, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return errors.New("numeric values are forbidden; encode integers as decimal strings")
	default:
		return fmt.Errorf("unsupported protocol JSON value %T", value)
	}
	return nil
}

func appendJSONString(out *bytes.Buffer, value string) error {
	if !utf8.ValidString(value) {
		return errors.New("invalid UTF-8 string")
	}
	var encoded bytes.Buffer
	encoder := json.NewEncoder(&encoded)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return err
	}
	quoted := bytes.TrimSuffix(encoded.Bytes(), []byte("\n"))
	quoted = bytes.ReplaceAll(quoted, []byte(`\u2028`), []byte("\u2028"))
	quoted = bytes.ReplaceAll(quoted, []byte(`\u2029`), []byte("\u2029"))
	out.Write(quoted)
	return nil
}

// validUnicodeEscapes rejects lone UTF-16 surrogates before encoding/json can
// replace them with U+FFFD. It skips ordinary escaped bytes and examines only
// JSON's \uXXXX escape sequences.
func validUnicodeEscapes(data []byte) bool {
	inString := false
	for i := 0; i < len(data); i++ {
		if !inString {
			if data[i] == '"' {
				inString = true
			}
			continue
		}
		if data[i] == '"' {
			inString = false
			continue
		}
		if data[i] != '\\' {
			continue
		}
		i++
		if i >= len(data) {
			return false
		}
		if data[i] != 'u' {
			continue
		}
		if i+4 >= len(data) {
			return false
		}
		code, err := hex.DecodeString(string(data[i+1 : i+5]))
		if err != nil {
			return false
		}
		unit := uint16(code[0])<<8 | uint16(code[1])
		if unit >= 0xdc00 && unit <= 0xdfff {
			return false
		}
		if unit >= 0xd800 && unit <= 0xdbff {
			if i+10 >= len(data) || data[i+5] != '\\' || data[i+6] != 'u' {
				return false
			}
			lowBytes, err := hex.DecodeString(string(data[i+7 : i+11]))
			if err != nil {
				return false
			}
			low := uint16(lowBytes[0])<<8 | uint16(lowBytes[1])
			if low < 0xdc00 || low > 0xdfff {
				return false
			}
			i += 10
		}
	}
	return !inString
}
