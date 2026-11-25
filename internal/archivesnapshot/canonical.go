package archivesnapshot

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"unicode/utf16"
	"unicode/utf8"
)

func canonicalizeSnapshotJSON(input []byte) ([]byte, error) {
	if len(input) == 0 || len(input) > maxSnapshotBytes || !utf8.Valid(input) || !validSnapshotUnicodeEscapes(input) {
		return nil, errors.New("snapshot JSON encoding is invalid")
	}
	decoder := json.NewDecoder(bytes.NewReader(input))
	decoder.UseNumber()
	value, err := decodeSnapshotJSON(decoder, 0)
	if err != nil {
		return nil, err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return nil, errors.New("snapshot has trailing JSON")
	}
	var output bytes.Buffer
	if err := appendSnapshotCanonical(&output, value); err != nil {
		return nil, err
	}
	return output.Bytes(), nil
}

func decodeSnapshotJSON(decoder *json.Decoder, depth int) (any, error) {
	if depth > 64 {
		return nil, errors.New("snapshot JSON nesting is too deep")
	}
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	if delimiter, ok := token.(json.Delim); ok {
		switch delimiter {
		case '{':
			object := map[string]any{}
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return nil, err
				}
				key, ok := keyToken.(string)
				if !ok {
					return nil, errors.New("snapshot object key is invalid")
				}
				if _, exists := object[key]; exists {
					return nil, errors.New("snapshot contains a duplicate property")
				}
				value, err := decodeSnapshotJSON(decoder, depth+1)
				if err != nil {
					return nil, err
				}
				object[key] = value
			}
			closeToken, err := decoder.Token()
			if err != nil || closeToken != json.Delim('}') {
				return nil, errors.New("snapshot object is incomplete")
			}
			return object, nil
		case '[':
			array := []any{}
			for decoder.More() {
				value, err := decodeSnapshotJSON(decoder, depth+1)
				if err != nil {
					return nil, err
				}
				array = append(array, value)
			}
			closeToken, err := decoder.Token()
			if err != nil || closeToken != json.Delim(']') {
				return nil, errors.New("snapshot array is incomplete")
			}
			return array, nil
		default:
			return nil, errors.New("snapshot JSON delimiter is invalid")
		}
	}
	switch value := token.(type) {
	case string, bool, nil, json.Number:
		return value, nil
	default:
		return nil, fmt.Errorf("snapshot JSON token %T is unsupported", token)
	}
}

func appendSnapshotCanonical(output *bytes.Buffer, value any) error {
	switch item := value.(type) {
	case nil:
		output.WriteString("null")
	case bool:
		if item {
			output.WriteString("true")
		} else {
			output.WriteString("false")
		}
	case string:
		return appendSnapshotString(output, item)
	case json.Number:
		canonical, err := canonicalSnapshotNumber(string(item))
		if err != nil {
			return err
		}
		output.WriteString(canonical)
	case []any:
		output.WriteByte('[')
		for i, child := range item {
			if i > 0 {
				output.WriteByte(',')
			}
			if err := appendSnapshotCanonical(output, child); err != nil {
				return err
			}
		}
		output.WriteByte(']')
	case map[string]any:
		keys := make([]string, 0, len(item))
		for key := range item {
			keys = append(keys, key)
		}
		sort.Slice(keys, func(i, j int) bool { return utf16Less(keys[i], keys[j]) })
		output.WriteByte('{')
		for i, key := range keys {
			if i > 0 {
				output.WriteByte(',')
			}
			if err := appendSnapshotString(output, key); err != nil {
				return err
			}
			output.WriteByte(':')
			if err := appendSnapshotCanonical(output, item[key]); err != nil {
				return err
			}
		}
		output.WriteByte('}')
	default:
		return fmt.Errorf("snapshot canonical type %T is unsupported", value)
	}
	return nil
}

func appendSnapshotString(output *bytes.Buffer, value string) error {
	if !utf8.ValidString(value) {
		return errors.New("snapshot string is invalid UTF-8")
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
	output.Write(quoted)
	return nil
}

func canonicalSnapshotNumber(raw string) (string, error) {
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil || math.IsInf(value, 0) || math.IsNaN(value) {
		return "", errors.New("snapshot number is not finite")
	}
	if value == 0 {
		return "0", nil
	}
	abs := math.Abs(value)
	if abs >= 1e-6 && abs < 1e21 {
		return strconv.FormatFloat(value, 'f', -1, 64), nil
	}
	scientific := strconv.FormatFloat(value, 'e', -1, 64)
	parts := bytes.SplitN([]byte(scientific), []byte("e"), 2)
	if len(parts) != 2 {
		return "", errors.New("snapshot number encoding failed")
	}
	exponent, err := strconv.Atoi(string(parts[1]))
	if err != nil {
		return "", errors.New("snapshot exponent is invalid")
	}
	sign := "+"
	if exponent < 0 {
		sign = "-"
		exponent = -exponent
	}
	return string(parts[0]) + "e" + sign + strconv.Itoa(exponent), nil
}

func utf16Less(left, right string) bool {
	a, b := utf16.Encode([]rune(left)), utf16.Encode([]rune(right))
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}
	return len(a) < len(b)
}

func validSnapshotUnicodeEscapes(data []byte) bool {
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
		unit, err := strconv.ParseUint(string(data[i+1:i+5]), 16, 16)
		if err != nil {
			return false
		}
		if unit >= 0xdc00 && unit <= 0xdfff {
			return false
		}
		if unit >= 0xd800 && unit <= 0xdbff {
			if i+10 >= len(data) || data[i+5] != '\\' || data[i+6] != 'u' {
				return false
			}
			low, err := strconv.ParseUint(string(data[i+7:i+11]), 16, 16)
			if err != nil || low < 0xdc00 || low > 0xdfff {
				return false
			}
			i += 10
		}
	}
	return !inString
}
