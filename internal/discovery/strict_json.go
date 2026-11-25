package discovery

import (
	"bytes"
	"encoding/json"
	"io"
)

// strictJSONDocument rejects duplicate object keys and trailing JSON values
// without imposing the smaller signed-event JSON limit on a public history
// bundle that may contain several megabytes of individually bounded events.
func strictJSONDocument(data []byte) bool {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if consumeJSONValue(decoder, 0) != nil {
		return false
	}
	_, err := decoder.Token()
	return err == io.EOF
}

func consumeJSONValue(decoder *json.Decoder, depth int) error {
	if depth > 32 {
		return io.ErrUnexpectedEOF
	}
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, isDelimiter := token.(json.Delim)
	if !isDelimiter {
		return nil
	}
	switch delimiter {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return io.ErrUnexpectedEOF
			}
			if _, duplicate := seen[key]; duplicate {
				return io.ErrUnexpectedEOF
			}
			seen[key] = struct{}{}
			if err := consumeJSONValue(decoder, depth+1); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil || closing != json.Delim('}') {
			return io.ErrUnexpectedEOF
		}
	case '[':
		for decoder.More() {
			if err := consumeJSONValue(decoder, depth+1); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil || closing != json.Delim(']') {
			return io.ErrUnexpectedEOF
		}
	default:
		return io.ErrUnexpectedEOF
	}
	return nil
}
