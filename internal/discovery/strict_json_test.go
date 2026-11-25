package discovery

import "testing"

func TestStrictJSONDocumentRejectsDuplicatesAndTrailingValues(t *testing.T) {
	for _, input := range []string{
		`{"identity_id":"gi_x","events":[]}`, // structure is checked by the caller
		`{"a":{"b":true},"items":["x",null]}`,
	} {
		if !strictJSONDocument([]byte(input)) {
			t.Fatalf("valid JSON was rejected: %s", input)
		}
	}
	for _, input := range []string{
		`{"identity_id":"first","identity_id":"second"}`,
		`{"outer":{"key":"first","key":"second"}}`,
		`{"one":true} {"two":true}`,
		`{"unterminated":`,
	} {
		if strictJSONDocument([]byte(input)) {
			t.Fatalf("invalid JSON was accepted: %s", input)
		}
	}
}
