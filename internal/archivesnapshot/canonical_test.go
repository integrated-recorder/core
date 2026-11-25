package archivesnapshot

import "testing"

func TestJCSNumberAndPropertyCanonicalization(t *testing.T) {
	for input, expected := range map[string]string{
		`{"z":1.0,"a":-0}`:                 `{"a":0,"z":1}`,
		`{"value":0.000001}`:               `{"value":0.000001}`,
		`{"value":0.0000001}`:              `{"value":1e-7}`,
		`{"value":1000000000000000000000}`: `{"value":1e+21}`,
		`{"value":333333333.33333329}`:     `{"value":333333333.3333333}`,
		`{"value":1.7976931348623157e308}`: `{"value":1.7976931348623157e+308}`,
		`{"value":9.999999999999997e22}`:   `{"value":9.999999999999997e+22}`,
		`{"value":1e23}`:                   `{"value":1e+23}`,
	} {
		canonical, err := canonicalizeSnapshotJSON([]byte(input))
		if err != nil || string(canonical) != expected {
			t.Errorf("canonicalize %s = %s, %v; want %s", input, canonical, err, expected)
		}
	}
}

func TestJCSPropertyOrderUsesUTF16CodeUnits(t *testing.T) {
	input := `{"€":"Euro","\r":"Carriage Return","דּ":"Hebrew","1":"One","😀":"Emoji","\u0080":"Control","ö":"Latin"}`
	expected := `{"\r":"Carriage Return","1":"One","":"Control","ö":"Latin","€":"Euro","😀":"Emoji","דּ":"Hebrew"}`
	canonical, err := canonicalizeSnapshotJSON([]byte(input))
	if err != nil || string(canonical) != expected {
		t.Fatalf("canonical=%s err=%v want=%s", canonical, err, expected)
	}
}

func TestJCSCanonicalizationRejectsDuplicatesAndInvalidNumbers(t *testing.T) {
	for _, input := range []string{
		`{"value":1,"value":2}`,
		`{"value":1e9999}`,
		`{"value":NaN}`,
		`{"value":1} {}`,
	} {
		if _, err := canonicalizeSnapshotJSON([]byte(input)); err == nil {
			t.Errorf("accepted invalid canonical JSON: %s", input)
		}
	}
}
