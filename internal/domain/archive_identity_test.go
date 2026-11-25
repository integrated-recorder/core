package domain

import "testing"

func TestNewArchiveIDIsValidAndIndependent(t *testing.T) {
	first, err := NewArchiveID()
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewArchiveID()
	if err != nil {
		t.Fatal(err)
	}
	if !ValidArchiveID(first) || !ValidArchiveID(second) {
		t.Fatalf("generated archive IDs are invalid: %q %q", first, second)
	}
	if first == second {
		t.Fatal("two independently generated archive IDs collided")
	}
}

func TestValidArchiveIDRejectsNonCanonicalValues(t *testing.T) {
	for _, value := range []string{"", "arc_short", "arc_!!!!!!!!!!!!!!!!!!!!!!", "arc_aaaaaaaaaaaaaaaaaaaaaa\n", "arc_aaaaaaaaaaaaaaaaaaaaab"} {
		if ValidArchiveID(value) {
			t.Errorf("accepted malformed archive ID %q", value)
		}
	}
}
