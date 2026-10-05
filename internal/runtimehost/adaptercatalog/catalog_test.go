//go:build unix

package adaptercatalog

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/plugintrust"
)

func TestReconcilePublishesStableImmutableSetAndReloads(t *testing.T) {
	root, source := newCatalogDirs(t)
	writeAdapter(t, source, "integrated-recorder-adapter-demo", validDescriptor("demo", "1"), "normal", "")
	catalog, err := Open(root, []string{source})
	if err != nil {
		t.Fatal(err)
	}
	first, err := catalog.Reconcile(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	second, err := catalog.Reconcile(context.Background(), first.ID)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID != second.ID || len(first.Entries) != 1 || first.Directory != second.Directory {
		t.Fatalf("stable set mismatch: first=%+v second=%+v", first, second)
	}
	if first.Entries[0].AdapterID != "demo" || first.Entries[0].ProtocolVersion != 1 || first.Entries[0].DescriptorFingerprint == "" {
		t.Fatalf("validated entry missing identity: %+v", first.Entries[0])
	}
	reopened, err := Open(root, []string{source})
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := reopened.Load(first.ID)
	if err != nil || loaded.ID != first.ID || len(loaded.Entries) != 1 {
		t.Fatalf("reopen/load = %+v, %v", loaded, err)
	}
	if got, want := mode(t, first.Directory), os.FileMode(0500); got.Perm() != want {
		t.Fatalf("snapshot bin mode = %v, want %v", got.Perm(), want)
	}
	if got, want := mode(t, filepath.Join(root, "sets", first.ID, "adapter-set.json")), os.FileMode(0400); got.Perm() != want {
		t.Fatalf("set manifest mode = %v, want %v", got.Perm(), want)
	}
	if got, want := mode(t, filepath.Join(root, "artifacts", first.Entries[0].ArtifactSHA256, "adapter")), os.FileMode(0500); got.Perm() != want {
		t.Fatalf("artifact mode = %v, want %v", got.Perm(), want)
	}
}

func TestReconcileWithAdditionalImmutableSource(t *testing.T) {
	root, source := newCatalogDirs(t)
	extra := filepath.Join(filepath.Dir(source), "registry-source")
	if err := os.Mkdir(extra, 0700); err != nil {
		t.Fatal(err)
	}
	writeAdapter(t, source, "integrated-recorder-adapter-local", validDescriptor("local", "1"), "normal", "")
	writeAdapter(t, extra, "integrated-recorder-adapter-registry", validDescriptor("registry", "2"), "normal", "")
	catalog, err := Open(root, []string{source})
	if err != nil {
		t.Fatal(err)
	}
	selected, err := catalog.ReconcileWithSources(context.Background(), "", []string{extra})
	if err != nil {
		t.Fatal(err)
	}
	if len(selected.Entries) != 2 || selected.Entries[0].AdapterID != "local" || selected.Entries[1].AdapterID != "registry" {
		t.Fatalf("reconcile with additional source = %+v", selected.Entries)
	}
	without, err := catalog.Reconcile(context.Background(), selected.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(without.Entries) != 1 || without.Entries[0].AdapterID != "local" {
		t.Fatalf("base reconciliation retained an ad hoc source: %+v", without.Entries)
	}
}

func TestSameDigestWithDifferentAttestationHasDifferentAdapterSetIdentity(t *testing.T) {
	root, sourceDir := newCatalogDirs(t)
	binary := filepath.Join(sourceDir, "integrated-recorder-adapter-demo")
	writeAdapterAt(t, binary, validDescriptor("demo", "1"), "normal", "")
	catalog, err := Open(root, nil)
	if err != nil {
		t.Fatal(err)
	}
	operator, err := catalog.ReconcileClassified(context.Background(), "", []Source{{
		Path: binary, Attestation: plugintrust.NewOperator(), AllowedIDs: []string{"demo"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	official, err := catalog.ReconcileClassified(context.Background(), operator.ID, []Source{{
		Path: binary, Attestation: plugintrust.NewOfficialRegistry(plugintrust.FirstParty), AllowedIDs: []string{"demo"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if operator.Entries[0].ArtifactSHA256 != official.Entries[0].ArtifactSHA256 || operator.ID == official.ID {
		t.Fatalf("attestation failed to participate in set identity: operator=%+v official=%+v", operator, official)
	}
	if operator.Entries[0].Attestation == nil || *operator.Entries[0].Attestation != plugintrust.NewOperator() || official.Entries[0].Attestation == nil || *official.Entries[0].Attestation != plugintrust.NewOfficialRegistry(plugintrust.FirstParty) {
		t.Fatalf("set did not preserve Host classifications: operator=%+v official=%+v", operator.Entries, official.Entries)
	}
}

func TestBundledSourceAllowedIDsAndReservedHLS(t *testing.T) {
	for _, tc := range []struct {
		name        string
		attestation plugintrust.Attestation
		allowed     []string
		wantCount   int
	}{
		{name: "operator cannot claim hls", attestation: plugintrust.NewOperator(), allowed: []string{"hls"}},
		{name: "bundled hls admitted", attestation: plugintrust.NewBundled(), allowed: []string{"hls"}, wantCount: 1},
		{name: "bundled allowlist is exact", attestation: plugintrust.NewBundled(), allowed: []string{"other"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root, sourceDir := newCatalogDirs(t)
			writeAdapter(t, sourceDir, "integrated-recorder-adapter-hls", validDescriptor("hls", "1"), "normal", "")
			catalog, _ := Open(root, nil)
			got, err := catalog.ReconcileClassified(context.Background(), "", []Source{{Path: sourceDir, Attestation: tc.attestation, AllowedIDs: tc.allowed}})
			if err != nil {
				t.Fatal(err)
			}
			if len(got.Entries) != tc.wantCount {
				t.Fatalf("entries=%+v rejected=%v, want count %d", got.Entries, got.RejectedCodes, tc.wantCount)
			}
			if tc.wantCount == 1 && (got.Entries[0].AdapterID != "hls" || *got.Entries[0].Attestation != plugintrust.NewBundled()) {
				t.Fatalf("HLS was not Host-classified bundled: %+v", got.Entries[0])
			}
		})
	}
}

func TestBundledHLSWinsOperatorBasenameCollision(t *testing.T) {
	root, bundledDir := newCatalogDirs(t)
	operatorDir := filepath.Join(t.TempDir(), "operator-adapters")
	if err := os.Mkdir(operatorDir, 0700); err != nil {
		t.Fatal(err)
	}
	name := "integrated-recorder-adapter-hls"
	writeAdapter(t, bundledDir, name, validDescriptor("hls", "1"), "normal", "")
	writeAdapter(t, operatorDir, name, validDescriptor("hls", "1"), "normal", "")
	catalog, err := Open(root, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = removeTreeOwned(root) })
	got, err := catalog.ReconcileClassified(context.Background(), "", []Source{
		{Path: bundledDir, Attestation: plugintrust.NewBundled(), AllowedIDs: []string{"hls"}},
		{Path: operatorDir, Attestation: plugintrust.NewOperator()},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Entries) != 1 || got.Entries[0].AdapterID != "hls" || got.Entries[0].Attestation == nil || *got.Entries[0].Attestation != plugintrust.NewBundled() {
		t.Fatalf("operator basename collision suppressed bundled HLS: %+v", got)
	}
	if !contains(got.RejectedCodes, "duplicate_binary_name") {
		t.Fatalf("operator collision was not reported: %+v", got)
	}
}

func TestAuthoritativeBundledHLSCollisionWinner(t *testing.T) {
	bundled := sourceCandidate{
		path: "/host/bundled/" + BinaryPrefix + "hls",
		source: Source{
			Path:        "/host/bundled",
			Attestation: plugintrust.NewBundled(),
			AllowedIDs:  []string{"hls"},
		},
	}
	operator := sourceCandidate{
		path: "/operator/" + BinaryPrefix + "custom-hls",
		source: Source{
			Path:        "/operator",
			Attestation: plugintrust.NewOperator(),
		},
	}
	otherBundled := sourceCandidate{
		path: "/other-bundled/" + BinaryPrefix + "custom-hls",
		source: Source{
			Path:        "/other-bundled",
			Attestation: plugintrust.NewBundled(),
			AllowedIDs:  []string{"hls"},
		},
	}
	entry := func(path string, attestation plugintrust.Attestation) candidate {
		return candidate{path: path, entry: Entry{AdapterID: "hls", Attestation: attestationPointer(attestation)}}
	}

	tests := []struct {
		name       string
		id         string
		candidates []candidate
		sources    map[string]sourceCandidate
		indexes    []int
		wantPath   string
	}{
		{
			name:       "one exact bundled candidate wins regardless of path order",
			id:         "hls",
			candidates: []candidate{entry(operator.path, plugintrust.NewOperator()), entry(bundled.path, plugintrust.NewBundled())},
			sources:    map[string]sourceCandidate{operator.path: operator, bundled.path: bundled},
			indexes:    []int{0, 1},
			wantPath:   bundled.path,
		},
		{
			name:       "second bundled candidate keeps group ambiguous",
			id:         "hls",
			candidates: []candidate{entry(bundled.path, plugintrust.NewBundled()), entry(otherBundled.path, plugintrust.NewBundled())},
			sources:    map[string]sourceCandidate{bundled.path: bundled, otherBundled.path: otherBundled},
			indexes:    []int{0, 1},
		},
		{
			name:       "missing exact bundled source keeps group ambiguous",
			id:         "hls",
			candidates: []candidate{entry(otherBundled.path, plugintrust.NewBundled()), entry(operator.path, plugintrust.NewOperator())},
			sources:    map[string]sourceCandidate{otherBundled.path: otherBundled, operator.path: operator},
			indexes:    []int{1, 0},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			winner, ok := authoritativeBundledHLSCollisionWinner(tc.id, tc.indexes, tc.candidates, tc.sources)
			if tc.wantPath == "" {
				if ok || winner != -1 {
					t.Fatalf("winner = %d, %v; want no authoritative winner", winner, ok)
				}
				return
			}
			if !ok || tc.candidates[winner].path != tc.wantPath {
				t.Fatalf("winner = %d, %v; want candidate %q", winner, ok, tc.wantPath)
			}
		})
	}
}

func TestOperatorHLSDescriptorCannotSuppressOrReplaceBundledHLS(t *testing.T) {
	root, bundledDir := newCatalogDirs(t)
	base := filepath.Dir(bundledDir)
	operatorDir := filepath.Join(base, "00-operator")
	if err := os.Mkdir(operatorDir, 0700); err != nil {
		t.Fatal(err)
	}
	writeAdapter(t, bundledDir, BinaryPrefix+"hls", validDescriptor("hls", "1"), "normal", "bundled bytes\n")
	operatorPath := writeAdapter(t, operatorDir, BinaryPrefix+"custom-hls", validDescriptor("hls", "1"), "normal", "operator bytes\n")
	catalog, err := Open(root, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer removeTreeOwned(root)
	bundled := Source{Path: bundledDir, Attestation: plugintrust.NewBundled(), AllowedIDs: []string{"hls"}}
	operator := Source{Path: operatorDir, Attestation: plugintrust.NewOperator(), AllowedIDs: []string{"hls"}}

	initial, err := catalog.ReconcileClassified(context.Background(), "", []Source{operator, bundled})
	if err != nil {
		t.Fatal(err)
	}
	if len(initial.Entries) != 1 || initial.Entries[0].AdapterID != "hls" || initial.Entries[0].Attestation == nil || *initial.Entries[0].Attestation != plugintrust.NewBundled() {
		t.Fatalf("operator candidate suppressed bundled HLS on initial reconcile: %+v", initial)
	}
	if !contains(initial.RejectedCodes, "reserved_adapter_id") {
		t.Fatalf("operator HLS descriptor was not rejected: %+v", initial)
	}
	knownGood := initial.Entries[0]

	writeAdapterAt(t, operatorPath, validDescriptor("hls", "2"), "normal", "replacement operator bytes\n")
	fallback, err := catalog.ReconcileClassified(context.Background(), initial.ID, []Source{bundled, operator})
	if err != nil {
		t.Fatal(err)
	}
	if fallback.ID != initial.ID || len(fallback.Entries) != 1 || !sameEntry(fallback.Entries[0], knownGood) {
		t.Fatalf("operator candidate replaced the known-good bundled HLS identity: initial=%+v fallback=%+v", initial, fallback)
	}
}

func TestAmbiguousNonAuthoritativeBundledHLSCandidatesAreRejected(t *testing.T) {
	for _, tc := range []struct {
		name            string
		firstAllowedIDs []string
	}{
		{name: "no exact authoritative candidate"},
		{name: "multiple bundled candidates", firstAllowedIDs: []string{"hls"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root, firstDir := newCatalogDirs(t)
			secondDir := filepath.Join(t.TempDir(), "other-bundled")
			if err := os.Mkdir(secondDir, 0700); err != nil {
				t.Fatal(err)
			}
			writeAdapter(t, firstDir, BinaryPrefix+"hls", validDescriptor("hls", "1"), "normal", "first bundled\n")
			writeAdapter(t, secondDir, BinaryPrefix+"custom-hls", validDescriptor("hls", "1"), "normal", "second bundled\n")
			catalog, err := Open(root, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer removeTreeOwned(root)
			got, err := catalog.ReconcileClassified(context.Background(), "", []Source{
				{Path: firstDir, Attestation: plugintrust.NewBundled(), AllowedIDs: tc.firstAllowedIDs},
				{Path: secondDir, Attestation: plugintrust.NewBundled(), AllowedIDs: []string{"hls"}},
			})
			if err != nil {
				t.Fatal(err)
			}
			if len(got.Entries) != 0 || got.RejectedCount != 2 || !contains(got.RejectedCodes, "duplicate_adapter_id") {
				t.Fatalf("ambiguous non-authoritative HLS group was not rejected: %+v", got)
			}
		})
	}
}

func TestConflictingBundledHLSBasenamesFailClosed(t *testing.T) {
	root, firstDir := newCatalogDirs(t)
	secondDir := filepath.Join(t.TempDir(), "bundled-adapters")
	if err := os.Mkdir(secondDir, 0700); err != nil {
		t.Fatal(err)
	}
	name := "integrated-recorder-adapter-hls"
	writeAdapter(t, firstDir, name, validDescriptor("hls", "1"), "normal", "")
	writeAdapter(t, secondDir, name, validDescriptor("hls", "2"), "normal", "")
	catalog, err := Open(root, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = removeTreeOwned(root) })
	got, err := catalog.ReconcileClassified(context.Background(), "", []Source{
		{Path: firstDir, Attestation: plugintrust.NewBundled(), AllowedIDs: []string{"hls"}},
		{Path: secondDir, Attestation: plugintrust.NewBundled(), AllowedIDs: []string{"hls"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Entries) != 0 || got.RejectedCount != 2 || !contains(got.RejectedCodes, "duplicate_binary_name") {
		t.Fatalf("conflicting bundled HLS candidates were not rejected: %+v", got)
	}
}

func TestClassifiedSourceLimitAllowsRegistryAndBundledInventory(t *testing.T) {
	base := t.TempDir()
	configured := make([]string, maxSourceDirs)
	for i := range configured {
		configured[i] = filepath.Join(base, fmt.Sprintf("operator-%02d", i))
	}
	catalog, err := Open(filepath.Join(base, "catalog"), configured)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = removeTreeOwned(catalog.root) })
	classified := make([]Source, 0, maxClassifiedSources)
	for i := 0; i < maxAdapters; i++ {
		classified = append(classified, Source{
			Path: filepath.Join(base, fmt.Sprintf("registry-%03d", i)), Attestation: plugintrust.NewCustomRegistry(),
		})
	}
	classified = append(classified, Source{
		Path: filepath.Join(base, "bundled-hls"), Attestation: plugintrust.NewBundled(), AllowedIDs: []string{"hls"},
	})
	if _, err := catalog.ReconcileClassified(context.Background(), "", classified); err != nil {
		t.Fatalf("maximum bounded classified inventory rejected: %v", err)
	}
	tooMany := append(append([]Source(nil), classified...), Source{
		Path: filepath.Join(base, "extra-source"), Attestation: plugintrust.NewOperator(),
	})
	if _, err := catalog.ReconcileClassified(context.Background(), "", tooMany); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("oversized classified inventory error = %v, want ErrInvalidConfig", err)
	}
}

func TestDescriptorSelfClaimsDoNotSetAttestation(t *testing.T) {
	root, sourceDir := newCatalogDirs(t)
	attestation := plugintrust.NewOperator()
	descriptor := strings.TrimSuffix(validDescriptor("demo", "1"), "}") + `,"trust":{"provenance":"bundled","authority":"core_release","publisher":"first_party","reviewed":true}}`
	writeAdapter(t, sourceDir, "integrated-recorder-adapter-demo", descriptor, "normal", "")
	catalog, _ := Open(root, nil)
	got, err := catalog.ReconcileClassified(context.Background(), "", []Source{{Path: sourceDir, Attestation: attestation}})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Entries) != 1 || got.Entries[0].Attestation == nil || *got.Entries[0].Attestation != plugintrust.NewOperator() {
		t.Fatalf("descriptor self-claim affected Host provenance: %+v", got.Entries)
	}
}

func TestLegacyAdapterSetWithoutAttestationRemainsLoadable(t *testing.T) {
	root, sourceDir := newCatalogDirs(t)
	writeAdapter(t, sourceDir, "integrated-recorder-adapter-demo", validDescriptor("demo", "1"), "normal", "")
	catalog, _ := Open(root, []string{sourceDir})
	original, err := catalog.Reconcile(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	legacyEntries := cloneEntries(original.Entries)
	legacyEntries[0].Attestation = nil
	legacyManifest, err := encodeManifest(legacyEntries)
	if err != nil {
		t.Fatal(err)
	}
	legacyID := digest(legacyManifest)
	originalDir := filepath.Join(root, "sets", original.ID)
	if err := os.Chmod(originalDir, 0700); err != nil {
		t.Fatal(err)
	}
	manifestPath := filepath.Join(originalDir, "adapter-set.json")
	if err := os.Chmod(manifestPath, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifestPath, legacyManifest, 0400); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(originalDir, filepath.Join(root, "sets", legacyID)); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Join(root, "sets", legacyID), 0500); err != nil {
		t.Fatal(err)
	}
	loaded, err := catalog.Load(legacyID)
	if err != nil || len(loaded.Entries) != 1 || loaded.Entries[0].Attestation != nil || loaded.Entries[0].EffectiveAttestation().Provenance != plugintrust.LegacyUnclassified {
		t.Fatalf("legacy adapter set load = %+v, %v", loaded, err)
	}
	// A rejected replacement with the same executable name exercises the
	// fallback path that carries this pre-provenance entry into a new publish.
	writeAdapter(t, sourceDir, "integrated-recorder-adapter-demo", validDescriptor("demo", "1"), "malformed", "")
	fallback, err := catalog.Reconcile(context.Background(), legacyID)
	if err != nil || fallback.ID != legacyID || len(fallback.Entries) != 1 || fallback.Entries[0].Attestation != nil || fallback.Entries[0].EffectiveAttestation().Provenance != plugintrust.LegacyUnclassified {
		t.Fatalf("legacy fallback acquired invented provenance: %+v, %v", fallback, err)
	}
}

func TestDifferentArtifactBytesProduceDifferentSetIdentity(t *testing.T) {
	root, source := newCatalogDirs(t)
	path := filepath.Join(source, "integrated-recorder-adapter-demo")
	writeAdapterAt(t, path, validDescriptor("demo", "1"), "normal", "")
	catalog, _ := Open(root, []string{source})
	one, err := catalog.Reconcile(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	writeAdapterAt(t, path, validDescriptor("demo", "2"), "normal", "# bytes changed\n")
	two, err := catalog.Reconcile(context.Background(), one.ID)
	if err != nil {
		t.Fatal(err)
	}
	if one.ID == two.ID || one.Entries[0].ArtifactSHA256 == two.Entries[0].ArtifactSHA256 {
		t.Fatalf("different bytes did not change identity: one=%+v two=%+v", one, two)
	}
}

func TestSameAdapterVersionCannotSilentlyChangeArtifactIdentity(t *testing.T) {
	for _, tc := range []struct {
		name       string
		descriptor func(string) string
		extra      string
	}{
		{
			name: "semantic descriptor changed",
			descriptor: func(value string) string {
				return strings.Replace(validDescriptor("demo", "1"), "Test Adapter", value, 1)
			},
			extra: "stable bytes\n",
		},
		{
			name:       "artifact bytes changed",
			descriptor: func(string) string { return validDescriptor("demo", "1") },
			extra:      "different bytes\n",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root, source := newCatalogDirs(t)
			path := filepath.Join(source, "integrated-recorder-adapter-demo")
			writeAdapterAt(t, path, validDescriptor("demo", "1"), "normal", "known-good\n")
			catalog, _ := Open(root, []string{source})
			fallback, err := catalog.Reconcile(context.Background(), "")
			if err != nil || len(fallback.Entries) != 1 {
				t.Fatalf("initial Reconcile() = %+v, %v", fallback, err)
			}
			old := fallback.Entries[0]
			marker := filepath.Join(t.TempDir(), "launches")
			writeAdapterAt(t, path, tc.descriptor("Changed Adapter"), "normal", tc.extra+fmt.Sprintf("echo launch >> %q\n", marker))
			candidateBytes, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			candidateDigest := digest(candidateBytes)

			for attempt := 0; attempt < 2; attempt++ {
				got, err := catalog.Reconcile(context.Background(), fallback.ID)
				if err != nil {
					t.Fatal(err)
				}
				if got.ID != fallback.ID || len(got.Entries) != 1 || !sameEntry(got.Entries[0], old) || got.RejectedCount != 1 || !contains(got.RejectedCodes, "identity_conflict") {
					t.Fatalf("same-version conflict replaced the known-good artifact: %+v", got)
				}
			}
			if _, err := os.Lstat(filepath.Join(root, "artifacts", candidateDigest)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("conflicting candidate artifact was published: %v", err)
			}
			launches, err := os.ReadFile(marker)
			if err != nil || strings.Count(string(launches), "launch") != 1 {
				t.Fatalf("quarantined identity conflict launches = %q, %v; want one probe", launches, err)
			}
		})
	}
}

func TestChangedSourceDuringImportIsRejectedAndFallbackPreserved(t *testing.T) {
	root, source := newCatalogDirs(t)
	path := filepath.Join(source, "integrated-recorder-adapter-demo")
	writeAdapterAt(t, path, validDescriptor("demo", "1"), "normal", "")
	catalog, _ := Open(root, []string{source})
	fallback, err := catalog.Reconcile(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	writeAdapterAt(t, path, validDescriptor("demo", "2"), "normal", "")
	catalog.afterSourceCopy = func(copied string) {
		if copied == path {
			_ = os.WriteFile(path, []byte("replacement bytes changed while scanning"), 0700)
		}
	}
	got, err := catalog.Reconcile(context.Background(), fallback.ID)
	catalog.afterSourceCopy = nil
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != fallback.ID || got.RejectedCount == 0 || !contains(got.RejectedCodes, "source_unstable") {
		t.Fatalf("unstable source did not preserve fallback: %+v", got)
	}
}

func TestRejectsUnsafeCandidateKindsAndOversizeWithoutExecuting(t *testing.T) {
	root, source := newCatalogDirs(t)
	catalog, _ := Open(root, []string{source})
	// A directory with a candidate name is non-regular and must be rejected.
	if err := os.Mkdir(filepath.Join(source, "integrated-recorder-adapter-directory"), 0700); err != nil {
		t.Fatal(err)
	}
	// A non-executable regular file is rejected before process launch.
	if err := os.WriteFile(filepath.Join(source, "integrated-recorder-adapter-noexec"), []byte("data"), 0600); err != nil {
		t.Fatal(err)
	}
	// A symlink is never followed.
	base := filepath.Join(source, "target")
	if err := os.WriteFile(base, []byte("#!/bin/sh\nexit 0\n"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(base, filepath.Join(source, "integrated-recorder-adapter-link")); err != nil {
		t.Fatal(err)
	}
	// Sparse oversized input is rejected from stat metadata without copying it.
	large := filepath.Join(source, "integrated-recorder-adapter-large")
	f, err := os.OpenFile(large, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0700)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(MaxArtifactBytes + 1); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(large, 0700); err != nil {
		t.Fatal(err)
	}
	snapshot, err := catalog.Reconcile(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Entries) != 0 || snapshot.RejectedCount < 4 {
		t.Fatalf("unsafe candidates accepted or not reported: %+v", snapshot)
	}
	if _, err := Open("relative/root", nil); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("relative root error = %v", err)
	}
	symlinkRoot := filepath.Join(t.TempDir(), "adapters")
	if err := os.Symlink(root, symlinkRoot); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(symlinkRoot, nil); !errors.Is(err, ErrUnsafeStore) {
		t.Fatalf("symlink root error = %v", err)
	}
	if safeBinaryName("integrated-recorder-adapter-../escape") || safeBinaryName("integrated-recorder-adapter-\\escape") || safeBinaryName("integrated-recorder-adapter-\x00bad") {
		t.Fatal("unsafe binary name accepted")
	}
}

func TestRejectsMalformedUnsupportedAndTimedOutAdapters(t *testing.T) {
	for _, tc := range []struct {
		name, behavior string
		descriptor     string
	}{
		{name: "malformed", behavior: "malformed", descriptor: validDescriptor("badjson", "1")},
		{name: "unsupported protocol", behavior: "normal", descriptor: strings.Replace(validDescriptor("badproto", "1"), `"protocol_version":1`, `"protocol_version":9`, 1)},
		{name: "invalid descriptor", behavior: "normal", descriptor: `{"id":"invalid","name":"No media","version":"1","protocol_version":1,"capabilities":["resolve"],"input_schema":{"fields":[]},"configuration_schema":{"fields":[]},"media_types":[]}`},
		{name: "timeout", behavior: "timeout", descriptor: validDescriptor("timeout", "1")},
		{name: "stdout garbage", behavior: "garbage", descriptor: validDescriptor("garbage", "1")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root, source := newCatalogDirs(t)
			writeAdapter(t, source, "integrated-recorder-adapter-"+strings.ReplaceAll(tc.name, " ", "-"), tc.descriptor, tc.behavior, "")
			catalog, _ := Open(root, []string{source})
			ctx, cancel := context.WithTimeout(context.Background(), 900*time.Millisecond)
			defer cancel()
			start := time.Now()
			snapshot, err := catalog.Reconcile(ctx, "")
			if err != nil {
				t.Fatalf("Reconcile error = %v", err)
			}
			if time.Since(start) > 5*time.Second {
				t.Fatal("bounded probe took too long")
			}
			if len(snapshot.Entries) != 0 || snapshot.RejectedCount != 1 {
				t.Fatalf("candidate was not rejected: %+v", snapshot)
			}
		})
	}
}

func TestDuplicateDescriptorIDsAreRejectedDeterministically(t *testing.T) {
	root, source := newCatalogDirs(t)
	desc := validDescriptor("same", "1")
	writeAdapter(t, source, "integrated-recorder-adapter-a", desc, "normal", "")
	writeAdapter(t, source, "integrated-recorder-adapter-b", desc, "normal", "# distinct source\n")
	catalog, _ := Open(root, []string{source})
	snapshot, err := catalog.Reconcile(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Entries) != 0 || snapshot.RejectedCount != 2 || !contains(snapshot.RejectedCodes, "duplicate_adapter_id") {
		t.Fatalf("duplicate IDs not rejected as a group: %+v", snapshot)
	}
}

func TestInvalidCandidatePreservesFallbackAndQuarantineAvoidsRepeatedLaunch(t *testing.T) {
	root, source := newCatalogDirs(t)
	writeAdapter(t, source, "integrated-recorder-adapter-good", validDescriptor("good", "1"), "normal", "")
	catalog, _ := Open(root, []string{source})
	fallback, err := catalog.Reconcile(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(t.TempDir(), "launches")
	badPath := filepath.Join(source, "integrated-recorder-adapter-bad")
	writeAdapterAt(t, badPath, validDescriptor("bad", "1"), "malformed", fmt.Sprintf("echo launch >> %q\n", marker))
	for i := 0; i < 2; i++ {
		got, err := catalog.Reconcile(context.Background(), fallback.ID)
		if err != nil {
			t.Fatal(err)
		}
		if got.ID != fallback.ID || got.RejectedCount != 1 {
			t.Fatalf("fallback changed: %+v", got)
		}
	}
	data, err := os.ReadFile(marker)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(string(data), "launch"); got != 1 {
		t.Fatalf("invalid artifact launches = %d, want 1", got)
	}
}

func TestRejectedCandidateDoesNotBlockIndependentValidAddition(t *testing.T) {
	root, source := newCatalogDirs(t)
	writeAdapter(t, source, "integrated-recorder-adapter-existing", validDescriptor("existing", "1"), "normal", "initial\n")
	catalog, _ := Open(root, []string{source})
	fallback, err := catalog.Reconcile(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	writeAdapter(t, source, "integrated-recorder-adapter-new", validDescriptor("new", "1"), "normal", "new\n")
	writeAdapter(t, source, "integrated-recorder-adapter-invalid", validDescriptor("invalid", "1"), "malformed", "")

	got, err := catalog.Reconcile(context.Background(), fallback.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID == fallback.ID || got.RejectedCount != 1 || len(got.RejectedCodes) == 0 {
		t.Fatalf("valid addition was not published alongside isolated rejection: fallback=%+v got=%+v", fallback, got)
	}
	if len(got.Entries) != 2 || got.Entries[0].AdapterID != "existing" || got.Entries[1].AdapterID != "new" {
		t.Fatalf("published entries = %+v, want existing and new", got.Entries)
	}
}

func TestInvalidReplacementRetainsFallbackAndAllowsIndependentAddition(t *testing.T) {
	root, source := newCatalogDirs(t)
	replacementPath := filepath.Join(source, "integrated-recorder-adapter-existing")
	writeAdapterAt(t, replacementPath, validDescriptor("existing", "1"), "normal", "v1\n")
	catalog, _ := Open(root, []string{source})
	fallback, err := catalog.Reconcile(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	old := fallback.Entries[0]
	writeAdapterAt(t, replacementPath, validDescriptor("existing", "2"), "malformed", "v2\n")
	writeAdapter(t, source, "integrated-recorder-adapter-new", validDescriptor("new", "1"), "normal", "new\n")

	got, err := catalog.Reconcile(context.Background(), fallback.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID == fallback.ID || got.RejectedCount != 1 || len(got.RejectedCodes) == 0 {
		t.Fatalf("unrelated addition did not apply with rejected replacement: fallback=%+v got=%+v", fallback, got)
	}
	if len(got.Entries) != 2 {
		t.Fatalf("entries = %+v, want retained old adapter and new adapter", got.Entries)
	}
	var retained, added *Entry
	for i := range got.Entries {
		entry := &got.Entries[i]
		switch entry.AdapterID {
		case "existing":
			retained = entry
		case "new":
			added = entry
		}
	}
	if retained == nil || added == nil || !sameEntry(*retained, old) {
		t.Fatalf("old adapter was not retained exactly: old=%+v got=%+v", old, got.Entries)
	}
	if _, err := catalog.Load(got.ID); err != nil {
		t.Fatalf("resulting mixed set is not loadable: %v", err)
	}
}

func TestDuplicateIDGroupRetainsMatchingFallbackWithoutChoosingCandidate(t *testing.T) {
	root, source := newCatalogDirs(t)
	pathA := filepath.Join(source, "integrated-recorder-adapter-a")
	writeAdapterAt(t, pathA, validDescriptor("shared", "1"), "normal", "old\n")
	catalog, _ := Open(root, []string{source})
	fallback, err := catalog.Reconcile(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	old := fallback.Entries[0]
	writeAdapterAt(t, pathA, validDescriptor("shared", "2"), "normal", "candidate a\n")
	writeAdapter(t, source, "integrated-recorder-adapter-b", validDescriptor("shared", "3"), "normal", "candidate b\n")

	got, err := catalog.Reconcile(context.Background(), fallback.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Entries) != 1 || !sameEntry(got.Entries[0], old) || got.RejectedCount != 2 || !contains(got.RejectedCodes, "duplicate_adapter_id") {
		t.Fatalf("ambiguous duplicate ID group did not retain only matching fallback: %+v", got)
	}
}

func TestEmptySetAndTamperedManifest(t *testing.T) {
	root, source := newCatalogDirs(t)
	catalog, _ := Open(root, []string{source})
	empty, err := catalog.Empty()
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := catalog.Load(empty.ID)
	if err != nil || len(loaded.Entries) != 0 {
		t.Fatalf("empty set load = %+v, %v", loaded, err)
	}
	manifestPath := filepath.Join(root, "sets", empty.ID, "adapter-set.json")
	if err := os.Chmod(manifestPath, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifestPath, []byte(`{"schema_version":1,"entries":[]} `), 0400); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.Load(empty.ID); !errors.Is(err, ErrInvalidSet) {
		t.Fatalf("tampered manifest error = %v", err)
	}
}

func TestCollectFailsClosedOnCorruptSet(t *testing.T) {
	root, source := newCatalogDirs(t)
	writeAdapter(t, source, "integrated-recorder-adapter-demo", validDescriptor("demo", "1"), "normal", "")
	catalog, _ := Open(root, []string{source})
	snapshot, err := catalog.Reconcile(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	manifestPath := filepath.Join(root, "sets", snapshot.ID, "adapter-set.json")
	if err := os.Chmod(manifestPath, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifestPath, []byte("broken"), 0400); err != nil {
		t.Fatal(err)
	}
	if err := catalog.Collect(nil); !errors.Is(err, ErrUnsafeStore) {
		t.Fatalf("Collect() error = %v", err)
	}
	artifact := filepath.Join(root, "artifacts", snapshot.Entries[0].ArtifactSHA256, "adapter")
	if _, err := os.Stat(artifact); err != nil {
		t.Fatalf("artifact deleted despite uncertain set reference: %v", err)
	}
}

func TestCollectRetainsReferencedSetAndRemovesUnreferencedObjects(t *testing.T) {
	root, source := newCatalogDirs(t)
	path := filepath.Join(source, "integrated-recorder-adapter-demo")
	writeAdapterAt(t, path, validDescriptor("demo", "1"), "normal", "v1\n")
	catalog, _ := Open(root, []string{source})
	v1, err := catalog.Reconcile(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	writeAdapterAt(t, path, validDescriptor("demo", "2"), "normal", "v2\n")
	v2, err := catalog.Reconcile(context.Background(), v1.ID)
	if err != nil {
		t.Fatal(err)
	}
	if v1.ID == v2.ID {
		t.Fatal("expected a second adapter set")
	}
	if err := catalog.Collect([]string{v1.ID}); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.Load(v1.ID); err != nil {
		t.Fatalf("kept set unavailable: %v", err)
	}
	if _, err := catalog.Load(v2.ID); !errors.Is(err, ErrSetNotFound) {
		t.Fatalf("collected set load error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "artifacts", v1.Entries[0].ArtifactSHA256, "adapter")); err != nil {
		t.Fatalf("referenced artifact removed: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(root, "artifacts", v2.Entries[0].ArtifactSHA256)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unreferenced artifact remains: %v", err)
	}
	if err := catalog.Collect(nil); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(root, "artifacts", v1.Entries[0].ArtifactSHA256)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unreferenced artifact after empty keep = %v", err)
	}
}

func TestRejectedProjectionContainsOnlyBoundedCodes(t *testing.T) {
	root, source := newCatalogDirs(t)
	writeAdapter(t, source, "integrated-recorder-adapter-bad", validDescriptor("bad", "1"), "malformed", "")
	catalog, _ := Open(root, []string{source})
	fallback, err := catalog.Empty()
	if err != nil {
		t.Fatal(err)
	}
	got, err := catalog.Reconcile(context.Background(), fallback.ID)
	if err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(got)
	if strings.Contains(string(encoded), source) || strings.Contains(string(encoded), "malformed") || len(got.RejectedCodes) > maxRejectedCodes {
		t.Fatalf("unsafe rejection projection: %s", encoded)
	}
}

func newCatalogDirs(t *testing.T) (string, string) {
	t.Helper()
	base := t.TempDir()
	root, source := filepath.Join(base, "runtime", "adapters"), filepath.Join(base, "external")
	t.Cleanup(func() { makeWritableForTest(base) })
	if err := os.Mkdir(source, 0700); err != nil {
		t.Fatal(err)
	}
	return root, source
}

func makeWritableForTest(path string) {
	info, err := os.Lstat(path)
	if err != nil || info.Mode()&os.ModeSymlink != 0 {
		return
	}
	if !info.IsDir() {
		_ = os.Chmod(path, 0600)
		return
	}
	_ = os.Chmod(path, 0700)
	items, err := os.ReadDir(path)
	if err != nil {
		return
	}
	for _, item := range items {
		makeWritableForTest(filepath.Join(path, item.Name()))
	}
}

func validDescriptor(id, version string) string {
	return fmt.Sprintf(`{"id":%q,"name":"Test Adapter","version":%q,"protocol_version":1,"capabilities":["resolve"],"input_schema":{"fields":[]},"configuration_schema":{"fields":[]},"media_types":["hls"]}`, id, version)
}

func writeAdapter(t *testing.T, dir, name, descriptor, behavior, extra string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	writeAdapterAt(t, path, descriptor, behavior, extra)
	return path
}

func writeAdapterAt(t *testing.T, path, descriptor, behavior, extra string) {
	t.Helper()
	var script string
	switch behavior {
	case "malformed":
		script = "#!/bin/sh\n" + extra + `while IFS= read -r line; do printf 'not-json\n'; exit 0; done` + "\n"
	case "timeout":
		script = "#!/bin/sh\n" + extra + `while IFS= read -r line; do exec sleep 20; done` + "\n"
	case "garbage":
		script = "#!/bin/sh\nprintf 'startup diagnostic\\n'\n" + extra + normalScript(descriptor)
	default:
		script = "#!/bin/sh\n" + extra + normalScript(descriptor)
	}
	if err := os.WriteFile(path, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0700); err != nil {
		t.Fatal(err)
	}
}

func normalScript(descriptor string) string {
	return `while IFS= read -r line; do
 id=$(printf '%s' "$line" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
 case "$line" in
` + fmt.Sprintf(`  *'"method":"describe"'*) printf '{"protocol_version":1,"id":"%%s","result":%%s}\n' "$id" '%s' ;;`, descriptor) + `
  *'"method":"shutdown"'*) printf '{"protocol_version":1,"id":"%s","result":{}}\n' "$id"; exit 0 ;;
 esac
done
`
}

func mode(t *testing.T, path string) os.FileMode {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return info.Mode()
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func sameEntry(left, right Entry) bool {
	if left.AdapterID != right.AdapterID || left.Version != right.Version || left.ProtocolVersion != right.ProtocolVersion || left.DescriptorFingerprint != right.DescriptorFingerprint || left.ArtifactSHA256 != right.ArtifactSHA256 || left.ArtifactSize != right.ArtifactSize || left.BinaryName != right.BinaryName {
		return false
	}
	if left.Attestation == nil || right.Attestation == nil {
		return left.Attestation == nil && right.Attestation == nil
	}
	return *left.Attestation == *right.Attestation
}

func TestDigestHelperMatchesSHA256(t *testing.T) {
	data := []byte("test")
	sum := sha256.Sum256(data)
	if digest(data) != hex.EncodeToString(sum[:]) {
		t.Fatal("digest mismatch")
	}
}
