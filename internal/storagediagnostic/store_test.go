package storagediagnostic

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

const testRecordingID = "0123456789abcdef0123456789abcdef"

func validDiagnostic(stage string) Diagnostic {
	return Diagnostic{
		Version: SchemaVersion, RecordedAt: time.Date(2026, 10, 8, 0, 0, 0, 0, time.UTC),
		RecordingID: testRecordingID, Classification: ClassificationCanonicalCommitFailed,
		StageChain: []string{stage}, ErrorTypeChain: []string{"*storage.canonicalCommitFailure", "*os.PathError"},
		ErrorCategories: []string{"canonical_commit_failed", "io_error"}, FileOperation: "rename", ErrnoCode: 28,
		CurrentJobKind: "media_payload", FirstFailureJobKind: "media_payload", CurrentAttempts: 1, FirstFailureAttempts: 1,
		IngestSnapshot: IngestSnapshot{BufferUsedBytes: 10, ReservedBytes: 16, QueueObjects: 1, QueueBytes: 10, ActiveWriters: 1, WriterConcurrency: 1},
		RecordingState: "recording", SourceClass: "live",
	}
}

func TestRecordFirstIsAtomicAndSurvivesReopen(t *testing.T) {
	root := t.TempDir()
	store, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	winner := validDiagnostic("media payload commit")
	if saved, created, err := store.RecordFirst(winner); err != nil || !created || saved.StageChain[0] != winner.StageChain[0] {
		t.Fatalf("first record saved=%+v created=%t err=%v", saved, created, err)
	}
	second := validDiagnostic("historical media payload commit")
	if saved, created, err := store.RecordFirst(second); err != nil || created || saved.StageChain[0] != winner.StageChain[0] {
		t.Fatalf("second record replaced first saved=%+v created=%t err=%v", saved, created, err)
	}
	reopened, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	got, err := reopened.Read(testRecordingID)
	if err != nil || got.StageChain[0] != winner.StageChain[0] || got.RecordingID != testRecordingID {
		t.Fatalf("reopened diagnostic=%+v err=%v", got, err)
	}
	path := filepath.Join(root, "management", "diagnostics", "recordings", testRecordingID+".json")
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("diagnostic file mode=%#o, want 0600", info.Mode().Perm())
	}
	for _, dir := range []string{"management", filepath.Join("management", "diagnostics"), filepath.Join("management", "diagnostics", "recordings")} {
		info, err := os.Stat(filepath.Join(root, dir))
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0700 {
			t.Fatalf("directory %s mode=%#o, want 0700", dir, info.Mode().Perm())
		}
	}
	if err := reopened.Delete(testRecordingID); err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.Read(testRecordingID); err != ErrNotFound {
		t.Fatalf("read after delete err=%v, want not found", err)
	}
}

func TestConcurrentRecordFirstKeepsExactlyOneDiagnostic(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const writers = 24
	var wg sync.WaitGroup
	var mu sync.Mutex
	created := 0
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			d := validDiagnostic("media payload commit")
			d.CurrentAttempts = index + 1
			_, didCreate, err := store.RecordFirst(d)
			if err != nil {
				t.Errorf("record first: %v", err)
				return
			}
			if didCreate {
				mu.Lock()
				created++
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	if created != 1 {
		t.Fatalf("published diagnostics=%d, want exactly one", created)
	}
	got, err := store.Read(testRecordingID)
	if err != nil || got.CurrentAttempts < 1 || got.CurrentAttempts > writers {
		t.Fatalf("saved concurrent diagnostic=%+v err=%v", got, err)
	}
}

func TestReadRejectsSymlinkAndInvalidDocument(t *testing.T) {
	root := t.TempDir()
	store, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	path := store.path(testRecordingID)
	if err := os.Symlink(filepath.Join(root, "missing-secret"), path); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	if _, err := store.Read(testRecordingID); err != ErrInvalidRecord {
		t.Fatalf("symlink read error=%v, want invalid record", err)
	}
}
