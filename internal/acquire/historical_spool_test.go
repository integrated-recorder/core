package acquire

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/storage"
)

func TestHistoricalPayloadSpoolPermissionsBoundsAndCleanup(t *testing.T) {
	directory, err := newHistoricalSpoolDirectory()
	if err != nil {
		t.Fatal(err)
	}
	directoryInfo, err := os.Stat(directory)
	if err != nil {
		t.Fatal(err)
	}
	if got := directoryInfo.Mode().Perm(); got != 0o700 {
		t.Fatalf("scratch directory mode=%#o, want 0700", got)
	}
	spool, err := newHistoricalPayloadSpool(directory)
	if err != nil {
		t.Fatal(err)
	}
	fileInfo, err := spool.file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if got := fileInfo.Mode().Perm(); got != 0o600 {
		t.Fatalf("scratch file mode=%#o, want 0600", got)
	}

	content := []byte("bounded-historical-object")
	if err := spool.write(bytes.NewReader(content), int64(len(content)), int64(len(content))); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(content)
	if spool.size != int64(len(content)) || spool.sha256 != hex.EncodeToString(digest[:]) {
		t.Fatalf("spool identity size/hash=%d/%s", spool.size, spool.sha256)
	}
	if err := spool.write(bytes.NewReader(append(content, '!')), int64(len(content)), -1); !errors.Is(err, storage.ErrIngestTooLarge) {
		t.Fatalf("oversized spool write error=%v, want %v", err, storage.ErrIngestTooLarge)
	}
	if err := spool.close(); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("scratch directory retained %d entries after close", len(entries))
	}
	if err := removeHistoricalSpoolDirectory(directory); err != nil {
		t.Fatal(err)
	}
}

func TestHistoricalSpoolReadPayloadRechecksExactBytes(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ingest, err := storage.NewIngestService(store, storage.DefaultIngestOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if closeErr := ingest.Close(ctx); closeErr != nil {
			t.Errorf("close ingest service: %v", closeErr)
		}
	})
	directory, err := newHistoricalSpoolDirectory()
	if err != nil {
		t.Fatal(err)
	}
	spool, err := newHistoricalPayloadSpool(directory)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := spool.close(); closeErr != nil {
			t.Errorf("close historical spool: %v", closeErr)
		}
		if removeErr := removeHistoricalSpoolDirectory(directory); removeErr != nil {
			t.Errorf("remove historical spool directory: %v", removeErr)
		}
	})
	content := []byte("exact-spooled-payload")
	if err := spool.write(bytes.NewReader(content), 1024, int64(len(content))); err != nil {
		t.Fatal(err)
	}
	payload, err := spool.readPayload(context.Background(), ingest, "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
	if err != nil {
		t.Fatal(err)
	}
	if got := payload.Result().SHA256; got != spool.sha256 {
		t.Fatalf("payload digest=%s, spool digest=%s", got, spool.sha256)
	}
	if !bytes.Equal(payload.Bytes(), content) {
		t.Fatalf("loaded payload=%q, want %q", payload.Bytes(), content)
	}
	payload.Release()
	if _, err := spool.file.WriteAt([]byte("X"), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := spool.readPayload(context.Background(), ingest, "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"); !errors.Is(err, errHistoricalScratch) {
		t.Fatalf("modified scratch payload accepted: %v", err)
	}
	if got := ingest.Snapshot().BufferUsedBytes; got != 0 {
		t.Fatalf("rejected scratch payload leaked %d ingest bytes", got)
	}
}
