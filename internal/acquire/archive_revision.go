package acquire

import (
	"errors"

	"github.com/integrated-recorder/core/internal/domain"
)

var ErrArchiveRevisionOverflow = errors.New("archive revision overflow")

// advanceArchiveRevision binds derived integrity/export results to changes in
// canonical selected objects. Legacy roots with no revision start at one on
// their first new canonical object mutation.
func advanceArchiveRevision(recording *domain.Recording) error {
	if recording == nil || recording.ArchiveRevision == ^uint64(0) {
		return ErrArchiveRevisionOverflow
	}
	if recording.ArchiveRevision == 0 {
		recording.ArchiveRevision = 1
		return nil
	}
	recording.ArchiveRevision++
	return nil
}
