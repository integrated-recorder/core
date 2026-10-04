package domain

import (
	"encoding/json"
	"fmt"

	"github.com/integrated-recorder/core/internal/streammeta"
)

func ValidateMetadataTimeline(items []MetadataRevision) error {
	if len(items) > streammeta.MaxTimelineRevisions {
		return fmt.Errorf("metadata timeline exceeds revision limit")
	}
	encoded, err := json.Marshal(items)
	if err != nil || len(encoded) > streammeta.MaxTimelineBytes {
		return fmt.Errorf("metadata timeline exceeds size limit")
	}
	for _, item := range items {
		if item.ObservedAt.IsZero() {
			return fmt.Errorf("metadata timeline timestamp is invalid")
		}
		if item.Title == nil && item.Description == nil {
			return fmt.Errorf("metadata timeline revision has no known fields")
		}
		if err := streammeta.ValidateText(item.Title, streammeta.MaxTitleBytes); err != nil {
			return err
		}
		if err := streammeta.ValidateText(item.Description, streammeta.MaxDescriptionBytes); err != nil {
			return err
		}
		if err := streammeta.ValidateTimestamp(item.SourceUpdatedAt); err != nil {
			return err
		}
	}
	return nil
}
