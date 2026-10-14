package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/integrated-recorder/core/internal/acquire"
	"github.com/integrated-recorder/core/internal/authn"
)

type acquisitionFlowRouteFixture struct {
	recordingManager
	view  acquire.AcquisitionFlowSnapshot
	calls int
}

func (m *acquisitionFlowRouteFixture) AcquisitionFlowSnapshot(_ context.Context, id string, start uint64, limit int) (acquire.AcquisitionFlowSnapshot, error) {
	m.calls++
	if id != m.view.RecordingID || start != 0 || limit != 96 {
		return acquire.AcquisitionFlowSnapshot{}, acquire.ErrAcquisitionFlowLimit
	}
	return m.view, nil
}

func TestRecordingAcquisitionFlowRouteIsReadOnlyBoundedAndSanitized(t *testing.T) {
	manager := &acquisitionFlowRouteFixture{view: acquire.AcquisitionFlowSnapshot{
		RecordingID: "0123456789abcdef0123456789abcdef", Live: true, FirstOrdinal: 1, TotalOrdinals: 1, StartOrdinal: 1,
		Segments: []acquire.AcquisitionFlowSegment{{TrackID: "main", Sequence: 10, PresentationOrdinal: 1, State: "pending", Observed: true}},
		Actors:   []acquire.AcquisitionFlowActor{},
	}}
	handler := NewWithOptions(manager, nil, nil, Options{})
	request := httptest.NewRequest(http.MethodGet, "/api/recordings/"+manager.view.RecordingID+"/acquisition-flow?limit=96", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || manager.calls != 1 || !strings.Contains(response.Body.String(), `"sequence":10`) || strings.Contains(response.Body.String(), "source_uri") {
		t.Fatalf("flow response status=%d calls=%d body=%s", response.Code, manager.calls, response.Body.String())
	}
	bad := httptest.NewRecorder()
	handler.ServeHTTP(bad, httptest.NewRequest(http.MethodGet, "/api/recordings/"+manager.view.RecordingID+"/acquisition-flow?limit=129", nil))
	if bad.Code != http.StatusBadRequest || manager.calls != 1 {
		t.Fatalf("unbounded request status=%d calls=%d body=%s", bad.Code, manager.calls, bad.Body.String())
	}
	permission, ok := recordingPermission(http.MethodGet, "/api/recordings/"+manager.view.RecordingID+"/acquisition-flow")
	if !ok || permission != authn.PermissionRecordingRead {
		t.Fatalf("acquisition flow permission=%q ok=%t", permission, ok)
	}
}
