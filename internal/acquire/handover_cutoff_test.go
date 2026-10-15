package acquire

import (
	"context"
	"testing"
)

func cutoffTestScheduler(states ...segmentTaskState) (*segmentScheduler, []*segmentTask) {
	ctx, cancel := context.WithCancel(context.Background())
	s := &segmentScheduler{ctx: ctx, cancel: cancel, changed: make(chan struct{}), tasks: make(map[segmentTaskKey]*segmentTask)}
	tasks := make([]*segmentTask, 0, len(states))
	for index, state := range states {
		key := segmentTaskKey{epoch: 1, discontinuitySequence: 2, sequence: uint64(index + 1)}
		task := &segmentTask{key: key, ordinal: uint64(index + 1), state: state}
		s.tasks[key] = task
		tasks = append(tasks, task)
		if state == segmentTaskQueued {
			s.ready = append(s.ready, task)
		}
	}
	return s, tasks
}

func TestHandoverCutoffAndWorkerClaimShareSchedulerLock(t *testing.T) {
	t.Run("worker claim wins", func(t *testing.T) {
		s, tasks := cutoffTestScheduler(segmentTaskQueued)
		s.mu.Lock()
		claimed := s.claimReadyTaskLocked("W1")
		s.mu.Unlock()
		if claimed != tasks[0] || claimed.state != segmentTaskInFlight {
			t.Fatalf("worker did not claim queued task before cutoff: task=%+v", claimed)
		}
		s.beginHandoverCutoff()
		if tasks[0].state != segmentTaskInFlight || !handoverDrainCohort(tasks[0]) {
			t.Fatalf("pre-cutoff attempt was not retained in source cohort: %+v", tasks[0])
		}
	})

	t.Run("cutoff wins", func(t *testing.T) {
		s, tasks := cutoffTestScheduler(segmentTaskQueued)
		s.beginHandoverCutoff()
		s.mu.Lock()
		claimed := s.claimReadyTaskLocked("W1")
		s.mu.Unlock()
		if claimed != nil {
			t.Fatalf("worker claimed a source body after cutoff: %+v", claimed)
		}
		if tasks[0].state != segmentTaskHandoverParked || tasks[0].workerID != "" || handoverDrainCohort(tasks[0]) {
			t.Fatalf("unstarted source task was not parked for target: %+v", tasks[0])
		}
	})
}

func TestHandoverCutoffParksQueuedRetryAwaitManifestAndRefreshWithoutGapCohort(t *testing.T) {
	s, tasks := cutoffTestScheduler(segmentTaskQueued, segmentTaskRetryWait, segmentTaskAwaitManifest, segmentTaskRefreshing)
	readyBefore := len(s.ready)
	canceled := 0
	for _, task := range tasks[1:3] {
		task.retryCancel = func() { canceled++ }
	}
	tasks[3].refreshCancel = func() { canceled++ }
	s.beginHandoverCutoff()
	if len(s.ready) != 0 || readyBefore != 1 {
		t.Fatalf("cutoff did not clear source dispatch queue: before=%d after=%d", readyBefore, len(s.ready))
	}
	for index, task := range tasks {
		if task.state != segmentTaskHandoverParked || handoverDrainCohort(task) {
			t.Fatalf("unstarted task %d remains in source drain cohort: %+v", index, task)
		}
	}
	if canceled != 3 {
		t.Fatalf("cutoff canceled %d refresh/retry operations, want 3", canceled)
	}
	if !s.liveAdmissionClosed || !s.handoverCutoff {
		t.Fatal("cutoff did not close live admission")
	}
}

func TestHandoverCutoffKeepsBufferedAndPersistingCanonicalWork(t *testing.T) {
	s, tasks := cutoffTestScheduler(segmentTaskBuffered, segmentTaskPersisting, segmentTaskGapCommitting)
	s.beginHandoverCutoff()
	for index, task := range tasks {
		if !handoverDrainCohort(task) || task.state == segmentTaskHandoverParked {
			t.Fatalf("accepted canonical task %d was removed from source drain cohort: %+v", index, task)
		}
	}
}

func TestHandoverCutoffClosesObserverCanonicalAdmission(t *testing.T) {
	e := &entry{}
	if !e.beginObserverCanonicalCommit() {
		t.Fatal("observer callback should be admitted before cutoff")
	}
	e.closeObserverCommitAdmission()
	if e.beginObserverCanonicalCommit() {
		t.Fatal("observer callback crossed canonical admission after cutoff")
	}
	e.openObserverCommitAdmission()
	if !e.beginObserverCanonicalCommit() {
		t.Fatal("source resume did not reopen observer canonical admission")
	}
}
