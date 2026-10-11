import { describe, expect, expectTypeOf, it } from 'vitest'
import { isRecordingState, recordingStates } from './api'
import type { DerivedJobState, ExportJob, IntegrityJobState } from './api'

describe('recording states', () => {
  it('matches the canonical backend state set', () => {
    expect(recordingStates).toEqual(['recording', 'stopped', 'completed', 'interrupted'])
    for (const state of recordingStates) expect(isRecordingState(state)).toBe(true)
    expect(isRecordingState('failed')).toBe(false)
  })
})

it('uses one bounded lifecycle for integrity and export wire jobs', () => {
  expectTypeOf<IntegrityJobState>().toEqualTypeOf<DerivedJobState>()
  expectTypeOf<ExportJob['state']>().toEqualTypeOf<DerivedJobState>()
})

// Keep progress-phase interruption out of durable job lifecycle state.
// @ts-expect-error restart interruption is an error code and progress phase, not a job state
const interruptedJobState: DerivedJobState = 'interrupted'
void interruptedJobState
