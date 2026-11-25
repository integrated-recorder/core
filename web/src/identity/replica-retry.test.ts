import { describe, expect, it } from 'vitest'
import { matchesReplicaRetryScope, resumableReplicaRetryIntents, storedKeyReplicaRetryRequest, type DurableReplicaRetryLease, type ReplicaRetryIntent, type ReplicaRetryScope } from './replica-retry'

const scope: ReplicaRetryScope = {
  identityID: 'gi_AAAAAAAAAAAAAAAAAAAAAA',
  nodeID: 'nd_AAAAAAAAAAAAAAAAAAAAAA',
  recordingID: 'rec_123',
  archiveID: 'arc_AAAAAAAAAAAAAAAAAAAAAA',
  snapshotID: `snap_${'a'.repeat(64)}`,
  ownerSubjectID: 'os_AAAAAAAAAAAAAAAAAAAAAA',
  grantHash: `sha256:${'b'.repeat(64)}`,
  providerNodeID: 'nd_BBBBBBBBBBBBBBBBBBBBBB',
}

const intent: ReplicaRetryIntent = { ...scope, leaseID: `rl_${'A'.repeat(22)}`, keyEpoch: 1710000000000 }

describe('replica retry scope', () => {
  it('permits a stored-key retry only for the exact original operation scope', () => {
    expect(matchesReplicaRetryScope(intent, scope)).toBe(true)
  })

  it.each([
    ['identityID', 'gi_BBBBBBBBBBBBBBBBBBBBBB'],
    ['nodeID', 'nd_BBBBBBBBBBBBBBBBBBBBBB'],
    ['recordingID', 'rec_other'],
    ['archiveID', 'arc_BBBBBBBBBBBBBBBBBBBBBB'],
    ['snapshotID', `snap_${'c'.repeat(64)}`],
    ['ownerSubjectID', 'os_BBBBBBBBBBBBBBBBBBBBBB'],
    ['grantHash', `sha256:${'d'.repeat(64)}`],
    ['providerNodeID', 'nd_CCCCCCCCCCCCCCCCCCCCCC'],
  ] as const)('rejects a changed %s', (key, value) => {
    expect(matchesReplicaRetryScope(intent, { ...scope, [key]: value })).toBe(false)
  })

  it('rejects an absent retry intent', () => {
    expect(matchesReplicaRetryScope(null, scope)).toBe(false)
  })

  it('builds a keyless retry request from the original durable operation identity', () => {
    const request = storedKeyReplicaRetryRequest(intent)
    expect(request).toEqual({
      owner_subject_id: scope.ownerSubjectID,
      grant_hash: scope.grantHash,
      key_epoch: intent.keyEpoch,
      lease_id: intent.leaseID,
      provider_node_id: scope.providerNodeID,
    })
    expect(request).not.toHaveProperty('data_key')
    expect(request).not.toHaveProperty('key_envelope')
  })

  it('resumes only unexpired offers bound to the exact owner grant and provider scope', () => {
    const now = Date.parse('2026-11-04T00:00:00Z')
    const candidate: DurableReplicaRetryLease = {
      lease_id: intent.leaseID,
      archive_id: scope.archiveID,
      snapshot_id: scope.snapshotID,
      key_epoch: String(intent.keyEpoch),
      owner_subject_id: scope.ownerSubjectID,
      grant_hash: scope.grantHash,
      provider_node_id: scope.providerNodeID,
      status: 'offered',
      expires_at: '2026-11-05T00:00:00Z',
    }

    expect(resumableReplicaRetryIntents(scope, [candidate], now)).toEqual([intent])
    expect(resumableReplicaRetryIntents(scope, [
      { ...candidate, owner_subject_id: 'os_BBBBBBBBBBBBBBBBBBBBBB' },
      { ...candidate, grant_hash: `sha256:${'c'.repeat(64)}` },
      { ...candidate, provider_node_id: 'nd_CCCCCCCCCCCCCCCCCCCCCC' },
      { ...candidate, status: 'active' },
      { ...candidate, expires_at: '2026-11-03T23:59:59Z' },
      { ...candidate, key_epoch: 'not-a-number' },
    ], now)).toEqual([])
  })
})
