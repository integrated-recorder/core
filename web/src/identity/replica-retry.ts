export type ReplicaRetryScope = Readonly<{
  identityID: string
  nodeID: string
  recordingID: string
  archiveID: string
  snapshotID: string
  ownerSubjectID: string
  grantHash: string
  providerNodeID: string
}>

export type ReplicaRetryIntent = ReplicaRetryScope & Readonly<{
  leaseID: string
  keyEpoch: number
}>

export type StoredKeyReplicaRetryRequest = Readonly<{
  owner_subject_id: string
  grant_hash: string
  key_epoch: number
  lease_id: string
  provider_node_id: string
}>

export type DurableReplicaRetryLease = Readonly<{
  lease_id: string
  archive_id: string
  snapshot_id: string
  key_epoch: string
  owner_subject_id: string
  grant_hash: string
  provider_node_id: string
  status: string
  expires_at: string
}>

export function matchesReplicaRetryScope(intent: ReplicaRetryIntent | null, current: ReplicaRetryScope): intent is ReplicaRetryIntent {
  return intent !== null
    && intent.identityID === current.identityID
    && intent.nodeID === current.nodeID
    && intent.recordingID === current.recordingID
    && intent.archiveID === current.archiveID
    && intent.snapshotID === current.snapshotID
    && intent.ownerSubjectID === current.ownerSubjectID
    && intent.grantHash === current.grantHash
    && intent.providerNodeID === current.providerNodeID
}

export function storedKeyReplicaRetryRequest(intent: ReplicaRetryIntent): StoredKeyReplicaRetryRequest {
  return {
    owner_subject_id: intent.ownerSubjectID,
    grant_hash: intent.grantHash,
    key_epoch: intent.keyEpoch,
    lease_id: intent.leaseID,
    provider_node_id: intent.providerNodeID,
  }
}

// Only a still-live offer can be replayed with the same signed lease identity.
// The server remains authoritative and rechecks the current grant before reuse.
export function resumableReplicaRetryIntents(
  scope: ReplicaRetryScope,
  leases: readonly DurableReplicaRetryLease[],
  now = Date.now(),
): ReplicaRetryIntent[] {
  return leases
    .filter(lease => {
      const keyEpoch = Number(lease.key_epoch)
      return lease.archive_id === scope.archiveID
        && lease.snapshot_id === scope.snapshotID
        && lease.owner_subject_id === scope.ownerSubjectID
        && lease.grant_hash === scope.grantHash
        && lease.provider_node_id === scope.providerNodeID
        && lease.status === 'offered'
        && Number.isSafeInteger(keyEpoch)
        && keyEpoch > 0
        && Number.isFinite(Date.parse(lease.expires_at))
        && Date.parse(lease.expires_at) > now
    })
    .sort((left, right) => left.lease_id.localeCompare(right.lease_id))
    .map(lease => ({ ...scope, leaseID: lease.lease_id, keyEpoch: Number(lease.key_epoch) }))
}
