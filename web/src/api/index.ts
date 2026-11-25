import { api, queryString } from './client'
import { fetchCiphertextRangeWithRetry } from '@/identity/ciphertext-range'
import type { AccountAuthorization, AccountRole, AccountSession, AccountUser, Adapter, AdapterConfig, AdapterDescriptor, AdapterReconcileResult, ApiSession, ArchiveIndexPage, ArchiveSnapshotIdentity, AuditEvent, Dashboard, ExportJob, GlobalIdentityForkChoice, GlobalIdentityHistoryBundle, GlobalIdentitySession, GlobalIdentitySnapshot, GlobalIdentityStatus, InstallationStatus, IntegrityJob, IntegrityResult, LogEntry, NodeEnrollmentProof, NodeIdentityInfo, NodeIdentityStatus, Notification, OwnerAuthoritySubject, OwnerAuthoritySubjects, PeerArchiveCapabilities, PeerArchiveHead, PeerSessions, PermissionID, PluginRegistryStatus, PreviewFramesResponse, RecordingDetail, RecordingEvent, RecordingMetadata, RecordingPage, RecordingSummary, RegisteredGlobalIdentity, ReplicaInbox, ReplicaLeaseList, ReplicaManifest, ReplicaTombstoneResult, Resource, ResourceRef, RuntimeUpdateStatus, Schema, SearchResult, SetupStorageTest, StorageInfo, StorageInstanceCreateBody, StorageInstanceSummary, StorageMetricsResponse, StoragePoolsResponse, StorageProviderConfig, StorageProviderConfigBody, StorageProviderStatus, StorageSettings, SystemInfo, SystemSettings, TimedEventPage, UserPreferences, VerifiedGlobalIdentityRecipient, WatchEvent, WatchView, WorkflowHistoryEvent, WorkflowProgress, WorkflowSummary } from '@/types/api'

export const authAPI = {
  session: () => api<ApiSession>('/api/auth/session'),
  bootstrap: (login: string, password: string) => api<ApiSession>('/api/auth/bootstrap', { method: 'POST', body: { login, password } }),
  login: (login: string, password: string) => api<ApiSession>('/api/auth/login', { method: 'POST', body: { login, password } }),
  logout: () => api<void>('/api/auth/logout', { method: 'POST' }),
}
export const globalIdentityAPI = {
  status: () => api<GlobalIdentityStatus>('/api/global-identity/status'),
  ownerAuthoritySubjects: () => api<OwnerAuthoritySubjects>('/api/owner-authority/subjects'),
  ownerAuthoritySubject: (subjectId: string) => api<OwnerAuthoritySubject>(`/api/owner-authority/subjects/${encodeURIComponent(subjectId)}`),
  syncOwnerAuthorityHistory: (subjectId: string) => api<OwnerAuthoritySubject>(`/api/owner-authority/subjects/${encodeURIComponent(subjectId)}/sync`, { method: 'POST' }),
  applyOwnerAuthorityEvent: (signedEvent: { event: unknown; signature: string }) => api<OwnerAuthoritySubject>('/api/owner-authority/events', { method: 'POST', body: signedEvent }),
  exportHistory: () => api<GlobalIdentityHistoryBundle>('/api/global-identity/history'),
  forkChoices: () => api<{ forks: GlobalIdentityForkChoice[] }>('/api/global-identity/forks'),
  resolveFork: (body: { canonical_event: string; signature: string }) => api<GlobalIdentitySnapshot>('/api/global-identity/forks/resolve', { method: 'POST', body }),
  publishHistory: () => api<{ identity_id: string; head_hash: string; providers: Array<{ host: string; success: boolean }> }>('/api/global-identity/history/publish', { method: 'POST' }),
  importHistory: (bundle: GlobalIdentityHistoryBundle) => api<GlobalIdentitySnapshot>('/api/global-identity/history/import', { method: 'POST', body: bundle }),
  registry: () => api<{ identities: RegisteredGlobalIdentity[] }>('/api/global-identity/registry'),
  verifiedRecipient: (identityId: string) => api<VerifiedGlobalIdentityRecipient>(`/api/global-identity/verified/${encodeURIComponent(identityId)}`),
  importRegistryHistory: (bundle: GlobalIdentityHistoryBundle) => api<RegisteredGlobalIdentity>('/api/global-identity/registry/import', { method: 'POST', body: bundle }),
  enrollmentChallenge: (identityId: string, previousEventHash: string) => api<NodeEnrollmentProof>('/api/node-identity/enrollment/challenge', { method: 'POST', body: { identity_id: identityId, previous_event_hash: previousEventHash } }),
  delegateNode: (body: { canonical_event: string; signature: string }) => api<GlobalIdentitySnapshot>('/api/global-identity/nodes/delegate', { method: 'POST', body }),
  revokeNode: (nodeId: string, body: { canonical_event: string; signature: string }) => api<GlobalIdentitySnapshot>(`/api/global-identity/nodes/${encodeURIComponent(nodeId)}/revoke`, { method: 'POST', body }),
  nodeStatus: () => api<NodeIdentityStatus>('/api/node-identity/status'),
  createNodeIdentity: () => api<{ created: true; node: NodeIdentityInfo; audit_recorded: boolean }>('/api/node-identity/create', { method: 'POST' }),
  registrationOptions: (identityId: string) => api<unknown>('/api/global-identity/registration/options', { method: 'POST', body: { identity_id: identityId } }),
  finishRegistration: (body: { credential: Record<string, unknown>; canonical_event: string; signature: string }) => api<{ initialized: true; identity: GlobalIdentitySnapshot }>('/api/global-identity/registration/finish', { method: 'POST', body }),
  revokeCredential: (credentialId: string, body: { canonical_event: string; signature: string }) => api<GlobalIdentitySnapshot>(`/api/global-identity/credentials/${encodeURIComponent(credentialId)}/revoke`, { method: 'POST', body }),
  loginOptions: (recipientPublicKey?: string) => api<unknown>('/api/global-identity/login/options', { method: 'POST', body: recipientPublicKey ? { recipient_public_key: recipientPublicKey } : {} }),
  loginFinish: (credential: Record<string, unknown>) => api<{ authenticated: true; identity_id: string }>('/api/global-identity/login/finish', { method: 'POST', body: credential }),
  session: () => api<GlobalIdentitySession>('/api/global-identity/session'),
  sessionKeyEpochs: (nodeID: string, ownerSubjectID: string, archiveID: string, grantHash: string, snapshotID: string) =>
    api<{ node_id: string; archive_id: string; snapshot_id: string; key_epochs: string[] }>(`/api/global-identity/peer-archives/${encodeURIComponent(nodeID)}/snapshots/${encodeURIComponent(snapshotID)}/session-key-epochs${queryString({ owner_subject_id: ownerSubjectID, archive_id: archiveID, grant_hash: grantHash })}`),
  sessionDEKEnvelope: (nodeID: string, ownerSubjectID: string, archiveID: string, grantHash: string, snapshotID: string, keyEpoch: string, csrfToken: string) =>
    api<import('@/identity/cryptoarchive').SessionDEKEnvelope>(`/api/global-identity/peer-archives/${encodeURIComponent(nodeID)}/snapshots/${encodeURIComponent(snapshotID)}/session-key-envelope${queryString({ owner_subject_id: ownerSubjectID, archive_id: archiveID, grant_hash: grantHash, key_epoch: keyEpoch })}`, { method: 'POST', headers: { 'X-IR-Global-CSRF': csrfToken }, body: {} }),
  logout: (csrfToken: string) => api<void>('/api/global-identity/logout', { method: 'POST', headers: { 'X-IR-Global-CSRF': csrfToken }, body: {} }),
}
export const peerSessionsAPI = {
  list: () => api<PeerSessions>('/api/peer-sessions'),
  connect: (identityId: string, nodeId: string) => api<void>('/api/peer-sessions', { method: 'POST', body: { identity_id: identityId, node_id: nodeId } }),
  disconnect: (nodeId: string) => api<void>(`/api/peer-sessions/${encodeURIComponent(nodeId)}`, { method: 'DELETE' }),
}
export const replicaImportsAPI = {
  offers: (sourceNodeID: string, ownerSubjectID: string, archiveID: string) =>
    api<ReplicaInbox>(`/api/peer-archives/${encodeURIComponent(sourceNodeID)}/replica-offers${queryString({ owner_subject_id: ownerSubjectID, archive_id: archiveID })}`),
  importFromPeer: (body: { source_node_id: string; owner_subject_id: string; grant_hash: string; archive_id: string; snapshot_id: string; lease_id: string }) =>
    api<{ lease: import('@/types/api').ReplicaLeaseSummary; snapshot_id: string; objects: number }>('/api/replica-imports', { method: 'POST', body }),
}
export const replicaLeasesAPI = {
  list: () => api<ReplicaLeaseList>('/api/replica-leases'),
}
export const replicaTombstonesAPI = {
  submit: (body: { owner_subject_id: string; archive_id: string; grant_hash: string; reason_code?: 'owner_requested' | 'policy_retention' }) =>
    api<ReplicaTombstoneResult>('/api/replica-tombstones', { method: 'POST', body }),
}
export const peerArchivesAPI = {
  capabilities: () => api<PeerArchiveCapabilities>('/api/peer-archives/capabilities'),
  head: (nodeID: string, ownerSubjectID: string, archiveID: string, grantHash: string) => api<PeerArchiveHead>(`/api/peer-archives/${encodeURIComponent(nodeID)}/head${queryString({ owner_subject_id: ownerSubjectID, archive_id: archiveID, grant_hash: grantHash })}`),
  snapshot: (nodeID: string, ownerSubjectID: string, archiveID: string, grantHash: string, snapshotID: string) =>
    api<PeerArchiveHead>(`/api/peer-archives/${encodeURIComponent(nodeID)}/snapshots/${encodeURIComponent(snapshotID)}${queryString({ owner_subject_id: ownerSubjectID, archive_id: archiveID, grant_hash: grantHash })}`),
  manifest: (nodeID: string, ownerSubjectID: string, archiveID: string, grantHash: string, snapshotID: string, keyEpoch: string) =>
    api<ReplicaManifest>(`/api/peer-archives/${encodeURIComponent(nodeID)}/snapshots/${encodeURIComponent(snapshotID)}/manifest${queryString({ owner_subject_id: ownerSubjectID, archive_id: archiveID, grant_hash: grantHash, key_epoch: keyEpoch })}`),
  keyEpochs: (nodeID: string, ownerSubjectID: string, archiveID: string, grantHash: string, snapshotID: string) =>
    api<{ node_id: string; archive_id: string; snapshot_id: string; key_epochs: string[] }>(`/api/peer-archives/${encodeURIComponent(nodeID)}/snapshots/${encodeURIComponent(snapshotID)}/key-epochs${queryString({ owner_subject_id: ownerSubjectID, archive_id: archiveID, grant_hash: grantHash })}`),
  keyEnvelope: (nodeID: string, ownerSubjectID: string, archiveID: string, grantHash: string, snapshotID: string, keyEpoch: string) =>
    api<import('@/identity/cryptoarchive').KeyEnvelope>(`/api/peer-archives/${encodeURIComponent(nodeID)}/snapshots/${encodeURIComponent(snapshotID)}/key-envelope${queryString({ owner_subject_id: ownerSubjectID, archive_id: archiveID, grant_hash: grantHash, key_epoch: keyEpoch })}`),
  ciphertextRange: async (nodeID: string, ownerSubjectID: string, archiveID: string, grantHash: string, snapshotID: string, keyEpoch: string, objectID: string, offset: number, length: number, signal?: AbortSignal) => {
    if (!Number.isSafeInteger(offset) || offset < 0 || !Number.isSafeInteger(length) || length < 1 || length > 4 * 1024 * 1024) {
      throw new TypeError('peer archive range is invalid')
    }
    const path = `/api/peer-archives/${encodeURIComponent(nodeID)}/snapshots/${encodeURIComponent(snapshotID)}/objects/${encodeURIComponent(objectID)}${queryString({ owner_subject_id: ownerSubjectID, archive_id: archiveID, grant_hash: grantHash, key_epoch: keyEpoch, offset, length })}`
    return fetchCiphertextRangeWithRetry(
      requestSignal => fetch(path, { credentials: 'same-origin', cache: 'no-store', signal: requestSignal }),
      offset,
      length,
      signal,
    )
  },
}
export const setupAPI = {
  status: () => api<InstallationStatus>('/api/setup/status'),
  begin: () => api<InstallationStatus>('/api/setup/begin', { method: 'POST', body: {} }),
  storageTest: () => api<SetupStorageTest>('/api/setup/storage-test', { method: 'POST', body: {} }),
  complete: () => api<InstallationStatus>('/api/setup/complete', { method: 'POST', body: {} }),
}
export const dashboardAPI = {
  get: () => api<Dashboard>('/api/dashboard'), storage: () => api<StorageInfo>('/api/system/storage'), info: () => api<SystemInfo>('/api/system/info'),
}
export type StorageMetricWindow = '1h' | '6h' | '24h'
export const storageAPI = {
  pools: () => api<StoragePoolsResponse>('/api/storage/pools'),
  metrics: (poolId: string, window: StorageMetricWindow) => api<StorageMetricsResponse>(`/api/storage/pools/${encodeURIComponent(poolId)}/metrics${queryString({ window })}`),
}
export const storageProvidersAPI = {
  status: () => api<StorageProviderStatus>('/api/runtime/storage/provider'),
  instances: () => api<StorageInstanceSummary[]>('/api/runtime/storage/instances'),
  createInstance: (body: StorageInstanceCreateBody) => api<StorageInstanceSummary>('/api/runtime/storage/instances', { method: 'POST', body }),
  instanceConfig: (id: string) => api<StorageProviderConfig>(`/api/runtime/storage/instances/${encodeURIComponent(id)}/config`),
  saveInstanceConfig: (id: string, body: StorageProviderConfigBody) => api<StorageProviderConfig>(`/api/runtime/storage/instances/${encodeURIComponent(id)}/config`, { method: 'PUT', body }),
  probeInstance: (id: string) => api<void>(`/api/runtime/storage/instances/${encodeURIComponent(id)}/probe`, { method: 'POST', body: {} }),
  activateInstance: (id: string) => api<StorageProviderStatus>(`/api/runtime/storage/instances/${encodeURIComponent(id)}/activate`, { method: 'POST', body: {} }),
  config: (id: string) => api<StorageProviderConfig>(`/api/runtime/storage/providers/${encodeURIComponent(id)}/config`),
  saveConfig: (id: string, body: StorageProviderConfigBody) => api<StorageProviderConfig>(`/api/runtime/storage/providers/${encodeURIComponent(id)}/config`, { method: 'PUT', body }),
  probe: (id: string) => api<void>(`/api/runtime/storage/providers/${encodeURIComponent(id)}/probe`, { method: 'POST', body: {} }),
  activate: (id: string) => api<StorageProviderStatus>(`/api/runtime/storage/providers/${encodeURIComponent(id)}/activate`, { method: 'POST', body: {} }),
}
export type RecordingQuery = { q?: string; state?: string; adapter?: string; resource_type?: string; started_after?: string; started_before?: string; has_gaps?: string; integrity?: string; tag?: string; sort?: string; limit?: number; cursor?: string }
export type PreviewQuery = { sampling: 'uniform' | 'recent' | 'nearest'; limit: number; time_seconds?: number }
function encodeRef(ref: ResourceRef) { return btoa(unescape(encodeURIComponent(JSON.stringify(ref)))).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '') }
export const recordingsAPI = {
  list: (query: RecordingQuery) => api<RecordingPage>(`/api/v2/recordings${queryString(query)}`),
  get: (id: string) => api<RecordingDetail>(`/api/recordings/${encodeURIComponent(id)}`),
  acquisitionFlow: (id: string, start = 0, limit = 96) => api<import('@/types/api').AcquisitionFlowSnapshot>(`/api/recordings/${encodeURIComponent(id)}/acquisition-flow${queryString({ start: start || undefined, limit })}`),
  lifecycle: (id: string) => api<import('@/types/api').RecordingLifecycle>(`/api/recordings/${encodeURIComponent(id)}/lifecycle`),
  start: (body: { adapter_id: string; input: Record<string, unknown>; resource?: ResourceRef; title?: string; preview_mode: 'disabled' | 'segment' }) => api<RecordingDetail | WorkflowProgress>('/api/recordings', { method: 'POST', body }),
  stop: (id: string) => api<RecordingDetail>(`/api/recordings/${encodeURIComponent(id)}/stop`, { method: 'POST' }),
  complete: (id: string) => api<RecordingDetail>(`/api/recordings/${encodeURIComponent(id)}/complete`, { method: 'POST' }),
  seal: (id: string) => api<import('@/types/api').RecordingLifecycle>(`/api/recordings/${encodeURIComponent(id)}/seal`, { method: 'POST' }),
  remove: (id: string) => api<void>(`/api/recordings/${encodeURIComponent(id)}`, { method: 'DELETE' }),
  tags: (id: string) => api<{ tags: string[] }>(`/api/recordings/${encodeURIComponent(id)}/tags`),
  setTags: (id: string, tags: string[]) => api<{ tags: string[] }>(`/api/recordings/${encodeURIComponent(id)}/tags`, { method: 'PUT', body: { tags } }),
  archive: (id: string, query: { limit?: number; cursor?: string } = {}) => api<ArchiveIndexPage>(`/api/recordings/${encodeURIComponent(id)}/archive/index${queryString(query)}`),
  archiveSnapshotIdentity: (id: string) => api<ArchiveSnapshotIdentity>(`/api/recordings/${encodeURIComponent(id)}/archive/snapshot`),
  encryptArchiveReplica: (id: string, body: { owner_subject_id: string; grant_hash: string; key_epoch: number; data_key?: string; key_envelope?: import('@/identity/cryptoarchive').KeyEnvelope }) => api<{ manifest: ReplicaManifest }>(`/api/recordings/${encodeURIComponent(id)}/replicas/encrypt`, { method: 'POST', body }),
  rekeyArchiveHistory: (id: string, body: { owner_subject_id: string; grant_hash: string; recipient_identity_id: string; previous_key_epoch: number; key_epoch: number; data_key?: string; key_envelope: import('@/identity/cryptoarchive').KeyEnvelope }) => api<{ verification: { archive_id: string; previous_key_epoch: string; new_key_epoch: string; snapshot_count: string; head_sequence: string; head_snapshot_id: string; bundle_set_commitment: string; envelope_sha256: string; recipient_key_id: string }; key_envelope: import('@/identity/cryptoarchive').KeyEnvelope }>(`/api/recordings/${encodeURIComponent(id)}/replicas/rekey`, { method: 'POST', body }),
  createReplicaOffer: (id: string, body: { owner_subject_id: string; grant_hash: string; key_epoch: number; data_key?: string; key_envelope?: import('@/identity/cryptoarchive').KeyEnvelope; lease_id: string; provider_node_id: string }) => api<{ manifest?: ReplicaManifest; lease: import('@/types/api').ReplicaLeaseSummary; reused?: boolean }>(`/api/recordings/${encodeURIComponent(id)}/replicas/offers`, { method: 'POST', body }),
  events: (id: string) => api<{ items: RecordingEvent[] }>(`/api/recordings/${encodeURIComponent(id)}/events`),
  timedEvents: (id: string, query: { limit?: number; cursor?: string; after?: number; tail?: 1; type?: string; from_ms?: number; to_ms?: number } = {}) => api<TimedEventPage>(`/api/recordings/${encodeURIComponent(id)}/timed-events${queryString(query)}`),
  timedEventsExport: (id: string) => `/api/recordings/${encodeURIComponent(id)}/timed-events/export`,
  metadata: (id: string) => api<RecordingMetadata>(`/api/recordings/${encodeURIComponent(id)}/metadata`),
  previews: (id: string, query: PreviewQuery) => api<PreviewFramesResponse>(`/api/recordings/${encodeURIComponent(id)}/previews${queryString(query)}`),
  previewFrame: (id: string, ordinal: number) => `/api/recordings/${encodeURIComponent(id)}/previews/${encodeURIComponent(String(ordinal))}`,
  enablePreviews: (id: string) => api<import('@/types/api').PreviewSummary>(`/api/recordings/${encodeURIComponent(id)}/previews`, { method: 'POST', body: { mode: 'segment' } }),
}
export const integrityAPI = {
  get: (id: string) => api<IntegrityResult>(`/api/recordings/${encodeURIComponent(id)}/integrity`),
  start: (id: string) => api<IntegrityJob>(`/api/recordings/${encodeURIComponent(id)}/integrity/verify`, { method: 'POST' }),
  job: (id: string) => api<IntegrityJob>(`/api/integrity/jobs/${encodeURIComponent(id)}`),
  cancel: (id: string) => api<IntegrityJob>(`/api/integrity/jobs/${encodeURIComponent(id)}/cancel`, { method: 'POST' }),
}
export const derivativeAPI = {
  exports: (id: string) => api<{ items: ExportJob[]; available: boolean }>(`/api/recordings/${encodeURIComponent(id)}/exports`),
  create: (id: string) => api<ExportJob>(`/api/recordings/${encodeURIComponent(id)}/exports`, { method: 'POST', body: { format: 'mkv' } }),
  remove: (id: string) => api<void>(`/api/exports/${encodeURIComponent(id)}`, { method: 'DELETE' }),
  download: (id: string) => `/api/exports/${encodeURIComponent(id)}/download`,
}
export const adaptersAPI = {
  list: () => api<Adapter[]>('/api/adapters'), get: (id: string) => api<Adapter>(`/api/adapters/${encodeURIComponent(id)}`),
  reconcile: () => api<AdapterReconcileResult>('/api/runtime/adapters/reconcile', { method: 'POST', body: {} }),
  schema: (id: string, resource?: ResourceRef) => api<{ input_schema: Schema; configuration_schema: Schema; resource_types?: AdapterDescriptor['resource_types']; media_types: string[] }>(`/api/adapters/${encodeURIComponent(id)}/schema${resource ? queryString({ resource: encodeRef(resource) }) : ''}`),
  config: (id: string, resource?: ResourceRef) => api<AdapterConfig>(`/api/adapters/${encodeURIComponent(id)}/config${resource ? queryString({ resource: encodeRef(resource) }) : ''}`),
  saveConfig: (id: string, body: { resource?: ResourceRef; values?: Record<string, unknown>; secrets?: Record<string, string>; clear_values?: string[]; clear_secrets?: string[] }) => api<AdapterConfig>(`/api/adapters/${encodeURIComponent(id)}/config`, { method: 'PUT', body }),
  action: (id: string, action: 'restart' | 'enable' | 'disable') => api<Adapter>(`/api/adapters/${encodeURIComponent(id)}/${action}`, { method: 'POST' }),
  resources: (id: string, options: { q?: string; parent?: ResourceRef; resource_type?: string; cursor?: string; limit?: number }) => api<{ items: Resource[]; next_cursor?: string }>(`/api/adapters/${encodeURIComponent(id)}/resources${options.q ? `/search${queryString({ q: options.q, parent: options.parent ? encodeRef(options.parent) : undefined, resource_type: options.resource_type, cursor: options.cursor, limit: options.limit })}` : queryString({ parent: options.parent ? encodeRef(options.parent) : undefined, resource_type: options.resource_type, cursor: options.cursor, limit: options.limit })}`),
}
export const pluginsAPI = {
  status: () => api<PluginRegistryStatus>('/api/runtime/plugins'),
  refresh: () => api<PluginRegistryStatus>('/api/runtime/plugins/refresh', { method: 'POST', body: {} }),
  install: (id: string) => api<PluginRegistryStatus>(`/api/runtime/plugins/${encodeURIComponent(id)}/install`, { method: 'POST', body: {} }),
  update: (id: string) => api<PluginRegistryStatus>(`/api/runtime/plugins/${encodeURIComponent(id)}/update`, { method: 'POST', body: {} }),
  uninstall: (id: string) => api<PluginRegistryStatus>(`/api/runtime/plugins/${encodeURIComponent(id)}`, { method: 'DELETE', body: {} }),
}
export const workflowsAPI = {
  list: () => api<WorkflowSummary[]>('/api/resolve-workflows'), get: (id: string) => api<WorkflowProgress>(`/api/resolve-workflows/${encodeURIComponent(id)}`),
  continue: (id: string, body: { values?: Record<string, unknown>; secrets?: Record<string, string>; persist_fields?: string[] }) => api<WorkflowProgress | RecordingDetail>(`/api/resolve-workflows/${encodeURIComponent(id)}/continue`, { method: 'POST', body }),
  cancel: (id: string) => api<void>(`/api/resolve-workflows/${encodeURIComponent(id)}`, { method: 'DELETE' }),
  history: () => api<{ items: WorkflowHistoryEvent[] }>('/api/workflow-history'),
}
export type WatchMutationBody = {
  adapter_id: string; input: Record<string, unknown>; input_secrets: Record<string, string>;
  resource?: ResourceRef; clear_resource?: boolean; title?: string; preview_mode?: 'disabled' | 'segment';
  check_interval_seconds?: number; clear_input_secrets?: string[]
}
export const watchesAPI = {
  list: () => api<{ items: WatchView[] }>('/api/watches'),
  get: (id: string) => api<WatchView>(`/api/watches/${encodeURIComponent(id)}`),
  create: (body: WatchMutationBody) => api<WatchView>('/api/watches', { method: 'POST', body }),
  update: (id: string, body: WatchMutationBody) => api<WatchView>(`/api/watches/${encodeURIComponent(id)}`, { method: 'PUT', body }),
  remove: (id: string) => api<void>(`/api/watches/${encodeURIComponent(id)}`, { method: 'DELETE' }),
  enable: (id: string) => api<WatchView>(`/api/watches/${encodeURIComponent(id)}/enable`, { method: 'POST' }),
  disable: (id: string) => api<WatchView>(`/api/watches/${encodeURIComponent(id)}/disable`, { method: 'POST' }),
  check: (id: string) => api<WatchView>(`/api/watches/${encodeURIComponent(id)}/check`, { method: 'POST' }),
  recordings: (id: string, limit = 20) => api<{ items: RecordingSummary[]; truncated?: boolean; retention_limit?: number }>(`/api/watches/${encodeURIComponent(id)}/recordings${queryString({ limit })}`),
  events: (id: string, limit = 50) => api<{ items: WatchEvent[] }>(`/api/watches/${encodeURIComponent(id)}/events${queryString({ limit })}`),
}
export const productAPI = {
  search: (q: string) => api<{ results: SearchResult[]; partial_errors?: string[] }>(`/api/search${queryString({ q, limit: 20 })}`),
  notifications: () => api<{ items: Notification[] }>('/api/notifications'),
  markRead: (id: string) => api<void>(`/api/notifications/${encodeURIComponent(id)}/read`, { method: 'POST' }),
  markAllRead: () => api<void>('/api/notifications/read-all', { method: 'POST' }),
  audit: () => api<{ items: AuditEvent[] }>('/api/audit'),
  logs: (query: { level?: string; component?: string; q?: string; limit?: number; cursor?: string }) => api<{ items: LogEntry[]; next_cursor?: string }>(`/api/logs${queryString(query)}`),
  settings: () => api<SystemSettings>('/api/settings'),
  saveSettings: (body: { ui?: { theme: 'system' | 'light' | 'dark' }; integrity?: { concurrency: number }; retention?: { enabled?: boolean; completed_after_days?: number }; storage?: StorageSettings }) => api<SystemSettings>('/api/settings', { method: 'PUT', body }),
  retentionCandidates: () => api<{ enabled: boolean; candidate_count: number; candidates: { id: string; stopped_at: string }[] }>('/api/retention/candidates'),
  runRetention: () => api<{ candidate_count: number; deleted_count: number; deleted_ids: string[] }>('/api/retention/run', { method: 'POST', body: {} }),
}
export const userPreferencesAPI = {
  get: () => api<UserPreferences>('/api/user/preferences'),
  update: (preferences: UserPreferences) => api<UserPreferences>('/api/user/preferences', { method: 'PUT', body: preferences }),
}
export const accountAPI = {
  self: () => api<AccountUser>('/api/user'),
  authorization: () => api<AccountAuthorization>('/api/user/authorization'),
  renameLogin: (body: { login: string; current_password: string }) => api<void>('/api/user', { method: 'PATCH', body }),
  changePassword: (body: { current_password: string; new_password: string }) => api<void>('/api/user/password', { method: 'POST', body }),
  sessions: () => api<{ items: AccountSession[] }>('/api/user/sessions'),
  revokeSession: (id: string) => api<void>(`/api/user/sessions/${encodeURIComponent(id)}`, { method: 'DELETE' }),
  revokeOthers: () => api<{ revoked_count: number }>('/api/user/sessions/revoke-others', { method: 'POST', body: {} }),
  users: () => api<{ items: AccountUser[] }>('/api/users'),
  createUser: (body: { login: string; password: string; roles: string[] }) => api<AccountUser>('/api/users', { method: 'POST', body }),
  setUserEnabled: (id: string, enabled: boolean) => api<AccountUser>(`/api/users/${encodeURIComponent(id)}/${enabled ? 'enable' : 'disable'}`, { method: 'POST', body: {} }),
  setUserRoles: (id: string, roles: string[]) => api<AccountUser>(`/api/users/${encodeURIComponent(id)}/roles`, { method: 'PATCH', body: { roles } }),
  deleteUser: (id: string) => api<void>(`/api/users/${encodeURIComponent(id)}`, { method: 'DELETE' }),
  roles: () => api<{ roles: AccountRole[]; permissions: PermissionID[] }>('/api/roles'),
  createRole: (body: { name: string; permissions: PermissionID[] }) => api<AccountRole>('/api/roles', { method: 'POST', body }),
  updateRole: (id: string, body: { name: string; permissions: PermissionID[] }) => api<AccountRole>(`/api/roles/${encodeURIComponent(id)}`, { method: 'PUT', body }),
  deleteRole: (id: string) => api<void>(`/api/roles/${encodeURIComponent(id)}`, { method: 'DELETE' }),
}
export const runtimeUpdateAPI = {
  status: () => api<RuntimeUpdateStatus>('/api/runtime/update'),
  check: () => api<RuntimeUpdateStatus>('/api/runtime/update/check', { method: 'POST', body: {} }),
  stage: () => api<RuntimeUpdateStatus>('/api/runtime/update/stage', { method: 'POST', body: {} }),
  activate: () => api<RuntimeUpdateStatus>('/api/runtime/update/activate', { method: 'POST', body: {} }),
  rollback: () => api<RuntimeUpdateStatus>('/api/runtime/update/rollback', { method: 'POST', body: {} }),
}
export const runtimeHTTPSAPI = {
  status: () => api<import('@/types/api').RuntimeHTTPSSettings>('/api/runtime/https'),
  enableSelfSigned: (body: { hosts: string[]; redirect_http: boolean }) => api<import('@/types/api').RuntimeHTTPSSettings>('/api/runtime/https', { method: 'PUT', body }),
  enableACME: (body: { hosts: string[]; redirect_http: boolean; acme: true }) => api<import('@/types/api').RuntimeHTTPSSettings>('/api/runtime/https', { method: 'PUT', body }),
  enableCertificate: (body: { hosts: string[]; redirect_http: boolean; certificate_pem: string; private_key_pem: string }) => api<import('@/types/api').RuntimeHTTPSSettings>('/api/runtime/https', { method: 'PUT', body }),
  enableSaved: () => api<import('@/types/api').RuntimeHTTPSSettings>('/api/runtime/https/enable', { method: 'POST' }),
  disable: () => api<import('@/types/api').RuntimeHTTPSSettings>('/api/runtime/https', { method: 'DELETE' }),
}
