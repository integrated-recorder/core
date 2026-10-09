import { api, queryString } from './client'
import type { Adapter, AdapterConfig, AdapterDescriptor, AdapterReconcileResult, ApiSession, ArchiveEntry, AuditEvent, Dashboard, ExportJob, InstallationStatus, IntegrityJob, IntegrityResult, LogEntry, Notification, PluginRegistryStatus, PreviewFramesResponse, RecordingDetail, RecordingEvent, RecordingMetadata, RecordingPage, RecordingSummary, Resource, ResourceRef, RuntimeUpdateStatus, Schema, SearchResult, SetupStorageTest, StorageInfo, StorageMetricsResponse, StoragePoolsResponse, StorageProviderConfig, StorageProviderConfigBody, StorageProviderStatus, StorageSettings, SystemInfo, SystemSettings, WatchEvent, WatchView, WorkflowHistoryEvent, WorkflowProgress, WorkflowSummary } from '@/types/api'

export const authAPI = {
  session: () => api<ApiSession>('/api/auth/session'),
  bootstrap: (token: string, password: string) => api<ApiSession>('/api/auth/bootstrap', { method: 'POST', body: { token, password } }),
  login: (password: string) => api<ApiSession>('/api/auth/login', { method: 'POST', body: { password } }),
  logout: () => api<void>('/api/auth/logout', { method: 'POST' }),
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
  lifecycle: (id: string) => api<import('@/types/api').RecordingLifecycle>(`/api/recordings/${encodeURIComponent(id)}/lifecycle`),
  start: (body: { adapter_id: string; input: Record<string, unknown>; resource?: ResourceRef; title?: string; preview_mode: 'disabled' | 'segment' }) => api<RecordingDetail | WorkflowProgress>('/api/recordings', { method: 'POST', body }),
  stop: (id: string) => api<RecordingDetail>(`/api/recordings/${encodeURIComponent(id)}/stop`, { method: 'POST' }),
  complete: (id: string) => api<RecordingDetail>(`/api/recordings/${encodeURIComponent(id)}/complete`, { method: 'POST' }),
  seal: (id: string) => api<import('@/types/api').RecordingLifecycle>(`/api/recordings/${encodeURIComponent(id)}/seal`, { method: 'POST' }),
  remove: (id: string) => api<void>(`/api/recordings/${encodeURIComponent(id)}`, { method: 'DELETE' }),
  tags: (id: string) => api<{ tags: string[] }>(`/api/recordings/${encodeURIComponent(id)}/tags`),
  setTags: (id: string, tags: string[]) => api<{ tags: string[] }>(`/api/recordings/${encodeURIComponent(id)}/tags`, { method: 'PUT', body: { tags } }),
  archive: (id: string) => api<{ recording_id: string; entries: ArchiveEntry[] }>(`/api/recordings/${encodeURIComponent(id)}/archive/index`),
  events: (id: string) => api<{ items: RecordingEvent[] }>(`/api/recordings/${encodeURIComponent(id)}/events`),
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
  search: (q: string) => api<{ results: SearchResult[] }>(`/api/search${queryString({ q, limit: 20 })}`),
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
export const runtimeUpdateAPI = {
  status: () => api<RuntimeUpdateStatus>('/api/runtime/update'),
  check: () => api<RuntimeUpdateStatus>('/api/runtime/update/check', { method: 'POST', body: {} }),
  stage: () => api<RuntimeUpdateStatus>('/api/runtime/update/stage', { method: 'POST', body: {} }),
  activate: () => api<RuntimeUpdateStatus>('/api/runtime/update/activate', { method: 'POST', body: {} }),
  rollback: () => api<RuntimeUpdateStatus>('/api/runtime/update/rollback', { method: 'POST', body: {} }),
}
