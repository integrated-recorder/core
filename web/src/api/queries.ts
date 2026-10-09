import { queryOptions } from '@tanstack/react-query'
import { adaptersAPI, authAPI, dashboardAPI, derivativeAPI, integrityAPI, pluginsAPI, productAPI, recordingsAPI, runtimeUpdateAPI, setupAPI, storageAPI, userPreferencesAPI, watchesAPI, workflowsAPI, type PreviewQuery, type RecordingQuery, type StorageMetricWindow } from './index'

export const qk = {
  session: ['auth', 'session'] as const, installation: ['installation', 'status'] as const, dashboard: ['dashboard'] as const, storage: ['system', 'storage'] as const, info: ['system', 'info'] as const,
  storagePools: ['storage', 'pools'] as const, storageMetrics: (poolId: string, window: StorageMetricWindow) => ['storage', 'pool', poolId, 'metrics', window] as const,
  settings: ['settings'] as const, runtimeUpdate: ['runtime-update'] as const, recordings: (query: RecordingQuery) => ['recordings', query] as const,
  recording: (id: string) => ['recording', id] as const, lifecycle: (id: string) => ['recording', id, 'lifecycle'] as const, tags: (id: string) => ['recording', id, 'tags'] as const,
  previews: (id: string, query: PreviewQuery) => ['recording', id, 'previews', query] as const,
  archive: (id: string) => ['recording', id, 'archive'] as const, events: (id: string) => ['recording', id, 'events'] as const,
  metadata: (id: string) => ['recording', id, 'metadata'] as const,
  integrity: (id: string) => ['recording', id, 'integrity'] as const, exports: (id: string) => ['recording', id, 'exports'] as const,
  adapters: ['adapters'] as const, plugins: ['plugin-registry'] as const, adapter: (id: string) => ['adapter', id] as const, schema: (id: string, resource?: unknown) => ['adapter', id, 'schema', resource] as const,
  config: (id: string, resource?: unknown) => ['adapter', id, 'config', resource] as const, resources: (id: string, query: unknown) => ['adapter', id, 'resources', query] as const,
  workflows: ['workflows'] as const, workflow: (id: string) => ['workflow', id] as const, workflowHistory: ['workflow-history'] as const,
  watches: ['watches'] as const, watch: (id: string) => ['watch', id] as const,
  watchRecordings: (id: string) => ['watch', id, 'recordings'] as const, watchEvents: (id: string) => ['watch', id, 'events'] as const,
  notifications: ['notifications'] as const, audit: ['audit'] as const, logs: (query: unknown) => ['logs', query] as const,
  userPreferences: ['user', 'preferences'] as const,
}
export const sessionQuery = queryOptions({ queryKey: qk.session, queryFn: authAPI.session, retry: false, staleTime: 10_000 })
export const userPreferencesQuery = queryOptions({ queryKey: qk.userPreferences, queryFn: () => userPreferencesAPI.get(), staleTime: 60_000, retry: false })
export const installationQuery = queryOptions({
  queryKey: qk.installation, queryFn: setupAPI.status, retry: false, staleTime: 0,
  refetchInterval: query => query.state.data?.state === 'ready' ? false : 5_000,
  refetchIntervalInBackground: false,
})
export const runtimeUpdateQuery = queryOptions({ queryKey: qk.runtimeUpdate, queryFn: () => runtimeUpdateAPI.status(), staleTime: 5_000, refetchInterval: 15_000, refetchIntervalInBackground: false })
export const adaptersQuery = queryOptions({ queryKey: qk.adapters, queryFn: adaptersAPI.list, staleTime: 15_000, refetchInterval: 20_000, refetchIntervalInBackground: false })
export const pluginsQuery = queryOptions({ queryKey: qk.plugins, queryFn: pluginsAPI.status, staleTime: 15_000 })
export const dashboardQuery = queryOptions({ queryKey: qk.dashboard, queryFn: dashboardAPI.get, staleTime: 10_000, refetchInterval: 15_000, refetchIntervalInBackground: false })
export const storagePoolsQuery = queryOptions({ queryKey: qk.storagePools, queryFn: storageAPI.pools, staleTime: 2_000, refetchInterval: 5_000, refetchIntervalInBackground: false })
export const storageMetricsQuery = (poolId: string, window: StorageMetricWindow, enabled = true) => queryOptions({
  queryKey: qk.storageMetrics(poolId, window), queryFn: () => storageAPI.metrics(poolId, window), enabled: enabled && Boolean(poolId),
  staleTime: 2_000, refetchInterval: 5_000, refetchIntervalInBackground: false,
})
export const notificationsQuery = queryOptions({ queryKey: qk.notifications, queryFn: productAPI.notifications, staleTime: 15_000, refetchInterval: 30_000, refetchIntervalInBackground: false })
export const workflowsQuery = queryOptions({ queryKey: qk.workflows, queryFn: workflowsAPI.list, staleTime: 5_000, refetchInterval: query => query.state.data?.some(workflow => workflow.in_progress) ? 8_000 : false, refetchIntervalInBackground: false })
const watchIsActive = (state: string) => state === 'checking' || state === 'starting' || state === 'recording' || state === 'backoff'
export function watchListPollInterval(items: readonly { state: string }[] | undefined): number {
  return items?.some(watch => watchIsActive(watch.state)) ? 4_000 : 15_000
}
export function watchDetailPollInterval(watch: { state: string; enabled: boolean } | undefined): number | false {
  if (!watch) return false
  if (watchIsActive(watch.state)) return 4_000
  return watch.enabled ? 10_000 : false
}
export const watchesQuery = queryOptions({
  queryKey: qk.watches, queryFn: watchesAPI.list, staleTime: 2_000,
  refetchInterval: query => watchListPollInterval(query.state.data?.items),
  refetchIntervalInBackground: false,
})
export const watchQuery = (id: string) => queryOptions({
  queryKey: qk.watch(id), queryFn: () => watchesAPI.get(id), staleTime: 2_000,
  refetchInterval: query => watchDetailPollInterval(query.state.data),
  refetchIntervalInBackground: false,
})
export const watchRecordingsQuery = (id: string) => queryOptions({ queryKey: qk.watchRecordings(id), queryFn: () => watchesAPI.recordings(id), staleTime: 10_000 })
export const watchEventsQuery = (id: string) => queryOptions({ queryKey: qk.watchEvents(id), queryFn: () => watchesAPI.events(id), staleTime: 10_000 })
export const recordingQuery = (id: string) => queryOptions({ queryKey: qk.recording(id), queryFn: () => recordingsAPI.get(id) })
export const recordingLifecycleQuery = (id: string) => queryOptions({ queryKey: qk.lifecycle(id), queryFn: () => recordingsAPI.lifecycle(id), staleTime: 0 })
export const previewsQuery = (id: string, query: PreviewQuery, active = false) => queryOptions({
  queryKey: qk.previews(id, query), queryFn: () => recordingsAPI.previews(id, query), staleTime: 0,
  refetchInterval: queryState => {
    if (active) return 3000
    const state = queryState.state.data?.state
    if (state === 'queued' || state === 'processing' || state === 'partial') return 2000
    return false
  }, refetchIntervalInBackground: false,
})
export const integrityQuery = (id: string) => queryOptions({ queryKey: qk.integrity(id), queryFn: () => integrityAPI.get(id), staleTime: 2000 })
export const exportsQuery = (id: string) => queryOptions({ queryKey: qk.exports(id), queryFn: () => derivativeAPI.exports(id), staleTime: 2000 })
