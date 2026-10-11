export type ResourceRef = { resource_type: string; resource_id: string; parent?: ResourceRef }
export type Resource = ResourceRef & { display_name?: string; attributes?: Record<string, unknown> }
export type FieldControl = 'text' | 'secret' | 'number' | 'boolean' | 'select' | 'multi-select' | 'textarea' | 'action' | 'status'
export type Option = { value: unknown; label: string }
export type FieldPersistence = { mode: 'forbidden' | 'optional' | 'required'; target: { scope: 'plugin' | 'current_resource' | 'resource'; resource?: ResourceRef } }
export type SchemaField = {
  key: string; control: FieldControl; label: string; description?: string; required?: boolean; inherit?: boolean;
  default?: unknown; constraints?: { min?: number; max?: number; min_length?: number; max_length?: number; pattern?: string; min_items?: number; max_items?: number };
  options?: Option[]; visible_when?: unknown; persistence?: FieldPersistence
}
export type Schema = { fields: SchemaField[] }
export type AdapterDescriptor = {
  id: string; name: string; version: string; protocol_version: number; capabilities?: string[];
  branding?: { icon_url?: string }; input_schema: Schema; configuration_schema: Schema; resource_types?: { type: string; parent_types?: string[]; configuration_schema?: Schema }[]; media_types: string[]
}
export type AdapterStatus = { id: string; name?: string; version?: string; protocol_version?: number; state: string; error?: string; generation?: number; restart_attempts?: number }
export type PluginTrust = {
  provenance: 'bundled' | 'registry' | 'operator'
  authority: 'core_release' | 'official' | 'custom' | 'local'
  publisher: 'first_party' | 'third_party' | 'unknown'
  reviewed: boolean
}
export type Adapter = { descriptor?: AdapterDescriptor; status: AdapterStatus; trust?: PluginTrust }
export type AdapterReconcileResult = { state: 'unchanged' | 'activated' | 'rejected' | 'failed'; active_adapter_count: number; rejected_count: number; failure_code?: string; generation_id?: string }
export type PluginRegistryStatus = {
  state: 'ready' | 'unavailable' | 'not_configured'; failure_code?: 'plugin_registry_unavailable';
  plugins: { id: string; type: 'source' | 'storage'; name: string; available_version?: string; installed_version?: string; installed: boolean; update_available: boolean; trust?: PluginTrust }[]
}
export type WatchState = 'disabled' | 'offline' | 'checking' | 'starting' | 'recording' | 'backoff' | 'attention_required' | 'suppressed'
export type WatchView = {
  id: string; adapter_id: string; adapter_name?: string; resource?: ResourceRef & { display_name?: string };
  input?: Record<string, unknown> | null; title?: string; enabled: boolean; preview_mode: PreviewMode;
  check_interval_seconds: number; state: WatchState; created_at: string; updated_at: string;
  last_checked_at?: string; next_check_at?: string; current_recording_id?: string;
  current_recording_state?: RecordingState; current_recording_preview?: PreviewSummary;
  input_secret_configured: Record<string, boolean>; last_error_code?: string
}
export type WatchEvent = { id: string; type: string; at: string; state?: WatchState; recording_id?: string; error_code?: string }
export const recordingStates = ['recording', 'stopped', 'completed', 'interrupted'] as const
export type RecordingState = typeof recordingStates[number]
export function isRecordingState(value: unknown): value is RecordingState { return typeof value === 'string' && (recordingStates as readonly string[]).includes(value) }
export type PreviewMode = 'disabled' | 'segment'
export type PreviewState = 'disabled' | 'unavailable' | 'queued' | 'processing' | 'partial' | 'ready' | 'failed'
export type PreviewSummary = {
  mode: PreviewMode; state: PreviewState; available: boolean; frame_count: number;
  image_archive_ordinal?: number; latest_archive_ordinal?: number; updated_at?: string
}
export type PreviewFrame = {
  archive_ordinal: number; track_id: string; source_epoch: number; sequence: number;
  segment_start_seconds: number; segment_duration_seconds: number; frame_time_seconds: number;
  segment_sha256: string; width: number; height: number; size: number; generated_at: string;
  state?: 'ready' | 'failed' | 'unsupported'; error_code?: string
}
export type PreviewFramesResponse = {
  recording_id: string; mode: PreviewMode; state: PreviewState; available: boolean;
  frame_count: number; items: PreviewFrame[]
}
export type Segment = { sequence?: number; source_sequence?: number; source_epoch?: number; archive_ordinal?: number; duration?: number; storage_path?: string; payload_size?: number; sha256?: string; discontinuity?: boolean; init_segment_id?: string; program_date_time?: string }
export type Gap = { track_id?: string; source_epoch?: number; from_sequence?: number; to_sequence?: number; duration_seconds?: number; reason?: string }
export type Track = { id?: string; name?: string; type?: string; segments?: Segment[]; init_segments?: Segment[]; [key: string]: unknown }
export type RecordingSummary = {
  id: string; title?: string; adapter_id?: string; adapter?: { id: string; name?: string; version?: string; protocol_version?: number; fingerprint?: string };
  resource?: ResourceRef & { display_name?: string }; state: RecordingState; created_at: string; started_at: string; stopped_at?: string | null;
  track_count?: number; segment_count?: number; duration_seconds?: number; gap_count?: number; archive_size_bytes?: number;
  media_payload_size_bytes?: number; manifest_size_bytes?: number; init_payload_size_bytes?: number; init_segment_count?: number;
  manifest_snapshot_count?: number; gap_segment_count?: number; gap_duration_seconds?: number | null; integrity?: IntegrityStatus; tags?: string[]; preview?: PreviewSummary
}
export type RecordingListItem = {
  id: string; title?: string; adapter_id: string; adapter_name?: string; state: RecordingState;
  resource_type?: string; resource_id?: string; tags: string[]; created_at: string; started_at: string;
  duration_seconds: number; archive_size_bytes: number | null; media_payload_size_bytes: number;
  manifest_size_bytes: number; init_payload_size_bytes: number; segment_count: number;
  init_segment_count: number; manifest_snapshot_count: number; gap_count: number;
  gap_segment_count: number; gap_duration_seconds: number | null; integrity: IntegrityStatus; preview?: PreviewSummary;
  statistics_status?: 'complete' | 'partial'; unavailable_fields?: string[]
}
export type RecordingDetail = {
  format_version?: number; id: string; title?: string; adapter_id?: string;
  adapter?: { id: string; name?: string; version: string; protocol_version: number; descriptor_fingerprint?: string };
  resource?: ResourceRef & { display_name?: string }; source_uri_classification?: string;
  source_url?: string; state: RecordingState; created_at: string; started_at: string; stopped_at?: string | null;
  tracks?: Record<string, Track>; gaps?: Gap[];
  manifest_snapshots?: { storage_path?: string; size?: number; sha256?: string; captured_at?: string }[];
  last_error?: string; segment_count?: number; duration_seconds?: number; statistics?: RecordingStatistics; integrity?: IntegrityStatus; preview?: PreviewSummary;
  archive_sealed?: boolean; archive_revision?: number; timeline_revision?: number
}
export type RecordingLifecycle = {
  recording_id: string; capture_state: RecordingState; archive_sealed: boolean; repairable: boolean;
  recovery_state: 'active' | 'idle' | 'sealed'; archive_revision: number; timeline_revision: number
}
export type SourceMetadataRevision = { observed_at: string; source_updated_at?: string; title?: string | null; description?: string | null }
export type RecordingMetadata = { current?: SourceMetadataRevision; items: SourceMetadataRevision[]; truncated: boolean }
export type IntegrityStatus = 'unknown' | 'verifying' | 'verified' | 'degraded' | 'failed'
export type RecordingStatistics = {
  duration_seconds?: number; archive_size_bytes?: number | null; media_payload_size_bytes?: number;
  manifest_size_bytes?: number; init_payload_size_bytes?: number; segment_count?: number;
  init_segment_count?: number; manifest_snapshot_count?: number; gap_count?: number;
  gap_segment_count?: number; gap_duration_seconds?: number | null; integrity?: IntegrityStatus;
  status?: 'complete' | 'partial'; unavailable_fields?: string[]
}
export type RecordingPage = { items: RecordingListItem[]; next_cursor?: string; total: number; total_is_partial?: boolean; partial_errors?: string[] }
export type RevisionFreshness = 'unknown' | 'current' | 'stale'
export type JobProgress = {
  current: number
  total?: number
  percent?: number
  indeterminate: boolean
  phase: string
  unit: string
  updated_at?: string
}
export type DerivedJobState = 'queued' | 'running' | 'completed' | 'failed' | 'canceled'
export type IntegrityJobState = DerivedJobState
export type IntegrityResult = { status: IntegrityStatus; last_verified_at?: string; objects_total?: number; objects_verified?: number; objects_missing?: number; objects_corrupt?: number; issues?: { code: string; path?: string }[]; freshness?: RevisionFreshness; revision_known?: boolean; source_archive_revision?: number; source_timeline_revision?: number; current_archive_revision?: number; current_timeline_revision?: number; active_job?: Pick<IntegrityJob, 'id' | 'state' | 'progress' | 'freshness' | 'error_code'> }
export type IntegrityJob = { id: string; recording_id: string; state: IntegrityJobState; created_at: string; started_at?: string; finished_at?: string; error_code?: string; source_revision_known?: boolean; source_archive_revision?: number; source_timeline_revision?: number; current_archive_revision?: number; current_timeline_revision?: number; freshness?: RevisionFreshness; progress?: JobProgress; result?: IntegrityResult }
export type ExportJob = { id: string; recording_id: string; state: DerivedJobState; format?: string; output_name?: string; created_at?: string; started_at?: string; updated_at?: string; finished_at?: string; error_code?: string; source_revision_known?: boolean; source_archive_revision?: number; source_timeline_revision?: number; current_archive_revision?: number; current_timeline_revision?: number; freshness?: RevisionFreshness; progress?: JobProgress }
export type Notification = { id: string; type: string; at: string; read: boolean; object_id?: string }
export type WorkflowProgress = {
  workflow_id: string; adapter_id: string; state: string; resource?: ResourceRef;
  challenge?: { schema: Schema; prompt?: WorkflowPrompt; persistable?: boolean };
  adapter?: { id: string; version: string; protocol_version: number }
}
export type WorkflowPrompt = { type: 'action' | 'prompt' | 'secret_prompt' | 'navigate' | 'display' | 'status' | 'complete' | 'error' | string; title?: string; message?: string; fields?: SchemaField[]; data?: unknown }
export type WorkflowSummary = { workflow_id: string; adapter_id: string; state: string; resource?: ResourceRef; challenge?: { prompt_type?: string; field_count: number; has_secret_fields: boolean }; created_at: string; updated_at: string; expires_at: string; in_progress: boolean }
export type WorkflowHistoryEvent = { id: string; workflow_id: string; adapter_id: string; state: string; at: string; resource?: ResourceRef; challenge?: { title?: string; message?: string; field_count: number; has_secret_fields: boolean } }
export type ConfigView = { values: Record<string, unknown>; secrets: Record<string, { configured: boolean }> }
export type AdapterConfig = {
  schema: Schema; values: Record<string, unknown>; secrets: Record<string, { configured: boolean }>;
  stored: ConfigView; effective: ConfigView; value_sources: Record<string, string>; secret_sources: Record<string, string>; current_scope: string
}
export type Dashboard = {
  active_recordings_count: number; completed_last_24h: number; interrupted_last_24h: number; recordings_total: number;
  segments_total: number; gaps_total: number; archive_bytes: number; archive_bytes_known?: boolean; statistics_status?: 'complete' | 'partial'; unavailable_fields?: string[];
  filesystem_total_bytes: number; filesystem_free_bytes: number; filesystem_used_bytes: number;
  integrity: Record<string, number>; adapters: Record<string, number>; export_available: boolean; recent_recordings: RecordingSummary[]; active_recordings: RecordingSummary[];
  watches?: { total: number; enabled: number; recording: number; offline: number; backoff: number; attention_required: number }
}
export type StorageInfo = { archive_root: string; filesystem_total_bytes: number; filesystem_used_bytes: number; filesystem_available_bytes: number; recordings_bytes: number | null; recordings_bytes_known?: boolean; recording_count: number; segment_count: number; init_segment_count: number; manifest_count: number; statistics_status?: 'complete' | 'partial'; unavailable_fields?: string[] }
export type StoragePool = {
  id: string; display_name: string; kind: string; role: string; health: string;
  capacity_known: boolean;
  capacity: { total_bytes: number; used_bytes: number; available_bytes: number; usage_ratio: number };
  throughput: { read_bytes_per_second: number; write_bytes_per_second: number; read_bytes_total: number; write_bytes_total: number; read_latency_ms: number; write_latency_ms: number };
  estimated_ceiling: { read_bytes_per_second?: number; write_bytes_per_second?: number; source: 'observed' | 'unknown' | 'configured' | 'benchmarked' };
  buffer: { used_bytes: number; capacity_bytes: number; utilization: number };
  queue: { objects: number; bytes: number; oldest_age_seconds: number };
  writers: { active: number; limit: number };
  errors_total: number
}
export type StorageMetricSample = { at: string; read_bytes_per_second: number; write_bytes_per_second: number; buffer_used_bytes: number; persist_queue_bytes: number }
export type StoragePoolsResponse = { items: StoragePool[] }
export type StorageMetricsResponse = { pool_id: string; sample_interval_ms: number; sample_interval_seconds: number; items: StorageMetricSample[] }
export type StorageProviderHealth = 'ready' | 'unknown' | 'failed'
export type StoragePrimaryProvider = { kind: 'plugin'; provider_id: string; instance_id?: string; version: string; state: 'ready' | 'unavailable' }
export type StorageProviderSummary = {
  id: string; name: string; version: string; configured: boolean; active: boolean;
  health: StorageProviderHealth; configuration_schema: Schema;
  distribution: 'bundled' | 'registry'; configuration_managed: boolean; uninstallable: boolean
}
export type StorageProviderStatus = { primary: StoragePrimaryProvider; providers: StorageProviderSummary[] }
export type StorageProviderConfig = { values: Record<string, unknown>; configured_secrets: string[] }
export type StorageProviderConfigBody = { values: Record<string, unknown>; secrets: Record<string, string> }
export type StorageInstanceSummary = { id: string; display_name: string; provider_id: string; provider_name: string; desired_set_id: string; active: boolean; health: 'ready' | 'unknown' | 'unavailable' }
export type StorageInstanceCreateBody = { display_name: string; provider_id: string; values: Record<string, unknown>; secrets: Record<string, string> }
export type SystemInfo = { version: string; commit: string; go_version: string; goos: string; goarch: string; started_at: string; uptime_seconds: number; export_available: boolean }
export type RuntimeBuildIdentity = { version: string; commit: string; build_time: string; release_channel: string; runtime_protocol_version: number }
export type RuntimeGenerationSummary = { id: string; version: string; commit: string; installed_at: string; state: string; active_recordings: number }
export type RuntimeReleaseSummary = { version: string; commit: string; build_time: string; release_channel: string; notes_summary?: string }
export type RuntimeUpdateStatus = {
  host: RuntimeBuildIdentity
  application: RuntimeBuildIdentity
  active_control?: RuntimeGenerationSummary
  default_engine?: RuntimeGenerationSummary
  active_generations: RuntimeGenerationSummary[]
  draining_generations: RuntimeGenerationSummary[]
  staged_release?: RuntimeReleaseSummary
  previous_release?: RuntimeReleaseSummary
  available_release?: RuntimeReleaseSummary
  verification_state: 'unknown' | 'not_checked' | 'checking' | 'verified' | 'failed'
  last_failure_code?: string
  updates_available: boolean
  update_unavailable_reason?: string
  handover_diagnostic?: {
    recording_id: string
    phase: string
    reason_code: string
    source_generation_id: string
    target_generation_id: string
    target_version: string
    occurred_at: string
    reconcile_state: 'pending' | 'succeeded'
    retryable: boolean
    recoverable: boolean
    ownership_retained: boolean
    resolved_at?: string
  }
}
export type StorageSettings = {
  ingest_memory: { global_buffer_bytes: number; per_recording_buffer_bytes: number; max_payload_bytes: number }
  queue_writer: { pending_queue_capacity: number; writer_concurrency: number }
  failure_handling: { persist_attempts: number; retry_initial_backoff_ms: number; retry_max_backoff_ms: number }
  observability: { sampling_interval_ms: number; metrics_retention_ms: number }
}
export type SystemSettings = {
  settings: {
    ui: { theme: 'system' | 'light' | 'dark' }
    integrity: { concurrency: number }
    retention: { enabled: boolean; completed_after_days: number }
    storage: StorageSettings
  }
  effective_storage: StorageSettings
  restart_required: string[]
}
export type SearchResult = { type: 'recording' | 'adapter' | 'resource' | 'workflow'; id?: string; workflow_id?: string; adapter_id?: string; resource_type?: string; resource_id?: string; title?: string; name?: string; display_name?: string; state?: string; resource?: ResourceRef }
export type ArchiveEntry = { kind: string; path: string; size: number; sha256?: string }
export type ArchiveIndexPage = { recording_id: string; entries: ArchiveEntry[]; next_cursor?: string; has_more: boolean }
export type RecordingEvent = { id: string; recording_id: string; type: string; at: string; count?: number; message?: string }
export type LogEntry = { at: string; level: string; component: string; message: string }
export type AuditEvent = { id: string; type: string; at: string; object_id?: string }
export type ApiSession = { auth_enabled: boolean; authenticated: boolean; needs_bootstrap: boolean; csrf_token?: string; expires_at?: string }
export type UserPreferences = { locale: 'system' | 'ko-KR' | 'en-US'; theme: 'system' | 'light' | 'dark'; timezone: string }
export type InstallationState = 'uninitialized' | 'setup_in_progress' | 'ready' | 'recovery_required'
export type InstallationStatus = {
  state: InstallationState
  administrator_configured: boolean
  claim_required: boolean
  recovery_required: boolean
  auth_disabled: boolean
  version: string
  release_channel: string
  diagnostic_code?: string
}
export type SetupStorageTest = {
  status: 'ready' | 'warning' | 'error'
  capacity_known: boolean
  free_bytes?: number
  write_test: 'passed' | 'failed'
  durability_test: 'passed' | 'failed'
  diagnostic_code?: string
}
