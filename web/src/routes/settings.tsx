import { useEffect, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Activity, Archive, Check, CircleAlert, Download, HardDrive, Info, Logs, ShieldCheck, SlidersHorizontal } from 'lucide-react'
import { dashboardAPI, productAPI, userPreferencesAPI } from '@/api'
import { qk, userPreferencesQuery } from '@/api/queries'
import { formatBytes, formatDate, formatDuration } from '@/lib/utils'
import { PageHeading } from '@/components/page-heading'
import { errorMessage } from '@/lib/errors'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Confirm } from '@/components/ui/confirm'
import { Input } from '@/components/ui/input'
import { Select, SelectItem } from '@/components/ui/select'
import { ErrorState, LoadingState, EmptyState } from '@/components/query-state'
import { useToast } from '@/components/ui/use-toast'
import { auditEventLabel } from '@/lib/labels'
import type { StorageSettings, SystemSettings, UserPreferences } from '@/types/api'
import { RuntimeUpdates } from '@/components/settings/runtime-updates'
import { useI18n } from '@/i18n/provider'
import { formatNumber, validTimeZone } from '@/lib/formatting'
import { resolveLocale, translate } from '@/i18n/catalog'

type Section = 'preferences' | 'storage' | 'updates' | 'logs' | 'audit'
export function SettingsPage() {
  const [section, setSection] = useState<Section>('preferences')
  const [logLevel, setLogLevel] = useState(''); const [logComponent, setLogComponent] = useState(''); const [logSearch, setLogSearch] = useState(''); const [appliedSearch, setAppliedSearch] = useState('')
  const client = useQueryClient(); const { toast } = useToast(); const { t } = useI18n()
  const settings = useQuery({ queryKey: qk.settings, queryFn: productAPI.settings })
  const storage = useQuery({ queryKey: qk.storage, queryFn: dashboardAPI.storage, enabled: section === 'storage' || section === 'preferences', staleTime: 20_000 })
  const system = useQuery({ queryKey: qk.info, queryFn: dashboardAPI.info, enabled: section === 'storage' || section === 'preferences', staleTime: 60_000 })
  const candidates = useQuery({ queryKey: ['retention', 'candidates'], queryFn: productAPI.retentionCandidates, enabled: section === 'storage', staleTime: 0 })
  const logs = useQuery({ queryKey: qk.logs({ logLevel, logComponent, appliedSearch }), queryFn: () => productAPI.logs({ level: logLevel || undefined, component: logComponent || undefined, q: appliedSearch || undefined, limit: 100 }), enabled: section === 'logs', staleTime: 5000 })
  const audit = useQuery({ queryKey: qk.audit, queryFn: productAPI.audit, enabled: section === 'audit', staleTime: 10_000 })
  const save = useMutation({ mutationFn: productAPI.saveSettings, onSuccess: result => { void client.setQueryData(qk.settings, result); toast(t('settings.systemSaved')) }, onError: error => toast(t('settings.systemSaveFailed'), errorMessage(error), 'error') })
  const saveStorage = useMutation({ mutationFn: (storage: StorageSettings) => productAPI.saveSettings({ storage }), onSuccess: result => { void client.setQueryData(qk.settings, result); toast(t('settings.saved')) }, onError: error => toast(t('settings.saveStorageFailed'), errorMessage(error), 'error') })
  const retentionRun = useMutation({ mutationFn: productAPI.runRetention, onSuccess: result => { toast(t('settings.retentionRunSuccess', { count: result.deleted_count })); void client.invalidateQueries({ queryKey: ['retention', 'candidates'] }); void client.invalidateQueries({ queryKey: ['recordings'] }); void client.invalidateQueries({ queryKey: qk.dashboard }); void client.invalidateQueries({ queryKey: qk.storage }); void client.invalidateQueries({ queryKey: qk.notifications }); void client.invalidateQueries({ queryKey: qk.audit }) }, onError: error => toast(t('settings.retentionRunFailed'), errorMessage(error), 'error') })
  const [concurrency, setConcurrency] = useState(1); const [retentionEnabled, setRetentionEnabled] = useState(false); const [retentionDays, setRetentionDays] = useState(365)
  useEffect(() => { if (!settings.data) return; setConcurrency(settings.data.settings.integrity.concurrency); setRetentionEnabled(settings.data.settings.retention.enabled); setRetentionDays(settings.data.settings.retention.completed_after_days) }, [settings.data])
  const tabs: { id: Section; label: string; icon: typeof SlidersHorizontal }[] = [{ id: 'preferences', label: t('settings.preferences'), icon: SlidersHorizontal }, { id: 'storage', label: t('settings.tab.storage'), icon: HardDrive }, { id: 'updates', label: t('settings.tab.updates'), icon: Download }, { id: 'logs', label: t('settings.tab.logs'), icon: Logs }, { id: 'audit', label: t('settings.tab.audit'), icon: ShieldCheck }]
  const submit = (event: React.FormEvent) => { event.preventDefault(); save.mutate({ integrity: { concurrency }, retention: { enabled: retentionEnabled, completed_after_days: retentionDays } }) }
  return <div className="page-enter"><PageHeading eyebrow={t('settings.systemEyebrow')} title={t('settings.systemTitle')} description={t('settings.systemDescription')} />
    <div className="mb-5 flex gap-1 overflow-x-auto border-b border-border" role="tablist" aria-label={t('settings.tablist')}>{tabs.map(item => <button key={item.id} id={'settings-tab-' + item.id} aria-controls={'settings-panel-' + item.id} role="tab" aria-selected={section === item.id} tabIndex={section === item.id ? 0 : -1} onClick={() => setSection(item.id)} className={`focus-ring inline-flex min-w-max items-center gap-2 border-b-2 px-3 py-2.5 text-sm ${section === item.id ? 'border-primary font-semibold text-primary' : 'border-transparent text-muted-foreground hover:text-foreground'}`}><item.icon className="h-4 w-4" />{item.label}</button>)}</div>
    {section === 'preferences' && <div role="tabpanel" id="settings-panel-preferences" aria-labelledby="settings-tab-preferences" tabIndex={0} className="grid gap-4 xl:grid-cols-[1fr_.75fr]"><div className="space-y-4"><UserPreferencesPanel /><Card><CardHeader><CardTitle className="flex items-center gap-2"><SlidersHorizontal className="h-4 w-4 text-primary" />{t('settings.systemSettings')}</CardTitle><p className="text-xs text-muted-foreground">{t('settings.systemHelp')}</p></CardHeader><CardContent>{settings.isLoading ? <LoadingState /> : settings.error ? <ErrorState message={errorMessage(settings.error)} retry={() => void settings.refetch()} /> : <form className="space-y-6" onSubmit={submit}><div className="space-y-2"><label className="text-sm font-medium">{t('settings.integrityConcurrency')}</label><Select value={String(concurrency)} onValueChange={value => setConcurrency(Number(value))}>{[1, 2, 3, 4].map(value => <SelectItem key={value} value={String(value)}>{value} {t('settings.items', { count: value })}</SelectItem>)}</Select><p className="text-xs text-muted-foreground">{t('settings.integrityConcurrencyHelp')}</p>{settings.data?.restart_required.includes('integrity.concurrency') && <p className="flex items-center gap-1 text-xs text-amber-700"><CircleAlert className="h-3.5 w-3.5" />{t('settings.restartRequired')}</p>}</div><div className="space-y-3 rounded-lg border border-border p-4"><div><p className="text-sm font-semibold">{t('settings.retentionTitle')}</p><p className="mt-1 text-xs leading-5 text-muted-foreground">{t('settings.retentionHelp')}</p></div><label className="flex items-center gap-2 text-sm"><input type="checkbox" checked={retentionEnabled} onChange={event => setRetentionEnabled(event.target.checked)} className="h-4 w-4 accent-primary" />{t('settings.retentionEnable')}</label><label className="block space-y-1.5 text-xs text-muted-foreground">{t('settings.retentionDays')}<Input type="number" min={1} max={3650} value={retentionDays} onChange={event => setRetentionDays(Number(event.target.value))} /></label></div><Button type="submit" disabled={save.isPending}><Check className="h-4 w-4" />{save.isPending ? t('settings.saving') : t('settings.systemSave')}</Button></form>}</CardContent></Card></div><div className="space-y-4"><Card><CardHeader><CardTitle>{t('settings.environment')}</CardTitle></CardHeader><CardContent>{system.isLoading ? <LoadingState /> : system.error ? <ErrorState message={errorMessage(system.error)} retry={() => void system.refetch()} /> : <dl className="grid grid-cols-2 gap-4">{[[t('settings.version'), system.data?.version], [t('settings.commit'), system.data?.commit], [t('settings.goVersion'), system.data?.go_version], [t('settings.architecture'), `${system.data?.goos} / ${system.data?.goarch}`], [t('settings.started'), formatDate(system.data?.started_at)], [t('settings.uptime'), formatDuration(system.data?.uptime_seconds)]].map(([label, value]) => <div key={label}><dt className="text-[11px] text-muted-foreground">{label}</dt><dd className="mt-1 break-all text-xs font-medium">{value || '—'}</dd></div>)}</dl>}</CardContent></Card><Card><CardHeader><CardTitle>{t('settings.features')}</CardTitle></CardHeader><CardContent className="space-y-3 text-xs"><div className="flex items-center justify-between"><span>{t('settings.remuxExport')}</span><Badge tone={system.data?.export_available ? 'green' : 'neutral'}>{system.data?.export_available ? t('settings.available') : t('settings.unavailable')}</Badge></div><p className="flex gap-2 leading-5 text-muted-foreground"><Info className="h-4 w-4 shrink-0 text-primary" />{t('settings.deploymentAdvice')}</p></CardContent></Card></div></div>}
    {section === 'storage' && <div role="tabpanel" id="settings-panel-storage" aria-labelledby="settings-tab-storage" tabIndex={0} className="space-y-4"><StorageSettingsForm data={settings.data} loading={settings.isLoading} error={settings.error} retry={() => void settings.refetch()} saving={saveStorage.isPending} saveError={saveStorage.error} saveResult={saveStorage.data} onSave={value => saveStorage.mutate(value)} /><div className="grid gap-4 xl:grid-cols-[1fr_.8fr]"><Card><CardHeader><CardTitle>{t('settings.archiveStorage')}</CardTitle><p className="text-xs text-muted-foreground">{t('settings.serverStorageDescription')}</p></CardHeader><CardContent>{storage.isLoading ? <LoadingState /> : storage.error ? <ErrorState message={errorMessage(storage.error)} retry={() => void storage.refetch()} /> : <div className="grid grid-cols-2 gap-5 sm:grid-cols-3">{[[t('settings.archiveBytes'), storage.data?.recordings_bytes_known === false ? '—' : formatBytes(storage.data?.recordings_bytes)], [t('settings.filesystemUsed'), formatBytes(storage.data?.filesystem_used_bytes)], [t('settings.availableSpace'), formatBytes(storage.data?.filesystem_available_bytes)], [t('settings.filesystemTotal'), formatBytes(storage.data?.filesystem_total_bytes)], [t('settings.recordingCount'), storage.data?.recording_count], [t('settings.segmentCount'), storage.data?.segment_count], [t('settings.initCount'), storage.data?.init_segment_count], [t('settings.manifestCount'), storage.data?.manifest_count]].map(([label, value]) => <div key={label}><p className="text-xs text-muted-foreground">{label}</p><p className="mt-1 text-lg font-semibold tabular-nums">{value ?? '—'}</p></div>)}</div>}</CardContent></Card><Card><CardHeader><CardTitle>{t('settings.retentionCandidates')}</CardTitle><p className="text-xs text-muted-foreground">{t('settings.retentionCandidatesHelp')}</p></CardHeader><CardContent>{candidates.isLoading ? <LoadingState /> : candidates.error ? <ErrorState message={errorMessage(candidates.error)} retry={() => void candidates.refetch()} /> : <><div className="flex items-baseline justify-between"><span className="text-3xl font-semibold">{candidates.data?.candidate_count ?? 0}</span><Badge tone={candidates.data?.enabled ? 'amber' : 'neutral'}>{candidates.data?.enabled ? t('settings.retentionEnabled') : t('settings.retentionDisabled')}</Badge></div>{candidates.data?.candidates.length ? <div className="mt-4 max-h-64 divide-y divide-border overflow-auto">{candidates.data.candidates.map(item => <div key={item.id} className="flex justify-between gap-3 py-2 text-xs"><span className="truncate font-mono">{item.id}</span><time className="shrink-0 text-muted-foreground">{formatDate(item.stopped_at)}</time></div>)}</div> : <p className="mt-3 text-xs text-muted-foreground">{t('settings.noRetentionCandidates')}</p>}<Confirm trigger={<Button variant="destructive" className="mt-4" disabled={!candidates.data?.enabled || !(candidates.data?.candidate_count ?? 0)}><Archive className="h-4 w-4" />{t('settings.runRetentionNow')}</Button>} title={t('settings.confirmRetentionTitle')} description={t('settings.confirmRetentionBody', { days: settings.data?.settings.retention.completed_after_days ?? 365, count: candidates.data?.candidate_count ?? 0 })} confirmLabel={t('settings.confirmRetention')} destructive onConfirm={() => retentionRun.mutate()} disabled={retentionRun.isPending} /></>}</CardContent></Card></div></div>}
    {section === 'updates' && <RuntimeUpdates />}
    {section === 'logs' && <Card role="tabpanel" id="settings-panel-logs" aria-labelledby="settings-tab-logs" tabIndex={0}><CardHeader><CardTitle>{t('settings.logsTitle')}</CardTitle><p className="text-xs text-muted-foreground">{t('settings.logsHelp')}</p></CardHeader><CardContent><form className="mb-4 grid gap-2 sm:grid-cols-[150px_180px_1fr_auto]" onSubmit={event => { event.preventDefault(); setAppliedSearch(logSearch) }}><Select value={logLevel || '__all'} onValueChange={value => setLogLevel(value === '__all' ? '' : value)}><SelectItem value="__all">{t('settings.allLevels')}</SelectItem>{['debug', 'info', 'warn', 'error'].map(value => <SelectItem key={value} value={value}>{value}</SelectItem>)}</Select><Input value={logComponent} onChange={event => setLogComponent(event.target.value)} placeholder={t('settings.component')} /><Input value={logSearch} onChange={event => setLogSearch(event.target.value)} placeholder={t('settings.messageSearch')} /><Button variant="outline">{t('settings.search')}</Button></form>{logs.isLoading ? <LoadingState /> : logs.error ? <ErrorState message={errorMessage(logs.error)} retry={() => void logs.refetch()} /> : logs.data?.items.length ? <div className="max-h-[600px] overflow-auto rounded-md border border-border"><table className="w-full min-w-[680px] text-left text-xs"><thead className="sticky top-0 bg-muted"><tr>{[t('settings.time'), t('settings.level'), t('settings.component'), t('settings.message')].map(value => <th key={value} className="px-3 py-2 font-semibold">{value}</th>)}</tr></thead><tbody>{logs.data.items.map((item, index) => <tr key={`${item.at}-${index}`} className="border-t border-border"><td className="whitespace-nowrap px-3 py-2 text-muted-foreground">{formatDate(item.at)}</td><td className="px-3 py-2"><Badge tone={item.level === 'error' ? 'red' : item.level === 'warn' ? 'amber' : 'neutral'}>{item.level}</Badge></td><td className="px-3 py-2">{item.component}</td><td className="max-w-2xl break-words px-3 py-2 font-mono">{item.message}</td></tr>)}</tbody></table></div> : <EmptyState title={t('settings.noLogs')} />}</CardContent></Card>}
    {section === 'audit' && <Card role="tabpanel" id="settings-panel-audit" aria-labelledby="settings-tab-audit" tabIndex={0}><CardHeader><CardTitle>{t('settings.auditTitle')}</CardTitle><p className="text-xs text-muted-foreground">{t('settings.auditHelp')}</p></CardHeader><CardContent>{audit.isLoading ? <LoadingState /> : audit.error ? <ErrorState message={errorMessage(audit.error)} retry={() => void audit.refetch()} /> : audit.data?.items.length ? <div className="divide-y divide-border">{audit.data.items.map(item => <div key={item.id} className="flex flex-wrap items-center gap-3 py-3"><span className="grid h-8 w-8 place-items-center rounded-md bg-muted"><Activity className="h-4 w-4 text-muted-foreground" /></span><span className="min-w-0 flex-1"><span className="block text-sm font-medium">{auditEventLabel(item.type)}</span><span className="block truncate text-xs text-muted-foreground">{item.object_id ?? t('settings.systemActor')}</span></span><time className="text-xs text-muted-foreground">{formatDate(item.at)}</time></div>)}</div> : <EmptyState title={t('settings.auditEmpty')} />}</CardContent></Card>}
  </div>
}

function UserPreferencesPanel() {
  const client = useQueryClient()
  const { toast } = useToast()
  const { preferences: current, setPreferences, t } = useI18n()
  const query = useQuery(userPreferencesQuery)
  const [form, setForm] = useState<UserPreferences>(current)
  const [invalidTimezone, setInvalidTimezone] = useState(false)
  useEffect(() => { if (query.data) setForm(query.data) }, [query.data])
  const save = useMutation({
    mutationFn: userPreferencesAPI.update,
    onSuccess: result => { client.setQueryData(qk.userPreferences, result); setPreferences(result); setForm(result); setInvalidTimezone(false); toast(translate(resolveLocale(result.locale), 'settings.saved')) },
    onError: error => toast(t('settings.saveFailed'), errorMessage(error), 'error'),
  })
  const submit = (event: React.FormEvent) => {
    event.preventDefault()
    const timezone = form.timezone.trim() || 'system'
    if (!validTimeZone(timezone)) { setInvalidTimezone(true); return }
    setInvalidTimezone(false)
    save.mutate({ ...form, timezone })
  }
  return <Card><CardHeader><CardTitle>{t('settings.title')}</CardTitle><p className="text-xs text-muted-foreground">{t('settings.description')}</p></CardHeader><CardContent>
    {query.isLoading ? <LoadingState /> : query.error ? <ErrorState message={errorMessage(query.error)} retry={() => void query.refetch()} /> : <form className="space-y-4" onSubmit={submit}>
      <div className="space-y-1.5"><label htmlFor="user-locale" className="text-sm font-medium">{t('settings.locale')}</label><Select id="user-locale" aria-label={t('settings.locale')} value={form.locale} onValueChange={value => setForm(current => ({ ...current, locale: value as UserPreferences['locale'] }))}><SelectItem value="system">{t('settings.locale.system')}</SelectItem><SelectItem value="ko-KR">{t('settings.locale.ko')}</SelectItem><SelectItem value="en-US">{t('settings.locale.en')}</SelectItem></Select></div>
      <div className="space-y-1.5"><label htmlFor="user-theme" className="text-sm font-medium">{t('settings.theme')}</label><Select id="user-theme" aria-label={t('settings.theme')} value={form.theme} onValueChange={value => setForm(current => ({ ...current, theme: value as UserPreferences['theme'] }))}><SelectItem value="system">{t('settings.theme.system')}</SelectItem><SelectItem value="light">{t('settings.theme.light')}</SelectItem><SelectItem value="dark">{t('settings.theme.dark')}</SelectItem></Select></div>
      <div className="space-y-1.5"><label htmlFor="user-timezone" className="text-sm font-medium">{t('settings.timezone')}</label><Input id="user-timezone" aria-describedby="user-timezone-help" aria-invalid={invalidTimezone} value={form.timezone} placeholder={t('settings.timezone.system')} onChange={event => setForm(current => ({ ...current, timezone: event.target.value }))} /><p id="user-timezone-help" className="text-xs text-muted-foreground">{t('settings.timezone.help')}</p>{invalidTimezone && <p role="alert" className="text-xs text-destructive">{t('errors.preferencesInvalid')}</p>}</div>
      <Button type="submit" disabled={save.isPending}><Check className="h-4 w-4" />{save.isPending ? t('settings.saving') : t('settings.save')}</Button>
    </form>}
  </CardContent></Card>
}

type StorageForm = {
  globalBufferMiB: string; perRecordingBufferMiB: string; maxPayloadMiB: string
  pendingQueueCapacity: string; persistAttempts: string; retryInitialBackoffMs: string; retryMaxBackoffMs: string
  samplingIntervalSeconds: string; metricsRetentionHours: string
}
const MIB = 1024 * 1024
const MIN_CEILING_OBSERVATION_MS = 5 * 60 * 1000
const MIN_DIRECTION_SAMPLES = 20
function minimumMetricsRetentionHours(samplingIntervalSeconds: string) {
  const samplingIntervalMs = Number(samplingIntervalSeconds || 1) * 1000
  const observationIntervals = Math.ceil(MIN_CEILING_OBSERVATION_MS / samplingIntervalMs)
  const retentionMs = Math.max(observationIntervals * samplingIntervalMs, samplingIntervalMs * MIN_DIRECTION_SAMPLES)
  return retentionMs / 3_600_000
}
function storageFormFromSettings(settings: StorageSettings): StorageForm {
  return {
    globalBufferMiB: String(Math.round(settings.ingest_memory.global_buffer_bytes / MIB)),
    perRecordingBufferMiB: String(Math.round(settings.ingest_memory.per_recording_buffer_bytes / MIB)),
    maxPayloadMiB: String(Math.round(settings.ingest_memory.max_payload_bytes / MIB)),
    pendingQueueCapacity: String(settings.queue_writer.pending_queue_capacity),
    persistAttempts: String(settings.failure_handling.persist_attempts),
    retryInitialBackoffMs: String(settings.failure_handling.retry_initial_backoff_ms),
    retryMaxBackoffMs: String(settings.failure_handling.retry_max_backoff_ms),
    samplingIntervalSeconds: String(settings.observability.sampling_interval_ms / 1000),
    metricsRetentionHours: String(settings.observability.metrics_retention_ms / 3_600_000),
  }
}
function storageSettingsFromForm(form: StorageForm): StorageSettings {
  const number = (value: string) => Number(value)
  return {
    ingest_memory: {
      global_buffer_bytes: Math.round(number(form.globalBufferMiB) * MIB),
      per_recording_buffer_bytes: Math.round(number(form.perRecordingBufferMiB) * MIB),
      max_payload_bytes: Math.round(number(form.maxPayloadMiB) * MIB),
    },
    queue_writer: { pending_queue_capacity: number(form.pendingQueueCapacity), writer_concurrency: 1 },
    failure_handling: {
      persist_attempts: number(form.persistAttempts),
      retry_initial_backoff_ms: number(form.retryInitialBackoffMs),
      retry_max_backoff_ms: number(form.retryMaxBackoffMs),
    },
    observability: {
      sampling_interval_ms: Math.round(number(form.samplingIntervalSeconds) * 1000),
      metrics_retention_ms: Math.round(number(form.metricsRetentionHours) * 3_600_000),
    },
  }
}
function formatMiB(bytes: number) { return `${formatNumber(bytes / MIB, { maximumFractionDigits: 1 })} MiB` }
function StorageNumberField({ id, label, value, unit, min, max, step = 1, description, onChange }: {
  id: string; label: string; value: string; unit: string; min: number; max: number; step?: number | 'any'; description?: string; onChange: (value: string) => void
}) {
  const descriptionId = `${id}-description`
  return <div className="space-y-1.5"><label htmlFor={id} className="block text-xs font-medium">{label}</label><div className="flex items-center gap-2"><Input id={id} type="number" inputMode="decimal" required min={min} max={max} step={step} value={value} aria-describedby={description ? descriptionId : undefined} onChange={event => onChange(event.target.value)} /><span className="shrink-0 text-xs text-muted-foreground">{unit}</span></div>{description && <p id={descriptionId} className="text-[11px] leading-4 text-muted-foreground">{description}</p>}</div>
}
function StorageSettingsForm({ data, loading, error, retry, saving, saveError, saveResult, onSave }: {
  data?: SystemSettings; loading: boolean; error: unknown; retry: () => void; saving: boolean; saveError: unknown; saveResult?: SystemSettings; onSave: (settings: StorageSettings) => void
}) {
  const { t } = useI18n()
  const [form, setForm] = useState<StorageForm | null>(null)
  useEffect(() => { if (data?.settings.storage) setForm(storageFormFromSettings(data.settings.storage)) }, [data?.settings.storage])
  if (loading) return <Card><CardContent className="pt-6"><LoadingState /></CardContent></Card>
  if (error) return <Card><CardContent className="pt-6"><ErrorState message={errorMessage(error)} retry={retry} /></CardContent></Card>
  if (!data || !form) return null
  const update = (key: keyof StorageForm) => (value: string) => setForm(current => current ? { ...current, [key]: value } : current)
  const pendingStorageKeys = data.restart_required.filter(key => key.startsWith('storage.'))
  const submit = (event: React.FormEvent) => { event.preventDefault(); onSave(storageSettingsFromForm(form)) }
  const effective = data.effective_storage
  return <Card><CardHeader><CardTitle className="flex items-center gap-2"><HardDrive className="h-4 w-4 text-primary" />{t('settings.advancedStorageTitle')}</CardTitle><p className="text-xs leading-5 text-muted-foreground">{t('settings.advancedStorageHelp')}</p></CardHeader><CardContent className="space-y-5">
    {pendingStorageKeys.length > 0 && <div role="status" className="flex gap-2 rounded-md border border-amber-500/40 bg-amber-50 p-3 text-xs leading-5 text-amber-950 dark:bg-amber-950/20 dark:text-amber-100"><CircleAlert className="mt-0.5 h-4 w-4 shrink-0" /><span>{t('settings.storageRestartWarning')}</span></div>}
    <div className="rounded-md bg-muted/60 p-3" data-testid="effective-storage-settings"><p className="text-xs font-semibold">{t('settings.effectiveNow')}</p><dl className="mt-2 grid grid-cols-2 gap-x-5 gap-y-2 text-xs sm:grid-cols-4"><div><dt className="text-muted-foreground">{t('settings.globalMemory')}</dt><dd className="mt-0.5 font-medium tabular-nums">{formatMiB(effective.ingest_memory.global_buffer_bytes)}</dd></div><div><dt className="text-muted-foreground">{t('settings.recordingMemory')}</dt><dd className="mt-0.5 font-medium tabular-nums">{formatMiB(effective.ingest_memory.per_recording_buffer_bytes)}</dd></div><div><dt className="text-muted-foreground">{t('settings.storageConcurrency')}</dt><dd className="mt-0.5 font-medium tabular-nums">{effective.queue_writer.writer_concurrency} {t('settings.items', { count: effective.queue_writer.writer_concurrency })}</dd></div><div><dt className="text-muted-foreground">{t('settings.samplingPeriod')}</dt><dd className="mt-0.5 font-medium tabular-nums">{formatDuration(effective.observability.sampling_interval_ms / 1000)}</dd></div></dl></div>
    <form className="space-y-5" onSubmit={submit}>
      <div className="grid gap-4 lg:grid-cols-2"><fieldset className="space-y-3 rounded-lg border border-border p-4"><legend className="px-1 text-sm font-semibold">{t('settings.memoryBuffer')}</legend><div className="grid gap-3 sm:grid-cols-2"><StorageNumberField id="storage-global-buffer" label={t('settings.globalBufferLimit')} value={form.globalBufferMiB} unit="MiB" min={64} max={2048} description={t('settings.globalBufferHelp')} onChange={update('globalBufferMiB')} /><StorageNumberField id="storage-recording-buffer" label={t('settings.recordingBufferLimit')} value={form.perRecordingBufferMiB} unit="MiB" min={1} max={1536} onChange={update('perRecordingBufferMiB')} /><StorageNumberField id="storage-max-payload" label={t('settings.maxPayload')} value={form.maxPayloadMiB} unit="MiB" min={1} max={1024} onChange={update('maxPayloadMiB')} /></div><p className="text-[11px] leading-4 text-muted-foreground">{t('settings.bufferNotArchive')}</p></fieldset>
        <fieldset className="space-y-3 rounded-lg border border-border p-4"><legend className="px-1 text-sm font-semibold">{t('settings.queueWriter')}</legend><div className="grid gap-3 sm:grid-cols-2"><StorageNumberField id="storage-queue-capacity" label={t('settings.maxQueue')} value={form.pendingQueueCapacity} unit={t('settings.items', { count: Number(form.pendingQueueCapacity) })} min={1} max={128} onChange={update('pendingQueueCapacity')} /><div className="space-y-1.5"><label htmlFor="storage-writer-concurrency" className="block text-xs font-medium">{t('settings.writerConcurrency')}</label><div className="flex items-center gap-2"><Input id="storage-writer-concurrency" type="number" value={1} disabled readOnly aria-describedby="storage-writer-guidance" /><span className="shrink-0 text-xs text-muted-foreground">{t('settings.items', { count: 1 })}</span></div><p id="storage-writer-guidance" className="text-[11px] leading-4 text-muted-foreground">{t('settings.writerGuidance')}</p></div></div></fieldset>
        <fieldset className="space-y-3 rounded-lg border border-border p-4"><legend className="px-1 text-sm font-semibold">{t('settings.failureHandling')}</legend><div className="grid gap-3 sm:grid-cols-3"><StorageNumberField id="storage-persist-attempts" label={t('settings.totalAttempts')} value={form.persistAttempts} unit="×" min={1} max={10} description={t('settings.attemptsHelp')} onChange={update('persistAttempts')} /><StorageNumberField id="storage-retry-initial" label={t('settings.initialBackoff')} value={form.retryInitialBackoffMs} unit="ms" min={10} max={30000} onChange={update('retryInitialBackoffMs')} /><StorageNumberField id="storage-retry-max" label={t('settings.maxBackoff')} value={form.retryMaxBackoffMs} unit="ms" min={10} max={300000} onChange={update('retryMaxBackoffMs')} /></div></fieldset>
        <fieldset className="space-y-3 rounded-lg border border-border p-4"><legend className="px-1 text-sm font-semibold">{t('settings.observability')}</legend><div className="grid gap-3 sm:grid-cols-2"><StorageNumberField id="storage-sampling-interval" label={t('settings.measurementInterval')} value={form.samplingIntervalSeconds} unit="s" min={1} max={3600} onChange={update('samplingIntervalSeconds')} /><StorageNumberField id="storage-metrics-retention" label={t('settings.metricsRetention')} value={form.metricsRetentionHours} unit="h" min={minimumMetricsRetentionHours(form.samplingIntervalSeconds)} max={24} step="any" description={t('settings.metricsGuidance')} onChange={update('metricsRetentionHours')} /></div></fieldset></div>
      {Boolean(saveError) && <p role="alert" className="text-sm text-destructive">{errorMessage(saveError)}</p>}
      {saveResult && <p role="status" className="text-xs text-muted-foreground">{t('settings.savedRestartPending')}</p>}
      <Button type="submit" disabled={saving}><Check className="h-4 w-4" />{saving ? t('settings.saving') : t('settings.saveStorage')}</Button>
    </form>
  </CardContent></Card>
}
