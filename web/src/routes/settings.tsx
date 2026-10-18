import { useEffect, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Link, useRouterState } from '@tanstack/react-router'
import { Activity, Archive, Check, CircleAlert, ShieldCheck } from 'lucide-react'
import { dashboardAPI, productAPI, userPreferencesAPI } from '@/api'
import { accountAuthorizationQuery, accountQuery, qk, userPreferencesQuery } from '@/api/queries'
import { formatDate, formatDuration } from '@/lib/utils'
import { PageHeading } from '@/components/page-heading'
import { errorMessage } from '@/lib/errors'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Confirm } from '@/components/ui/confirm'
import { Input } from '@/components/ui/input'
import { Select, SelectItem } from '@/components/ui/select'
import { EmptyState, ErrorState, LoadingState } from '@/components/query-state'
import { useToast } from '@/components/ui/use-toast'
import { auditEventLabel } from '@/lib/labels'
import type { StorageSettings, SystemSettings, UserPreferences } from '@/types/api'
import { RuntimeUpdates } from '@/components/settings/runtime-updates'
import { useI18n } from '@/i18n/provider'
import { formatNumber, validTimeZone } from '@/lib/formatting'
import { resolveLocale, translate } from '@/i18n/catalog'
import { AccountSecurityPanel, UsersManagementPanel } from '@/components/settings/account-security'
import { HTTPSSettings } from '@/components/settings/https-settings'

export type SettingsSection = 'preferences' | 'security' | 'sessions' | 'runtime' | 'users' | 'updates' | 'diagnostics' | 'audit' | 'retention' | 'advanced-storage'

export function SettingsPage({ section }: { section: SettingsSection }) {
  const { t } = useI18n()
  const pathname = useRouterState({ select: state => state.location.pathname })
  const account = useQuery(accountQuery)
  const authorization = useQuery(accountAuthorizationQuery)
  const can = (permission: string) => authorization.data?.permissions?.includes(permission) ?? false
  const [logLevel, setLogLevel] = useState('')
  const [logComponent, setLogComponent] = useState('')
  const [logSearch, setLogSearch] = useState('')
  const [appliedSearch, setAppliedSearch] = useState('')
  const systemSection = section === 'runtime' || section === 'retention' || section === 'advanced-storage'
  const canReadSystem = can('system.read')
  const canEditSystem = can('system.settings.manage')
  const settings = useQuery({ queryKey: qk.settings, queryFn: productAPI.settings, enabled: canEditSystem && systemSection })
  const systemInfo = useQuery({ queryKey: qk.info, queryFn: dashboardAPI.info, enabled: canReadSystem && section === 'runtime', staleTime: 60_000 })
  const logsSection = useQuery({ queryKey: qk.logs({ level: logLevel, component: logComponent, q: appliedSearch }), queryFn: () => productAPI.logs({ level: logLevel || undefined, component: logComponent || undefined, q: appliedSearch || undefined, limit: 100 }), enabled: can('system.diagnostics.read') && section === 'diagnostics', staleTime: 5_000 })
  const audit = useQuery({ queryKey: qk.audit, queryFn: productAPI.audit, enabled: can('audit.read') && section === 'audit', staleTime: 10_000 })
  const titleKey = ({ preferences: 'settings.preferences', security: 'account.profile', sessions: 'account.sessions', runtime: 'settings.runtimeDetails', users: 'account.users', updates: 'settings.tab.updates', diagnostics: 'settings.tab.logs', audit: 'settings.tab.audit', retention: 'settings.retentionTitle', 'advanced-storage': 'settings.advancedStorageTitle' } as const)[section]
  return <div className="page-enter space-y-4"><PageHeading title={t(titleKey)} />
    {['preferences', 'security', 'sessions'].includes(section) && <nav aria-label={t('account.myAccount')} className="flex flex-wrap gap-4 border-b border-border pb-2 text-sm"><Link to="/account/preferences" aria-current={pathname === '/account/preferences' ? 'page' : undefined} className={sectionLinkClass(pathname, '/account/preferences')}>{t('account.preferences')}</Link><Link to="/account/security" aria-current={pathname === '/account/security' ? 'page' : undefined} className={sectionLinkClass(pathname, '/account/security')}>{t('account.profile')}</Link><Link to="/account/sessions" aria-current={pathname === '/account/sessions' ? 'page' : undefined} className={sectionLinkClass(pathname, '/account/sessions')}>{t('account.sessions')}</Link></nav>}
    {['runtime', 'users', 'updates', 'diagnostics', 'audit'].includes(section) && <nav aria-label={t('nav.system')} className="flex flex-wrap gap-4 border-b border-border pb-2 text-sm">{(canReadSystem || canEditSystem) && <Link to="/system" aria-current={pathname === '/system' ? 'page' : undefined} className={sectionLinkClass(pathname, '/system')}>{t('settings.runtimeDetails')}</Link>}{(can('user.read') || can('user.create') || can('user.manage') || can('role.read') || can('role.manage')) && <Link to="/system/users" aria-current={pathname === '/system/users' ? 'page' : undefined} className={sectionLinkClass(pathname, '/system/users')}>{t('nav.users')}</Link>}{can('system.update.manage') && <Link to="/system/updates" aria-current={pathname === '/system/updates' ? 'page' : undefined} className={sectionLinkClass(pathname, '/system/updates')}>{t('settings.tab.updates')}</Link>}{can('system.diagnostics.read') && <Link to="/system/diagnostics" aria-current={pathname === '/system/diagnostics' ? 'page' : undefined} className={sectionLinkClass(pathname, '/system/diagnostics')}>{t('settings.tab.logs')}</Link>}{can('audit.read') && <Link to="/system/audit" aria-current={pathname === '/system/audit' ? 'page' : undefined} className={sectionLinkClass(pathname, '/system/audit')}>{t('settings.tab.audit')}</Link>}</nav>}
    {['retention', 'advanced-storage'].includes(section) && <nav aria-label={t('storage.pageTitle')} className="flex flex-wrap gap-4 border-b border-border pb-2 text-sm">{can('storage.read') && <Link to="/storage" aria-current={pathname === '/storage' ? 'page' : undefined} className={sectionLinkClass(pathname, '/storage')}>{t('storage.pageTitle')}</Link>}{can('storage.manage') && <Link to="/storage/locations" aria-current={pathname === '/storage/locations' ? 'page' : undefined} className={sectionLinkClass(pathname, '/storage/locations')}>{t('storage.instances')}</Link>}{(can('recording.delete') || can('system.settings.manage')) && <Link to="/storage/retention" aria-current={pathname === '/storage/retention' ? 'page' : undefined} className={sectionLinkClass(pathname, '/storage/retention')}>{t('settings.retentionTitle')}</Link>}{can('system.settings.manage') && <Link to="/storage/advanced" aria-current={pathname === '/storage/advanced' ? 'page' : undefined} className={sectionLinkClass(pathname, '/storage/advanced')}>{t('settings.advancedStorageTitle')}</Link>}</nav>}
    {section === 'preferences' && <UserPreferencesPanel />}
    {section === 'security' && (account.isLoading ? <LoadingState /> : account.error ? <ErrorState message={errorMessage(account.error)} retry={() => void account.refetch()} /> : account.data ? <AccountSecurityPanel account={account.data} /> : null)}
    {section === 'sessions' && (account.isLoading ? <LoadingState /> : account.error ? <ErrorState message={errorMessage(account.error)} retry={() => void account.refetch()} /> : account.data ? <AccountSecurityPanel account={account.data} section="sessions" /> : null)}
    {section === 'users' && (account.isLoading ? <LoadingState /> : account.error ? <ErrorState message={errorMessage(account.error)} retry={() => void account.refetch()} /> : account.data ? <UsersManagementPanel currentUser={account.data} /> : null)}
    {section === 'runtime' && <RuntimePage settings={settings.data} settingsLoading={settings.isLoading} settingsError={settings.error} retrySettings={() => void settings.refetch()} system={systemInfo.data} systemLoading={systemInfo.isLoading} systemError={systemInfo.error} retrySystem={() => void systemInfo.refetch()} canEdit={canEditSystem} />}
    {section === 'updates' && <RuntimeUpdates />}
    {section === 'diagnostics' && <DiagnosticsPage query={logsSection} level={logLevel} setLevel={setLogLevel} component={logComponent} setComponent={setLogComponent} search={logSearch} setSearch={setLogSearch} onSearch={() => setAppliedSearch(logSearch)} />}
    {section === 'audit' && <AuditPage query={audit} />}
    {section === 'retention' && <RetentionPage settings={settings.data} loading={settings.isLoading} error={settings.error} retry={() => void settings.refetch()} canConfigure={can('system.settings.manage')} canRun={can('recording.delete')} />}
    {section === 'advanced-storage' && <AdvancedStoragePage settings={settings.data} loading={settings.isLoading} error={settings.error} retry={() => void settings.refetch()} />}
  </div>
}

function sectionLinkClass(pathname: string, target: string) {
  return `focus-ring rounded-sm ${pathname === target ? 'font-semibold text-primary' : 'text-muted-foreground hover:text-foreground'}`
}

function RuntimePage({ settings, settingsLoading, settingsError, retrySettings, system, systemLoading, systemError, retrySystem, canEdit }: {
  settings?: SystemSettings; settingsLoading: boolean; settingsError: unknown; retrySettings: () => void
  system?: Awaited<ReturnType<typeof dashboardAPI.info>>; systemLoading: boolean; systemError: unknown; retrySystem: () => void
  canEdit: boolean
}) {
  const { t } = useI18n(); const client = useQueryClient(); const { toast } = useToast()
  const [concurrency, setConcurrency] = useState(1)
  useEffect(() => { if (settings) setConcurrency(settings.settings.integrity.concurrency) }, [settings])
  const save = useMutation({ mutationFn: () => productAPI.saveSettings({ integrity: { concurrency } }), onSuccess: result => { client.setQueryData(qk.settings, result); toast(t('settings.systemSaved')) }, onError: error => toast(t('settings.systemSaveFailed'), errorMessage(error), 'error') })
  return <div className="grid gap-4 xl:grid-cols-[minmax(0,1fr)_minmax(0,.8fr)]">
    {canEdit && <HTTPSSettings />}
    {canEdit && <Card><CardHeader><CardTitle>{t('settings.integrityConcurrency')}</CardTitle></CardHeader><CardContent>{settingsLoading ? <LoadingState /> : settingsError ? <ErrorState message={errorMessage(settingsError)} retry={retrySettings} /> : <form className="space-y-4" onSubmit={event => { event.preventDefault(); save.mutate() }}><Select aria-label={t('settings.integrityConcurrency')} value={String(concurrency)} onValueChange={value => setConcurrency(Number(value))}>{[1, 2, 3, 4].map(value => <SelectItem key={value} value={String(value)}>{value} {t('settings.items', { count: value })}</SelectItem>)}</Select>{settings?.restart_required.includes('integrity.concurrency') && <p className="flex items-center gap-1 text-xs text-amber-700"><CircleAlert className="h-3.5 w-3.5" />{t('settings.restartRequired')}</p>}<Button type="submit" disabled={save.isPending}><Check className="h-4 w-4" />{save.isPending ? t('settings.saving') : t('settings.systemSave')}</Button></form>}</CardContent></Card>}
    <Card><CardHeader><CardTitle>{t('settings.environment')}</CardTitle></CardHeader><CardContent>{systemLoading ? <LoadingState /> : systemError ? <ErrorState message={errorMessage(systemError)} retry={retrySystem} /> : <><dl className="grid grid-cols-2 gap-4">{[[t('settings.version'), system?.version], [t('settings.commit'), system?.commit], [t('settings.goVersion'), system?.go_version], [t('settings.architecture'), `${system?.goos} / ${system?.goarch}`], [t('settings.started'), system?.started_at ? formatDate(system.started_at) : '—'], [t('settings.uptime'), system?.uptime_seconds ? formatDuration(system.uptime_seconds) : '—']].map(([label, value]) => <div key={label}><dt className="text-xs text-muted-foreground">{label}</dt><dd className="mt-1 break-all text-sm font-medium">{value || '—'}</dd></div>)}</dl><div className="mt-4 border-t border-border pt-3"><p className="text-xs font-semibold">{t('settings.remuxExport')}</p><Badge className="mt-2" tone={system?.export_available ? 'green' : 'neutral'}>{system?.export_available ? t('settings.available') : t('settings.unavailable')}</Badge></div></>}</CardContent></Card>
  </div>
}

function RetentionPage({ settings, loading, error, retry, canConfigure, canRun }: { settings?: SystemSettings; loading: boolean; error: unknown; retry: () => void; canConfigure: boolean; canRun: boolean }) {
  const { t } = useI18n(); const client = useQueryClient(); const { toast } = useToast()
  const candidates = useQuery({ queryKey: ['retention', 'candidates'], queryFn: productAPI.retentionCandidates, staleTime: 0 })
  const [enabled, setEnabled] = useState(false); const [days, setDays] = useState(365)
  useEffect(() => { if (settings) { setEnabled(settings.settings.retention.enabled); setDays(settings.settings.retention.completed_after_days) } }, [settings])
  const save = useMutation({ mutationFn: () => productAPI.saveSettings({ retention: { enabled, completed_after_days: days } }), onSuccess: result => { client.setQueryData(qk.settings, result); toast(t('settings.systemSaved')) }, onError: err => toast(t('settings.systemSaveFailed'), errorMessage(err), 'error') })
  const run = useMutation({ mutationFn: productAPI.runRetention, onSuccess: result => { toast(t('settings.retentionRunSuccess', { count: result.deleted_count })); void client.invalidateQueries({ queryKey: ['retention', 'candidates'] }); void client.invalidateQueries({ queryKey: ['recordings'] }); void client.invalidateQueries({ queryKey: qk.dashboard }); void client.invalidateQueries({ queryKey: qk.storage }); void client.invalidateQueries({ queryKey: qk.notifications }); void client.invalidateQueries({ queryKey: qk.audit }) }, onError: err => toast(t('settings.retentionRunFailed'), errorMessage(err), 'error') })
  return <div className="grid gap-4 xl:grid-cols-2">{canConfigure && <Card><CardContent className="pt-4">{loading ? <LoadingState /> : error ? <ErrorState message={errorMessage(error)} retry={retry} /> : <form className="space-y-4" onSubmit={event => { event.preventDefault(); save.mutate() }}><label className="flex items-center gap-2 text-sm"><input type="checkbox" checked={enabled} onChange={event => setEnabled(event.target.checked)} className="h-4 w-4 accent-primary" />{t('settings.retentionEnable')}</label><label className="block space-y-1.5 text-sm">{t('settings.retentionDays')}<Input type="number" min={1} max={3650} value={days} onChange={event => setDays(Number(event.target.value))} /></label><Button type="submit" disabled={save.isPending}>{t('settings.systemSave')}</Button></form>}</CardContent></Card>}<Card><CardHeader><CardTitle>{t('settings.retentionCandidates')}</CardTitle></CardHeader><CardContent>{candidates.isLoading ? <LoadingState /> : candidates.error ? <ErrorState message={errorMessage(candidates.error)} retry={() => void candidates.refetch()} /> : <><div className="flex items-center justify-between"><span className="text-2xl font-semibold">{candidates.data?.candidate_count ?? 0}</span><Badge tone={candidates.data?.enabled ? 'amber' : 'neutral'}>{candidates.data?.enabled ? t('settings.retentionEnabled') : t('settings.retentionDisabled')}</Badge></div>{candidates.data?.candidates.length ? <div className="mt-3 max-h-64 divide-y divide-border overflow-auto">{candidates.data.candidates.map(item => <div key={item.id} className="flex justify-between gap-3 py-2 text-xs"><span className="truncate font-mono">{item.id}</span><time className="shrink-0 text-muted-foreground">{formatDate(item.stopped_at)}</time></div>)}</div> : <EmptyState title={t('settings.noRetentionCandidates')} />}{canRun && <Confirm trigger={<Button className="mt-4" variant="destructive" disabled={!candidates.data?.enabled || !(candidates.data?.candidate_count ?? 0)}><Archive className="h-4 w-4" />{t('settings.runRetentionNow')}</Button>} title={t('settings.confirmRetentionTitle')} description={t('settings.confirmRetentionBody', { days: settings?.settings.retention.completed_after_days ?? days, count: candidates.data?.candidate_count ?? 0 })} confirmLabel={t('settings.confirmRetention')} destructive onConfirm={() => run.mutate()} disabled={run.isPending} />}</>}</CardContent></Card></div>
}

function AdvancedStoragePage({ settings, loading, error, retry }: { settings?: SystemSettings; loading: boolean; error: unknown; retry: () => void }) {
  const client = useQueryClient(); const { toast } = useToast(); const { t } = useI18n()
  const save = useMutation({ mutationFn: (storage: StorageSettings) => productAPI.saveSettings({ storage }), onSuccess: result => { client.setQueryData(qk.settings, result); toast(t('settings.saved')) }, onError: err => toast(t('settings.saveStorageFailed'), errorMessage(err), 'error') })
  return <StorageSettingsForm data={settings} loading={loading} error={error} retry={retry} saving={save.isPending} saveError={save.error} saveResult={save.data} onSave={value => save.mutate(value)} />
}

function DiagnosticsPage({ query, level, setLevel, component, setComponent, search, setSearch, onSearch }: {
  query: ReturnType<typeof useQuery<Awaited<ReturnType<typeof productAPI.logs>>>>
  level: string; setLevel: (value: string) => void; component: string; setComponent: (value: string) => void
  search: string; setSearch: (value: string) => void; onSearch: () => void
}) {
  const { t } = useI18n()
  return <Card><CardHeader><CardTitle>{t('settings.logsTitle')}</CardTitle></CardHeader><CardContent><form className="mb-4 grid gap-2 sm:grid-cols-[150px_180px_1fr_auto]" onSubmit={event => { event.preventDefault(); onSearch() }}><Select aria-label={t('settings.allLevels')} value={level || '__all'} onValueChange={value => setLevel(value === '__all' ? '' : value)}><SelectItem value="__all">{t('settings.allLevels')}</SelectItem>{['debug', 'info', 'warn', 'error'].map(value => <SelectItem key={value} value={value}>{value}</SelectItem>)}</Select><Input aria-label={t('settings.component')} value={component} onChange={event => setComponent(event.target.value)} placeholder={t('settings.component')} /><Input aria-label={t('settings.messageSearch')} value={search} onChange={event => setSearch(event.target.value)} placeholder={t('settings.messageSearch')} /><Button variant="outline">{t('settings.search')}</Button></form>{query.isLoading ? <LoadingState /> : query.error ? <ErrorState message={errorMessage(query.error)} retry={() => void query.refetch()} /> : query.data?.items.length ? <div className="max-h-[600px] overflow-auto"><table className="w-full min-w-[640px] text-left text-xs"><thead className="sticky top-0 bg-muted"><tr>{[t('settings.time'), t('settings.level'), t('settings.component'), t('settings.message')].map(value => <th key={value} className="px-3 py-2">{value}</th>)}</tr></thead><tbody>{query.data.items.map((item, index) => <tr key={`${item.at}-${index}`} className="border-t border-border"><td className="whitespace-nowrap px-3 py-2 text-muted-foreground">{formatDate(item.at)}</td><td className="px-3 py-2"><Badge tone={item.level === 'error' ? 'red' : item.level === 'warn' ? 'amber' : 'neutral'}>{item.level}</Badge></td><td className="px-3 py-2">{item.component}</td><td className="max-w-2xl break-words px-3 py-2 font-mono">{item.message}</td></tr>)}</tbody></table></div> : <EmptyState title={t('settings.noLogs')} />}</CardContent></Card>
}

function AuditPage({ query }: { query: ReturnType<typeof useQuery<Awaited<ReturnType<typeof productAPI.audit>>>> }) {
  const { t } = useI18n()
  return <Card><CardHeader><CardTitle className="flex items-center gap-2"><ShieldCheck className="h-4 w-4 text-primary" />{t('settings.auditTitle')}</CardTitle></CardHeader><CardContent>{query.isLoading ? <LoadingState /> : query.error ? <ErrorState message={errorMessage(query.error)} retry={() => void query.refetch()} /> : query.data?.items.length ? <div className="divide-y divide-border">{query.data.items.map(item => { const actor = item.actor?.type === 'user' ? `${t('settings.userActor')} ${item.actor.user_id ?? ''}` : item.actor?.type === 'local_admin' ? t('settings.localAdminActor') : t('settings.systemActor'); return <div key={item.id} className="flex flex-wrap items-center gap-3 py-3"><span className="grid h-8 w-8 place-items-center rounded-md bg-muted"><Activity className="h-4 w-4 text-muted-foreground" /></span><span className="min-w-0 flex-1"><span className="block text-sm font-medium">{auditEventLabel(item.type)}</span><span className="block truncate text-xs text-muted-foreground">{actor}{item.object_id ? ` · ${item.object_id}` : ''}</span></span><time className="text-xs text-muted-foreground">{formatDate(item.at)}</time></div> })}</div> : <EmptyState title={t('settings.auditEmpty')} />}</CardContent></Card>
}

export function UserPreferencesPanel() {
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
  return <Card><CardContent className="pt-4">
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
export function StorageSettingsForm({ data, loading, error, retry, saving, saveError, saveResult, onSave }: {
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
  return <Card><CardContent className="space-y-5 pt-4">
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
