import { useEffect, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Activity, Archive, Check, CircleAlert, Download, HardDrive, Info, Logs, ShieldCheck, SlidersHorizontal } from 'lucide-react'
import { dashboardAPI, productAPI } from '@/api'
import { qk } from '@/api/queries'
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
import { applyResolvedTheme, resolveTheme, type ThemePreference } from '@/lib/theme'
import { auditEventLabel } from '@/lib/labels'
import type { StorageSettings, SystemSettings } from '@/types/api'
import { RuntimeUpdates } from '@/components/settings/runtime-updates'

type Section = 'preferences' | 'storage' | 'updates' | 'logs' | 'audit'
export function SettingsPage() {
  const [section, setSection] = useState<Section>('preferences')
  const [logLevel, setLogLevel] = useState(''); const [logComponent, setLogComponent] = useState(''); const [logSearch, setLogSearch] = useState(''); const [appliedSearch, setAppliedSearch] = useState('')
  const client = useQueryClient(); const { toast } = useToast()
  const settings = useQuery({ queryKey: qk.settings, queryFn: productAPI.settings })
  const storage = useQuery({ queryKey: qk.storage, queryFn: dashboardAPI.storage, enabled: section === 'storage' || section === 'preferences', staleTime: 20_000 })
  const system = useQuery({ queryKey: qk.info, queryFn: dashboardAPI.info, enabled: section === 'storage' || section === 'preferences', staleTime: 60_000 })
  const candidates = useQuery({ queryKey: ['retention', 'candidates'], queryFn: productAPI.retentionCandidates, enabled: section === 'storage', staleTime: 0 })
  const logs = useQuery({ queryKey: qk.logs({ logLevel, logComponent, appliedSearch }), queryFn: () => productAPI.logs({ level: logLevel || undefined, component: logComponent || undefined, q: appliedSearch || undefined, limit: 100 }), enabled: section === 'logs', staleTime: 5000 })
  const audit = useQuery({ queryKey: qk.audit, queryFn: productAPI.audit, enabled: section === 'audit', staleTime: 10_000 })
  const save = useMutation({ mutationFn: productAPI.saveSettings, onSuccess: result => { void client.setQueryData(qk.settings, result); toast('설정을 저장했습니다.'); applyTheme(result.settings.ui.theme) }, onError: error => toast('설정 저장 실패', errorMessage(error), 'error') })
  const saveStorage = useMutation({ mutationFn: (storage: StorageSettings) => productAPI.saveSettings({ storage }), onSuccess: result => { void client.setQueryData(qk.settings, result); toast('수집·저장 설정을 저장했습니다.') }, onError: error => toast('수집·저장 설정 저장 실패', errorMessage(error), 'error') })
  const retentionRun = useMutation({ mutationFn: productAPI.runRetention, onSuccess: result => { toast(`${result.deleted_count}개 녹화를 정리했습니다.`); void client.invalidateQueries({ queryKey: ['retention', 'candidates'] }); void client.invalidateQueries({ queryKey: ['recordings'] }); void client.invalidateQueries({ queryKey: qk.dashboard }); void client.invalidateQueries({ queryKey: qk.storage }); void client.invalidateQueries({ queryKey: qk.notifications }); void client.invalidateQueries({ queryKey: qk.audit }) }, onError: error => toast('정리 작업 실패', errorMessage(error), 'error') })
  const [theme, setTheme] = useState<'system' | 'light' | 'dark'>('system'); const [concurrency, setConcurrency] = useState(1); const [retentionEnabled, setRetentionEnabled] = useState(false); const [retentionDays, setRetentionDays] = useState(365)
  useEffect(() => { if (!settings.data) return; setTheme(settings.data.settings.ui.theme); setConcurrency(settings.data.settings.integrity.concurrency); setRetentionEnabled(settings.data.settings.retention.enabled); setRetentionDays(settings.data.settings.retention.completed_after_days); applyTheme(settings.data.settings.ui.theme) }, [settings.data])
  const tabs: { id: Section; label: string; icon: typeof SlidersHorizontal }[] = [{ id: 'preferences', label: '설정', icon: SlidersHorizontal }, { id: 'storage', label: '저장소', icon: HardDrive }, { id: 'updates', label: '업데이트', icon: Download }, { id: 'logs', label: '로그', icon: Logs }, { id: 'audit', label: '감사 기록', icon: ShieldCheck }]
  const submit = (event: React.FormEvent) => { event.preventDefault(); save.mutate({ ui: { theme }, integrity: { concurrency }, retention: { enabled: retentionEnabled, completed_after_days: retentionDays } }) }
  return <div className="page-enter"><PageHeading eyebrow="시스템 관리" title="설정" description="현재 서버 실행 환경에서 지원하는 설정과 운영 정보를 관리합니다." />
    <div className="mb-5 flex gap-1 overflow-x-auto border-b border-border" role="tablist" aria-label="설정 섹션">{tabs.map(item => <button key={item.id} id={'settings-tab-' + item.id} aria-controls={'settings-panel-' + item.id} role="tab" aria-selected={section === item.id} tabIndex={section === item.id ? 0 : -1} onClick={() => setSection(item.id)} className={`focus-ring inline-flex min-w-max items-center gap-2 border-b-2 px-3 py-2.5 text-sm ${section === item.id ? 'border-primary font-semibold text-primary' : 'border-transparent text-muted-foreground hover:text-foreground'}`}><item.icon className="h-4 w-4" />{item.label}</button>)}</div>
    {section === 'preferences' && <div role="tabpanel" id="settings-panel-preferences" aria-labelledby="settings-tab-preferences" tabIndex={0} className="grid gap-4 xl:grid-cols-[1fr_.75fr]"><Card><CardHeader><CardTitle className="flex items-center gap-2"><SlidersHorizontal className="h-4 w-4 text-primary" />시스템 설정</CardTitle><p className="text-xs text-muted-foreground">지원되는 설정은 즉시 적용되며, 일부 항목은 재시작 후 적용됩니다.</p></CardHeader><CardContent>{settings.isLoading ? <LoadingState /> : settings.error ? <ErrorState message={errorMessage(settings.error)} retry={() => void settings.refetch()} /> : <form className="space-y-6" onSubmit={submit}><div className="space-y-2"><label className="text-sm font-medium">UI 테마</label><Select value={theme} onValueChange={value => setTheme(value as typeof theme)}><SelectItem value="system">시스템 설정 따르기</SelectItem><SelectItem value="light">라이트</SelectItem><SelectItem value="dark">다크</SelectItem></Select><p className="text-xs text-muted-foreground">설정 저장 후 이 브라우저에 적용됩니다.</p></div><div className="space-y-2"><label className="text-sm font-medium">무결성 검사 동시 실행 수</label><Select value={String(concurrency)} onValueChange={value => setConcurrency(Number(value))}>{[1, 2, 3, 4].map(value => <SelectItem key={value} value={String(value)}>{value}개</SelectItem>)}</Select><p className="text-xs text-muted-foreground">동시에 검사할 보관 데이터 수입니다.</p>{settings.data?.restart_required.includes('integrity.concurrency') && <p className="flex items-center gap-1 text-xs text-amber-700"><CircleAlert className="h-3.5 w-3.5" />이 변경은 재시작 후 적용됩니다.</p>}</div><div className="space-y-3 rounded-lg border border-border p-4"><div><p className="text-sm font-semibold">자동 보존 기간 정리</p><p className="mt-1 text-xs leading-5 text-muted-foreground">기본은 꺼져 있습니다. 켜면 오래된 완료 녹화를 자동으로 삭제합니다.</p></div><label className="flex items-center gap-2 text-sm"><input type="checkbox" checked={retentionEnabled} onChange={event => setRetentionEnabled(event.target.checked)} className="h-4 w-4 accent-primary" />자동 정리 활성화</label><label className="block space-y-1.5 text-xs text-muted-foreground">완료 후 보관 일수<Input type="number" min={1} max={3650} value={retentionDays} onChange={event => setRetentionDays(Number(event.target.value))} /></label></div><Button type="submit" disabled={save.isPending}><Check className="h-4 w-4" />{save.isPending ? '저장 중…' : '설정 저장'}</Button></form>}</CardContent></Card><div className="space-y-4"><Card><CardHeader><CardTitle>실행 환경</CardTitle></CardHeader><CardContent>{system.isLoading ? <LoadingState /> : system.error ? <ErrorState message={errorMessage(system.error)} retry={() => void system.refetch()} /> : <dl className="grid grid-cols-2 gap-4">{[['버전', system.data?.version], ['커밋', system.data?.commit], ['Go', system.data?.go_version], ['OS / 아키텍처', `${system.data?.goos} / ${system.data?.goarch}`], ['시작', formatDate(system.data?.started_at)], ['가동 시간', formatDuration(system.data?.uptime_seconds)]].map(([label, value]) => <div key={label}><dt className="text-[11px] text-muted-foreground">{label}</dt><dd className="mt-1 break-all text-xs font-medium">{value || '—'}</dd></div>)}</dl>}</CardContent></Card><Card><CardHeader><CardTitle>기능 상태</CardTitle></CardHeader><CardContent className="space-y-3 text-xs"><div className="flex items-center justify-between"><span>리먹스 내보내기</span><Badge tone={system.data?.export_available ? 'green' : 'neutral'}>{system.data?.export_available ? '사용 가능' : '사용 불가'}</Badge></div><p className="flex gap-2 leading-5 text-muted-foreground"><Info className="h-4 w-4 shrink-0 text-primary" />네트워크에 노출되는 배포는 신뢰할 수 있는 역방향 프록시와 HTTPS 및 인증 경계 뒤에서 운영하세요.</p></CardContent></Card></div></div>}
    {section === 'storage' && <div role="tabpanel" id="settings-panel-storage" aria-labelledby="settings-tab-storage" tabIndex={0} className="space-y-4"><StorageSettingsForm data={settings.data} loading={settings.isLoading} error={settings.error} retry={() => void settings.refetch()} saving={saveStorage.isPending} saveError={saveStorage.error} saveResult={saveStorage.data} onSave={value => saveStorage.mutate(value)} /><div className="grid gap-4 xl:grid-cols-[1fr_.8fr]"><Card><CardHeader><CardTitle>보관 저장소</CardTitle><p className="text-xs text-muted-foreground">서버가 계산한 디렉터리와 파일 시스템 사용량</p></CardHeader><CardContent>{storage.isLoading ? <LoadingState /> : storage.error ? <ErrorState message={errorMessage(storage.error)} retry={() => void storage.refetch()} /> : <div className="grid grid-cols-2 gap-5 sm:grid-cols-3">{[['녹화 보관 데이터', storage.data?.recordings_bytes_known === false ? '—' : formatBytes(storage.data?.recordings_bytes)], ['파일 시스템 사용량', formatBytes(storage.data?.filesystem_used_bytes)], ['사용 가능', formatBytes(storage.data?.filesystem_available_bytes)], ['파일 시스템 전체', formatBytes(storage.data?.filesystem_total_bytes)], ['녹화 수', storage.data?.recording_count], ['세그먼트', storage.data?.segment_count], ['초기화 객체', storage.data?.init_segment_count], ['매니페스트 스냅샷', storage.data?.manifest_count]].map(([label, value]) => <div key={label}><p className="text-xs text-muted-foreground">{label}</p><p className="mt-1 text-lg font-semibold tabular-nums">{value ?? '—'}</p></div>)}</div>}</CardContent></Card><Card><CardHeader><CardTitle>자동 정리 대상</CardTitle><p className="text-xs text-muted-foreground">현재 정책으로 삭제 대상으로 계산된 항목</p></CardHeader><CardContent>{candidates.isLoading ? <LoadingState /> : candidates.error ? <ErrorState message={errorMessage(candidates.error)} retry={() => void candidates.refetch()} /> : <><div className="flex items-baseline justify-between"><span className="text-3xl font-semibold">{candidates.data?.candidate_count ?? 0}</span><Badge tone={candidates.data?.enabled ? 'amber' : 'neutral'}>{candidates.data?.enabled ? '자동 정리 켜짐' : '자동 정리 꺼짐'}</Badge></div>{candidates.data?.candidates.length ? <div className="mt-4 max-h-64 divide-y divide-border overflow-auto">{candidates.data.candidates.map(item => <div key={item.id} className="flex justify-between gap-3 py-2 text-xs"><span className="truncate font-mono">{item.id}</span><time className="shrink-0 text-muted-foreground">{formatDate(item.stopped_at)}</time></div>)}</div> : <p className="mt-3 text-xs text-muted-foreground">정리 후보가 없습니다.</p>}<Confirm trigger={<Button variant="destructive" className="mt-4" disabled={!candidates.data?.enabled || !(candidates.data?.candidate_count ?? 0)}><Archive className="h-4 w-4" />지금 정리 실행</Button>} title="자동 정리를 실행할까요?" description={`현재 설정(${settings.data?.settings.retention.completed_after_days ?? 365}일)보다 오래된 완료 녹화입니다. ${candidates.data?.candidate_count ?? 0}개가 삭제 대상으로 계산되었습니다.`} confirmLabel="정리 실행" destructive onConfirm={() => retentionRun.mutate()} disabled={retentionRun.isPending} /></>}</CardContent></Card></div></div>}
    {section === 'updates' && <RuntimeUpdates />}
    {section === 'logs' && <Card role="tabpanel" id="settings-panel-logs" aria-labelledby="settings-tab-logs" tabIndex={0}><CardHeader><CardTitle>애플리케이션 로그</CardTitle><p className="text-xs text-muted-foreground">서버가 보유한 제한된 로그 스트림입니다. 파일 시스템 로그 파일에는 접근하지 않습니다.</p></CardHeader><CardContent><form className="mb-4 grid gap-2 sm:grid-cols-[150px_180px_1fr_auto]" onSubmit={event => { event.preventDefault(); setAppliedSearch(logSearch) }}><Select value={logLevel || '__all'} onValueChange={value => setLogLevel(value === '__all' ? '' : value)}><SelectItem value="__all">모든 수준</SelectItem>{['debug', 'info', 'warn', 'error'].map(value => <SelectItem key={value} value={value}>{value}</SelectItem>)}</Select><Input value={logComponent} onChange={event => setLogComponent(event.target.value)} placeholder="구성 요소" /><Input value={logSearch} onChange={event => setLogSearch(event.target.value)} placeholder="메시지 검색" /><Button variant="outline">검색</Button></form>{logs.isLoading ? <LoadingState /> : logs.error ? <ErrorState message={errorMessage(logs.error)} retry={() => void logs.refetch()} /> : logs.data?.items.length ? <div className="max-h-[600px] overflow-auto rounded-md border border-border"><table className="w-full min-w-[680px] text-left text-xs"><thead className="sticky top-0 bg-muted"><tr>{['시간', '수준', '구성 요소', '메시지'].map(value => <th key={value} className="px-3 py-2 font-semibold">{value}</th>)}</tr></thead><tbody>{logs.data.items.map((item, index) => <tr key={`${item.at}-${index}`} className="border-t border-border"><td className="whitespace-nowrap px-3 py-2 text-muted-foreground">{formatDate(item.at)}</td><td className="px-3 py-2"><Badge tone={item.level === 'error' ? 'red' : item.level === 'warn' ? 'amber' : 'neutral'}>{item.level}</Badge></td><td className="px-3 py-2">{item.component}</td><td className="max-w-2xl break-words px-3 py-2 font-mono">{item.message}</td></tr>)}</tbody></table></div> : <EmptyState title="표시할 로그가 없습니다." />}</CardContent></Card>}
    {section === 'audit' && <Card role="tabpanel" id="settings-panel-audit" aria-labelledby="settings-tab-audit" tabIndex={0}><CardHeader><CardTitle>감사 기록</CardTitle><p className="text-xs text-muted-foreground">민감 설정 값 없이 관리 동작만 기록합니다.</p></CardHeader><CardContent>{audit.isLoading ? <LoadingState /> : audit.error ? <ErrorState message={errorMessage(audit.error)} retry={() => void audit.refetch()} /> : audit.data?.items.length ? <div className="divide-y divide-border">{audit.data.items.map(item => <div key={item.id} className="flex flex-wrap items-center gap-3 py-3"><span className="grid h-8 w-8 place-items-center rounded-md bg-muted"><Activity className="h-4 w-4 text-muted-foreground" /></span><span className="min-w-0 flex-1"><span className="block text-sm font-medium">{auditEventLabel(item.type)}</span><span className="block truncate text-xs text-muted-foreground">{item.object_id ?? '시스템'}</span></span><time className="text-xs text-muted-foreground">{formatDate(item.at)}</time></div>)}</div> : <EmptyState title="감사 기록이 없습니다." />}</CardContent></Card>}
  </div>
}
function applyTheme(theme: ThemePreference) { applyResolvedTheme(resolveTheme(theme, matchMedia('(prefers-color-scheme: dark)').matches)); localStorage.setItem('ir-theme', theme) }

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
function formatMiB(bytes: number) { return `${(bytes / MIB).toLocaleString('ko-KR', { maximumFractionDigits: 1 })} MiB` }
function StorageNumberField({ id, label, value, unit, min, max, step = 1, description, onChange }: {
  id: string; label: string; value: string; unit: string; min: number; max: number; step?: number | 'any'; description?: string; onChange: (value: string) => void
}) {
  const descriptionId = `${id}-description`
  return <div className="space-y-1.5"><label htmlFor={id} className="block text-xs font-medium">{label}</label><div className="flex items-center gap-2"><Input id={id} type="number" inputMode="decimal" required min={min} max={max} step={step} value={value} aria-describedby={description ? descriptionId : undefined} onChange={event => onChange(event.target.value)} /><span className="shrink-0 text-xs text-muted-foreground">{unit}</span></div>{description && <p id={descriptionId} className="text-[11px] leading-4 text-muted-foreground">{description}</p>}</div>
}
function StorageSettingsForm({ data, loading, error, retry, saving, saveError, saveResult, onSave }: {
  data?: SystemSettings; loading: boolean; error: unknown; retry: () => void; saving: boolean; saveError: unknown; saveResult?: SystemSettings; onSave: (settings: StorageSettings) => void
}) {
  const [form, setForm] = useState<StorageForm | null>(null)
  useEffect(() => { if (data?.settings.storage) setForm(storageFormFromSettings(data.settings.storage)) }, [data?.settings.storage])
  if (loading) return <Card><CardContent className="pt-6"><LoadingState /></CardContent></Card>
  if (error) return <Card><CardContent className="pt-6"><ErrorState message={errorMessage(error)} retry={retry} /></CardContent></Card>
  if (!data || !form) return null
  const update = (key: keyof StorageForm) => (value: string) => setForm(current => current ? { ...current, [key]: value } : current)
  const pendingStorageKeys = data.restart_required.filter(key => key.startsWith('storage.'))
  const submit = (event: React.FormEvent) => { event.preventDefault(); onSave(storageSettingsFromForm(form)) }
  const effective = data.effective_storage
  return <Card><CardHeader><CardTitle className="flex items-center gap-2"><HardDrive className="h-4 w-4 text-primary" />고급 수집·저장 설정</CardTitle><p className="text-xs leading-5 text-muted-foreground">운영 환경에 맞게 메모리, 대기열, 재시도, 측정 기록을 조정합니다. 변경된 값은 서비스 재시작 후 적용됩니다.</p></CardHeader><CardContent className="space-y-5">
    {pendingStorageKeys.length > 0 && <div role="status" className="flex gap-2 rounded-md border border-amber-500/40 bg-amber-50 p-3 text-xs leading-5 text-amber-950 dark:bg-amber-950/20 dark:text-amber-100"><CircleAlert className="mt-0.5 h-4 w-4 shrink-0" /><span>저장된 수집·저장 설정이 현재 실행값과 다릅니다. 서버를 재시작해야 적용됩니다.</span></div>}
    <div className="rounded-md bg-muted/60 p-3" data-testid="effective-storage-settings"><p className="text-xs font-semibold">현재 적용 중</p><dl className="mt-2 grid grid-cols-2 gap-x-5 gap-y-2 text-xs sm:grid-cols-4"><div><dt className="text-muted-foreground">전체 메모리 한도</dt><dd className="mt-0.5 font-medium tabular-nums">{formatMiB(effective.ingest_memory.global_buffer_bytes)}</dd></div><div><dt className="text-muted-foreground">녹화별 메모리 한도</dt><dd className="mt-0.5 font-medium tabular-nums">{formatMiB(effective.ingest_memory.per_recording_buffer_bytes)}</dd></div><div><dt className="text-muted-foreground">저장 작업 동시 실행</dt><dd className="mt-0.5 font-medium tabular-nums">{effective.queue_writer.writer_concurrency}개</dd></div><div><dt className="text-muted-foreground">측정 주기</dt><dd className="mt-0.5 font-medium tabular-nums">{effective.observability.sampling_interval_ms / 1000}초</dd></div></dl></div>
    <form className="space-y-5" onSubmit={submit}>
      <div className="grid gap-4 lg:grid-cols-2"><fieldset className="space-y-3 rounded-lg border border-border p-4"><legend className="px-1 text-sm font-semibold">메모리 버퍼</legend><div className="grid gap-3 sm:grid-cols-2"><StorageNumberField id="storage-global-buffer" label="전체 버퍼 한도" value={form.globalBufferMiB} unit="MiB" min={64} max={2048} description="전체 동시 수집 작업이 나누어 사용하는 휘발성 메모리 한도입니다." onChange={update('globalBufferMiB')} /><StorageNumberField id="storage-recording-buffer" label="녹화별 버퍼 한도" value={form.perRecordingBufferMiB} unit="MiB" min={1} max={1536} onChange={update('perRecordingBufferMiB')} /><StorageNumberField id="storage-max-payload" label="단일 페이로드 최대 크기" value={form.maxPayloadMiB} unit="MiB" min={1} max={1024} onChange={update('maxPayloadMiB')} /></div><p className="text-[11px] leading-4 text-muted-foreground">버퍼에만 있는 데이터는 아직 보관 데이터로 확정되지 않습니다.</p></fieldset>
        <fieldset className="space-y-3 rounded-lg border border-border p-4"><legend className="px-1 text-sm font-semibold">대기열·저장 작업</legend><div className="grid gap-3 sm:grid-cols-2"><StorageNumberField id="storage-queue-capacity" label="저장 대기열 최대 크기" value={form.pendingQueueCapacity} unit="개" min={1} max={128} onChange={update('pendingQueueCapacity')} /><div className="space-y-1.5"><label htmlFor="storage-writer-concurrency" className="block text-xs font-medium">저장 작업 동시 실행 수</label><div className="flex items-center gap-2"><Input id="storage-writer-concurrency" type="number" value={1} disabled readOnly aria-describedby="storage-writer-guidance" /><span className="shrink-0 text-xs text-muted-foreground">개</span></div><p id="storage-writer-guidance" className="text-[11px] leading-4 text-muted-foreground">현재는 순서 보장을 위해 1개만 지원합니다. 높은 동시성이 항상 빠른 것은 아니며 HDD나 느린 저장소에서는 처리량을 낮출 수 있습니다.</p></div></div></fieldset>
        <fieldset className="space-y-3 rounded-lg border border-border p-4"><legend className="px-1 text-sm font-semibold">실패 처리</legend><div className="grid gap-3 sm:grid-cols-3"><StorageNumberField id="storage-persist-attempts" label="총 저장 시도 횟수" value={form.persistAttempts} unit="회" min={1} max={10} description="최초 저장 시도를 포함한 최대 시도 횟수입니다." onChange={update('persistAttempts')} /><StorageNumberField id="storage-retry-initial" label="초기 대기" value={form.retryInitialBackoffMs} unit="ms" min={10} max={30000} onChange={update('retryInitialBackoffMs')} /><StorageNumberField id="storage-retry-max" label="최대 대기" value={form.retryMaxBackoffMs} unit="ms" min={10} max={300000} onChange={update('retryMaxBackoffMs')} /></div></fieldset>
        <fieldset className="space-y-3 rounded-lg border border-border p-4"><legend className="px-1 text-sm font-semibold">관측 기록</legend><div className="grid gap-3 sm:grid-cols-2"><StorageNumberField id="storage-sampling-interval" label="측정 간격" value={form.samplingIntervalSeconds} unit="초" min={1} max={3600} onChange={update('samplingIntervalSeconds')} /><StorageNumberField id="storage-metrics-retention" label="측정 기록 보존 기간" value={form.metricsRetentionHours} unit="시간" min={minimumMetricsRetentionHours(form.samplingIntervalSeconds)} max={24} step="any" description="관측 상한 계산에는 최소 5분의 비유휴 기록과 방향별 20개 샘플이 필요합니다." onChange={update('metricsRetentionHours')} /></div></fieldset></div>
      {Boolean(saveError) && <p role="alert" className="text-sm text-destructive">{errorMessage(saveError)}</p>}
      {saveResult && <p role="status" className="text-xs text-muted-foreground">설정을 저장했습니다. 현재 적용값은 서버 재시작 전까지 유지됩니다.</p>}
      <Button type="submit" disabled={saving}><Check className="h-4 w-4" />{saving ? '저장 중…' : '수집·저장 설정 저장'}</Button>
    </form>
  </CardContent></Card>
}
