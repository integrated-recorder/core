import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import type { ReactNode } from 'react'
import { Link } from '@tanstack/react-router'
import { Activity, BellRing, Check, Clock3, Pause, Play, Plus, Radio, RefreshCw } from 'lucide-react'
import { watchesAPI } from '@/api'
import { adaptersQuery, watchesQuery } from '@/api/queries'
import { AdapterMark } from '@/components/adapter/adapter-mark'
import { EmptyState, ErrorState, LoadingState } from '@/components/query-state'
import { PageHeading } from '@/components/page-heading'
import { StatusBadge } from '@/components/status-badge'
import { Button } from '@/components/ui/button'
import { Confirm } from '@/components/ui/confirm'
import { WatchPreview } from '@/components/watch/watch-preview'
import { invalidateWatchData, watchErrorLabel } from '@/lib/watches'
import { formatDate, resourceLabel } from '@/lib/utils'
import { useToast } from '@/components/ui/use-toast'
import type { Adapter, WatchView } from '@/types/api'
import { errorMessage } from '@/lib/errors'
import { useI18n } from '@/i18n/provider'

export function WatchesPage() {
  const query = useQuery(watchesQuery)
  const adapters = useQuery(adaptersQuery)
  const client = useQueryClient()
  const { toast } = useToast()
  const { t } = useI18n()
  const action = useMutation({
    mutationFn: ({ id, kind }: { id: string; kind: 'enable' | 'disable' | 'check' }) => watchesAPI[kind](id),
    onSuccess: async (watch, variables) => {
      await invalidateWatchData(client, watch.id)
      toast(variables.kind === 'check' ? t('watch.page.checkSuccess') : variables.kind === 'enable' ? t('watch.page.enableSuccess') : t('watch.page.disableSuccess'), undefined, 'success')
    },
    onError: (_error, variables) => toast(variables.kind === 'check' ? t('watch.page.checkFailed') : t('watch.page.actionFailed'), t('watch.page.retry'), 'error'),
  })
  const items = query.data?.items ?? []
  const summary = {
    total: items.length,
    enabled: items.filter(item => item.enabled).length,
    recording: items.filter(item => item.state === 'recording').length,
    offline: items.filter(item => item.enabled && item.state === 'offline').length,
    attention: items.filter(item => item.state === 'attention_required').length,
  }
  const adapterById = new Map((adapters.data ?? []).map(adapter => [adapter.status.id, adapter]))

  return <div className="page-enter">
    <PageHeading title={t('watch.page.title')} actions={<><Button variant="outline" onClick={() => void query.refetch()}><RefreshCw className="h-4 w-4" />{t('watch.page.refresh')}</Button><Link to="/watches/new"><Button><Plus className="h-4 w-4" />{t('watch.page.create')}</Button></Link></>} />
    {!query.isLoading && !query.error && <div className="mb-5 grid grid-cols-2 gap-x-4 gap-y-3 border-b border-border pb-3 sm:grid-cols-5"><Summary label={t('watch.page.registered')} value={summary.total} icon={<Activity className="h-4 w-4" />} /><Summary label={t('watch.page.enabled')} value={summary.enabled} icon={<Check className="h-4 w-4" />} /><Summary label={t('watch.page.recording')} value={summary.recording} icon={<Radio className="h-4 w-4" />} /><Summary label={t('watch.page.offline')} value={summary.offline} icon={<Clock3 className="h-4 w-4" />} /><Summary label={t('watch.page.attention')} value={summary.attention} icon={<BellRing className="h-4 w-4" />} /></div>}
    {query.isLoading ? <LoadingState label={t('watch.page.loading')} /> : query.error ? <ErrorState message={errorMessage(query.error)} retry={() => void query.refetch()} /> : items.length ? <div className="overflow-hidden rounded-lg border border-border bg-card"><div className="hidden grid-cols-[minmax(260px,1.3fr)_minmax(120px,.7fr)_minmax(170px,.9fr)_minmax(145px,.8fr)_auto] items-center gap-4 border-b border-border bg-muted/40 px-4 py-2.5 text-[10px] font-semibold text-muted-foreground md:grid"><span>Watch</span><span>{t('watch.page.state')}</span><span>{t('watch.page.currentRecording')}</span><span>{t('watch.page.nextCheck')}</span><span className="text-right">{t('watch.page.actions')}</span></div><div className="divide-y divide-border">{items.map(watch => <WatchRow key={watch.id} watch={watch} adapter={adapterById.get(watch.adapter_id)} busy={action.isPending} onAction={kind => action.mutate({ id: watch.id, kind })} />)}</div></div> : <EmptyState title={t('watch.page.empty')} />}
  </div>
}

function WatchRow({ watch, adapter, busy, onAction }: { watch: WatchView; adapter?: Adapter; busy: boolean; onAction: (kind: 'enable' | 'disable' | 'check') => void }) {
  const { t, locale } = useI18n()
  const title = watch.title || watch.resource?.display_name || adapter?.descriptor?.name || watch.adapter_name || watch.adapter_id
  const identity = <span className="min-w-0 flex-1"><span className="block truncate text-sm font-semibold" title={title}>{title}</span><span className="mt-1 block truncate text-xs text-muted-foreground">{resourceLabel(watch.resource)}</span><span className="mt-1 block text-[11px] text-muted-foreground">{t('watch.page.checkInterval', { seconds: watch.check_interval_seconds })} · {t('watch.page.preview')}: {t(watch.preview_mode === 'segment' ? 'watch.page.previewOn' : 'watch.page.previewOff')}</span></span>
  return <article className="grid min-w-0 gap-3 px-4 py-3.5 md:grid-cols-[minmax(260px,1.3fr)_minmax(120px,.7fr)_minmax(170px,.9fr)_minmax(145px,.8fr)_auto] md:items-center md:gap-4">
    <div className="flex min-w-0 items-center gap-3"><WatchPreview watch={watch} className="w-24 md:w-28" /><AdapterMark adapter={adapter} adapterId={watch.adapter_id} size="sm" />{identity}</div>
    <div className="flex items-center gap-2 md:block"><StatusBadge state={watch.enabled ? watch.state : 'disabled'} />{watch.last_error_code && <p className="mt-1 text-[10px] text-amber-700 dark:text-amber-300">{watchErrorLabel(watch.last_error_code, locale)}</p>}</div>
    <div className="min-w-0">{watch.current_recording_id ? <Link to="/recordings/$recordingId" params={{ recordingId: watch.current_recording_id }} className="block truncate text-sm font-medium text-primary hover:underline">{watch.current_recording_state ? <StatusBadge state={watch.current_recording_state} /> : t('watch.page.openRecording')}</Link> : <span className="text-xs text-muted-foreground">{t('watch.page.noneRunning')}</span>}</div>
    <div className="text-xs text-muted-foreground"><span className="block">{t('watch.page.lastCheck')} · {formatDate(watch.last_checked_at)}</span><span className="mt-1 block">{t('watch.page.next')} · {watch.enabled ? formatDate(watch.next_check_at) : t('watch.page.stopped')}</span></div>
    <div className="flex flex-wrap items-center justify-start gap-1.5 md:justify-end"><Button size="sm" variant="outline" onClick={() => onAction('check')} disabled={busy || !watch.enabled} aria-label={`${title} ${t('watch.page.check')}`}><RefreshCw className="h-3.5 w-3.5" />{t('watch.page.check')}</Button>{watch.enabled ? <Confirm trigger={<Button size="sm" variant="outline" disabled={busy}><Pause className="h-3.5 w-3.5" />{t('watch.page.stop')}</Button>} title={t('watch.page.stopTitle')} description={t('watch.page.stopDescription')} confirmLabel={t('watch.page.stopAction')} onConfirm={() => onAction('disable')} disabled={busy} /> : <Button size="sm" variant="outline" onClick={() => onAction('enable')} disabled={busy}><Play className="h-3.5 w-3.5" />{t('watch.page.enable')}</Button>}<Link to="/watches/$watchId" params={{ watchId: watch.id }}><Button size="sm" variant="ghost">{t('watch.page.details')}</Button></Link></div>
    <p className="text-[10px] text-muted-foreground md:hidden">{adapter?.descriptor?.name ?? watch.adapter_name ?? watch.adapter_id} · {t('watch.page.lastCheck')} {formatDate(watch.last_checked_at)}</p>
  </article>
}

function Summary({ label, value, icon }: { label: string; value: number; icon: ReactNode }) { return <div className="flex min-w-0 items-center justify-between gap-2"><span><span className="block truncate text-[10px] text-muted-foreground">{label}</span><span className="mt-1 block text-lg font-semibold tabular-nums">{value}</span></span><span className="text-muted-foreground">{icon}</span></div> }
