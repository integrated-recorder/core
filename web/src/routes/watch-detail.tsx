import { useRef, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Link, useNavigate, useParams } from '@tanstack/react-router'
import { ArrowLeft, Clock3, Pause, Play, RefreshCw, Settings2 } from 'lucide-react'
import { adaptersAPI, watchesAPI, type WatchMutationBody } from '@/api'
import { adaptersQuery, dashboardQuery, qk, watchEventsQuery, watchQuery, watchRecordingsQuery } from '@/api/queries'
import { AdapterMark } from '@/components/adapter/adapter-mark'
import { EmptyState, ErrorState, LoadingState } from '@/components/query-state'
import { PageHeading } from '@/components/page-heading'
import { StatusBadge } from '@/components/status-badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Confirm } from '@/components/ui/confirm'
import { WatchForm } from '@/components/watch/watch-form'
import { WatchDeleteAction } from '@/components/watch/watch-delete-action'
import { WatchPreview } from '@/components/watch/watch-preview'
import { watchErrorLabel, watchEventLabel } from '@/lib/watches'
import { errorMessage } from '@/lib/errors'
import { formatDate, formatDuration, resourceLabel } from '@/lib/utils'
import { useToast } from '@/components/ui/use-toast'
import { useI18n } from '@/i18n/provider'

export function WatchDetailPage() {
  const { watchId } = useParams({ from: '/watches/$watchId' })
  const { locale, t } = useI18n()
  const [editing, setEditing] = useState(false)
  const watchOptions = watchQuery(watchId)
  const query = useQuery({ ...watchOptions, refetchInterval: editing ? false : watchOptions.refetchInterval })
  const watch = query.data
  const adapters = useQuery(adaptersQuery)
  const recordingsOptions = watchRecordingsQuery(watchId)
  const active = !editing && (watch?.state === 'recording' || (watch?.enabled && watch.state === 'starting'))
  const recordings = useQuery({ ...recordingsOptions, refetchInterval: active ? 5_000 : false, refetchIntervalInBackground: false })
  const eventsOptions = watchEventsQuery(watchId)
  const events = useQuery({ ...eventsOptions, refetchInterval: active ? 15_000 : false, refetchIntervalInBackground: false })
  const client = useQueryClient()
  const navigate = useNavigate()
  const { toast } = useToast()
  const pendingUpdateBody = useRef<WatchMutationBody | undefined>(undefined)
  const adapter = adapters.data?.find(item => item.status.id === watch?.adapter_id)
  const schema = useQuery({ queryKey: qk.schema(watch?.adapter_id ?? '', watch?.resource), queryFn: () => adaptersAPI.schema(watch!.adapter_id, watch?.resource), enabled: Boolean(editing && watch?.adapter_id && adapter?.descriptor), retry: false, staleTime: 60_000 })
  const action = useMutation({
    mutationFn: ({ kind }: { kind: 'enable' | 'disable' | 'check' }) => watchesAPI[kind](watchId),
    onSuccess: async (_result, variables) => {
      await invalidate(client, watchId)
      toast(variables.kind === 'check' ? t('watch.detail.checkSuccess') : variables.kind === 'enable' ? t('watch.detail.enableSuccess') : t('watch.detail.disableSuccess'))
    },
    onError: () => toast(t('watch.detail.actionFailed'), t('watch.detail.retry'), 'error'),
  })
  const update = useMutation({
    mutationFn: () => {
      const body = pendingUpdateBody.current
      pendingUpdateBody.current = undefined
      if (!body) throw new Error('Watch input is unavailable')
      return watchesAPI.update(watchId, body)
    },
    onSuccess: async () => { setEditing(false); await invalidate(client, watchId); toast(t('watch.detail.updated')) },
    gcTime: 0,
    onError: () => toast(t('watch.detail.updateFailed'), t('watch.detail.updateHelp'), 'error'),
  })
  const remove = useMutation({
    mutationFn: () => watchesAPI.remove(watchId),
    onSuccess: async () => {
      await Promise.all([client.invalidateQueries({ queryKey: qk.watches }), client.invalidateQueries({ queryKey: dashboardQuery.queryKey })])
      toast(t('watch.delete.success'))
      await navigate({ to: '/watches' })
    },
    onError: () => toast(t('watch.delete.failed'), undefined, 'error'),
  })

  if (query.isLoading) return <LoadingState label={t('watch.page.loading')} />
  if (query.error || !watch) return <ErrorState message={query.error ? errorMessage(query.error) : t('watch.detail.missing')} retry={() => void query.refetch()} />
  const displayName = watch.title || watch.resource?.display_name || adapter?.descriptor?.name || watch.adapter_name || watch.adapter_id

  return <div className="page-enter">
    <PageHeading title={displayName} actions={<>
      <Link to="/watches"><Button variant="outline"><ArrowLeft className="h-4 w-4" />{t('watch.page.title')}</Button></Link>
      <Button variant="ghost" size="icon" aria-label={t('watch.page.refresh')} onClick={() => void query.refetch()}><RefreshCw className="h-4 w-4" /></Button>
      <Button variant="outline" onClick={() => setEditing(value => !value)}><Settings2 className="h-4 w-4" />{editing ? t('watch.detail.closeEdit') : t('watch.detail.edit')}</Button>
      {watch.enabled ? <Confirm trigger={<Button variant="outline" disabled={action.isPending}><Pause className="h-4 w-4" />{t('watch.detail.pause')}</Button>} title={t('watch.detail.pauseTitle')} description={t('watch.detail.pauseDescription')} confirmLabel={t('watch.detail.pause')} onConfirm={() => action.mutate({ kind: 'disable' })} disabled={action.isPending} /> : <Button variant="outline" disabled={action.isPending} onClick={() => action.mutate({ kind: 'enable' })}><Play className="h-4 w-4" />{t('watch.detail.enable')}</Button>}
      <Button variant="ghost" disabled={action.isPending || !watch.enabled} onClick={() => action.mutate({ kind: 'check' })}><RefreshCw className="h-4 w-4" />{t('watch.detail.checkNow')}</Button>
      <WatchDeleteAction onDelete={() => remove.mutate()} disabled={remove.isPending} />
    </>} />
    <div className="grid gap-4 xl:grid-cols-[minmax(0,1fr)_330px]">
      <div className="space-y-4">
        <Card><CardHeader className="flex-row items-center gap-3"><AdapterMark adapter={adapter} adapterId={watch.adapter_id} size="md" /><div className="min-w-0 flex-1"><CardTitle className="truncate">{adapter?.descriptor?.name ?? watch.adapter_name ?? watch.adapter_id}</CardTitle><p className="mt-1 truncate text-xs text-muted-foreground">{resourceLabel(watch.resource)}</p></div><StatusBadge state={watch.enabled ? watch.state : 'disabled'} /></CardHeader><CardContent>
          {watch.current_recording_id && <div className="mb-4 flex flex-wrap items-center gap-4 rounded-md bg-muted/30 p-3"><WatchPreview watch={watch} className="w-36" /><div className="min-w-0 flex-1"><p className="text-xs text-muted-foreground">{t('watch.detail.recording')}</p><p className="mt-1"><StatusBadge state={watch.current_recording_state ?? 'recording'} /></p><Link to="/recordings/$recordingId" params={{ recordingId: watch.current_recording_id }} className="mt-2 inline-block text-sm font-medium text-primary hover:underline">{t('watch.detail.openRecording')}</Link></div></div>}
          <div className="grid gap-x-5 gap-y-4 sm:grid-cols-2 lg:grid-cols-3"><Info label={t('watch.detail.checkInterval')} value={t('watch.page.checkInterval', { seconds: watch.check_interval_seconds })} /><Info label={t('watch.detail.lastCheck')} value={formatDate(watch.last_checked_at)} /><Info label={t('watch.detail.nextCheck')} value={watch.enabled ? formatDate(watch.next_check_at) : t('watch.detail.stopped')} /><Info label={t('watch.detail.preview')} value={watch.preview_mode === 'segment' ? t('watch.detail.previewOn') : t('watch.detail.previewOff')} /><Info label={t('watch.detail.created')} value={formatDate(watch.created_at)} /><Info label={t('watch.detail.updatedAt')} value={formatDate(watch.updated_at)} /></div>
          {watch.last_error_code && <p className="mt-4 rounded-md bg-amber-500/10 px-3 py-2 text-sm text-amber-800 dark:text-amber-200">{watchErrorLabel(watch.last_error_code, locale)}</p>}
        </CardContent></Card>
        {editing && <Card><CardHeader><CardTitle>{t('watch.detail.editTitle')}</CardTitle><p className="text-xs text-muted-foreground">{t('watch.detail.editSecretHelp')}</p></CardHeader><CardContent>{!adapter?.descriptor ? <EmptyState title={t('watch.detail.adapterUnavailable')} description={t('watch.detail.adapterReadyHelp')} /> : schema.isLoading ? <LoadingState /> : schema.error || !schema.data ? <ErrorState message={schema.error ? errorMessage(schema.error) : t('watch.detail.formUnavailable')} retry={() => void schema.refetch()} /> : <WatchForm key={watch.id} adapter={adapter} schema={schema.data.input_schema} watch={watch} submitLabel={update.isPending ? t('watch.detail.saving') : t('watch.detail.save')} busy={update.isPending} onSubmit={value => { pendingUpdateBody.current = value; update.mutate() }} />}</CardContent></Card>}
        <Card><CardHeader><CardTitle>{t('watch.detail.recentRecordings')}</CardTitle></CardHeader><CardContent>{recordings.isLoading ? <LoadingState /> : recordings.error ? <p role="alert" className="text-sm text-muted-foreground">{t('watch.detail.recordingsFailed')}</p> : recordings.data?.items.length ? <div className="divide-y divide-border">{recordings.data.items.map(recording => <Link key={recording.id} to="/recordings/$recordingId" params={{ recordingId: recording.id }} className="flex min-w-0 items-center gap-3 py-3"><StatusBadge state={recording.state} /><span className="min-w-0 flex-1"><span className="block truncate text-sm font-medium">{recording.title || recording.id}</span><span className="mt-1 block truncate text-xs text-muted-foreground">{formatDate(recording.started_at)} · {formatDuration(recording.duration_seconds)}</span></span><span className="shrink-0 text-xs text-muted-foreground">{t('watch.detail.segmentCount', { count: recording.segment_count ?? 0 })}</span></Link>)}</div> : <EmptyState title={t('watch.detail.noRecordings')} />}{recordings.data?.truncated && <p className="mt-3 text-xs text-muted-foreground">{t('watch.detail.truncatedHistory')}</p>}</CardContent></Card>
      </div>
      <details className="group h-fit rounded-lg border border-border bg-card p-4 xl:sticky xl:top-4">
        <summary className="flex cursor-pointer list-none items-center gap-2 font-semibold"><Clock3 className="h-4 w-4" />{t('watch.detail.history')}<span className="ml-auto text-xs font-normal text-muted-foreground">{events.data?.items.length ?? 0}</span></summary>
        <div className="mt-3 border-t border-border pt-3">{events.isLoading ? <LoadingState /> : events.error ? <p role="alert" className="text-sm text-muted-foreground">{t('watch.detail.historyFailed')}</p> : events.data?.items.length ? <ol className="max-h-[650px] divide-y divide-border overflow-auto">{events.data.items.map(event => <li key={event.id} className="flex items-start gap-3 py-3"><span className="mt-1.5 h-2 w-2 shrink-0 rounded-full bg-primary" /><div className="min-w-0 flex-1"><p className="text-sm font-medium">{watchEventLabel(event, locale)}</p>{event.state && <p className="mt-1"><StatusBadge state={event.state} /></p>}{event.recording_id && <Link to="/recordings/$recordingId" params={{ recordingId: event.recording_id }} className="mt-1 block truncate text-xs text-primary hover:underline">{t('watch.detail.recordingLink', { id: event.recording_id })}</Link>}<time className="mt-1 block text-[10px] text-muted-foreground">{formatDate(event.at)}</time></div></li>)}</ol> : <EmptyState title={t('watch.detail.noEvents')} />}</div>
      </details>
    </div>
  </div>
}

function Info({ label, value }: { label: string; value: string }) { return <div><p className="text-[11px] text-muted-foreground">{label}</p><p className="mt-1 break-all text-sm font-medium">{value}</p></div> }

async function invalidate(client: ReturnType<typeof useQueryClient>, id: string) {
  await Promise.all([
    client.invalidateQueries({ queryKey: qk.watches }),
    client.invalidateQueries({ queryKey: dashboardQuery.queryKey }),
    client.invalidateQueries({ queryKey: qk.watch(id) }),
    client.invalidateQueries({ queryKey: qk.watchRecordings(id) }),
    client.invalidateQueries({ queryKey: qk.watchEvents(id) }),
  ])
}
