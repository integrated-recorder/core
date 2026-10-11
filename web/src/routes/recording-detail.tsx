import { useEffect, useRef, useState } from 'react'
import { Link, useNavigate, useParams } from '@tanstack/react-router'
import { useInfiniteQuery, useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { ArrowDownToLine, BadgeCheck, CircleAlert, Download, FileArchive, FileCheck2, Film, Play, ShieldCheck, Tag, Trash2, XCircle } from 'lucide-react'
import { derivativeAPI, integrityAPI, recordingsAPI } from '@/api'
import { APIError } from '@/api/client'
import { adaptersQuery, exportsQuery, integrityQuery, previewsQuery, qk, recordingLifecycleQuery, recordingQuery } from '@/api/queries'
import { formatBytes, formatDate, formatDuration, resourceLabel } from '@/lib/utils'
import { formatNumber } from '@/lib/formatting'
import { PageHeading } from '@/components/page-heading'
import { RecordingPlayer } from '@/components/recording/player'
import { StatusBadge } from '@/components/status-badge'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Confirm } from '@/components/ui/confirm'
import { Input } from '@/components/ui/input'
import { LoadingState, ErrorState, EmptyState } from '@/components/query-state'
import { useToast } from '@/components/ui/use-toast'
import { isFFmpegAvailable } from '@/lib/derivatives'
import type { ArchiveEntry, DerivedJobState, ExportJob, Gap, RecordingMetadata, Segment } from '@/types/api'
import { AdapterMark } from '@/components/adapter/adapter-mark'
import { LivePreviewViewport, PreviewFrameGrid, PreviewStateMessage } from '@/components/recording/previews'
import { seekToPreview } from '@/lib/previews'
import { recordingEventLabel } from '@/lib/labels'
import { archiveIndexNextPageParam, orderRecordingEvents, recordingHasCommittedMedia, timelineProjectionOmitted } from './recording-detail-utils'
import { useI18n } from '@/i18n/provider'
import { JobProgress } from '@/components/job-progress'
import { localizedAPIError } from '@/i18n/catalog'

const activeJob = (state: string) => state === 'queued' || state === 'running'
export function RecordingDetailPage() {
  const { recordingId } = useParams({ from: '/recordings/$recordingId' }); const client = useQueryClient(); const navigate = useNavigate(); const { toast } = useToast()
  const { t, locale } = useI18n()
  const [tags, setTags] = useState<string[]>([]); const [tagInput, setTagInput] = useState(''); const [tagEditing, setTagEditing] = useState(false); const [tab, setTab] = useState<'overview' | 'archive' | 'events'>('overview'); const [jobId, setJobId] = useState<string>()
  const [activeView, setActiveView] = useState<'hls' | 'preview'>('hls')
  const recording = useQuery({ ...recordingQuery(recordingId), refetchInterval: query => query.state.data?.state === 'recording' ? 3000 : false, refetchIntervalInBackground: false }); const lifecycle = useQuery({ ...recordingLifecycleQuery(recordingId), enabled: Boolean(recording.data && recording.data.state !== 'recording' && !recording.data.archive_sealed), refetchInterval: query => query.state.data?.archive_sealed || recording.data?.archive_sealed ? false : 5000, refetchIntervalInBackground: false }); const integrity = useQuery({ ...integrityQuery(recordingId), refetchInterval: query => query.state.data?.status === 'verifying' || activeJob(query.state.data?.active_job?.state ?? '') ? 1500 : false, refetchIntervalInBackground: false }); const exports = useQuery({ ...exportsQuery(recordingId), refetchInterval: query => query.state.data?.items.some(job => activeJob(job.state)) ? 2000 : false, refetchIntervalInBackground: false })
  const previewSampling = recording.data?.state === 'recording' ? 'recent' : 'uniform'
  const preview = useQuery({ ...previewsQuery(recordingId, { sampling: previewSampling, limit: 48 }, recording.data?.state === 'recording'), enabled: recording.data?.preview?.mode === 'segment' })
  const adapters = useQuery(adaptersQuery)
  const archive = useInfiniteQuery({ queryKey: qk.archive(recordingId), queryFn: ({ pageParam }) => recordingsAPI.archive(recordingId, { limit: 100, cursor: pageParam }), initialPageParam: undefined as string | undefined, getNextPageParam: archiveIndexNextPageParam, enabled: tab === 'archive' })
  const events = useQuery({ queryKey: qk.events(recordingId), queryFn: () => recordingsAPI.events(recordingId), enabled: tab === 'events' })
  const metadata = useQuery({ queryKey: qk.metadata(recordingId), queryFn: () => recordingsAPI.metadata(recordingId), enabled: Boolean(recording.data), refetchInterval: recording.data?.state === 'recording' ? 30_000 : false, refetchIntervalInBackground: false })
  const tagsQuery = useQuery({ queryKey: qk.tags(recordingId), queryFn: () => recordingsAPI.tags(recordingId), staleTime: 30_000 })
  const playerRef = useRef<HTMLVideoElement | null>(null)
  const stop = useMutation({ mutationFn: () => recordingsAPI.stop(recordingId), onSuccess: () => { toast(t('detail.toast.stopped')); void client.invalidateQueries({ queryKey: qk.recording(recordingId) }); void client.invalidateQueries({ queryKey: qk.dashboard }) }, onError: error => toast(t('detail.toast.stopFailed'), localizedErrorMessage(error, locale), 'error') })
  const complete = useMutation({ mutationFn: () => recordingsAPI.complete(recordingId), onSuccess: () => { toast(t('detail.toast.completed')); void client.invalidateQueries({ queryKey: qk.recording(recordingId) }); void client.invalidateQueries({ queryKey: qk.lifecycle(recordingId) }); void client.invalidateQueries({ queryKey: ['recordings'] }) }, onError: error => toast(t('detail.toast.completeFailed'), localizedErrorMessage(error, locale), 'error') })
  const seal = useMutation({ mutationFn: () => recordingsAPI.seal(recordingId), onSuccess: () => { toast(t('detail.toast.sealed')); void client.invalidateQueries({ queryKey: qk.recording(recordingId) }); void client.invalidateQueries({ queryKey: qk.lifecycle(recordingId) }); void client.invalidateQueries({ queryKey: ['recordings'] }) }, onError: error => toast(t('detail.toast.sealFailed'), localizedErrorMessage(error, locale), 'error') })
  const remove = useMutation({ mutationFn: () => recordingsAPI.remove(recordingId), onSuccess: async () => { toast(t('detail.toast.deleted')); await client.invalidateQueries({ queryKey: ['recordings'] }); await navigate({ to: '/recordings' }) }, onError: error => toast(t('detail.toast.deleteFailed'), localizedErrorMessage(error, locale), 'error') })
  const saveTags = useMutation({ mutationFn: () => recordingsAPI.setTags(recordingId, tagInput.split(',').map(x => x.trim()).filter(Boolean)), onSuccess: result => { setTags(result.tags); setTagInput(result.tags.join(', ')); setTagEditing(false); toast(t('detail.toast.tagsSaved')); void client.invalidateQueries({ queryKey: ['recordings'] }); void client.invalidateQueries({ queryKey: qk.tags(recordingId) }) }, onError: error => toast(t('detail.toast.tagsFailed'), localizedErrorMessage(error, locale), 'error') })
  const verify = useMutation({ mutationFn: () => integrityAPI.start(recordingId), onSuccess: job => { setJobId(job.id); toast(t('detail.toast.verifyStarted')); void client.invalidateQueries({ queryKey: qk.integrity(recordingId) }) }, onError: error => toast(t('detail.toast.verifyFailed'), localizedErrorMessage(error, locale), 'error') })
  const exportStart = useMutation({ mutationFn: () => derivativeAPI.create(recordingId), onSuccess: () => { toast(t('detail.toast.exportStarted')); void client.invalidateQueries({ queryKey: qk.exports(recordingId) }) }, onError: error => toast(t('detail.toast.exportFailed'), localizedErrorMessage(error, locale), 'error') })
  const enablePreviews = useMutation({ mutationFn: () => recordingsAPI.enablePreviews(recordingId), onSuccess: async () => { toast(t('detail.toast.previewStarted')); await client.invalidateQueries({ queryKey: qk.recording(recordingId) }); void client.invalidateQueries({ queryKey: ['recording', recordingId, 'previews'] }); void client.invalidateQueries({ queryKey: ['recordings'] }); void client.invalidateQueries({ queryKey: qk.dashboard }) }, onError: error => toast(t('detail.toast.previewFailed'), localizedErrorMessage(error, locale), 'error') })
  const removeExport = useMutation({ mutationFn: derivativeAPI.remove, onSuccess: (_result, id) => { const wasActive = exports.data?.items.some(item => item.id === id && activeJob(item.state)); toast(wasActive ? t('detail.toast.exportCancelled') : t('detail.toast.exportDeleted')); void client.invalidateQueries({ queryKey: qk.exports(recordingId) }) }, onError: error => toast(t('detail.toast.exportChangeFailed'), localizedErrorMessage(error, locale), 'error') })
  const job = useQuery({ queryKey: ['integrity-job', jobId], queryFn: () => integrityAPI.job(jobId!), enabled: Boolean(jobId), refetchInterval: query => query.state.data && activeJob(query.state.data.state) ? 1500 : false, refetchIntervalInBackground: false, staleTime: 0 })
  const cancelJob = useMutation({ mutationFn: () => integrityAPI.cancel(jobId!), onSuccess: () => { toast(t('detail.toast.verifyCancelled')); void client.invalidateQueries({ queryKey: ['integrity-job', jobId] }) } })
  useEffect(() => { if (tagsQuery.data && !tagEditing) { setTags(tagsQuery.data.tags); setTagInput(tagsQuery.data.tags.join(', ')) } }, [tagsQuery.data, tagEditing])
  const activeIntegrityJobID = integrity.data?.active_job?.id
  const activeIntegrityJobState = integrity.data?.active_job?.state
  useEffect(() => { if (activeIntegrityJobID && activeIntegrityJobState && activeJob(activeIntegrityJobState)) setJobId(activeIntegrityJobID) }, [activeIntegrityJobID, activeIntegrityJobState])
  useEffect(() => { setActiveView('hls') }, [recordingId])
  if (recording.isLoading) return <LoadingState label={t('common.loading')} />
  if (recording.error || !recording.data) return <ErrorState title={t('common.loadFailed')} retryLabel={t('common.retry')} message={recording.error ? localizedErrorMessage(recording.error, locale) : t('detail.notFound')} retry={() => void recording.refetch()} />
  const item = recording.data; const stats = item.statistics ?? {}; const trackValues = Object.values(item.tracks ?? {}); const ffmpegAvailable = isFFmpegAvailable(exports.data); const hasCommittedMedia = recordingHasCommittedMedia(item); const segments = Object.entries(item.tracks ?? {}).flatMap(([trackId, track]) => (track.segments ?? []).map(segment => ({ ...segment, trackId }))).sort((a, b) => a.trackId.localeCompare(b.trackId) || (a.archive_ordinal ?? a.sequence ?? 0) - (b.archive_ordinal ?? b.sequence ?? 0))
  const gaps = item.gaps ?? []; const integrityState = integrity.data?.status ?? stats.integrity ?? 'unknown'
  const currentAdapter = adapters.data?.find(adapter => adapter.status.id === item.adapter_id)
  const selectActivePreview = () => {
    setActiveView('preview')
    if (item.preview?.mode !== 'segment' && item.preview?.state !== 'unavailable' && !enablePreviews.isPending) enablePreviews.mutate()
  }
  return <div className="page-enter"><PageHeading eyebrow={t('detail.eyebrow')} title={item.title || t('detail.titleFallback')} description={`${item.id} · ${item.adapter?.name ?? item.adapter_id ?? t('detail.adapterUnknown')} · ${resourceLabel(item.resource)}`} actions={<><Link to="/recordings"><Button variant="outline" size="sm"><ArrowDownToLine className="h-4 w-4 rotate-180" />{t('detail.back')}</Button></Link>{item.state === 'recording' && <Confirm trigger={<Button size="sm" variant="outline"><XCircle className="h-4 w-4" />{t('detail.stop')}</Button>} title={t('detail.stopTitle')} description={t('detail.stopDescription')} confirmLabel={t('detail.stop')} onConfirm={() => stop.mutate()} disabled={stop.isPending} />}{item.state === 'stopped' && <Button size="sm" variant="outline" onClick={() => complete.mutate()} disabled={complete.isPending}>{t('detail.complete')}</Button>}{item.state !== 'recording' && !item.archive_sealed && !lifecycle.data?.archive_sealed && <Confirm trigger={<Button size="sm" variant="outline">{t('detail.seal')}</Button>} title={t('detail.sealTitle')} description={t('detail.sealDescription')} confirmLabel={t('detail.sealAction')} destructive onConfirm={() => seal.mutate()} disabled={seal.isPending} />}<Button size="sm" variant="outline" onClick={() => document.getElementById('vod-player')?.scrollIntoView({ behavior: 'smooth' })} disabled={item.state === 'recording' && !hasCommittedMedia}><Play className="h-4 w-4" />{t('detail.play')}</Button><Confirm trigger={<Button size="sm" variant="destructive" disabled={remove.isPending || item.state === 'recording'}><Trash2 className="h-4 w-4" />{t('detail.delete')}</Button>} title={t('detail.deleteTitle')} description={t('detail.deleteDescription')} confirmLabel={t('detail.deleteAction')} destructive onConfirm={() => remove.mutate()} disabled={remove.isPending || item.state === 'recording'} /></>} />
    <div className="mb-5 flex flex-wrap items-center gap-2"><AdapterMark adapter={currentAdapter} adapterId={item.adapter_id} name={item.adapter?.name} showName size="sm" /><StatusBadge state={item.state} /><Badge tone={item.archive_sealed || lifecycle.data?.archive_sealed ? 'neutral' : 'green'}>{item.archive_sealed || lifecycle.data?.archive_sealed ? t('detail.archiveSealed') : t('detail.repairable')}</Badge><Badge tone="neutral">{formatDate(item.started_at)}</Badge>{item.stopped_at && <Badge tone="neutral">{t('detail.ended', { date: formatDate(item.stopped_at) })}</Badge>}<Badge tone={item.source_uri_classification === 'sensitive' ? 'amber' : 'neutral'}>{t('detail.originalURI')} · {item.source_uri_classification === 'sensitive' ? t('detail.sensitiveURI') : item.source_uri_classification ?? t('detail.unclassified')}</Badge></div>
    <div className="grid gap-4 xl:grid-cols-[minmax(0,1.6fr)_minmax(300px,.8fr)]">
      <div id="vod-player">
        <Card>
          <CardHeader className="flex-row items-center justify-between">
            <div>
              <CardTitle>{item.state === 'recording' ? t('detail.player.liveTitle') : t('detail.player.vodTitle')}</CardTitle>
              <p className="mt-1 text-xs text-muted-foreground">{item.state === 'recording' ? t('detail.player.liveDescription') : t('detail.player.vodDescription')}</p>
            </div>
            <Badge tone="blue">{item.state === 'recording' ? t('detail.player.liveBadge') : t('detail.player.vodBadge')}</Badge>
          </CardHeader>
          <CardContent className="space-y-3">
            {item.state === 'recording' && <div role="group" aria-label={t('detail.player.viewMode')} className="flex flex-wrap gap-2">
              <Button size="sm" variant={activeView === 'hls' ? 'default' : 'outline'} aria-pressed={activeView === 'hls'} onClick={() => setActiveView('hls')}>{t('detail.player.liveHls')}</Button>
              <Button size="sm" variant={activeView === 'preview' ? 'default' : 'outline'} aria-pressed={activeView === 'preview'} onClick={selectActivePreview}>{t('detail.player.currentPreview')}</Button>
            </div>}
            {item.state !== 'recording' || activeView === 'hls'
              ? <RecordingPlayer recordingId={recordingId} active={item.state === 'recording'} hasCommittedSegments={hasCommittedMedia} timelineRevision={item.state === 'recording' ? undefined : lifecycle.data?.timeline_revision ?? item.timeline_revision} onVideoRef={video => { playerRef.current = video }} />
              : <div className="space-y-2">
                <LivePreviewViewport recordingId={recordingId} summary={item.preview} adapter={currentAdapter} adapterId={item.adapter_id} adapterName={item.adapter?.name} />
                <div className="flex flex-wrap items-center justify-between gap-2">
                  <PreviewStateMessage state={item.preview?.state} />
                  {item.preview?.mode !== 'segment' && item.preview?.state !== 'unavailable' && <Button size="sm" variant="outline" onClick={() => enablePreviews.mutate()} disabled={enablePreviews.isPending}>{enablePreviews.isPending ? t('detail.requestPending') : t('detail.enablePreview')}</Button>}
                </div>
                {item.preview?.latest_archive_ordinal != null && <p className="text-xs text-muted-foreground">{t('detail.latestArchiveOrdinal', { ordinal: formatNumber(item.preview.latest_archive_ordinal) })}</p>}
              </div>}
          </CardContent>
        </Card>
      </div>
      <Card>
        <CardHeader>
          <CardTitle>{t('detail.summary.title')}</CardTitle>
          <p className="mt-1 text-xs text-muted-foreground">{t('detail.summary.description')}</p>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-2 gap-x-5 gap-y-4">
            {[
              [t('detail.summary.duration'), formatDuration(stats.duration_seconds)],
              [t('detail.summary.archiveSize'), formatBytes(stats.archive_size_bytes)],
              [t('detail.summary.mediaPayload'), formatBytes(stats.media_payload_size_bytes)],
              [t('detail.summary.initData'), formatBytes(stats.init_payload_size_bytes)],
              [t('detail.summary.manifestData'), formatBytes(stats.manifest_size_bytes)],
              [t('detail.summary.mediaSegments'), stats.segment_count ?? segments.length],
              [t('detail.summary.initSegments'), stats.init_segment_count ?? trackValues.reduce((total, track) => total + (track.init_segments?.length ?? 0), 0)],
              [t('detail.summary.manifestSnapshots'), stats.manifest_snapshot_count ?? item.manifest_snapshots?.length ?? 0],
              [t('detail.summary.gaps'), stats.gap_count ?? item.gaps?.length ?? 0],
              [t('detail.summary.gapDuration'), stats.gap_duration_seconds == null ? t('detail.notReported') : formatDuration(stats.gap_duration_seconds)],
            ].map(([label, value]) => <Stat key={String(label)} label={String(label)} value={String(value)} />)}
          </div>
          <div className="mt-5 border-t border-border pt-4">
            <div className="flex items-center justify-between"><span className="text-xs text-muted-foreground">{t('detail.integrity')}</span><StatusBadge state={integrityState} /></div>
            <p className="mt-2 text-xs text-muted-foreground">{t('detail.lastCheck', { date: formatDate(integrity.data?.last_verified_at) })}</p>
            {integrity.data?.freshness === 'stale' && <p role="status" className="mt-1 text-xs text-amber-700 dark:text-amber-300">{t('detail.integrityStale')}</p>}
            {integrity.data?.freshness === 'unknown' && integrity.data?.last_verified_at && <p role="status" className="mt-1 text-xs text-muted-foreground">{t('detail.integrityUnknown')}</p>}
            <p className="mt-1 text-xs text-muted-foreground">{t('detail.integrityCounts', { verified: formatNumber(integrity.data?.objects_verified ?? 0), missing: formatNumber(integrity.data?.objects_missing ?? 0), corrupt: formatNumber(integrity.data?.objects_corrupt ?? 0) })}</p>
            <div className="mt-3 flex flex-wrap gap-2">
              <Button size="sm" onClick={() => verify.mutate()} disabled={verify.isPending || item.state === 'recording'}><ShieldCheck className="h-4 w-4" />{t('detail.verify')}</Button>
              {job.data && activeJob(job.data.state) && <Button variant="outline" size="sm" onClick={() => cancelJob.mutate()}>{t('detail.cancelVerify')}</Button>}
            </div>
            {(job.data && isTerminalJob(job.data.state) || integrity.data?.active_job && isTerminalJob(integrity.data.active_job.state)) && <div className="mt-2 space-y-1" role="status"><StatusBadge state={job.data?.state ?? integrity.data?.active_job?.state} /><JobTerminalMessage state={job.data?.state ?? integrity.data?.active_job?.state} errorCode={job.data?.error_code ?? integrity.data?.active_job?.error_code} /></div>}
            {(job.data && activeJob(job.data.state) || integrity.data?.active_job && activeJob(integrity.data.active_job.state)) && <JobProgress label={t('progress.integrity')} progress={job.data?.progress ?? integrity.data?.active_job?.progress} />}
            {job.data?.freshness === 'stale' && <p role="status" className="mt-2 text-xs text-amber-700 dark:text-amber-300">{t('progress.freshness.stale')}</p>}
          </div>
        </CardContent>
      </Card>
    </div>
    <section className="mt-4" aria-labelledby="source-metadata-heading"><MetadataTimeline data={metadata.data} loading={metadata.isLoading} error={metadata.error} retry={() => void metadata.refetch()} /></section>
    {item.state !== 'recording' && <section className="mt-4" aria-labelledby="preview-heading"><Card><CardHeader className="flex-row items-start justify-between gap-3"><div><CardTitle id="preview-heading">{t('detail.preview.title')}</CardTitle><p className="mt-1 text-xs text-muted-foreground">{t('detail.preview.description')}</p></div>{item.preview?.mode !== 'segment' && item.preview?.state !== 'unavailable' && <Button size="sm" onClick={() => enablePreviews.mutate()} disabled={enablePreviews.isPending}><Film className="h-4 w-4" />{enablePreviews.isPending ? t('detail.requestPending') : t('detail.enablePreview')}</Button>}</CardHeader><CardContent className="space-y-4">{item.preview?.state === 'unavailable' ? <PreviewStateMessage state="unavailable" /> : item.preview?.mode !== 'segment' ? <div className="space-y-2"><PreviewStateMessage state={item.preview?.state ?? 'disabled'} /><p className="text-xs text-muted-foreground">{t('detail.preview.background')}</p></div> : <><div className="flex flex-wrap items-center justify-between gap-2"><PreviewStateMessage state={preview.data?.state ?? item.preview.state} /><span className="text-xs tabular-nums text-muted-foreground">{t('detail.preview.frameCount', { count: formatNumber(preview.data?.items.length ?? 0) })}</span></div>{preview.error ? <p role="status" className="text-xs text-muted-foreground">{t('detail.preview.loadFailed')}</p> : preview.isLoading ? <p role="status" className="text-xs text-muted-foreground">{t('detail.preview.loading')}</p> : preview.data?.items.length ? <PreviewFrameGrid items={preview.data.items} recordingId={recordingId} onSeek={seconds => { seekToPreview(playerRef.current, seconds) }} /> : <p className="text-xs text-muted-foreground">{t('detail.preview.empty')}</p>}</>}</CardContent></Card></section>}
    <Card className="mt-4"><CardHeader className="flex-row items-center justify-between"><div><CardTitle>{t('detail.timeline.title')}</CardTitle><p className="mt-1 text-xs text-muted-foreground">{t('detail.timeline.description')}</p></div><span className="text-xs text-muted-foreground">{t('detail.timeline.summary', { segments: formatNumber(stats.segment_count ?? item.segment_count ?? segments.length), gaps: formatNumber(stats.gap_count ?? gaps.length) })}</span></CardHeader><CardContent>{timelineProjectionOmitted(item, segments.length, gaps.length) ? <p role="status" className="text-sm text-muted-foreground">{t('detail.timeline.omitted')}</p> : <Timeline segments={segments} gaps={gaps} />}</CardContent></Card>
    <div className="mt-4 grid gap-4 xl:grid-cols-[1fr_1fr]"><Card><CardHeader className="flex-row items-center justify-between"><div><CardTitle>{t('detail.tags.title')}</CardTitle><p className="mt-1 text-xs text-muted-foreground">{t('detail.tags.description')}</p></div><Tag className="h-4 w-4 text-muted-foreground" /></CardHeader><CardContent>{tagEditing ? <div className="flex gap-2"><Input aria-label={t('detail.tags.label')} value={tagInput} onChange={event => setTagInput(event.target.value)} placeholder="concert, important" maxLength={512} /><Button onClick={() => saveTags.mutate()} disabled={saveTags.isPending}>{t('detail.tags.save')}</Button><Button variant="outline" onClick={() => { setTagInput(tags.join(', ')); setTagEditing(false) }}>{t('detail.tags.cancel')}</Button></div> : <div className="flex min-h-10 flex-wrap items-center gap-2">{tags.length ? tags.map(tag => <Badge key={tag}>{tag}</Badge>) : <span className="text-sm text-muted-foreground">{t('detail.tags.empty')}</span>}<Button variant="ghost" size="sm" className="ml-auto" onClick={() => { setTagInput(tags.join(', ')); setTagEditing(true) }}>{t('detail.tags.edit')}</Button></div>}</CardContent></Card><Card><CardHeader className="flex-row items-center justify-between"><div><CardTitle>{t('detail.derivatives.title')}</CardTitle><p className="mt-1 text-xs text-muted-foreground">{t('detail.derivatives.description')}</p></div><FileArchive className="h-4 w-4 text-muted-foreground" /></CardHeader><CardContent><div className="flex flex-wrap items-center justify-between gap-3"><div><p className="text-sm font-medium">{t('detail.derivatives.name')}</p><p className="text-xs text-muted-foreground">{exports.isLoading ? t('detail.derivatives.checking') : ffmpegAvailable ? t('detail.derivatives.copyMode') : t('detail.derivatives.unavailable')}</p></div><Button size="sm" onClick={() => exportStart.mutate()} disabled={!ffmpegAvailable || exportStart.isPending || item.state === 'recording'}><FileArchive className="h-4 w-4" />{t('detail.derivatives.start')}</Button></div><div className="mt-3 divide-y divide-border">{exports.data?.items.map(job => <ExportRow key={job.id} item={job} onDelete={() => removeExport.mutate(job.id)} deleting={removeExport.isPending} />)}{exports.data && !exports.data.items.length && <p className="py-3 text-xs text-muted-foreground">{t('detail.derivatives.empty')}</p>}</div></CardContent></Card></div>
    <Card className="mt-4"><div className="flex gap-1 border-b border-border px-4 pt-3" role="tablist" aria-label={t('detail.tabs.label')}>{(['overview', 'archive', 'events'] as const).map(key => <button key={key} id={`recording-tab-${key}`} aria-controls={`recording-panel-${key}`} role="tab" aria-selected={tab === key} tabIndex={tab === key ? 0 : -1} className={`focus-ring rounded-t-md px-3 py-2 text-sm ${tab === key ? 'border-b-2 border-primary font-semibold text-primary' : 'text-muted-foreground hover:text-foreground'}`} onClick={() => setTab(key)}>{key === 'overview' ? t('detail.tabs.overview') : key === 'archive' ? t('detail.tabs.archive') : t('timeline.events')}</button>)}</div><CardContent id={`recording-panel-${tab}`} role="tabpanel" aria-labelledby={`recording-tab-${tab}`} tabIndex={0} className="pt-4">{tab === 'overview' && <div className="grid gap-4 sm:grid-cols-2"><Info label={t('detail.info.adapter')} value={item.adapter ? `${item.adapter.name ?? item.adapter.id} · v${item.adapter.version} · protocol v${item.adapter.protocol_version}` : t('detail.info.adapterMissing')} /><Info label={t('detail.info.resource')} value={resourceLabel(item.resource)} /><Info label={t('detail.info.recordingID')} value={item.id} mono /><Info label={t('detail.info.uriClassification')} value={item.source_uri_classification ?? t('detail.info.uriRedacted')} /></div>}{tab === 'archive' && <ArchiveIndex entries={archive.data?.pages.flatMap(page => page.entries)} loading={archive.isLoading} loadingMore={archive.isFetchingNextPage} hasMore={archive.hasNextPage} loadMore={() => void archive.fetchNextPage()} error={archive.error} retry={() => void archive.refetch()} />}{tab === 'events' && <Events items={events.data?.items} loading={events.isLoading} error={events.error} retry={() => void events.refetch()} />}</CardContent></Card>
    <div className="mt-4"><Card><CardHeader><CardTitle>{t('detail.preservation.title')}</CardTitle></CardHeader><CardContent className="space-y-3 text-xs leading-5 text-muted-foreground"><p className="flex gap-2"><BadgeCheck className="mt-0.5 h-4 w-4 shrink-0 text-emerald-600" />{t('detail.preservation.media')}</p><p className="flex gap-2"><FileCheck2 className="mt-0.5 h-4 w-4 shrink-0 text-primary" />{t('detail.preservation.objects')}</p><p className="flex gap-2"><CircleAlert className="mt-0.5 h-4 w-4 shrink-0 text-amber-600" />{t('detail.preservation.uri')}</p></CardContent></Card></div>
  </div>
}

export function MetadataTimeline({ data, loading, error, retry }: { data?: RecordingMetadata; loading: boolean; error: unknown; retry: () => void }) {
  const { t, locale } = useI18n()
  const valueLabel = (value?: string | null) => value == null ? t('metadata.unknown') : value === '' ? t('metadata.empty') : value
  return <Card><CardHeader><CardTitle id="source-metadata-heading">{t('metadata.title')}</CardTitle><p className="mt-1 text-xs text-muted-foreground">{t('metadata.description')}</p></CardHeader><CardContent className="space-y-4">
    {loading && !data ? <p role="status" className="text-sm text-muted-foreground">{t('metadata.loading')}</p> : error ? <ErrorState title={t('common.loadFailed')} retryLabel={t('common.retry')} message={localizedErrorMessage(error, locale)} retry={retry} /> : !data?.current ? <p className="text-sm text-muted-foreground">{t('metadata.emptyState')}</p> : <>
      <div className="grid gap-4 sm:grid-cols-2"><MetadataField label={t('metadata.currentTitle')} value={valueLabel(data.current.title)} /><MetadataField label={t('metadata.currentDescription')} value={valueLabel(data.current.description)} /></div>
      <div className="flex flex-wrap gap-x-5 gap-y-1 text-xs text-muted-foreground"><span>{t('metadata.lastObserved', { date: formatDate(data.current.observed_at) })}</span>{data.current.source_updated_at && <span>{t('metadata.sourceUpdated', { date: formatDate(data.current.source_updated_at) })}</span>}</div>
      {data.items.length > 1 && <div className="border-t border-border pt-4"><h3 className="mb-3 text-sm font-semibold">{t('metadata.history')}</h3><ol className="space-y-4">{data.items.map((revision, index) => <li key={`${revision.observed_at}-${index}`} className="border-l-2 border-primary/30 pl-3"><time className="text-xs text-muted-foreground">{formatDate(revision.observed_at)}</time><div className="mt-1 grid gap-2 sm:grid-cols-2"><MetadataField label={t('metadata.titleField')} value={valueLabel(revision.title)} /><MetadataField label={t('metadata.descriptionField')} value={valueLabel(revision.description)} /></div>{revision.source_updated_at && <p className="mt-1 text-[11px] text-muted-foreground">{t('metadata.sourceUpdated', { date: formatDate(revision.source_updated_at) })}</p>}</li>)}</ol></div>}
      {data.truncated && <p role="status" className="text-xs text-amber-700 dark:text-amber-300">{t('metadata.truncated')}</p>}
    </>}
  </CardContent></Card>
}
function MetadataField({ label, value }: { label: string; value: string }) { return <div className="min-w-0"><p className="text-[11px] text-muted-foreground">{label}</p><p className="mt-1 whitespace-pre-wrap break-words text-sm">{value}</p></div> }
function Stat({ label, value }: { label: string; value: string | number }) { return <div><p className="text-[11px] text-muted-foreground">{label}</p><p className="mt-1 text-sm font-semibold tabular-nums">{value}</p></div> }
function Info({ label, value, mono }: { label: string; value: string; mono?: boolean }) { return <div><p className="text-[11px] text-muted-foreground">{label}</p><p className={`mt-1 break-all text-sm ${mono ? 'font-mono text-xs' : 'font-medium'}`}>{value}</p></div> }
export function Timeline({ segments, gaps }: { segments: (Segment & { trackId: string })[]; gaps: Gap[] }) {
  const { t } = useI18n()
  if (!segments.length && !gaps.length) return <EmptyState title={t('timeline.capture.empty')} />
  const trackIds = [...new Set([...segments.map(item => item.trackId), ...gaps.map(item => item.track_id ?? t('timeline.capture.unknownTrack'))])].sort()
  return <div className="space-y-5">{trackIds.map(trackId => {
    const trackSegments = segments.filter(item => item.trackId === trackId)
    const trackGaps = gaps.filter(item => (item.track_id ?? t('timeline.capture.unknownTrack')) === trackId)
    const entries = [
      ...trackSegments.map(item => ({ kind: 'segment' as const, epoch: item.source_epoch ?? 0, sequence: item.sequence ?? 0, end: item.sequence ?? 0, item })),
      ...trackGaps.map(item => ({ kind: 'gap' as const, epoch: item.source_epoch ?? 0, sequence: item.from_sequence ?? 0, end: item.to_sequence ?? item.from_sequence ?? 0, item })),
    ].sort((a, b) => a.epoch - b.epoch || a.sequence - b.sequence || (a.kind === 'gap' ? -1 : 1))
    return <section key={trackId} aria-label={t('timeline.capture.trackAria', { track: trackId })}>
      <div className="mb-2 flex items-center justify-between gap-2"><h3 className="truncate text-xs font-semibold">{t('timeline.capture.track', { track: trackId })}</h3><span className="shrink-0 text-[11px] text-muted-foreground">{t('timeline.capture.trackSummary', { segments: formatNumber(trackSegments.length), gaps: formatNumber(trackGaps.length) })}</span></div>
      <div className="flex h-8 overflow-hidden rounded-md bg-muted" role="img" aria-label={t('timeline.capture.imageAria', { segments: formatNumber(trackSegments.length), gaps: formatNumber(trackGaps.length) })}>
        {entries.map((entry, index) => <span key={`${entry.kind}-${entry.epoch}-${entry.sequence}-${index}`} className={`min-w-[3px] ${entry.kind === 'gap' ? 'bg-amber-500' : 'bg-blue-500'}`} style={{ flexGrow: entry.kind === 'gap' ? Math.min(1000, Math.max(1, entry.end - entry.sequence + 1)) : 1, flexBasis: 0 }} title={entry.kind === 'gap' ? t('timeline.capture.gapMarker', { epoch: entry.epoch, start: entry.sequence, end: entry.end }) : t('timeline.capture.segmentMarker', { epoch: entry.epoch, sequence: entry.sequence, discontinuity: entry.item.discontinuity ? t('timeline.capture.discontinuitySuffix') : '' })} />)}
      </div>
      <div className="mt-2 flex flex-wrap gap-x-4 gap-y-1 text-[10px] text-muted-foreground"><span className="flex items-center gap-1.5"><i className="h-2 w-2 rounded-sm bg-blue-500" />{t('timeline.capture.stored')}</span><span className="flex items-center gap-1.5"><i className="h-2 w-2 rounded-sm bg-amber-500" />{t('timeline.capture.recordedGap')}</span>{trackSegments.some(item => item.discontinuity) && <span>{t('timeline.capture.discontinuityCount', { count: formatNumber(trackSegments.filter(item => item.discontinuity).length) })}</span>}</div>
      <div className="mt-3 max-h-48 overflow-y-auto rounded-md border border-border"><table className="w-full text-left text-xs"><thead className="sticky top-0 bg-muted"><tr><th className="px-3 py-2 font-medium">{t('timeline.capture.archiveOrdinal')}</th><th className="px-3 py-2 font-medium">{t('timeline.capture.sourceCoordinate')}</th><th className="px-3 py-2 font-medium">{t('timeline.capture.duration')}</th><th className="px-3 py-2 font-medium">{t('timeline.capture.status')}</th></tr></thead><tbody>{trackSegments.map((item, index) => <tr key={`${trackId}-${item.archive_ordinal ?? index}`} className="border-t border-border"><td className="px-3 py-2 font-mono">{item.archive_ordinal == null ? '—' : formatNumber(item.archive_ordinal)}</td><td className="px-3 py-2 font-mono">{formatNumber(item.source_epoch ?? 0)} / {item.sequence == null ? '—' : formatNumber(item.sequence)}</td><td className="px-3 py-2">{item.duration == null ? '—' : formatDuration(item.duration)}</td><td className="px-3 py-2">{item.discontinuity ? <Badge tone="amber">{t('timeline.capture.discontinuity')}</Badge> : <Badge tone="green">{t('timeline.capture.present')}</Badge>}</td></tr>)}</tbody></table></div>
      {trackGaps.length > 0 && <ul className="mt-2 space-y-1 text-[11px] text-amber-700 dark:text-amber-300">{trackGaps.map((gap, index) => <li key={`${gap.source_epoch ?? 0}-${gap.from_sequence ?? 0}-${index}`}>{t('timeline.capture.gapLine', { epoch: formatNumber(gap.source_epoch ?? 0), start: gap.from_sequence == null ? '—' : formatNumber(gap.from_sequence), end: gap.to_sequence == null ? gap.from_sequence == null ? '—' : formatNumber(gap.from_sequence) : formatNumber(gap.to_sequence), reason: gap.reason ? t('timeline.capture.reasonSuffix', { reason: gap.reason }) : '' })}</li>)}</ul>}
    </section>
  })}</div>
}
function ArchiveIndex({ entries, loading, loadingMore, hasMore, loadMore, error, retry }: { entries?: ArchiveEntry[]; loading: boolean; loadingMore: boolean; hasMore: boolean; loadMore: () => void; error: unknown; retry: () => void }) {
  const { t, locale } = useI18n()
  if (loading) return <LoadingState label={t('common.loading')} />
  if (error) return <ErrorState title={t('common.loadFailed')} retryLabel={t('common.retry')} message={localizedErrorMessage(error, locale)} retry={retry} />
  if (!entries?.length) return <EmptyState title={t('archive.empty')} />
  return <div><div className="overflow-x-auto"><table className="w-full min-w-[560px] text-left text-xs"><thead><tr className="border-b border-border text-muted-foreground"><th className="pb-2">{t('archive.kind')}</th><th className="pb-2">{t('archive.path')}</th><th className="pb-2 text-right">{t('archive.size')}</th><th className="pb-2">{t('archive.sha256')}</th></tr></thead><tbody>{entries.map(entry => <tr key={entry.path} className="border-b border-border/60 last:border-0"><td className="py-2"><Badge>{entry.kind}</Badge></td><td className="py-2 font-mono">{entry.path}</td><td className="py-2 text-right tabular-nums">{formatBytes(entry.size)}</td><td className="max-w-56 truncate py-2 font-mono text-[10px]">{entry.sha256 ?? '—'}</td></tr>)}</tbody></table></div>{hasMore && <div className="pt-3"><Button variant="outline" size="sm" onClick={loadMore} disabled={loadingMore}>{loadingMore ? t('common.loading') : t('archive.more')}</Button></div>}</div>
}
export function Events({ items, loading, error, retry }: { items?: { id: string; type: string; at: string; count?: number; message?: string }[]; loading: boolean; error: unknown; retry: () => void }) {
  const { t, locale } = useI18n()
  if (loading) return <LoadingState label={t('common.loading')} />
  if (error) return <ErrorState title={t('common.loadFailed')} retryLabel={t('common.retry')} message={localizedErrorMessage(error, locale)} retry={retry} />
  if (!items?.length) return <EmptyState title={t('events.empty')} />
  const ordered = orderRecordingEvents(items)
  return <ol className="space-y-4" aria-label={t('timeline.events')}>
    {ordered.map((event, index) => <li key={event.id} className="relative min-h-8 pl-7">
      <span aria-hidden="true" className="absolute left-[3px] top-1.5 z-10 h-2 w-2 rounded-full bg-primary ring-2 ring-background" />
      {index < ordered.length - 1 && <span aria-hidden="true" className="absolute bottom-[-1rem] left-[6.5px] top-4 w-px bg-border" />}
      <div className="min-w-0"><div className="flex flex-wrap items-center justify-between gap-2"><span className="text-sm font-medium">{recordingEventLabel(event.type, locale)}</span><time className="text-[11px] text-muted-foreground">{formatDate(event.at)}</time></div>{event.count != null && <p className="mt-1 text-xs text-muted-foreground">{t('timeline.segmentCount', { count: formatNumber(event.count, {}, locale) })}</p>}</div>
    </li>)}
  </ol>
}
export function ExportRow({ item, onDelete, deleting }: { item: ExportJob; onDelete: () => void; deleting: boolean }) {
  const { t } = useI18n()
  const terminal = isTerminalJob(item.state)
  return <div className="py-3"><div className="flex flex-wrap items-center gap-2"><StatusBadge state={item.state} />{item.freshness === 'stale' && <Badge tone="amber">{t('export.stale')}</Badge>}{item.freshness === 'unknown' && item.state === 'completed' && <Badge tone="neutral">{t('export.freshnessUnknown')}</Badge>}<span className="min-w-0 flex-1 truncate text-xs">{item.output_name ?? item.id}</span>{item.state === 'completed' && <a href={derivativeAPI.download(item.id)}><Button variant="outline" size="sm"><Download className="h-3.5 w-3.5" />{t('export.download')}</Button></a>}{activeJob(item.state) ? <Button variant="outline" size="sm" onClick={onDelete} disabled={deleting} aria-label={t('export.cancelLabel')}><XCircle className="h-3.5 w-3.5" />{t('export.cancel')}</Button> : <Confirm trigger={<Button variant="ghost" size="icon" aria-label={t('export.deleteLabel')} disabled={deleting}><Trash2 className="h-4 w-4" /></Button>} title={t('export.deleteTitle')} description={t('export.deleteDescription')} confirmLabel={t('export.deleteAction')} destructive onConfirm={onDelete} disabled={deleting} />}</div>{activeJob(item.state) && <JobProgress label={t('progress.export')} progress={item.progress} />}{terminal && <JobTerminalMessage state={item.state} errorCode={item.error_code} />}</div>
}

function isTerminalJob(state?: DerivedJobState) { return state === 'completed' || state === 'failed' || state === 'canceled' }
export function JobTerminalMessage({ state, errorCode }: { state?: DerivedJobState; errorCode?: string }) {
  const { t } = useI18n()
  if (state === 'failed' && errorCode === 'interrupted_by_restart') return <p className="mt-1 text-xs text-muted-foreground">{t('detail.job.interrupted')}</p>
  if (state === 'failed') return <JobErrorMessage errorCode={errorCode} />
  if (state === 'canceled') return <p className="mt-1 text-xs text-muted-foreground">{t('detail.job.cancelled')}</p>
  return null
}
function JobErrorMessage({ errorCode }: { errorCode?: string }) {
  const { locale } = useI18n()
  return <p className="mt-1 text-xs text-destructive">{localizedAPIError(locale, errorCode, 500)}</p>
}

function localizedErrorMessage(error: unknown, locale: Parameters<typeof localizedAPIError>[0]) {
  if (error instanceof APIError) return localizedAPIError(locale, error.errorCode, error.status)
  return localizedAPIError(locale)
}
