import { useEffect, useRef, useState } from 'react'
import { Link, useNavigate, useParams } from '@tanstack/react-router'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { ArrowDownToLine, BadgeCheck, CircleAlert, Download, FileArchive, FileCheck2, Film, Play, ShieldCheck, Tag, Trash2, XCircle } from 'lucide-react'
import { derivativeAPI, integrityAPI, recordingsAPI } from '@/api'
import { adaptersQuery, exportsQuery, integrityQuery, previewsQuery, qk, recordingLifecycleQuery, recordingQuery } from '@/api/queries'
import { formatBytes, formatDate, formatDuration, resourceLabel } from '@/lib/utils'
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
import { errorMessage } from '@/lib/errors'
import { isFFmpegAvailable } from '@/lib/derivatives'
import type { ArchiveEntry, ExportJob, Gap, RecordingMetadata, Segment } from '@/types/api'
import { AdapterMark } from '@/components/adapter/adapter-mark'
import { LivePreviewViewport, PreviewFrameGrid, PreviewStateMessage } from '@/components/recording/previews'
import { seekToPreview } from '@/lib/previews'
import { recordingEventLabel, recordingEventMessageLabel } from '@/lib/labels'

const activeJob = (state: string) => state === 'queued' || state === 'running'
export function RecordingDetailPage() {
  const { recordingId } = useParams({ from: '/recordings/$recordingId' }); const client = useQueryClient(); const navigate = useNavigate(); const { toast } = useToast()
  const [tags, setTags] = useState<string[]>([]); const [tagInput, setTagInput] = useState(''); const [tagEditing, setTagEditing] = useState(false); const [tab, setTab] = useState<'overview' | 'archive' | 'events'>('overview'); const [jobId, setJobId] = useState<string>()
  const [activeView, setActiveView] = useState<'hls' | 'preview'>('hls')
  const recording = useQuery({ ...recordingQuery(recordingId), refetchInterval: query => query.state.data?.state === 'recording' ? 3000 : false, refetchIntervalInBackground: false }); const lifecycle = useQuery({ ...recordingLifecycleQuery(recordingId), enabled: Boolean(recording.data && recording.data.state !== 'recording' && !recording.data.archive_sealed), refetchInterval: query => query.state.data?.archive_sealed || recording.data?.archive_sealed ? false : 5000, refetchIntervalInBackground: false }); const integrity = useQuery({ ...integrityQuery(recordingId), refetchInterval: query => query.state.data?.status === 'verifying' ? 1500 : false, refetchIntervalInBackground: false }); const exports = useQuery({ ...exportsQuery(recordingId), refetchInterval: query => query.state.data?.items.some(job => activeJob(job.state)) ? 2000 : false, refetchIntervalInBackground: false })
  const previewSampling = recording.data?.state === 'recording' ? 'recent' : 'uniform'
  const preview = useQuery({ ...previewsQuery(recordingId, { sampling: previewSampling, limit: 48 }, recording.data?.state === 'recording'), enabled: recording.data?.preview?.mode === 'segment' })
  const adapters = useQuery(adaptersQuery)
  const archive = useQuery({ queryKey: qk.archive(recordingId), queryFn: () => recordingsAPI.archive(recordingId), enabled: tab === 'archive' })
  const events = useQuery({ queryKey: qk.events(recordingId), queryFn: () => recordingsAPI.events(recordingId), enabled: tab === 'events' })
  const metadata = useQuery({ queryKey: qk.metadata(recordingId), queryFn: () => recordingsAPI.metadata(recordingId), enabled: Boolean(recording.data), refetchInterval: recording.data?.state === 'recording' ? 30_000 : false, refetchIntervalInBackground: false })
  const tagsQuery = useQuery({ queryKey: qk.tags(recordingId), queryFn: () => recordingsAPI.tags(recordingId), staleTime: 30_000 })
  const playerRef = useRef<HTMLVideoElement | null>(null)
  const stop = useMutation({ mutationFn: () => recordingsAPI.stop(recordingId), onSuccess: () => { toast('녹화가 중지되었습니다.'); void client.invalidateQueries({ queryKey: qk.recording(recordingId) }); void client.invalidateQueries({ queryKey: qk.dashboard }) }, onError: error => toast('중지 실패', errorMessage(error), 'error') })
  const complete = useMutation({ mutationFn: () => recordingsAPI.complete(recordingId), onSuccess: () => { toast('녹화가 완료 상태로 표시되었습니다.'); void client.invalidateQueries({ queryKey: qk.recording(recordingId) }); void client.invalidateQueries({ queryKey: qk.lifecycle(recordingId) }); void client.invalidateQueries({ queryKey: ['recordings'] }) }, onError: error => toast('완료 처리 실패', errorMessage(error), 'error') })
  const seal = useMutation({ mutationFn: () => recordingsAPI.seal(recordingId), onSuccess: () => { toast('보관 archive를 봉인했습니다. 이후 복구 작업은 허용되지 않습니다.'); void client.invalidateQueries({ queryKey: qk.recording(recordingId) }); void client.invalidateQueries({ queryKey: qk.lifecycle(recordingId) }); void client.invalidateQueries({ queryKey: ['recordings'] }) }, onError: error => toast('archive 봉인 실패', errorMessage(error), 'error') })
  const remove = useMutation({ mutationFn: () => recordingsAPI.remove(recordingId), onSuccess: async () => { toast('보관 데이터를 삭제했습니다.'); await client.invalidateQueries({ queryKey: ['recordings'] }); await navigate({ to: '/recordings' }) }, onError: error => toast('삭제 실패', errorMessage(error), 'error') })
  const saveTags = useMutation({ mutationFn: () => recordingsAPI.setTags(recordingId, tagInput.split(',').map(x => x.trim()).filter(Boolean)), onSuccess: result => { setTags(result.tags); setTagInput(result.tags.join(', ')); setTagEditing(false); toast('태그를 저장했습니다.'); void client.invalidateQueries({ queryKey: ['recordings'] }); void client.invalidateQueries({ queryKey: qk.tags(recordingId) }) }, onError: error => toast('태그 저장 실패', errorMessage(error), 'error') })
  const verify = useMutation({ mutationFn: () => integrityAPI.start(recordingId), onSuccess: job => { setJobId(job.id); toast('무결성 검사를 시작했습니다.'); void client.invalidateQueries({ queryKey: qk.integrity(recordingId) }) }, onError: error => toast('검증 시작 실패', errorMessage(error), 'error') })
  const exportStart = useMutation({ mutationFn: () => derivativeAPI.create(recordingId), onSuccess: () => { toast('MKV 리먹스 작업을 등록했습니다.'); void client.invalidateQueries({ queryKey: qk.exports(recordingId) }) }, onError: error => toast('내보내기를 사용할 수 없습니다.', errorMessage(error), 'error') })
  const enablePreviews = useMutation({ mutationFn: () => recordingsAPI.enablePreviews(recordingId), onSuccess: async () => { toast('장면 미리보기 생성을 시작했습니다.'); await client.invalidateQueries({ queryKey: qk.recording(recordingId) }); void client.invalidateQueries({ queryKey: ['recording', recordingId, 'previews'] }); void client.invalidateQueries({ queryKey: ['recordings'] }); void client.invalidateQueries({ queryKey: qk.dashboard }) }, onError: error => toast('미리보기를 시작하지 못했습니다.', errorMessage(error), 'error') })
  const removeExport = useMutation({ mutationFn: derivativeAPI.remove, onSuccess: (_result, id) => { const wasActive = exports.data?.items.some(item => item.id === id && activeJob(item.state)); toast(wasActive ? '내보내기 작업을 취소했습니다.' : '내보내기 파생 항목을 삭제했습니다.'); void client.invalidateQueries({ queryKey: qk.exports(recordingId) }) }, onError: error => toast('내보내기 작업을 변경하지 못했습니다.', errorMessage(error), 'error') })
  const job = useQuery({ queryKey: ['integrity-job', jobId], queryFn: () => integrityAPI.job(jobId!), enabled: Boolean(jobId), refetchInterval: query => query.state.data && activeJob(query.state.data.state) ? 1500 : false, refetchIntervalInBackground: false, staleTime: 0 })
  const cancelJob = useMutation({ mutationFn: () => integrityAPI.cancel(jobId!), onSuccess: () => { toast('검증 작업을 취소했습니다.'); void client.invalidateQueries({ queryKey: ['integrity-job', jobId] }) } })
  useEffect(() => { if (tagsQuery.data && !tagEditing) { setTags(tagsQuery.data.tags); setTagInput(tagsQuery.data.tags.join(', ')) } }, [tagsQuery.data, tagEditing])
  useEffect(() => { setActiveView('hls') }, [recordingId])
  if (recording.isLoading) return <LoadingState />
  if (recording.error || !recording.data) return <ErrorState message={recording.error ? errorMessage(recording.error) : '녹화를 찾을 수 없습니다.'} retry={() => void recording.refetch()} />
  const item = recording.data; const stats = item.statistics ?? {}; const trackValues = Object.values(item.tracks ?? {}); const ffmpegAvailable = isFFmpegAvailable(exports.data); const hasCommittedMedia = (item.tracks?.main?.segments?.length ?? 0) > 0; const segments = Object.entries(item.tracks ?? {}).flatMap(([trackId, track]) => (track.segments ?? []).map(segment => ({ ...segment, trackId }))).sort((a, b) => a.trackId.localeCompare(b.trackId) || (a.archive_ordinal ?? a.sequence ?? 0) - (b.archive_ordinal ?? b.sequence ?? 0))
  const gaps = item.gaps ?? []; const integrityState = integrity.data?.status ?? stats.integrity ?? 'unknown'
  const currentAdapter = adapters.data?.find(adapter => adapter.status.id === item.adapter_id)
  const selectActivePreview = () => {
    setActiveView('preview')
    if (item.preview?.mode !== 'segment' && item.preview?.state !== 'unavailable' && !enablePreviews.isPending) enablePreviews.mutate()
  }
  return <div className="page-enter"><PageHeading eyebrow="녹화 보관 데이터" title={item.title || '제목 없는 녹화'} description={`${item.id} · ${item.adapter?.name ?? item.adapter_id ?? '어댑터 정보 없음'} · ${resourceLabel(item.resource)}`} actions={<><Link to="/recordings"><Button variant="outline" size="sm"><ArrowDownToLine className="h-4 w-4 rotate-180" />목록</Button></Link>{item.state === 'recording' && <Confirm trigger={<Button size="sm" variant="outline"><XCircle className="h-4 w-4" />중지</Button>} title="녹화를 중지할까요?" description="현재까지 수집된 원본 세그먼트를 정리하고 VOD 매니페스트를 생성합니다." confirmLabel="중지" onConfirm={() => stop.mutate()} disabled={stop.isPending} />}{item.state === 'stopped' && <Button size="sm" variant="outline" onClick={() => complete.mutate()} disabled={complete.isPending}>완료 처리</Button>}{item.state !== 'recording' && !item.archive_sealed && !lifecycle.data?.archive_sealed && <Confirm trigger={<Button size="sm" variant="outline">Archive 봉인</Button>} title="Archive를 봉인할까요?" description="봉인하면 이후 historical 복구와 새 claim을 받을 수 없습니다. 이 작업은 되돌릴 수 없습니다." confirmLabel="봉인" destructive onConfirm={() => seal.mutate()} disabled={seal.isPending} />}<Button size="sm" variant="outline" onClick={() => document.getElementById('vod-player')?.scrollIntoView({ behavior: 'smooth' })} disabled={item.state === 'recording' && !hasCommittedMedia}><Play className="h-4 w-4" />재생</Button><Confirm trigger={<Button size="sm" variant="destructive" disabled={remove.isPending || item.state === 'recording'}><Trash2 className="h-4 w-4" />삭제</Button>} title="보관 데이터를 삭제할까요?" description="이 작업은 recording.json, 매니페스트 스냅샷, 원본 미디어 데이터를 포함한 보관 데이터 전체를 삭제합니다. 되돌릴 수 없습니다." confirmLabel="보관 데이터 삭제" destructive onConfirm={() => remove.mutate()} disabled={remove.isPending || item.state === 'recording'} /></>} />
    <div className="mb-5 flex flex-wrap items-center gap-2"><AdapterMark adapter={currentAdapter} adapterId={item.adapter_id} name={item.adapter?.name} showName size="sm" /><StatusBadge state={item.state} /><Badge tone={item.archive_sealed || lifecycle.data?.archive_sealed ? 'neutral' : 'green'}>{item.archive_sealed || lifecycle.data?.archive_sealed ? 'Archive sealed' : '복구 가능'}</Badge><Badge tone="neutral">{formatDate(item.started_at)}</Badge>{item.stopped_at && <Badge tone="neutral">종료 {formatDate(item.stopped_at)}</Badge>}<Badge tone={item.source_uri_classification === 'sensitive' ? 'amber' : 'neutral'}>원본 URI · {item.source_uri_classification === 'sensitive' ? '민감 정보 포함' : item.source_uri_classification ?? '미분류'}</Badge></div>
    <div className="grid gap-4 xl:grid-cols-[minmax(0,1.6fr)_minmax(300px,.8fr)]">
      <div id="vod-player">
        <Card>
          <CardHeader className="flex-row items-center justify-between">
            <div>
              <CardTitle>{item.state === 'recording' ? '녹화 중 보기' : 'VOD 재생'}</CardTitle>
              <p className="mt-1 text-xs text-muted-foreground">{item.state === 'recording' ? '이미 보관된 세그먼트만 같은 출처에서 재생합니다.' : '저장된 원본 세그먼트로 만든 탐색 가능한 HLS 재생 목록'}</p>
            </div>
            <Badge tone="blue">{item.state === 'recording' ? '실시간 보관 재생' : 'HLS VOD'}</Badge>
          </CardHeader>
          <CardContent className="space-y-3">
            {item.state === 'recording' && <div role="group" aria-label="녹화 중 표시 방식" className="flex flex-wrap gap-2">
              <Button size="sm" variant={activeView === 'hls' ? 'default' : 'outline'} aria-pressed={activeView === 'hls'} onClick={() => setActiveView('hls')}>실시간 HLS</Button>
              <Button size="sm" variant={activeView === 'preview' ? 'default' : 'outline'} aria-pressed={activeView === 'preview'} onClick={selectActivePreview}>현재 미리보기</Button>
            </div>}
            {item.state !== 'recording' || activeView === 'hls'
              ? <RecordingPlayer recordingId={recordingId} active={item.state === 'recording'} hasCommittedSegments={hasCommittedMedia} timelineRevision={item.state === 'recording' ? undefined : lifecycle.data?.timeline_revision ?? item.timeline_revision} onVideoRef={video => { playerRef.current = video }} />
              : <div className="space-y-2">
                <LivePreviewViewport recordingId={recordingId} summary={item.preview} adapter={currentAdapter} adapterId={item.adapter_id} adapterName={item.adapter?.name} />
                <div className="flex flex-wrap items-center justify-between gap-2">
                  <PreviewStateMessage state={item.preview?.state} />
                  {item.preview?.mode !== 'segment' && item.preview?.state !== 'unavailable' && <Button size="sm" variant="outline" onClick={() => enablePreviews.mutate()} disabled={enablePreviews.isPending}>{enablePreviews.isPending ? '요청 중…' : '장면 미리보기 생성'}</Button>}
                </div>
                {item.preview?.latest_archive_ordinal != null && <p className="text-xs text-muted-foreground">최근 보관 순번 {item.preview.latest_archive_ordinal}</p>}
              </div>}
          </CardContent>
        </Card>
      </div>
      <Card>
        <CardHeader>
          <CardTitle>보관 요약</CardTitle>
          <p className="mt-1 text-xs text-muted-foreground">원본 보관 데이터에서 계산한 통계</p>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-2 gap-x-5 gap-y-4">
            {[
              ['재생 시간', formatDuration(stats.duration_seconds)],
              ['보관 용량', formatBytes(stats.archive_size_bytes)],
              ['미디어 페이로드', formatBytes(stats.media_payload_size_bytes)],
              ['초기화 데이터', formatBytes(stats.init_payload_size_bytes)],
              ['매니페스트 데이터', formatBytes(stats.manifest_size_bytes)],
              ['미디어 세그먼트', stats.segment_count ?? segments.length],
              ['초기화 세그먼트', stats.init_segment_count ?? trackValues.reduce((total, track) => total + (track.init_segments?.length ?? 0), 0)],
              ['매니페스트 스냅샷', stats.manifest_snapshot_count ?? item.manifest_snapshots?.length ?? 0],
              ['누락 구간', stats.gap_count ?? item.gaps?.length ?? 0],
              ['누락 시간', stats.gap_duration_seconds == null ? '미보고' : formatDuration(stats.gap_duration_seconds)],
            ].map(([label, value]) => <Stat key={String(label)} label={String(label)} value={String(value)} />)}
          </div>
          <div className="mt-5 border-t border-border pt-4">
            <div className="flex items-center justify-between"><span className="text-xs text-muted-foreground">무결성</span><StatusBadge state={integrityState} /></div>
            <p className="mt-2 text-xs text-muted-foreground">마지막 검사 {formatDate(integrity.data?.last_verified_at)}</p>
            {integrity.data?.freshness === 'stale' && <p role="status" className="mt-1 text-xs text-amber-700 dark:text-amber-300">검사 후 보관 내용이 변경되었습니다. 다시 검사하면 현재 archive를 확인합니다.</p>}
            {integrity.data?.freshness === 'unknown' && integrity.data?.last_verified_at && <p role="status" className="mt-1 text-xs text-muted-foreground">검사 기준 revision을 알 수 없는 이전 결과입니다.</p>}
            <p className="mt-1 text-xs text-muted-foreground">{integrity.data?.objects_verified ?? 0} / {integrity.data?.objects_total ?? 0}개 확인 · {integrity.data?.objects_missing ?? 0}개 누락 · {integrity.data?.objects_corrupt ?? 0}개 손상</p>
            <div className="mt-3 flex flex-wrap gap-2">
              <Button size="sm" onClick={() => verify.mutate()} disabled={verify.isPending || item.state === 'recording'}><ShieldCheck className="h-4 w-4" />무결성 검사</Button>
              {job.data && activeJob(job.data.state) && <Button variant="outline" size="sm" onClick={() => cancelJob.mutate()}>검사 취소</Button>}
            </div>
            {job.data && <p className="mt-2 text-xs text-muted-foreground">작업 {job.data.state}{job.data.result?.objects_total ? ` · ${job.data.result.objects_verified}/${job.data.result.objects_total}` : ''}</p>}
          </div>
        </CardContent>
      </Card>
    </div>
    <section className="mt-4" aria-labelledby="source-metadata-heading"><MetadataTimeline data={metadata.data} loading={metadata.isLoading} error={metadata.error} retry={() => void metadata.refetch()} /></section>
    {item.state !== 'recording' && <section className="mt-4" aria-labelledby="preview-heading"><Card><CardHeader className="flex-row items-start justify-between gap-3"><div><CardTitle id="preview-heading">장면 미리보기</CardTitle><p className="mt-1 text-xs text-muted-foreground">보관된 세그먼트에서 생성한 프레임을 골라 재생 위치로 이동합니다.</p></div>{item.preview?.mode !== 'segment' && item.preview?.state !== 'unavailable' && <Button size="sm" onClick={() => enablePreviews.mutate()} disabled={enablePreviews.isPending}><Film className="h-4 w-4" />{enablePreviews.isPending ? '요청 중…' : '장면 미리보기 생성'}</Button>}</CardHeader><CardContent className="space-y-4">{item.preview?.state === 'unavailable' ? <PreviewStateMessage state="unavailable" /> : item.preview?.mode !== 'segment' ? <div className="space-y-2"><PreviewStateMessage state={item.preview?.state ?? 'disabled'} /><p className="text-xs text-muted-foreground">기존 보관 세그먼트를 사용해 백그라운드에서 생성합니다. 원본 보관 데이터는 바뀌지 않습니다.</p></div> : <><div className="flex flex-wrap items-center justify-between gap-2"><PreviewStateMessage state={preview.data?.state ?? item.preview.state} /><span className="text-xs tabular-nums text-muted-foreground">{preview.data?.items.length ?? 0}개 프레임</span></div>{preview.error ? <p role="status" className="text-xs text-muted-foreground">장면 미리보기를 불러오지 못했습니다. 잠시 후 다시 확인하세요.</p> : preview.isLoading ? <p role="status" className="text-xs text-muted-foreground">장면 미리보기를 불러오는 중입니다.</p> : preview.data?.items.length ? <PreviewFrameGrid items={preview.data.items} recordingId={recordingId} onSeek={seconds => { seekToPreview(playerRef.current, seconds) }} /> : <p className="text-xs text-muted-foreground">아직 사용할 수 있는 프레임이 없습니다.</p>}</>}</CardContent></Card></section>}
    <Card className="mt-4"><CardHeader className="flex-row items-center justify-between"><div><CardTitle>수집 순서</CardTitle><p className="mt-1 text-xs text-muted-foreground">트랙별 원본 세대 / 순번 · 시간 추정 없이 실제 세그먼트와 기록된 누락 구간 표시</p></div><span className="text-xs text-muted-foreground">{segments.length}개 세그먼트 · {gaps.length}개 누락 구간</span></CardHeader><CardContent><Timeline segments={segments} gaps={gaps} /></CardContent></Card>
    <div className="mt-4 grid gap-4 xl:grid-cols-[1fr_1fr]"><Card><CardHeader className="flex-row items-center justify-between"><div><CardTitle>태그</CardTitle><p className="mt-1 text-xs text-muted-foreground">관리용 메타데이터 · 원본 미디어에는 영향 없음</p></div><Tag className="h-4 w-4 text-muted-foreground" /></CardHeader><CardContent>{tagEditing ? <div className="flex gap-2"><Input aria-label="태그 목록" value={tagInput} onChange={event => setTagInput(event.target.value)} placeholder="concert, important" maxLength={512} /><Button onClick={() => saveTags.mutate()} disabled={saveTags.isPending}>저장</Button><Button variant="outline" onClick={() => { setTagInput(tags.join(', ')); setTagEditing(false) }}>취소</Button></div> : <div className="flex min-h-10 flex-wrap items-center gap-2">{tags.length ? tags.map(tag => <Badge key={tag}>{tag}</Badge>) : <span className="text-sm text-muted-foreground">태그 없음</span>}<Button variant="ghost" size="sm" className="ml-auto" onClick={() => { setTagInput(tags.join(', ')); setTagEditing(true) }}>편집</Button></div>}</CardContent></Card><Card><CardHeader className="flex-row items-center justify-between"><div><CardTitle>파생물</CardTitle><p className="mt-1 text-xs text-muted-foreground">원본 보관 데이터는 변경하지 않습니다.</p></div><FileArchive className="h-4 w-4 text-muted-foreground" /></CardHeader><CardContent><div className="flex flex-wrap items-center justify-between gap-3"><div><p className="text-sm font-medium">MKV 리먹스</p><p className="text-xs text-muted-foreground">{exports.isLoading ? 'FFmpeg 지원 여부 확인 중' : ffmpegAvailable ? 'FFmpeg 복사 모드로 파생 파일을 만듭니다.' : '이 서버에서 내보내기를 사용할 수 없습니다.'}</p></div><Button size="sm" onClick={() => exportStart.mutate()} disabled={!ffmpegAvailable || exportStart.isPending || item.state === 'recording'}><FileArchive className="h-4 w-4" />내보내기 시작</Button></div><div className="mt-3 divide-y divide-border">{exports.data?.items.map(job => <ExportRow key={job.id} item={job} onDelete={() => removeExport.mutate(job.id)} deleting={removeExport.isPending} />)}{exports.data && !exports.data.items.length && <p className="py-3 text-xs text-muted-foreground">내보내기 작업이 없습니다.</p>}</div></CardContent></Card></div>
    <Card className="mt-4"><div className="flex gap-1 border-b border-border px-4 pt-3" role="tablist" aria-label="녹화 세부 정보">{(['overview', 'archive', 'events'] as const).map(key => <button key={key} id={`recording-tab-${key}`} aria-controls={`recording-panel-${key}`} role="tab" aria-selected={tab === key} tabIndex={tab === key ? 0 : -1} className={`focus-ring rounded-t-md px-3 py-2 text-sm ${tab === key ? 'border-b-2 border-primary font-semibold text-primary' : 'text-muted-foreground hover:text-foreground'}`} onClick={() => setTab(key)}>{key === 'overview' ? '개요' : key === 'archive' ? '보관 데이터 목록' : '이벤트'}</button>)}</div><CardContent id={`recording-panel-${tab}`} role="tabpanel" aria-labelledby={`recording-tab-${tab}`} tabIndex={0} className="pt-4">{tab === 'overview' && <div className="grid gap-4 sm:grid-cols-2"><Info label="어댑터 출처 정보" value={item.adapter ? `${item.adapter.name ?? item.adapter.id} · v${item.adapter.version} · 프로토콜 v${item.adapter.protocol_version}` : '저장된 어댑터 출처 정보 없음'} /><Info label="리소스 참조" value={resourceLabel(item.resource)} /><Info label="녹화 ID" value={item.id} mono /><Info label="원본 URI 분류" value={item.source_uri_classification ?? '민감 정보 포함 · API에서 가림'} /></div>}{tab === 'archive' && <ArchiveIndex entries={archive.data?.entries} loading={archive.isLoading} error={archive.error} retry={() => void archive.refetch()} />}{tab === 'events' && <Events items={events.data?.items} loading={events.isLoading} error={events.error} retry={() => void events.refetch()} />}</CardContent></Card>
    <div className="mt-4"><Card><CardHeader><CardTitle>보존 메모</CardTitle></CardHeader><CardContent className="space-y-3 text-xs leading-5 text-muted-foreground"><p className="flex gap-2"><BadgeCheck className="mt-0.5 h-4 w-4 shrink-0 text-emerald-600" />미디어 페이로드는 원본에서 받은 바이트 그대로 보존됩니다.</p><p className="flex gap-2"><FileCheck2 className="mt-0.5 h-4 w-4 shrink-0 text-primary" />매니페스트, 초기화 객체, 미디어 세그먼트는 각각 별도의 보관 객체로 관리됩니다.</p><p className="flex gap-2"><CircleAlert className="mt-0.5 h-4 w-4 shrink-0 text-amber-600" />민감한 원본 URI는 API에서 가려지며, URI 기반 추정은 표시하지 않습니다.</p></CardContent></Card></div>
  </div>
}
export function MetadataTimeline({ data, loading, error, retry }: { data?: RecordingMetadata; loading: boolean; error: unknown; retry: () => void }) {
  const valueLabel = (value?: string | null) => value == null ? '알 수 없음' : value === '' ? '— (비어 있음)' : value
  return <Card><CardHeader><CardTitle id="source-metadata-heading">방송 메타데이터</CardTitle><p className="mt-1 text-xs text-muted-foreground">원본 방송에서 관측한 제목과 설명의 변경 기록입니다.</p></CardHeader><CardContent className="space-y-4">
    {loading && !data ? <p role="status" className="text-sm text-muted-foreground">방송 메타데이터를 불러오는 중입니다.</p> : error ? <ErrorState message={errorMessage(error)} retry={retry} /> : !data?.current ? <p className="text-sm text-muted-foreground">수집된 방송 메타데이터가 없습니다.</p> : <>
      <div className="grid gap-4 sm:grid-cols-2"><MetadataField label="현재 방송 제목" value={valueLabel(data.current.title)} /><MetadataField label="현재 방송 설명" value={valueLabel(data.current.description)} /></div>
      <div className="flex flex-wrap gap-x-5 gap-y-1 text-xs text-muted-foreground"><span>마지막 관측 {formatDate(data.current.observed_at)}</span>{data.current.source_updated_at && <span>원본 업데이트 {formatDate(data.current.source_updated_at)}</span>}</div>
      {data.items.length > 1 && <div className="border-t border-border pt-4"><h3 className="mb-3 text-sm font-semibold">메타데이터 변경 기록</h3><ol className="space-y-4">{data.items.map((revision, index) => <li key={`${revision.observed_at}-${index}`} className="border-l-2 border-primary/30 pl-3"><time className="text-xs text-muted-foreground">{formatDate(revision.observed_at)}</time><div className="mt-1 grid gap-2 sm:grid-cols-2"><MetadataField label="제목" value={valueLabel(revision.title)} /><MetadataField label="설명" value={valueLabel(revision.description)} /></div>{revision.source_updated_at && <p className="mt-1 text-[11px] text-muted-foreground">원본 업데이트 {formatDate(revision.source_updated_at)}</p>}</li>)}</ol></div>}
      {data.truncated && <p role="status" className="text-xs text-amber-700 dark:text-amber-300">메타데이터 변경 기록이 보존 한도에 도달했거나 일부 이전 기록이 생략되었습니다.</p>}
    </>}
  </CardContent></Card>
}
function MetadataField({ label, value }: { label: string; value: string }) { return <div className="min-w-0"><p className="text-[11px] text-muted-foreground">{label}</p><p className="mt-1 whitespace-pre-wrap break-words text-sm">{value}</p></div> }
function Stat({ label, value }: { label: string; value: string | number }) { return <div><p className="text-[11px] text-muted-foreground">{label}</p><p className="mt-1 text-sm font-semibold tabular-nums">{value}</p></div> }
function Info({ label, value, mono }: { label: string; value: string; mono?: boolean }) { return <div><p className="text-[11px] text-muted-foreground">{label}</p><p className={`mt-1 break-all text-sm ${mono ? 'font-mono text-xs' : 'font-medium'}`}>{value}</p></div> }
export function Timeline({ segments, gaps }: { segments: (Segment & { trackId: string })[]; gaps: Gap[] }) {
  if (!segments.length && !gaps.length) return <EmptyState title="수집 순서 데이터가 없습니다." />
  const trackIds = [...new Set([...segments.map(item => item.trackId), ...gaps.map(item => item.track_id ?? '알 수 없음')])].sort()
  return <div className="space-y-5">{trackIds.map(trackId => {
    const trackSegments = segments.filter(item => item.trackId === trackId)
    const trackGaps = gaps.filter(item => (item.track_id ?? '알 수 없음') === trackId)
    const entries = [
      ...trackSegments.map(item => ({ kind: 'segment' as const, epoch: item.source_epoch ?? 0, sequence: item.sequence ?? 0, end: item.sequence ?? 0, item })),
      ...trackGaps.map(item => ({ kind: 'gap' as const, epoch: item.source_epoch ?? 0, sequence: item.from_sequence ?? 0, end: item.to_sequence ?? item.from_sequence ?? 0, item })),
    ].sort((a, b) => a.epoch - b.epoch || a.sequence - b.sequence || (a.kind === 'gap' ? -1 : 1))
    return <section key={trackId} aria-label={`트랙 ${trackId} 수집 타임라인`}>
      <div className="mb-2 flex items-center justify-between gap-2"><h3 className="truncate text-xs font-semibold">트랙 {trackId}</h3><span className="shrink-0 text-[11px] text-muted-foreground">{trackSegments.length}개 세그먼트 · {trackGaps.length}개 누락 구간</span></div>
      <div className="flex h-8 overflow-hidden rounded-md bg-muted" role="img" aria-label={`원본 세대와 순번 순서 기준 ${trackSegments.length}개 수집 세그먼트와 ${trackGaps.length}개 누락 기록`}>
        {entries.map((entry, index) => <span key={`${entry.kind}-${entry.epoch}-${entry.sequence}-${index}`} className={`min-w-[3px] ${entry.kind === 'gap' ? 'bg-amber-500' : 'bg-blue-500'}`} style={{ flexGrow: entry.kind === 'gap' ? Math.min(1000, Math.max(1, entry.end - entry.sequence + 1)) : 1, flexBasis: 0 }} title={entry.kind === 'gap' ? `누락 · 세대 ${entry.epoch} · 순번 ${entry.sequence}–${entry.end}` : `세그먼트 · 세대 ${entry.epoch} · 순번 ${entry.sequence}${entry.item.discontinuity ? ' · 불연속' : ''}`} />)}
      </div>
      <div className="mt-2 flex flex-wrap gap-x-4 gap-y-1 text-[10px] text-muted-foreground"><span className="flex items-center gap-1.5"><i className="h-2 w-2 rounded-sm bg-blue-500" />보존된 세그먼트</span><span className="flex items-center gap-1.5"><i className="h-2 w-2 rounded-sm bg-amber-500" />기록된 누락</span>{trackSegments.some(item => item.discontinuity) && <span>불연속 {trackSegments.filter(item => item.discontinuity).length}회</span>}</div>
      <div className="mt-3 max-h-48 overflow-y-auto rounded-md border border-border"><table className="w-full text-left text-xs"><thead className="sticky top-0 bg-muted"><tr><th className="px-3 py-2 font-medium">보관 순번</th><th className="px-3 py-2 font-medium">세대 / 원본 순번</th><th className="px-3 py-2 font-medium">재생 시간</th><th className="px-3 py-2 font-medium">상태</th></tr></thead><tbody>{trackSegments.map((item, index) => <tr key={`${trackId}-${item.archive_ordinal ?? index}`} className="border-t border-border"><td className="px-3 py-2 font-mono">{item.archive_ordinal ?? '—'}</td><td className="px-3 py-2 font-mono">{item.source_epoch ?? 0} / {item.sequence ?? '—'}</td><td className="px-3 py-2">{item.duration == null ? '—' : formatDuration(item.duration)}</td><td className="px-3 py-2">{item.discontinuity ? <Badge tone="amber">불연속</Badge> : <Badge tone="green">수집됨</Badge>}</td></tr>)}</tbody></table></div>
      {trackGaps.length > 0 && <ul className="mt-2 space-y-1 text-[11px] text-amber-700 dark:text-amber-300">{trackGaps.map((gap, index) => <li key={`${gap.source_epoch ?? 0}-${gap.from_sequence ?? 0}-${index}`}>누락 · 세대 {gap.source_epoch ?? 0} · 순번 {gap.from_sequence ?? '—'}–{gap.to_sequence ?? gap.from_sequence ?? '—'}{gap.reason ? ` · ${gap.reason}` : ''}</li>)}</ul>}
    </section>
  })}</div>
}
function ArchiveIndex({ entries, loading, error, retry }: { entries?: ArchiveEntry[]; loading: boolean; error: unknown; retry: () => void }) { if (loading) return <LoadingState />; if (error) return <ErrorState message={errorMessage(error)} retry={retry} />; if (!entries?.length) return <EmptyState title="보관 데이터 목록이 비어 있습니다." />; return <div className="overflow-x-auto"><table className="w-full min-w-[560px] text-left text-xs"><thead><tr className="border-b border-border text-muted-foreground"><th className="pb-2">종류</th><th className="pb-2">상대 경로</th><th className="pb-2 text-right">크기</th><th className="pb-2">SHA-256</th></tr></thead><tbody>{entries.map(entry => <tr key={entry.path} className="border-b border-border/60 last:border-0"><td className="py-2"><Badge>{entry.kind}</Badge></td><td className="py-2 font-mono">{entry.path}</td><td className="py-2 text-right tabular-nums">{formatBytes(entry.size)}</td><td className="max-w-56 truncate py-2 font-mono text-[10px]">{entry.sha256 ?? '—'}</td></tr>)}</tbody></table></div> }
function Events({ items, loading, error, retry }: { items?: { id: string; type: string; at: string; count?: number; message?: string }[]; loading: boolean; error: unknown; retry: () => void }) { if (loading) return <LoadingState />; if (error) return <ErrorState message={errorMessage(error)} retry={retry} />; if (!items?.length) return <EmptyState title="이벤트가 없습니다." />; return <ol className="space-y-3">{items.map(event => <li key={event.id} className="flex gap-3"><span className="relative mt-1.5 h-2 w-2 shrink-0 rounded-full bg-primary after:absolute after:left-1/2 after:top-2 after:h-8 after:w-px after:-translate-x-1/2 after:bg-border last:after:hidden" /><div className="min-w-0 flex-1"><div className="flex flex-wrap items-center justify-between gap-2"><span className="text-sm font-medium">{recordingEventLabel(event.type)}</span><time className="text-[11px] text-muted-foreground">{formatDate(event.at)}</time></div>{event.message && <p className="mt-1 text-xs text-muted-foreground">{recordingEventMessageLabel(event.message)}</p>}{event.count && <p className="mt-1 text-xs text-muted-foreground">{event.count}개 세그먼트</p>}</div></li>)}</ol> }
export function ExportRow({ item, onDelete, deleting }: { item: ExportJob; onDelete: () => void; deleting: boolean }) { return <div className="flex flex-wrap items-center gap-2 py-3"><StatusBadge state={item.state} />{item.freshness === 'stale' && <Badge tone="amber">이전 archive 기준</Badge>}{item.freshness === 'unknown' && item.state === 'completed' && <Badge tone="neutral">기준 revision 알 수 없음</Badge>}<span className="min-w-0 flex-1 truncate text-xs">{item.output_name ?? item.id}</span>{item.state === 'completed' && <a href={derivativeAPI.download(item.id)}><Button variant="outline" size="sm"><Download className="h-3.5 w-3.5" />받기</Button></a>}{activeJob(item.state) ? <Button variant="outline" size="sm" onClick={onDelete} disabled={deleting} aria-label="내보내기 작업 취소"><XCircle className="h-3.5 w-3.5" />작업 취소</Button> : <Confirm trigger={<Button variant="ghost" size="icon" aria-label="내보내기 항목 삭제" disabled={deleting}><Trash2 className="h-4 w-4" /></Button>} title="내보내기 항목을 삭제할까요?" description="파생 내보내기 파일을 제거합니다. 원본 녹화 보관 데이터에는 영향이 없습니다." confirmLabel="내보내기 삭제" destructive onConfirm={onDelete} disabled={deleting} />}</div> }
