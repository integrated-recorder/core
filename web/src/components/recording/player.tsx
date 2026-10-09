import { useEffect, useRef, useState } from 'react'
import type Hls from 'hls.js'
import { CircleAlert, Film } from 'lucide-react'

type PlaylistSegment = { identity: string; duration: number; start: number; programDateTime?: number }
type PlaybackAnchor = { identity: string; offset: number; index: number; programDateTime?: number; playlist: PlaylistSegment[]; currentTime: number }
type RestorePosition = { recordingId: string; currentTime: number; wasPlaying: boolean; anchor?: PlaybackAnchor }

function parseMediaPlaylist(text: string, playlistURL: string): PlaylistSegment[] {
  const segments: PlaylistSegment[] = []
  let pendingDuration: number | undefined
  let elapsed = 0
  let pendingByteRange: { length: number; offset?: number } | undefined
  let nextProgramDateTime: number | undefined
  let previousRangeURI = ''
  let previousRangeEnd = 0

  for (const rawLine of text.split(/\r?\n/)) {
    const line = rawLine.trim()
    if (!line) continue
    if (line.startsWith('#EXTINF:')) {
      if (pendingDuration !== undefined) return []
      const rawDuration = line.slice('#EXTINF:'.length).split(',', 1)[0]?.trim()
      const duration = rawDuration ? Number(rawDuration) : Number.NaN
      pendingDuration = Number.isFinite(duration) && duration >= 0 ? duration : undefined
      if (pendingDuration === undefined) return []
      pendingByteRange = undefined
      continue
    }
    if (line.startsWith('#EXT-X-BYTERANGE:')) {
      const match = /^(\d+)(?:@(\d+))?$/.exec(line.slice('#EXT-X-BYTERANGE:'.length).trim())
      if (!match) return []
      const length = Number(match[1])
      const offset = match[2] === undefined ? undefined : Number(match[2])
      if (!Number.isSafeInteger(length) || length <= 0 || (offset !== undefined && (!Number.isSafeInteger(offset) || offset < 0))) return []
      pendingByteRange = { length, offset }
      continue
    }
    if (line.startsWith('#EXT-X-PROGRAM-DATE-TIME:')) {
      const timestamp = Date.parse(line.slice('#EXT-X-PROGRAM-DATE-TIME:'.length).trim())
      nextProgramDateTime = Number.isFinite(timestamp) ? timestamp : undefined
      continue
    }
    if (line.startsWith('#')) continue
    if (pendingDuration === undefined) return []

    try {
      const uri = new URL(line, new URL(playlistURL, window.location.href))
      const resourceURI = `${uri.pathname}${uri.search}`
      let identity = resourceURI
      if (pendingByteRange) {
        const offset = pendingByteRange.offset ?? (previousRangeURI === resourceURI ? previousRangeEnd : undefined)
        if (offset === undefined || !Number.isSafeInteger(offset + pendingByteRange.length)) return []
        identity += `#byterange=${offset}:${pendingByteRange.length}`
        previousRangeURI = resourceURI
        previousRangeEnd = offset + pendingByteRange.length
      } else {
        previousRangeURI = ''
        previousRangeEnd = 0
      }
      segments.push({ identity, duration: pendingDuration, start: elapsed, programDateTime: nextProgramDateTime })
      if (nextProgramDateTime !== undefined) nextProgramDateTime += pendingDuration * 1000
      elapsed += pendingDuration
    } catch { return [] }
    pendingDuration = undefined
    pendingByteRange = undefined
  }
  return pendingDuration === undefined ? segments : []
}

function anchorAtTime(segments: PlaylistSegment[], currentTime: number): PlaybackAnchor | undefined {
  const index = segments.findIndex(item => currentTime >= item.start && currentTime < item.start + item.duration)
  const segment = segments[index]
  return segment ? { identity: segment.identity, offset: currentTime - segment.start, index, programDateTime: segment.programDateTime, playlist: segments, currentTime } : undefined
}

function positionForAnchor(segments: PlaylistSegment[], anchor: PlaybackAnchor): number | undefined {
  const segment = segments.find(item => item.identity === anchor.identity)
  if (segment) return segment.start + Math.min(anchor.offset, segment.duration)

  if (anchor.programDateTime !== undefined) {
    const datedSegment = segments.find(item => item.programDateTime === anchor.programDateTime)
    if (datedSegment) return datedSegment.start + Math.min(anchor.offset, datedSegment.duration)
  }

  const candidates = anchor.playlist
    .filter((_item, index) => index !== anchor.index)
    .map(item => ({
      item,
      distance: anchor.currentTime < item.start
        ? item.start - anchor.currentTime
        : anchor.currentTime > item.start + item.duration
          ? anchor.currentTime - item.start - item.duration
          : 0,
      before: item.start + item.duration <= anchor.currentTime,
    }))
    .sort((left, right) => left.distance - right.distance)
  for (const candidate of candidates) {
    const surviving = segments.find(item => item.identity === candidate.item.identity)
    if (!surviving) continue
    return candidate.before
      ? surviving.start + Math.max(0, surviving.duration - Math.min(0.05, surviving.duration / 2))
      : surviving.start
  }
  return undefined
}

function trackPlaylistURL(recordingId: string, timelineRevision?: number): string {
  const path = `/api/recordings/${encodeURIComponent(recordingId)}/play/tracks/main/playlist.m3u8`
  return timelineRevision === undefined ? path : `${path}?timeline_revision=${encodeURIComponent(String(timelineRevision))}`
}

export function RecordingPlayer({ recordingId, active = false, hasCommittedSegments = true, timelineRevision, onVideoRef }: { recordingId: string; active?: boolean; hasCommittedSegments?: boolean; timelineRevision?: number; onVideoRef?: (video: HTMLVideoElement | null) => void }) {
  const videoRef = useRef<HTMLVideoElement>(null)
  const restoreRef = useRef<RestorePosition | undefined>(undefined)
  const vodPlaylistRef = useRef<{ recordingId: string; segments: PlaylistSegment[] } | undefined>(undefined)
  const [error, setError] = useState('')
  useEffect(() => {
    const video = videoRef.current
    if (!video) return
    let hls: Hls | undefined
    let mounted = true
    setError('')
    if (active && !hasCommittedSegments) {
      video.pause()
      video.removeAttribute('src')
      video.load()
      return () => { mounted = false }
    }
    const baseSource = active
      ? `/api/recordings/${encodeURIComponent(recordingId)}/play/live/master.m3u8`
      : `/api/recordings/${encodeURIComponent(recordingId)}/play/master.m3u8`
    const source = !active && timelineRevision !== undefined
      ? `${baseSource}?timeline_revision=${encodeURIComponent(String(timelineRevision))}`
      : baseSource
    const restore = restoreRef.current?.recordingId === recordingId ? restoreRef.current : undefined
    if (restoreRef.current && !restore) restoreRef.current = undefined
    let metadataLoaded = false
    let restoreTarget: number | undefined
    const applyRestore = () => {
      if (!mounted || !restore || !metadataLoaded || restoreTarget === undefined) return
      const maximum = Number.isFinite(video.duration) ? Math.max(0, video.duration - 0.25) : restoreTarget
      video.currentTime = Math.min(restoreTarget, maximum)
      restoreRef.current = undefined
      if (restore.wasPlaying) void video.play().catch(() => undefined)
    }
    const restorePosition = () => { metadataLoaded = true; applyRestore() }
    video.addEventListener('loadedmetadata', restorePosition)

    let playlistPromise: Promise<PlaylistSegment[] | undefined> | undefined
    if (!active) {
      const url = trackPlaylistURL(recordingId, timelineRevision)
      playlistPromise = (async () => {
        try {
          const response = await fetch(url, { cache: 'no-store' })
          if (!response.ok) return undefined
          const segments = parseMediaPlaylist(await response.text(), url)
          if (mounted) vodPlaylistRef.current = { recordingId, segments }
          return segments
        } catch {
          return undefined
        }
      })()
    }
    if (restore) {
      const resolveAnchor = playlistPromise
        ? playlistPromise.then(segments => restore.anchor && segments ? positionForAnchor(segments, restore.anchor) : undefined)
        : Promise.resolve(undefined)
      void resolveAnchor.then(position => {
        if (!mounted) return
        restoreTarget = position ?? restore.currentTime
        applyRestore()
      })
    }
    if (video.canPlayType('application/vnd.apple.mpegurl')) {
      video.src = source
    } else {
      void import('hls.js').then(({ default: HlsModule }) => {
        if (!mounted) return
        if (!HlsModule.isSupported()) { setError('이 브라우저에서 HLS 재생을 지원하지 않습니다.'); return }
        hls = new HlsModule({ enableWorker: true, lowLatencyMode: false })
        hls.loadSource(source)
        hls.attachMedia(video)
        hls.on(HlsModule.Events.ERROR, (_event, data) => { if (data.fatal && mounted) setError('재생을 시작하지 못했습니다. 보관 데이터 상태 또는 네트워크를 확인하세요.') })
      }).catch(() => { if (mounted) setError('HLS 재생 모듈을 불러오지 못했습니다.') })
    }
    return () => {
      if (!active) {
        const currentTime = Number.isFinite(video.currentTime) ? video.currentTime : 0
        const currentPlaylist = vodPlaylistRef.current?.recordingId === recordingId ? vodPlaylistRef.current.segments : undefined
        restoreRef.current = {
          recordingId,
          currentTime,
          wasPlaying: !video.paused && !video.ended,
          anchor: currentPlaylist ? anchorAtTime(currentPlaylist, currentTime) : undefined,
        }
      }
      mounted = false
      video.removeEventListener('loadedmetadata', restorePosition)
      hls?.destroy(); video.pause(); video.removeAttribute('src'); video.load()
    }
  }, [recordingId, active, hasCommittedSegments, timelineRevision])

  if (active && !hasCommittedSegments) return <div className="mx-auto grid aspect-video max-h-[360px] place-items-center rounded-md bg-muted p-6 text-center" role="status"><div><CircleAlert className="mx-auto h-6 w-6 text-muted-foreground" /><p className="mt-2 text-sm font-medium">첫 세그먼트를 기다리는 중입니다.</p><p className="mt-1 text-xs text-muted-foreground">세그먼트가 보관되면 실시간 HLS 재생이 시작됩니다.</p></div></div>
  return <div className="overflow-hidden rounded-lg border border-border bg-black"><div className="relative mx-auto aspect-video max-h-[360px] w-full"><video ref={node => { videoRef.current = node; onVideoRef?.(node) }} className="h-full w-full object-contain" controls playsInline preload="metadata" aria-label={active ? '녹화 중 실시간 HLS 재생' : '녹화 VOD 재생'} />{error && <div className="absolute inset-0 grid place-items-center bg-black/80 p-6 text-center text-white"><div><CircleAlert className="mx-auto h-6 w-6 text-amber-300" /><p className="mt-2 text-sm font-medium">재생할 수 없습니다</p><p className="mt-1 max-w-sm text-xs text-white/70">{error}</p></div></div>}{!error && <div className="pointer-events-none absolute left-3 top-3 inline-flex items-center gap-1.5 rounded bg-black/55 px-2 py-1 text-[10px] font-medium text-white"><Film className="h-3 w-3" />{active ? '보관 세그먼트' : '원본 세그먼트'}</div>}</div></div>
}
