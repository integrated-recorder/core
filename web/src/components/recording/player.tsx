import { useEffect, useRef, useState } from 'react'
import type Hls from 'hls.js'
import { CircleAlert, Film } from 'lucide-react'

export function RecordingPlayer({ recordingId, active = false, hasCommittedSegments = true, timelineRevision, onVideoRef }: { recordingId: string; active?: boolean; hasCommittedSegments?: boolean; timelineRevision?: number; onVideoRef?: (video: HTMLVideoElement | null) => void }) {
  const videoRef = useRef<HTMLVideoElement>(null)
  const restoreRef = useRef<{ recordingId: string; currentTime: number; wasPlaying: boolean } | undefined>(undefined)
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
    const restorePosition = () => {
      if (!mounted || !restore) return
      const maximum = Number.isFinite(video.duration) ? Math.max(0, video.duration - 0.25) : restore.currentTime
      video.currentTime = Math.min(restore.currentTime, maximum)
      restoreRef.current = undefined
      if (restore.wasPlaying) void video.play().catch(() => undefined)
    }
    video.addEventListener('loadedmetadata', restorePosition)
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
      if (!active && video.currentSrc) {
        restoreRef.current = { recordingId, currentTime: Number.isFinite(video.currentTime) ? video.currentTime : 0, wasPlaying: !video.paused && !video.ended }
      }
      mounted = false
      video.removeEventListener('loadedmetadata', restorePosition)
      hls?.destroy(); video.pause(); video.removeAttribute('src'); video.load()
    }
  }, [recordingId, active, hasCommittedSegments, timelineRevision])

  if (active && !hasCommittedSegments) return <div className="mx-auto grid aspect-video max-h-[360px] place-items-center rounded-md bg-muted p-6 text-center" role="status"><div><CircleAlert className="mx-auto h-6 w-6 text-muted-foreground" /><p className="mt-2 text-sm font-medium">첫 세그먼트를 기다리는 중입니다.</p><p className="mt-1 text-xs text-muted-foreground">세그먼트가 보관되면 실시간 HLS 재생이 시작됩니다.</p></div></div>
  return <div className="overflow-hidden rounded-lg border border-border bg-black"><div className="relative mx-auto aspect-video max-h-[360px] w-full"><video ref={node => { videoRef.current = node; onVideoRef?.(node) }} className="h-full w-full object-contain" controls playsInline preload="metadata" aria-label={active ? '녹화 중 실시간 HLS 재생' : '녹화 VOD 재생'} />{error && <div className="absolute inset-0 grid place-items-center bg-black/80 p-6 text-center text-white"><div><CircleAlert className="mx-auto h-6 w-6 text-amber-300" /><p className="mt-2 text-sm font-medium">재생할 수 없습니다</p><p className="mt-1 max-w-sm text-xs text-white/70">{error}</p></div></div>}{!error && <div className="pointer-events-none absolute left-3 top-3 inline-flex items-center gap-1.5 rounded bg-black/55 px-2 py-1 text-[10px] font-medium text-white"><Film className="h-3 w-3" />{active ? '보관 세그먼트' : '원본 세그먼트'}</div>}</div></div>
}
