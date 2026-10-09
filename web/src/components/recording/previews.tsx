import { useEffect, useState } from 'react'
import { Cable, Film } from 'lucide-react'
import { AdapterMark } from '@/components/adapter/adapter-mark'
import type { Adapter, PreviewFrame, PreviewSummary } from '@/types/api'
import { formatDuration } from '@/lib/utils'
import { formatPreviewClock, previewFrameURL, uniquePreviewFrames } from '@/lib/previews'
import { useI18n } from '@/i18n/provider'

export function PreviewThumbnail({
  recordingId, summary, adapter, adapterId, adapterName, className = '',
}: {
  recordingId: string; summary?: PreviewSummary; adapter?: Adapter; adapterId?: string; adapterName?: string; className?: string
}) {
  const { t } = useI18n()
  const ordinal = summary?.mode === 'segment' && summary.frame_count > 0 ? summary.image_archive_ordinal : undefined
  const src = ordinal == null ? undefined : previewFrameURL(recordingId, ordinal, summary?.updated_at ?? String(ordinal))
  const [failedURL, setFailedURL] = useState<string>()
  const showFrame = Boolean(src && failedURL !== src)
  return <span role="img" className={`relative grid aspect-video shrink-0 place-items-center overflow-hidden rounded-md border border-border bg-muted ${className}`} aria-label={showFrame ? t('preview.label') : t('preview.representative', { name: adapterName ?? adapterId ?? t('recordings.noTitle') })}>
    {showFrame && src ? <img key={src} src={src} alt="" loading="lazy" decoding="async" className="h-full w-full object-cover" onError={() => setFailedURL(src)} /> : <span className="grid h-full w-full place-items-center" aria-hidden="true">{adapter ? <AdapterMark adapter={adapter} adapterId={adapterId} name={adapterName} size="xs" /> : adapterId || adapterName ? <AdapterMark adapterId={adapterId} name={adapterName} size="xs" /> : <Cable className="h-4 w-4 text-muted-foreground" />}</span>}
  </span>
}

export function LivePreviewViewport({
  recordingId, summary, adapter, adapterId, adapterName,
}: {
  recordingId: string; summary?: PreviewSummary; adapter?: Adapter; adapterId?: string; adapterName?: string
}) {
  const { t } = useI18n()
  const ordinal = summary?.mode === 'segment' && summary.frame_count > 0 ? summary.image_archive_ordinal : undefined
  // A ready frame is immutable for its archive ordinal. Index timestamps may
  // change when a different segment finishes, so they must not retrigger the
  // same live image or cause a duplicate cross-fade.
  const requestedSrc = ordinal == null ? undefined : previewFrameURL(recordingId, ordinal, String(ordinal))
  const [frames, setFrames] = useState<{ current?: string; previous?: string; pending?: string }>({})

  useEffect(() => {
    if (!frames.previous) return
    const previous = frames.previous
    const timer = window.setTimeout(() => setFrames(current => current.previous === previous ? { ...current, previous: undefined } : current), 360)
    return () => window.clearTimeout(timer)
  }, [frames.previous])

  useEffect(() => {
    if (!requestedSrc) return
    setFrames(current => current.current === requestedSrc || current.pending === requestedSrc ? current : { ...current, pending: requestedSrc })
  }, [requestedSrc])

  const hasImage = Boolean(frames.current)
  const name = adapterName ?? adapterId ?? t('recordings.noTitle')
  return <div className="relative mx-auto grid aspect-video max-h-[360px] w-full place-items-center overflow-hidden rounded-lg border border-border bg-muted" role="img" aria-label={hasImage ? t('preview.currentScene') : t('preview.waiting', { name })}>
    {!hasImage && <span className="grid h-full w-full place-items-center" aria-hidden="true">{adapter ? <AdapterMark adapter={adapter} adapterId={adapterId} name={adapterName} size="lg" /> : adapterId || adapterName ? <AdapterMark adapterId={adapterId} name={adapterName} size="lg" /> : <Film className="h-8 w-8 text-muted-foreground" />}</span>}
    {frames.previous && <img src={frames.previous} alt="" aria-hidden="true" className="live-preview-fade-out absolute inset-0 h-full w-full object-contain" />}
    {frames.current && <img src={frames.current} alt="" aria-hidden="true" className={frames.previous ? 'live-preview-fade-in absolute inset-0 h-full w-full object-contain' : 'absolute inset-0 h-full w-full object-contain'} />}
    {frames.pending && <img
      src={frames.pending}
      alt=""
      aria-hidden="true"
      data-preview-pending="true"
      className="hidden"
      onLoad={event => {
        const loaded = event.currentTarget.getAttribute('src') ?? event.currentTarget.src
        setFrames(current => current.pending !== loaded ? current : current.current ? { current: loaded, previous: current.current } : { current: loaded })
      }}
      onError={event => {
        const failed = event.currentTarget.getAttribute('src') ?? event.currentTarget.src
        setFrames(current => current.pending === failed ? { ...current, pending: undefined } : current)
      }}
    />}
    <div className="pointer-events-none absolute bottom-2 left-2 rounded bg-black/60 px-2 py-1 text-[10px] font-medium text-white">{t('preview.current')}</div>
  </div>
}

export function PreviewFrameGrid({ items, recordingId, onSeek, label }: {
  items: PreviewFrame[]; recordingId: string; onSeek: (seconds: number) => void; label?: string
}) {
  const { t } = useI18n()
  const frames = uniquePreviewFrames(items)
  if (!frames.length) return <p className="text-xs text-muted-foreground">{t('preview.empty')}</p>
  return <div role="group" className="grid grid-cols-3 gap-2 sm:grid-cols-4 md:grid-cols-6 xl:grid-cols-8" aria-label={label ?? t('preview.label')}>
    {frames.map(frame => {
      const time = formatPreviewClock(frame.frame_time_seconds)
      return <PreviewFrameCell key={frame.archive_ordinal} frame={frame} recordingId={recordingId} time={time} onSeek={onSeek} />
    })}
  </div>
}

function PreviewFrameCell({ frame, recordingId, time, onSeek }: { frame: PreviewFrame; recordingId: string; time: string; onSeek: (seconds: number) => void }) {
  const { t } = useI18n()
  const src = previewFrameURL(recordingId, frame.archive_ordinal, frame.generated_at)
  const [failedURL, setFailedURL] = useState<string>()
  const spokenTime = formatDuration(frame.frame_time_seconds)
  return <button type="button" onClick={() => onSeek(frame.frame_time_seconds)} aria-label={t('preview.seek', { time: spokenTime })} title={t('preview.seek', { time })} className="focus-ring group relative aspect-video min-w-0 overflow-hidden rounded-md border border-border bg-muted text-left transition hover:border-primary hover:ring-1 hover:ring-primary/40">
        {failedURL === src ? <Film aria-hidden="true" className="absolute inset-0 m-auto h-5 w-5 text-muted-foreground" /> : <img src={src} alt="" loading="lazy" decoding="async" className="h-full w-full object-cover transition group-hover:brightness-90" onError={() => setFailedURL(src)} />}
        <span aria-hidden="true" className="absolute inset-x-0 bottom-0 bg-gradient-to-t from-black/80 to-transparent px-1.5 pb-1 pt-3 text-right font-mono text-[9px] leading-none text-white">{time}</span>
  </button>
}

export function PreviewStateMessage({ state }: { state?: PreviewSummary['state'] }) {
  const { t } = useI18n()
  const stateKey = state && ['disabled', 'unavailable', 'queued', 'processing', 'partial', 'ready', 'failed'].includes(state) ? `preview.state.${state}` as const : 'preview.state.unknown'
  return <p role="status" className="flex items-center gap-2 text-xs text-muted-foreground"><Film className="h-3.5 w-3.5 shrink-0" />{t(stateKey)}</p>
}
