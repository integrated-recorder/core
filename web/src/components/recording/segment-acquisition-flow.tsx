import { useEffect, useMemo, useRef, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { recordingsAPI } from '@/api'
import { Button } from '@/components/ui/button'
import { formatNumber } from '@/lib/formatting'
import { formatDuration } from '@/lib/utils'
import { useI18n } from '@/i18n/provider'
import type { AcquisitionFlowActor } from '@/types/api'

const PAGE_SIZE = 96

export function SegmentAcquisitionFlow({ recordingId, active }: { recordingId: string; active: boolean }) {
  const { t } = useI18n()
  const scroller = useRef<HTMLDivElement>(null)
  const programmaticScroll = useRef(false)
  const scrollFrame = useRef<number | undefined>(undefined)
  const followTimer = useRef<number | undefined>(undefined)
  const [pageStart, setPageStart] = useState(0)
  const [following, setFollowing] = useState(true)
  const followingRef = useRef(true)
  const [viewportWidth, setViewportWidth] = useState(0)
  const [selectedOrdinal, setSelectedOrdinal] = useState<number>()
  const head = useQuery({
    queryKey: ['recording', recordingId, 'acquisition-flow', 'head'],
    queryFn: () => recordingsAPI.acquisitionFlow(recordingId, 0, PAGE_SIZE),
    enabled: Boolean(recordingId), staleTime: 0,
    refetchInterval: active ? 3000 : false, refetchIntervalInBackground: false,
  })
  const page = useQuery({
    queryKey: ['recording', recordingId, 'acquisition-flow', 'page', pageStart],
    queryFn: () => recordingsAPI.acquisitionFlow(recordingId, pageStart, PAGE_SIZE),
    enabled: Boolean(recordingId) && pageStart > 0, staleTime: 0, gcTime: 0,
    refetchIntervalInBackground: false,
  })
  const snapshot = pageStart === 0 ? head.data : page.data
  const hasSnapshot = Boolean(snapshot)
  const first = head.data?.first_ordinal ?? snapshot?.first_ordinal ?? 1
  const total = Math.max(head.data?.total_ordinals ?? 0, snapshot?.total_ordinals ?? 0)
  const count = Math.max(0, total - first + 1)
  const step = count > 200_000 ? 6 : count > 40_000 ? 8 : count > 8_000 ? 10 : 16
  const cellWidth = count > 0 ? Math.min(step, 12_000_000 / count) : step
  const timelineWidth = count > 0 ? count * cellWidth : 160
  const canvasWidth = Math.max(viewportWidth, timelineWidth)
  const leadingSpace = Math.max(0, canvasWidth - timelineWidth)
  const segments = snapshot?.segments ?? []
  const actors = useMemo(() => snapshot?.actors ?? [], [snapshot?.actors])
  const actorsByOrdinal = useMemo(() => {
    const result = new Map<number, AcquisitionFlowActor>()
    for (const actor of actors) if (actor.presentation_ordinal) result.set(actor.presentation_ordinal, actor)
    return result
  }, [actors])
  const selected = selectedOrdinal == null ? undefined : segments.find(segment => segment.presentation_ordinal === selectedOrdinal)
  const selectedActor = selected ? actorsByOrdinal.get(selected.presentation_ordinal) : undefined
  const selectedAttempt = selectedActor?.attempt ?? selected?.attempt
  const manifestWindow = head.data?.window ?? snapshot?.window
  const liveEdge = head.data?.total_ordinals
  const liveWindowFirst = head.data?.window?.first_ordinal
  const liveWindowLast = head.data?.window?.last_ordinal

  useEffect(() => {
    const element = scroller.current
    if (!element) return
    const updateWidth = () => setViewportWidth(element.clientWidth)
    updateWidth()
    window.addEventListener('resize', updateWidth)
    const observer = typeof ResizeObserver === 'undefined' ? undefined : new ResizeObserver(updateWidth)
    observer?.observe(element)
    return () => {
      window.removeEventListener('resize', updateWidth)
      observer?.disconnect()
    }
  }, [hasSnapshot])

  useEffect(() => {
    const element = scroller.current
    if (!element || !liveEdge || !followingRef.current) return
    programmaticScroll.current = true
    const reducedMotion = window.matchMedia?.('(prefers-reduced-motion: reduce)').matches
    if (typeof element.scrollTo === 'function') element.scrollTo({ left: element.scrollWidth, behavior: reducedMotion ? 'auto' : 'smooth' })
    else element.scrollLeft = element.scrollWidth
    if (followTimer.current != null) window.clearTimeout(followTimer.current)
    followTimer.current = window.setTimeout(() => { programmaticScroll.current = false }, 1000)
  }, [liveEdge, liveWindowFirst, liveWindowLast])

  useEffect(() => () => {
    if (scrollFrame.current != null) cancelAnimationFrame(scrollFrame.current)
    if (followTimer.current != null) window.clearTimeout(followTimer.current)
  }, [])

  function updatePageFromScroll() {
    const element = scroller.current
    if (!element || count < 1) return
    const distanceFromEnd = element.scrollWidth - element.clientWidth - element.scrollLeft
    if (!programmaticScroll.current) setFollowMode(distanceFromEnd <= cellWidth * 2)
    const visibleOrdinal = Math.min(total, first + Math.floor(element.scrollLeft / cellWidth))
    const wanted = first + Math.floor(Math.max(0, visibleOrdinal - first) / PAGE_SIZE) * PAGE_SIZE
    const latestStart = Math.max(first, total - PAGE_SIZE + 1)
    setPageStart(wanted >= latestStart ? 0 : wanted)
  }

  function goLive() {
    setFollowMode(true)
    setPageStart(0)
    const element = scroller.current
    if (!element) return
    const reducedMotion = window.matchMedia?.('(prefers-reduced-motion: reduce)').matches
    programmaticScroll.current = true
    if (typeof element.scrollTo === 'function') element.scrollTo({ left: element.scrollWidth, behavior: reducedMotion ? 'auto' : 'smooth' })
    else element.scrollLeft = element.scrollWidth
    if (followTimer.current != null) window.clearTimeout(followTimer.current)
    followTimer.current = window.setTimeout(() => { programmaticScroll.current = false }, 1000)
  }

  function setFollowMode(value: boolean) {
    followingRef.current = value
    setFollowing(value)
  }

  function takeManualControl() {
    programmaticScroll.current = false
  }

  if (head.isError && !head.data) return null
  if (!snapshot && !head.isLoading) return null
  if (count === 0 && !head.data?.live && !actors.length) return null

  const canvasSegments = [...segments].sort((a, b) => a.presentation_ordinal - b.presentation_ordinal)
  return <section className="mb-5 min-w-0" aria-label={t('detail.flow.title')} data-testid="segment-acquisition-flow">
    <div className="mb-2 flex min-h-7 items-center justify-between gap-3">
      <div className="flex items-center gap-2 text-sm font-medium">
        <span className={`h-2 w-2 rounded-full ${head.data?.live ? 'bg-emerald-500' : actors.length ? 'bg-sky-500' : 'bg-muted-foreground/40'}`} aria-hidden="true" />
        <span>{head.data?.live ? t('detail.flow.recording') : actors.length ? t('detail.flow.recovering') : t('detail.flow.title')}</span>
      </div>
      {!following && <Button size="sm" variant="ghost" className="h-7 px-2 text-xs" onClick={goLive} data-testid="flow-follow-live">● {t('detail.flow.followLive')}</Button>}
    </div>
    <div ref={scroller} className="relative overflow-x-auto overscroll-x-contain rounded-lg border border-border/70 bg-muted/20 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring" tabIndex={0} aria-label={t('detail.flow.scrollLabel')} data-testid="flow-scroll" onWheel={takeManualControl} onTouchMove={takeManualControl} onPointerDown={takeManualControl} onKeyDown={event => {
      if (['ArrowLeft', 'ArrowRight', 'Home', 'End', 'PageUp', 'PageDown'].includes(event.key)) takeManualControl()
    }} onScroll={() => {
      if (scrollFrame.current != null) cancelAnimationFrame(scrollFrame.current)
      scrollFrame.current = requestAnimationFrame(updatePageFromScroll)
    }}>
      <div className="relative h-[76px]" style={{ width: canvasWidth }} data-testid="flow-canvas" data-ordinal-count={count} data-leading-space={leadingSpace}>
        {manifestWindow?.first_ordinal && manifestWindow.last_ordinal && manifestWindow.last_ordinal >= manifestWindow.first_ordinal && <>
          <div className="absolute top-5 h-2 border-x border-t border-sky-500/70" style={{ left: leadingSpace + (manifestWindow.first_ordinal - first) * cellWidth, width: Math.max(cellWidth, (manifestWindow.last_ordinal - manifestWindow.first_ordinal + 1) * cellWidth) }} data-testid="flow-live-window" />
          <span className="absolute top-0 -translate-x-1/2 whitespace-nowrap text-[10px] text-sky-700 dark:text-sky-300" style={{ left: leadingSpace + ((manifestWindow.first_ordinal + manifestWindow.last_ordinal) / 2 - first) * cellWidth }}>{t('detail.flow.liveWindow')}</span>
        </>}
        {canvasSegments.map(segment => {
          const ordinal = segment.presentation_ordinal
          const actor = actorsByOrdinal.get(ordinal)
          const left = leadingSpace + (ordinal - first) * cellWidth
          const statusLabel = segment.retrying ? t('detail.flow.retrying') : segment.state === 'present' ? t('detail.flow.present') : segment.state === 'unresolved' ? t('detail.flow.failed') : t('detail.flow.pending')
          const ariaLabel = actor ? t('detail.flow.segmentWithWorker', { sequence: formatNumber(segment.sequence), status: statusLabel, worker: actor.worker_id }) : t('detail.flow.segment', { sequence: formatNumber(segment.sequence), status: statusLabel })
          return <div key={`${segment.track_id}-${segment.source_epoch}-${segment.discontinuity_sequence}-${segment.sequence}`} className="absolute top-[34px] h-7" style={{ left, width: Math.max(3, cellWidth - 3) }}>
            {(actor || segment.retrying) && <div className={`pointer-events-none absolute -inset-x-[3px] -inset-y-[3px] rounded-md border ${segment.retrying || actor?.state === 'retry_wait' || actor?.state === 'refreshing' || (actor?.attempt ?? 0) > 1 ? 'border-dashed border-sky-500 motion-safe:animate-pulse' : 'border-sky-500/90'} motion-reduce:animate-none`} data-testid={actor ? `flow-worker-ring-${actor.worker_id}` : 'flow-retry-ring'} data-worker-id={actor?.worker_id} data-presentation-ordinal={ordinal}>
              {actor && <span className="absolute -top-2 left-1/2 -translate-x-1/2 rounded-full bg-sky-600 px-1.5 py-px text-[9px] font-semibold leading-3 text-white">{actor.worker_id}</span>}
            </div>}
            <button type="button" className={`h-full w-full rounded-[5px] border transition-colors duration-500 motion-reduce:transition-none focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring ${segment.state === 'present' ? 'border-emerald-600 bg-emerald-500' : segment.state === 'unresolved' ? 'border-destructive bg-destructive/75' : 'border-border bg-muted'} ${segment.inferred ? 'border-dashed' : ''} ${selectedOrdinal === ordinal ? 'ring-2 ring-foreground/70 ring-offset-1 ring-offset-background' : ''}`} style={{ minWidth: 3 }} aria-label={ariaLabel} title={ariaLabel} aria-pressed={selectedOrdinal === ordinal} data-testid="flow-segment" data-sequence={segment.sequence} data-state={segment.state} data-ordinal={ordinal} onClick={() => setSelectedOrdinal(ordinal)} />
          </div>
        })}
      </div>
    </div>
    {!manifestWindow && head.data?.live && <p className="mt-1 text-[11px] text-muted-foreground">{t('detail.flow.waiting')}</p>}
    {head.error && <p className="mt-1 text-[11px] text-muted-foreground" role="status">{t('detail.flow.loadFailed')}</p>}
    {selected && <div className="mt-2 flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-muted-foreground" role="status" data-testid="flow-selection">
      <span className="font-medium text-foreground">{t('detail.flow.segmentTitle', { sequence: formatNumber(selected.sequence) })}</span>
      <span>{t('detail.flow.currentState')}: {selected.state === 'present' ? t('detail.flow.present') : selected.state === 'unresolved' ? t('detail.flow.failed') : t('detail.flow.pending')}</span>
      {selectedActor && <span>{selectedActor.acquisition_class === 'historical' ? t('detail.flow.historicalWorker', { worker: selectedActor.worker_id }) : t('detail.flow.liveWorker', { worker: selectedActor.worker_id })}{selectedAttempt != null && selectedAttempt > 0 ? ` · ${t('detail.flow.attempt', { count: selectedAttempt })}` : ''}</span>}
      {selected.recovered && <span>{t('detail.flow.recovered')}</span>}
      {selected.duration_ms != null && <span>{t('detail.flow.duration')}: {formatDuration(selected.duration_ms / 1000)}</span>}
    </div>}
    {head.data?.live && head.data?.total_ordinals != null && <div className="sr-only" aria-live="polite">{t('detail.flow.observedCount', { count: formatNumber(head.data.total_ordinals) })}</div>}
  </section>
}
