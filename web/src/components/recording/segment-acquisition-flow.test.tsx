import { act, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { authAPI, recordingsAPI, userPreferencesAPI } from '@/api'
import { I18nProvider } from '@/i18n/provider'
import type { AcquisitionFlowSnapshot } from '@/types/api'
import { SegmentAcquisitionFlow } from './segment-acquisition-flow'

const baseSnapshot: AcquisitionFlowSnapshot = {
  recording_id: 'flow-test', capture_state: 'recording', live: true, archive_revision: 12, timeline_revision: 9,
  first_ordinal: 1, total_ordinals: 3, start_ordinal: 1, has_older: false,
  window: { track_id: 'main', source_epoch: 0, discontinuity_sequence: 0, first_sequence: 501, last_sequence: 503, first_ordinal: 2, last_ordinal: 3, segment_count: 2, observed_at: '2026-10-14T00:00:00Z' },
  segments: [
    { track_id: 'main', source_epoch: 0, discontinuity_sequence: 0, sequence: 501, presentation_ordinal: 1, duration_ms: 2000, state: 'present', observed: true },
    { track_id: 'main', source_epoch: 0, discontinuity_sequence: 0, sequence: 502, presentation_ordinal: 2, duration_ms: 2000, state: 'pending', observed: true },
    { track_id: 'main', source_epoch: 0, discontinuity_sequence: 0, sequence: 503, presentation_ordinal: 3, duration_ms: 2000, state: 'unresolved', observed: true },
  ],
  actors: [{ actor_id: 'local:live:W1:1', node_id: 'local', node_label: 'Local', worker_id: 'W1', acquisition_class: 'live', track_id: 'main', source_epoch: 0, discontinuity_sequence: 0, sequence: 502, presentation_ordinal: 2, state: 'fetching', attempt: 1 }],
}

function renderFlow(snapshot = baseSnapshot) {
  vi.spyOn(authAPI, 'session').mockResolvedValue({ auth_enabled: true, authenticated: true, needs_bootstrap: false })
  vi.spyOn(userPreferencesAPI, 'get').mockResolvedValue({ locale: 'en-US', theme: 'system', timezone: 'system' })
  vi.spyOn(recordingsAPI, 'acquisitionFlow').mockResolvedValue(snapshot)
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  const view = render(<QueryClientProvider client={client}><I18nProvider><SegmentAcquisitionFlow recordingId="flow-test" active={false} /></I18nProvider></QueryClientProvider>)
  return { ...view, client }
}

afterEach(() => {
  vi.restoreAllMocks()
  vi.unstubAllGlobals()
})

describe('recording segment acquisition flow', () => {
  it('renders only observed coordinates, current state colors, worker ownership, and the accepted manifest window', async () => {
    renderFlow()
    await waitFor(() => expect(screen.getAllByTestId('flow-segment')).toHaveLength(3))
    const cells = screen.getAllByTestId('flow-segment')
    expect(cells.map(cell => cell.getAttribute('data-sequence'))).toEqual(['501', '502', '503'])
    expect(cells[0]).toHaveAttribute('data-state', 'present')
    expect(cells[1]).toHaveAttribute('data-state', 'pending')
    expect(cells[1].className).toContain('bg-muted')
    expect(cells[2]).toHaveAttribute('data-state', 'unresolved')
    expect(cells[2].className).toContain('bg-destructive')
    expect(cells[0].className).toContain('bg-emerald-500')
    expect(screen.getByTestId('flow-worker-ring-W1')).toBeInTheDocument()
    expect(screen.getByTestId('flow-worker-ring-W1')).toHaveAttribute('data-presentation-ordinal', '2')
    expect(screen.getByTestId('flow-live-window')).toBeInTheDocument()
    expect(Number(screen.getByTestId('flow-worker-ring-W1').getAttribute('data-presentation-ordinal'))).toBeGreaterThan(1)
    expect(screen.queryByText('504')).not.toBeInTheDocument()
  })

  it('adds a newly observed segment from the next snapshot and keeps recovered media present', async () => {
    const view = renderFlow()
    await waitFor(() => expect(screen.getAllByTestId('flow-segment')).toHaveLength(3))
    const previousBracketPosition = screen.getByTestId('flow-live-window').getAttribute('style')
    const nextSnapshot: AcquisitionFlowSnapshot = {
      ...baseSnapshot,
      total_ordinals: 4,
      window: { ...baseSnapshot.window!, first_sequence: 502, last_sequence: 504, first_ordinal: 3, last_ordinal: 4, segment_count: 3 },
      segments: [
        ...baseSnapshot.segments.map(segment => segment.sequence === 503 ? { ...segment, state: 'present' as const, recovered: true } : segment),
        { ...baseSnapshot.segments[1]!, sequence: 504, presentation_ordinal: 4, state: 'pending', observed: true },
      ],
      actors: [],
    }
    act(() => view.client.setQueryData(['recording', 'flow-test', 'acquisition-flow', 'head'], nextSnapshot))
    await waitFor(() => expect(screen.getAllByTestId('flow-segment')).toHaveLength(4))
    const cells = screen.getAllByTestId('flow-segment')
    expect(cells.map(cell => cell.getAttribute('data-sequence'))).toEqual(['501', '502', '503', '504'])
    expect(cells[2]).toHaveAttribute('data-state', 'present')
    expect(cells[2].className).toContain('bg-emerald-500')
    expect(screen.getByTestId('flow-live-window').getAttribute('style')).not.toBe(previousBracketPosition)
    fireEvent.click(cells[2]!)
    expect(screen.getByTestId('flow-selection')).toHaveTextContent('Recovered')
    expect(screen.getByTestId('flow-live-window')).toBeInTheDocument()
  })

  it('keeps a short live strip at the right edge so newly observed segments move it forward', async () => {
    const resizeCallbacks: ResizeObserverCallback[] = []
    vi.stubGlobal('ResizeObserver', class {
      constructor(callback: ResizeObserverCallback) { resizeCallbacks.push(callback) }
      observe() {}
      disconnect() {}
      unobserve() {}
    })
    const view = renderFlow()
    await waitFor(() => expect(screen.getAllByTestId('flow-segment')).toHaveLength(3))
    const scroller = screen.getByTestId('flow-scroll')
    Object.defineProperty(scroller, 'clientWidth', { configurable: true, value: 600 })
    act(() => resizeCallbacks[0]?.([], {} as ResizeObserver))
    await waitFor(() => expect(screen.getByTestId('flow-canvas')).toHaveAttribute('data-leading-space', '552'))
    const firstBefore = Number((screen.getAllByTestId('flow-segment')[0] as HTMLElement).parentElement?.style.left.replace('px', ''))
    const liveEdgeBefore = Number((screen.getAllByTestId('flow-segment')[2] as HTMLElement).parentElement?.style.left.replace('px', ''))
    expect(firstBefore).toBe(552)
    expect(liveEdgeBefore).toBe(584)

    const nextSnapshot: AcquisitionFlowSnapshot = {
      ...baseSnapshot,
      total_ordinals: 4,
      window: { ...baseSnapshot.window!, first_sequence: 502, last_sequence: 504, first_ordinal: 3, last_ordinal: 4, segment_count: 3 },
      segments: [...baseSnapshot.segments, { ...baseSnapshot.segments[2]!, sequence: 504, presentation_ordinal: 4, state: 'pending', observed: true }],
      actors: [],
    }
    act(() => view.client.setQueryData(['recording', 'flow-test', 'acquisition-flow', 'head'], nextSnapshot))
    await waitFor(() => expect(screen.getAllByTestId('flow-segment')).toHaveLength(4))
    const cells = screen.getAllByTestId('flow-segment')
    expect(Number((cells[0] as HTMLElement).parentElement?.style.left.replace('px', ''))).toBe(536)
    expect(Number((cells[3] as HTMLElement).parentElement?.style.left.replace('px', ''))).toBe(584)
    expect(cells[3]).toHaveAttribute('data-sequence', '504')
  })

  it('uses the same ownership ring for historical recovery outside the current manifest window', async () => {
    const historicalSnapshot: AcquisitionFlowSnapshot = {
      ...baseSnapshot,
      actors: [{ actor_id: 'local:historical:H1:4', node_id: 'local', node_label: 'Local', worker_id: 'H1', acquisition_class: 'historical', track_id: 'main', source_epoch: 0, discontinuity_sequence: 0, sequence: 503, presentation_ordinal: 3, state: 'fetching', attempt: 1 }],
    }
    renderFlow(historicalSnapshot)
    await waitFor(() => expect(screen.getAllByTestId('flow-segment')).toHaveLength(3))
    const ring = screen.getByTestId('flow-worker-ring-H1')
    expect(ring).toHaveAttribute('data-presentation-ordinal', '3')
    expect(ring.getAttribute('class')).not.toContain('border-dashed')
    fireEvent.click(screen.getAllByTestId('flow-segment')[2]!)
    expect(screen.getByTestId('flow-selection')).toHaveTextContent('Recovery worker H1')
  })

  it('shows retry with a dashed segment outline and changes recovered gaps to present', async () => {
    const snapshot: AcquisitionFlowSnapshot = {
      ...baseSnapshot,
      segments: baseSnapshot.segments.map(segment => segment.sequence === 503 ? { ...segment, state: 'present', recovered: true, retrying: true } : segment),
      actors: [],
    }
    renderFlow(snapshot)
    await waitFor(() => expect(screen.getAllByTestId('flow-segment')).toHaveLength(3))
    const recovered = screen.getAllByTestId('flow-segment')[2]
    expect(recovered).toHaveAttribute('data-state', 'present')
    expect(screen.getByTestId('flow-retry-ring')).toBeInTheDocument()
    expect(screen.getByTestId('flow-retry-ring').getAttribute('class')).toContain('border-dashed')
    fireEvent.click(recovered)
    expect(screen.getByTestId('flow-selection')).toHaveTextContent('Recovered')
  })

  it('bounds DOM rendering for long recordings and restores follow mode after manual scrolling', async () => {
    const longHistory: AcquisitionFlowSnapshot = {
      ...baseSnapshot, first_ordinal: 1, total_ordinals: 18_000, start_ordinal: 17_905,
      segments: Array.from({ length: 96 }, (_, index) => ({ ...baseSnapshot.segments[0], sequence: 17_905 + index, presentation_ordinal: 17_905 + index })),
    }
    const view = renderFlow(longHistory)
    await waitFor(() => expect(screen.getAllByTestId('flow-segment')).toHaveLength(96))
    expect(view.container.querySelectorAll('[data-testid="flow-segment"]')).toHaveLength(96)
    const scroller = screen.getByTestId('flow-scroll')
    Object.defineProperty(scroller, 'scrollWidth', { configurable: true, value: 900 })
    Object.defineProperty(scroller, 'clientWidth', { configurable: true, value: 200 })
    Object.defineProperty(scroller, 'scrollLeft', { configurable: true, writable: true, value: 0 })
    fireEvent.wheel(scroller, { deltaX: -500 })
    fireEvent.scroll(scroller)
    expect(await screen.findByTestId('flow-follow-live')).toBeInTheDocument()
    fireEvent.click(screen.getByTestId('flow-follow-live'))
    expect(screen.queryByTestId('flow-follow-live')).not.toBeInTheDocument()
  })
})
