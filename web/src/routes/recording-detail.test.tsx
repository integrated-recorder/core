import { render, screen } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { Events, ExportRow, MetadataTimeline, Timeline } from './recording-detail'
import { archiveIndexNextPageParam, orderRecordingEvents, recordingHasCommittedMedia, timelineProjectionOmitted } from './recording-detail-utils'
import { authAPI, userPreferencesAPI } from '@/api'
import { I18nProvider } from '@/i18n/provider'

afterEach(() => { vi.restoreAllMocks(); document.documentElement.lang = 'ko-KR' })

describe('bounded V2 detail projection', () => {
  it('stops archive pagination when the API returns an empty terminal cursor', () => {
    expect(archiveIndexNextPageParam({ next_cursor: 'next-page' })).toBe('next-page')
    expect(archiveIndexNextPageParam({ next_cursor: '' })).toBeUndefined()
    expect(archiveIndexNextPageParam({})).toBeUndefined()
  })

  it('uses summary counts to enable live playback without loading media history', () => {
    expect(recordingHasCommittedMedia({ segment_count: 12, statistics: { segment_count: 12 } })).toBe(true)
    expect(recordingHasCommittedMedia({ segment_count: 0, statistics: { segment_count: 0 }, tracks: {} })).toBe(false)
  })

  it('does not present omitted V2 history as an empty timeline', () => {
    expect(timelineProjectionOmitted({ format_version: 2, segment_count: 100, statistics: { segment_count: 100, gap_count: 3 } }, 0, 0)).toBe(true)
    expect(timelineProjectionOmitted({ format_version: 2, segment_count: 10, statistics: { segment_count: 10, gap_count: 0 } }, 10, 0)).toBe(false)
    expect(timelineProjectionOmitted({ format_version: 1, segment_count: 10 }, 0, 0)).toBe(false)
  })
})

describe('recording capture timeline', () => {
  it('orders events oldest to newest with a deterministic ID tie-break', () => {
    const ordered = orderRecordingEvents([
      { id: 'z', type: 'recording_started', at: '2026-01-01T00:00:02Z' },
      { id: 'b', type: 'manifest_observed', at: '2026-01-01T00:00:01Z' },
      { id: 'a', type: 'source_refreshed', at: '2026-01-01T00:00:01Z' },
    ])
    expect(ordered.map(item => item.id)).toEqual(['a', 'b', 'z'])
  })
  it('orders stop before completed when operation timestamps tie', () => {
    const ordered = orderRecordingEvents([
      { id: 'completed', type: 'recording_completed', at: '2026-01-01T00:00:01Z' },
      { id: 'stopped', type: 'recording_stopped', at: '2026-01-01T00:00:01Z' },
    ])
    expect(ordered.map(item => item.type)).toEqual(['recording_stopped', 'recording_completed'])
  })
  it('orders integrity and export starts before completion at tied timestamps', () => {
    const ordered = orderRecordingEvents([
      { id: 'integrity-done', type: 'integrity_completed', at: '2026-01-01T00:00:01Z' },
      { id: 'export-done', type: 'export_completed', at: '2026-01-01T00:00:01Z' },
      { id: 'integrity-start', type: 'integrity_started', at: '2026-01-01T00:00:01Z' },
      { id: 'export-start', type: 'export_started', at: '2026-01-01T00:00:01Z' },
    ])
    expect(ordered.map(item => item.id)).toEqual(['export-start', 'integrity-start', 'export-done', 'integrity-done'])
  })
  it('orders each track by source epoch and sequence, not by source sequence alone', () => {
    render(<Timeline
      segments={[
        { trackId: 'main', source_epoch: 1, sequence: 1, archive_ordinal: 0 },
        { trackId: 'main', source_epoch: 0, sequence: 99, archive_ordinal: 1 },
      ]}
      gaps={[{ track_id: 'main', source_epoch: 1, from_sequence: 2, to_sequence: 3 }]}
    />)

    expect(screen.getByLabelText(/트랙 main 수집 타임라인/)).toBeTruthy()
    const markers = screen.getAllByTitle(/^(세그먼트|누락)/).map(marker => marker.getAttribute('title'))
    expect(markers).toEqual([
      '세그먼트 · 세대 0 · 순번 99',
      '세그먼트 · 세대 1 · 순번 1',
      '누락 · 세대 1 · 순번 2–3',
    ])
    const gap = screen.getByTitle('누락 · 세대 1 · 순번 2–3')
    expect(gap).toHaveStyle({ flexGrow: '2' })
  })
})

describe('recording event presentation', () => {
  it.each([
    { locale: 'ko-KR' as const, labels: ['매니페스트 확인', '녹화 중지', '무결성 검사 시작', '내보내기 완료'], count: '74개 세그먼트' },
    { locale: 'en-US' as const, labels: ['Manifest observed', 'Recording stopped', 'Integrity check started', 'Export completed'], count: '74 segments' },
  ])('uses stable event types and localized counts for $locale', async ({ locale, labels, count }) => {
    vi.spyOn(authAPI, 'session').mockResolvedValue({ auth_enabled: true, authenticated: true, needs_bootstrap: false })
    vi.spyOn(userPreferencesAPI, 'get').mockResolvedValue({ locale, theme: 'system', timezone: 'system' })
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    render(<QueryClientProvider client={client}><I18nProvider><Events items={[
      { id: 'manifest', type: 'manifest_observed', at: '2026-01-01T00:00:01Z', count: 74, message: '매니페스트를 확인했습니다. 74 segments' },
      { id: 'stop', type: 'recording_stopped', at: '2026-01-01T00:00:02Z', message: '녹화를 중지했습니다.' },
      { id: 'integrity', type: 'integrity_started', at: '2026-01-01T00:00:03Z', message: 'integrity job started' },
      { id: 'export', type: 'export_completed', at: '2026-01-01T00:00:04Z', message: 'export completed' },
    ]} loading={false} error={undefined} retry={() => undefined} /></I18nProvider></QueryClientProvider>)

    expect(await screen.findByText(labels[0])).toBeInTheDocument()
    expect(screen.getByText(count)).toBeInTheDocument()
    for (const label of labels) expect(screen.getByText(label)).toBeInTheDocument()
    expect(screen.queryByText('매니페스트를 확인했습니다. 74 segments')).not.toBeInTheDocument()
    expect(screen.queryByText('녹화를 중지했습니다.')).not.toBeInTheDocument()
    expect(screen.queryByText('integrity job started')).not.toBeInTheDocument()
    expect(screen.queryByText('export completed')).not.toBeInTheDocument()
  })
})

describe('source metadata timeline', () => {
  it('shows the empty state when an older recording has no metadata history', () => {
    render(<MetadataTimeline data={{ items: [], truncated: false }} loading={false} error={undefined} retry={() => undefined} />)
    expect(screen.getByText('수집된 방송 메타데이터가 없습니다.')).toBeTruthy()
  })

  it('renders source text as text, preserves empty values, and shows chronological revisions', () => {
    const untrustedText = '<script>do not execute</script>'
    const observedAt = '2026-09-29T12:00:00Z'
    const laterAt = '2026-09-29T12:30:00Z'
    const { container } = render(<MetadataTimeline data={{
      current: { observed_at: laterAt, title: 'Updated title', description: '' },
      items: [
        { observed_at: observedAt, title: untrustedText, description: 'plain description' },
        { observed_at: laterAt, title: 'Updated title', description: '' },
      ],
      truncated: true,
    }} loading={false} error={undefined} retry={() => undefined} />)

    expect(screen.getByText(untrustedText)).toBeTruthy()
    expect(container.querySelector('script')).toBeNull()
    expect(screen.getAllByText('— (비어 있음)').length).toBeGreaterThan(0)
    expect(screen.getByText('메타데이터 변경 기록')).toBeTruthy()
    expect(screen.getByText(/보존 한도에 도달/)).toBeTruthy()
  })

  it('localizes metadata and capture timeline with the saved English preference', async () => {
    vi.spyOn(authAPI, 'session').mockResolvedValue({ auth_enabled: true, authenticated: true, needs_bootstrap: false })
    vi.spyOn(userPreferencesAPI, 'get').mockResolvedValue({ locale: 'en-US', theme: 'system', timezone: 'system' })
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    render(<QueryClientProvider client={client}><I18nProvider>
      <MetadataTimeline data={{ current: { observed_at: '2026-01-01T00:00:00Z', title: 'Title' }, items: [{ observed_at: '2026-01-01T00:00:00Z', title: 'Title' }], truncated: false }} loading={false} error={undefined} retry={() => undefined} />
      <Timeline segments={[{ trackId: 'main', source_epoch: 0, sequence: 1 }]} gaps={[]} />
      <ExportRow item={{ id: 'job-1', recording_id: 'recording-1', state: 'failed', error_code: 'export_failed' }} onDelete={() => undefined} deleting={false} />
    </I18nProvider></QueryClientProvider>)

    expect(await screen.findByText('Broadcast metadata')).toBeInTheDocument()
    expect(screen.getByLabelText('Capture timeline for track main')).toBeInTheDocument()
    expect(screen.getByText('The server could not process the request. Try again shortly.')).toBeInTheDocument()
    expect(screen.queryByText('export_failed')).not.toBeInTheDocument()
    expect(document.documentElement.lang).toBe('en-US')
  })
})
