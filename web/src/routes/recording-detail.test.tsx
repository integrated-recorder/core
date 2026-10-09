import { render, screen } from '@testing-library/react'
import { describe, expect, it } from 'vitest'
import { MetadataTimeline, Timeline } from './recording-detail'
import { recordingHasCommittedMedia, timelineProjectionOmitted } from './recording-detail-utils'

describe('bounded V2 detail projection', () => {
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
})
