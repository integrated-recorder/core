import type { RecordingDetail } from '@/types/api'

export type RecordingEventView = { id: string; at: string; type?: string }

export function archiveIndexNextPageParam(page: { next_cursor?: string }) {
  return page.next_cursor || undefined
}

const eventTieOrder: Record<string, number> = {
  recording_started: 10,
  manifest_observed: 20,
  source_refreshed: 20,
  segment_retry: 20,
  gap_detected: 20,
  gap_committed: 20,
  integrity_started: 30,
  export_started: 30,
  integrity_completed: 40,
  export_completed: 40,
  recording_stopped: 50,
  recording_completed: 60,
  recording_interrupted: 60,
}

export function orderRecordingEvents<T extends RecordingEventView>(items: readonly T[]): T[] {
  return [...items].sort((left, right) => {
    const leftAt = Date.parse(left.at)
    const rightAt = Date.parse(right.at)
    const leftTime = Number.isNaN(leftAt) ? Number.POSITIVE_INFINITY : leftAt
    const rightTime = Number.isNaN(rightAt) ? Number.POSITIVE_INFINITY : rightAt
    const leftTieOrder = eventTieOrder[left.type?.toLowerCase() ?? ''] ?? 100
    const rightTieOrder = eventTieOrder[right.type?.toLowerCase() ?? ''] ?? 100
    return leftTime - rightTime || leftTieOrder - rightTieOrder || (left.id < right.id ? -1 : left.id > right.id ? 1 : 0)
  })
}

export function recordingHasCommittedMedia(item: Partial<Pick<RecordingDetail, 'statistics' | 'segment_count' | 'tracks'>>): boolean {
  return (item.statistics?.segment_count ?? item.segment_count ?? item.tracks?.main?.segments?.length ?? 0) > 0
}

export function timelineProjectionOmitted(item: Partial<Pick<RecordingDetail, 'format_version' | 'statistics' | 'segment_count'>>, loadedSegments: number, loadedGaps: number): boolean {
  if (item.format_version !== 2) return false
  const totalSegments = item.statistics?.segment_count ?? item.segment_count ?? loadedSegments
  const totalGaps = item.statistics?.gap_count ?? loadedGaps
  return totalSegments > loadedSegments || totalGaps > loadedGaps
}
