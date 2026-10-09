import type { RecordingDetail } from '@/types/api'

export function recordingHasCommittedMedia(item: Partial<Pick<RecordingDetail, 'statistics' | 'segment_count' | 'tracks'>>): boolean {
  return (item.statistics?.segment_count ?? item.segment_count ?? item.tracks?.main?.segments?.length ?? 0) > 0
}

export function timelineProjectionOmitted(item: Partial<Pick<RecordingDetail, 'format_version' | 'statistics' | 'segment_count'>>, loadedSegments: number, loadedGaps: number): boolean {
  if (item.format_version !== 2) return false
  const totalSegments = item.statistics?.segment_count ?? item.segment_count ?? loadedSegments
  const totalGaps = item.statistics?.gap_count ?? loadedGaps
  return totalSegments > loadedSegments || totalGaps > loadedGaps
}
