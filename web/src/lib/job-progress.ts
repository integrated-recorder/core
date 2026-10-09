import type { JobProgress } from '@/types/api'

export function jobProgressPercent(progress?: JobProgress): number | undefined {
  if (!progress || progress.indeterminate || !Number.isFinite(progress.total) || !Number.isFinite(progress.current) || (progress.total ?? 0) <= 0) return undefined
  const value = progress.percent != null && Number.isFinite(progress.percent) ? progress.percent : progress.current / progress.total! * 100
  return Math.max(0, Math.min(100, value))
}
