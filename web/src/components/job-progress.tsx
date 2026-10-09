import { formatBytes } from '@/lib/utils'
import { formatNumber } from '@/lib/formatting'
import { useI18n } from '@/i18n/provider'
import { jobProgressPercent } from '@/lib/job-progress'
import type { JobProgress } from '@/types/api'

const phaseKeys = new Set(['enumerating', 'preparing_inputs', 'reading', 'verifying', 'checking_objects', 'remuxing', 'publishing', 'writing', 'finalizing', 'queued', 'completed', 'failed', 'canceled', 'interrupted'])
const unitKeys = new Set(['objects', 'segments', 'bytes'])

export function JobProgress({ progress, label }: { progress?: JobProgress; label: string }) {
  const { t } = useI18n()
  const percent = jobProgressPercent(progress)
  const phase = phaseKeys.has(progress?.phase ?? '') ? t(`progress.phase.${progress?.phase}` as Parameters<typeof t>[0]) : t('progress.phase.default')
  const unit = unitKeys.has(progress?.unit ?? '') ? t(`progress.unit.${progress?.unit}` as Parameters<typeof t>[0]) : t('progress.unit.objects')
  const value = progress?.unit === 'bytes' ? formatBytes(progress.current) : formatNumber(progress?.current ?? 0)
  const total = progress?.total == null ? undefined : progress.unit === 'bytes' ? formatBytes(progress.total) : formatNumber(progress.total)
  const count = total ? `${value} / ${total} ${unit}` : `${value} ${unit}`
  const valueText = percent === undefined ? t('progress.indeterminate') : t('progress.percent', { percent: formatNumber(Math.round(percent)) })
  return <div className="mt-2 space-y-1.5" role="status" aria-live="polite">
    <div className="flex flex-wrap items-center justify-between gap-2 text-[11px]"><span className="text-muted-foreground">{phase} · {count}</span>{percent !== undefined && <span className="font-medium tabular-nums">{valueText}</span>}</div>
    <div role="progressbar" aria-label={label} aria-valuemin={0} aria-valuemax={100} aria-valuenow={percent === undefined ? undefined : Math.round(percent)} aria-valuetext={valueText} className="h-2 overflow-hidden rounded-full bg-muted">
      <div className={`h-full rounded-full bg-primary transition-[width] duration-300 ${percent === undefined ? 'w-1/3 animate-pulse' : ''}`} style={percent === undefined ? undefined : { width: `${percent}%` }} />
    </div>
  </div>
}
