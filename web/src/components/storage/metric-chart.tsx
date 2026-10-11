import { useId } from 'react'
import type { StorageMetricSample, StoragePool } from '@/types/api'
import { EmptyState } from '@/components/query-state'
import { formatBytes } from '@/lib/utils'
import { formatDateTime } from '@/lib/formatting'
import { useI18n } from '@/i18n/provider'

type ChartKind = 'throughput' | 'backlog'
type Series = { key: keyof StorageMetricSample; label: string; color: string; format: (value: number) => string }

export function MetricChart({ title, samples, kind, pool }: { title: string; samples: StorageMetricSample[]; kind: ChartKind; pool?: StoragePool }) {
  const { t } = useI18n()
  const uid = useId().replaceAll(':', '')
  if (!samples.length) return <EmptyState title={t('storage.chart.noSamples')} description={t('storage.chart.noSamplesHelp')} />

  const series: Series[] = kind === 'throughput' ? [
    { key: 'read_bytes_per_second', label: t('storage.chart.read'), color: 'hsl(var(--primary))', format: formatRate },
    { key: 'write_bytes_per_second', label: t('storage.chart.write'), color: 'hsl(155 62% 40%)', format: formatRate },
  ] : [
    { key: 'buffer_used_bytes', label: t('storage.chart.buffer'), color: 'hsl(var(--primary))', format: formatBytes },
    { key: 'persist_queue_bytes', label: t('storage.chart.queue'), color: 'hsl(155 62% 40%)', format: formatBytes },
  ]
  const usable = downsample(samples.filter(sample => Number.isFinite(Date.parse(sample.at))).sort((a, b) => Date.parse(a.at) - Date.parse(b.at)), 240)
  if (!usable.length) return <EmptyState title={t('storage.chart.noValidSamples')} />
  const maxSample = Math.max(0, ...usable.flatMap(sample => series.map(item => finiteNumber(sample[item.key]))))
  const ceilings = kind === 'throughput' ? meaningfulCeilings(pool, t) : []
  const maximum = Math.max(1, maxSample, ...ceilings.map(item => item.value))
  const width = 800, height = 280, left = 76, right = 18, top = 20, bottom = 48
  const plotWidth = width - left - right, plotHeight = height - top - bottom
  const x = (index: number) => left + (usable.length <= 1 ? plotWidth / 2 : index / (usable.length - 1) * plotWidth)
  const y = (value: number) => top + plotHeight - Math.max(0, value) / maximum * plotHeight
  const ticks = [0, 0.25, 0.5, 0.75, 1]
  const ids = { title: `${uid}-title`, desc: `${uid}-desc` }
  const unit = kind === 'throughput' ? t('storage.chart.bytePerSecond') : t('storage.chart.byte')

  return <div className="min-w-0" data-testid={`metric-chart-${kind}`}>
    <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
      <h3 className="text-sm font-semibold">{title}</h3>
      <div className="flex flex-wrap gap-x-4 gap-y-1 text-[11px] text-muted-foreground" aria-label={t('storage.chart.legend')}>
        {series.map(item => <span key={item.key} className="inline-flex items-center gap-1.5"><span className="h-0.5 w-3 rounded" style={{ backgroundColor: item.color }} />{item.label}</span>)}
        {ceilings.map(item => <span key={item.key} className="inline-flex items-center gap-1.5"><span className="h-0 w-3 border-t border-dashed" style={{ borderColor: item.color }} />{item.label}</span>)}
      </div>
    </div>
    <div className="w-full overflow-hidden rounded-md border border-border/70 bg-background/50">
      <svg viewBox={`0 0 ${width} ${height}`} className="block h-auto w-full" role="img" aria-labelledby={`${ids.title} ${ids.desc}`}>
        <title id={ids.title}>{title}</title>
        <desc id={ids.desc}>{t('storage.chart.description', { series: series.map(item => item.label).join(', '), unit })}</desc>
        {ticks.map((tick, index) => {
          const value = maximum * tick
          const yy = y(value)
          return <g key={tick}>
            <line x1={left} x2={width - right} y1={yy} y2={yy} stroke="hsl(var(--border))" strokeDasharray={index === 0 ? undefined : '3 5'} />
            <text x={left - 9} y={yy + 4} textAnchor="end" fill="hsl(var(--muted-foreground))" fontSize="11">{kind === 'throughput' ? formatRate(value) : formatBytes(value)}</text>
          </g>
        })}
        <line x1={left} x2={width - right} y1={top + plotHeight} y2={top + plotHeight} stroke="hsl(var(--border))" />
        {ceilings.map(item => <g key={item.key}>
          <line x1={left} x2={width - right} y1={y(item.value)} y2={y(item.value)} stroke={item.color} strokeDasharray="7 5" strokeWidth="1.5" />
          <title>{item.label}: {formatRate(item.value)}</title>
        </g>)}
        {series.map(item => {
          const points = usable.map((sample, index) => `${x(index)},${y(finiteNumber(sample[item.key]))}`).join(' ')
          return <g key={item.key}>
            <polyline points={points} fill="none" stroke={item.color} strokeWidth="2.5" strokeLinejoin="round" strokeLinecap="round" />
            {usable.map((sample, index) => <circle key={`${item.key}-${sample.at}-${index}`} cx={x(index)} cy={y(finiteNumber(sample[item.key]))} r={usable.length < 30 ? 3 : 1.5} fill={item.color}>
              <title>{`${formatSampleTime(sample.at)} · ${item.label}: ${item.format(finiteNumber(sample[item.key]))}`}</title>
            </circle>)}
          </g>
        })}
        {xAxisIndexes(usable.length).map(index => <text key={index} x={x(index)} y={height - 15} textAnchor={index === 0 ? 'start' : index === usable.length - 1 ? 'end' : 'middle'} fill="hsl(var(--muted-foreground))" fontSize="11">{formatSampleTime(usable[index]!.at)}</text>)}
      </svg>
    </div>
    <p className="sr-only">{t('storage.chart.sampleCount', { count: usable.length, unit })}</p>
  </div>
}

function meaningfulCeilings(pool: StoragePool | undefined, t: ReturnType<typeof useI18n>['t']) {
  if (!pool || pool.estimated_ceiling.source === 'unknown') return []
  const labels = { observed: t('storage.chart.ceiling.observed'), configured: t('storage.chart.ceiling.configured'), benchmarked: t('storage.chart.ceiling.benchmarked') }
  const sourceLabel = labels[pool.estimated_ceiling.source]
  if (!sourceLabel) return []
  const result: { key: string; label: string; value: number; color: string }[] = []
  const read = pool.estimated_ceiling.read_bytes_per_second
  const write = pool.estimated_ceiling.write_bytes_per_second
  if (read !== undefined && Number.isFinite(read) && read > 0) result.push({ key: 'read', label: t('storage.chart.readCeiling', { source: sourceLabel }), value: read, color: 'hsl(var(--primary))' })
  if (write !== undefined && Number.isFinite(write) && write > 0) result.push({ key: 'write', label: t('storage.chart.writeCeiling', { source: sourceLabel }), value: write, color: 'hsl(155 62% 40%)' })
  return result
}
function xAxisIndexes(length: number) { return length <= 1 ? [0] : [...new Set([0, Math.floor((length - 1) / 2), length - 1])] }
function downsample<T>(items: T[], limit: number) {
  if (items.length <= limit) return items
  return Array.from({ length: limit }, (_, index) => items[Math.round(index * (items.length - 1) / (limit - 1))]!)
}
function finiteNumber(value: unknown) { return typeof value === 'number' && Number.isFinite(value) ? Math.max(0, value) : 0 }
function formatRate(value: number) { return `${formatBytes(value)}/s` }
function formatSampleTime(value: string) {
  const date = new Date(value)
  if (!Number.isFinite(date.getTime())) return value
  return formatDateTime(date, { month: 'numeric', day: 'numeric', hour: '2-digit', minute: '2-digit', hourCycle: 'h23' })
}
