import { clsx, type ClassValue } from 'clsx'
import { twMerge } from 'tailwind-merge'
import { formatDateTime, formatNumber, getFormatPreferences } from '@/lib/formatting'

export function cn(...inputs: ClassValue[]) { return twMerge(clsx(inputs)) }
export function formatBytes(value?: number | null) {
  if (value == null || !Number.isFinite(value)) return '—'
  if (value < 1024) return `${formatNumber(value)} B`
  const units = ['KB', 'MB', 'GB', 'TB', 'PB']
  let size = value / 1024, index = 0
  while (size >= 1024 && index < units.length - 1) { size /= 1024; index++ }
  const fractionDigits = size >= 100 ? 0 : size >= 10 ? 1 : 2
  return `${formatNumber(size, { minimumFractionDigits: fractionDigits, maximumFractionDigits: fractionDigits })} ${units[index]}`
}
export function formatDuration(seconds?: number | null) {
  if (seconds == null || !Number.isFinite(seconds) || seconds < 0) return '—'
  const s = Math.floor(seconds), h = Math.floor(s / 3600), m = Math.floor((s % 3600) / 60), r = s % 60
  const formatUnit = (value: number, unit: Intl.NumberFormatOptions['unit']) => new Intl.NumberFormat(getFormatPreferences().locale, { style: 'unit', unit, unitDisplay: 'short' }).format(value)
  if (h) return `${formatUnit(h, 'hour')} ${formatUnit(m, 'minute')}`
  if (m) return `${formatUnit(m, 'minute')} ${formatUnit(r, 'second')}`
  return formatUnit(r, 'second')
}
export function formatDate(value?: string | null) {
  if (!value) return '—'
  return formatDateTime(value)
}
export function humanize(value?: string) { return value?.replaceAll('_', ' ').replace(/\b\w/g, c => c.toUpperCase()) ?? 'Unknown' }
export function resourceLabel(resource?: { resource_type: string; resource_id: string; display_name?: string }) {
  return resource ? `${resource.display_name ? `${resource.display_name} · ` : ''}${resource.resource_type} / ${resource.resource_id}` : '—'
}
