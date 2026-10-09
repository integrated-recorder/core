import type { ResolvedLocale } from '@/i18n/catalog'

let currentLocale: ResolvedLocale = 'ko-KR'
let currentTimezone = 'system'

export function configureFormatPreferences(locale: ResolvedLocale, timezone: string) {
  currentLocale = locale
  currentTimezone = timezone
}

export function formatNumber(value: number, options: Intl.NumberFormatOptions = {}, locale: ResolvedLocale = currentLocale): string {
  return new Intl.NumberFormat(locale, options).format(value)
}

export function formatDateTime(value: string | Date, options: Intl.DateTimeFormatOptions = {}, locale: ResolvedLocale = currentLocale, timezone = currentTimezone): string {
  const date = value instanceof Date ? value : new Date(value)
  if (Number.isNaN(date.getTime())) return '—'
  const hasDateStyle = options.dateStyle !== undefined || options.timeStyle !== undefined
  const defaults: Intl.DateTimeFormatOptions = hasDateStyle ? {} : {
    year: 'numeric', month: 'numeric', day: 'numeric', hour: '2-digit', minute: '2-digit', hourCycle: 'h23',
  }
  return new Intl.DateTimeFormat(locale, { ...defaults, ...options, ...(timezone === 'system' ? {} : { timeZone: timezone }) }).format(date)
}

export function formatRelativeTime(value: number, unit: Intl.RelativeTimeFormatUnit, locale: ResolvedLocale = currentLocale): string {
  return new Intl.RelativeTimeFormat(locale, { numeric: 'auto' }).format(value, unit)
}

export function resolvedTimeZone(timezone = currentTimezone): string {
  return timezone === 'system' ? new Intl.DateTimeFormat().resolvedOptions().timeZone : timezone
}

export function formatTimeZoneName(value: string | Date = new Date(), locale: ResolvedLocale = currentLocale, timezone = currentTimezone): string {
  const date = value instanceof Date ? value : new Date(value)
  if (Number.isNaN(date.getTime())) return '—'
  return new Intl.DateTimeFormat(locale, { timeZone: resolvedTimeZone(timezone), timeZoneName: 'short' }).format(date)
}

export function validTimeZone(value: string): boolean {
  if (value === 'system') return true
  try { new Intl.DateTimeFormat('en-US', { timeZone: value }); return true } catch { return false }
}

export function getFormatPreferences() { return { locale: currentLocale, timezone: currentTimezone } }
