import { afterEach, describe, expect, it } from 'vitest'
import { configureFormatPreferences, formatDateTime, formatNumber, formatRelativeTime, formatTimeZoneName, getFormatPreferences, resolvedTimeZone, validTimeZone } from './formatting'

afterEach(() => configureFormatPreferences('ko-KR', 'system'))

describe('locale and time zone formatters', () => {
  it('formats numbers with requested locale', () => {
    expect(formatNumber(1234.5, { maximumFractionDigits: 1 }, 'en-US')).toBe('1,234.5')
    expect(formatNumber(1234.5, { maximumFractionDigits: 1 }, 'ko-KR')).toBe('1,234.5')
  })

  it('uses persisted preference for date time zone and locale', () => {
    configureFormatPreferences('en-US', 'Asia/Seoul')
    expect(getFormatPreferences()).toEqual({ locale: 'en-US', timezone: 'Asia/Seoul' })
    expect(formatDateTime('2025-01-01T00:00:00Z', { dateStyle: 'short', timeStyle: 'short' })).toContain('9:00 AM')
  })

  it('falls back for invalid dates and formats relative time through Intl', () => {
    expect(formatDateTime('not-a-date')).toBe('—')
    expect(formatRelativeTime(-1, 'day', 'en-US')).toBe('yesterday')
    expect(resolvedTimeZone('Asia/Seoul')).toBe('Asia/Seoul')
    expect(formatTimeZoneName('2025-01-01T00:00:00Z', 'en-US', 'Asia/Seoul')).toContain('GMT+9')
    expect(validTimeZone('Asia/Seoul')).toBe(true)
    expect(validTimeZone('Mars/Olympus')).toBe(false)
  })
})
