import { describe, expect, it } from 'vitest'
import { catalogs, localizedAPIError, resolveLocale, translate } from './catalog'

describe('translation catalogs', () => {
  it('keeps Korean and English keys and interpolation placeholders in parity', () => {
    expect(Object.keys(catalogs['ko-KR']).sort()).toEqual(Object.keys(catalogs['en-US']).sort())
    for (const key of Object.keys(catalogs['ko-KR']) as (keyof typeof catalogs['ko-KR'])[]) {
      const ko = catalogs['ko-KR'][key].match(/\{[A-Za-z0-9_]+\}/g)?.sort() ?? []
      const en = catalogs['en-US'][key].match(/\{[A-Za-z0-9_]+\}/g)?.sort() ?? []
      expect(en, key).toEqual(ko)
    }
  })

  it('resolves explicit locale and system locale with a supported fallback', () => {
    expect(resolveLocale('ko-KR', ['en-US'])).toBe('ko-KR')
    expect(resolveLocale('system', ['ko-KR', 'en-US'])).toBe('ko-KR')
    expect(resolveLocale('system', ['en-US', 'ko-KR'])).toBe('en-US')
    expect(resolveLocale('system', ['fr-FR'])).toBe('en-US')
  })

  it('interpolates values and maps only stable error codes to localized copy', () => {
    expect(translate('en-US', 'progress.percent', { percent: 32 })).toBe('32%')
    expect(localizedAPIError('en-US', 'unsupported_archive_format', 400)).toContain('not supported')
    expect(localizedAPIError('ko-KR', 'unknown_private_detail', 500)).toContain('서버가')
    expect(localizedAPIError('en-US', undefined, 409)).not.toContain('raw server text')
  })
})
