import { APIError } from '@/api/client'
import { localizedAPIError, resolveLocale } from '@/i18n/catalog'
import { getFormatPreferences } from '@/lib/formatting'

export function errorMessage(error: unknown): string {
  if (error instanceof APIError) {
    const locale = getFormatPreferences().locale
    return localizedAPIError(locale ?? resolveLocale('system'), error.errorCode, error.status)
  }
  return localizedAPIError(getFormatPreferences().locale ?? resolveLocale('system'))
}
