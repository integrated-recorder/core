import { createContext, useCallback, useContext, useEffect, useMemo, useState, type ReactNode } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { authAPI } from '@/api'
import { qk, userPreferencesQuery } from '@/api/queries'
import { applyResolvedTheme, resolveTheme, type ThemePreference } from '@/lib/theme'
import { configureFormatPreferences } from '@/lib/formatting'
import { resolveLocale, translate, type LocalePreference, type ResolvedLocale, type TranslationKey, type TranslationValues } from './catalog'
import type { UserPreferences } from '@/types/api'

/* eslint-disable react-refresh/only-export-components -- provider module intentionally exports its hook. */

type I18nContextValue = {
  preferences: UserPreferences
  locale: ResolvedLocale
  t: (key: TranslationKey, values?: TranslationValues) => string
  setPreferences: (preferences: UserPreferences) => void
}

const defaults: UserPreferences = { locale: 'system', theme: 'system', timezone: 'system' }
function prefersDarkMode() {
  return typeof window.matchMedia === 'function' && window.matchMedia('(prefers-color-scheme: dark)').matches
}
const fallback: I18nContextValue = {
  preferences: defaults,
  locale: 'ko-KR',
  t: (key, values) => translate('ko-KR', key, values),
  setPreferences: () => undefined,
}
const I18nContext = createContext<I18nContextValue>(fallback)

export function I18nProvider({ children }: { children: ReactNode }) {
  const queryClient = useQueryClient()
  const [preferences, setPreferences] = useState<UserPreferences>(defaults)
  const session = useQuery({ queryKey: ['auth', 'session'], queryFn: authAPI.session, staleTime: 10_000, retry: false })
  const saved = useQuery({ ...userPreferencesQuery, enabled: session.data?.authenticated === true })
  useEffect(() => {
    if (session.data?.authenticated === false) {
      setPreferences(defaults)
      queryClient.removeQueries({ queryKey: qk.userPreferences })
    }
  }, [queryClient, session.data?.authenticated])
  useEffect(() => { if (session.data?.authenticated && saved.data) setPreferences(saved.data) }, [saved.data, session.data?.authenticated])
  const locale = resolveLocale(preferences.locale)
  useEffect(() => {
    document.documentElement.lang = locale
    configureFormatPreferences(locale, preferences.timezone)
    const update = (theme: ThemePreference) => applyResolvedTheme(resolveTheme(theme, prefersDarkMode()))
    update(preferences.theme)
    if (preferences.theme !== 'system') return
    if (typeof window.matchMedia !== 'function') return
    const media = window.matchMedia('(prefers-color-scheme: dark)')
    const onChange = () => update(preferences.theme)
    media.addEventListener('change', onChange)
    return () => media.removeEventListener('change', onChange)
  }, [locale, preferences.theme, preferences.timezone])
  const t = useCallback((key: TranslationKey, values?: TranslationValues) => translate(locale, key, values), [locale])
  const value = useMemo(() => ({ preferences, locale, t, setPreferences }), [preferences, locale, t])
  return <I18nContext.Provider value={value}>{children}</I18nContext.Provider>
}

export function useI18n() { return useContext(I18nContext) }

export type { LocalePreference }
