import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { authAPI, userPreferencesAPI } from '@/api'
import { qk } from '@/api/queries'
import { I18nProvider, useI18n } from './provider'

function Probe() {
  const { locale, t, preferences, setPreferences } = useI18n()
  return <><p>{locale} · {t('nav.recordings')}</p><p>{preferences.theme} · {preferences.timezone}</p><button onClick={() => setPreferences({ ...preferences, locale: 'ko-KR' })}>switch locale</button></>
}

afterEach(() => {
  vi.restoreAllMocks()
  document.documentElement.lang = 'en-US'
  document.documentElement.classList.remove('dark')
})

describe('I18nProvider', () => {
  it('loads per-user locale, updates document lang, and presents matching catalog', async () => {
    vi.spyOn(authAPI, 'session').mockResolvedValue({ auth_enabled: true, authenticated: true, needs_bootstrap: false })
    vi.spyOn(userPreferencesAPI, 'get').mockResolvedValue({ locale: 'en-US', theme: 'light', timezone: 'America/New_York' })
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    render(<QueryClientProvider client={queryClient}><I18nProvider><Probe /></I18nProvider></QueryClientProvider>)
    expect(await screen.findByText('en-US · Recordings')).toBeInTheDocument()
    expect(await screen.findByText('light · America/New_York')).toBeInTheDocument()
    await waitFor(() => expect(document.documentElement.lang).toBe('en-US'))
    expect(userPreferencesAPI.get).toHaveBeenCalledTimes(1)
    fireEvent.click(screen.getByRole('button', { name: 'switch locale' }))
    await waitFor(() => expect(document.documentElement.lang).toBe('ko-KR'))
    expect(screen.getByText('ko-KR · 녹화')).toBeInTheDocument()
  })

  it('drops cached preferences at logout before loading next signed-in user preferences', async () => {
    vi.spyOn(authAPI, 'session').mockResolvedValue({ auth_enabled: true, authenticated: true, needs_bootstrap: false })
    vi.spyOn(userPreferencesAPI, 'get')
      .mockResolvedValueOnce({ locale: 'ko-KR', theme: 'dark', timezone: 'Asia/Seoul' })
      .mockResolvedValueOnce({ locale: 'en-US', theme: 'light', timezone: 'America/New_York' })
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    render(<QueryClientProvider client={queryClient}><I18nProvider><Probe /></I18nProvider></QueryClientProvider>)
    expect(await screen.findByText('ko-KR · 녹화')).toBeInTheDocument()
    expect(screen.getByText('dark · Asia/Seoul')).toBeInTheDocument()
    queryClient.setQueryData(qk.session, { auth_enabled: true, authenticated: false, needs_bootstrap: false })
    await waitFor(() => expect(screen.getByText('system · system')).toBeInTheDocument())
    queryClient.setQueryData(qk.session, { auth_enabled: true, authenticated: true, needs_bootstrap: false })
    expect(await screen.findByText('en-US · Recordings')).toBeInTheDocument()
    expect(await screen.findByText('light · America/New_York')).toBeInTheDocument()
    await waitFor(() => expect(document.documentElement.lang).toBe('en-US'))
    expect(userPreferencesAPI.get).toHaveBeenCalledTimes(2)
  })
})
