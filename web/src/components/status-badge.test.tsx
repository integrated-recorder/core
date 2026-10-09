import { render, screen } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { authAPI, userPreferencesAPI } from '@/api'
import { I18nProvider } from '@/i18n/provider'
import { StatusBadge } from './status-badge'

afterEach(() => { vi.restoreAllMocks(); document.documentElement.lang = 'ko-KR' })

describe('recording status labels', () => {
  it.each([
    ['recording', '녹화 중'], ['stopped', '중지됨'], ['completed', '완료'], ['interrupted', '중단됨'],
  ])('renders canonical state %s as %s', (state, label) => {
    render(<StatusBadge state={state} />)
    expect(screen.getByText(label)).toBeInTheDocument()
  })

  it('localizes plugin readiness with the saved user locale', async () => {
    vi.spyOn(authAPI, 'session').mockResolvedValue({ auth_enabled: true, authenticated: true, needs_bootstrap: false })
    vi.spyOn(userPreferencesAPI, 'get').mockResolvedValue({ locale: 'en-US', theme: 'system', timezone: 'system' })
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    render(<QueryClientProvider client={client}><I18nProvider><StatusBadge state="ready" /></I18nProvider></QueryClientProvider>)

    expect(await screen.findByText('Ready')).toBeInTheDocument()
    expect(screen.queryByText('준비됨')).not.toBeInTheDocument()
  })
})
