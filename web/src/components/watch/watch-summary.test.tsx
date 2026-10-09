import { describe, expect, it, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { createMemoryHistory, createRootRoute, createRoute, createRouter, RouterProvider } from '@tanstack/react-router'
import { authAPI, userPreferencesAPI } from '@/api'
import { qk } from '@/api/queries'
import { I18nProvider } from '@/i18n/provider'
import { WatchSummary } from './watch-summary'

describe('dashboard Watch summary', () => {
  it('renders the actual API summary counts', async () => {
    vi.spyOn(window, 'scrollTo').mockImplementation(() => undefined)
    const root = createRootRoute()
    const summaryRoute = createRoute({ getParentRoute: () => root, path: '/', component: () => <WatchSummary summary={{ total: 5, enabled: 4, recording: 2, offline: 1, backoff: 1, attention_required: 0 }} /> })
    const watchesRoute = createRoute({ getParentRoute: () => root, path: '/watches', component: () => null })
    const router = createRouter({ routeTree: root.addChildren([summaryRoute, watchesRoute]), history: createMemoryHistory({ initialEntries: ['/'] }), scrollRestoration: false })
    await router.load()
    render(<RouterProvider router={router} />)
    expect(screen.getByText('등록된 Watch').parentElement).toHaveTextContent('5')
    expect(screen.getByText('활성 Watch').parentElement).toHaveTextContent('4')
    expect(screen.getByText('녹화 중').parentElement).toHaveTextContent('2')
    expect(screen.getByText('오프라인').parentElement).toHaveTextContent('1')
    expect(screen.getByRole('link', { name: /자동 녹화 관리/ })).toHaveAttribute('href', '/watches')
  })

  it('renders dashboard watch summary in the signed-in user locale', async () => {
    vi.spyOn(window, 'scrollTo').mockImplementation(() => undefined)
    vi.spyOn(authAPI, 'session').mockResolvedValue({ auth_enabled: true, authenticated: true, needs_bootstrap: false })
    vi.spyOn(userPreferencesAPI, 'get').mockResolvedValue({ locale: 'en-US', theme: 'system', timezone: 'system' })
    const root = createRootRoute()
    const summaryRoute = createRoute({ getParentRoute: () => root, path: '/', component: () => <WatchSummary summary={{ total: 5, enabled: 4, recording: 2, offline: 1, backoff: 1, attention_required: 0 }} /> })
    const watchesRoute = createRoute({ getParentRoute: () => root, path: '/watches', component: () => null })
    const router = createRouter({ routeTree: root.addChildren([summaryRoute, watchesRoute]), history: createMemoryHistory({ initialEntries: ['/'] }), scrollRestoration: false })
    await router.load()
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    render(<QueryClientProvider client={client}><I18nProvider><RouterProvider router={router} /></I18nProvider></QueryClientProvider>)
    expect(await screen.findByText('Registered watches')).toBeInTheDocument()
    expect(screen.getByText('Enabled watches').parentElement).toHaveTextContent('4')
    expect(screen.getByText('Recording').parentElement).toHaveTextContent('2')
    expect(screen.getByText('Offline').parentElement).toHaveTextContent('1')
    expect(screen.getByText('Retry delayed').parentElement).toHaveTextContent('1')
    expect(screen.getByText('Needs attention').parentElement).toHaveTextContent('0')
    expect(screen.getByRole('link', { name: 'Manage automation' })).toHaveAttribute('href', '/watches')
    client.setQueryData(qk.userPreferences, { locale: 'ko-KR', theme: 'system', timezone: 'system' })
    expect(await screen.findByText('등록된 Watch')).toBeInTheDocument()
  })
})
