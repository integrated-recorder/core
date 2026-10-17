import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { createMemoryHistory, createRootRoute, createRoute, createRouter, RouterProvider } from '@tanstack/react-router'
import { render, screen, waitFor } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { accountAPI, adaptersAPI, dashboardAPI, productAPI, workflowsAPI } from '@/api'
import { ToastProvider } from '@/components/ui/toast'
import { AppShell } from './app-shell'

afterEach(() => vi.restoreAllMocks())

describe('permission-aware application shell', () => {
  it('hides recording navigation and avoids recording APIs without recording.read', async () => {
    vi.stubGlobal('scrollTo', vi.fn())
    vi.spyOn(accountAPI, 'self').mockResolvedValue({ user_id: 'usr-test', login: 'viewer-custom', roles: ['custom-view'], role: 'custom-view', status: 'active', created_at: '2026-10-01T00:00:00Z' })
    vi.spyOn(accountAPI, 'authorization').mockResolvedValue({ user_id: 'usr-test', login: 'viewer-custom', roles: ['custom-view'], permissions: ['account.profile.self', 'account.password.self', 'account.sessions.self', 'user.preferences.self'] })
    const dashboard = vi.spyOn(dashboardAPI, 'get').mockResolvedValue({} as Awaited<ReturnType<typeof dashboardAPI.get>>)
    const adapters = vi.spyOn(adaptersAPI, 'list').mockResolvedValue([])
    const workflows = vi.spyOn(workflowsAPI, 'list').mockResolvedValue([])
    const search = vi.spyOn(productAPI, 'search').mockResolvedValue({ results: [] })

    const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } })
    const root = createRootRoute()
    const route = createRoute({ getParentRoute: () => root, path: '/', component: AppShell })
    const router = createRouter({ routeTree: root.addChildren([route]), history: createMemoryHistory({ initialEntries: ['/'] }) })
    await router.load()

    render(<QueryClientProvider client={client}><ToastProvider><RouterProvider router={router} /></ToastProvider></QueryClientProvider>)

    await waitFor(() => expect(accountAPI.authorization).toHaveBeenCalled())
    expect(screen.queryByRole('link', { name: '대시보드' })).not.toBeInTheDocument()
    expect(screen.queryByRole('link', { name: '녹화' })).not.toBeInTheDocument()
    expect(screen.queryByRole('textbox', { name: '전체 검색' })).not.toBeInTheDocument()
    expect(dashboard).not.toHaveBeenCalled()
    expect(adapters).not.toHaveBeenCalled()
    expect(workflows).not.toHaveBeenCalled()
    expect(search).not.toHaveBeenCalled()
  })
})
