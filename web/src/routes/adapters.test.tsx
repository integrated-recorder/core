import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { createMemoryHistory, createRootRoute, createRoute, createRouter, RouterProvider } from '@tanstack/react-router'
import { fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { AdaptersPage, PluginsPage } from './adapters'
import { ToastProvider } from '@/components/ui/toast'
import { qk } from '@/api/queries'

afterEach(() => { vi.unstubAllGlobals(); vi.restoreAllMocks() })

async function renderAdapterSurface(Component: React.ComponentType, installedAdapters: unknown[] = []) {
  vi.spyOn(window, 'scrollTo').mockImplementation(() => undefined)
  const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } })
  client.setQueryData(['adapters'], installedAdapters)
  client.setQueryData(['user', 'account'], { user_id: 'usr-00000000000000000000000000000001', login: 'owner', roles: ['owner'], role: 'owner', status: 'active', created_at: '2026-10-01T00:00:00Z' })
  client.setQueryData(qk.accountAuthorization, { user_id: 'usr-00000000000000000000000000000001', login: 'owner', roles: ['owner'], permissions: ['plugin.read', 'plugin.manage', 'plugin.configure'] })
  const root = createRootRoute()
  const surfaceRoute = createRoute({ getParentRoute: () => root, path: '/', component: () => <Component /> })
  const storageRoute = createRoute({ getParentRoute: () => root, path: '/storage/locations', component: () => null })
  const router = createRouter({ routeTree: root.addChildren([surfaceRoute, storageRoute]), history: createMemoryHistory({ initialEntries: ['/'] }), scrollRestoration: false })
  await router.load()
  return render(<QueryClientProvider client={client}><ToastProvider><RouterProvider router={router} /></ToastProvider></QueryClientProvider>)
}
const renderAdapters = (installedAdapters: unknown[] = []) => renderAdapterSurface(AdaptersPage, installedAdapters)
const renderPlugins = () => renderAdapterSurface(PluginsPage)

const availableRegistry = { state: 'ready', plugins: [{ id: 'demo', name: 'Demo Adapter', available_version: '1.2.0', installed: false, update_available: false }] }

describe('adapter discovery controls', () => {
  it('requests an immediate Host reconciliation and shows its safe result', async () => {
    const fetchMock = vi.fn(async (_input: RequestInfo | URL, init?: RequestInit) => {
      void init
      const url = String(_input)
      const body = url === '/api/runtime/adapters/reconcile'
        ? { state: 'activated', active_adapter_count: 2, rejected_count: 0, generation_id: 'gen-123' }
        : []
      return new Response(JSON.stringify(body), { status: 200, headers: { 'content-type': 'application/json' } })
    })
    vi.stubGlobal('fetch', fetchMock)

    await renderAdapters()
    fireEvent.click(await screen.findByRole('button', { name: '플러그인 다시 검색' }))

    await waitFor(() => expect(fetchMock.mock.calls.some(([url, init]) => url === '/api/runtime/adapters/reconcile' && init?.method === 'POST')).toBe(true))
    expect(await screen.findByText('새 플러그인 세대를 활성화했습니다.')).toBeInTheDocument()
    expect(screen.getByRole('heading', { name: '소스' })).toBeInTheDocument()
    expect(screen.queryByRole('heading', { name: 'Plugin Registry' })).not.toBeInTheDocument()
    expect(fetchMock.mock.calls.some(([url]) => url === '/api/runtime/plugins')).toBe(false)
    expect(screen.queryByText(/운영자 executable 가져오기는 기본적으로 비활성화되어 있습니다/)).not.toBeInTheDocument()
    expect(screen.queryByText(/어댑터 디렉터리에 넣으면/)).not.toBeInTheDocument()
  })

  it('reports an unconfigured registry and disables refresh while preserving the distinction from outage', async () => {
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input)
      const body = url === '/api/runtime/plugins'
        ? { state: 'not_configured', plugins: [] }
        : []
      return new Response(JSON.stringify(body), { status: 200, headers: { 'content-type': 'application/json' } })
    })
    vi.stubGlobal('fetch', fetchMock)

    await renderPlugins()

    await waitFor(() => expect(screen.getAllByRole('status').some(status => /Registry가 설정되지 않았습니다/.test(status.textContent ?? ''))).toBe(true))
    expect(screen.getByRole('button', { name: 'Registry 새로고침' })).toBeDisabled()
    expect(screen.queryByText(/새 Source Plugin 목록을 표시하지 못했습니다/)).not.toBeInTheDocument()
    expect(screen.getAllByRole('status')).toHaveLength(1)
  })

  it('shows the registry and installs an available plugin through the Host API', async () => {
    let installed = false
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input)
      let body: unknown = []
      if (url === '/api/runtime/plugins') body = installed
        ? { state: 'ready', plugins: [{ id: 'demo', name: 'Demo Adapter', available_version: '1.2.0', installed_version: '1.2.0', installed: true, update_available: false }] }
        : availableRegistry
      if (url === '/api/runtime/plugins/demo/install' && init?.method === 'POST') {
        installed = true
        body = { state: 'ready', plugins: [{ id: 'demo', name: 'Demo Adapter', available_version: '1.2.0', installed_version: '1.2.0', installed: true, update_available: false }] }
      }
      return new Response(JSON.stringify(body), { status: 200, headers: { 'content-type': 'application/json' } })
    })
    vi.stubGlobal('fetch', fetchMock)

    await renderPlugins()
    expect(await screen.findByText('Demo Adapter')).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: '설치' }))

    await waitFor(() => expect(fetchMock.mock.calls.some(([url, init]) => url === '/api/runtime/plugins/demo/install' && init?.method === 'POST')).toBe(true))
    expect(await screen.findByText('플러그인을 설치하고 새 plugin generation을 활성화했습니다.')).toBeInTheDocument()
  })

  it('separates typed source and storage registry entries and does not activate storage on install', async () => {
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input)
      if (url === '/api/runtime/plugins/storage-sample/install' && init?.method === 'POST') {
        return new Response(JSON.stringify({ state: 'ready', plugins: [{ id: 'storage-sample', type: 'storage', name: 'Sample Storage', available_version: '1.0.0', installed_version: '1.0.0', installed: true, update_available: false }] }), { status: 200, headers: { 'content-type': 'application/json' } })
      }
      const body = url === '/api/runtime/plugins' ? { state: 'ready', plugins: [
        { id: 'source-sample', type: 'source', name: 'Sample Source', available_version: '2.0.0', installed: false, update_available: false },
        { id: 'storage-sample', type: 'storage', name: 'Sample Storage', available_version: '1.0.0', installed: false, update_available: false },
      ] } : []
      return new Response(JSON.stringify(body), { status: 200, headers: { 'content-type': 'application/json' } })
    })
    vi.stubGlobal('fetch', fetchMock)

    await renderPlugins()
    expect(await screen.findByRole('region', { name: 'Source Plugin' })).toHaveTextContent('Sample Source')
    expect(screen.getByRole('region', { name: 'Storage Provider' })).toHaveTextContent('Sample Storage')
    expect(screen.getByText(/기본 저장소는 바뀌지 않습니다/)).toBeInTheDocument()
    const storageSection = screen.getByRole('region', { name: 'Storage Provider' })
    fireEvent.click(within(storageSection).getByRole('button', { name: '설치' }))
    await waitFor(() => expect(fetchMock.mock.calls.some(([url, init]) => url === '/api/runtime/plugins/storage-sample/install' && init?.method === 'POST')).toBe(true))
    expect(await screen.findByText('Storage provider 실행 파일을 설치했습니다.')).toBeInTheDocument()
    expect(screen.queryByText('기본 저장소로 활성화됨')).not.toBeInTheDocument()
    expect(fetchMock.mock.calls.some(([url]) => String(url).endsWith('/activate'))).toBe(false)
  })

  it('shows automatic update health and distinguishes storage artifact, configuration, and active state', async () => {
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input)
      const body = url === '/api/runtime/plugins' ? {
        state: 'ready',
        automatic_update: { enabled: true, state: 'idle', last_checked_at: '2026-10-17T01:02:03Z' },
        plugins: [
          { id: 'storage-artifact', type: 'storage', name: 'Artifact only', installed_version: '2.0.0', installed: true, update_available: false, storage_state: 'artifact_installed' },
          { id: 'storage-configured', type: 'storage', name: 'Configured', installed_version: '2.0.0', installed: true, update_available: false, storage_state: 'configured' },
          { id: 'storage-active', type: 'storage', name: 'Active', installed_version: '2.0.0', installed: true, update_available: false, storage_state: 'active' },
        ],
      } : []
      return new Response(JSON.stringify(body), { status: 200, headers: { 'content-type': 'application/json' } })
    })
    vi.stubGlobal('fetch', fetchMock)

    await renderPlugins()

    expect(await screen.findByText('설치된 플러그인은 Registry의 stable 버전에 맞춰 자동 확인·업데이트됩니다.')).toBeInTheDocument()
    expect(screen.getByText(/마지막 자동 확인:/)).toBeInTheDocument()
    expect(screen.getByText('새 실행 파일만 설치됨 · 저장소 설정이 필요합니다. 기존 저장소는 유지됩니다.')).toBeInTheDocument()
    expect(screen.getByText('설정됨 · 새 저장소 버전은 명시적으로 활성화하기 전까지 사용되지 않습니다.')).toBeInTheDocument()
    expect(screen.getByText('현재 활성 저장소 버전입니다.')).toBeInTheDocument()
  })

  it('shows a bounded automatic-update failure without hiding installed plugins', async () => {
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input)
      const body = url === '/api/runtime/plugins' ? {
        state: 'unavailable', failure_code: 'plugin_registry_unavailable',
        automatic_update: { enabled: true, state: 'failed', last_failure_code: 'plugin_registry_unavailable' },
        plugins: [{ id: 'demo', type: 'source', name: 'Installed Demo', installed_version: '1.0.0', installed: true, update_available: false }],
      } : []
      return new Response(JSON.stringify(body), { status: 200, headers: { 'content-type': 'application/json' } })
    })
    vi.stubGlobal('fetch', fetchMock)

    await renderPlugins()

    expect(await screen.findByText('자동 업데이트를 완료하지 못했습니다. 설치된 플러그인은 계속 사용할 수 있습니다.')).toBeInTheDocument()
    expect(screen.getByText('Installed Demo')).toBeInTheDocument()
    expect(screen.queryByText(/fixture storage registry unavailable/)).not.toBeInTheDocument()
  })

  it('keeps installed plugins visible while the registry is unavailable', async () => {
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input)
      const body = url === '/api/runtime/plugins'
        ? { state: 'unavailable', failure_code: 'plugin_registry_unavailable', plugins: [{ id: 'demo', name: 'Demo Adapter', installed_version: '1.0.0', installed: true, update_available: false }] }
        : []
      return new Response(JSON.stringify(body), { status: 200, headers: { 'content-type': 'application/json' } })
    })
    vi.stubGlobal('fetch', fetchMock)

    await renderPlugins()

    expect(await screen.findByText(/Registry에 연결할 수 없습니다\. 이미 설치된 Source Plugin과 storage provider는 계속 사용할 수 있습니다\./)).toBeInTheDocument()
    expect(screen.getByText(/설치됨 1\.0\.0/)).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Registry 새로고침' })).toBeEnabled()
  })

  it('shows one security warning for unreviewed installed or available plugins', async () => {
    const trust = { provenance: 'operator', authority: 'local', publisher: 'unknown', reviewed: false }
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input)
      const body = url === '/api/runtime/plugins'
        ? { state: 'ready', plugins: [{ id: 'local-demo', type: 'source', name: 'Local Demo', available_version: '1.0.0', installed: false, update_available: false, trust }] }
        : [{ status: { id: 'local-installed', name: 'Local Installed', state: 'ready' }, trust }]
      return new Response(JSON.stringify(body), { status: 200, headers: { 'content-type': 'application/json' } })
    })
    vi.stubGlobal('fetch', fetchMock)

    await renderAdapters([{ status: { id: 'local-installed', name: 'Local Installed', state: 'ready' }, trust }])

    expect(await screen.findByRole('note')).toHaveTextContent('일부 플러그인은 검토되지 않았으며 Core와 같은 OS 권한으로 실행됩니다.')
    expect(screen.getAllByText('일부 플러그인은 검토되지 않았으며 Core와 같은 OS 권한으로 실행됩니다.')).toHaveLength(1)
    expect(screen.getByText('Local Installed')).toBeInTheDocument()
    expect(fetchMock.mock.calls.some(([url]) => url === '/api/runtime/plugins')).toBe(false)
  })

  it('shows artifact verification failures separately from registry availability', async () => {
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input)
      if (url === '/api/runtime/plugins/demo/install' && init?.method === 'POST') {
        return new Response(JSON.stringify({ error: 'plugin_verification_failed', message: '다운로드한 플러그인을 검증하지 못했습니다.' }), { status: 422, headers: { 'content-type': 'application/json' } })
      }
      const body = url === '/api/runtime/plugins' ? availableRegistry : []
      return new Response(JSON.stringify(body), { status: 200, headers: { 'content-type': 'application/json' } })
    })
    vi.stubGlobal('fetch', fetchMock)

    await renderPlugins()
    fireEvent.click(await screen.findByRole('button', { name: '설치' }))
    await waitFor(() => expect(fetchMock.mock.calls.some(([url, init]) => url === '/api/runtime/plugins/demo/install' && init?.method === 'POST')).toBe(true))

    expect(await screen.findByText('입력 값을 확인한 뒤 다시 시도하세요.')).toBeInTheDocument()
    expect(screen.queryByText(/Registry에 연결할 수 없습니다/)).not.toBeInTheDocument()
  })
})
