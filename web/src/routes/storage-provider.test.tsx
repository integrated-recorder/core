import { createMemoryHistory, createRootRoute, createRoute, createRouter, RouterProvider } from '@tanstack/react-router'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { ToastProvider } from '@/components/ui/toast'
import { qk } from '@/api/queries'
import { accountAPI } from '@/api'
import { StorageLocationsPage, StoragePage } from './storage'

afterEach(() => { vi.unstubAllGlobals(); vi.restoreAllMocks() })

const provider = {
  id: 'fixture-storage', name: 'Fixture Storage', version: '1.0.0', configured: true, active: false, health: 'unknown',
  distribution: 'registry', configuration_managed: false, uninstallable: true,
  configuration_schema: { fields: [
    { key: 'endpoint', control: 'text', label: 'Endpoint' },
    { key: 'access_token', control: 'secret', label: 'Access token' },
  ] },
}
const localProvider = {
  id: 'local', name: 'Local Storage', version: '1.0.0', configured: true, active: true, health: 'ready',
  distribution: 'bundled', configuration_managed: true, uninstallable: false,
  configuration_schema: { fields: [{ key: 'root', control: 'text', label: 'Root' }] },
}

function status(probeReady = false, active = false) {
  return {
    primary: active ? { kind: 'plugin', provider_id: provider.id, version: provider.version, state: 'ready' } : { kind: 'plugin', provider_id: localProvider.id, version: localProvider.version, state: 'ready' },
    providers: [{ ...localProvider, active: !active }, { ...provider, active, health: probeReady ? 'ready' : 'unknown' }],
  }
}

async function renderStorage(fetchMock: ReturnType<typeof vi.fn>, Component: React.ComponentType = StorageLocationsPage) {
  vi.stubGlobal('fetch', fetchMock)
  vi.stubGlobal('scrollTo', vi.fn())
  vi.spyOn(accountAPI, 'authorization').mockResolvedValue({ user_id: 'usr-00000000000000000000000000000001', login: 'admin-test', roles: ['administrator'], permissions: ['storage.read', 'storage.manage', 'plugin.read'] })
  const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } })
  client.setQueryData(qk.session, { auth_enabled: true, authenticated: true, needs_bootstrap: false, csrf_token: 'test-csrf' })
  const root = createRootRoute()
  const pageRoute = createRoute({ getParentRoute: () => root, path: '/', component: () => <Component /> })
  const pluginsRoute = createRoute({ getParentRoute: () => root, path: '/plugins', component: () => null })
  const router = createRouter({ routeTree: root.addChildren([pageRoute, pluginsRoute]), history: createMemoryHistory({ initialEntries: ['/'] }), scrollRestoration: false })
  await router.load()
  return render(<QueryClientProvider client={client}><ToastProvider><RouterProvider router={router} /></ToastProvider></QueryClientProvider>)
}

describe('storage information architecture', () => {
  it('keeps provider metadata on Overview and sends all editable storage operations to instances', async () => {
    const calls: Array<{ url: string; method: string }> = []
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input)
      calls.push({ url, method: 'GET' })
      if (url === '/api/runtime/storage/provider') return new Response(JSON.stringify(status()), { status: 200, headers: { 'content-type': 'application/json' } })
      if (url === '/api/system/storage') return new Response(JSON.stringify({ recordings_bytes: 0, filesystem_total_bytes: 0, filesystem_used_bytes: 0, filesystem_available_bytes: 0, recording_count: 0, segment_count: 0, init_segment_count: 0, manifest_count: 0 }), { status: 200, headers: { 'content-type': 'application/json' } })
      return new Response(JSON.stringify({ items: [] }), { status: 200, headers: { 'content-type': 'application/json' } })
    })

    await renderStorage(fetchMock, StoragePage)
    expect(await screen.findByRole('heading', { name: '저장소' })).toBeInTheDocument()
    expect(await screen.findByText('Local Storage')).toBeInTheDocument()
    expect(screen.getByText('Fixture Storage')).toBeInTheDocument()
    expect(screen.getByRole('link', { name: /저장소 인스턴스/ })).toHaveAttribute('href', '/storage/locations')
    expect(screen.getByRole('link', { name: /플러그인/ })).toHaveAttribute('href', '/plugins')
    expect(screen.queryByLabelText('Endpoint')).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /연결 검사|기본 저장소로 활성화/ })).not.toBeInTheDocument()
    expect(calls.some(call => /\/api\/runtime\/storage\/providers\/[^/]+\/(?:config|probe|activate)$/.test(call.url))).toBe(false)
  })

  it('creates two local instances, updates one immutable config, probes and activates only the selected instance', async () => {
    let nextID = 0
    let activeInstanceID = ''
    let activeSetID = ''
    const instances: Array<{ id: string; display_name: string; provider_id: string; provider_name: string; desired_set_id: string; active: boolean; health: 'ready' | 'unknown' | 'unavailable' }> = []
    const configs = new Map<string, { values: Record<string, unknown>; configured_secrets: string[] }>()
    const calls: Array<{ url: string; method: string; body?: Record<string, unknown> }> = []
    const activations: Array<{ id: string; setID: string }> = []
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input)
      const method = init?.method ?? 'GET'
      const body = init?.body ? JSON.parse(String(init.body)) as Record<string, unknown> : undefined
      calls.push({ url, method, body })
      if (url === '/api/runtime/storage/provider') {
        return new Response(JSON.stringify({
          primary: { kind: 'plugin', provider_id: 'local', instance_id: activeInstanceID || undefined, version: '1.0.0', state: 'ready' },
          providers: [{ ...localProvider, active: !activeInstanceID, configuration_schema: { fields: [] } }],
        }), { status: 200, headers: { 'content-type': 'application/json' } })
      }
      if (url === '/api/runtime/storage/instances') {
        if (method === 'POST') {
          nextID += 1
          const id = `si_${String(nextID).padStart(32, '0')}`
          const desiredSetID = String(nextID).padStart(64, '0')
          const createBody = body ?? {}
          const config = { values: createBody.values as Record<string, unknown>, configured_secrets: [] as string[] }
          configs.set(id, config)
          const instance = { id, display_name: String(createBody.display_name), provider_id: 'local', provider_name: 'Local Storage', desired_set_id: desiredSetID, active: false, health: 'unknown' as const }
          instances.push(instance)
          return new Response(JSON.stringify(instance), { status: 201, headers: { 'content-type': 'application/json' } })
        }
        return new Response(JSON.stringify(instances.map(item => ({ ...item, active: item.id === activeInstanceID && item.desired_set_id === activeSetID }))), { status: 200, headers: { 'content-type': 'application/json' } })
      }
      const match = url.match(/^\/api\/runtime\/storage\/instances\/(si_[0-9a-f]+)\/(config|probe|activate)$/)
      if (match) {
        const [, id, operation] = match
        if (operation === 'config' && method === 'GET') return new Response(JSON.stringify(configs.get(id)), { status: 200, headers: { 'content-type': 'application/json' } })
        if (operation === 'config' && method === 'PUT') {
          const current = configs.get(id) ?? { values: {}, configured_secrets: [] }
          current.values = { ...current.values, ...(body?.values as Record<string, unknown> ?? {}) }
          configs.set(id, current)
          const instance = instances.find(item => item.id === id)
          if (instance) instance.desired_set_id = String(1000 + instances.indexOf(instance)).padStart(64, '0')
          return new Response(JSON.stringify(current), { status: 200, headers: { 'content-type': 'application/json' } })
        }
        if (operation === 'probe' && method === 'POST') return new Response(null, { status: 204 })
        if (operation === 'activate' && method === 'POST') {
          activeInstanceID = id
          activeSetID = instances.find(item => item.id === id)?.desired_set_id ?? ''
          activations.push({ id, setID: activeSetID })
          return new Response(JSON.stringify({ primary: { kind: 'plugin', provider_id: 'local', instance_id: id, version: '1.0.0', state: 'ready' }, providers: [{ ...localProvider, active: true, configuration_schema: { fields: [] } }] }), { status: 200, headers: { 'content-type': 'application/json' } })
        }
      }
      return new Response(JSON.stringify({ items: [] }), { status: 200, headers: { 'content-type': 'application/json' } })
    })

    await renderStorage(fetchMock, StorageLocationsPage)
    fireEvent.click(await screen.findByRole('button', { name: '인스턴스 추가' }))
    fireEvent.change(screen.getByLabelText('표시 이름'), { target: { value: 'SSD archive' } })
    fireEvent.change(screen.getAllByRole('textbox', { name: /보관 데이터 경로/ })[0], { target: { value: '/mnt/ssd/archive' } })
    fireEvent.click(screen.getByRole('button', { name: '저장소 인스턴스 만들기' }))
    expect(await screen.findByText('SSD archive')).toBeInTheDocument()

    fireEvent.click(screen.getByRole('button', { name: '인스턴스 추가' }))
    fireEvent.change(screen.getByLabelText('표시 이름'), { target: { value: 'HDD archive' } })
    fireEvent.change(screen.getAllByRole('textbox', { name: /보관 데이터 경로/ })[0], { target: { value: '/mnt/hdd/archive' } })
    fireEvent.click(screen.getByRole('button', { name: '저장소 인스턴스 만들기' }))
    expect(await screen.findByText('HDD archive')).toBeInTheDocument()

    await waitFor(() => expect(instances).toHaveLength(2))
    expect(instances[0].provider_id).toBe('local')
    expect(instances[1].provider_id).toBe('local')
    expect(instances[0].id).not.toBe(instances[1].id)
    expect(instances[0].desired_set_id).not.toBe(instances[1].desired_set_id)

    const ssdCard = () => screen.getByText('SSD archive').closest('section')!
    fireEvent.click(within(ssdCard()).getByRole('button', { name: '기본 저장소로 활성화' }))
    expect(screen.getByRole('dialog')).toHaveTextContent('SSD archive')
    fireEvent.click(screen.getByRole('button', { name: '확인 후 활성화' }))
    await waitFor(() => expect(activations).toHaveLength(1))
    expect(activations[0]).toEqual({ id: instances[0].id, setID: instances[0].desired_set_id })

    const roots = await screen.findAllByRole('textbox', { name: /보관 데이터 경로/ })
    fireEvent.change(roots[0], { target: { value: '/mnt/ssd/archive-v2' } })
    const saveButtons = screen.getAllByRole('button', { name: '새 설정 저장' })
    fireEvent.click(saveButtons[0])
    await waitFor(() => expect(calls.some(call => call.url === `/api/runtime/storage/instances/${instances[0].id}/config` && call.method === 'PUT')).toBe(true))
    expect(instances[0].desired_set_id).not.toBe(instances[1].desired_set_id)
    await waitFor(() => expect(within(ssdCard()).getByRole('button', { name: '기본 저장소로 활성화' })).toBeEnabled())

    // Backend active state matches exact immutable set pin. Editing the active
    // instance keeps its stable ID but makes its new desired set activatable.
    fireEvent.click(within(ssdCard()).getByRole('button', { name: '기본 저장소로 활성화' }))
    expect(screen.getByRole('dialog')).toHaveTextContent('SSD archive')
    fireEvent.click(screen.getByRole('button', { name: '확인 후 활성화' }))
    await waitFor(() => expect(activations).toHaveLength(2))
    expect(activations[1]).toEqual({ id: instances[0].id, setID: instances[0].desired_set_id })

    const probeButtons = await screen.findAllByRole('button', { name: '연결 검사' })
    fireEvent.click(probeButtons.at(-1)!)
    await waitFor(() => expect(calls.some(call => call.url === `/api/runtime/storage/instances/${instances[1].id}/probe` && call.method === 'POST')).toBe(true))

    const hddCard = screen.getByText('HDD archive').closest('section')!
    fireEvent.click(within(hddCard).getByRole('button', { name: '기본 저장소로 활성화' }))
    expect(screen.getByRole('dialog')).toHaveTextContent('HDD archive')
    fireEvent.click(screen.getByRole('button', { name: '확인 후 활성화' }))
    await waitFor(() => expect(calls.some(call => call.url === `/api/runtime/storage/instances/${instances[1].id}/activate` && call.method === 'POST')).toBe(true))
    expect(activations.at(-1)).toEqual({ id: instances[1].id, setID: instances[1].desired_set_id })
    await waitFor(() => expect(screen.getByText('기본 저장소', { selector: 'span' })).toBeInTheDocument())
  })
})
