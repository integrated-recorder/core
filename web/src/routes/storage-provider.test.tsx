import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { ToastProvider } from '@/components/ui/toast'
import { StoragePage } from './storage'

afterEach(() => vi.unstubAllGlobals())

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

function renderStorage(fetchMock: ReturnType<typeof vi.fn>) {
  vi.stubGlobal('fetch', fetchMock)
  const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } })
  return render(<QueryClientProvider client={client}><ToastProvider><StoragePage /></ToastProvider></QueryClientProvider>)
}

describe('storage provider lifecycle UI', () => {
  it('shows bundled local storage without requesting or exposing its managed root configuration', async () => {
    let localConfigCalls = 0
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input)
      if (url === '/api/runtime/storage/provider') return new Response(JSON.stringify(status()), { status: 200, headers: { 'content-type': 'application/json' } })
      if (url === '/api/runtime/storage/providers/local/config') {
        localConfigCalls += 1
        return new Response(JSON.stringify({ values: { root: '/data/storage/local' }, configured_secrets: [] }), { status: 200, headers: { 'content-type': 'application/json' } })
      }
      return new Response(JSON.stringify({ items: [] }), { status: 200, headers: { 'content-type': 'application/json' } })
    })

    renderStorage(fetchMock)
    expect(await screen.findByText('Local Storage')).toBeInTheDocument()
    expect(screen.getByText('Bundled')).toBeInTheDocument()
    expect(screen.getByText('설치됨')).toBeInTheDocument()
    expect(screen.getByText('local · 1.0.0')).toBeInTheDocument()
    expect(screen.getByText('기본 저장소', { selector: 'span' })).toBeInTheDocument()
    expect(screen.getByText('연결 준비됨')).toBeInTheDocument()
    expect(screen.queryByLabelText('Root')).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /삭제|제거|uninstall/i })).not.toBeInTheDocument()
    expect(screen.queryByText('/data/storage/local')).not.toBeInTheDocument()
    expect(localConfigCalls).toBe(0)
  })

  it('keeps stored secrets write-only and submits only a newly entered secret', async () => {
    let submitted: { values: Record<string, unknown>; secrets: Record<string, string> } | undefined
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input)
      let body: unknown = { items: [] }
      if (url === '/api/runtime/storage/provider') body = status()
      if (url === `/api/runtime/storage/providers/${provider.id}/config`) {
        if (init?.method === 'PUT') {
          submitted = JSON.parse(String(init.body)) as typeof submitted
          body = { values: { endpoint: 'https://objects.example.invalid' }, configured_secrets: ['access_token'] }
        } else body = { values: { endpoint: 'https://objects.example.invalid' }, configured_secrets: ['access_token'] }
      }
      return new Response(JSON.stringify(body), { status: 200, headers: { 'content-type': 'application/json' } })
    })

    renderStorage(fetchMock)
    const secretInput = await screen.findByLabelText('Access token')
    expect(secretInput).toHaveAttribute('placeholder', '설정된 값이 있습니다 · 새 값 입력 시 교체')
    expect(screen.queryByText(/previous-secret-value/)).not.toBeInTheDocument()
    fireEvent.change(secretInput, { target: { value: 'replacement-secret' } })
    fireEvent.click(screen.getByRole('button', { name: '설정 저장' }))

    await waitFor(() => expect(submitted).toEqual({ values: {}, secrets: { access_token: 'replacement-secret' } }))
    expect(JSON.stringify(submitted)).not.toContain('previous-secret-value')
  })

  it('probes separately from activation and hides unsafe provider error detail', async () => {
    let probed = false
    let activationCalls = 0
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input)
      if (url === '/api/runtime/storage/provider') return new Response(JSON.stringify(status(probed)), { status: 200, headers: { 'content-type': 'application/json' } })
      if (url === `/api/runtime/storage/providers/${provider.id}/config`) return new Response(JSON.stringify({ values: {}, configured_secrets: ['access_token'] }), { status: 200, headers: { 'content-type': 'application/json' } })
      if (url === `/api/runtime/storage/providers/${provider.id}/probe` && init?.method === 'POST') {
        probed = true
        return new Response(null, { status: 204 })
      }
      if (url === `/api/runtime/storage/providers/${provider.id}/activate` && init?.method === 'POST') {
        activationCalls += 1
        return new Response(JSON.stringify({ error: 'storage_backend_switch_requires_empty_archive', message: 'provider-private https://objects.invalid/path?token=private-value' }), { status: 409, headers: { 'content-type': 'application/json' } })
      }
      return new Response(JSON.stringify({ items: [] }), { status: 200, headers: { 'content-type': 'application/json' } })
    })

    renderStorage(fetchMock)
    const probeButtons = await screen.findAllByRole('button', { name: '연결 검사' })
    fireEvent.click(probeButtons[1])
    await waitFor(() => expect(fetchMock.mock.calls.some(([url, init]) => url === `/api/runtime/storage/providers/${provider.id}/probe` && init?.method === 'POST')).toBe(true))
    expect(await screen.findByText('연결 및 읽기·쓰기 검사가 성공했습니다.')).toBeInTheDocument()

    fireEvent.click(await screen.findByRole('button', { name: '기본 저장소로 활성화' }))
    expect(screen.getByRole('dialog')).toHaveTextContent('기존 녹화는 자동으로 이동하지 않으며')
    expect(activationCalls).toBe(0)
    fireEvent.click(screen.getByRole('button', { name: '확인 후 활성화' }))
    await waitFor(() => expect(activationCalls).toBe(1))
    expect(await screen.findByRole('alert')).toHaveTextContent('저장소 작업을 완료하지 못했습니다.')
    expect(screen.queryByText(/objects\.invalid|private-value/)).not.toBeInTheDocument()
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

    renderStorage(fetchMock)
    fireEvent.click(await screen.findByRole('button', { name: '인스턴스 추가' }))
    fireEvent.change(screen.getByLabelText('표시 이름'), { target: { value: 'SSD archive' } })
    fireEvent.change(screen.getAllByRole('textbox', { name: /Archive root/ })[0], { target: { value: '/mnt/ssd/archive' } })
    fireEvent.click(screen.getByRole('button', { name: '인스턴스 만들기' }))
    expect(await screen.findByText('SSD archive')).toBeInTheDocument()

    fireEvent.click(screen.getByRole('button', { name: '인스턴스 추가' }))
    fireEvent.change(screen.getByLabelText('표시 이름'), { target: { value: 'HDD archive' } })
    fireEvent.change(screen.getAllByRole('textbox', { name: /Archive root/ })[0], { target: { value: '/mnt/hdd/archive' } })
    fireEvent.click(screen.getByRole('button', { name: '인스턴스 만들기' }))
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

    const roots = await screen.findAllByRole('textbox', { name: /Archive root/ })
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

    fireEvent.click(screen.getAllByRole('button', { name: '기본 저장소로 활성화' })[1])
    expect(screen.getByRole('dialog')).toHaveTextContent('HDD archive')
    fireEvent.click(screen.getByRole('button', { name: '확인 후 활성화' }))
    await waitFor(() => expect(calls.some(call => call.url === `/api/runtime/storage/instances/${instances[1].id}/activate` && call.method === 'POST')).toBe(true))
    expect(activations.at(-1)).toEqual({ id: instances[1].id, setID: instances[1].desired_set_id })
    await waitFor(() => expect(screen.getByText('기본 저장소', { selector: 'span' })).toBeInTheDocument())
  })
})
