import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { globalIdentityAPI, peerArchivesAPI } from '@/api'
import { openArchiveDEK } from '@/identity/cryptoarchive'
import { RemoteArchiveBrowser } from './remote-archive-browser'

vi.mock('@/identity/recovery', () => ({
  decodeRecoveryFile: vi.fn(),
  deriveRecoveryKeys: vi.fn(async () => ({
    identityId: `gi_${'I'.repeat(22)}`,
    signerKeyId: `ik_${'K'.repeat(43)}`,
    signingPublicKey: new Uint8Array(32),
    encryptionPrivateKey: new Uint8Array(32),
    destroy: vi.fn(),
  })),
  recoveryEntropyFromMnemonic: vi.fn(() => new Uint8Array(32)),
}))

vi.mock('@/identity/cryptoarchive', () => ({
  openArchiveDEK: vi.fn(async () => new Uint8Array(32)),
  createSessionDEKKeyPair: vi.fn(() => ({ publicKey: new Uint8Array([1, 2, 3]), privateKey: new Uint8Array([4, 5, 6]), destroy: vi.fn() })),
  openSessionDEKEnvelope: vi.fn(async () => new Uint8Array(32).fill(0x31)),
}))

vi.mock('@/identity/webauthn', () => ({
  base64UrlBytes: vi.fn(() => 'AQID'),
  serializeAssertionCredential: vi.fn(() => ({ id: 'test-assertion' })),
  webAuthnRequestOptions: vi.fn((value: unknown) => value),
}))

vi.mock('@/identity/peer-playback', () => ({
  createPeerArchiveFragmentLoader: vi.fn(() => ({})),
  createPeerPlaybackPlan: vi.fn(() => ({ tracks: ['main'], objects: new Map(), playlist: () => '#EXTM3U' })),
  decryptPeerPlaybackObject: vi.fn(),
}))

vi.mock('hls.js', () => ({
  default: class MockHls {
    static Events = { ERROR: 'error' }
    static isSupported() { return true }
    attachMedia() {}
    on() {}
    loadSource() {}
    destroy() {}
  },
}))

const nodeA = `nd_${'A'.repeat(22)}`
const nodeB = `nd_${'B'.repeat(22)}`
const ownerSubjectID = `os_${'S'.repeat(22)}`
const archiveID = `arc_${'A'.repeat(22)}`
const grantHash = `sha256:${'a'.repeat(64)}`

function capabilities() {
  return {
    local_node_id: `nd_${'L'.repeat(22)}`,
    sessions: [
      { identity_id: `gi_${'I'.repeat(22)}`, node_id: nodeB, connected_at: new Date().toISOString() },
      { identity_id: `gi_${'I'.repeat(22)}`, node_id: nodeA, connected_at: new Date().toISOString() },
    ],
    capabilities: [{ owner_subject_id: ownerSubjectID, archive_id: archiveID, grant_hash: grantHash, expires_at: new Date(Date.now() + 60_000).toISOString() }],
  }
}

function head(nodeID: string, snapshotID = `snap_${'c'.repeat(64)}`, sequence = 5, previousSnapshotID?: string, contentHash = `sha256:${'d'.repeat(64)}`) {
  return {
    node_id: nodeID,
    snapshot: { schema_version: 2, sequence, ...(previousSnapshotID ? { previous_snapshot_id: previousSnapshotID } : {}), snapshot_id: snapshotID, content_hash: contentHash, content: {
      archive_id: archiveID, session: { id: 'session-public', adapter_id: 'hls' },
      objects: [{ timeline_ordinal: 9, duration: 2.5, coordinate: { session_id: 'session-public', track_id: 'main', source_epoch: 2, discontinuity_sequence: 3, sequence: 9 } }],
      coverage: [],
    } },
  }
}

function renderBrowser() {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } })
  render(<QueryClientProvider client={queryClient}><RemoteArchiveBrowser /></QueryClientProvider>)
  return queryClient
}

describe('remote archive browser', () => {
  afterEach(() => { vi.restoreAllMocks(); vi.unstubAllGlobals() })

  it('checks connected authorized replicas and chooses a source only when immutable heads agree', async () => {
    const calls: string[] = []
    vi.stubGlobal('fetch', vi.fn(async (input: RequestInfo | URL) => {
      const path = String(input)
      calls.push(path)
      if (path === '/api/peer-archives/capabilities') {
        return new Response(JSON.stringify(capabilities()), { status: 200, headers: { 'Content-Type': 'application/json' } })
      }
      const requestedNode = path.match(/^\/api\/peer-archives\/([^/]+)\/head/u)?.[1] ?? ''
      return new Response(JSON.stringify(head(requestedNode)), { status: 200, headers: { 'Content-Type': 'application/json' } })
    }))
    renderBrowser()

    expect(screen.queryByLabelText('조회할 연결 Node')).not.toBeInTheDocument()
    fireEvent.click(await screen.findByRole('button', { name: '스냅샷 목록 불러오기' }))
    expect(await screen.findByText('스냅샷 5 · 객체 1개')).toBeInTheDocument()
    const requests = calls.filter(path => path.includes('/head?'))
    expect(requests).toHaveLength(2)
    expect(requests[0]).toMatch(new RegExp(`/api/peer-archives/${nodeA}/head`, 'u'))
    expect(requests[1]).toMatch(new RegExp(`/api/peer-archives/${nodeB}/head`, 'u'))
    for (const request of requests) {
      expect(request).toContain(`owner_subject_id=${ownerSubjectID}`)
      expect(request).toContain(`archive_id=${archiveID}`)
      expect(request).toContain(`grant_hash=${encodeURIComponent(grantHash)}`)
    }
  })

  it('fails closed when one Node ID is simultaneously attributed to multiple identities', async () => {
    const state = capabilities()
    state.sessions.push({ identity_id: `gi_${'J'.repeat(22)}`, node_id: nodeA, connected_at: new Date().toISOString() })
    const calls: string[] = []
    vi.stubGlobal('fetch', vi.fn(async (input: RequestInfo | URL) => {
      const path = String(input)
      calls.push(path)
      if (path === '/api/peer-archives/capabilities') {
        return new Response(JSON.stringify(state), { status: 200, headers: { 'Content-Type': 'application/json' } })
      }
      const requestedNode = path.match(/^\/api\/peer-archives\/([^/]+)\/head/u)?.[1] ?? ''
      return new Response(JSON.stringify(head(requestedNode)), { status: 200, headers: { 'Content-Type': 'application/json' } })
    }))
    renderBrowser()

    fireEvent.click(await screen.findByRole('button', { name: '스냅샷 목록 불러오기' }))

    expect(await screen.findByRole('alert')).toHaveTextContent('요청을 완료하지 못했습니다. 잠시 후 다시 시도하세요.')
    expect(calls.some(path => path.includes('/head?'))).toBe(false)
    expect(screen.queryByText('스냅샷 5 · 객체 1개')).not.toBeInTheDocument()
  })

  it('walks the immutable snapshot predecessor chain under the same archive capability', async () => {
    const previousID = `snap_${'b'.repeat(64)}`
    vi.spyOn(HTMLMediaElement.prototype, 'pause').mockImplementation(() => {})
    vi.spyOn(HTMLMediaElement.prototype, 'load').mockImplementation(() => {})
    const calls: string[] = []
    vi.stubGlobal('fetch', vi.fn(async (input: RequestInfo | URL) => {
      const path = String(input)
      calls.push(path)
      if (path === '/api/peer-archives/capabilities') return new Response(JSON.stringify(capabilities()), { status: 200, headers: { 'Content-Type': 'application/json' } })
      const requestedNode = path.match(/^\/api\/peer-archives\/([^/]+)\//u)?.[1] ?? ''
      if (path.includes('/head?')) return new Response(JSON.stringify(head(requestedNode, `snap_${'c'.repeat(64)}`, 5, previousID)), { status: 200, headers: { 'Content-Type': 'application/json' } })
      if (path.includes(`/snapshots/${previousID}?`)) return new Response(JSON.stringify(head(requestedNode, previousID, 4)), { status: 200, headers: { 'Content-Type': 'application/json' } })
      return new Response('{}', { status: 404, headers: { 'Content-Type': 'application/json' } })
    }))
    renderBrowser()

    fireEvent.click(await screen.findByRole('button', { name: '스냅샷 목록 불러오기' }))
    expect(await screen.findByText('스냅샷 5 · 객체 1개')).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: '이전 스냅샷' }))
    expect(await screen.findByText('스냅샷 4 · 객체 1개')).toBeInTheDocument()
    const historicalRequests = calls.filter(path => path.includes(`/snapshots/${previousID}?`))
    expect(historicalRequests).toHaveLength(2)
    for (const request of historicalRequests) {
      expect(request).toContain(`owner_subject_id=${ownerSubjectID}`)
      expect(request).toContain(`archive_id=${archiveID}`)
      expect(request).toContain(`grant_hash=${encodeURIComponent(grantHash)}`)
    }
    expect(screen.getByRole('button', { name: '최신 스냅샷' })).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: '최신 스냅샷' }))
    expect(await screen.findByText('스냅샷 5 · 객체 1개')).toBeInTheDocument()
  })

  it('refuses to choose an old snapshot if connected replicas disagree about its content', async () => {
    const previousID = `snap_${'b'.repeat(64)}`
    vi.spyOn(HTMLMediaElement.prototype, 'pause').mockImplementation(() => {})
    vi.spyOn(HTMLMediaElement.prototype, 'load').mockImplementation(() => {})
    vi.stubGlobal('fetch', vi.fn(async (input: RequestInfo | URL) => {
      const path = String(input)
      if (path === '/api/peer-archives/capabilities') return new Response(JSON.stringify(capabilities()), { status: 200, headers: { 'Content-Type': 'application/json' } })
      const requestedNode = path.match(/^\/api\/peer-archives\/([^/]+)\//u)?.[1] ?? ''
      if (path.includes('/head?')) return new Response(JSON.stringify(head(requestedNode, `snap_${'c'.repeat(64)}`, 5, previousID)), { status: 200, headers: { 'Content-Type': 'application/json' } })
      const contentHash = requestedNode === nodeA ? `sha256:${'1'.repeat(64)}` : `sha256:${'2'.repeat(64)}`
      return new Response(JSON.stringify(head(requestedNode, previousID, 4, undefined, contentHash)), { status: 200, headers: { 'Content-Type': 'application/json' } })
    }))
    renderBrowser()

    fireEvent.click(await screen.findByRole('button', { name: '스냅샷 목록 불러오기' }))
    expect(await screen.findByText('스냅샷 5 · 객체 1개')).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: '이전 스냅샷' }))
    expect(await screen.findByRole('alert')).toHaveTextContent('연결된 복제본의 불변 스냅샷이 서로 다릅니다')
    expect(screen.queryByText('스냅샷 4 · 객체 1개')).not.toBeInTheDocument()
  })

  it('refuses to auto-select when connected replicas report different immutable snapshots', async () => {
    vi.stubGlobal('fetch', vi.fn(async (input: RequestInfo | URL) => {
      const path = String(input)
      if (path === '/api/peer-archives/capabilities') {
        return new Response(JSON.stringify(capabilities()), { status: 200, headers: { 'Content-Type': 'application/json' } })
      }
      const requestedNode = path.match(/^\/api\/peer-archives\/([^/]+)\/head/u)?.[1] ?? ''
      const snapshotID = requestedNode === nodeA ? `snap_${'a'.repeat(64)}` : `snap_${'b'.repeat(64)}`
      return new Response(JSON.stringify(head(requestedNode, snapshotID)), { status: 200, headers: { 'Content-Type': 'application/json' } })
    }))
    renderBrowser()

    fireEvent.click(await screen.findByRole('button', { name: '스냅샷 목록 불러오기' }))
    expect(await screen.findByRole('alert')).toHaveTextContent('연결된 복제본의 불변 스냅샷이 서로 다릅니다')
    expect(screen.queryByText(/스냅샷 5/u)).not.toBeInTheDocument()
  })

  it('reports connected replicas that could not be compared when another replica is available', async () => {
    vi.stubGlobal('fetch', vi.fn(async (input: RequestInfo | URL) => {
      const path = String(input)
      if (path === '/api/peer-archives/capabilities') {
        return new Response(JSON.stringify(capabilities()), { status: 200, headers: { 'Content-Type': 'application/json' } })
      }
      const requestedNode = path.match(/^\/api\/peer-archives\/([^/]+)\/head/u)?.[1] ?? ''
      if (requestedNode === nodeA) {
        return new Response(JSON.stringify({ code: 'peer_unavailable', message: 'unavailable' }), { status: 503, headers: { 'Content-Type': 'application/json' } })
      }
      return new Response(JSON.stringify(head(requestedNode)), { status: 200, headers: { 'Content-Type': 'application/json' } })
    }))
    renderBrowser()

    fireEvent.click(await screen.findByRole('button', { name: '스냅샷 목록 불러오기' }))
    expect(await screen.findByText('응답한 복제본 1/2개만 확인했습니다. 응답하지 않은 노드는 비교되지 않았습니다.')).toBeInTheDocument()
    expect(screen.getByText('스냅샷 5 · 객체 1개')).toBeInTheDocument()
  })

  it('discovers a usable key epoch from the peer without asking for manual numeric input', async () => {
    const calls: string[] = []
    vi.stubGlobal('fetch', vi.fn(async (input: RequestInfo | URL) => {
      const path = String(input)
      calls.push(path)
      if (path === '/api/peer-archives/capabilities') return new Response(JSON.stringify(capabilities()), { status: 200, headers: { 'Content-Type': 'application/json' } })
      if (path.includes('/head?')) {
        const requestedNode = path.match(/^\/api\/peer-archives\/([^/]+)\/head/u)?.[1] ?? ''
        return new Response(JSON.stringify(head(requestedNode)), { status: 200, headers: { 'Content-Type': 'application/json' } })
      }
      if (path.includes('/key-epochs?')) return new Response(JSON.stringify({ node_id: nodeA, archive_id: archiveID, snapshot_id: `snap_${'c'.repeat(64)}`, key_epochs: ['7', '5'] }), { status: 200, headers: { 'Content-Type': 'application/json' } })
      if (path.includes('/manifest?')) {
        const keyEpoch = new URL(path, 'https://recorder.test').searchParams.get('key_epoch') ?? ''
        return new Response(JSON.stringify({ replica_manifest_version: 'ir.replica-manifest/1', archive_id: archiveID, snapshot_id: `snap_${'c'.repeat(64)}`, key_epoch: keyEpoch, objects: [] }), { status: 200, headers: { 'Content-Type': 'application/json' } })
      }
      if (path.includes('/key-envelope?')) return new Response(JSON.stringify({ envelope_version: 'ir.key-envelope/1' }), { status: 200, headers: { 'Content-Type': 'application/json' } })
      return new Response('{}', { status: 404, headers: { 'Content-Type': 'application/json' } })
    }))
    vi.stubGlobal('crypto', { subtle: {} })
    Object.defineProperty(window, 'isSecureContext', { configurable: true, value: true })
    const NativeURL = URL
    const revokeObjectURL = vi.fn()
    vi.stubGlobal('URL', Object.assign(class extends NativeURL {}, { createObjectURL: vi.fn(() => 'blob:peer-playback'), revokeObjectURL }))
    const dek = new Uint8Array(32).fill(0x7b)
    vi.mocked(openArchiveDEK).mockResolvedValue(dek)
    const pause = vi.spyOn(HTMLMediaElement.prototype, 'pause').mockImplementation(() => {})
    const load = vi.spyOn(HTMLMediaElement.prototype, 'load').mockImplementation(() => {})
    const queryClient = renderBrowser()

    fireEvent.click(await screen.findByRole('button', { name: '스냅샷 목록 불러오기' }))
    await screen.findByText('스냅샷 5 · 객체 1개')
    expect(screen.queryByLabelText('키 Epoch')).not.toBeInTheDocument()
    fireEvent.change(screen.getByLabelText('Global Identity 복구 문구'), { target: { value: 'recovery phrase fixture' } })
    fireEvent.click(screen.getByRole('button', { name: '복호화 재생' }))

    expect(await screen.findByText('확인된 키 Epoch: 7')).toBeInTheDocument()
    expect(calls.some(path => path.includes('/key-epochs?'))).toBe(true)
    expect(calls.some(path => path.includes('/manifest?') && path.includes('key_epoch=7'))).toBe(true)
    expect(calls.some(path => path.includes('/key-envelope?') && path.includes('key_epoch=7'))).toBe(true)

    queryClient.setQueryData(['peerArchives', 'capabilities'], {
      ...capabilities(),
      sessions: [],
    })

    await waitFor(() => {
      expect(pause).toHaveBeenCalled()
      expect(load).toHaveBeenCalled()
      expect(revokeObjectURL).toHaveBeenCalledWith('blob:peer-playback')
      expect(dek.every(byte => byte === 0)).toBe(true)
      expect(screen.getByRole('alert')).toHaveTextContent('Recorder 연결이 끊겨 재생을 중지하고 복호화 키를 지웠습니다.')
      expect(screen.getByRole('button', { name: '복호화 재생' })).toBeDisabled()
      expect(screen.getByRole('button', { name: 'Passkey로 재생 키 받기' })).toBeDisabled()
    })

    queryClient.setQueryData(['peerArchives', 'capabilities'], capabilities())
    expect(await screen.findByRole('status')).toHaveTextContent('Recorder 연결이 복구됐습니다.')
    expect(screen.getByRole('button', { name: 'Passkey로 재생 키 받기' })).toBeEnabled()
    const secondDEK = new Uint8Array(32).fill(0x5a)
    vi.mocked(openArchiveDEK).mockResolvedValue(secondDEK)
    fireEvent.change(screen.getByLabelText('Global Identity 복구 문구'), { target: { value: 'recovery phrase fixture' } })
    fireEvent.click(screen.getByRole('button', { name: '복호화 재생' }))
    expect(await screen.findByText('확인된 키 Epoch: 7')).toBeInTheDocument()

    const ambiguousIdentity = `gi_${'Z'.repeat(22)}`
    queryClient.setQueryData(['peerArchives', 'capabilities'], {
      ...capabilities(),
      sessions: [...capabilities().sessions, { identity_id: ambiguousIdentity, node_id: nodeA, connected_at: new Date().toISOString() }],
    })
    await waitFor(() => {
      expect(revokeObjectURL).toHaveBeenCalledTimes(2)
      expect(secondDEK.every(byte => byte === 0)).toBe(true)
      expect(screen.getByRole('alert')).toHaveTextContent('연결된 Recorder 신원이 달라지거나 여러 신원으로 모호해져')
      expect(screen.getByRole('button', { name: 'Passkey로 재생 키 받기' })).toBeDisabled()
    })

    queryClient.setQueryData(['peerArchives', 'capabilities'], capabilities())
    fireEvent.change(screen.getByLabelText('Global Identity 복구 문구'), { target: { value: 'recovery phrase fixture' } })
    await waitFor(() => expect(screen.getByRole('button', { name: '복호화 재생' })).toBeEnabled())
    const thirdDEK = new Uint8Array(32).fill(0x3c)
    vi.mocked(openArchiveDEK).mockResolvedValue(thirdDEK)
    fireEvent.click(screen.getByRole('button', { name: '복호화 재생' }))
    expect(await screen.findByText('확인된 키 Epoch: 7')).toBeInTheDocument()

    queryClient.setQueryData(['peerArchives', 'capabilities'], { ...capabilities(), capabilities: [] })
    await waitFor(() => {
      expect(revokeObjectURL).toHaveBeenCalledTimes(3)
      expect(thirdDEK.every(byte => byte === 0)).toBe(true)
      expect(screen.getByRole('alert')).toHaveTextContent('아카이브 권한이 만료되거나 철회되어 재생을 중지하고 복호화 키를 지웠습니다.')
    })
  })

  it('uses Passkey-bound ephemeral key delivery without recovery input', async () => {
    vi.stubGlobal('crypto', { subtle: {} })
    Object.defineProperty(window, 'isSecureContext', { configurable: true, value: true })
    const NativeURL = URL
    vi.stubGlobal('URL', Object.assign(class extends NativeURL {}, { createObjectURL: vi.fn(() => 'blob:passkey-playback'), revokeObjectURL: vi.fn() }))
    vi.spyOn(HTMLMediaElement.prototype, 'pause').mockImplementation(() => {})
    vi.spyOn(HTMLMediaElement.prototype, 'load').mockImplementation(() => {})
    vi.stubGlobal('navigator', { credentials: { get: vi.fn().mockResolvedValue({} as Credential) } })
    const loginOptions = vi.spyOn(globalIdentityAPI, 'loginOptions').mockResolvedValue({ publicKey: { challenge: 'AQID' } })
    const loginFinish = vi.spyOn(globalIdentityAPI, 'loginFinish').mockResolvedValue({ authenticated: true, identity_id: `gi_${'I'.repeat(22)}` })
    const identitySession = vi.spyOn(globalIdentityAPI, 'session').mockResolvedValue({
      authenticated: true,
      identity_id: `gi_${'I'.repeat(22)}`,
      global_csrf_token: 'test-global-csrf',
      playback_key_bound: true,
    })
    const keyEpochs = vi.spyOn(globalIdentityAPI, 'sessionKeyEpochs').mockResolvedValue({
      node_id: nodeA,
      archive_id: archiveID,
      snapshot_id: `snap_${'c'.repeat(64)}`,
      key_epochs: ['7'],
    })
    const sessionEnvelope = vi.spyOn(globalIdentityAPI, 'sessionDEKEnvelope').mockResolvedValue({ envelope_version: 'ir.session-dek-envelope/1' } as never)
    vi.spyOn(peerArchivesAPI, 'manifest').mockResolvedValue({
      replica_manifest_version: 'ir.replica-manifest/1',
      archive_id: archiveID,
      snapshot_id: `snap_${'c'.repeat(64)}`,
      key_epoch: '7',
      objects: [],
    } as never)
    const calls: string[] = []
    vi.stubGlobal('fetch', vi.fn(async (input: RequestInfo | URL) => {
      const path = String(input)
      calls.push(path)
      if (path === '/api/peer-archives/capabilities') return new Response(JSON.stringify(capabilities()), { status: 200, headers: { 'Content-Type': 'application/json' } })
      const requestedNode = path.match(/^\/api\/peer-archives\/([^/]+)\/head/u)?.[1] ?? ''
      return new Response(JSON.stringify(head(requestedNode)), { status: 200, headers: { 'Content-Type': 'application/json' } })
    }))
    renderBrowser()

    fireEvent.click(await screen.findByRole('button', { name: '스냅샷 목록 불러오기' }))
    await screen.findByText('스냅샷 5 · 객체 1개')
    fireEvent.click(screen.getByRole('button', { name: 'Passkey로 재생 키 받기' }))

    expect(await screen.findByText('확인된 키 Epoch: 7')).toBeInTheDocument()
    expect(loginOptions).toHaveBeenCalledWith('AQID')
    expect(loginFinish).toHaveBeenCalledWith({ id: 'test-assertion' })
    expect(identitySession).toHaveBeenCalledOnce()
    expect(keyEpochs).toHaveBeenCalledWith(nodeA, ownerSubjectID, archiveID, grantHash, `snap_${'c'.repeat(64)}`)
    expect(sessionEnvelope).toHaveBeenCalledWith(nodeA, ownerSubjectID, archiveID, grantHash, `snap_${'c'.repeat(64)}`, '7', 'test-global-csrf')
    expect(calls.some(path => path.includes('/key-envelope?'))).toBe(false)
    expect(screen.queryByText('복구 문구 또는 복구 파일을 제공하세요.')).not.toBeInTheDocument()
  })

  it('clears playback secrets when the page is hidden and refreshes authorization when visible', async () => {
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const path = String(input)
      if (path === '/api/peer-archives/capabilities') return new Response(JSON.stringify(capabilities()), { status: 200, headers: { 'Content-Type': 'application/json' } })
      if (path.includes('/head?')) return new Response(JSON.stringify(head(nodeA)), { status: 200, headers: { 'Content-Type': 'application/json' } })
      if (path.includes('/key-epochs?')) return new Response(JSON.stringify({ node_id: nodeA, archive_id: archiveID, snapshot_id: `snap_${'c'.repeat(64)}`, key_epochs: ['7'] }), { status: 200, headers: { 'Content-Type': 'application/json' } })
      if (path.includes('/manifest?')) return new Response(JSON.stringify({ replica_manifest_version: 'ir.replica-manifest/1', archive_id: archiveID, snapshot_id: `snap_${'c'.repeat(64)}`, key_epoch: '7', objects: [] }), { status: 200, headers: { 'Content-Type': 'application/json' } })
      if (path.includes('/key-envelope?')) return new Response(JSON.stringify({ envelope_version: 'ir.key-envelope/1' }), { status: 200, headers: { 'Content-Type': 'application/json' } })
      return new Response('{}', { status: 404, headers: { 'Content-Type': 'application/json' } })
    })
    vi.stubGlobal('fetch', fetchMock)
    vi.stubGlobal('crypto', { subtle: {} })
    Object.defineProperty(window, 'isSecureContext', { configurable: true, value: true })
    Object.defineProperty(document, 'visibilityState', { configurable: true, value: 'visible' })
    const NativeURL = URL
    const revokeObjectURL = vi.fn()
    vi.stubGlobal('URL', Object.assign(class extends NativeURL {}, { createObjectURL: vi.fn(() => 'blob:hidden-playback'), revokeObjectURL }))
    vi.spyOn(HTMLMediaElement.prototype, 'pause').mockImplementation(() => {})
    vi.spyOn(HTMLMediaElement.prototype, 'load').mockImplementation(() => {})
    const dek = new Uint8Array(32).fill(0x7b)
    vi.mocked(openArchiveDEK).mockResolvedValue(dek)
    renderBrowser()

    fireEvent.click(await screen.findByRole('button', { name: '스냅샷 목록 불러오기' }))
    await screen.findByText('스냅샷 5 · 객체 1개')
    fireEvent.change(screen.getByLabelText('Global Identity 복구 문구'), { target: { value: 'recovery phrase fixture' } })
    fireEvent.click(screen.getByRole('button', { name: '복호화 재생' }))
    await screen.findByText('확인된 키 Epoch: 7')

    Object.defineProperty(document, 'visibilityState', { configurable: true, value: 'hidden' })
    fireEvent(document, new Event('visibilitychange'))
    await waitFor(() => {
      expect(dek.every(byte => byte === 0)).toBe(true)
      expect(revokeObjectURL).toHaveBeenCalledWith('blob:hidden-playback')
      expect(screen.getByLabelText('Global Identity 복구 문구')).toHaveValue('')
      expect(screen.getByRole('alert')).toHaveTextContent('페이지가 숨겨져 재생을 중지하고 복호화 키와 입력한 복구 자료를 지웠습니다.')
    })

    const capabilityReads = fetchMock.mock.calls.filter(([path]) => String(path) === '/api/peer-archives/capabilities').length
    Object.defineProperty(document, 'visibilityState', { configurable: true, value: 'visible' })
    fireEvent(document, new Event('visibilitychange'))
    await waitFor(() => expect(fetchMock.mock.calls.filter(([path]) => String(path) === '/api/peer-archives/capabilities').length).toBeGreaterThan(capabilityReads))
  })
})
