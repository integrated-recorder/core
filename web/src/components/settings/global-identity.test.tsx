import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { webcrypto } from 'node:crypto'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { GlobalIdentityPanel } from './global-identity'
import { deriveRecoveryKeys, recoveryMnemonic, signIdentityGenesis } from '@/identity/recovery'
import { capabilityGrantHash } from '@/identity/authority'
import { recipientKeyId } from '@/identity/cryptoarchive'

const enrollmentQRMocks = vi.hoisted(() => ({ decodeFromVideoDevice: vi.fn() }))
vi.mock('@zxing/browser', () => ({
  BrowserQRCodeReader: class {
    decodeFromVideoDevice = enrollmentQRMocks.decodeFromVideoDevice
  },
}))

async function makeForkFixture(keys: Awaited<ReturnType<typeof deriveRecoveryKeys>>) {
  const previousHash = ''
  const encode = (signed: ReturnType<typeof signIdentityGenesis>) => ({ canonical_event: Buffer.from(signed.canonicalEvent).toString('base64url'), signature: Buffer.from(signed.signature).toString('base64url') })
  const first = encode(signIdentityGenesis(keys, 'challenge-one', 'credential-one', new Date('2026-01-01T00:00:00.000Z')))
  const second = encode(signIdentityGenesis(keys, 'challenge-two', 'credential-two', new Date('2026-01-01T00:00:00.000Z')))
  const hash = async (entry: { canonical_event: string }) => {
    const canonical = Buffer.from(entry.canonical_event, 'base64url')
    const prefix = new TextEncoder().encode('IR/identity-event/v1/hash\0')
    const bytes = new Uint8Array(prefix.length + canonical.length)
    bytes.set(prefix); bytes.set(canonical, prefix.length)
    const digest = new Uint8Array(await webcrypto.subtle.digest('SHA-256', bytes))
    return `sha256:${[...digest].map(byte => byte.toString(16).padStart(2, '0')).join('')}`
  }
  const [firstHead, secondHead, firstDirect, secondDirect] = await Promise.all([hash(first), hash(second), hash(first), hash(second)])
  const [lower, upper] = [firstDirect, secondDirect].sort()
  const forkInput = new TextEncoder().encode(`IR/global-identity-fork/v1\0${previousHash}\0${lower}\0${upper}`)
  const forkDigest = new Uint8Array(await webcrypto.subtle.digest('SHA-256', forkInput))
  const forkID = `sha256:${[...forkDigest].map(byte => byte.toString(16).padStart(2, '0')).join('')}`
  return {
    choice: { fork_id: forkID, previous_event_hash: previousHash, first_head_hash: firstHead, second_head_hash: secondHead, first_sequence: '0', second_sequence: '0' },
    bundle: { format: 'ir.identity.history/1' as const, identity_id: keys.identityId, events: [first], fork_evidence: [{ previous_event_hash: previousHash, first_event: first, second_event: second, first_branch: [first], second_branch: [second] }] },
    first, second,
  }
}

describe('global identity account panel', () => {
  afterEach(() => { vi.restoreAllMocks(); vi.unstubAllGlobals() })

  it('resolves a frozen identity only after a recovery-root-signed branch choice', async () => {
    vi.stubGlobal('crypto', webcrypto as unknown as Crypto)
    const entropy = Uint8Array.from({ length: 32 }, (_, index) => index + 1)
    const recoveryPhrase = recoveryMnemonic(entropy)
    const recoveryKeys = await deriveRecoveryKeys(entropy.slice())
    const identityID = recoveryKeys.identityId
    const fixture = await makeForkFixture(recoveryKeys)
    recoveryKeys.destroy()
    const firstHead = fixture.choice.first_head_hash
    const secondHead = fixture.choice.second_head_hash
    const forkID = fixture.choice.fork_id
    const previousHash = fixture.choice.previous_event_hash
    let frozen = true
    let resolutionRequest: { canonical_event: string; signature: string } | undefined
    const firstIdentity = {
      identity_id: identityID, sequence: '2', head_hash: firstHead, created_at: new Date().toISOString(),
      credential_count: 1, credential_ids: ['credential'], node_ids: [], frozen: true, fork_count: 1,
    }
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const path = String(input)
      let payload: unknown = {}
      if (path === '/api/global-identity/status') payload = { initialized: true, identity: frozen ? firstIdentity : { ...firstIdentity, sequence: '1', head_hash: `sha256:${'5'.repeat(64)}`, frozen: false } }
      else if (path === '/api/global-identity/forks') payload = { forks: frozen ? [fixture.choice] : [] }
      else if (path === '/api/global-identity/history') payload = fixture.bundle
      else if (path === '/api/global-identity/session') payload = { authenticated: false }
      else if (path === '/api/global-identity/registry') payload = { identities: [] }
      else if (path === '/api/node-identity/status') payload = { created: false, node: null }
      else if (path === '/api/owner-authority/subjects') payload = { subjects: [] }
      else if (path === '/api/global-identity/forks/resolve') {
        resolutionRequest = JSON.parse(String(init?.body)) as { canonical_event: string; signature: string }
        frozen = false
        payload = { ...firstIdentity, sequence: '3', head_hash: `sha256:${'5'.repeat(64)}`, frozen: false }
      }
      else throw new Error(`unexpected API path: ${path}`)
      return new Response(JSON.stringify(payload), { status: 200, headers: { 'Content-Type': 'application/json' } })
    })
    vi.stubGlobal('fetch', fetchMock)
    const confirm = vi.spyOn(window, 'confirm').mockReturnValue(true)

    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    queryClient.setQueryData(['user', 'authorization'], { user_id: 'usr_owner', login: 'local-owner', roles: ['owner'], permissions: ['system.settings.manage'] })
    render(<QueryClientProvider client={queryClient}><GlobalIdentityPanel /></QueryClientProvider>)

    const resolveButton = await screen.findByRole('button', { name: '선택한 분기를 서명하고 해결' })
    expect(resolveButton).toBeDisabled()
    const firstTranscript = await screen.findByText('관찰된 분기 1 서명 기록 · 이벤트 1개')
    const secondTranscript = await screen.findByText('관찰된 분기 2 서명 기록 · 이벤트 1개')
    fireEvent.click(firstTranscript)
    fireEvent.click(secondTranscript)
    await waitFor(() => {
      expect(document.body.textContent).toContain(fixture.first.canonical_event)
      expect(document.body.textContent).toContain(fixture.first.signature)
      expect(document.body.textContent).toContain(fixture.second.canonical_event)
      expect(document.body.textContent).toContain(fixture.second.signature)
    })
    fireEvent.click(screen.getByRole('button', { name: /관찰된 분기 2 · seq 0/u }))
    expect(resolveButton).toBeDisabled()
    fireEvent.change(screen.getByLabelText('24개 복구 단어'), { target: { value: recoveryPhrase } })
    expect(resolveButton).toBeEnabled()
    fireEvent.click(resolveButton)

    await waitFor(() => expect(resolutionRequest).toBeDefined())
    expect(confirm).toHaveBeenCalledWith(`이 서명된 이력 머리값을 권한 기준으로 사용합니까? ${secondHead}`)
    const signedEvent = JSON.parse(Buffer.from(resolutionRequest!.canonical_event, 'base64url').toString('utf8')) as {
      identity_id: string
      sequence: string
      previous_event_hash: string
      operation: string
      payload: Record<string, string>
    }
    expect(signedEvent).toMatchObject({
      identity_id: identityID,
      sequence: '1',
      previous_event_hash: secondHead,
      operation: 'fork_resolve',
      payload: {
        fork_id: forkID,
        previous_event_hash: previousHash,
        first_head_hash: firstHead,
        second_head_hash: secondHead,
        selected_head_hash: secondHead,
      },
    })
    expect(resolutionRequest!.signature).toMatch(/^[A-Za-z0-9_-]+$/u)
    expect(await screen.findByText('복구 루트 서명 분기 해결을 저장했습니다.')).toBeInTheDocument()
    await waitFor(() => expect(screen.queryByRole('button', { name: '선택한 분기를 서명하고 해결' })).not.toBeInTheDocument())
    expect(screen.queryByText(/신원 이력이 동결되었습니다/u)).not.toBeInTheDocument()
  })

  it('lets an operator export fork transcripts and review both heads without a local Node Key', async () => {
    const identityID = `gi_${'F'.repeat(22)}`
    const firstHead = `sha256:${'1'.repeat(64)}`
    const secondHead = `sha256:${'2'.repeat(64)}`
    const forkID = `sha256:${'3'.repeat(64)}`
    const calls: string[] = []
    vi.spyOn(HTMLAnchorElement.prototype, 'click').mockImplementation(() => {})
    vi.stubGlobal('fetch', vi.fn(async (input: RequestInfo | URL) => {
      const path = String(input)
      calls.push(path)
      let payload: unknown = {}
      if (path === '/api/global-identity/status') payload = { initialized: true, identity: { identity_id: identityID, sequence: '2', head_hash: firstHead, created_at: new Date().toISOString(), credential_count: 1, credential_ids: ['credential'], node_ids: [], frozen: true, fork_count: 1 } }
      else if (path === '/api/global-identity/forks') payload = { forks: [{ fork_id: forkID, previous_event_hash: `sha256:${'4'.repeat(64)}`, first_head_hash: firstHead, second_head_hash: secondHead, first_sequence: '2', second_sequence: '2' }] }
      else if (path === '/api/global-identity/history') payload = { format: 'ir.identity.history/1', identity_id: identityID, events: [], fork_evidence: [] }
      else if (path === '/api/global-identity/session') payload = { authenticated: false }
      else if (path === '/api/global-identity/registry') payload = { identities: [] }
      else if (path === '/api/owner-authority/subjects') payload = { subjects: [] }
      else if (path === '/api/node-identity/status') payload = { created: false, node: null }
      else throw new Error(`unexpected API path: ${path}`)
      return new Response(JSON.stringify(payload), { status: 200, headers: { 'Content-Type': 'application/json' } })
    }))

    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    queryClient.setQueryData(['user', 'authorization'], { user_id: 'usr_owner', login: 'local-owner', roles: ['owner'], permissions: ['system.settings.manage'] })
    render(<QueryClientProvider client={queryClient}><GlobalIdentityPanel /></QueryClientProvider>)

    expect(await screen.findByText('검증된 이력 분기 해결')).toBeInTheDocument()
    expect(await screen.findByText('양쪽의 완전한 검증 기록을 확인할 수 없습니다. 분기 해결은 사용할 수 없습니다.')).toHaveAttribute('role', 'alert')
    expect(await screen.findByRole('button', { name: /관찰된 분기 1 · seq 2/u })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /관찰된 분기 2 · seq 2/u })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /관찰된 분기 1 · seq 2/u })).toBeDisabled()
    expect(screen.getByRole('button', { name: /선택한 분기를 서명하고 해결/u })).toBeDisabled()
    const exportButton = await screen.findByRole('button', { name: '공개 신원 이력 다운로드' })
    fireEvent.click(exportButton)
    await waitFor(() => expect(calls).toContain('/api/global-identity/history'))
    expect(screen.getByRole('button', { name: '이 설치의 Node Key 만들기' })).toBeInTheDocument()
  })

  it('keeps recovery secrets in the browser and submits only a signed genesis plus the Passkey credential', async () => {
    vi.stubGlobal('crypto', webcrypto as unknown as Crypto)
    Object.defineProperty(window, 'isSecureContext', { configurable: true, value: true })
    const createCredential = vi.fn(async () => ({
      id: 'AQIDBA', rawId: Uint8Array.from([1, 2, 3, 4]).buffer, type: 'public-key', authenticatorAttachment: 'platform',
      getClientExtensionResults: () => ({}),
      response: { attestationObject: Uint8Array.from([5, 6]).buffer, clientDataJSON: Uint8Array.from([7, 8]).buffer, getTransports: () => ['internal'] },
    } as unknown as Credential))
    Object.defineProperty(navigator, 'credentials', { configurable: true, value: { create: createCredential, get: vi.fn() } })

    let initialized = false
    let nodeCreated = false
    let posted: unknown
    let delegated: unknown
    let ownerAuthorityEvent: unknown
    const challenge = 'AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE'
    const historyHead = `sha256:${'a'.repeat(64)}`
    let identity = { identity_id: `gi_${'A'.repeat(22)}`, sequence: '0', head_hash: historyHead, created_at: new Date().toISOString(), credential_count: 1, credential_ids: ['AQIDBA'], node_ids: [] as string[], frozen: false, fork_count: 0 }
    const finish = vi.fn()
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const path = String(input)
      let payload: unknown
      if (path === '/api/global-identity/status') payload = initialized ? { initialized: true, identity } : { initialized: false, identity: null }
      else if (path === '/api/global-identity/session') payload = { authenticated: false }
      else if (path === '/api/owner-authority/subjects') payload = { subjects: [] }
      else if (path === '/api/owner-authority/events') {
        ownerAuthorityEvent = JSON.parse(String(init?.body))
        const event = (ownerAuthorityEvent as { event: { subject_id: string; sequence: string; payload: { subject: Record<string, unknown> } } }).event
        payload = { subject: event.payload.subject, sequence: event.sequence, head_hash: historyHead, frozen: false, fork_count: 0, grants: [] }
      }
      else if (path === '/api/node-identity/status') payload = nodeCreated ? { created: true, node: { node_id: 'nd_node_test', public_key: 'AQID' } } : { created: false, node: null }
      else if (path === '/api/node-identity/create') { nodeCreated = true; payload = { created: true, node: { node_id: 'nd_node_test', public_key: 'AQID' }, audit_recorded: true } }
      else if (path === '/api/global-identity/registration/options') payload = { publicKey: { challenge, rp: { id: 'localhost', name: 'Integrated Recorder' }, user: { id: 'AQID', name: 'test-identity', displayName: 'test-identity' }, pubKeyCredParams: [{ type: 'public-key', alg: -7 }] } }
      else if (path === '/api/global-identity/registration/finish') {
        posted = JSON.parse(String(init?.body))
        const event = JSON.parse(Buffer.from((posted as { canonical_event: string }).canonical_event, 'base64url').toString('utf8')) as { identity_id: string }
        identity = { ...identity, identity_id: event.identity_id }
        initialized = true
        finish()
        payload = { initialized: true, identity }
      } else if (path === '/api/node-identity/enrollment/challenge') {
        const context = JSON.parse(String(init?.body)) as { identity_id: string; previous_event_hash: string }
        payload = { ...context, node_id: 'nd_node_test', node_public_key: 'AQID', enrollment_nonce: challenge, node_proof: 'AQID', expires_at: new Date(Date.now() + 60_000).toISOString() }
      } else if (path === '/api/global-identity/nodes/delegate') {
        delegated = JSON.parse(String(init?.body))
        identity = { ...identity, sequence: '1', node_ids: ['nd_node_test'] }
        payload = identity
      } else throw new Error(`unexpected API path: ${path}`)
      return new Response(JSON.stringify(payload), { status: 200, headers: { 'Content-Type': 'application/json' } })
    })
    vi.stubGlobal('fetch', fetchMock)

    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    queryClient.setQueryData(['user', 'authorization'], { user_id: 'usr_test', login: 'owner', roles: ['owner'], permissions: ['system.settings.manage'] })
    render(<QueryClientProvider client={queryClient}><GlobalIdentityPanel /></QueryClientProvider>)
    fireEvent.click(await screen.findByRole('button', { name: '글로벌 신원 만들기' }))
    const phrase = await screen.findByRole('list', { name: '24개 복구 단어' })
    expect(phrase.querySelectorAll('li')).toHaveLength(24)
    const recoveryPhrase = Array.from(phrase.querySelectorAll('li')).map(item => (item.textContent ?? '').replace(/^\d+\.\s*/u, '')).join(' ')
    fireEvent.click(screen.getByRole('checkbox'))
    fireEvent.click(screen.getByRole('button', { name: '첫 Passkey 등록' }))

    await waitFor(() => expect(finish).toHaveBeenCalledOnce())
    expect(createCredential).toHaveBeenCalledOnce()
    const body = posted as { canonical_event: string; signature: string; credential: Record<string, unknown> }
    const canonical = Buffer.from(body.canonical_event, 'base64url').toString('utf8')
    expect(canonical).toContain('"operation":"genesis"')
    expect(canonical).toContain(`"webauthn_challenge":"${challenge}"`)
    expect(canonical).toContain('"recovery_encryption_public_key"')
    expect(canonical).not.toContain(phrase.textContent)
    expect(body.signature).toMatch(/^[A-Za-z0-9_-]+$/u)
    expect(body.credential).toMatchObject({ rawId: 'AQIDBA', type: 'public-key' })
    expect(screen.queryByRole('list', { name: '24개 복구 단어' })).not.toBeInTheDocument()
    fireEvent.click(await screen.findByRole('button', { name: '이 설치의 Node Key 만들기' }))
    expect(await screen.findByText('nd_node_test')).toBeInTheDocument()
    fireEvent.click(await screen.findByRole('button', { name: '일회용 등록 증명 발급' }))
    await screen.findByLabelText('Node 등록 증명 JSON')
    const proofInput = screen.getByLabelText('등록 증명 JSON 파일')
    fireEvent.change(proofInput, { target: { files: [new File(['x'.repeat(16 * 1024 + 1)], 'oversized.json', { type: 'application/json' })] } })
    expect(await screen.findByText('등록 증명 파일이 16 KiB 제한보다 큽니다.')).toBeInTheDocument()
    fireEvent.change(proofInput, { target: { files: [new File(['not-json'], 'invalid.json', { type: 'application/json' })] } })
    expect(await screen.findByText('등록 증명 JSON을 읽을 수 없습니다.')).toBeInTheDocument()
    const proofFile = {
      identity_id: identity.identity_id,
      previous_event_hash: historyHead,
      node_id: 'nd_node_test',
      node_public_key: 'AQID',
      enrollment_nonce: challenge,
      node_proof: 'AQID',
      expires_at: new Date(Date.now() + 60_000).toISOString(),
    }
    fireEvent.change(proofInput, { target: { files: [new File([JSON.stringify({ ...proofFile, private_key: 'must-not-be-accepted' })], 'unexpected-field.json', { type: 'application/json' })] } })
    expect(await screen.findByText('등록 증명 JSON을 읽을 수 없습니다.')).toBeInTheDocument()
    fireEvent.change(proofInput, { target: { files: [new File([JSON.stringify(proofFile)], 'node-enrollment.json', { type: 'application/json' })] } })
    expect(await screen.findByText(/등록 증명 파일을 읽었습니다/u)).toBeInTheDocument()
    expect(screen.getByLabelText('Node 등록 증명 JSON')).toHaveValue(JSON.stringify(proofFile, null, 2))
    fireEvent.change(screen.getByLabelText('24개 복구 단어'), { target: { value: recoveryPhrase } })
    fireEvent.click(screen.getByRole('button', { name: '복구 키로 Node 위임' }))
    await waitFor(() => expect(delegated).toBeTruthy())
    const delegation = JSON.parse(Buffer.from((delegated as { canonical_event: string }).canonical_event, 'base64url').toString('utf8')) as { operation: string; identity_id: string; sequence: string; payload: Record<string, string> }
    expect(delegation).toMatchObject({ operation: 'node_delegate', identity_id: identity.identity_id, sequence: '1', payload: { node_id: 'nd_node_test', node_public_key: 'AQID', enrollment_nonce: challenge, node_proof: 'AQID' } })
    expect(JSON.stringify(delegated)).not.toContain(recoveryPhrase)
    expect(JSON.stringify(delegated)).not.toContain('private_key')

    fireEvent.change(screen.getByLabelText('24개 복구 단어'), { target: { value: recoveryPhrase } })
    fireEvent.click(await screen.findByRole('button', { name: 'OwnerSubject 만들기' }))
    await waitFor(() => expect(ownerAuthorityEvent).toBeTruthy())
    const authorityWire = ownerAuthorityEvent as { event: { event_version: string; operation: string; sequence: string; identity_id: string; subject_id: string; issued_at: string; payload: { subject: Record<string, unknown> } }; signature: string }
    expect(authorityWire.event).toMatchObject({ event_version: 'ir.owner-authority.event/1', operation: 'subject_create', sequence: '1', identity_id: identity.identity_id })
    expect(authorityWire.event.subject_id).toMatch(/^os_[A-Za-z0-9_-]{22}$/u)
    expect(authorityWire.event.issued_at).toMatch(/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ$/u)
    expect(authorityWire.event.payload.subject).toMatchObject({ subject_id: authorityWire.event.subject_id, subject_type: 'person', controller: { kind: 'identity', id: identity.identity_id }, policy_epoch: '1' })
    expect(authorityWire.event.payload.subject.created_at).toBe(authorityWire.event.issued_at)
    expect(authorityWire.signature).toMatch(/^[A-Za-z0-9_-]+$/u)
    expect(JSON.stringify(authorityWire)).not.toContain(recoveryPhrase)
    expect(screen.getByRole('status')).toHaveTextContent('서명된 이력에 OwnerSubject를 만들었습니다.')
    expect(fetchMock.mock.calls.some(([input]) => String(input) === '/api/replica-leases')).toBe(false)
  })

  it('creates a target Node Key, issues a bound proof, and joins only from a signed history file', async () => {
    let nodeCreated = false
    let initialized = false
    const identityID = `gi_${'A'.repeat(22)}`
    const head = `sha256:${'b'.repeat(64)}`
    const proof = { identity_id: identityID, previous_event_hash: head, node_id: 'nd_target_test', node_public_key: 'AQID', enrollment_nonce: 'AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE', node_proof: 'AQID', expires_at: new Date(Date.now() + 60_000).toISOString() }
    const history = { format: 'ir.identity.history/1', identity_id: identityID, events: [{ canonical_event: 'c2lnbmVkLWV2ZW50', signature: 'c2lnbmF0dXJl' }] }
    let importedBody = ''
    vi.stubGlobal('fetch', vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const path = String(input)
      let payload: unknown
      if (path === '/api/global-identity/status') payload = initialized ? { initialized: true, identity: { identity_id: identityID, sequence: '1', head_hash: head, created_at: new Date().toISOString(), credential_count: 0, credential_ids: [], node_ids: ['nd_target_test'], frozen: false, fork_count: 0 } } : { initialized: false, identity: null }
      else if (path === '/api/global-identity/session') payload = { authenticated: false }
      else if (path === '/api/node-identity/status') payload = nodeCreated ? { created: true, node: { node_id: 'nd_target_test', public_key: 'AQID' } } : { created: false, node: null }
      else if (path === '/api/node-identity/create') { nodeCreated = true; payload = { created: true, node: { node_id: 'nd_target_test', public_key: 'AQID' }, audit_recorded: true } }
      else if (path === '/api/node-identity/enrollment/challenge') { expect(JSON.parse(String(init?.body))).toEqual({ identity_id: identityID, previous_event_hash: head }); payload = proof }
      else if (path === '/api/global-identity/history/import') { importedBody = String(init?.body); initialized = true; payload = { identity_id: identityID, sequence: '1', head_hash: head } }
      else throw new Error(`unexpected API path: ${path}`)
      return new Response(JSON.stringify(payload), { status: 200, headers: { 'Content-Type': 'application/json' } })
    }))

    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    queryClient.setQueryData(['user', 'authorization'], { user_id: 'usr_target', login: 'local-owner', roles: ['owner'], permissions: ['system.settings.manage'] })
    render(<QueryClientProvider client={queryClient}><GlobalIdentityPanel /></QueryClientProvider>)
    const createNode = await screen.findByRole('button', { name: '이 설치의 Node Key 만들기' })
    await waitFor(() => expect(createNode).toBeEnabled())
    fireEvent.click(createNode)
    fireEvent.change(await screen.findByLabelText('안정된 신원 ID'), { target: { value: identityID } })
    fireEvent.change(await screen.findByLabelText('현재 이력 머리값'), { target: { value: head } })
    fireEvent.click(screen.getByRole('button', { name: '일회용 등록 증명 발급' }))
    expect(await screen.findByLabelText('Node 등록 증명 JSON')).toHaveValue(JSON.stringify(proof, null, 2))
    const file = new File([JSON.stringify(history)], 'identity-history.json', { type: 'application/json' })
    fireEvent.change(screen.getByLabelText('서명된 신원 이력 JSON'), { target: { files: [file] } })
    fireEvent.click(screen.getByRole('button', { name: '위임된 신원에 연결' }))
    await waitFor(() => expect(initialized).toBe(true))
    expect(importedBody).toContain('"format":"ir.identity.history/1"')
    expect(importedBody).not.toContain('private_key')
    expect(importedBody).not.toContain('signing_seed')
    expect(await screen.findByText(identityID)).toBeInTheDocument()
  })

  it('scans a strict enrollment QR and sends it through the existing recovery-root delegation signature', async () => {
    vi.stubGlobal('crypto', webcrypto as unknown as Crypto)
    Object.defineProperty(window, 'isSecureContext', { configurable: true, value: true })
    Object.defineProperty(navigator, 'mediaDevices', { configurable: true, value: { getUserMedia: vi.fn() } })
    enrollmentQRMocks.decodeFromVideoDevice.mockReset()

    const entropy = Uint8Array.from({ length: 32 }, (_, index) => 80 + index)
    const recoveryPhrase = recoveryMnemonic(entropy)
    const recoveryKeys = await deriveRecoveryKeys(entropy.slice())
    const identityID = recoveryKeys.identityId
    recoveryKeys.destroy()
    const head = `sha256:${'b'.repeat(64)}`
    const proof = {
      identity_id: identityID,
      previous_event_hash: head,
      node_id: 'nd_target_test',
      node_public_key: 'AQID',
      enrollment_nonce: 'AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE',
      node_proof: 'AQID',
      expires_at: new Date(Date.now() + 60_000).toISOString(),
    }
    let identity = { identity_id: identityID, sequence: '0', head_hash: head, created_at: new Date().toISOString(), credential_count: 1, credential_ids: ['credential'], node_ids: [] as string[], frozen: false, fork_count: 0 }
    let delegated: { canonical_event: string; signature: string } | undefined
    let scanPayload = JSON.stringify({ ...proof, private_key: 'must-not-be-accepted' })
    const stopCamera = vi.fn()
    enrollmentQRMocks.decodeFromVideoDevice.mockImplementation(async (_device: unknown, _video: unknown, callback: (result: { getText: () => string } | undefined, error: unknown, controls: { stop: () => void }) => void) => {
      setTimeout(() => callback({ getText: () => scanPayload }, undefined, { stop: stopCamera }), 0)
      return { stop: stopCamera }
    })
    vi.stubGlobal('fetch', vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const path = String(input)
      let payload: unknown = {}
      if (path === '/api/global-identity/status') payload = { initialized: true, identity }
      else if (path === '/api/global-identity/session') payload = { authenticated: false }
      else if (path === '/api/global-identity/registry') payload = { identities: [] }
      else if (path === '/api/owner-authority/subjects') payload = { subjects: [] }
      else if (path === '/api/node-identity/status') payload = { created: true, node: { node_id: 'nd_owner_test', public_key: 'BAUG' } }
      else if (path === '/api/global-identity/nodes/delegate') {
        delegated = JSON.parse(String(init?.body)) as { canonical_event: string; signature: string }
        identity = { ...identity, sequence: '1', node_ids: ['nd_target_test'] }
        payload = identity
      } else throw new Error(`unexpected API path: ${path}`)
      return new Response(JSON.stringify(payload), { status: 200, headers: { 'Content-Type': 'application/json' } })
    }))

    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    queryClient.setQueryData(['user', 'authorization'], { user_id: 'usr_owner', login: 'local-owner', roles: ['owner'], permissions: ['system.settings.manage'] })
    render(<QueryClientProvider client={queryClient}><GlobalIdentityPanel /></QueryClientProvider>)

    fireEvent.click(await screen.findByRole('button', { name: 'Node 등록 QR 스캔' }))
    expect(await screen.findByText('등록 증명 JSON을 읽을 수 없습니다.')).toBeInTheDocument()
    expect(screen.getByLabelText('Node 등록 증명 JSON')).toHaveValue('')
    expect(delegated).toBeUndefined()
    expect(stopCamera).toHaveBeenCalledOnce()
    scanPayload = JSON.stringify(proof)
    fireEvent.click(await screen.findByRole('button', { name: 'Node 등록 QR 스캔' }))
    expect(await screen.findByText('QR 등록 증명을 읽었습니다. 위임 전에 신원과 만료 정보를 확인하세요.')).toBeInTheDocument()
    expect(screen.getByLabelText('Node 등록 증명 JSON')).toHaveValue(JSON.stringify(proof, null, 2))
    expect(stopCamera).toHaveBeenCalledTimes(2)
    fireEvent.change(screen.getByLabelText('24개 복구 단어'), { target: { value: recoveryPhrase } })
    fireEvent.click(screen.getByRole('button', { name: '복구 키로 Node 위임' }))
    await waitFor(() => expect(delegated).toBeDefined())

    const signedEvent = JSON.parse(Buffer.from(delegated!.canonical_event, 'base64url').toString('utf8')) as { operation: string; identity_id: string; sequence: string; previous_event_hash: string; payload: Record<string, string> }
    expect(signedEvent).toMatchObject({
      operation: 'node_delegate',
      identity_id: identityID,
      sequence: '1',
      previous_event_hash: head,
      payload: { node_id: proof.node_id, node_public_key: proof.node_public_key, enrollment_nonce: proof.enrollment_nonce, node_proof: proof.node_proof },
    })
    expect(delegated!.signature).toMatch(/^[A-Za-z0-9_-]+$/u)
    expect(stopCamera).toHaveBeenCalledTimes(2)
  })

  it('shows automatic same-identity sessions and explicitly connects a currently authorized foreign identity', async () => {
    const identityID = `gi_${'I'.repeat(22)}`
    const localNode = `nd_${'L'.repeat(22)}`
    const remoteNode = `nd_${'R'.repeat(22)}`
    const foreignIdentity = `gi_${'F'.repeat(22)}`
    const foreignNode = `nd_${'X'.repeat(22)}`
    const calls: { path: string; method: string }[] = []
    let connectBody: unknown
    vi.stubGlobal('fetch', vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const path = String(input)
      const method = init?.method ?? 'GET'
      calls.push({ path, method })
      let payload: unknown = {}
      if (path === '/api/peer-sessions' && method === 'POST') {
        connectBody = JSON.parse(String(init?.body))
        return new Response(null, { status: 204 })
      }
      if (path === '/api/user/authorization') payload = { user_id: 'usr_owner', login: 'owner', roles: ['owner'], permissions: ['system.settings.manage'] }
      else if (path === '/api/global-identity/status') payload = { initialized: true, identity: { identity_id: identityID, sequence: '2', head_hash: `sha256:${'a'.repeat(64)}`, created_at: new Date().toISOString(), credential_count: 1, credential_ids: ['credential'], node_ids: [localNode, remoteNode], frozen: false, fork_count: 0 } }
      else if (path === '/api/global-identity/registry') payload = { identities: [] }
      else if (path === '/api/global-identity/session') payload = { authenticated: false }
      else if (path === '/api/owner-authority/subjects') payload = { subjects: [] }
      else if (path === '/api/node-identity/status') payload = { created: true, node: { node_id: localNode, public_key: 'AQID' } }
      else if (path === '/api/peer-sessions' && method === 'GET') payload = { sessions: [{ identity_id: identityID, node_id: remoteNode, connected_at: new Date().toISOString() }] }
      else throw new Error(`unexpected API path: ${method} ${path}`)
      return new Response(JSON.stringify(payload), { status: 200, headers: { 'Content-Type': 'application/json' } })
    }))
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    queryClient.setQueryData(['user', 'authorization'], { user_id: 'usr_owner', login: 'owner', roles: ['owner'], permissions: ['system.settings.manage'] })
    render(<QueryClientProvider client={queryClient}><GlobalIdentityPanel /></QueryClientProvider>)

    expect(await screen.findByText(/^연결됨:/u)).toBeInTheDocument()
    expect(screen.getByText(/다른 신원과는 현재 서명된 OwnerSubject 권한/u)).toBeInTheDocument()
    expect(screen.getByLabelText('상대 Global Identity ID')).toBeInTheDocument()
    expect(screen.getByLabelText('상대 Node ID')).toBeInTheDocument()
    fireEvent.change(screen.getByLabelText('상대 Global Identity ID'), { target: { value: foreignIdentity } })
    fireEvent.change(screen.getByLabelText('상대 Node ID'), { target: { value: foreignNode } })
    fireEvent.click(screen.getByRole('button', { name: '다른 신원에 연결' }))
    await waitFor(() => expect(connectBody).toEqual({ identity_id: foreignIdentity, node_id: foreignNode }))
    expect(await screen.findByText('Node 연결이 인증되었습니다.')).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: '연결 해제' })).not.toBeInTheDocument()
    expect(calls).toContainEqual({ path: '/api/peer-sessions', method: 'GET' })
    expect(calls.some(call => call.method === 'DELETE')).toBe(false)
  })

  it('imports public remote history through the verified registry without sending recovery material', async () => {
    const identityID = `gi_${'R'.repeat(22)}`
    const headHash = `sha256:${'c'.repeat(64)}`
    const bundle = { format: 'ir.identity.history/1', identity_id: identityID, events: [{ canonical_event: 'c2lnbmVkLWV2ZW50', signature: 'c2lnbmF0dXJl' }] }
    const registered = { identity_id: identityID, sequence: '1', head_hash: headHash, frozen: false, fork_count: 0, node_ids: [`nd_${'N'.repeat(22)}`], verified_at: new Date().toISOString() }
    let imported: unknown
    let listed = false
    vi.stubGlobal('fetch', vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const path = String(input)
      let payload: unknown
      if (path === '/api/user/authorization') payload = { user_id: 'usr_test', login: 'local-owner', roles: ['owner'], permissions: ['system.settings.manage'] }
      else if (path === '/api/global-identity/status') payload = { initialized: false, identity: null }
      else if (path === '/api/owner-authority/subjects') payload = { subjects: [] }
      else if (path === '/api/global-identity/session') payload = { authenticated: false }
      else if (path === '/api/node-identity/status') payload = { created: false, node: null }
      else if (path === '/api/global-identity/registry/import' && init?.method === 'POST') {
        imported = JSON.parse(String(init.body))
        payload = registered
      } else if (path === '/api/global-identity/registry') {
        payload = { identities: listed ? [registered] : [] }
        listed = true
      } else throw new Error(`unexpected API path: ${path}`)
      return new Response(JSON.stringify(payload), { status: 200, headers: { 'Content-Type': 'application/json' } })
    }))

    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    queryClient.setQueryData(['user', 'authorization'], { user_id: 'usr_test', login: 'local-owner', roles: ['owner'], permissions: ['system.settings.manage'] })
    render(<QueryClientProvider client={queryClient}><GlobalIdentityPanel /></QueryClientProvider>)
    const file = new File([JSON.stringify(bundle)], 'remote-history.json', { type: 'application/json' })
    fireEvent.change(await screen.findByLabelText('원격 신원 공개 이력 JSON'), { target: { files: [file] } })
    fireEvent.click(screen.getByRole('button', { name: '서명 검증 후 원격 이력 가져오기' }))
    await waitFor(() => expect(imported).toEqual(bundle))
    expect(JSON.stringify(imported)).not.toMatch(/recovery|private|seed|passkey/iu)
    expect(await screen.findByText(identityID)).toBeInTheDocument()
    expect(screen.getByText(/검증된 순번 1/u)).toBeInTheDocument()
  })

  it('issues and revokes a scoped capability with browser-only recovery signing material', async () => {
    vi.stubGlobal('crypto', webcrypto as unknown as Crypto)
    const entropy = Uint8Array.from({ length: 32 }, (_, index) => index + 1)
    const recoveryPhrase = recoveryMnemonic(entropy)
    const keys = await deriveRecoveryKeys(entropy)
    const identityID = keys.identityId
    keys.destroy()
    const identityHead = `sha256:${'a'.repeat(64)}`
    const subjectID = `os_${'O'.repeat(22)}`
    const subjectHead = `sha256:${'b'.repeat(64)}`
    const nodeID = `nd_${'N'.repeat(22)}`
    const archiveID = `arc_${'A'.repeat(22)}`
    const identity = { identity_id: identityID, sequence: '1', head_hash: identityHead, created_at: new Date().toISOString(), credential_count: 1, credential_ids: ['credential'], node_ids: [], frozen: false, fork_count: 0 }
    let authorityView: Record<string, unknown> & { grants: Record<string, unknown>[]; archive_ids: string[] } = {
      subject: { subject_id: subjectID, subject_type: 'person', controller: { kind: 'identity', id: identityID }, controller_policy: 'global_identity_recovery_root/1', policy_epoch: '1', created_at: new Date().toISOString() },
      replication_policy: { minimum_replicas: '0', grace_period_seconds: '0', capacity_limit_bytes: '0', relocate_after_grace: false },
      archive_ids: [], rekey_required_archives: [`arc_${'Z'.repeat(22)}`], sequence: '1', head_hash: subjectHead, frozen: false, fork_count: 0, grants: [],
    }
    const postedEvents: { event: { operation: string; sequence: string; subject_id: string; payload: Record<string, unknown> }; signature: string }[] = []
    vi.stubGlobal('fetch', vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const path = String(input)
      let payload: unknown
      if (path === '/api/global-identity/status') payload = { initialized: true, identity }
      else if (path === '/api/global-identity/session') payload = { authenticated: false }
      else if (path === '/api/owner-authority/subjects') payload = { subjects: [authorityView] }
      else if (path === '/api/recordings/recording-for-capability/archive/snapshot') payload = { recording_id: 'recording-for-capability', archive_id: archiveID, snapshot_id: `snap_${'b'.repeat(64)}`, sequence: 4, object_count: 7, coverage_count: 1 }
      else if (path === '/api/node-identity/status') payload = { created: false, node: null }
      else if (path === '/api/owner-authority/events') {
        const posted = JSON.parse(String(init?.body)) as typeof postedEvents[number]
        postedEvents.push(posted)
        if (posted.event.operation === 'archive_claim') {
          authorityView = { ...authorityView, archive_ids: [...authorityView.archive_ids, posted.event.payload.archive_id as string], sequence: posted.event.sequence, head_hash: `sha256:${'e'.repeat(64)}` }
        } else if (posted.event.operation === 'grant_issue') {
          const signed = (posted.event.payload.chain as { grant: Record<string, unknown> }[])[0]
          const grant = signed.grant
          const grantHash = await capabilityGrantHash(grant as never)
          authorityView = {
            ...authorityView, sequence: posted.event.sequence, head_hash: `sha256:${'c'.repeat(64)}`,
            grants: [{ grant_hash: grantHash, issuer: grant.issuer as Record<string, unknown>, audience: grant.audience as Record<string, unknown>, archive_id: grant.archive_id as string, snapshot_id: grant.snapshot_id as string | undefined, actions: grant.actions as string[], expires_at: grant.expires_at as string, revoked: false, delegation_depth: grant.delegation_depth as string }],
          }
        } else if (posted.event.operation === 'grant_revoke') {
          authorityView = { ...authorityView, sequence: posted.event.sequence, head_hash: `sha256:${'d'.repeat(64)}`, grants: authorityView.grants.map(grant => grant.grant_hash === posted.event.payload.grant_hash ? { ...grant, revoked: true } : grant) }
        } else if (posted.event.operation === 'replication_policy_set') {
          authorityView = { ...authorityView, sequence: posted.event.sequence, head_hash: `sha256:${'f'.repeat(64)}`, replication_policy: posted.event.payload.policy }
        }
        payload = authorityView
      } else throw new Error(`unexpected API path: ${path}`)
      return new Response(JSON.stringify(payload), { status: 200, headers: { 'Content-Type': 'application/json' } })
    }))

    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    queryClient.setQueryData(['user', 'authorization'], { user_id: 'usr_test', login: 'local-owner', roles: ['owner'], permissions: ['system.settings.manage'] })
    vi.spyOn(window, 'confirm').mockReturnValue(true)
    render(<QueryClientProvider client={queryClient}><GlobalIdentityPanel /></QueryClientProvider>)
    expect(await screen.findByText('소유권 이전 후 새 복호화 키 검증이 끝나지 않아 이 아카이브 권한이 잠겨 있습니다.')).toBeInTheDocument()
    fireEvent.change(await screen.findByLabelText('24개 복구 단어'), { target: { value: recoveryPhrase } })
    fireEvent.change(screen.getByLabelText('대상 Identity 또는 Node ID'), { target: { value: nodeID } })
    fireEvent.change(screen.getByLabelText('녹화 ID'), { target: { value: 'recording-for-capability' } })
    fireEvent.click(screen.getByRole('button', { name: '봉인된 스냅샷 조회' }))
    await waitFor(() => expect(screen.getByLabelText('Archive ID')).toHaveValue(archiveID))
    expect(screen.getByLabelText('선택적 Snapshot ID')).toHaveValue(`snap_${'b'.repeat(64)}`)
    fireEvent.change(screen.getByLabelText('Archive ID'), { target: { value: archiveID } })
    fireEvent.click(screen.getByRole('button', { name: '선택한 아카이브 연결' }))
    await waitFor(() => expect(postedEvents).toHaveLength(1))
    expect(postedEvents[0].event).toMatchObject({ operation: 'archive_claim', sequence: '2', payload: { archive_id: archiveID } })
    expect(JSON.stringify(postedEvents[0])).not.toContain(recoveryPhrase)
    expect(JSON.stringify(postedEvents[0])).not.toContain('private_key')

    const rotateKeyAction = await screen.findByLabelText('아카이브 키 회전')
    expect(rotateKeyAction).not.toBeChecked()
    fireEvent.click(rotateKeyAction)
    fireEvent.change(screen.getByLabelText('24개 복구 단어'), { target: { value: recoveryPhrase } })
    fireEvent.click(screen.getByRole('button', { name: '아카이브 Capability 발급' }))
    await waitFor(() => expect(postedEvents).toHaveLength(2))
    expect(postedEvents[1].event).toMatchObject({ operation: 'grant_issue', identity_id: identityID, sequence: '3', payload: { chain: [{ grant: { issuer: { kind: 'identity', id: identityID }, audience: { kind: 'node', id: nodeID }, archive_id: archiveID, actions: ['archive.view', 'key.rotate'], policy_epoch: '1' } }] } })
    expect(JSON.stringify(postedEvents[1])).not.toContain(recoveryPhrase)
    expect(JSON.stringify(postedEvents[1])).not.toContain('private_key')

    fireEvent.change(screen.getByLabelText('24개 복구 단어'), { target: { value: recoveryPhrase } })
    fireEvent.click(await screen.findByRole('button', { name: '권한 폐기' }))
    await waitFor(() => expect(postedEvents).toHaveLength(3))
    expect(postedEvents[2].event).toMatchObject({ operation: 'grant_revoke', sequence: '4', payload: { grant_hash: authorityView.grants[0].grant_hash } })
    expect(JSON.stringify(postedEvents[2])).not.toContain(recoveryPhrase)
    expect(await screen.findByText(/폐기됨/u)).toBeInTheDocument()

    fireEvent.change(screen.getByLabelText('24개 복구 단어'), { target: { value: recoveryPhrase } })
    fireEvent.change(screen.getByLabelText('최소 복제본 수 (0–16)'), { target: { value: '2' } })
    fireEvent.change(screen.getByLabelText('재배치 전 유예 시간 (초, 최대 30일)'), { target: { value: '3600' } })
    fireEvent.change(screen.getByLabelText('복제본 총 용량 제한 (바이트, 0은 제한 없음)'), { target: { value: '1073741824' } })
    fireEvent.click(screen.getByLabelText('유예 시간 후 다른 공급자로 재배치 허용'))
    fireEvent.click(screen.getByRole('button', { name: '서명된 정책 저장' }))
    await waitFor(() => expect(postedEvents).toHaveLength(4))
    expect(postedEvents[3].event).toMatchObject({
      operation: 'replication_policy_set', identity_id: identityID, subject_id: subjectID, sequence: '5',
      payload: { policy: { minimum_replicas: '2', grace_period_seconds: '3600', capacity_limit_bytes: '1073741824', relocate_after_grace: true } },
    })
    expect(JSON.stringify(postedEvents[3])).not.toContain(recoveryPhrase)
    expect(await screen.findByText('OwnerSubject 복제 정책을 서명된 이력에 저장했습니다.')).toBeInTheDocument()
  })

  it('creates a dual-consent transfer offer from current verified identity heads', async () => {
    vi.stubGlobal('crypto', webcrypto as unknown as Crypto)
    Object.defineProperty(window, 'isSecureContext', { configurable: true, value: true })
    Object.defineProperty(navigator, 'credentials', { configurable: true, value: {} })
    const sourceEntropy = Uint8Array.from({ length: 32 }, (_, index) => index + 31)
    const recipientEntropy = Uint8Array.from({ length: 32 }, (_, index) => index + 71)
    const recoveryPhrase = recoveryMnemonic(sourceEntropy)
    const sourceKeys = await deriveRecoveryKeys(sourceEntropy.slice())
    const recipientKeys = await deriveRecoveryKeys(recipientEntropy.slice())
    const identityID = sourceKeys.identityId
    const recipientID = recipientKeys.identityId
    const identityHead = `sha256:${'a'.repeat(64)}`
    const recipientHead = `sha256:${'b'.repeat(64)}`
    const verifiedAt = new Date().toISOString()
    const sourceVerified = { identity_id: identityID, head_hash: identityHead, verified_at: verifiedAt, fresh_until: new Date(Date.now() + 4 * 60_000).toISOString(), frozen: false, identity_signing_public_key: Buffer.from(sourceKeys.signingPublicKey).toString('base64url'), recovery_encryption_public_key: Buffer.from(sourceKeys.encryptionPublicKey).toString('base64url') }
    const recipientVerified = { identity_id: recipientID, head_hash: recipientHead, verified_at: verifiedAt, fresh_until: new Date(Date.now() + 4 * 60_000).toISOString(), frozen: false, identity_signing_public_key: Buffer.from(recipientKeys.signingPublicKey).toString('base64url'), recovery_encryption_public_key: Buffer.from(recipientKeys.encryptionPublicKey).toString('base64url') }
    sourceKeys.destroy()
    recipientKeys.destroy()
    const subjectID = `os_${'T'.repeat(22)}`
    const authoritySubject = {
      subject: { subject_id: subjectID, subject_type: 'person', controller: { kind: 'identity', id: identityID }, controller_policy: 'global_identity_recovery_root/1', policy_epoch: '1', created_at: verifiedAt },
      archive_ids: [], rekey_required_archives: [], sequence: '1', head_hash: `sha256:${'c'.repeat(64)}`, frozen: false, fork_count: 0, grants: [],
    }
    const paths: string[] = []
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const path = String(input)
      paths.push(path)
      let payload: unknown
      if (path === '/api/global-identity/status') payload = { initialized: true, identity: { identity_id: identityID, sequence: '1', head_hash: identityHead, created_at: verifiedAt, credential_count: 1, credential_ids: ['credential'], node_ids: [], frozen: false, fork_count: 0 } }
      else if (path === '/api/global-identity/session') payload = { authenticated: false }
      else if (path === '/api/global-identity/registry') payload = { identities: [] }
      else if (path === '/api/node-identity/status') payload = { created: false, node: null }
      else if (path === '/api/owner-authority/subjects') payload = { subjects: [authoritySubject] }
      else if (path === `/api/global-identity/verified/${identityID}`) payload = sourceVerified
      else if (path === `/api/global-identity/verified/${recipientID}`) payload = recipientVerified
      else throw new Error(`unexpected API path: ${path}`)
      return new Response(JSON.stringify(payload), { status: 200, headers: { 'Content-Type': 'application/json' } })
    })
    vi.stubGlobal('fetch', fetchMock)
    vi.spyOn(HTMLAnchorElement.prototype, 'click').mockImplementation(() => {})
    const downloaded: Blob[] = []
    vi.spyOn(URL, 'createObjectURL').mockImplementation(blob => { downloaded.push(blob as Blob); return 'blob:transfer-offer' })

    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    queryClient.setQueryData(['user', 'authorization'], { user_id: 'usr_test', login: 'local-owner', roles: ['owner'], permissions: ['system.settings.manage'] })
    render(<QueryClientProvider client={queryClient}><GlobalIdentityPanel /></QueryClientProvider>)
    fireEvent.change(await screen.findByLabelText('24개 복구 단어'), { target: { value: recoveryPhrase } })
    fireEvent.change(screen.getByLabelText('수신 Global Identity ID'), { target: { value: recipientID } })
    fireEvent.click(await screen.findByRole('button', { name: '서명된 이전 제안 다운로드' }))
    expect(await screen.findByText('서명된 제안을 다운로드했습니다. 수신자에게 안전한 경로로 전달하세요.')).toBeInTheDocument()
    expect(paths).toContain(`/api/global-identity/verified/${identityID}`)
    expect(paths).toContain(`/api/global-identity/verified/${recipientID}`)
    expect(downloaded).toHaveLength(1)
    expect(screen.getByRole('button', { name: '서명된 이전 제안 다운로드' })).toBeDisabled()
  })

  it('resumes a possibly committed replica offer after reload without recovery material', async () => {
    vi.stubGlobal('crypto', webcrypto as unknown as Crypto)
    Object.defineProperty(navigator, 'credentials', { configurable: true, value: {} })
    const browserWindow = window
    Object.defineProperty(browserWindow, 'isSecureContext', { configurable: true, value: true })
    vi.stubGlobal('window', new Proxy(browserWindow, {
      get(target, property) {
        if (property === 'isSecureContext') return true
        if (property === 'location') return { ...target.location, protocol: 'https:' }
        return Reflect.get(target, property, target)
      },
    }))

    const entropy = Uint8Array.from({ length: 32 }, (_, index) => index + 9)
    const recoveryPhrase = recoveryMnemonic(entropy)
    const recoveryKeys = await deriveRecoveryKeys(entropy.slice())
    const identityID = recoveryKeys.identityId
    const recoveryPublicKey = Buffer.from(recoveryKeys.encryptionPublicKey).toString('base64url')
    recoveryKeys.destroy()

    const localNodeID = `nd_${'L'.repeat(22)}`
    const providerNodeID = `nd_${'P'.repeat(22)}`
    const subjectID = `os_${'O'.repeat(22)}`
    const archiveID = `arc_${'A'.repeat(22)}`
    const snapshotID = `snap_${'B'.repeat(64)}`
    const grantHash = `sha256:${'c'.repeat(64)}`
    const offerBodies: Record<string, unknown>[] = []
    let durableLease: Record<string, unknown> | null = null

    vi.stubGlobal('fetch', vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const path = String(input)
      let payload: unknown
      if (path === '/api/global-identity/status') payload = {
        initialized: true,
        identity: {
          identity_id: identityID, sequence: '2', head_hash: `sha256:${'a'.repeat(64)}`,
          recovery_encryption_public_key: recoveryPublicKey, created_at: new Date().toISOString(),
          credential_count: 1, credential_ids: ['credential'], node_ids: [localNodeID, providerNodeID], frozen: false, fork_count: 0,
        },
      }
      else if (path === '/api/global-identity/registry') payload = { identities: [] }
      else if (path === '/api/global-identity/session') payload = { authenticated: false }
      else if (path === '/api/node-identity/status') payload = { created: true, node: { node_id: localNodeID, public_key: 'AQID' } }
      else if (path === '/api/owner-authority/subjects') payload = {
        subjects: [{
          subject: { subject_id: subjectID, subject_type: 'person', controller: { kind: 'identity', id: identityID }, controller_policy: 'global_identity_recovery_root/1', policy_epoch: '1', created_at: new Date().toISOString() },
          archive_ids: [archiveID], sequence: '2', head_hash: `sha256:${'b'.repeat(64)}`, frozen: false, fork_count: 0,
          grants: [{ grant_hash: grantHash, subject_id: subjectID, issuer: { kind: 'identity', id: identityID }, audience: { kind: 'node', id: localNodeID }, archive_id: archiveID, snapshot_id: snapshotID, actions: ['archive.replicate'], expires_at: new Date(Date.now() + 60_000).toISOString(), delegation_depth: '0', revoked: false }],
        }],
      }
      else if (path === '/api/peer-sessions') payload = { sessions: [{ identity_id: identityID, node_id: providerNodeID, connected_at: new Date().toISOString() }] }
      else if (path === '/api/replica-leases') payload = { leases: durableLease ? [durableLease] : [], tombstones: [] }
      else if (path === '/api/recordings/recording-retry/archive/snapshot') payload = { recording_id: 'recording-retry', archive_id: archiveID, snapshot_id: snapshotID, sequence: 7, object_count: 3, coverage_count: 3 }
      else if (path === '/api/recordings/recording-retry/replicas/offers') {
        const body = JSON.parse(String(init?.body)) as Record<string, unknown>
        offerBodies.push(body)
        durableLease = {
          lease_id: body.lease_id, status: 'offered', archive_id: archiveID, snapshot_id: snapshotID,
          provider_node_id: providerNodeID, owner_subject_id: body.owner_subject_id,
          grant_hash: body.grant_hash, key_epoch: String(body.key_epoch), expires_at: new Date(Date.now() + 60_000).toISOString(),
        }
        if (offerBodies.length === 1) {
          return new Response(JSON.stringify({ error_code: 'response_lost', error: 'The response was lost after the offer was committed.' }), { status: 503, headers: { 'Content-Type': 'application/json' } })
        }
        payload = { lease: durableLease, reused: true }
      }
      else throw new Error(`unexpected API request: ${init?.method ?? 'GET'} ${path}`)
      return new Response(JSON.stringify(payload), { status: 200, headers: { 'Content-Type': 'application/json' } })
    }))

    const makeQueryClient = () => {
      const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
      client.setQueryData(['user', 'authorization'], { user_id: 'usr_owner', login: 'owner', roles: ['owner'], permissions: ['system.settings.manage', 'recording.read'] })
      return client
    }
    let queryClient = makeQueryClient()
    let panel = render(<QueryClientProvider client={queryClient}><GlobalIdentityPanel /></QueryClientProvider>)

    fireEvent.change(await screen.findByLabelText('암호화할 녹화 ID'), { target: { value: 'recording-retry' } })
    fireEvent.click(screen.getByRole('button', { name: '봉인된 스냅샷 확인' }))
    await waitFor(() => expect(screen.getByText(`${archiveID} · ${snapshotID} · 객체 3개`)).toBeInTheDocument())
    fireEvent.change(screen.getByLabelText('복제본을 보관할 위임 Node'), { target: { value: providerNodeID } })
    fireEvent.change(screen.getByLabelText('24개 복구 단어'), { target: { value: recoveryPhrase } })
    fireEvent.click(screen.getByRole('button', { name: '암호화하고 서명된 복제 Offer 만들기' }))

    expect(await screen.findByRole('alert')).toBeInTheDocument()
    await screen.findByRole('button', { name: '같은 복제 요청 안전하게 재시도' })
    await waitFor(() => expect(queryClient.getQueryData(['replica-leases'])).toMatchObject({ leases: [{ lease_id: offerBodies[0]?.lease_id }] }))

    panel.unmount()
    queryClient = makeQueryClient()
    panel = render(<QueryClientProvider client={queryClient}><GlobalIdentityPanel /></QueryClientProvider>)
    fireEvent.change(await screen.findByLabelText('암호화할 녹화 ID'), { target: { value: 'recording-retry' } })
    fireEvent.click(screen.getByRole('button', { name: '봉인된 스냅샷 확인' }))
    await waitFor(() => expect(screen.getByText(`${archiveID} · ${snapshotID} · 객체 3개`)).toBeInTheDocument())
    fireEvent.change(screen.getByLabelText('복제본을 보관할 위임 Node'), { target: { value: providerNodeID } })
    fireEvent.click(await screen.findByRole('button', { name: '저장된 Offer 재개' }))
    const retryAfterReload = screen.getByRole('button', { name: '같은 복제 요청 안전하게 재시도' })
    expect(retryAfterReload).toBeEnabled()
    fireEvent.click(retryAfterReload)

    await waitFor(() => expect(offerBodies).toHaveLength(2))
    expect(offerBodies[0]).toHaveProperty('data_key')
    expect(offerBodies[0]).toHaveProperty('key_envelope')
    expect(offerBodies[1]).not.toHaveProperty('data_key')
    expect(offerBodies[1]).not.toHaveProperty('key_envelope')
    expect(offerBodies[1]).toMatchObject({ lease_id: offerBodies[0].lease_id, key_epoch: offerBodies[0].key_epoch, provider_node_id: providerNodeID })
    expect(await screen.findByText(`기존 서명 Offer ${offerBodies[0].lease_id}를 재사용했습니다.`)).toBeInTheDocument()
  })

  it('syncs an agreed signed OwnerSubject history by ID from every Discovery mirror', async () => {
    const identityID = `gi_${'G'.repeat(22)}`
    const subjectID = `os_${'S'.repeat(22)}`
    const subjectView = {
      subject: { subject_id: subjectID, subject_type: 'person', controller: { kind: 'identity', id: identityID }, controller_policy: 'global_identity_recovery_root/1', policy_epoch: '1', created_at: new Date().toISOString() },
      archive_ids: [], rekey_required_archives: [], sequence: '1', head_hash: `sha256:${'a'.repeat(64)}`, frozen: false, fork_count: 0, grants: [],
    }
    let synced = false
    let syncRequest: { path: string; method?: string } | null = null
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const path = String(input)
      let payload: unknown = {}
      if (path === '/api/global-identity/status') payload = { initialized: true, identity: { identity_id: identityID, sequence: '1', head_hash: `sha256:${'b'.repeat(64)}`, created_at: new Date().toISOString(), credential_count: 1, credential_ids: ['credential'], node_ids: [], frozen: false, fork_count: 0 } }
      else if (path === '/api/global-identity/session') payload = { authenticated: false }
      else if (path === '/api/global-identity/registry') payload = { identities: [] }
      else if (path === '/api/node-identity/status') payload = { created: false, node: null }
      else if (path === '/api/owner-authority/subjects') payload = { subjects: synced ? [subjectView] : [] }
      else if (path === `/api/owner-authority/subjects/${subjectID}/sync`) {
        syncRequest = { path, method: init?.method }
        synced = true
        payload = subjectView
      }
      else throw new Error(`unexpected API request: ${init?.method ?? 'GET'} ${path}`)
      return new Response(JSON.stringify(payload), { status: 200, headers: { 'Content-Type': 'application/json' } })
    })
    vi.stubGlobal('fetch', fetchMock)

    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    queryClient.setQueryData(['user', 'authorization'], { user_id: 'usr_owner', login: 'local-owner', roles: ['owner'], permissions: ['system.settings.manage'] })
    render(<QueryClientProvider client={queryClient}><GlobalIdentityPanel /></QueryClientProvider>)

    fireEvent.change(await screen.findByLabelText('동기화할 OwnerSubject ID'), { target: { value: subjectID } })
    fireEvent.click(screen.getByRole('button', { name: '서명 이력 동기화' }))

    await waitFor(() => expect(syncRequest).toEqual({ path: `/api/owner-authority/subjects/${subjectID}/sync`, method: 'POST' }))
    expect(await screen.findByText('모든 Discovery 제공자가 동의한 서명 OwnerSubject 이력을 가져왔습니다.')).toBeInTheDocument()
    expect((await screen.findAllByText(subjectID)).length).toBeGreaterThan(0)
  })

  it('signs rekey completion from the exact recipient evidence files after refreshing identity state', async () => {
    vi.stubGlobal('crypto', webcrypto as unknown as Crypto)
    const browserWindow = window
    Object.defineProperty(browserWindow, 'isSecureContext', { configurable: true, value: true })
    vi.stubGlobal('window', new Proxy(browserWindow, {
      get(target, property) {
        if (property === 'isSecureContext') return true
        if (property === 'location') return { ...target.location, protocol: 'https:' }
        return Reflect.get(target, property, target)
      },
    }))
    Object.defineProperty(navigator, 'credentials', { configurable: true, value: {} })

    const entropy = Uint8Array.from({ length: 32 }, (_, index) => index + 41)
    const recoveryPhrase = recoveryMnemonic(entropy)
    const keys = await deriveRecoveryKeys(entropy.slice())
    const identityID = keys.identityId
    const signingPublicKey = Buffer.from(keys.signingPublicKey).toString('base64url')
    const recoveryEncryptionPublicKey = Buffer.from(keys.encryptionPublicKey).toString('base64url')
    const recipientID = recipientKeyId(keys.encryptionPublicKey)
    keys.destroy()

    const subjectID = `os_${'S'.repeat(22)}`
    const archiveID = `arc_${'A'.repeat(22)}`
    const identityHead = `sha256:${'1'.repeat(64)}`
    const subjectHead = `sha256:${'2'.repeat(64)}`
    const transferHash = `sha256:${'3'.repeat(64)}`
    const identity = {
      identity_id: identityID, sequence: '3', head_hash: identityHead, created_at: new Date().toISOString(),
      credential_count: 1, credential_ids: ['credential'], node_ids: [], frozen: false, fork_count: 0,
    }
    const subjectBase = {
      subject: { subject_id: subjectID, subject_type: 'person' as const, controller: { kind: 'identity' as const, id: identityID }, controller_policy: 'global_identity_recovery_root/1', policy_epoch: '2', created_at: new Date().toISOString() },
      archive_ids: [archiveID], rekey_required_archives: [archiveID], rekey_required_since_event_hash: transferHash,
      sequence: '5', head_hash: subjectHead, frozen: false, fork_count: 0, grants: [],
    }
    type SubjectView = Omit<typeof subjectBase, 'rekey_required_since_event_hash'> & { rekey_required_since_event_hash?: string }
    let subjectView: SubjectView = subjectBase
    let verifiedRecipientReads = 0
    let completionRequest: { event: { operation: string; identity_id: string; sequence: string; subject_id: string; payload: Record<string, unknown> }; signature: string } | undefined

    vi.stubGlobal('fetch', vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const path = String(input)
      let payload: unknown = {}
      if (path === '/api/global-identity/status') payload = { initialized: true, identity }
      else if (path === '/api/global-identity/session') payload = { authenticated: false }
      else if (path === '/api/global-identity/registry') payload = { identities: [] }
      else if (path === '/api/node-identity/status') payload = { created: false, node: null }
      else if (path === '/api/owner-authority/subjects') payload = { subjects: [subjectView] }
      else if (path === `/api/global-identity/verified/${identityID}`) {
        verifiedRecipientReads += 1
        const now = Date.now()
        payload = {
          identity_id: identityID, head_hash: identityHead,
          verified_at: new Date(now - 1000).toISOString(), fresh_until: new Date(now + 4 * 60_000).toISOString(), frozen: false,
          identity_signing_public_key: signingPublicKey, recovery_encryption_public_key: recoveryEncryptionPublicKey,
        }
      }
      else if (path === '/api/owner-authority/events') {
        completionRequest = JSON.parse(String(init?.body)) as typeof completionRequest
        subjectView = { ...subjectBase, rekey_required_archives: [], rekey_required_since_event_hash: undefined, sequence: '6', head_hash: `sha256:${'4'.repeat(64)}` }
        payload = subjectView
      }
      else throw new Error(`unexpected API request: ${init?.method ?? 'GET'} ${path}`)
      return new Response(JSON.stringify(payload), { status: 200, headers: { 'Content-Type': 'application/json' } })
    }))

    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    queryClient.setQueryData(['user', 'authorization'], { user_id: 'usr_owner', login: 'local-owner', roles: ['owner'], permissions: ['system.settings.manage'] })
    render(<QueryClientProvider client={queryClient}><GlobalIdentityPanel /></QueryClientProvider>)

    expect(await screen.findByText(/이 아카이브 권한이 잠겨 있습니다/u)).toBeInTheDocument()
    fireEvent.change(await screen.findByLabelText('24개 복구 단어'), { target: { value: recoveryPhrase } })
    const evidence = {
      format: 'ir.owner-subject-rekey-evidence/1', owner_subject_id: subjectID, source_identity_id: `gi_${'T'.repeat(22)}`, recipient_identity_id: identityID,
      archive_id: archiveID,
      evidence: {
        archive_id: archiveID, previous_key_epoch: '1', new_key_epoch: '2', snapshot_count: '2',
        head_snapshot_id: `snap_${'a'.repeat(64)}`, bundle_set_commitment: `sha256:${'b'.repeat(64)}`,
        envelope_sha256: `sha256:${'c'.repeat(64)}`, recipient_key_id: recipientID,
      },
    }
    const file = new File([JSON.stringify(evidence)], 'verified-rekey-evidence.json', { type: 'application/json' })
    fireEvent.change(screen.getByLabelText('검증된 Archive evidence JSON 파일들'), { target: { files: [file] } })
    const complete = screen.getByRole('button', { name: '모든 pending Archive 완료 서명' })
    expect(complete).toBeEnabled()
    fireEvent.click(complete)

    await waitFor(() => expect(verifiedRecipientReads).toBe(1))
    await waitFor(() => expect(completionRequest).toBeDefined())
    expect(verifiedRecipientReads).toBe(1)
    expect(completionRequest!.event).toMatchObject({
      operation: 'archive_rekey_complete', identity_id: identityID, subject_id: subjectID, sequence: '6',
      payload: { transfer_event_hash: transferHash, archives: [{ archive_id: archiveID, previous_key_epoch: '1', new_key_epoch: '2', recipient_key_id: recipientID }] },
    })
    expect(completionRequest!.signature).toMatch(/^[A-Za-z0-9_-]+$/u)
    expect(JSON.stringify(completionRequest)).not.toContain(recoveryPhrase)
    expect(JSON.stringify(completionRequest)).not.toContain('private_key')
    expect(await screen.findByText('모든 pending Archive의 재키잉 증거를 새 OwnerSubject 이력에 서명했습니다.')).toBeInTheDocument()
  })
})
