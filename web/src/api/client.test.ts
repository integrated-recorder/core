import { afterEach, describe, expect, it, vi } from 'vitest'
import { api, invalidateCSRFToken, setCSRFToken } from './client'
import { peerArchivesAPI, recordingsAPI, replicaImportsAPI } from './index'

describe('API CSRF token scope', () => {
  afterEach(() => { invalidateCSRFToken(); vi.unstubAllGlobals() })

  it('does not let a separate global-identity CSRF projection replace the local account token', async () => {
    const requests: RequestInit[] = []
    vi.stubGlobal('fetch', vi.fn(async (_input: RequestInfo | URL, init?: RequestInit) => {
      requests.push(init ?? {})
      const data = requests.length === 1 ? { authenticated: true, global_csrf_token: 'global-token' } : { ok: true }
      return new Response(JSON.stringify(data), { status: 200, headers: { 'Content-Type': 'application/json' } })
    }))
    setCSRFToken('local-account-token')

    await api('/api/global-identity/session')
    await api('/api/global-identity/registration/options', { method: 'POST', body: {} })

    const headers = new Headers(requests[1]?.headers)
    expect(headers.get('X-CSRF-Token')).toBe('local-account-token')
  })

  it('submits a replica offer through the HTTPS-only public recording API', async () => {
    const requests: Array<{ url: string; init?: RequestInit }> = []
    const request = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      requests.push({ url: String(input), init })
      return new Response(JSON.stringify({ lease: { lease_id: 'rl_AAAAAAAAAAAAAAAAAAAAAA', status: 'offered' } }), {
        status: 200, headers: { 'Content-Type': 'application/json' },
      })
    })
    vi.stubGlobal('fetch', request)
    const body: Parameters<typeof recordingsAPI.createReplicaOffer>[1] = {
      owner_subject_id: 'os_AAAAAAAAAAAAAAAAAAAAAA', grant_hash: 'sha256:' + 'a'.repeat(64),
      key_epoch: 123, data_key: 'ephemeral', key_envelope: {
        envelope_version: 'ir.key-envelope/1', archive_id: 'arc_AAAAAAAAAAAAAAAAAAAAAA', key_epoch: '123',
        recipient_key_id: 'recipient', recipient_public_key_sha256: 'sha256:' + 'b'.repeat(64),
        kem: 'DHKEM(X25519,HKDF-SHA256)', kdf: 'HKDF-SHA256', aead: 'AES-256-GCM',
        encapsulation: 'AQ', ciphertext: 'AQ', signer_key_id: 'signer', binding_hash: 'sha256:' + 'c'.repeat(64), signature: 'AQ',
      },
      lease_id: 'rl_AAAAAAAAAAAAAAAAAAAAAA', provider_node_id: 'nd_BBBBBBBBBBBBBBBBBBBBBB',
    }

    await recordingsAPI.createReplicaOffer('recording/one', body)

    expect(request).toHaveBeenCalledOnce()
    expect(requests[0]?.url).toBe('/api/recordings/recording%2Fone/replicas/offers')
    const init = requests[0]?.init
    expect(init?.method).toBe('POST')
    expect(JSON.parse(String(init?.body))).toEqual(body)
  })

  it('retries the same replica offer without resubmitting browser key material', async () => {
    const requests: Array<{ url: string; init?: RequestInit }> = []
    vi.stubGlobal('fetch', vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      requests.push({ url: String(input), init })
      return new Response(JSON.stringify({ lease: { lease_id: 'rl_AAAAAAAAAAAAAAAAAAAAAA', status: 'offered' }, reused: true }), {
        status: 200, headers: { 'Content-Type': 'application/json' },
      })
    }))
    const body: Parameters<typeof recordingsAPI.createReplicaOffer>[1] = {
      owner_subject_id: 'os_AAAAAAAAAAAAAAAAAAAAAA', grant_hash: 'sha256:' + 'a'.repeat(64),
      key_epoch: 123, lease_id: 'rl_AAAAAAAAAAAAAAAAAAAAAA', provider_node_id: 'nd_BBBBBBBBBBBBBBBBBBBBBB',
    }

    await recordingsAPI.createReplicaOffer('recording/one', body)

    expect(requests).toHaveLength(1)
    expect(requests[0]?.url).toBe('/api/recordings/recording%2Fone/replicas/offers')
    expect(JSON.parse(String(requests[0]?.init?.body))).toEqual(body)
    expect(String(requests[0]?.init?.body)).not.toMatch(/data_key|key_envelope/)
  })

  it('allows a replication retry to reuse the source Engine key without resubmitting key material', async () => {
    const requests: Array<{ url: string; init?: RequestInit }> = []
    vi.stubGlobal('fetch', vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      requests.push({ url: String(input), init })
      return new Response(JSON.stringify({ manifest: {} }), {
        status: 200, headers: { 'Content-Type': 'application/json' },
      })
    }))
    const body: Parameters<typeof recordingsAPI.encryptArchiveReplica>[1] = {
      owner_subject_id: 'os_AAAAAAAAAAAAAAAAAAAAAA',
      grant_hash: 'sha256:' + 'a'.repeat(64),
      key_epoch: 123,
    }

    await recordingsAPI.encryptArchiveReplica('recording/one', body)

    expect(requests).toHaveLength(1)
    expect(requests[0]?.url).toBe('/api/recordings/recording%2Fone/replicas/encrypt')
    expect(JSON.parse(String(requests[0]?.init?.body))).toEqual(body)
    expect(JSON.stringify(requests[0]?.init?.body)).not.toMatch(/data_key|key_envelope/)
  })

  it('submits explicit replica acceptance and import scope to the public API', async () => {
    const requests: Array<{ url: string; init?: RequestInit }> = []
    vi.stubGlobal('fetch', vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      requests.push({ url: String(input), init })
      return new Response(JSON.stringify({ snapshot_id: 'snap_' + 'a'.repeat(64), objects: 3, lease: { lease_id: 'rl_AAAAAAAAAAAAAAAAAAAAAA', status: 'active' } }), {
        status: 200, headers: { 'Content-Type': 'application/json' },
      })
    }))
    const body = {
      source_node_id: 'nd_BBBBBBBBBBBBBBBBBBBBBB',
      owner_subject_id: 'os_AAAAAAAAAAAAAAAAAAAAAA',
      grant_hash: 'sha256:' + 'a'.repeat(64),
      archive_id: 'arc_AAAAAAAAAAAAAAAAAAAAAA',
      snapshot_id: 'snap_' + 'a'.repeat(64),
      lease_id: 'rl_AAAAAAAAAAAAAAAAAAAAAA',
    }

    await replicaImportsAPI.importFromPeer(body)

    expect(requests).toHaveLength(1)
    expect(requests[0]?.url).toBe('/api/replica-imports')
    expect(requests[0]?.init?.method).toBe('POST')
    expect(JSON.parse(String(requests[0]?.init?.body))).toEqual(body)
  })

  it('queries only the selected source, OwnerSubject, and archive for pending replica offers', async () => {
    const requests: string[] = []
    vi.stubGlobal('fetch', vi.fn(async (input: RequestInfo | URL) => {
      requests.push(String(input))
      return new Response(JSON.stringify({
        source_node_id: 'nd_BBBBBBBBBBBBBBBBBBBBBB',
        archive_id: 'arc_AAAAAAAAAAAAAAAAAAAAAA',
        offers: [],
      }), { status: 200, headers: { 'Content-Type': 'application/json' } })
    }))

    await replicaImportsAPI.offers(
      'nd_BBBBBBBBBBBBBBBBBBBBBB',
      'os_AAAAAAAAAAAAAAAAAAAAAA',
      'arc_CCCCCCCCCCCCCCCCCCCCCC',
    )

    expect(requests).toEqual([
      '/api/peer-archives/nd_BBBBBBBBBBBBBBBBBBBBBB/replica-offers?owner_subject_id=os_AAAAAAAAAAAAAAAAAAAAAA&archive_id=arc_CCCCCCCCCCCCCCCCCCCCCC',
    ])
  })

  it('retries a temporary ciphertext range server failure without changing request scope', async () => {
    const requests: Array<{ url: string; init?: RequestInit }> = []
    vi.stubGlobal('fetch', vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      requests.push({ url: String(input), init })
      if (requests.length === 1) return new Response(null, { status: 503 })
      return new Response(new Uint8Array([11, 12, 13]), {
        status: 200, headers: { 'Content-Type': 'application/octet-stream' },
      })
    }))
    const controller = new AbortController()

    await expect(peerArchivesAPI.ciphertextRange(
      'nd_AAAAAAAAAAAAAAAAAAAAAA',
      'os_BBBBBBBBBBBBBBBBBBBBBB',
      'arc_CCCCCCCCCCCCCCCCCCCCCC',
      `sha256:${'d'.repeat(64)}`,
      `snap_${'e'.repeat(64)}`,
      '1710000000000',
      `ob_${'F'.repeat(43)}`,
      4096,
      3,
      controller.signal,
    )).resolves.toEqual(new Uint8Array([11, 12, 13]))

    expect(requests).toHaveLength(2)
    expect(requests[0]?.url).toBe(requests[1]?.url)
    expect(requests[0]?.url).toContain('offset=4096&length=3')
    expect(requests[0]?.init?.credentials).toBe('same-origin')
    expect(requests[0]?.init?.signal).toBe(controller.signal)
  })
})
