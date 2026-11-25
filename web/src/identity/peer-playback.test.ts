import { sha256 } from '@noble/hashes/sha2.js'
import { describe, expect, it, vi } from 'vitest'
import type { FragmentLoaderContext, HlsConfig, LoaderCallbacks, LoaderConfiguration } from 'hls.js'
import { encryptArchiveObject, type CryptoObjectDescriptor } from './cryptoarchive'
import { createPeerArchiveFragmentLoader, createPeerPlaybackPlan, decryptPeerPlaybackObject, fetchCiphertextRangeWithRetry } from './peer-playback'
import type { PeerArchiveHead, ReplicaManifest } from '@/types/api'

const archiveID = `arc_${'A'.repeat(22)}`
const snapshotID = `snap_${'b'.repeat(64)}`
const sessionID = `session-${'c'.repeat(64)}`
const initID = `ob_${'D'.repeat(43)}`
const media1ID = `ob_${'E'.repeat(43)}`
const media2ID = `ob_${'F'.repeat(43)}`
const digest = (bytes: Uint8Array) => `sha256:${Array.from(sha256(bytes), byte => byte.toString(16).padStart(2, '0')).join('')}`

function fixture(): { snapshot: PeerArchiveHead['snapshot']; manifest: ReplicaManifest } {
  const coordinate = (sequence: number, kind: 'media' | 'init' = 'media') => ({
    session_id: sessionID, track_id: 'main', source_epoch: 1, discontinuity_sequence: 0, sequence, kind,
  })
  const snapshot = {
    schema_version: 2, sequence: 3, snapshot_id: snapshotID, content_hash: `sha256:${'1'.repeat(64)}`,
    content: {
      archive_id: archiveID, session: { id: sessionID, adapter_id: 'fixture' },
      objects: [
        { timeline_ordinal: 0, duration: 0, init_identity: 'initref_shared', state: 'present' as const, plaintext_size: 4, plaintext_sha256: `sha256:${'2'.repeat(64)}`, coordinate: coordinate(0, 'init') },
        { timeline_ordinal: 1, duration: 2, init_identity: 'initref_shared', state: 'present' as const, plaintext_size: 4, plaintext_sha256: `sha256:${'3'.repeat(64)}`, coordinate: coordinate(1) },
        { timeline_ordinal: 2, duration: 3, init_identity: 'initref_shared', state: 'present' as const, plaintext_size: 4, plaintext_sha256: `sha256:${'4'.repeat(64)}`, coordinate: coordinate(2) },
      ], coverage: [],
    },
  } as unknown as PeerArchiveHead['snapshot']
  const manifest: ReplicaManifest = {
    replica_manifest_version: 'ir.replica-manifest/1', archive_id: archiveID, snapshot_id: snapshotID, key_epoch: '1710000000000',
    objects: [
      { object_id: initID, ciphertext_size: '90', ciphertext_sha256: `sha256:${'5'.repeat(64)}`, coordinate: coordinate(0, 'init') },
      { object_id: media1ID, ciphertext_size: '90', ciphertext_sha256: `sha256:${'6'.repeat(64)}`, coordinate: coordinate(1) },
      { object_id: media2ID, ciphertext_size: '90', ciphertext_sha256: `sha256:${'7'.repeat(64)}`, coordinate: coordinate(2) },
    ],
  }
  return { snapshot, manifest }
}

describe('encrypted peer HLS playback', () => {
  it('retries a transient range transport failure and returns the exact ciphertext bytes', async () => {
    const request = vi.fn()
      .mockRejectedValueOnce(new TypeError('temporary network failure'))
      .mockResolvedValueOnce(new Response(new Uint8Array([4, 5, 6]), {
        status: 200, headers: { 'content-type': 'application/octet-stream' },
      }))
    await expect(fetchCiphertextRangeWithRetry(request, 0, 3)).resolves.toEqual(new Uint8Array([4, 5, 6]))
    expect(request).toHaveBeenCalledTimes(2)
  })

  it('retries a transient server response but does not relax the final payload checks', async () => {
    const request = vi.fn()
      .mockResolvedValueOnce(new Response(null, { status: 503 }))
      .mockResolvedValueOnce(new Response(new Uint8Array([7, 8, 9]), {
        status: 200, headers: { 'content-type': 'application/octet-stream' },
      }))
    await expect(fetchCiphertextRangeWithRetry(request, 5, 3)).resolves.toEqual(new Uint8Array([7, 8, 9]))
    expect(request).toHaveBeenCalledTimes(2)
  })

  it('does not retry authorization, content-type, or range-length failures', async () => {
    const denied = vi.fn().mockResolvedValue(new Response(null, { status: 403 }))
    await expect(fetchCiphertextRangeWithRetry(denied, 0, 3)).rejects.toThrow('(403)')
    expect(denied).toHaveBeenCalledTimes(1)

    const unsupported = vi.fn().mockResolvedValue(new Response(null, { status: 501 }))
    await expect(fetchCiphertextRangeWithRetry(unsupported, 0, 3)).rejects.toThrow('(501)')
    expect(unsupported).toHaveBeenCalledTimes(1)

    const wrongType = vi.fn().mockResolvedValue(new Response(new Uint8Array([1, 2, 3]), {
      status: 200, headers: { 'content-type': 'text/html' },
    }))
    await expect(fetchCiphertextRangeWithRetry(wrongType, 0, 3)).rejects.toThrow('response type is invalid')
    expect(wrongType).toHaveBeenCalledTimes(1)

    const wrongLength = vi.fn().mockResolvedValue(new Response(new Uint8Array([1, 2]), {
      status: 200, headers: { 'content-type': 'application/octet-stream' },
    }))
    await expect(fetchCiphertextRangeWithRetry(wrongLength, 0, 3)).rejects.toThrow('range length is invalid')
    expect(wrongLength).toHaveBeenCalledTimes(1)
  })

  it('cancels a pending retry when playback is aborted', async () => {
    const controller = new AbortController()
    const request = vi.fn().mockRejectedValue(new TypeError('temporary network failure'))
    const pending = fetchCiphertextRangeWithRetry(request, 0, 3, controller.signal)
    controller.abort()
    await expect(pending).rejects.toMatchObject({ name: 'AbortError' })
    expect(request).toHaveBeenCalledTimes(1)
  })

  it('creates a local HLS playlist bound to the verified snapshot and maps init objects', () => {
    const { snapshot, manifest } = fixture()
    const plan = createPeerPlaybackPlan(snapshot, manifest)
    const playlist = plan.playlist('main')
    expect(plan.tracks).toEqual(['main'])
    expect(plan.objects.size).toBe(3)
    expect(playlist).toContain('#EXT-X-TARGETDURATION:3')
    expect(playlist).toContain(`#EXT-X-MAP:URI="https://peer-object.invalid/o/${initID}"`)
    expect(playlist).toContain(`https://peer-object.invalid/o/${media1ID}`)
    expect(playlist).toContain(`https://peer-object.invalid/o/${media2ID}`)
    expect(playlist).toContain('#EXT-X-ENDLIST')
  })

  it('fails closed when a media object references an unavailable init map', () => {
    const { snapshot, manifest } = fixture()
    manifest.objects = manifest.objects.filter(object => object.object_id !== initID)
    expect(() => createPeerPlaybackPlan(snapshot, manifest)).toThrow('manifest does not exactly match')
  })

  it('fetches bounded ciphertext ranges and verifies/decrypts the original object bytes', async () => {
    const plain = new TextEncoder().encode('canonical encrypted segment payload')
    const descriptor: CryptoObjectDescriptor = {
      archiveId: archiveID,
      coordinate: { sessionId: sessionID, trackId: 'main', sourceEpoch: '1', discontinuitySequence: '0', sequence: '1', kind: 'media' },
      objectType: 'media', plaintextSize: plain.length, plaintextSha256: digest(plain), keyEpoch: '1',
    }
    const dek = new Uint8Array(32).fill(77)
    const encrypted: Uint8Array[] = []
    const encryptedInfo = await encryptArchiveObject(dek, descriptor, new ReadableStream({ start(controller) { controller.enqueue(plain); controller.close() } }), async chunk => { encrypted.push(chunk.slice()) })
    const ciphertext = new Uint8Array(encrypted.reduce((size, part) => size + part.length, 0))
    let position = 0
    for (const part of encrypted) { ciphertext.set(part, position); position += part.length }
    const bound = { ...descriptor, ciphertextSize: encryptedInfo.ciphertextSize, ciphertextSha256: encryptedInfo.ciphertextSha256 }
    const calls: number[] = []
    const restored = await decryptPeerPlaybackObject({ objectID: media1ID, ciphertextSize: ciphertext.length, descriptor: bound }, dek,
      async (_objectID, offset, length) => { calls.push(length); return ciphertext.slice(offset, offset + length) }, new AbortController().signal)
    expect(Array.from(restored)).toEqual(Array.from(plain))
    expect(calls.reduce((sum, length) => sum + length, 0)).toBe(ciphertext.length)
    expect(calls.every(length => length <= 4 * 1024 * 1024)).toBe(true)

    const invalidCalls: number[] = []
    await expect(decryptPeerPlaybackObject({
      objectID: media1ID,
      ciphertextSize: ciphertext.length,
      descriptor: { ...bound, plaintextSize: 64 * 1024 * 1024 + 1 },
    }, dek, async (_objectID, offset, length) => {
      invalidCalls.push(offset)
      return ciphertext.slice(offset, offset + length)
    }, new AbortController().signal)).rejects.toThrow('too large for browser playback')
    expect(invalidCalls).toEqual([])

    await expect(decryptPeerPlaybackObject({
      objectID: media1ID,
      ciphertextSize: ciphertext.length,
      descriptor: { ...bound, plaintextSha256: digest(new TextEncoder().encode('wrong plaintext digest')) },
    }, dek, async (_objectID, offset, length) => ciphertext.slice(offset, offset + length), new AbortController().signal))
      .rejects.toThrow()

    await expect(decryptPeerPlaybackObject({
      objectID: media1ID,
      ciphertextSize: ciphertext.length,
      descriptor: { ...bound, ciphertextSha256: digest(new TextEncoder().encode('wrong ciphertext digest')) },
    }, dek, async (_objectID, offset, length) => ciphertext.slice(offset, offset + length), new AbortController().signal))
      .rejects.toThrow('archive ciphertext digest is invalid')
  })

  it('ignores an older fragment completion after HLS reuses the loader for a new request', async () => {
    let finishOld!: (bytes: Uint8Array) => void
    const fetchObject = vi.fn((objectID: string) => objectID === media1ID
      ? new Promise<Uint8Array>(resolve => { finishOld = resolve })
      : Promise.resolve(new Uint8Array([9, 8, 7])))
    const LoaderClass = createPeerArchiveFragmentLoader(fetchObject)
    const loader = new LoaderClass({} as HlsConfig)
    const onSuccess = vi.fn()
    const onError = vi.fn()
    const callbacks = { onSuccess, onError, onTimeout: vi.fn() } as unknown as LoaderCallbacks<FragmentLoaderContext>
    const config = { timeout: 1000, loadPolicy: { maxLoadTimeMs: 1000 } } as LoaderConfiguration
    const context = (objectID: string) => ({ url: `https://peer-object.invalid/o/${objectID}`, responseType: 'arraybuffer', type: 'media-fragment' }) as FragmentLoaderContext
    loader.load(context(media1ID), config, callbacks)
    loader.load(context(media2ID), config, callbacks)
    await new Promise(resolve => setTimeout(resolve, 0))
    expect(onSuccess).toHaveBeenCalledTimes(1)
    finishOld(new Uint8Array([1, 2, 3]))
    await new Promise(resolve => setTimeout(resolve, 0))
    expect(onSuccess).toHaveBeenCalledTimes(1)
    expect(onSuccess.mock.calls[0]?.[0].url).toContain(media2ID)
    expect(onError).not.toHaveBeenCalled()
    loader.destroy()
  })
})
