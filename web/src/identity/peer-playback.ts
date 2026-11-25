import { LoadStats, type FragmentLoaderContext, type FragmentLoaderConstructor, type Loader, type LoaderCallbacks, type LoaderConfiguration } from 'hls.js'
import type { PeerArchiveHead, PeerReplicaManifest } from '@/types/api'
import { decryptArchiveObject, type CryptoObjectDescriptor } from './cryptoarchive'
import { PEER_CIPHERTEXT_RANGE_BYTES as RANGE_BYTES } from './ciphertext-range'
export { fetchCiphertextRangeWithRetry } from './ciphertext-range'

const PEER_OBJECT_URL = 'https://peer-object.invalid/o/'
const MAX_BROWSER_OBJECT_BYTES = 64 * 1024 * 1024

export type PeerPlaybackObject = {
  objectID: string
  ciphertextSize: number
  descriptor: CryptoObjectDescriptor
}

export type PeerPlaybackPlan = {
  tracks: string[]
  objects: Map<string, PeerPlaybackObject>
  playlist(trackID: string): string
}

function coordinateKey(coordinate: {
  session_id: string
  track_id: string
  source_epoch: number
  discontinuity_sequence: number
  sequence: number
  kind?: string
}): string {
  const kind = coordinate.kind || 'media'
  return [coordinate.session_id, coordinate.track_id, coordinate.source_epoch, coordinate.discontinuity_sequence, coordinate.sequence, kind].join('\u0000')
}

function canonicalObjectID(value: string): boolean {
  return /^ob_[A-Za-z0-9_-]{43}$/u.test(value)
}

function safeObjectSize(size: string): number {
  if (!/^[1-9][0-9]{0,15}$/u.test(size)) throw new TypeError('peer archive object size is invalid')
  const parsed = Number(size)
  if (!Number.isSafeInteger(parsed) || parsed > MAX_BROWSER_OBJECT_BYTES + 2 * 1024 * 1024) {
    throw new TypeError('peer archive object is too large for browser playback')
  }
  return parsed
}

/**
 * Builds a local HLS playlist from the independently verified snapshot and
 * its coordinate-bound encrypted manifest. URIs use a reserved invalid host;
 * the custom HLS fragment loader resolves them only through the authenticated
 * same-origin peer API and never performs a direct network request to it.
 */
export function createPeerPlaybackPlan(snapshot: PeerArchiveHead['snapshot'], manifest: PeerReplicaManifest): PeerPlaybackPlan {
  if (snapshot.schema_version !== 2 || !/^snap_[0-9a-f]{64}$/u.test(snapshot.snapshot_id)
    || manifest.replica_manifest_version !== 'ir.replica-manifest/1'
    || manifest.snapshot_id !== snapshot.snapshot_id || manifest.archive_id !== snapshot.content.archive_id
    || !/^[1-9][0-9]{0,19}$/u.test(manifest.key_epoch)) {
    throw new TypeError('remote archive snapshot and encrypted manifest do not match')
  }
  const manifestObjects = new Map<string, PeerPlaybackObject>()
  for (const item of manifest.objects) {
    if (!canonicalObjectID(item.object_id) || !/^sha256:[0-9a-f]{64}$/u.test(item.ciphertext_sha256)) {
      throw new TypeError('remote archive manifest contains an invalid object')
    }
    const kind = item.coordinate.kind || 'media'
    if (kind !== 'media' && kind !== 'init') throw new TypeError('remote archive object kind is invalid')
    const key = coordinateKey({ ...item.coordinate, kind })
    if (manifestObjects.has(key)) throw new TypeError('remote archive manifest repeats a coordinate')
    manifestObjects.set(key, {
      objectID: item.object_id,
      ciphertextSize: safeObjectSize(item.ciphertext_size),
      descriptor: { archiveId: manifest.archive_id, coordinate: {
        sessionId: item.coordinate.session_id,
        trackId: item.coordinate.track_id,
        sourceEpoch: String(item.coordinate.source_epoch),
        discontinuitySequence: String(item.coordinate.discontinuity_sequence),
        sequence: String(item.coordinate.sequence),
        kind,
      }, objectType: kind, plaintextSize: 0, plaintextSha256: '', keyEpoch: manifest.key_epoch,
        ciphertextSize: safeObjectSize(item.ciphertext_size), ciphertextSha256: item.ciphertext_sha256 },
    })
  }

  const snapshotObjects = snapshot.content.objects
  const initIDs = new Map<string, string>()
  const objects = new Map<string, PeerPlaybackObject>()
  for (const object of snapshotObjects) {
    const key = coordinateKey(object.coordinate)
    const encrypted = manifestObjects.get(key)
    if (!encrypted) continue
    if (object.state !== 'present' || !/^sha256:[0-9a-f]{64}$/u.test(object.plaintext_sha256)
      || !Number.isSafeInteger(object.plaintext_size) || object.plaintext_size < 1
      || object.plaintext_size > MAX_BROWSER_OBJECT_BYTES) {
      throw new TypeError('remote archive snapshot object is not playable')
    }
    encrypted.descriptor.plaintextSize = object.plaintext_size
    encrypted.descriptor.plaintextSha256 = object.plaintext_sha256
    if (object.coordinate.kind === 'init') {
      if (!object.init_identity) throw new TypeError('remote archive init object has no stable identity')
      const previous = initIDs.get(object.init_identity)
      if (previous && previous !== encrypted.objectID) throw new TypeError('remote archive init identity is ambiguous')
      initIDs.set(object.init_identity, encrypted.objectID)
    }
    objects.set(encrypted.objectID, encrypted)
  }
  const presentObjects = snapshotObjects.filter(object => object.state === 'present')
  if (presentObjects.some(object => !manifestObjects.has(coordinateKey(object.coordinate)))
    || manifestObjects.size !== objects.size) {
    throw new TypeError('remote replica manifest does not exactly match present snapshot objects')
  }

  const tracks = [...new Set(snapshotObjects.filter(object => object.coordinate.kind !== 'init' && object.state === 'present')
    .map(object => object.coordinate.track_id))].sort()
  const mediaByTrack = new Map<string, typeof snapshotObjects>()
  for (const track of tracks) {
    const media = snapshotObjects.filter(object => object.coordinate.track_id === track && object.coordinate.kind !== 'init'
      && object.state === 'present' && object.timeline_ordinal > 0)
      .sort((left, right) => left.timeline_ordinal - right.timeline_ordinal)
    if (media.some((object, index) => object.timeline_ordinal !== index + 1)) {
      throw new TypeError('remote archive timeline is not contiguous')
    }
    mediaByTrack.set(track, media)
  }

  return {
    tracks,
    objects,
    playlist(trackID: string): string {
      const media = mediaByTrack.get(trackID)
      if (!media?.length) throw new TypeError('remote archive track has no playable media')
      const entries: string[] = []
      let previous: (typeof media)[number] | undefined
      let previousInit = ''
      let targetDuration = 1
      for (const item of media) {
        const key = coordinateKey(item.coordinate)
        const encrypted = manifestObjects.get(key)
        if (!encrypted || !objects.has(encrypted.objectID)) throw new TypeError('remote archive media object is unavailable')
        if (previous && (item.coordinate.source_epoch !== previous.coordinate.source_epoch
          || item.coordinate.discontinuity_sequence !== previous.coordinate.discontinuity_sequence
          || item.coordinate.sequence !== previous.coordinate.sequence + 1
          || item.discontinuity)) entries.push('#EXT-X-DISCONTINUITY')
        if (item.init_identity && item.init_identity !== previousInit) {
          const initObjectID = initIDs.get(item.init_identity)
          if (!initObjectID || !objects.has(initObjectID)) throw new TypeError('remote archive init object is unavailable')
          entries.push(`#EXT-X-MAP:URI="${PEER_OBJECT_URL}${encodeURIComponent(initObjectID)}"`)
          previousInit = item.init_identity
        }
        if (item.program_date_time) {
          const date = new Date(item.program_date_time)
          if (!Number.isFinite(date.getTime())) throw new TypeError('remote archive program date is invalid')
          entries.push(`#EXT-X-PROGRAM-DATE-TIME:${date.toISOString()}`)
        }
        entries.push(`#EXTINF:${item.duration},`, `${PEER_OBJECT_URL}${encodeURIComponent(encrypted.objectID)}`)
        targetDuration = Math.max(targetDuration, Math.ceil(item.duration))
        previous = item
      }
      return ['#EXTM3U', '#EXT-X-VERSION:7', `#EXT-X-TARGETDURATION:${targetDuration}`,
        `#EXT-X-MEDIA-SEQUENCE:${media[0].coordinate.sequence}`, '#EXT-X-PLAYLIST-TYPE:VOD', ...entries, '#EXT-X-ENDLIST', ''].join('\n')
    },
  }
}

export type CiphertextRangeFetcher = (objectID: string, offset: number, length: number, signal: AbortSignal) => Promise<Uint8Array>

export async function decryptPeerPlaybackObject(
  object: PeerPlaybackObject,
  dek: Uint8Array,
  fetchRange: CiphertextRangeFetcher,
  signal: AbortSignal,
): Promise<Uint8Array> {
  if (!Number.isSafeInteger(object.ciphertextSize) || object.ciphertextSize < 1
    || object.ciphertextSize > MAX_BROWSER_OBJECT_BYTES + 2 * 1024 * 1024
    || !Number.isSafeInteger(object.descriptor.plaintextSize) || object.descriptor.plaintextSize < 1
    || object.descriptor.plaintextSize > MAX_BROWSER_OBJECT_BYTES) {
    throw new TypeError('peer archive object is too large for browser playback')
  }
  let offset = 0
  const source = new ReadableStream<Uint8Array>({
    pull: async controller => {
      if (signal.aborted) { controller.error(new DOMException('Aborted', 'AbortError')); return }
      if (offset >= object.ciphertextSize) { controller.close(); return }
      const length = Math.min(RANGE_BYTES, object.ciphertextSize - offset)
      try {
        const chunk = await fetchRange(object.objectID, offset, length, signal)
        if (chunk.byteLength !== length) throw new TypeError('peer ciphertext range length is invalid')
        offset += length
        controller.enqueue(chunk)
      } catch (error) { controller.error(error) }
    },
    cancel: () => undefined,
  })
  // Stage into one bounded buffer instead of retaining every decrypted chunk
  // and then copying them into a second full-object buffer. Do not return the
  // buffer until decryptArchiveObject has verified the complete plaintext and
  // ciphertext digests; clear it on every failure path.
  const result = new Uint8Array(object.descriptor.plaintextSize)
  let cursor = 0
  try {
    await decryptArchiveObject(dek, object.descriptor, source, async chunk => {
      try {
        if (chunk.byteLength > result.byteLength - cursor) throw new TypeError('peer archive plaintext exceeds its declared size')
        result.set(chunk, cursor)
        cursor += chunk.byteLength
      } finally {
        chunk.fill(0)
      }
    })
    if (cursor !== result.byteLength) throw new TypeError('peer archive plaintext size is invalid')
    return result
  } catch (error) {
    result.fill(0)
    throw error
  }
}

export function createPeerArchiveFragmentLoader(fetchObject: (objectID: string, signal: AbortSignal) => Promise<Uint8Array>): FragmentLoaderConstructor {
  return class PeerArchiveFragmentLoader implements Loader<FragmentLoaderContext> {
    context: FragmentLoaderContext | null = null
    stats = new LoadStats()
    private callbacks: LoaderCallbacks<FragmentLoaderContext> | null = null
    private controller: AbortController | null = null
    private timer: ReturnType<typeof setTimeout> | null = null
    private finished = false
    private generation = 0

    constructor() {}

    load(context: FragmentLoaderContext, config: LoaderConfiguration, callbacks: LoaderCallbacks<FragmentLoaderContext>): void {
      this.abortWork()
      const generation = ++this.generation
      this.context = context
      this.callbacks = callbacks
      const controller = new AbortController()
      this.controller = controller
      const requestContext = context
      const requestCallbacks = callbacks
      this.stats = new LoadStats()
      this.stats.loading.start = performance.now()
      this.finished = false
      const timeout = config.loadPolicy.maxLoadTimeMs || config.timeout || 30_000
      this.timer = setTimeout(() => {
        if (generation !== this.generation || this.finished) return
        this.finished = true
        controller.abort()
        this.stats.aborted = true
        this.stats.loading.end = performance.now()
        requestCallbacks.onTimeout(this.stats, requestContext, null)
      }, timeout)
      let objectID = ''
      try {
        const match = new URL(context.url)
        objectID = match.hostname === 'peer-object.invalid' && /^\/o\/ob_[A-Za-z0-9_-]{43}$/u.test(match.pathname) && !match.search && !match.hash
          ? match.pathname.slice('/o/'.length) : ''
      } catch { /* invalid loader URL is reported through the normal error callback */ }
      if (!objectID) {
        queueMicrotask(() => this.fail('Invalid peer object reference.', generation))
        return
      }
      void fetchObject(decodeURIComponent(objectID), controller.signal).then(data => {
        if (generation !== this.generation || this.finished || controller.signal.aborted) { data.fill(0); return }
        const exact = data.slice().buffer as ArrayBuffer
        this.stats.loaded = data.byteLength
        this.stats.total = data.byteLength
        this.stats.chunkCount = 1
        this.stats.loading.first = performance.now()
        this.stats.loading.end = performance.now()
        data.fill(0)
        this.finished = true
        this.clearTimer()
        requestCallbacks.onSuccess({ url: context.url, data: exact, code: 200 }, this.stats, requestContext, null)
      }).catch(() => this.fail('Encrypted peer object could not be verified or loaded.', generation))
    }

    abort(): void {
      if (!this.finished && this.context && this.callbacks) {
        this.finished = true
        this.generation++
        this.abortWork()
        this.stats.aborted = true
        this.callbacks.onAbort?.(this.stats, this.context, null)
      } else this.abortWork()
    }

    destroy(): void { this.abortWork(); this.callbacks = null; this.context = null }

    private fail(message: string, generation: number): void {
      if (generation !== this.generation || this.finished || !this.context || !this.callbacks) return
      this.finished = true
      this.controller?.abort()
      this.clearTimer()
      this.stats.loading.end = performance.now()
      this.callbacks.onError({ code: 502, text: message }, this.context, null, this.stats)
    }

    private abortWork(): void {
      this.controller?.abort()
      this.controller = null
      this.clearTimer()
    }

    private clearTimer(): void {
      if (this.timer !== null) clearTimeout(this.timer)
      this.timer = null
    }
  }
}
