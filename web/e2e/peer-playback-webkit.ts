import Hls from 'hls.js'
import { sha256 } from '@noble/hashes/sha2.js'
import { encryptArchiveObject, type CryptoObjectDescriptor } from '../src/identity/cryptoarchive'
import { createPeerArchiveFragmentLoader, decryptPeerPlaybackObject, type PeerPlaybackObject } from '../src/identity/peer-playback'

const result = document.querySelector<HTMLOutputElement>('#result')
const playButton = document.querySelector<HTMLButtonElement>('#play')
const video = document.querySelector<HTMLVideoElement>('#video')
const fixturePrefix = '/e2e/fixtures/peer-playback/'
const archiveId = `arc_${'A'.repeat(22)}`
const sessionId = `session-${'b'.repeat(64)}`

function hex(bytes: Uint8Array): string {
  return Array.from(bytes, byte => byte.toString(16).padStart(2, '0')).join('')
}

async function readFixture(name: string): Promise<Uint8Array> {
  const response = await fetch(`${fixturePrefix}${name}`, { cache: 'no-store' })
  if (!response.ok) throw new Error(`encrypted media fixture ${name} could not be loaded`)
  return new Uint8Array(await response.arrayBuffer())
}

async function encryptFixture(objectID: string, kind: 'init' | 'media', sequence: number, name: string): Promise<{
  object: PeerPlaybackObject
  ciphertext: Uint8Array
}> {
  const plaintext = await readFixture(name)
  const descriptor: CryptoObjectDescriptor = {
    archiveId,
    coordinate: {
      sessionId,
      trackId: 'video',
      sourceEpoch: '1',
      discontinuitySequence: '0',
      sequence: String(sequence),
      kind,
    },
    objectType: kind,
    plaintextSize: plaintext.byteLength,
    plaintextSha256: `sha256:${hex(sha256(plaintext))}`,
    keyEpoch: '1',
  }
  const dek = new Uint8Array(32).fill(0x31)
  const chunks: Uint8Array[] = []
  const encrypted = await encryptArchiveObject(
    dek,
    descriptor,
    new ReadableStream<Uint8Array>({ start(controller) { controller.enqueue(plaintext); controller.close() } }),
    async chunk => { chunks.push(chunk.slice()) },
  )
  const ciphertext = new Uint8Array(encrypted.ciphertextSize)
  let offset = 0
  for (const chunk of chunks) {
    ciphertext.set(chunk, offset)
    offset += chunk.byteLength
    chunk.fill(0)
  }
  const object: PeerPlaybackObject = {
    objectID,
    ciphertextSize: encrypted.ciphertextSize,
    descriptor: {
      ...descriptor,
      ciphertextSize: encrypted.ciphertextSize,
      ciphertextSha256: encrypted.ciphertextSha256,
    },
  }
  plaintext.fill(0)
  dek.fill(0)
  return { object, ciphertext }
}

function waitForVideoEvent(target: HTMLVideoElement, eventName: string, timeout = 10_000): Promise<void> {
  return new Promise((resolve, reject) => {
    const timer = window.setTimeout(() => {
      target.removeEventListener(eventName, onEvent)
      reject(new Error(`WebKit did not emit ${eventName}`))
    }, timeout)
    const onEvent = () => {
      window.clearTimeout(timer)
      target.removeEventListener(eventName, onEvent)
      resolve()
    }
    target.addEventListener(eventName, onEvent, { once: true })
  })
}

async function run(): Promise<void> {
  if (!globalThis.crypto?.subtle || !window.isSecureContext) throw new Error('WebKit secure browser cryptography is unavailable')
  if (!Hls.isSupported()) throw new Error('WebKit does not report HLS/MSE support')
  if (!result || !playButton || !video) throw new Error('WebKit playback test elements are missing')

  const dek = new Uint8Array(32).fill(0x31)
  const fixtureSpecs = [
    { id: `ob_${'A'.repeat(43)}`, kind: 'init' as const, sequence: 0, name: 'init.mp4' },
    { id: `ob_${'B'.repeat(43)}`, kind: 'media' as const, sequence: 1, name: 'segment00.m4s' },
    { id: `ob_${'C'.repeat(43)}`, kind: 'media' as const, sequence: 2, name: 'segment01.m4s' },
    { id: `ob_${'D'.repeat(43)}`, kind: 'media' as const, sequence: 3, name: 'segment02.m4s' },
  ]
  const encrypted = await Promise.all(fixtureSpecs.map(item => encryptFixture(item.id, item.kind, item.sequence, item.name)))
  const byID = new Map(encrypted.map(item => [item.object.objectID, item]))
  const loadedIDs = new Set<string>()
  const fetchObject = (id: string, signal: AbortSignal) => {
    const stored = byID.get(id)
    if (!stored) throw new Error('unexpected encrypted peer object request')
    loadedIDs.add(id)
    return decryptPeerPlaybackObject(stored.object, dek, async (requestedID, offset, length, rangeSignal) => {
      if (requestedID !== id || signal.aborted || rangeSignal.aborted) throw new DOMException('Aborted', 'AbortError')
      return stored.ciphertext.slice(offset, offset + length)
    }, signal)
  }
  const FragmentLoader = createPeerArchiveFragmentLoader(fetchObject)
  const playlist = [
    '#EXTM3U',
    '#EXT-X-VERSION:7',
    '#EXT-X-TARGETDURATION:1',
    '#EXT-X-MEDIA-SEQUENCE:1',
    '#EXT-X-PLAYLIST-TYPE:VOD',
    `#EXT-X-MAP:URI="https://peer-object.invalid/o/${fixtureSpecs[0].id}"`,
    ...fixtureSpecs.slice(1).flatMap(item => ['#EXTINF:1.000000,', `https://peer-object.invalid/o/${item.id}`]),
    '#EXT-X-ENDLIST',
    '',
  ].join('\n')
  const playlistURL = URL.createObjectURL(new Blob([playlist], { type: 'application/vnd.apple.mpegurl' }))
  const hls = new Hls({ enableWorker: false, lowLatencyMode: false, maxBufferLength: 8, fLoader: FragmentLoader })
  let failed = false
  const errors: string[] = []
  hls.on(Hls.Events.ERROR, (_event, data) => {
    if (data.fatal) {
      failed = true
      errors.push(data.details)
    }
  })
  const loadedMetadata = waitForVideoEvent(video, 'loadedmetadata')
  hls.attachMedia(video)
  hls.loadSource(playlistURL)
  await loadedMetadata
  if (failed || video.duration < 2.5) throw new Error(`WebKit HLS/MSE setup failed: ${errors.join(', ') || 'invalid VOD duration'}`)
  result.value = JSON.stringify({ stage: 'ready', secure_context: true, webcrypto: true, hls_mse: true, duration: video.duration })
  playButton.disabled = false

  playButton.addEventListener('click', () => {
    void (async () => {
      const playing = waitForVideoEvent(video, 'playing')
      await video.play()
      await playing
      const playbackStarted = await new Promise<boolean>(resolve => {
        const timeout = window.setTimeout(() => resolve(false), 4_000)
        const check = () => {
          if (video.currentTime >= 0.2) {
            window.clearTimeout(timeout)
            video.removeEventListener('timeupdate', check)
            resolve(true)
          }
        }
        video.addEventListener('timeupdate', check)
      })
      if (!playbackStarted) throw new Error('WebKit playback time did not advance')
      video.pause()
      const seeked = waitForVideoEvent(video, 'seeked')
      video.currentTime = Math.min(2.2, video.duration - 0.2)
      await seeked
      if (failed || Math.abs(video.currentTime - Math.min(2.2, video.duration - 0.2)) > 0.15) {
        throw new Error(`WebKit seek failed: ${errors.join(', ') || video.currentTime}`)
      }
      if (loadedIDs.size !== fixtureSpecs.length) throw new Error('WebKit did not fetch every encrypted init/media object')
      result.value = JSON.stringify({
        stage: 'complete', secure_context: true, webcrypto: true, hls_mse: true,
        fragment_decrypted: true, mse_playback: true, seek_success: true,
        duration: video.duration, seek_seconds: video.currentTime, encrypted_objects: loadedIDs.size,
      })
    })().catch(error => {
      result.value = JSON.stringify({ error: error instanceof Error ? error.message : 'unknown playback failure' })
    })
  }, { once: true })

  window.addEventListener('pagehide', () => {
    hls.destroy()
    URL.revokeObjectURL(playlistURL)
    dek.fill(0)
    for (const item of encrypted) item.ciphertext.fill(0)
  }, { once: true })
}

void run().catch(error => {
  if (result) result.value = JSON.stringify({ error: error instanceof Error ? error.message : 'unknown failure' })
})
