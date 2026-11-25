export const PEER_CIPHERTEXT_RANGE_BYTES = 4 * 1024 * 1024

const RETRY_DELAYS_MS = [250, 500, 1000] as const

/** Fetch one bounded ciphertext range, retrying transport failures only. */
export async function fetchCiphertextRangeWithRetry(
  request: (signal?: AbortSignal) => Promise<Response>,
  offset: number,
  length: number,
  signal?: AbortSignal,
): Promise<Uint8Array> {
  if (!Number.isSafeInteger(offset) || offset < 0 || !Number.isSafeInteger(length)
    || length < 1 || length > PEER_CIPHERTEXT_RANGE_BYTES) {
    throw new TypeError('peer ciphertext range is invalid')
  }
  for (let attempt = 0; ; attempt += 1) {
    if (signal?.aborted) throw new DOMException('Aborted', 'AbortError')
    let response: Response
    try {
      response = await request(signal)
    } catch (error) {
      if (signal?.aborted) throw new DOMException('Aborted', 'AbortError')
      if (error instanceof DOMException && error.name === 'AbortError') throw error
      if (attempt >= RETRY_DELAYS_MS.length) throw new Error('Peer archive ciphertext is unavailable.')
      await waitForRetry(RETRY_DELAYS_MS[attempt] ?? 1000, signal)
      continue
    }
    if (!response.ok) {
      const retryable = response.status === 408 || response.status === 425 || response.status === 429
        || response.status === 500 || response.status === 502 || response.status === 503 || response.status === 504
      void response.body?.cancel().catch(() => undefined)
      if (!retryable) throw new Error(`Peer archive ciphertext request failed (${response.status}).`)
      if (attempt >= RETRY_DELAYS_MS.length) throw new Error(`Peer archive ciphertext request failed (${response.status}).`)
      await waitForRetry(RETRY_DELAYS_MS[attempt] ?? 1000, signal)
      continue
    }
    if (response.headers.get('content-type') !== 'application/octet-stream') {
      void response.body?.cancel().catch(() => undefined)
      throw new Error('Peer archive ciphertext response type is invalid.')
    }
    let bytes: Uint8Array
    try {
      bytes = new Uint8Array(await response.arrayBuffer())
    } catch {
      if (signal?.aborted) throw new DOMException('Aborted', 'AbortError')
      if (attempt >= RETRY_DELAYS_MS.length) throw new Error('Peer archive ciphertext is unavailable.')
      await waitForRetry(RETRY_DELAYS_MS[attempt] ?? 1000, signal)
      continue
    }
    if (bytes.byteLength !== length) {
      bytes.fill(0)
      throw new Error('Peer archive ciphertext range length is invalid.')
    }
    return bytes
  }
}

function waitForRetry(delayMs: number, signal?: AbortSignal): Promise<void> {
  if (signal?.aborted) return Promise.reject(new DOMException('Aborted', 'AbortError'))
  return new Promise((resolve, reject) => {
    const finish = (error?: DOMException) => {
      clearTimeout(timer)
      signal?.removeEventListener('abort', onAbort)
      if (error) reject(error)
      else resolve()
    }
    const onAbort = () => finish(new DOMException('Aborted', 'AbortError'))
    const timer = setTimeout(() => finish(), delayMs)
    signal?.addEventListener('abort', onAbort, { once: true })
  })
}
