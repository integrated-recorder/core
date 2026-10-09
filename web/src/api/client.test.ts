import { afterEach, describe, expect, it, vi } from 'vitest'
import { api, setCSRFToken } from './client'
import { recordingsAPI, userPreferencesAPI } from './index'

afterEach(() => { vi.unstubAllGlobals(); setCSRFToken(undefined) })

describe('typed API client', () => {
  it('attaches the current CSRF token and same-origin credentials to mutations', async () => {
    const fetchMock = vi.fn().mockResolvedValue(new Response(JSON.stringify({ ok: true }), { status: 200, headers: { 'content-type': 'application/json' } }))
    vi.stubGlobal('fetch', fetchMock)
    setCSRFToken('csrf-test')
    await api('/api/example', { method: 'POST', body: { value: 1 } })
    const [, init] = fetchMock.mock.calls[0] as [string, RequestInit]
    expect(init.credentials).toBe('same-origin')
    expect(new Headers(init.headers).get('X-CSRF-Token')).toBe('csrf-test')
    expect(init.body).toBe('{"value":1}')
  })

  it('keeps recording search filters and cursors server-side in the v2 request', async () => {
    const fetchMock = vi.fn().mockResolvedValue(new Response(JSON.stringify({ items: [], total: 0 }), { status: 200, headers: { 'content-type': 'application/json' } }))
    vi.stubGlobal('fetch', fetchMock)
    await recordingsAPI.list({ q: 'live show', state: 'completed', tag: 'important', sort: '-started_at', limit: 25, cursor: 'next-page' })
    expect(fetchMock.mock.calls[0]?.[0]).toContain('/api/v2/recordings?q=live+show&state=completed&tag=important&sort=-started_at&limit=25&cursor=next-page')
  })

  it('requests archive index pages with bounded limit and cursor', async () => {
    const fetchMock = vi.fn().mockResolvedValue(new Response(JSON.stringify({ recording_id: 'rec-1', entries: [], next_cursor: 'cursor-2', has_more: true }), { status: 200, headers: { 'content-type': 'application/json' } }))
    vi.stubGlobal('fetch', fetchMock)
    const page = await recordingsAPI.archive('rec-1', { limit: 20, cursor: 'cursor-1' })
    expect(page.has_more).toBe(true)
    expect(fetchMock.mock.calls[0]?.[0]).toBe('/api/recordings/rec-1/archive/index?limit=20&cursor=cursor-1')
  })

  it('uses current-user preference endpoints without accepting a user ID', async () => {
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(new Response(JSON.stringify({ locale: 'ko-KR', theme: 'dark', timezone: 'Asia/Seoul' }), { status: 200, headers: { 'content-type': 'application/json' } }))
      .mockResolvedValueOnce(new Response(JSON.stringify({ locale: 'en-US', theme: 'light', timezone: 'America/New_York' }), { status: 200, headers: { 'content-type': 'application/json' } }))
    vi.stubGlobal('fetch', fetchMock)
    await userPreferencesAPI.get()
    await userPreferencesAPI.update({ locale: 'en-US', theme: 'light', timezone: 'America/New_York' })
    expect(fetchMock.mock.calls[0]?.[0]).toBe('/api/user/preferences')
    expect(fetchMock.mock.calls[1]?.[0]).toBe('/api/user/preferences')
    expect(JSON.parse(String(fetchMock.mock.calls[1]?.[1]?.body))).toEqual({ locale: 'en-US', theme: 'light', timezone: 'America/New_York' })
  })

  it('sends preview policy only as management-level recording creation input and uses the frame-index API', async () => {
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(new Response(JSON.stringify({ id: 'rec-1' }), { status: 200, headers: { 'content-type': 'application/json' } }))
      .mockResolvedValueOnce(new Response(JSON.stringify({ recording_id: 'rec-1', mode: 'segment', state: 'ready', available: true, frame_count: 1, items: [] }), { status: 200, headers: { 'content-type': 'application/json' } }))
      .mockResolvedValueOnce(new Response(JSON.stringify({ mode: 'segment', state: 'queued', available: true, frame_count: 0 }), { status: 202, headers: { 'content-type': 'application/json' } }))
    vi.stubGlobal('fetch', fetchMock)
    await recordingsAPI.start({ adapter_id: 'owncast', input: {}, preview_mode: 'disabled' })
    await recordingsAPI.previews('rec-1', { sampling: 'uniform', limit: 48 })
    await recordingsAPI.enablePreviews('rec-1')
    const startRequest = fetchMock.mock.calls[0] as [string, RequestInit]
    expect(JSON.parse(String(startRequest[1].body))).toMatchObject({ preview_mode: 'disabled' })
    expect(fetchMock.mock.calls[1]?.[0]).toContain('/api/recordings/rec-1/previews?sampling=uniform&limit=48')
    expect(fetchMock.mock.calls[2]?.[0]).toBe('/api/recordings/rec-1/previews')
    expect(fetchMock.mock.calls[2]?.[1]?.body).toBe('{"mode":"segment"}')
  })

  it('returns structured API errors with request identifiers', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(JSON.stringify({ error_code: 'archive_conflict', error: 'conflict' }), { status: 409, headers: { 'content-type': 'application/json', 'X-Request-ID': 'req-1' } })))
    await expect(api('/api/example')).rejects.toMatchObject({ status: 409, message: 'conflict', errorCode: 'archive_conflict', requestID: 'req-1' })
  })

  it('uses a CSRF token returned by bootstrap or login for the next mutation', async () => {
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(new Response(JSON.stringify({ authenticated: true, csrf_token: 'fresh-token' }), { status: 200, headers: { 'content-type': 'application/json' } }))
      .mockResolvedValueOnce(new Response(JSON.stringify({ ok: true }), { status: 200, headers: { 'content-type': 'application/json' } }))
    vi.stubGlobal('fetch', fetchMock)
    await api('/api/auth/login', { method: 'POST', body: { password: 'never persisted' } })
    await api('/api/example', { method: 'PUT', body: { value: 1 } })
    const [, mutation] = fetchMock.mock.calls[1] as [string, RequestInit]
    expect(new Headers(mutation.headers).get('X-CSRF-Token')).toBe('fresh-token')
  })

  it('deduplicates concurrent protected 401 handling, clears stale CSRF, and leaves credential errors on the login screen', async () => {
    setCSRFToken('stale-token')
    const onUnauthorized = vi.fn()
    window.addEventListener('ir:unauthorized', onUnauthorized)
    const unauthorized = new Response(JSON.stringify({ error: 'expired' }), { status: 401, headers: { 'content-type': 'application/json' } })
    const fetchMock = vi.fn().mockResolvedValue(unauthorized)
    vi.stubGlobal('fetch', fetchMock)
    await Promise.allSettled([api('/api/recordings'), api('/api/settings')])
    expect(onUnauthorized).toHaveBeenCalledOnce()

    fetchMock.mockResolvedValueOnce(new Response(JSON.stringify({ ok: true }), { status: 200, headers: { 'content-type': 'application/json' } }))
    await api('/api/example', { method: 'POST', body: {} })
    const [, request] = fetchMock.mock.calls.at(-1) as [string, RequestInit]
    expect(new Headers(request.headers).has('X-CSRF-Token')).toBe(false)

    onUnauthorized.mockClear()
    await Promise.allSettled([api('/api/auth/login', { method: 'POST', body: { password: 'wrong' } }), api('/api/auth/bootstrap', { method: 'POST', body: { token: 'wrong', password: 'secret' } })])
    expect(onUnauthorized).not.toHaveBeenCalled()
    window.removeEventListener('ir:unauthorized', onUnauthorized)
  })
})
