import { act, render, waitFor } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { RecordingPlayer } from './player'

const hlsState = vi.hoisted(() => ({ destroy: vi.fn(), loadSource: vi.fn(), attachMedia: vi.fn(), on: vi.fn() }))
vi.mock('hls.js', () => ({ default: class HlsMock {
  static Events = { ERROR: 'error' }
  static isSupported() { return true }
  constructor() {}
  loadSource(source: string) { hlsState.loadSource(source) }
  attachMedia(video: HTMLVideoElement) { hlsState.attachMedia(video) }
  on(event: string, callback: (...args: unknown[]) => void) { hlsState.on(event, callback) }
  destroy() { hlsState.destroy() }
} }))

afterEach(() => { vi.restoreAllMocks(); vi.clearAllMocks() })

function playlist(...segments: Array<[string, number]>): string {
  return `#EXTM3U\n#EXT-X-VERSION:7\n${segments.map(([uri, duration]) => `#EXTINF:${duration},\n${uri}`).join('\n')}\n#EXT-X-ENDLIST\n`
}

function mockRevisionPlaylists(playlists: Record<string, string>) {
  const textRead = vi.fn()
  const fetchMock = vi.spyOn(globalThis, 'fetch').mockImplementation(async input => {
    const url = new URL(String(input), 'http://localhost')
    const revision = url.searchParams.get('timeline_revision') ?? 'initial'
    const body = playlists[revision]
    return { ok: body !== undefined, text: async () => { textRead(); return body ?? '' } } as Response
  })
  return { fetchMock, textRead }
}

async function flushPlaylistReads() {
  await act(async () => {
    await Promise.resolve()
    await Promise.resolve()
    await Promise.resolve()
  })
}

function setVideoState(video: HTMLVideoElement, currentTime: number, playing: boolean) {
  let position = currentTime
  Object.defineProperty(video, 'currentTime', { configurable: true, get: () => position, set: value => { position = value } })
  Object.defineProperty(video, 'duration', { configurable: true, get: () => 100 })
  Object.defineProperty(video, 'paused', { configurable: true, get: () => !playing })
  Object.defineProperty(video, 'ended', { configurable: true, get: () => false })
  vi.spyOn(video, 'pause').mockImplementation(() => undefined)
  vi.spyOn(video, 'load').mockImplementation(() => undefined)
  return vi.spyOn(video, 'play').mockResolvedValue(undefined)
}

describe('RecordingPlayer lifecycle', () => {
  it('waits for the first committed segment without requesting a manifest', async () => {
    const fetchMock = vi.spyOn(globalThis, 'fetch')
    const view = render(<RecordingPlayer recordingId="rec-live" active hasCommittedSegments={false} />)
    expect(view.getByRole('status')).toHaveTextContent('첫 세그먼트를 기다리는 중입니다.')
    expect(view.container.querySelector('video')).toBeNull()
    expect(hlsState.loadSource).not.toHaveBeenCalled()
    expect(fetchMock).not.toHaveBeenCalled()
    view.unmount()
    fetchMock.mockRestore()
  })

  it('loads the live archive HLS endpoint once committed media exists', async () => {
    const view = render(<RecordingPlayer recordingId="rec-live" active hasCommittedSegments />)
    await waitFor(() => expect(hlsState.loadSource).toHaveBeenCalledWith('/api/recordings/rec-live/play/live/master.m3u8'))
    const video = view.container.querySelector('video')!
    expect(video.controls).toBe(true)
    expect(video).toHaveAttribute('aria-label', '녹화 중 실시간 HLS 재생')
    vi.spyOn(video, 'pause').mockImplementation(() => undefined)
    vi.spyOn(video, 'load').mockImplementation(() => undefined)
    view.unmount()
  })

  it('switches from live archive HLS to terminal VOD and cleans up the previous source', async () => {
    const view = render(<RecordingPlayer recordingId="rec-live" active hasCommittedSegments />)
    await waitFor(() => expect(hlsState.loadSource).toHaveBeenCalledWith('/api/recordings/rec-live/play/live/master.m3u8'))
    const video = view.container.querySelector('video')!
    const pause = vi.spyOn(video, 'pause').mockImplementation(() => undefined)
    const load = vi.spyOn(video, 'load').mockImplementation(() => undefined)
    view.rerender(<RecordingPlayer recordingId="rec-live" active={false} />)
    await waitFor(() => expect(hlsState.loadSource).toHaveBeenCalledWith('/api/recordings/rec-live/play/master.m3u8'))
    expect(hlsState.destroy).toHaveBeenCalledTimes(1)
    expect(pause).toHaveBeenCalled()
    expect(load).toHaveBeenCalled()
    expect(video.controls).toBe(true)
    view.unmount()
  })

  it('reloads terminal VOD when its timeline revision changes', async () => {
    const view = render(<RecordingPlayer recordingId="rec-repair" timelineRevision={4} />)
    await waitFor(() => expect(hlsState.loadSource).toHaveBeenCalledWith('/api/recordings/rec-repair/play/master.m3u8?timeline_revision=4'))
    const video = view.container.querySelector('video')!
    vi.spyOn(video, 'pause').mockImplementation(() => undefined)
    vi.spyOn(video, 'load').mockImplementation(() => undefined)
    view.rerender(<RecordingPlayer recordingId="rec-repair" timelineRevision={5} />)
    await waitFor(() => expect(hlsState.loadSource).toHaveBeenCalledWith('/api/recordings/rec-repair/play/master.m3u8?timeline_revision=5'))
    expect(hlsState.destroy).toHaveBeenCalledTimes(1)
    view.unmount()
  })

  it('keeps same segment and in-segment offset after historical prefix insertion', async () => {
    const { fetchMock, textRead } = mockRevisionPlaylists({
      '4': playlist(['A.ts', 10], ['B.ts', 10], ['C.ts', 10], ['D.ts', 10], ['E.ts', 10]),
      '5': playlist(['X.ts', 10], ['Y.ts', 10], ['Z.ts', 10], ['A.ts', 10], ['B.ts', 10], ['C.ts', 10], ['D.ts', 10], ['E.ts', 10]),
    })
    const view = render(<RecordingPlayer recordingId="rec-prefix" timelineRevision={4} />)
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1))
    await waitFor(() => expect(textRead).toHaveBeenCalledTimes(1))
    await flushPlaylistReads()
    const video = view.container.querySelector('video')!
    const play = setVideoState(video, 32.5, true)

    view.rerender(<RecordingPlayer recordingId="rec-prefix" timelineRevision={5} />)
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2))
    await waitFor(() => expect(textRead).toHaveBeenCalledTimes(2))
    await flushPlaylistReads()
    video.dispatchEvent(new Event('loadedmetadata'))

    await waitFor(() => expect(video.currentTime).toBeCloseTo(62.5))
    expect(play).toHaveBeenCalledTimes(1)
    expect(fetchMock.mock.calls.map(([url]) => String(url))).toEqual([
      '/api/recordings/rec-prefix/play/tracks/main/playlist.m3u8?timeline_revision=4',
      '/api/recordings/rec-prefix/play/tracks/main/playlist.m3u8?timeline_revision=5',
    ])
    view.unmount()
  })

  it('keeps same segment after mid-gap repair and preserves paused state', async () => {
    const { fetchMock, textRead } = mockRevisionPlaylists({
      '7': playlist(['A.ts', 10], ['B.ts', 10], ['D.ts', 10], ['E.ts', 10]),
      '8': playlist(['A.ts', 10], ['B.ts', 10], ['C.ts', 10], ['D.ts', 10], ['E.ts', 10]),
    })
    const view = render(<RecordingPlayer recordingId="rec-gap" timelineRevision={7} />)
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1))
    await waitFor(() => expect(textRead).toHaveBeenCalledTimes(1))
    await flushPlaylistReads()
    const video = view.container.querySelector('video')!
    const play = setVideoState(video, 22.25, false)

    view.rerender(<RecordingPlayer recordingId="rec-gap" timelineRevision={8} />)
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2))
    await waitFor(() => expect(textRead).toHaveBeenCalledTimes(2))
    await flushPlaylistReads()
    video.dispatchEvent(new Event('loadedmetadata'))

    await waitFor(() => expect(video.currentTime).toBeCloseTo(32.25))
    expect(play).not.toHaveBeenCalled()
    view.unmount()
  })

  it('uses matching program date time when segment URI changes', async () => {
    const oldPlaylist = '#EXTM3U\n#EXT-X-PROGRAM-DATE-TIME:2026-10-09T10:00:00Z\n#EXTINF:10,\nA.ts\n#EXTINF:10,\nD-old.ts\n#EXT-X-ENDLIST\n'
    const repairedPlaylist = '#EXTM3U\n#EXT-X-PROGRAM-DATE-TIME:2026-10-09T09:59:50Z\n#EXTINF:10,\nX.ts\n#EXTINF:10,\nA.ts\n#EXTINF:10,\nD-new.ts\n#EXT-X-ENDLIST\n'
    const { fetchMock, textRead } = mockRevisionPlaylists({ '10': oldPlaylist, '11': repairedPlaylist })
    const view = render(<RecordingPlayer recordingId="rec-pdt" timelineRevision={10} />)
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1))
    await waitFor(() => expect(textRead).toHaveBeenCalledTimes(1))
    await flushPlaylistReads()
    const video = view.container.querySelector('video')!
    setVideoState(video, 12.5, false)

    view.rerender(<RecordingPlayer recordingId="rec-pdt" timelineRevision={11} />)
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2))
    await waitFor(() => expect(textRead).toHaveBeenCalledTimes(2))
    await flushPlaylistReads()
    video.dispatchEvent(new Event('loadedmetadata'))

    await waitFor(() => expect(video.currentTime).toBeCloseTo(22.5))
    view.unmount()
  })

  it('uses effective byte-range offset as part of segment identity', async () => {
    const oldPlaylist = '#EXTM3U\n#EXTINF:4,\n#EXT-X-BYTERANGE:100@0\npacked.ts\n#EXTINF:4,\n#EXT-X-BYTERANGE:100\npacked.ts\n#EXT-X-ENDLIST\n'
    const repairedPlaylist = '#EXTM3U\n#EXTINF:2,\nX.ts\n#EXTINF:4,\n#EXT-X-BYTERANGE:100@0\npacked.ts\n#EXTINF:4,\n#EXT-X-BYTERANGE:100@100\npacked.ts\n#EXT-X-ENDLIST\n'
    const { fetchMock, textRead } = mockRevisionPlaylists({ '20': oldPlaylist, '21': repairedPlaylist })
    const view = render(<RecordingPlayer recordingId="rec-range" timelineRevision={20} />)
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1))
    await waitFor(() => expect(textRead).toHaveBeenCalledTimes(1))
    await flushPlaylistReads()
    const video = view.container.querySelector('video')!
    setVideoState(video, 4.5, false)

    view.rerender(<RecordingPlayer recordingId="rec-range" timelineRevision={21} />)
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2))
    await waitFor(() => expect(textRead).toHaveBeenCalledTimes(2))
    await flushPlaylistReads()
    video.dispatchEvent(new Event('loadedmetadata'))

    await waitFor(() => expect(video.currentTime).toBeCloseTo(6.5))
    view.unmount()
  })

  it('uses nearest surviving segment identity before absolute-time fallback', async () => {
    const { fetchMock, textRead } = mockRevisionPlaylists({
      '30': playlist(['A.ts', 10], ['B.ts', 10], ['C.ts', 10], ['D.ts', 10], ['E.ts', 10]),
      '31': playlist(['X.ts', 10], ['A.ts', 10], ['B.ts', 10], ['C.ts', 10], ['E.ts', 10]),
    })
    const view = render(<RecordingPlayer recordingId="rec-nearest" timelineRevision={30} />)
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1))
    await waitFor(() => expect(textRead).toHaveBeenCalledTimes(1))
    await flushPlaylistReads()
    const video = view.container.querySelector('video')!
    setVideoState(video, 32.4, false)

    view.rerender(<RecordingPlayer recordingId="rec-nearest" timelineRevision={31} />)
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2))
    await waitFor(() => expect(textRead).toHaveBeenCalledTimes(2))
    await flushPlaylistReads()
    video.dispatchEvent(new Event('loadedmetadata'))

    await waitFor(() => expect(video.currentTime).toBeCloseTo(39.95))
    view.unmount()
  })

  it('falls back to absolute time when anchored segment no longer exists', async () => {
    const { fetchMock, textRead } = mockRevisionPlaylists({
      '2': playlist(['A.ts', 10], ['B.ts', 10], ['C.ts', 10], ['D.ts', 10]),
      '3': playlist(['X.ts', 10], ['Y.ts', 10], ['Z.ts', 10]),
    })
    const view = render(<RecordingPlayer recordingId="rec-missing-anchor" timelineRevision={2} />)
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1))
    await waitFor(() => expect(textRead).toHaveBeenCalledTimes(1))
    await flushPlaylistReads()
    const video = view.container.querySelector('video')!
    const play = setVideoState(video, 32.4, false)

    view.rerender(<RecordingPlayer recordingId="rec-missing-anchor" timelineRevision={3} />)
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2))
    await waitFor(() => expect(textRead).toHaveBeenCalledTimes(2))
    await flushPlaylistReads()
    video.dispatchEvent(new Event('loadedmetadata'))

    await waitFor(() => expect(video.currentTime).toBeCloseTo(32.4))
    expect(play).not.toHaveBeenCalled()
    view.unmount()
  })

  it('destroys hls.js and clears the media element when unmounted', async () => {
    const onVideoRef = vi.fn()
    const view = render(<RecordingPlayer recordingId="rec-1" onVideoRef={onVideoRef} />)
    await waitFor(() => expect(hlsState.loadSource).toHaveBeenCalledWith('/api/recordings/rec-1/play/master.m3u8'))
    const video = view.container.querySelector('video')!
    expect(video.controls).toBe(true)
    expect(onVideoRef).toHaveBeenCalledWith(video)
    const pause = vi.spyOn(video, 'pause').mockImplementation(() => undefined)
    const load = vi.spyOn(video, 'load').mockImplementation(() => undefined)
    view.unmount()
    expect(onVideoRef).toHaveBeenLastCalledWith(null)
    expect(hlsState.destroy).toHaveBeenCalledTimes(1)
    expect(video.hasAttribute('src')).toBe(false)
    expect(pause).toHaveBeenCalled()
    expect(load).toHaveBeenCalled()
  })
})
