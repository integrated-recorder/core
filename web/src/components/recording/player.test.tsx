import { render, waitFor } from '@testing-library/react'
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

afterEach(() => vi.clearAllMocks())

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
