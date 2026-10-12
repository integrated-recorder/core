import { fireEvent, render, screen } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { authAPI, userPreferencesAPI } from '@/api'
import { I18nProvider } from '@/i18n/provider'
import { TimedEventTimeline } from './timed-event-timeline'

function renderTimeline(props: Partial<Parameters<typeof TimedEventTimeline>[0]> = {}) {
  vi.spyOn(authAPI, 'session').mockResolvedValue({ auth_enabled: true, authenticated: true, needs_bootstrap: false })
  vi.spyOn(userPreferencesAPI, 'get').mockResolvedValue({ locale: 'en-US', theme: 'system', timezone: 'system' })
  return render(<QueryClientProvider client={new QueryClient({ defaultOptions: { queries: { retry: false } } })}><I18nProvider><TimedEventTimeline items={[]} loading={false} retry={() => undefined} live={false} currentTime={0} hasMore={false} loadingMore={false} loadMore={() => undefined} onSeek={() => undefined} exportHref="/export" {...props} /></I18nProvider></QueryClientProvider>)
}

afterEach(() => vi.restoreAllMocks())

describe('timed event replay timeline', () => {
  it('stays absent when a recording has no source timed events', () => {
    const view = renderTimeline()
    expect(view.container).toBeEmptyDOMElement()
  })

  it('shows the common chat projection and seeks to its VOD offset', async () => {
    const onSeek = vi.fn()
    renderTimeline({
      items: [{ event_archive_ordinal: 1, event_timeline_ordinal: 1, event_type: 'chat.message', observed_at: '2026-01-01T00:00:02Z', presentation_offset_ms: 2000, actor: { display_name: 'viewer' }, message: { text: 'hello from chat' }, extension: { access_token: 'must stay hidden' } }],
      currentTime: 2.4,
      onSeek,
    })
    const message = await screen.findByText('hello from chat')
    expect(screen.getByText('viewer')).toBeInTheDocument()
    expect(message.closest('button')).toHaveAttribute('aria-current', 'true')
    expect(screen.queryByText('must stay hidden')).not.toBeInTheDocument()
    fireEvent.click(message.closest('button')!)
    expect(onSeek).toHaveBeenCalledWith(2)
  })

  it('shows recent active events without offering a live seek', async () => {
    renderTimeline({ items: [{ event_archive_ordinal: 2, event_timeline_ordinal: 2, event_type: 'custom.example', observed_at: '2026-01-01T00:00:02Z', presentation_offset_ms: 2000 }], live: true })
    expect(await screen.findByText('custom.example')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /custom\.example/ })).toBeDisabled()
  })
})
