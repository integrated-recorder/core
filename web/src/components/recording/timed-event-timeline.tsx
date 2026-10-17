import { Download, MessageSquare } from 'lucide-react'
import type { TimedEvent } from '@/types/api'
import { formatDuration } from '@/lib/utils'
import { Button } from '@/components/ui/button'
import { useI18n } from '@/i18n/provider'

function messageText(event: TimedEvent) {
  const text = event.message?.text?.trim()
  if (text) return text
  return event.message?.fragments?.map(fragment => fragment.text).join('').trim() ?? ''
}

type TimedEventLabelKey = 'timedEvents.type.message' | 'timedEvents.type.edit' | 'timedEvents.type.delete' | 'timedEvents.type.system' | 'timedEvents.type.donation' | 'timedEvents.type.subscription'
function labelFor(event: TimedEvent, t: (key: TimedEventLabelKey) => string) {
  switch (event.event_type) {
    case 'chat.message': return t('timedEvents.type.message')
    case 'chat.edit': return t('timedEvents.type.edit')
    case 'chat.delete': return t('timedEvents.type.delete')
    case 'chat.system': return t('timedEvents.type.system')
    case 'support.donation': return t('timedEvents.type.donation')
    case 'support.subscription': return t('timedEvents.type.subscription')
    default: return event.event_type
  }
}

export function TimedEventTimeline({ items, loading, errorMessage, retry, live, currentTime, hasMore, loadingMore, loadMore, onSeek, exportHref }: {
  items?: TimedEvent[]
  loading: boolean
  errorMessage?: string
  retry: () => void
  live: boolean
  currentTime: number
  hasMore: boolean
  loadingMore: boolean
  loadMore: () => void
  onSeek: (seconds: number) => void
  exportHref?: string
}) {
  const { t } = useI18n()
  if (loading && !items?.length) return <p className="py-4 text-sm text-muted-foreground" role="status">{t('common.loading')}</p>
  if (errorMessage && !items?.length) return <div role="alert" className="flex flex-wrap items-center justify-between gap-2 py-3"><p className="text-sm text-muted-foreground">{errorMessage}</p><Button size="sm" variant="outline" onClick={retry}>{t('common.retry')}</Button></div>
  if (!items?.length) return null
  const activeOrdinal = !live ? items.reduce((active, event) => event.presentation_offset_ms <= currentTime * 1000 ? event.event_archive_ordinal : active, 0) : 0
  return <section aria-label={t('timedEvents.title')}>
    <div className="mb-2 flex items-center justify-between gap-3">
      <h3 className="text-sm font-semibold">{t('timedEvents.title')}</h3>
      {exportHref && <a href={exportHref} download><Button size="sm" variant="ghost"><Download className="h-3.5 w-3.5" />{t('timedEvents.export')}</Button></a>}
    </div>
    <ol className="max-h-[28rem] divide-y divide-border overflow-y-auto rounded-md border border-border" aria-label={t('timedEvents.title')}>
      {items.map(event => {
        const text = messageText(event)
        const actor = event.actor?.display_name?.trim()
        const commonMessage = event.event_type.startsWith('chat.') && text
          ? text
          : event.event_type === 'support.donation' && event.amount
            ? `${event.amount}${event.currency ? ` ${event.currency}` : ''}`
            : ''
        return <li key={event.event_archive_ordinal}>
          <button type="button" disabled={live} onClick={() => onSeek(event.presentation_offset_ms / 1000)} aria-current={event.event_archive_ordinal === activeOrdinal ? 'true' : undefined} className="focus-ring flex w-full items-start gap-3 px-3 py-2.5 text-left aria-[current=true]:bg-primary/5 disabled:cursor-default">
            <time className="shrink-0 pt-0.5 font-mono text-[11px] tabular-nums text-muted-foreground">{formatDuration(event.presentation_offset_ms / 1000)}</time>
            <div className="min-w-0 flex-1">
              <div className="flex flex-wrap items-center gap-x-2 gap-y-0.5">
                {event.event_type.startsWith('chat.') && <MessageSquare aria-hidden="true" className="h-3.5 w-3.5 text-muted-foreground" />}
                {actor && <span className="max-w-48 truncate text-xs font-semibold">{actor}</span>}
                <span className="text-[11px] text-muted-foreground">{labelFor(event, t)}</span>
              </div>
              {commonMessage && <p className="mt-1 whitespace-pre-wrap break-words text-sm">{commonMessage}</p>}
            </div>
          </button>
        </li>
      })}
    </ol>
    {errorMessage && <p role="status" className="mt-2 text-xs text-muted-foreground">{errorMessage}</p>}
    {hasMore && <div className="pt-2"><Button size="sm" variant="outline" onClick={loadMore} disabled={loadingMore}>{loadingMore ? t('common.loading') : t('timedEvents.more')}</Button></div>}
  </section>
}
