import type { Adapter, WatchEvent } from '@/types/api'
import type { QueryClient } from '@tanstack/react-query'
import { dashboardQuery, qk } from '@/api/queries'
import { translate, type ResolvedLocale, type TranslationKey } from '@/i18n/catalog'

export function watchCapableAdapters(adapters: readonly Adapter[] | undefined): Adapter[] {
  return (adapters ?? []).filter(adapter => adapter.status.state === 'ready' && adapter.descriptor?.capabilities?.includes('watch'))
}

const eventLabels: Record<string, TranslationKey> = {
  watch_created: 'watch.event.created', watch_updated: 'watch.event.updated', watch_enabled: 'watch.event.enabled', watch_disabled: 'watch.event.disabled',
  live_detected: 'watch.event.liveDetected', recording_started: 'watch.event.recordingStarted', recording_ended: 'watch.event.recordingEnded',
  check_failed: 'watch.event.checkFailed', attention_required: 'watch.event.attention', watch_deleted: 'watch.event.deleted', manual_watch_check: 'watch.event.manualCheck',
}

export function watchEventLabel(event: WatchEvent, locale: ResolvedLocale = 'ko-KR'): string {
  return translate(locale, eventLabels[event.type] ?? 'watch.event.changed')
}

export function watchErrorLabel(code?: string, locale: ResolvedLocale = 'ko-KR'): string | undefined {
  if (!code) return undefined
  const labels: Record<string, TranslationKey> = {
    adapter_unavailable: 'watch.error.adapterUnavailable', adapter_disabled: 'watch.error.adapterDisabled',
    adapter_restarting: 'watch.error.adapterRestarting', adapter_timeout: 'watch.error.adapterTimeout',
    authentication_required: 'watch.error.authenticationRequired', interaction_required: 'watch.error.interactionRequired',
    check_failed: 'watch.error.checkFailed', resolve_failed: 'watch.error.resolveFailed',
  }
  return translate(locale, labels[code] ?? 'watch.error.unknown')
}

export async function invalidateWatchData(client: QueryClient, id?: string) {
  await Promise.all([
    client.invalidateQueries({ queryKey: qk.watches }),
    client.invalidateQueries({ queryKey: dashboardQuery.queryKey }),
    ...(id ? [client.invalidateQueries({ queryKey: qk.watch(id) })] : []),
  ])
}
