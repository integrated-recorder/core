import type { RecordingListItem } from '@/types/api'
import { listResourceLabel } from '@/lib/recordings'
import { useQuery } from '@tanstack/react-query'
import { adaptersQuery } from '@/api/queries'
import { AdapterMark } from '@/components/adapter/adapter-mark'
import { PreviewThumbnail } from '@/components/recording/previews'
import { Link } from '@tanstack/react-router'
import type { PreviewSummary } from '@/types/api'
import { useI18n } from '@/i18n/provider'

export function RecordingListIdentity({ item }: { item: RecordingListItem }) {
  const adapters = useQuery(adaptersQuery)
  const adapter = adapters.data?.find(candidate => candidate.status.id === item.adapter_id)
  return <div className="flex max-w-[200px] min-w-0 items-center gap-2"><AdapterMark adapter={adapter} adapterId={item.adapter_id} name={item.adapter_name} size="sm" /><span className="min-w-0"><span className="block truncate text-xs font-medium" title={item.adapter_name || item.adapter_id}>{item.adapter_name || item.adapter_id}</span><span className="mt-0.5 block truncate text-[10px] text-muted-foreground" title={listResourceLabel(item)}>{listResourceLabel(item)}</span></span></div>
}

export function RecordingTitle({ recordingId, title, preview, adapterId, adapterName, adapter }: {
  recordingId: string; title?: string; preview?: PreviewSummary; adapterId?: string; adapterName?: string; adapter?: import('@/types/api').Adapter
}) {
  const { t } = useI18n()
  const adapters = useQuery({ ...adaptersQuery, enabled: !adapter })
  const currentAdapter = adapter ?? adapters.data?.find(candidate => candidate.status.id === adapterId)
  return <div className="flex min-w-0 items-center gap-2.5">
    <PreviewThumbnail recordingId={recordingId} summary={preview} adapter={currentAdapter} adapterId={adapterId} adapterName={adapterName} className="w-[76px]" />
    <span className="min-w-0">
      <Link to="/recordings/$recordingId" params={{ recordingId }} className="block truncate font-medium hover:text-primary" title={title}>{title || t('recordings.noTitle')}</Link>
      <span className="mt-0.5 block truncate font-mono text-[10px] text-muted-foreground">{recordingId}</span>
    </span>
  </div>
}
