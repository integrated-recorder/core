import { CircleAlert } from 'lucide-react'
import { Badge } from '@/components/ui/badge'
import type { PluginTrust } from '@/types/api'

type Props = { trust?: PluginTrust; compact?: boolean; warning?: boolean }

const provenanceLabels: Record<PluginTrust['provenance'], string> = {
  bundled: 'Bundled',
  registry: 'Official Registry',
  operator: 'Local operator',
}

const publisherLabels: Record<PluginTrust['publisher'], string> = {
  first_party: 'First-party',
  third_party: 'Third-party',
  unknown: 'Unknown publisher',
}

function hasValidTrustPair(trust: PluginTrust) {
  const knownPublisher = trust.publisher === 'first_party' || trust.publisher === 'third_party' || trust.publisher === 'unknown'
  return knownPublisher && typeof trust.reviewed === 'boolean' && ((trust.provenance === 'bundled' && trust.authority === 'core_release') ||
    (trust.provenance === 'registry' && (trust.authority === 'official' || trust.authority === 'custom')) ||
    (trust.provenance === 'operator' && trust.authority === 'local'))
}

export function PluginTrustBadges({ trust, compact = false, warning = false }: Props) {
  if (!trust || !hasValidTrustPair(trust)) return <Badge aria-label="Legacy plugin, provenance unavailable">Legacy plugin · provenance unavailable</Badge>

  const authorityLabel = trust.provenance === 'registry'
    ? trust.authority === 'official' ? 'Official Registry' : trust.authority === 'custom' ? 'Custom Registry' : 'Local operator'
    : provenanceLabels[trust.provenance]
  const isUnreviewedOrigin = trust.provenance === 'operator' || (trust.provenance === 'registry' && trust.authority === 'custom')
  const showRegistryReviewed = trust.provenance === 'registry' && trust.authority === 'official' && trust.reviewed

  return <span className="inline-flex min-w-0 flex-wrap items-center gap-1.5">
    <Badge tone={trust.provenance === 'bundled' ? 'blue' : 'neutral'}>{authorityLabel}</Badge>
    <Badge>{publisherLabels[trust.publisher]}</Badge>
    {showRegistryReviewed && <Badge tone="green">Registry reviewed</Badge>}
    {warning && isUnreviewedOrigin && <span className={`inline-flex items-center gap-1 text-[10px] leading-4 text-amber-800 dark:text-amber-200 ${compact ? 'max-w-full' : ''}`}>
      <CircleAlert aria-hidden="true" className="h-3 w-3 shrink-0" />
      <span>Integrated Recorder에서 검토되지 않았으며 샌드박스 없이 실행됩니다.</span>
    </span>}
  </span>
}
