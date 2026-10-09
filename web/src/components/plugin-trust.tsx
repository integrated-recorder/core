import { CircleAlert } from 'lucide-react'
import { Badge } from '@/components/ui/badge'
import type { PluginTrust } from '@/types/api'
import { useI18n } from '@/i18n/provider'

type Props = { trust?: PluginTrust; compact?: boolean; warning?: boolean }

function hasValidTrustPair(trust: PluginTrust) {
  const knownPublisher = trust.publisher === 'first_party' || trust.publisher === 'third_party' || trust.publisher === 'unknown'
  return knownPublisher && typeof trust.reviewed === 'boolean' && ((trust.provenance === 'bundled' && trust.authority === 'core_release') ||
    (trust.provenance === 'registry' && (trust.authority === 'official' || trust.authority === 'custom')) ||
    (trust.provenance === 'operator' && trust.authority === 'local'))
}

export function PluginTrustBadges({ trust, compact = false, warning = false }: Props) {
  const { t } = useI18n()
  if (!trust || !hasValidTrustPair(trust)) return <Badge aria-label={t('plugins.trust.legacy')}>{t('plugins.trust.legacy')}</Badge>

  const authorityLabel = trust.provenance === 'registry'
    ? trust.authority === 'official' ? t('plugins.trust.officialRegistry') : trust.authority === 'custom' ? t('plugins.trust.customRegistry') : t('plugins.trust.localOperator')
    : trust.provenance === 'bundled' ? t('plugins.trust.bundled') : t('plugins.trust.localOperator')
  const publisherLabel = trust.publisher === 'first_party' ? t('plugins.trust.firstParty') : trust.publisher === 'third_party' ? t('plugins.trust.thirdParty') : t('plugins.trust.unknownPublisher')
  const isUnreviewedOrigin = trust.provenance === 'operator' || (trust.provenance === 'registry' && trust.authority === 'custom')
  const showRegistryApproved = trust.provenance === 'registry' && trust.authority === 'official' && trust.reviewed

  return <span className="inline-flex min-w-0 flex-wrap items-center gap-1.5">
    <Badge tone={trust.provenance === 'bundled' ? 'blue' : 'neutral'}>{authorityLabel}</Badge>
    <Badge>{publisherLabel}</Badge>
    {showRegistryApproved && <Badge tone="green">{t('plugins.trust.registryApproved')}</Badge>}
    {warning && isUnreviewedOrigin && <span className={`inline-flex items-center gap-1 text-[10px] leading-4 text-amber-800 dark:text-amber-200 ${compact ? 'max-w-full' : ''}`}>
      <CircleAlert aria-hidden="true" className="h-3 w-3 shrink-0" />
      <span>{t('plugins.trust.warning')}</span>
    </span>}
  </span>
}
