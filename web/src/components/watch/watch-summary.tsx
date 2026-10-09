import { Link } from '@tanstack/react-router'
import { ArrowRight, Radio, ShieldAlert, Timer, UsersRound } from 'lucide-react'
import { Card, CardContent } from '@/components/ui/card'
import type { Dashboard } from '@/types/api'
import { useI18n } from '@/i18n/provider'

export function WatchSummary({ summary }: { summary: NonNullable<Dashboard['watches']> }) {
  const { t } = useI18n()
  const items = [
    { label: t('dashboard.watches.registered'), value: summary.total, icon: UsersRound },
    { label: t('dashboard.watches.enabled'), value: summary.enabled, icon: Timer },
    { label: t('dashboard.watches.recording'), value: summary.recording, icon: Radio },
    { label: t('dashboard.watches.offline'), value: summary.offline, icon: null },
    { label: t('dashboard.watches.backoff'), value: summary.backoff, icon: null },
    { label: t('dashboard.watches.attention'), value: summary.attention_required, icon: ShieldAlert },
  ]
  return <Card>
    <CardContent className="flex flex-wrap items-center gap-x-7 gap-y-3 py-4">
      {items.map(({ label, value, icon: Icon }) => <div key={label} className="flex min-w-[94px] items-center gap-2">
        {Icon && <Icon className="h-4 w-4 shrink-0 text-muted-foreground" aria-hidden="true" />}
        <span><span className="block text-[10px] text-muted-foreground">{label}</span><span className="text-sm font-semibold tabular-nums">{value}</span></span>
      </div>)}
      <Link to="/watches" className="ml-auto inline-flex items-center gap-1 text-xs font-medium text-primary">{t('dashboard.watches.manage')} <ArrowRight className="h-3.5 w-3.5" /></Link>
    </CardContent>
  </Card>
}
