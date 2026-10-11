import { ChevronRight, Home, MoveUp } from 'lucide-react'
import { Button } from '@/components/ui/button'
import type { ResourceRef } from '@/types/api'
import { resourceBreadcrumbs, resourceIdentity } from '@/lib/resources'
import { useI18n } from '@/i18n/provider'

export function ResourceBreadcrumbs({ resource, onNavigate }: { resource?: ResourceRef; onNavigate: (resource?: ResourceRef) => void }) {
  const { t } = useI18n()
  const chain = resourceBreadcrumbs(resource)
  return <nav aria-label={t('adapter.detail.resourcePath')} className="flex flex-wrap items-center gap-1 text-xs">
    <Button type="button" size="sm" variant={!resource ? 'secondary' : 'ghost'} aria-current={!resource ? 'location' : undefined} onClick={() => onNavigate(undefined)} disabled={!resource}><Home className="h-3.5 w-3.5" />{t('adapter.detail.root')}</Button>
    {chain.map((item, index) => <span key={`${item.resource_type}:${item.resource_id}:${index}`} className="inline-flex items-center gap-1"><ChevronRight aria-hidden="true" className="h-3 w-3 text-muted-foreground" /><Button type="button" size="sm" variant={index === chain.length - 1 ? 'secondary' : 'ghost'} aria-current={index === chain.length - 1 ? 'location' : undefined} onClick={() => onNavigate(item)}>{resourceIdentity(item)}</Button></span>)}
    {resource?.parent && <Button type="button" variant="outline" size="sm" className="ml-auto" onClick={() => onNavigate(resource.parent)}><MoveUp className="h-3.5 w-3.5" />{t('adapter.detail.parent')}</Button>}
  </nav>
}
