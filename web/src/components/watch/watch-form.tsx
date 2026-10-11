import { useMemo, useState } from 'react'
import { useInfiniteQuery } from '@tanstack/react-query'
import { ChevronDown, Search } from 'lucide-react'
import { adaptersAPI } from '@/api'
import { qk } from '@/api/queries'
import { SchemaForm } from '@/components/schema-form'
import { ResourceBreadcrumbs } from '@/components/adapter/resource-breadcrumbs'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { supportsCapability } from '@/lib/capabilities'
import { resourceLabel } from '@/lib/utils'
import { useI18n } from '@/i18n/provider'
import type { Adapter, ResourceRef, Schema, WatchView } from '@/types/api'

export type WatchFormValue = {
  adapter_id: string
  input: Record<string, unknown>
  input_secrets: Record<string, string>
  clear_input_secrets?: string[]
  resource?: ResourceRef
  clear_resource?: boolean
  title?: string
  preview_mode: 'disabled' | 'segment'
  check_interval_seconds: number
}

type Props = {
  adapter: Adapter
  schema: Schema
  watch?: WatchView
  busy?: boolean
  submitLabel: string
  onSubmit: (value: WatchFormValue) => void | Promise<void>
}

export function WatchForm({ adapter, schema, watch, busy, submitLabel, onSubmit }: Props) {
  const { t } = useI18n()
  const [title, setTitle] = useState(watch?.title ?? '')
  const [interval, setInterval] = useState(watch?.check_interval_seconds ?? 10)
  const [previewMode, setPreviewMode] = useState<'disabled' | 'segment'>(watch?.preview_mode ?? 'segment')
  const [resource, setResource] = useState<ResourceRef | undefined>(watch?.resource)
  const [browseParent, setBrowseParent] = useState<ResourceRef | undefined>()
  const [browseOpen, setBrowseOpen] = useState(false)
  const [search, setSearch] = useState('')
  const [resourceType, setResourceType] = useState('')
  const [appliedSearch, setAppliedSearch] = useState('')
  const [appliedType, setAppliedType] = useState('')
  const [intervalError, setIntervalError] = useState('')
  const canBrowse = supportsCapability(adapter, 'resource_browse')
  const watchInput = watch?.input
  const secretConfigured = watch?.input_secret_configured
  const initialValues = useMemo(() => watchInput ? safeInitialValues(schema, watchInput, secretConfigured) : undefined, [schema, watchInput, secretConfigured])
  const resources = useInfiniteQuery({
    queryKey: qk.resources(adapter.status.id, { purpose: 'watch', q: appliedSearch, resourceType: appliedType, parent: browseParent }),
    queryFn: ({ pageParam }) => adaptersAPI.resources(adapter.status.id, {
      q: appliedSearch || undefined, resource_type: appliedType || undefined, parent: browseParent,
      cursor: pageParam, limit: 20,
    }),
    initialPageParam: undefined as string | undefined,
    getNextPageParam: page => page.next_cursor,
    enabled: canBrowse && browseOpen,
    retry: false,
  })
  const items = resources.data?.pages.flatMap(page => page.items) ?? []
  const submit = (result: { values: Record<string, unknown>; secrets: Record<string, string>; clearSecrets: string[] }) => {
    if (!Number.isInteger(interval) || interval < 2 || interval > 3600) {
      setIntervalError(t('watch.form.intervalError'))
      return
    }
    setIntervalError('')
    return onSubmit({
      adapter_id: adapter.status.id,
      input: result.values,
      input_secrets: result.secrets,
      ...(result.clearSecrets.length ? { clear_input_secrets: result.clearSecrets } : {}),
      ...(resource ? { resource } : {}),
      ...(watch?.resource && !resource ? { clear_resource: true } : {}),
      ...(watch ? { title: title.trim() } : title.trim() ? { title: title.trim() } : {}),
      preview_mode: previewMode,
      check_interval_seconds: interval,
    })
  }

  const runResourceSearch = () => {
    setAppliedSearch(search.trim())
    setAppliedType(resourceType.trim())
    setBrowseOpen(true)
  }

  return <div className="space-y-4">
    <Card>
      <CardHeader><CardTitle>{t('watch.form.title')}</CardTitle></CardHeader>
      <CardContent className="grid gap-4 sm:grid-cols-2">
        <label className="space-y-2"><span className="text-sm font-medium">{t('watch.form.recordingTitle')} <span className="text-xs text-muted-foreground">({t('watch.form.optional')})</span></span><Input value={title} onChange={event => setTitle(event.target.value)} maxLength={256} placeholder={t('watch.form.titlePlaceholder')} /></label>
        <label className="space-y-2"><span className="text-sm font-medium">{t('watch.form.interval')}</span><Input type="number" min={2} max={3600} step={1} value={interval} onChange={event => { setInterval(event.target.value === '' ? 0 : Number(event.target.value)); setIntervalError('') }} aria-invalid={Boolean(intervalError)} aria-describedby={intervalError ? 'watch-interval-error' : undefined} />{intervalError && <span id="watch-interval-error" className="block text-xs text-destructive" role="alert">{intervalError}</span>}</label>
        <label className="flex cursor-pointer items-start gap-3 rounded-md border border-border p-3 sm:col-span-2"><input type="checkbox" className="mt-0.5 h-4 w-4 accent-primary" checked={previewMode === 'segment'} onChange={event => setPreviewMode(event.target.checked ? 'segment' : 'disabled')} /><span><span className="block text-sm font-medium">{t('watch.form.previewTitle')}</span><span className="mt-1 block text-xs leading-5 text-muted-foreground">{t('watch.form.previewHelp')}</span></span></label>
        {canBrowse && <div className="space-y-3 sm:col-span-2"><div className="flex flex-wrap items-center justify-between gap-2"><p className="text-sm font-medium">{t('watch.form.resource')} <span className="text-xs text-muted-foreground">({t('watch.form.optional')})</span></p><Button type="button" variant="outline" size="sm" onClick={() => setBrowseOpen(open => !open)} aria-expanded={browseOpen}>{browseOpen ? t('watch.form.hideBrowse') : t('watch.form.browse')}<ChevronDown className="h-3.5 w-3.5" /></Button></div>
          {resource && <p className="text-xs text-muted-foreground">{t('watch.form.selectedResource')}: <span className="font-medium text-foreground">{resourceLabel(resource)}</span><Button type="button" variant="link" size="sm" className="ml-1 h-auto p-0" onClick={() => setResource(undefined)}>{t('watch.form.clear')}</Button></p>}
          {browseOpen && <div className="rounded-md border border-border p-3"><ResourceBreadcrumbs resource={browseParent} onNavigate={setBrowseParent} /><div className="mt-3 grid gap-2 sm:grid-cols-[1fr_180px_auto]"><Input value={search} onChange={event => setSearch(event.target.value)} onKeyDown={event => { if (event.key === 'Enter') runResourceSearch() }} placeholder={t('watch.form.searchPlaceholder')} aria-label={t('watch.form.searchLabel')} /><Input value={resourceType} onChange={event => setResourceType(event.target.value)} placeholder={t('watch.form.typePlaceholder')} aria-label={t('watch.form.typeLabel')} /><Button type="button" variant="outline" onClick={runResourceSearch}><Search className="h-4 w-4" />{t('watch.form.search')}</Button></div>
            {resources.isLoading ? <p className="py-4 text-center text-xs text-muted-foreground">{t('watch.form.loadingResources')}</p> : resources.error ? <p role="alert" className="py-3 text-xs text-destructive">{t('watch.form.resourcesFailed')}</p> : items.length ? <div className="mt-2 divide-y divide-border">{items.map((item, index) => <div key={`${item.resource_type}:${item.resource_id}:${index}`} className="flex min-w-0 items-center gap-3 py-2"><button type="button" className="min-w-0 flex-1 text-left" onClick={() => setBrowseParent({ resource_type: item.resource_type, resource_id: item.resource_id, ...(item.parent ? { parent: item.parent } : {}) })}><span className="block truncate text-sm font-medium">{item.display_name ?? item.resource_id}</span><span className="block truncate text-[11px] text-muted-foreground">{item.resource_type} / {item.resource_id}</span></button><Button type="button" variant={resource?.resource_id === item.resource_id && resource.resource_type === item.resource_type ? 'secondary' : 'outline'} size="sm" onClick={() => setResource({ resource_type: item.resource_type, resource_id: item.resource_id, ...(item.parent ? { parent: item.parent } : {}) })}>{resource?.resource_id === item.resource_id && resource.resource_type === item.resource_type ? t('watch.form.chosen') : t('watch.form.choose')}</Button></div>)}</div> : resources.data ? <p className="py-4 text-center text-xs text-muted-foreground">{t('watch.form.emptyResources')}</p> : <p className="py-4 text-center text-xs text-muted-foreground">{t('watch.form.searchPrompt')}</p>}
            {resources.hasNextPage && <Button type="button" variant="ghost" size="sm" className="mt-2" onClick={() => void resources.fetchNextPage()} disabled={resources.isFetchingNextPage}>{resources.isFetchingNextPage ? t('common.loading') : t('watch.form.loadMore')}</Button>}
          </div>}
        </div>}
      </CardContent>
    </Card>
    <Card><CardHeader><CardTitle>{t('watch.form.inputTitle')}</CardTitle></CardHeader><CardContent><SchemaForm
      schema={schema}
      initialValues={initialValues}
      initialSecrets={watch?.input_secret_configured}
      mode={watch ? 'watch' : 'input'}
      submitLabel={submitLabel}
      onSubmit={result => submit(result)}
      busy={busy}
    /></CardContent></Card>
  </div>
}

function safeInitialValues(schema: Schema, input: Record<string, unknown>, configuredSecrets?: Record<string, boolean>) {
  const secretKeys = new Set([...schema.fields.filter(field => field.control === 'secret').map(field => field.key), ...Object.keys(configuredSecrets ?? {})])
  return Object.fromEntries(Object.entries(input).filter(([key]) => !secretKeys.has(key)))
}
