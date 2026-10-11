import { useEffect, useState } from 'react'
import { Link, useParams, useSearch } from '@tanstack/react-router'
import { useInfiniteQuery, useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { ArrowLeft, Cable, CircleAlert, KeyRound, RefreshCw, Search, Settings2, Workflow } from 'lucide-react'
import { adaptersAPI } from '@/api'
import { adaptersQuery, qk, workflowsQuery } from '@/api/queries'
import { SchemaForm } from '@/components/schema-form'
import { PageHeading } from '@/components/page-heading'
import { StatusBadge } from '@/components/status-badge'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Confirm } from '@/components/ui/confirm'
import { EmptyState, ErrorState, LoadingState } from '@/components/query-state'
import { Input } from '@/components/ui/input'
import { useToast } from '@/components/ui/use-toast'
import { formatDate, resourceLabel } from '@/lib/utils'
import { supportsCapability } from '@/lib/capabilities'
import { ResourceBreadcrumbs } from '@/components/adapter/resource-breadcrumbs'
import { errorMessage } from '@/lib/errors'
import type { Resource, ResourceRef } from '@/types/api'
import { AdapterMark } from '@/components/adapter/adapter-mark'
import { PluginTrustBadges } from '@/components/plugin-trust'
import { adapterStateLabel, workflowStateLabel } from '@/lib/labels'
import { useI18n } from '@/i18n/provider'

type Tab = 'overview' | 'settings' | 'resources' | 'workflows'
export function AdapterDetailPage() {
  const { adapterId } = useParams({ from: '/adapters/$adapterId' })
  const routeSearch = useSearch({ from: '/adapters/$adapterId' })
  const { t } = useI18n()
  const client = useQueryClient()
  const { toast } = useToast()
  const [tab, setTab] = useState<Tab>(routeSearch.tab ?? 'overview')
  const [resource, setResource] = useState<ResourceRef>()
  const [q, setQ] = useState('')
  const [resourceType, setResourceType] = useState('')
  const [searchTerm, setSearchTerm] = useState('')
  const adapter = useQuery({ queryKey: qk.adapter(adapterId), queryFn: () => adaptersAPI.get(adapterId) })
  const list = useQuery(adaptersQuery)
  const current = adapter.data ?? list.data?.find(item => item.status.id === adapterId)
  const descriptor = current?.descriptor
  const config = useQuery({ queryKey: qk.config(adapterId, resource), queryFn: () => adaptersAPI.config(adapterId, resource), enabled: Boolean(descriptor && tab === 'settings'), retry: false })
  const workflows = useQuery(workflowsQuery)
  const canBrowse = supportsCapability(current, 'resource_browse')
  useEffect(() => { if (routeSearch.tab === 'resources' && canBrowse) setTab('resources'); else if (!adapter.isLoading && !canBrowse && tab === 'resources') setTab('overview') }, [adapter.isLoading, canBrowse, routeSearch.tab, tab])
  const resources = useInfiniteQuery({ queryKey: qk.resources(adapterId, { q: searchTerm, resource, resourceType }), queryFn: ({ pageParam }) => adaptersAPI.resources(adapterId, { q: searchTerm || undefined, parent: resource, resource_type: resourceType || undefined, cursor: pageParam, limit: 20 }), initialPageParam: undefined as string | undefined, getNextPageParam: page => page.next_cursor, enabled: Boolean(canBrowse && tab === 'resources' && descriptor), retry: false })
  const controls = useMutation({ mutationFn: (action: 'restart' | 'enable' | 'disable') => adaptersAPI.action(adapterId, action), onSuccess: result => { toast(t('adapter.detail.toastStatus', { state: adapterStateLabel(result.status.state) })); void client.invalidateQueries({ queryKey: qk.adapters }); void client.invalidateQueries({ queryKey: qk.adapter(adapterId) }) }, onError: error => toast(t('adapter.detail.toastActionFailed'), errorMessage(error), 'error') })
  const save = useMutation({ mutationFn: (body: { values: Record<string, unknown>; secrets: Record<string, string>; clear_values: string[]; clear_secrets: string[] }) => adaptersAPI.saveConfig(adapterId, { ...body, resource }), onSuccess: () => { toast(t('adapter.detail.toastSaved')); void client.invalidateQueries({ queryKey: qk.config(adapterId, resource) }) }, onError: error => toast(t('adapter.detail.toastSaveFailed'), errorMessage(error), 'error') })
  const allResourceTypes = descriptor?.resource_types ?? []
  const adapterWorkflows = workflows.data?.filter(item => item.adapter_id === adapterId) ?? []
  const resourceItems = resources.data?.pages.flatMap(page => page.items) ?? []
  if (adapter.isLoading) return <LoadingState />
  if (adapter.error || !current) return <ErrorState message={adapter.error ? errorMessage(adapter.error) : t('adapter.detail.missing')} retry={() => void adapter.refetch()} />
  const restarting = current.status.state === 'restarting'
  const controlsBusy = controls.isPending || restarting
  const canRestart = Boolean(descriptor) && current.status.state !== 'disabled' && current.status.state !== 'rejected' && !controlsBusy
  const canEnable = Boolean(descriptor) && current.status.state === 'disabled' && !controlsBusy
  const canDisable = Boolean(descriptor) && current.status.state !== 'disabled' && current.status.state !== 'rejected' && !controlsBusy
  const tabs: { key: Tab; label: string; icon: typeof Cable; unavailable?: boolean }[] = [
    { key: 'overview', label: t('adapter.detail.tab.overview'), icon: Cable },
    { key: 'settings', label: t('adapter.detail.tab.settings'), icon: Settings2 },
    { key: 'resources', label: t('adapter.detail.tab.resources'), icon: Search, unavailable: !canBrowse },
    { key: 'workflows', label: t('adapter.detail.tab.workflows'), icon: Workflow },
  ]
  return <div className="page-enter">
    <PageHeading title={<span className="flex min-w-0 items-center gap-3"><AdapterMark adapter={current} size="lg" /><span className="min-w-0 truncate">{descriptor?.name ?? current.status.name ?? adapterId}</span></span>} actions={<>
      <Link to="/adapters"><Button variant="outline" size="sm"><ArrowLeft className="h-4 w-4" />{t('adapter.detail.back')}</Button></Link>
      {descriptor && <>
        <Confirm trigger={<Button variant="outline" size="sm" disabled={!canRestart}><RefreshCw className="h-4 w-4" />{restarting ? t('adapter.detail.restarting') : t('adapter.detail.restart')}</Button>} title={t('adapter.detail.restartTitle')} description={t('adapter.detail.restartBody')} confirmLabel={t('adapter.detail.restart')} onConfirm={() => controls.mutate('restart')} disabled={!canRestart} />
        {current.status.state === 'disabled' ? <Confirm trigger={<Button size="sm" disabled={!canEnable}>{t('adapter.detail.enable')}</Button>} title={t('adapter.detail.enableTitle')} description={t('adapter.detail.enableBody')} confirmLabel={t('adapter.detail.enable')} onConfirm={() => controls.mutate('enable')} disabled={!canEnable} /> : <Confirm trigger={<Button variant="destructive" size="sm" disabled={!canDisable}>{t('adapter.detail.disable')}</Button>} title={t('adapter.detail.disableTitle')} description={t('adapter.detail.disableBody')} confirmLabel={t('adapter.detail.disable')} destructive onConfirm={() => controls.mutate('disable')} disabled={!canDisable} />}
      </>}
    </>} />
    <div className="mb-4 flex gap-1 overflow-x-auto border-b border-border" role="tablist" aria-label={descriptor?.name ?? adapterId}>{tabs.filter(item => !item.unavailable).map(item => <button key={item.key} id={`adapter-tab-${item.key}`} aria-controls={`adapter-panel-${item.key}`} role="tab" aria-selected={tab === item.key} tabIndex={tab === item.key ? 0 : -1} onClick={() => setTab(item.key)} className={`focus-ring inline-flex min-w-max items-center gap-2 border-b-2 px-3 py-2.5 text-sm ${tab === item.key ? 'border-primary font-semibold text-primary' : 'border-transparent text-muted-foreground hover:text-foreground'}`}><item.icon className="h-4 w-4" />{item.label}</button>)}</div>
    {tab === 'overview' && <div role="tabpanel" id="adapter-panel-overview" aria-labelledby="adapter-tab-overview" tabIndex={0} className="grid gap-4 xl:grid-cols-[minmax(0,1fr)_320px]">
      <Card><CardHeader><CardTitle>{t('adapter.detail.info')}</CardTitle></CardHeader><CardContent className="space-y-4">
        <div><p className="text-xs font-semibold">{t('adapter.detail.publisher')}</p><div className="mt-2"><PluginTrustBadges trust={current.trust} warning /></div>{current.trust?.provenance === 'registry' && current.trust.authority === 'official' && current.trust.reviewed && <p className="mt-2 text-xs text-muted-foreground">{t('adapter.detail.registryApprovedHelp')}</p>}</div>
        <div><p className="text-xs font-semibold">{t('adapter.detail.capabilities')}</p><div className="mt-2 flex flex-wrap gap-1.5">{descriptor?.capabilities?.map(cap => <Badge key={cap} tone="blue">{cap}</Badge>) ?? <span className="text-xs text-muted-foreground">{t('adapter.detail.unreported')}</span>}</div></div>
        {Boolean(descriptor?.media_types.length) && <div><p className="text-xs font-semibold">{t('adapter.detail.mediaTypes')}</p><div className="mt-2 flex flex-wrap gap-1.5">{descriptor?.media_types.map(type => <Badge key={type}>{type}</Badge>)}</div></div>}
        <details className="border-t border-border pt-3"><summary className="cursor-pointer text-xs font-medium">{t('adapter.detail.technicalDetails')}</summary><dl className="mt-3 grid grid-cols-2 gap-x-4 gap-y-3 sm:grid-cols-3">{[[t('adapter.detail.id'), descriptor?.id ?? current.status.id], [t('adapter.detail.version'), descriptor?.version ?? current.status.version ?? '—'], [t('adapter.detail.protocol'), `v${descriptor?.protocol_version ?? current.status.protocol_version ?? '—'}`], [t('adapter.detail.generation'), current.status.generation ?? '—'], [t('adapter.detail.restarts'), current.status.restart_attempts ?? 0]].map(([label, value]) => <Info key={String(label)} label={String(label)} value={String(value)} />)}</dl>{allResourceTypes.length > 0 && <div className="mt-4"><p className="text-xs font-semibold">{t('adapter.detail.resourceTypes')}</p><div className="mt-2 divide-y divide-border">{allResourceTypes.map(item => <div key={item.type} className="py-2"><p className="font-mono text-xs">{item.type}</p><p className="mt-1 text-[11px] text-muted-foreground">{t('adapter.detail.allowedParents')}: {item.parent_types?.join(', ') || t('adapter.detail.none')}</p></div>)}</div></div>}{!allResourceTypes.length && <p className="mt-3 text-xs text-muted-foreground">{t('adapter.detail.noResourceTypes')}</p>}</details>
      </CardContent></Card>
      <Card><CardHeader><CardTitle>{t('adapter.detail.runtime')}</CardTitle></CardHeader><CardContent><StatusBadge state={current.status.state} />{current.status.error && <p className="mt-3 flex items-start gap-2 rounded-md bg-amber-500/10 p-3 text-xs leading-5 text-amber-800 dark:text-amber-200"><CircleAlert className="mt-0.5 h-4 w-4 shrink-0" />{current.status.error}</p>}<p className="mt-3 text-xs text-muted-foreground">{t('adapter.detail.runtimeSecurity')}</p></CardContent></Card>
    </div>}
    {tab === 'settings' && <div role="tabpanel" id="adapter-panel-settings" aria-labelledby="adapter-tab-settings" tabIndex={0} className="grid gap-4 xl:grid-cols-[minmax(0,1fr)_320px]"><Card><CardHeader><CardTitle>{t('adapter.detail.settings')}</CardTitle><p className="text-xs text-muted-foreground">{t('adapter.detail.secretHelp')}</p></CardHeader><CardContent>{config.isLoading ? <LoadingState /> : config.error || !config.data ? <ErrorState message={config.error ? errorMessage(config.error) : t('adapter.detail.settingsUnavailable')} retry={() => void config.refetch()} /> : config.data.schema.fields.length ? <SchemaForm schema={config.data.schema} initialValues={config.data.effective.values} storedValues={config.data.stored.values} initialSecrets={config.data.effective.secrets} valueSources={config.data.value_sources} secretSources={config.data.secret_sources} mode="config" submitLabel={t('adapter.detail.saveSettings')} onSubmit={result => save.mutate({ values: result.values, secrets: result.secrets, clear_values: result.clearValues, clear_secrets: result.clearSecrets })} busy={save.isPending} /> : <EmptyState title={t('adapter.detail.noSettings')} />}</CardContent></Card><ConfigScopeSelector adapterId={adapterId} resource={resource} onSelect={setResource} canBrowse={canBrowse} /></div>}
    {tab === 'resources' && canBrowse && <Card role="tabpanel" id="adapter-panel-resources" aria-labelledby="adapter-tab-resources" tabIndex={0}><CardHeader><CardTitle>{t('adapter.detail.resources')}</CardTitle></CardHeader><CardContent><ResourceBreadcrumbs resource={resource} onNavigate={setResource} /><div className="mt-3 grid gap-2 sm:grid-cols-[1fr_190px_auto]"><div className="relative"><Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" /><Input className="pl-9" value={q} onChange={event => setQ(event.target.value)} onKeyDown={event => event.key === 'Enter' && setSearchTerm(q)} placeholder={t('adapter.detail.searchName')} aria-label={t('adapter.detail.searchName')} /></div><Input value={resourceType} onChange={event => setResourceType(event.target.value)} placeholder={t('adapter.detail.resourceType')} aria-label={t('adapter.detail.resourceType')} /><Button onClick={() => setSearchTerm(q)}><Search className="h-4 w-4" />{t('adapter.detail.search')}</Button></div>{resource && <p className="mt-3 text-xs text-muted-foreground">{t('adapter.detail.parent')}: {resourceLabel(resource)}</p>}{resources.isLoading ? <LoadingState label={t('adapter.detail.loading')} /> : resources.error ? <ErrorState message={errorMessage(resources.error)} retry={() => void resources.refetch()} /> : resourceItems.length ? <div className="mt-4 divide-y divide-border">{resourceItems.map((item, index) => <div key={`${item.resource_type}-${item.resource_id}-${index}`} className="flex flex-wrap items-center gap-3 py-3"><div className="min-w-0 flex-1"><p className="truncate text-sm font-medium">{item.display_name ?? item.resource_id}</p><p className="mt-1 truncate font-mono text-xs text-muted-foreground">{item.resource_type} / {item.resource_id}</p></div><Button variant="outline" size="sm" onClick={() => setResource({ resource_type: item.resource_type, resource_id: item.resource_id, parent: item.parent })}>{t('adapter.detail.browseChildren')}</Button><Button size="sm" onClick={() => { setResource({ resource_type: item.resource_type, resource_id: item.resource_id, parent: item.parent }); setTab('settings') }}>{t('adapter.detail.useScope')}</Button></div>)}</div> : resources.data ? <EmptyState title={t('adapter.detail.emptyResources')} description={t('adapter.detail.emptyResourceHelp')} /> : null}{resources.hasNextPage && <Button variant="outline" size="sm" className="mt-3" onClick={() => void resources.fetchNextPage()} disabled={resources.isFetchingNextPage}>{resources.isFetchingNextPage ? t('common.loading') : t('adapter.detail.more')}</Button>}</CardContent></Card>}
    {tab === 'workflows' && <Card role="tabpanel" id="adapter-panel-workflows" aria-labelledby="adapter-tab-workflows" tabIndex={0}><CardHeader><CardTitle>{t('adapter.detail.workflows')}</CardTitle></CardHeader><CardContent>{adapterWorkflows.length ? <div className="divide-y divide-border">{adapterWorkflows.map(workflow => <Link key={workflow.workflow_id} to="/workflows/$workflowId" params={{ workflowId: workflow.workflow_id }} className="flex items-center justify-between gap-3 py-3"><span><span className="block text-sm font-medium">{workflow.workflow_id}</span><span className="mt-1 block text-xs text-muted-foreground">{workflowStateLabel(workflow.state)} · {t('adapter.detail.expires')} {formatDate(workflow.expires_at)}</span></span><StatusBadge state={workflow.state} /></Link>)}</div> : <EmptyState title={t('adapter.detail.emptyWorkflows')} />}</CardContent></Card>}
  </div>
}

function Info({ label, value }: { label: string; value: string | number }) { return <div><dt className="text-[11px] text-muted-foreground">{label}</dt><dd className="mt-1 break-all text-sm font-medium">{value}</dd></div> }

function ConfigScopeSelector({ adapterId, resource, onSelect, canBrowse }: { adapterId: string; resource?: ResourceRef; onSelect: (resource?: ResourceRef) => void; canBrowse: boolean }) {
  const { t } = useI18n()
  const [q, setQ] = useState('')
  const [search, setSearch] = useState('')
  const [resourceType, setResourceType] = useState('')
  const query = useQuery({ queryKey: qk.resources(adapterId, { search, resourceType }), queryFn: () => adaptersAPI.resources(adapterId, { q: search || undefined, resource_type: resourceType || undefined, limit: 20 }), enabled: Boolean(canBrowse && search.length >= 2), retry: false })
  return <Card><CardHeader><CardTitle className="flex items-center gap-2"><KeyRound className="h-4 w-4" />{t('adapter.detail.scope')}</CardTitle><p className="text-xs text-muted-foreground">{t('adapter.detail.currentScope')}: {resource ? resourceLabel(resource) : t('adapter.detail.commonScope')}</p></CardHeader><CardContent><Button variant={!resource ? 'secondary' : 'outline'} size="sm" onClick={() => onSelect(undefined)}>{t('adapter.detail.setCommonScope')}</Button>{canBrowse && <><div className="mt-3 flex gap-2"><Input value={q} onChange={event => setQ(event.target.value)} placeholder={t('adapter.detail.searchResources')} aria-label={t('adapter.detail.searchResources')} /><Button variant="outline" size="sm" onClick={() => setSearch(q)}>{t('adapter.detail.search')}</Button></div><Input className="mt-2" value={resourceType} onChange={event => setResourceType(event.target.value)} placeholder={t('adapter.detail.resourceTypeOptional')} aria-label={t('adapter.detail.resourceTypeOptional')} />{query.data?.items.map((item: Resource, index: number) => <button key={`${item.resource_type}-${item.resource_id}-${index}`} className="mt-2 block w-full rounded-md border border-border p-2 text-left hover:bg-muted" onClick={() => onSelect({ resource_type: item.resource_type, resource_id: item.resource_id, parent: item.parent })}><span className="block truncate text-xs font-medium">{item.display_name ?? item.resource_id}</span><span className="block truncate text-[10px] text-muted-foreground">{item.resource_type}</span></button>)}</>}<details className="mt-3 border-t border-border pt-3"><summary className="cursor-pointer text-[11px] font-medium">{t('adapter.detail.scopeBehavior')}</summary><p className="mt-2 text-[11px] leading-4 text-muted-foreground">{t('adapter.detail.scopeBehaviorHelp')}</p></details></CardContent></Card>
}
