import { Link } from '@tanstack/react-router'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { ArrowRight, CircleAlert, Download, PackageOpen, RefreshCw, Trash2, Upload } from 'lucide-react'
import { adaptersAPI, pluginsAPI } from '@/api'
import { accountQuery, adaptersQuery, pluginsQuery, qk } from '@/api/queries'
import { PageHeading } from '@/components/page-heading'
import { errorMessage } from '@/lib/errors'
import { StatusBadge } from '@/components/status-badge'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { EmptyState, ErrorState, LoadingState } from '@/components/query-state'
import type { Adapter, PluginRegistryStatus } from '@/types/api'
import { AdapterMark } from '@/components/adapter/adapter-mark'
import { PluginTrustBadges } from '@/components/plugin-trust'
import { useToast } from '@/components/ui/use-toast'
import { useI18n } from '@/i18n/provider'

export function AdaptersPage() {
  const { t } = useI18n()
  const query = useQuery(adaptersQuery)
  const account = useQuery(accountQuery)
  const owner = account.data?.role === 'owner'
  const client = useQueryClient()
  const { toast } = useToast()
  const reconcile = useMutation({
    mutationFn: adaptersAPI.reconcile,
    onSuccess: async result => {
      await client.invalidateQueries({ queryKey: qk.adapters })
      if (result.state === 'rejected') toast(t('plugins.scanRejected'), t('plugins.invalidCandidates', { count: result.rejected_count }), 'info')
      else if (result.state === 'failed') toast(t('plugins.scanFailed'), t('plugins.activePreserved'), 'error')
      else if (result.rejected_count > 0) toast(t('plugins.generationActivated'), t('plugins.invalidCandidates', { count: result.rejected_count }), 'info')
      else toast(result.state === 'activated' ? t('plugins.generationActivated') : t('plugins.current'))
    },
    onError: error => toast(t('plugins.scanFailed'), errorMessage(error), 'error'),
  })
  const actions = <div className="flex flex-wrap gap-2">{owner && <Button variant="outline" disabled={reconcile.isPending} onClick={() => reconcile.mutate()}><RefreshCw className={`h-4 w-4 ${reconcile.isPending ? 'animate-spin' : ''}`} />{t('plugins.rescan')}</Button>}<Button variant="ghost" onClick={() => void query.refetch()}><RefreshCw className="h-4 w-4" />{t('plugins.refresh')}</Button></div>
  const unreviewedInstalled = query.data?.some(adapter => isUnreviewed(adapter.trust)) ?? false
  return <div className="page-enter space-y-4">
    <PageHeading title={t('nav.sources')} actions={actions} />
    {unreviewedInstalled ? <div role="note" className="flex items-start gap-2 rounded-md border border-amber-300/60 bg-amber-50/60 px-3 py-2 text-xs text-amber-900 dark:border-amber-800 dark:bg-amber-950/30 dark:text-amber-100"><CircleAlert className="mt-0.5 h-3.5 w-3.5 shrink-0" />{t('plugins.trust.pageWarning')}</div> : null}
    {query.isLoading ? <LoadingState /> : query.error ? <ErrorState message={errorMessage(query.error)} retry={() => void query.refetch()} /> : query.data?.length ? <div className="overflow-hidden rounded-lg border border-border bg-card"><div className="divide-y divide-border">{query.data.map(adapter => <AdapterRow key={adapter.status.id} adapter={adapter} />)}</div></div> : <EmptyState title={t('plugins.empty')} />}
  </div>
}

export function PluginsPage() {
  const { t } = useI18n()
  return <div className="page-enter space-y-4"><PageHeading title={t('nav.plugins')} /><PluginRegistrySection /></div>
}

function isUnreviewed(trust: Adapter['trust']): boolean {
  return trust?.reviewed === false && (trust.provenance === 'operator' || (trust.provenance === 'registry' && trust.authority === 'custom'))
}

function PluginRegistrySection() {
  const { t } = useI18n()
  const query = useQuery(pluginsQuery)
  const client = useQueryClient()
  const { toast } = useToast()
  const invalidate = async () => {
    await Promise.all([client.invalidateQueries({ queryKey: qk.plugins }), client.invalidateQueries({ queryKey: qk.adapters })])
  }
  const refresh = useMutation({
    mutationFn: pluginsAPI.refresh,
    onSuccess: async status => { await invalidate(); toast(status.state === 'ready' ? t('plugins.registry.ready') : t('plugins.registry.failed'), status.state === 'ready' ? undefined : t('plugins.registry.unavailable'), status.state === 'ready' ? 'success' : 'info') },
    onError: async error => { await client.invalidateQueries({ queryKey: qk.plugins }); toast(t('plugins.registry.refreshFailed'), errorMessage(error), 'error') },
  })
  const install = useMutation({
    mutationFn: ({ id }: { id: string; type: 'source' | 'storage' }) => pluginsAPI.install(id),
    onSuccess: async (_result, plugin) => { await invalidate(); toast(plugin.type === 'storage' ? t('plugins.installStorageSuccess') : t('plugins.installSourceSuccess'), plugin.type === 'storage' ? t('plugins.configureAfterInstall') : undefined) },
    onError: error => toast(t('plugins.installFailed'), errorMessage(error), 'error'),
  })
  const update = useMutation({
    mutationFn: ({ id }: { id: string; type: 'source' | 'storage' }) => pluginsAPI.update(id),
    onSuccess: async (_result, plugin) => { await invalidate(); toast(plugin.type === 'storage' ? t('plugins.updateStorageSuccess') : t('plugins.updateSourceSuccess'), plugin.type === 'storage' ? t('plugins.defaultUnchanged') : t('plugins.oldGenerationPinned')) },
    onError: error => toast(t('plugins.updateFailed'), errorMessage(error), 'error'),
  })
  const uninstall = useMutation({
    mutationFn: ({ id }: { id: string; type: 'source' | 'storage' }) => pluginsAPI.uninstall(id),
    onSuccess: async (_result, plugin) => { await invalidate(); toast(plugin.type === 'storage' ? t('plugins.removeStorageSuccess') : t('plugins.removeSourceSuccess'), t('plugins.referencedFilesPreserved')) },
    onError: error => toast(t('plugins.removeFailed'), errorMessage(error), 'error'),
  })
  const busy = refresh.isPending || install.isPending || update.isPending || uninstall.isPending
  const status = query.data as PluginRegistryStatus | undefined
  const sourcePlugins = status?.plugins.filter(plugin => plugin.type !== 'storage') ?? []
  const storagePlugins = status?.plugins.filter(plugin => plugin.type === 'storage') ?? []
  const notConfigured = status?.state === 'not_configured'
  return <section aria-labelledby="plugin-registry-title" className="overflow-hidden rounded-lg border border-border bg-card">
    <div className="flex flex-wrap items-center justify-between gap-3 border-b border-border px-4 py-3">
      <div className="flex items-center gap-2"><PackageOpen className="h-4 w-4 text-primary" /><h2 id="plugin-registry-title" className="text-sm font-semibold">{t('plugins.registry.title')}</h2></div>
      <Button variant="outline" disabled={busy || notConfigured} onClick={() => refresh.mutate()}><RefreshCw className={`h-4 w-4 ${refresh.isPending ? 'animate-spin' : ''}`} />{t('plugins.registry.refresh')}</Button>
    </div>
    {query.isPending ? <div className="p-4"><LoadingState /></div> : query.error ? <div className="p-4"><ErrorState message={errorMessage(query.error)} retry={() => void query.refetch()} /></div> : <>
      {notConfigured && <div role="status" className="flex items-start gap-2 border-b border-border bg-muted/30 px-4 py-3 text-sm text-muted-foreground"><CircleAlert className="mt-0.5 h-4 w-4 shrink-0" />{t('plugins.registry.notConfigured')}</div>}
      {status?.state === 'unavailable' && <div role="status" className="flex items-start gap-2 border-b border-border bg-muted/30 px-4 py-3 text-sm text-muted-foreground"><CircleAlert className="mt-0.5 h-4 w-4 shrink-0" />{t('plugins.registry.unavailable')}</div>}
      <PluginGroup title={t('plugins.group.sources')} plugins={sourcePlugins} busy={busy} onInstall={plugin => install.mutate(plugin)} onUpdate={plugin => update.mutate(plugin)} onUninstall={plugin => uninstall.mutate(plugin)} registryState={status?.state} />
      <div className="border-t border-border">
        <PluginGroup title={t('plugins.group.storage')} description={t('plugins.group.storageHelp')} plugins={storagePlugins} busy={busy} onInstall={plugin => install.mutate(plugin)} onUpdate={plugin => update.mutate(plugin)} onUninstall={plugin => uninstall.mutate(plugin)} registryState={status?.state} storage />
        <div className="px-4 pb-4"><Link to="/storage/locations" className="text-xs font-medium text-primary hover:underline">{t('plugins.storageSettings')} <ArrowRight className="inline h-3 w-3" /></Link></div>
      </div>
    </>}
  </section>
}

type RegistryPlugin = PluginRegistryStatus['plugins'][number]
function PluginGroup({ title, description, plugins, busy, onInstall, onUpdate, onUninstall, registryState, storage = false }: {
  title: string; description?: string; plugins: RegistryPlugin[]; busy: boolean;
  onInstall: (plugin: RegistryPlugin) => void; onUpdate: (plugin: RegistryPlugin) => void;
  onUninstall: (plugin: RegistryPlugin) => void; registryState?: PluginRegistryStatus['state']; storage?: boolean
}) {
  const { t } = useI18n()
  return <section aria-label={title}>
    <div className="px-4 pb-2 pt-4"><h3 className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">{title}</h3>{description && <p className="mt-1 text-xs text-muted-foreground">{description}</p>}</div>
    {plugins.length ? <div className="divide-y divide-border">{plugins.map(plugin => <div key={plugin.id} className="flex flex-wrap items-center justify-between gap-3 px-4 py-3">
      <div className="min-w-0"><div className="flex items-center gap-2"><span className="truncate text-sm font-medium">{plugin.name}</span>{plugin.installed && <Badge tone="green">{t('plugins.installed')}</Badge>}</div><p className="mt-1 truncate font-mono text-[11px] text-muted-foreground">{plugin.id} · {plugin.installed ? `${t('plugins.installed')} ${plugin.installed_version ?? t('plugins.unknownVersion')}` : t('plugins.notInstalled')}{plugin.available_version ? ` · ${t('plugins.availableVersion', { version: plugin.available_version })}` : ''}</p><div className="mt-1.5"><PluginTrustBadges trust={plugin.trust} compact /></div></div>
      <div className="flex flex-wrap gap-2">{!plugin.installed && plugin.available_version && <Button size="sm" disabled={busy || registryState !== 'ready'} onClick={() => onInstall(plugin)}><Download className="h-3.5 w-3.5" />{t('plugins.install')}</Button>}{plugin.installed && plugin.update_available && <Button size="sm" variant="outline" disabled={busy || registryState !== 'ready'} onClick={() => onUpdate(plugin)}><Upload className="h-3.5 w-3.5" />{t('plugins.update')}</Button>}{plugin.installed && <Button size="sm" variant="ghost" disabled={busy} onClick={() => onUninstall(plugin)}><Trash2 className="h-3.5 w-3.5" />{t('plugins.uninstall')}</Button>}</div>
    </div>)}</div> : registryState === 'ready' ? <p className="px-4 pb-4 text-sm text-muted-foreground">{t('plugins.noCandidates', { kind: storage ? t('plugins.group.storage') : t('plugins.group.sources') })}</p> : null}
  </section>
}

function AdapterRow({ adapter }: { adapter: Adapter }) {
  const { t } = useI18n()
  const descriptor = adapter.descriptor
  const name = descriptor?.name ?? adapter.status.name ?? adapter.status.id
  return <Link to="/sources/$adapterId" params={{ adapterId: adapter.status.id }} search={{ tab: undefined }} className="flex min-w-0 items-center justify-between gap-3 px-4 py-3 transition-colors hover:bg-muted/30">
    <span className="flex min-w-0 items-center gap-3"><AdapterMark adapter={adapter} size="md" /><span className="min-w-0"><span className="block truncate text-sm font-semibold" title={name}>{name}</span>{adapter.status.error && <span className="mt-1 flex items-center gap-1 text-xs text-muted-foreground"><CircleAlert className="h-3 w-3" />{t('plugins.statusDetails')}</span>}</span></span>
    <span className="flex shrink-0 items-center gap-2"><StatusBadge state={adapter.status.state} /><ArrowRight className="h-3.5 w-3.5 text-muted-foreground" /></span>
  </Link>
}
