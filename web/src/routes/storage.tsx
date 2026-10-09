import { useState, type ReactNode } from 'react'
import { Link, useParams } from '@tanstack/react-router'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { ArrowLeft, ArrowRight, Check, Database, Gauge, HardDrive, Layers3, RefreshCw, Server, ShieldCheck, Timer, TriangleAlert, Waves } from 'lucide-react'
import { storageMetricsQuery, storagePoolsQuery } from '@/api/queries'
import { storageProvidersAPI, type StorageMetricWindow } from '@/api'
import type { Schema, StorageInstanceCreateBody, StorageInstanceSummary, StoragePool, StorageProviderConfigBody, StorageProviderSummary, StorageProviderStatus } from '@/types/api'
import { MetricChart } from '@/components/storage/metric-chart'
import { EmptyState, ErrorState, LoadingState } from '@/components/query-state'
import { PageHeading } from '@/components/page-heading'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Select, SelectItem } from '@/components/ui/select'
import { errorMessage } from '@/lib/errors'
import { formatBytes, formatDuration } from '@/lib/utils'
import { formatNumber } from '@/lib/formatting'
import { SchemaForm } from '@/components/schema-form'
import { Dialog, DialogContent, DialogDescription, DialogTitle } from '@/components/ui/dialog'
import { useToast } from '@/components/ui/use-toast'
import { Input } from '@/components/ui/input'
import { useI18n } from '@/i18n/provider'

export function StoragePage() {
  const { t } = useI18n()
  const pools = useQuery(storagePoolsQuery)
  return <div className="page-enter">
    <PageHeading eyebrow={t('storage.pageEyebrow')} title={t('storage.pageTitle')} description={t('storage.pageDescription')} actions={<Button variant="outline" onClick={() => void pools.refetch()}><RefreshCw className="h-4 w-4" />{t('storage.refresh')}</Button>} />
    <StorageProviderManagement />
    {pools.isLoading ? <LoadingState label={t('storage.loading')} /> : pools.error ? <ErrorState message={errorMessage(pools.error)} retry={() => void pools.refetch()} /> : <StoragePoolList pools={pools.data?.items ?? []} />}
  </div>
}

function StorageProviderManagement() {
  const { t } = useI18n()
  const client = useQueryClient()
  const queryKey = ['runtime-storage-provider'] as const
  const query = useQuery({ queryKey, queryFn: storageProvidersAPI.status, staleTime: 5_000 })
  const retry = () => void query.refetch()
  return <section aria-labelledby="primary-storage-title" className="mb-8 space-y-4">
    <Card>
      <CardHeader className="flex-row flex-wrap items-start justify-between gap-3 border-b border-border/70">
        <div><CardTitle id="primary-storage-title" className="flex items-center gap-2"><HardDrive className="h-4 w-4 text-primary" />{t('storage.primary')}</CardTitle><p className="mt-1 text-xs text-muted-foreground">{t('storage.primaryHelp')}</p></div>
        <Button variant="outline" onClick={() => { retry(); void client.invalidateQueries({ queryKey }) }} disabled={query.isFetching}><RefreshCw className={`h-4 w-4 ${query.isFetching ? 'animate-spin' : ''}`} />{t('storage.refresh')}</Button>
      </CardHeader>
      <CardContent className="pt-4">
        {query.isLoading ? <LoadingState label={t('storage.primaryLoading')} /> : query.error ? <div role="alert" className="space-y-2"><p className="text-sm text-muted-foreground">{t('storage.primaryFailed')}</p><Button variant="outline" onClick={retry}>{t('storage.retry')}</Button></div> : query.data ? <PrimaryStorageSummary status={query.data} /> : null}
      </CardContent>
    </Card>
    {query.data && <div className="space-y-3">
      <div><h2 className="text-base font-semibold">{t('storage.providers')}</h2><p className="mt-1 text-xs text-muted-foreground">{t('storage.providerHelp')}</p></div>
      {query.data.providers.length ? <div className="grid min-w-0 gap-4 xl:grid-cols-2">{query.data.providers.map(provider => <StorageProviderCard key={provider.id} provider={provider} />)}</div> : <EmptyState title={t('storage.noProviders')} description={t('storage.noProvidersHelp')} />}
    </div>}
    {query.data && <StorageInstanceManagement providers={query.data.providers} />}
  </section>
}

function localInstanceSchema(t: ReturnType<typeof useI18n>['t']): Schema { return { fields: [{ key: 'root', control: 'text', label: t('storage.localRoot'), required: true, constraints: { max_length: 4096 } }] } }

function StorageInstanceManagement({ providers }: { providers: StorageProviderSummary[] }) {
  const { t } = useI18n()
  const client = useQueryClient()
  const { toast } = useToast()
  const queryKey = ['storage-instances'] as const
  const instances = useQuery({ queryKey, queryFn: storageProvidersAPI.instances, staleTime: 5_000 })
  const [creating, setCreating] = useState(false)
  const [displayName, setDisplayName] = useState('')
  const [providerID, setProviderID] = useState(providers[0]?.id ?? '')
  const provider = providers.find(item => item.id === providerID)
  const schema = provider?.id === 'local' ? localInstanceSchema(t) : provider?.configuration_schema
  const invalidate = async () => Promise.all([
    client.invalidateQueries({ queryKey }),
    client.invalidateQueries({ queryKey: ['runtime-storage-provider'] }),
    client.invalidateQueries({ queryKey: ['storage', 'pools'] }),
  ])
  const create = useMutation({
    mutationFn: (body: StorageInstanceCreateBody) => storageProvidersAPI.createInstance(body),
    onSuccess: async () => { setCreating(false); setDisplayName(''); toast(t('storage.createSuccess')); await invalidate() },
  })
  return <section aria-labelledby="storage-instances-title" className="space-y-3">
    <div className="flex flex-wrap items-center justify-between gap-3"><div><h2 id="storage-instances-title" className="text-base font-semibold">{t('storage.instances')}</h2><p className="mt-1 text-xs text-muted-foreground">{t('storage.instancesHelp')}</p></div><Button variant="outline" onClick={() => setCreating(value => !value)}>{creating ? t('storage.instanceClose') : t('storage.instanceAdd')}</Button></div>
    {creating && <Card><CardHeader><CardTitle className="text-base">{t('storage.instanceCreate')}</CardTitle><p className="text-xs text-muted-foreground">{t('storage.instanceCreateHelp')}</p></CardHeader><CardContent className="space-y-4">
      <label className="block space-y-2"><span className="text-sm font-medium">{t('storage.displayName')}</span><Input value={displayName} onChange={event => setDisplayName(event.target.value)} maxLength={128} placeholder={t('storage.instancePlaceholder')} /></label>
      <label className="block space-y-2"><span className="text-sm font-medium">{t('storage.provider')}</span><select className="h-9 w-full rounded-md border border-input bg-background px-3 text-sm" value={providerID} onChange={event => setProviderID(event.target.value)} aria-label={t('storage.provider')}>{providers.map(item => <option key={item.id} value={item.id}>{item.name} · {item.id}</option>)}</select></label>
      {schema?.fields.length ? <SchemaForm key={`instance-create-${providerID}`} schema={schema} mode="input" submitLabel={t('storage.instanceCreate')} busy={create.isPending} onSubmit={({ values, secrets }) => create.mutate({ display_name: displayName, provider_id: providerID, values, secrets })} /> : <p className="text-sm text-muted-foreground">{t('storage.instanceNoFields')}</p>}
      {create.error && <p role="alert" className="text-sm text-destructive">{storageActionError(create.error, t)}</p>}
    </CardContent></Card>}
    {instances.isLoading ? <LoadingState label={t('storage.instanceLoading')} /> : instances.error ? <div role="alert" className="space-y-2"><p className="text-sm text-muted-foreground">{t('storage.instanceLoadFailed')}</p><Button variant="outline" onClick={() => void instances.refetch()}>{t('storage.retry')}</Button></div> : instances.data?.length ? <div className="grid min-w-0 gap-4 xl:grid-cols-2">{instances.data.map(instance => <StorageInstanceCard key={instance.id} instance={instance} provider={providers.find(item => item.id === instance.provider_id)} active={instance.active} onChanged={invalidate} />)}</div> : <EmptyState title={t('storage.noInstances')} description={t('storage.noInstancesHelp')} />}
  </section>
}

function StorageInstanceCard({ instance, provider, active, onChanged }: { instance: StorageInstanceSummary; provider?: StorageProviderSummary; active: boolean; onChanged: () => Promise<unknown> }) {
  const { t } = useI18n()
  const client = useQueryClient()
  const { toast } = useToast()
  const [confirmOpen, setConfirmOpen] = useState(false)
  const [message, setMessage] = useState('')
  const configKey = ['storage-instance-config', instance.id] as const
  const config = useQuery({ queryKey: configKey, queryFn: () => storageProvidersAPI.instanceConfig(instance.id), staleTime: 10_000 })
  const schema = instance.provider_id === 'local' ? localInstanceSchema(t) : provider?.configuration_schema
  const invalidate = async () => Promise.all([onChanged(), client.invalidateQueries({ queryKey: configKey })])
  const save = useMutation({ mutationFn: (body: StorageProviderConfigBody) => storageProvidersAPI.saveInstanceConfig(instance.id, body), onSuccess: async () => { setMessage(t('storage.savedSuccess')); toast(t('storage.saveConfigSuccess')); await invalidate() }, onError: error => setMessage(storageActionError(error, t)) })
  const probe = useMutation({ mutationFn: () => storageProvidersAPI.probeInstance(instance.id), onSuccess: () => { setMessage(t('storage.probeSuccess')); toast(t('storage.probeToast')) }, onError: error => setMessage(storageActionError(error, t)) })
  const activate = useMutation({ mutationFn: () => storageProvidersAPI.activateInstance(instance.id), onSuccess: async () => { setConfirmOpen(false); setMessage(t('storage.activateGenerationSuccess')); toast(t('storage.activateToast')); await invalidate() }, onError: error => { setConfirmOpen(false); setMessage(storageActionError(error, t)) } })
  const busy = save.isPending || probe.isPending || activate.isPending
  const configuredSecrets = Object.fromEntries((config.data?.configured_secrets ?? []).map(name => [name, true]))
  return <Card className="min-w-0">
    <CardHeader className="flex-row flex-wrap items-start justify-between gap-3 border-b border-border/70"><div className="min-w-0"><CardTitle className="truncate text-base">{instance.display_name}</CardTitle><p className="mt-1 font-mono text-[11px] text-muted-foreground">{instance.provider_name} · {instance.provider_id}</p></div><div className="flex flex-wrap gap-1.5"><Badge tone={active ? 'blue' : 'neutral'}>{active ? t('storage.defaultBadge') : t('storage.savedBadge')}</Badge><Badge tone={instance.health === 'ready' ? 'green' : instance.health === 'unavailable' ? 'red' : 'amber'}>{instance.health === 'ready' ? t('storage.providerReady') : instance.health === 'unavailable' ? t('storage.providerUpdateRequired') : t('storage.needsCheck')}</Badge></div></CardHeader>
    <CardContent className="space-y-4 pt-4">{!provider ? <p role="status" className="text-sm text-muted-foreground">{t('storage.providerArtifactMissing')}</p> : !schema?.fields.length ? <p className="text-sm text-muted-foreground">{t('storage.noConfigFields')}</p> : config.isLoading ? <LoadingState label={t('storage.instanceConfigLoading')} /> : config.error ? <p role="alert" className="text-sm text-muted-foreground">{t('storage.configLoadFailed')}</p> : <SchemaForm key={`${instance.id}-${JSON.stringify(config.data?.values ?? {})}-${(config.data?.configured_secrets ?? []).join(',')}`} schema={schema} initialValues={config.data?.values} initialSecrets={configuredSecrets} mode="config" submitLabel={t('storage.saveNewConfig')} busy={busy} onSubmit={({ values, secrets }) => { setMessage(''); save.mutate({ values, secrets }) }} />}
      <div className="flex flex-wrap gap-2 border-t border-border/70 pt-4"><Button variant="outline" disabled={!provider || instance.health === 'unavailable' || busy} onClick={() => { setMessage(''); probe.mutate() }}>{probe.isPending ? t('storage.checking') : t('storage.checkConnection')}</Button><Button disabled={!provider || instance.health === 'unavailable' || active || busy} onClick={() => { setMessage(''); setConfirmOpen(true) }}>{active ? <><Check className="h-4 w-4" />{t('storage.default')}</> : t('storage.activateAsDefault')}</Button></div>
      {message && <p role={message === t('storage.savedSuccess') || message === t('storage.probeSuccess') || message === t('storage.activateGenerationSuccess') ? 'status' : 'alert'} className="text-sm text-muted-foreground">{message}</p>}
      <Dialog open={confirmOpen} onOpenChange={open => { if (!activate.isPending) setConfirmOpen(open) }}><DialogContent aria-describedby={`storage-instance-activate-${instance.id}`}><DialogTitle>{t('storage.confirmActivateTitle')}</DialogTitle><DialogDescription id={`storage-instance-activate-${instance.id}`} className="mt-2 text-sm leading-6 text-muted-foreground">{t('storage.confirmActivateBody', { name: instance.display_name })}</DialogDescription><div className="mt-5 flex justify-end gap-2"><Button variant="outline" disabled={activate.isPending} onClick={() => setConfirmOpen(false)}>{t('storage.cancel')}</Button><Button disabled={activate.isPending} onClick={() => activate.mutate()}>{activate.isPending ? t('storage.activating') : t('storage.confirmActivate')}</Button></div></DialogContent></Dialog>
    </CardContent>
  </Card>
}

function PrimaryStorageSummary({ status }: { status: StorageProviderStatus }) {
  const { t } = useI18n()
  const provider = status.providers.find(item => item.id === status.primary.provider_id)
  const label = status.primary.provider_id === 'local' ? 'Local Storage' : provider?.name ?? status.primary.provider_id
  return <div className="flex flex-wrap items-center justify-between gap-3">
    <div><p className="text-sm font-semibold">{label} · {status.primary.version}</p><p className="mt-1 text-xs text-muted-foreground">Provider ID: {status.primary.provider_id}</p></div>
    <Badge tone={status.primary.state === 'ready' ? 'green' : 'amber'}>{status.primary.state === 'ready' ? t('storage.primaryReady') : t('storage.primaryUnavailable')}</Badge>
  </div>
}

function StorageProviderCard({ provider }: { provider: StorageProviderSummary }) {
  const { t } = useI18n()
  const client = useQueryClient()
  const { toast } = useToast()
  const [confirmOpen, setConfirmOpen] = useState(false)
  const [actionMessage, setActionMessage] = useState('')
  const configKey = ['storage-provider-config', provider.id] as const
  const configQuery = useQuery({ queryKey: configKey, queryFn: () => storageProvidersAPI.config(provider.id), staleTime: 10_000, enabled: !provider.configuration_managed })
  const invalidate = async () => {
    await Promise.all([
      client.invalidateQueries({ queryKey: ['runtime-storage-provider'] }),
      client.invalidateQueries({ queryKey: configKey }),
      client.invalidateQueries({ queryKey: ['storage', 'pools'] }),
    ])
  }
  const save = useMutation({
    mutationFn: (body: StorageProviderConfigBody) => storageProvidersAPI.saveConfig(provider.id, body),
    onSuccess: async () => { setActionMessage(t('storage.configSaved')); toast(t('storage.configSavedToast')); await invalidate() },
    onError: error => setActionMessage(storageActionError(error, t)),
  })
  const probe = useMutation({
    mutationFn: () => storageProvidersAPI.probe(provider.id),
    onSuccess: async () => { setActionMessage(t('storage.probeSuccess')); toast(t('storage.probeProviderToast')); await invalidate() },
    onError: error => setActionMessage(storageActionError(error, t)),
  })
  const activate = useMutation({
    mutationFn: () => storageProvidersAPI.activate(provider.id),
    onSuccess: async () => { setConfirmOpen(false); setActionMessage(t('storage.providerGenerationActivated')); toast(t('storage.defaultChanged')); await invalidate() },
    onError: error => { setConfirmOpen(false); setActionMessage(storageActionError(error, t)) },
  })
  const busy = save.isPending || probe.isPending || activate.isPending
  const configuredSecrets = Object.fromEntries((configQuery.data?.configured_secrets ?? []).map(name => [name, true]))
  const bundled = provider.distribution === 'bundled'
  return <Card className="min-w-0">
    <CardHeader className="flex-row flex-wrap items-start justify-between gap-3 border-b border-border/70">
      <div className="min-w-0"><CardTitle className="truncate text-base">{provider.id === 'local' ? 'Local Storage' : provider.name}</CardTitle><p className="mt-1 font-mono text-[11px] text-muted-foreground">{provider.id} · {provider.version}</p></div>
      <div className="flex flex-wrap gap-1.5">{bundled && <><Badge tone="neutral">{t('plugins.trust.bundled')}</Badge><Badge tone="green">{t('storage.providerInstalled')}</Badge></>}<Badge tone={provider.configured ? 'green' : 'amber'}>{provider.configured ? t('storage.providerConfigured') : t('storage.providerNeedsConfig')}</Badge><Badge tone={healthTone(provider.health)}>{healthLabel(provider.health, t)}</Badge>{provider.active && <Badge tone="blue">{t('storage.default')}</Badge>}</div>
    </CardHeader>
    <CardContent className="space-y-5 pt-4">
      {provider.configuration_managed ? <p className="text-sm text-muted-foreground">{t('storage.providerManagedConfig')}</p> : configQuery.isLoading ? <LoadingState label={t('storage.providerConfigLoading')} /> : configQuery.error ? <div role="alert" className="space-y-2"><p className="text-sm text-muted-foreground">{t('storage.instanceConfigFailed')}</p><Button variant="outline" onClick={() => void configQuery.refetch()}>{t('storage.retry')}</Button></div> : <SchemaForm
        key={`${provider.id}-${JSON.stringify(configQuery.data?.values ?? {})}-${(configQuery.data?.configured_secrets ?? []).join(',')}`}
        schema={provider.configuration_schema}
        initialValues={configQuery.data?.values}
        initialSecrets={configuredSecrets}
        mode="config"
        submitLabel={t('storage.saveProviderConfig')}
        busy={busy}
        onSubmit={({ values, secrets }) => { setActionMessage(''); save.mutate({ values, secrets }) }}
      />}
      <div className="flex flex-wrap gap-2 border-t border-border/70 pt-4">
        <Button variant="outline" disabled={!provider.configured || busy} onClick={() => { setActionMessage(''); probe.mutate() }}>{probe.isPending ? t('storage.checking') : t('storage.checkConnection')}</Button>
        <Button disabled={provider.active || !provider.configured || provider.health !== 'ready' || busy} onClick={() => { setActionMessage(''); setConfirmOpen(true) }}>{provider.active ? <><Check className="h-4 w-4" />{t('storage.default')}</> : t('storage.activateAsDefault')}</Button>
      </div>
      {!provider.configuration_managed && !provider.configured && <p className="text-xs text-muted-foreground">{t('storage.configBeforeActivate')}</p>}
      {actionMessage && <p role={actionMessage === t('storage.configSaved') || actionMessage === t('storage.probeSuccess') || actionMessage === t('storage.providerGenerationActivated') ? 'status' : 'alert'} className="text-sm text-muted-foreground">{actionMessage}</p>}
      <Dialog open={confirmOpen} onOpenChange={open => { if (!activate.isPending) setConfirmOpen(open) }}>
        <DialogContent aria-describedby={`storage-activate-description-${provider.id}`}>
          <DialogTitle>{t('storage.confirmDefaultTitle')}</DialogTitle>
          <DialogDescription id={`storage-activate-description-${provider.id}`} className="mt-2 text-sm leading-6 text-muted-foreground">{t('storage.confirmDefaultBody', { name: provider.name })}</DialogDescription>
          <div className="mt-5 flex justify-end gap-2"><Button variant="outline" disabled={activate.isPending} onClick={() => setConfirmOpen(false)}>{t('storage.cancel')}</Button><Button disabled={activate.isPending} onClick={() => activate.mutate()}>{activate.isPending ? t('storage.activating') : t('storage.confirmActivate')}</Button></div>
        </DialogContent>
      </Dialog>
    </CardContent>
  </Card>
}

function healthTone(health: StorageProviderSummary['health']): 'green' | 'amber' | 'red' | 'neutral' {
  return health === 'ready' ? 'green' : health === 'failed' ? 'red' : 'neutral'
}
function healthLabel(health: StorageProviderSummary['health'], t: ReturnType<typeof useI18n>['t']) {
  return health === 'ready' ? t('state.ready') : health === 'failed' ? t('state.failed') : t('storage.needsCheck')
}
function storageActionError(error: unknown, t: ReturnType<typeof useI18n>['t']): string {
  return errorMessage(error) || t('errors.server')
}

export function StoragePoolList({ pools }: { pools: StoragePool[] }) {
  const { t } = useI18n()
  if (!pools.length) return <EmptyState title={t('storage.pool.none')} description={t('storage.pool.noneHelp')} />
  return <div className="grid min-w-0 gap-4 xl:grid-cols-2">{pools.map(pool => <StoragePoolCard key={pool.id} pool={pool} />)}</div>
}

export function StoragePoolCard({ pool }: { pool: StoragePool }) {
  const { t } = useI18n()
  const ratio = boundedRatio(pool.capacity.usage_ratio)
  return <Card className="min-w-0 overflow-hidden">
    <CardHeader className="flex-row items-start justify-between gap-4 border-b border-border/70">
      <div className="flex min-w-0 items-start gap-3"><span className="grid h-10 w-10 shrink-0 place-items-center rounded-lg bg-primary/10 text-primary"><HardDrive className="h-5 w-5" /></span><div className="min-w-0"><Link to="/storage/$poolId" params={{ poolId: pool.id }} className="focus-ring rounded-sm"><CardTitle className="truncate text-base" title={pool.display_name}>{pool.display_name}</CardTitle></Link><p className="mt-1 truncate font-mono text-[11px] text-muted-foreground">{pool.id} · {kindLabel(pool.kind, t)} · {roleLabel(pool.role, t)}</p></div></div>
      <PoolHealth health={pool.health} />
    </CardHeader>
    <CardContent className="grid gap-5 pt-4 sm:grid-cols-2">
      <section aria-label={t('storage.pool.filesystemCapacity')} className="min-w-0">{pool.capacity_known ? <><div className="mb-2 flex items-baseline justify-between gap-2"><span className="text-xs font-medium text-muted-foreground">{t('storage.pool.filesystemCapacity')}</span><span className="text-xs tabular-nums">{formatBytes(pool.capacity.used_bytes)} / {formatBytes(pool.capacity.total_bytes)}</span></div><progress className="storage-progress" value={ratio} max={1} aria-label={t('storage.pool.usage', { percent: formatNumber(ratio * 100, { maximumFractionDigits: 0 }) })} /><div className="mt-2 flex justify-between text-[11px] text-muted-foreground"><span>{t('storage.pool.usage', { percent: formatNumber(ratio * 100, { maximumFractionDigits: 1 }) })}</span><span>{formatBytes(pool.capacity.available_bytes)} {t('storage.pool.available')}</span></div></> : <div className="flex items-baseline justify-between gap-2"><span className="text-xs font-medium text-muted-foreground">{t('storage.pool.filesystemCapacity')}</span><span className="text-xs text-muted-foreground">{t('storage.pool.unknownCapacity')}</span></div>}</section>
      <div className="grid grid-cols-2 gap-3">
        <MetricValue icon={<Gauge className="h-3.5 w-3.5" />} label={t('storage.pool.recorderRead')} value={formatRate(pool.throughput.read_bytes_per_second)} />
        <MetricValue icon={<Waves className="h-3.5 w-3.5" />} label={t('storage.pool.recorderWrite')} value={formatRate(pool.throughput.write_bytes_per_second)} />
        <MetricValue icon={<Layers3 className="h-3.5 w-3.5" />} label={t('storage.pool.captureBuffer')} value={`${formatBytes(pool.buffer.used_bytes)} / ${formatBytes(pool.buffer.capacity_bytes)}`} />
        <MetricValue icon={<Database className="h-3.5 w-3.5" />} label={t('storage.pool.writeQueue')} value={`${t('storage.pool.objects', { count: pool.queue.objects })} · ${formatBytes(pool.queue.bytes)}`} />
      </div>
      <div className="flex min-w-0 flex-wrap items-center justify-between gap-x-4 gap-y-2 border-t border-border/70 pt-3 text-xs sm:col-span-2">
        <CeilingSummary pool={pool} />
        <span className="text-muted-foreground">{t('storage.pool.oldestWait')} {formatDuration(pool.queue.oldest_age_seconds)} · {t('storage.pool.writers')} {pool.writers.active}/{pool.writers.limit}</span>
        <Link to="/storage/$poolId" params={{ poolId: pool.id }} className="ml-auto inline-flex items-center gap-1 font-medium text-primary">{t('storage.pool.details')} <ArrowRight className="h-3.5 w-3.5" /></Link>
      </div>
    </CardContent>
  </Card>
}

export function StoragePoolDetailPage() {
  const { t } = useI18n()
  const { poolId } = useParams({ from: '/storage/$poolId' })
  const [window, setWindow] = useState<StorageMetricWindow>('1h')
  const pools = useQuery(storagePoolsQuery)
  const pool = pools.data?.items.find(item => item.id === poolId)
  const metrics = useQuery(storageMetricsQuery(poolId, window, !pools.isLoading && !pools.error && Boolean(pool)))

  return <div className="page-enter">
    <PageHeading eyebrow={t('storage.pool.pageEyebrow')} title={pool?.display_name ?? poolId} description={pool ? `${pool.id} · ${kindLabel(pool.kind, t)} · ${roleLabel(pool.role, t)}` : t('storage.pool.pageDescription')} actions={<><Select aria-label={t('storage.pool.timeWindow')} value={window} onValueChange={value => setWindow(value as StorageMetricWindow)} className="w-28"><SelectItem value="1h">{t('storage.pool.last1h')}</SelectItem><SelectItem value="6h">{t('storage.pool.last6h')}</SelectItem><SelectItem value="24h">{t('storage.pool.last24h')}</SelectItem></Select><Link to="/storage"><Button variant="outline"><ArrowLeft className="h-4 w-4" />{t('storage.pool.list')}</Button></Link></>} />
    {pools.isLoading ? <LoadingState label={t('storage.loading')} /> : pools.error ? <ErrorState message={errorMessage(pools.error)} retry={() => void pools.refetch()} /> : !pool ? <EmptyState title={t('storage.pool.notFound')} description={t('storage.pool.notFoundHelp')} /> : <>
      <PoolDetailSummary pool={pool} />
      <div className="mt-5 flex items-center justify-between gap-3"><h2 className="text-base font-semibold">{t('storage.pool.recentMetrics')}</h2>{metrics.data && <span className="text-xs text-muted-foreground">{t('storage.pool.sampleInterval', { seconds: formatNumber(metrics.data.sample_interval_seconds) })}</span>}</div>
      <div className="mt-3 grid gap-4 xl:grid-cols-2">
        {metrics.isLoading ? <Card><CardContent className="pt-5"><LoadingState label={t('storage.pool.metricsLoading')} /></CardContent></Card> : metrics.error ? <div className="xl:col-span-2"><ErrorState message={errorMessage(metrics.error)} retry={() => void metrics.refetch()} /></div> : metrics.data?.items.length ? <>
          <Card><CardContent className="pt-5"><MetricChart title={t('storage.pool.readWriteChart')} samples={metrics.data.items} kind="throughput" pool={pool} /></CardContent></Card>
          <Card><CardContent className="pt-5"><MetricChart title={t('storage.pool.backlogChart')} samples={metrics.data.items} kind="backlog" /></CardContent></Card>
        </> : <div className="xl:col-span-2"><EmptyState title={t('storage.pool.noMetrics')} description={t('storage.pool.noMetricsHelp')} /></div>}
      </div>
    </>}
  </div>
}

function PoolDetailSummary({ pool }: { pool: StoragePool }) {
  const { t } = useI18n()
  const ratio = boundedRatio(pool.capacity.usage_ratio)
  return <div className="grid gap-4 xl:grid-cols-[1.1fr_1fr]">
    <Card><CardHeader className="flex-row items-center justify-between"><CardTitle className="flex items-center gap-2"><HardDrive className="h-4 w-4 text-primary" />{t('storage.pool.status')}</CardTitle><PoolHealth health={pool.health} /></CardHeader><CardContent className="space-y-4"><div>{pool.capacity_known ? <><div className="flex justify-between gap-3 text-xs"><span className="text-muted-foreground">{t('storage.pool.filesystemUsage')}</span><span className="tabular-nums">{formatBytes(pool.capacity.used_bytes)} / {formatBytes(pool.capacity.total_bytes)}</span></div><progress className="storage-progress mt-2" value={ratio} max={1} aria-label={t('storage.pool.usagePercent', { percent: formatNumber(ratio * 100, { maximumFractionDigits: 0 }) })} /><p className="mt-1 text-right text-[11px] text-muted-foreground">{formatBytes(pool.capacity.available_bytes)} {t('storage.pool.available')} · {t('storage.pool.usage', { percent: formatNumber(ratio * 100, { maximumFractionDigits: 1 }) })}</p></> : <p className="text-sm text-muted-foreground">{t('storage.pool.filesystemCapacity')}: {t('storage.pool.unknownCapacity')}</p>}</div><dl className="grid grid-cols-2 gap-4 border-t border-border pt-4 sm:grid-cols-4">{[[t('storage.pool.type'), kindLabel(pool.kind, t)], [t('storage.pool.role'), roleLabel(pool.role, t)], [t('storage.pool.totalRead'), formatBytes(pool.throughput.read_bytes_total)], [t('storage.pool.totalWrite'), formatBytes(pool.throughput.write_bytes_total)]].map(([label, value]) => <div key={label}><dt className="text-[11px] text-muted-foreground">{label}</dt><dd className="mt-1 break-words text-sm font-medium tabular-nums">{value}</dd></div>)}</dl></CardContent></Card>
    <Card><CardHeader><CardTitle className="flex items-center gap-2"><Server className="h-4 w-4 text-primary" />{t('storage.pool.queueState')}</CardTitle></CardHeader><CardContent className="grid grid-cols-2 gap-x-5 gap-y-4 sm:grid-cols-3">{[
      [t('storage.pool.recorderRead'), formatRate(pool.throughput.read_bytes_per_second)], [t('storage.pool.recorderWrite'), formatRate(pool.throughput.write_bytes_per_second)], [t('storage.pool.readLatency'), `${formatNumber(pool.throughput.read_latency_ms, { maximumFractionDigits: 1 })} ms`], [t('storage.pool.writeLatency'), `${formatNumber(pool.throughput.write_latency_ms, { maximumFractionDigits: 1 })} ms`],
      [t('storage.pool.captureBuffer'), `${formatBytes(pool.buffer.used_bytes)} / ${formatBytes(pool.buffer.capacity_bytes)}`], [t('storage.pool.writeQueue'), `${t('storage.pool.objects', { count: pool.queue.objects })} · ${formatBytes(pool.queue.bytes)}`], [t('storage.pool.oldestWait'), formatDuration(pool.queue.oldest_age_seconds)],
      [t('storage.pool.activeWriters'), `${formatNumber(pool.writers.active)} / ${formatNumber(pool.writers.limit)}`], [t('storage.pool.errorCount'), formatNumber(pool.errors_total)],
    ].map(([label, value]) => <div key={label} className="min-w-0"><p className="text-[11px] text-muted-foreground">{label}</p><p className="mt-1 truncate text-sm font-semibold tabular-nums" title={value}>{value}</p></div>)}<div className="col-span-2 border-t border-border pt-3 sm:col-span-3"><CeilingSummary pool={pool} /></div></CardContent></Card>
  </div>
}

function MetricValue({ icon, label, value }: { icon: ReactNode; label: string; value: string }) { return <div className="min-w-0"><div className="flex items-center gap-1.5 text-[10px] text-muted-foreground">{icon}{label}</div><p className="mt-1 truncate text-xs font-semibold tabular-nums" title={value}>{value}</p></div> }
function PoolHealth({ health }: { health: string }) {
  const { t } = useI18n()
  const appearance = health === 'healthy' ? { label: t('storage.pool.health.healthy'), tone: 'green' as const, icon: ShieldCheck } : health === 'degraded' ? { label: t('storage.pool.health.degraded'), tone: 'amber' as const, icon: TriangleAlert } : health === 'unavailable' ? { label: t('storage.pool.health.unavailable'), tone: 'red' as const, icon: TriangleAlert } : { label: health || t('storage.pool.health.unknown'), tone: 'neutral' as const, icon: Timer }
  const Icon = appearance.icon
  return <Badge tone={appearance.tone}><Icon className="h-3 w-3" />{appearance.label}</Badge>
}
function CeilingSummary({ pool }: { pool: StoragePool }) {
  const { t } = useI18n()
  const ceiling = pool.estimated_ceiling
  const source = ceiling.source
  if (source === 'unknown' || (!ceiling.read_bytes_per_second && !ceiling.write_bytes_per_second)) return <span className="inline-flex items-center gap-1.5 text-xs text-muted-foreground"><Gauge className="h-3.5 w-3.5" />{t('storage.pool.ceiling.unknown')}</span>
  const sourceLabel = source === 'observed' ? t('storage.pool.ceiling.observed') : source === 'configured' ? t('storage.pool.ceiling.configured') : source === 'benchmarked' ? t('storage.pool.ceiling.benchmarked') : t('storage.pool.ceiling.default')
  const parts = [ceiling.read_bytes_per_second ? t('storage.pool.readRate', { rate: formatRate(ceiling.read_bytes_per_second) }) : '', ceiling.write_bytes_per_second ? t('storage.pool.writeRate', { rate: formatRate(ceiling.write_bytes_per_second) }) : ''].filter(Boolean)
  return <span className="inline-flex flex-wrap items-center gap-x-1.5 text-xs"><Gauge className="h-3.5 w-3.5 text-muted-foreground" /><span className="text-muted-foreground">{sourceLabel}:</span><span className="font-medium tabular-nums">{parts.join(' · ')}</span></span>
}
function boundedRatio(value: number) { return Number.isFinite(value) ? Math.min(1, Math.max(0, value)) : 0 }
function formatRate(value: number) { return `${formatBytes(value)}/s` }
function roleLabel(role: string, t: ReturnType<typeof useI18n>['t']) { return role === 'primary' ? t('storage.pool.role.primary') : role }
function kindLabel(kind: string, t: ReturnType<typeof useI18n>['t']) { return kind === 'local' ? t('storage.pool.kind.local') : kind }
