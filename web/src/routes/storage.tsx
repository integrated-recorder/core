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
import { formatBytes } from '@/lib/utils'
import { SchemaForm } from '@/components/schema-form'
import { Dialog, DialogContent, DialogDescription, DialogTitle } from '@/components/ui/dialog'
import { useToast } from '@/components/ui/use-toast'
import { Input } from '@/components/ui/input'

export function StoragePage() {
  const pools = useQuery(storagePoolsQuery)
  return <div className="page-enter">
    <PageHeading eyebrow="운영 현황" title="저장소" description="기본 보관 저장소와 설치된 storage provider를 관리하고, 저장 풀의 용량과 읽기·쓰기 상태를 확인합니다." actions={<Button variant="outline" onClick={() => void pools.refetch()}><RefreshCw className="h-4 w-4" />새로고침</Button>} />
    <StorageProviderManagement />
    {pools.isLoading ? <LoadingState label="저장 풀을 불러오는 중입니다" /> : pools.error ? <ErrorState message={errorMessage(pools.error)} retry={() => void pools.refetch()} /> : <StoragePoolList pools={pools.data?.items ?? []} />}
  </div>
}

function StorageProviderManagement() {
  const client = useQueryClient()
  const queryKey = ['runtime-storage-provider'] as const
  const query = useQuery({ queryKey, queryFn: storageProvidersAPI.status, staleTime: 5_000 })
  const retry = () => void query.refetch()
  return <section aria-labelledby="primary-storage-title" className="mb-8 space-y-4">
    <Card>
      <CardHeader className="flex-row flex-wrap items-start justify-between gap-3 border-b border-border/70">
        <div><CardTitle id="primary-storage-title" className="flex items-center gap-2"><HardDrive className="h-4 w-4 text-primary" />기본 보관 저장소</CardTitle><p className="mt-1 text-xs text-muted-foreground">Core가 archive 형식과 기록 의미를 관리하고, provider는 객체의 물리적 저장과 조회를 담당합니다.</p></div>
        <Button variant="outline" onClick={() => { retry(); void client.invalidateQueries({ queryKey }) }} disabled={query.isFetching}><RefreshCw className={`h-4 w-4 ${query.isFetching ? 'animate-spin' : ''}`} />새로고침</Button>
      </CardHeader>
      <CardContent className="pt-4">
        {query.isLoading ? <LoadingState label="기본 저장소 상태를 불러오는 중입니다" /> : query.error ? <div role="alert" className="space-y-2"><p className="text-sm text-muted-foreground">저장소 상태를 불러오지 못했습니다. 내부 경로와 provider 오류 세부 정보는 표시하지 않습니다.</p><Button variant="outline" onClick={retry}>다시 시도</Button></div> : query.data ? <PrimaryStorageSummary status={query.data} /> : null}
      </CardContent>
    </Card>
    {query.data && <div className="space-y-3">
      <div><h2 className="text-base font-semibold">설치된 Storage Provider</h2><p className="mt-1 text-xs text-muted-foreground">설정, 연결 검사, 기본 저장소 활성화는 각각 별도 단계입니다. 저장소 전환은 기존 archive를 이동하지 않으며 archive가 비어 있지 않으면 거부될 수 있습니다.</p></div>
      {query.data.providers.length ? <div className="grid min-w-0 gap-4 xl:grid-cols-2">{query.data.providers.map(provider => <StorageProviderCard key={provider.id} provider={provider} />)}</div> : <EmptyState title="설치된 storage provider가 없습니다." description="Plugin Registry에서 provider를 설치할 수 있습니다. 설치만으로는 기본 저장소가 바뀌지 않습니다." />}
    </div>}
    {query.data && <StorageInstanceManagement providers={query.data.providers} />}
  </section>
}

const localInstanceSchema: Schema = { fields: [{ key: 'root', control: 'text', label: 'Archive root', required: true, constraints: { max_length: 4096 } }] }

function StorageInstanceManagement({ providers }: { providers: StorageProviderSummary[] }) {
  const client = useQueryClient()
  const { toast } = useToast()
  const queryKey = ['storage-instances'] as const
  const instances = useQuery({ queryKey, queryFn: storageProvidersAPI.instances, staleTime: 5_000 })
  const [creating, setCreating] = useState(false)
  const [displayName, setDisplayName] = useState('')
  const [providerID, setProviderID] = useState(providers[0]?.id ?? '')
  const provider = providers.find(item => item.id === providerID)
  const schema = provider?.id === 'local' ? localInstanceSchema : provider?.configuration_schema
  const invalidate = async () => Promise.all([
    client.invalidateQueries({ queryKey }),
    client.invalidateQueries({ queryKey: ['runtime-storage-provider'] }),
    client.invalidateQueries({ queryKey: ['storage', 'pools'] }),
  ])
  const create = useMutation({
    mutationFn: (body: StorageInstanceCreateBody) => storageProvidersAPI.createInstance(body),
    onSuccess: async () => { setCreating(false); setDisplayName(''); toast('저장소 인스턴스를 만들었습니다.'); await invalidate() },
  })
  return <section aria-labelledby="storage-instances-title" className="space-y-3">
    <div className="flex flex-wrap items-center justify-between gap-3"><div><h2 id="storage-instances-title" className="text-base font-semibold">저장소 인스턴스</h2><p className="mt-1 text-xs text-muted-foreground">같은 provider로 여러 설정을 저장합니다. 활성화한 세대는 선택한 immutable 설정을 고정합니다.</p></div><Button variant="outline" onClick={() => setCreating(value => !value)}>{creating ? '만들기 닫기' : '인스턴스 추가'}</Button></div>
    {creating && <Card><CardHeader><CardTitle className="text-base">저장소 인스턴스 만들기</CardTitle><p className="text-xs text-muted-foreground">자격 증명은 쓰기 전용으로 저장합니다. 새 인스턴스는 별도 활성화 전까지 기본 저장소를 바꾸지 않습니다.</p></CardHeader><CardContent className="space-y-4">
      <label className="block space-y-2"><span className="text-sm font-medium">표시 이름</span><Input value={displayName} onChange={event => setDisplayName(event.target.value)} maxLength={128} placeholder="예: SSD archive" /></label>
      <label className="block space-y-2"><span className="text-sm font-medium">Storage Provider</span><select className="h-9 w-full rounded-md border border-input bg-background px-3 text-sm" value={providerID} onChange={event => setProviderID(event.target.value)} aria-label="Storage Provider">{providers.map(item => <option key={item.id} value={item.id}>{item.name} · {item.id}</option>)}</select></label>
      {schema?.fields.length ? <SchemaForm key={`instance-create-${providerID}`} schema={schema} mode="input" submitLabel="인스턴스 만들기" busy={create.isPending} onSubmit={({ values, secrets }) => create.mutate({ display_name: displayName, provider_id: providerID, values, secrets })} /> : <p className="text-sm text-muted-foreground">선택한 provider에 설정 항목이 없습니다.</p>}
      {create.error && <p role="alert" className="text-sm text-destructive">{storageActionError(create.error)}</p>}
    </CardContent></Card>}
    {instances.isLoading ? <LoadingState label="저장소 인스턴스를 불러오는 중입니다" /> : instances.error ? <div role="alert" className="space-y-2"><p className="text-sm text-muted-foreground">저장소 인스턴스를 불러오지 못했습니다.</p><Button variant="outline" onClick={() => void instances.refetch()}>다시 시도</Button></div> : instances.data?.length ? <div className="grid min-w-0 gap-4 xl:grid-cols-2">{instances.data.map(instance => <StorageInstanceCard key={instance.id} instance={instance} provider={providers.find(item => item.id === instance.provider_id)} active={instance.active} onChanged={invalidate} />)}</div> : <EmptyState title="저장소 인스턴스가 없습니다." description="Provider 설정마다 이름이 있는 인스턴스를 만들 수 있습니다." />}
  </section>
}

function StorageInstanceCard({ instance, provider, active, onChanged }: { instance: StorageInstanceSummary; provider?: StorageProviderSummary; active: boolean; onChanged: () => Promise<unknown> }) {
  const client = useQueryClient()
  const { toast } = useToast()
  const [confirmOpen, setConfirmOpen] = useState(false)
  const [message, setMessage] = useState('')
  const configKey = ['storage-instance-config', instance.id] as const
  const config = useQuery({ queryKey: configKey, queryFn: () => storageProvidersAPI.instanceConfig(instance.id), staleTime: 10_000 })
  const schema = instance.provider_id === 'local' ? localInstanceSchema : provider?.configuration_schema
  const invalidate = async () => Promise.all([onChanged(), client.invalidateQueries({ queryKey: configKey })])
  const save = useMutation({ mutationFn: (body: StorageProviderConfigBody) => storageProvidersAPI.saveInstanceConfig(instance.id, body), onSuccess: async () => { setMessage('새 immutable 설정을 저장했습니다. 기존 세대는 이전 설정을 계속 사용합니다.'); toast('저장소 인스턴스 설정을 저장했습니다.'); await invalidate() }, onError: error => setMessage(storageActionError(error)) })
  const probe = useMutation({ mutationFn: () => storageProvidersAPI.probeInstance(instance.id), onSuccess: () => { setMessage('연결 및 읽기·쓰기 검사가 성공했습니다.'); toast('저장소 인스턴스 연결 검사를 통과했습니다.') }, onError: error => setMessage(storageActionError(error)) })
  const activate = useMutation({ mutationFn: () => storageProvidersAPI.activateInstance(instance.id), onSuccess: async () => { setConfirmOpen(false); setMessage('새 storage generation을 활성화했습니다.'); toast('저장소 인스턴스를 활성화했습니다.'); await invalidate() }, onError: error => { setConfirmOpen(false); setMessage(storageActionError(error)) } })
  const busy = save.isPending || probe.isPending || activate.isPending
  const configuredSecrets = Object.fromEntries((config.data?.configured_secrets ?? []).map(name => [name, true]))
  return <Card className="min-w-0">
    <CardHeader className="flex-row flex-wrap items-start justify-between gap-3 border-b border-border/70"><div className="min-w-0"><CardTitle className="truncate text-base">{instance.display_name}</CardTitle><p className="mt-1 font-mono text-[11px] text-muted-foreground">{instance.provider_name} · {instance.provider_id}</p></div><div className="flex flex-wrap gap-1.5"><Badge tone={active ? 'blue' : 'neutral'}>{active ? '기본 저장소' : '저장됨'}</Badge><Badge tone={instance.health === 'ready' ? 'green' : instance.health === 'unavailable' ? 'red' : 'amber'}>{instance.health === 'ready' ? '준비됨' : instance.health === 'unavailable' ? 'Provider 업데이트 필요' : '검사 필요'}</Badge></div></CardHeader>
    <CardContent className="space-y-4 pt-4">{!provider ? <p role="status" className="text-sm text-muted-foreground">현재 provider artifact가 설치되어 있지 않습니다. 기존 세대가 고정한 immutable 설정은 보존됩니다.</p> : !schema?.fields.length ? <p className="text-sm text-muted-foreground">이 provider에는 설정 항목이 없습니다.</p> : config.isLoading ? <LoadingState label="인스턴스 설정을 불러오는 중입니다" /> : config.error ? <p role="alert" className="text-sm text-muted-foreground">인스턴스 설정을 불러오지 못했습니다.</p> : <SchemaForm key={`${instance.id}-${JSON.stringify(config.data?.values ?? {})}-${(config.data?.configured_secrets ?? []).join(',')}`} schema={schema} initialValues={config.data?.values} initialSecrets={configuredSecrets} mode="config" submitLabel="새 설정 저장" busy={busy} onSubmit={({ values, secrets }) => { setMessage(''); save.mutate({ values, secrets }) }} />}
      <div className="flex flex-wrap gap-2 border-t border-border/70 pt-4"><Button variant="outline" disabled={!provider || instance.health === 'unavailable' || busy} onClick={() => { setMessage(''); probe.mutate() }}>{probe.isPending ? '검사 중…' : '연결 검사'}</Button><Button disabled={!provider || instance.health === 'unavailable' || active || busy} onClick={() => { setMessage(''); setConfirmOpen(true) }}>{active ? <><Check className="h-4 w-4" />기본 저장소</> : '기본 저장소로 활성화'}</Button></div>
      {message && <p role={message.includes('성공') || message.includes('저장했습니다') || message.includes('활성화했습니다') ? 'status' : 'alert'} className="text-sm text-muted-foreground">{message}</p>}
      <Dialog open={confirmOpen} onOpenChange={open => { if (!activate.isPending) setConfirmOpen(open) }}><DialogContent aria-describedby={`storage-instance-activate-${instance.id}`}><DialogTitle>이 저장소 인스턴스를 활성화할까요?</DialogTitle><DialogDescription id={`storage-instance-activate-${instance.id}`} className="mt-2 text-sm leading-6 text-muted-foreground">새 generation은 {instance.display_name}의 현재 immutable 설정을 고정합니다. 기존 녹화는 이전 storage generation을 계속 사용합니다.</DialogDescription><div className="mt-5 flex justify-end gap-2"><Button variant="outline" disabled={activate.isPending} onClick={() => setConfirmOpen(false)}>취소</Button><Button disabled={activate.isPending} onClick={() => activate.mutate()}>{activate.isPending ? '활성화 중…' : '확인 후 활성화'}</Button></div></DialogContent></Dialog>
    </CardContent>
  </Card>
}

function PrimaryStorageSummary({ status }: { status: StorageProviderStatus }) {
  const provider = status.providers.find(item => item.id === status.primary.provider_id)
  const label = status.primary.provider_id === 'local' ? 'Local Storage' : provider?.name ?? status.primary.provider_id
  return <div className="flex flex-wrap items-center justify-between gap-3">
    <div><p className="text-sm font-semibold">{label} · {status.primary.version}</p><p className="mt-1 text-xs text-muted-foreground">Provider ID: {status.primary.provider_id}</p></div>
    <Badge tone={status.primary.state === 'ready' ? 'green' : 'amber'}>{status.primary.state === 'ready' ? '활성 · 준비됨' : '활성 저장소 사용 불가'}</Badge>
  </div>
}

function StorageProviderCard({ provider }: { provider: StorageProviderSummary }) {
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
    onSuccess: async () => { setActionMessage('설정을 저장했습니다. 이제 연결을 검사할 수 있습니다.'); toast('Storage provider 설정을 저장했습니다.'); await invalidate() },
    onError: error => setActionMessage(storageActionError(error)),
  })
  const probe = useMutation({
    mutationFn: () => storageProvidersAPI.probe(provider.id),
    onSuccess: async () => { setActionMessage('연결 및 읽기·쓰기 검사가 성공했습니다.'); toast('Storage provider 연결 검사를 통과했습니다.'); await invalidate() },
    onError: error => setActionMessage(storageActionError(error)),
  })
  const activate = useMutation({
    mutationFn: () => storageProvidersAPI.activate(provider.id),
    onSuccess: async () => { setConfirmOpen(false); setActionMessage('새 storage provider generation을 활성화했습니다.'); toast('기본 저장소를 변경했습니다.'); await invalidate() },
    onError: error => { setConfirmOpen(false); setActionMessage(storageActionError(error)) },
  })
  const busy = save.isPending || probe.isPending || activate.isPending
  const configuredSecrets = Object.fromEntries((configQuery.data?.configured_secrets ?? []).map(name => [name, true]))
  const bundled = provider.distribution === 'bundled'
  return <Card className="min-w-0">
    <CardHeader className="flex-row flex-wrap items-start justify-between gap-3 border-b border-border/70">
      <div className="min-w-0"><CardTitle className="truncate text-base">{provider.id === 'local' ? 'Local Storage' : provider.name}</CardTitle><p className="mt-1 font-mono text-[11px] text-muted-foreground">{provider.id} · {provider.version}</p></div>
      <div className="flex flex-wrap gap-1.5">{bundled && <><Badge tone="neutral">Bundled</Badge><Badge tone="green">설치됨</Badge></>}<Badge tone={provider.configured ? 'green' : 'amber'}>{provider.configured ? '설정됨' : '설정 필요'}</Badge><Badge tone={healthTone(provider.health)}>{healthLabel(provider.health)}</Badge>{provider.active && <Badge tone="blue">기본 저장소</Badge>}</div>
    </CardHeader>
    <CardContent className="space-y-5 pt-4">
      {provider.configuration_managed ? <p className="text-sm text-muted-foreground">이 provider의 설정은 Runtime Host가 관리합니다. 별도의 경로나 자격 증명 설정은 필요하지 않습니다.</p> : configQuery.isLoading ? <LoadingState label="Provider 설정 양식을 불러오는 중입니다" /> : configQuery.error ? <div role="alert" className="space-y-2"><p className="text-sm text-muted-foreground">설정 양식을 불러오지 못했습니다.</p><Button variant="outline" onClick={() => void configQuery.refetch()}>다시 시도</Button></div> : <SchemaForm
        key={`${provider.id}-${JSON.stringify(configQuery.data?.values ?? {})}-${(configQuery.data?.configured_secrets ?? []).join(',')}`}
        schema={provider.configuration_schema}
        initialValues={configQuery.data?.values}
        initialSecrets={configuredSecrets}
        mode="config"
        submitLabel="설정 저장"
        busy={busy}
        onSubmit={({ values, secrets }) => { setActionMessage(''); save.mutate({ values, secrets }) }}
      />}
      <div className="flex flex-wrap gap-2 border-t border-border/70 pt-4">
        <Button variant="outline" disabled={!provider.configured || busy} onClick={() => { setActionMessage(''); probe.mutate() }}>{probe.isPending ? '검사 중…' : '연결 검사'}</Button>
        <Button disabled={provider.active || !provider.configured || provider.health !== 'ready' || busy} onClick={() => { setActionMessage(''); setConfirmOpen(true) }}>{provider.active ? <><Check className="h-4 w-4" />기본 저장소</> : '기본 저장소로 활성화'}</Button>
      </div>
      {!provider.configuration_managed && !provider.configured && <p className="text-xs text-muted-foreground">설정을 저장한 뒤 연결 검사를 통과해야 활성화할 수 있습니다.</p>}
      {actionMessage && <p role={actionMessage.includes('성공') || actionMessage.includes('활성화') || actionMessage.includes('저장했습니다') ? 'status' : 'alert'} className="text-sm text-muted-foreground">{actionMessage}</p>}
      <Dialog open={confirmOpen} onOpenChange={open => { if (!activate.isPending) setConfirmOpen(open) }}>
        <DialogContent aria-describedby={`storage-activate-description-${provider.id}`}>
          <DialogTitle>기본 저장소를 변경할까요?</DialogTitle>
          <DialogDescription id={`storage-activate-description-${provider.id}`} className="mt-2 text-sm leading-6 text-muted-foreground">{provider.name}을 기본 저장소로 사용합니다. 기존 녹화는 자동으로 이동하지 않으며, archive가 비어 있지 않으면 전환이 거부될 수 있습니다. 진행 중인 녹화는 기존 storage generation을 계속 사용합니다.</DialogDescription>
          <div className="mt-5 flex justify-end gap-2"><Button variant="outline" disabled={activate.isPending} onClick={() => setConfirmOpen(false)}>취소</Button><Button disabled={activate.isPending} onClick={() => activate.mutate()}>{activate.isPending ? '활성화 중…' : '확인 후 활성화'}</Button></div>
        </DialogContent>
      </Dialog>
    </CardContent>
  </Card>
}

function healthTone(health: StorageProviderSummary['health']): 'green' | 'amber' | 'red' | 'neutral' {
  return health === 'ready' ? 'green' : health === 'failed' ? 'red' : 'neutral'
}
function healthLabel(health: StorageProviderSummary['health']) {
  return health === 'ready' ? '연결 준비됨' : health === 'failed' ? '검사 실패' : '검사 필요'
}
function storageActionError(error: unknown): string {
  const text = errorMessage(error)
  const safeMessages: Record<string, string> = {
    storage_provider_not_installed: 'Storage provider가 설치되어 있지 않습니다.',
    storage_provider_not_configured: 'Storage provider 설정을 먼저 완료하세요.',
    storage_provider_unavailable: 'Storage provider를 사용할 수 없습니다.',
    storage_provider_probe_failed: '연결 검사를 통과하지 못했습니다. 설정을 확인하고 다시 시도하세요.',
    storage_provider_identity_mismatch: 'Storage provider identity가 일치하지 않습니다.',
    storage_provider_protocol_unsupported: '지원하지 않는 Storage Provider Protocol 버전입니다.',
    storage_backend_in_use: '기존 녹화가 사용 중이라 저장소를 변경할 수 없습니다.',
    storage_backend_switch_requires_empty_archive: '기존 archive를 이동하지 않으므로 비어 있지 않은 archive에서는 저장소를 변경할 수 없습니다.',
    storage_operation_conflict: '다른 저장소 작업이 진행 중입니다. 잠시 후 다시 시도하세요.',
    storage_activation_failed: '기본 저장소를 활성화하지 못했습니다.',
    storage_instance_not_found: '저장소 인스턴스를 찾을 수 없습니다. 새로고침 후 다시 시도하세요.',
  }
  return safeMessages[text] ?? '저장소 작업을 완료하지 못했습니다. 설정과 연결 상태를 확인한 뒤 다시 시도하세요.'
}

export function StoragePoolList({ pools }: { pools: StoragePool[] }) {
  if (!pools.length) return <EmptyState title="등록된 저장 풀이 없습니다." description="서버에서 사용할 수 있는 저장 풀이 확인되면 여기에 표시됩니다." />
  return <div className="grid min-w-0 gap-4 xl:grid-cols-2">{pools.map(pool => <StoragePoolCard key={pool.id} pool={pool} />)}</div>
}

export function StoragePoolCard({ pool }: { pool: StoragePool }) {
  const ratio = boundedRatio(pool.capacity.usage_ratio)
  return <Card className="min-w-0 overflow-hidden">
    <CardHeader className="flex-row items-start justify-between gap-4 border-b border-border/70">
      <div className="flex min-w-0 items-start gap-3"><span className="grid h-10 w-10 shrink-0 place-items-center rounded-lg bg-primary/10 text-primary"><HardDrive className="h-5 w-5" /></span><div className="min-w-0"><Link to="/storage/$poolId" params={{ poolId: pool.id }} className="focus-ring rounded-sm"><CardTitle className="truncate text-base" title={pool.display_name}>{pool.display_name}</CardTitle></Link><p className="mt-1 truncate font-mono text-[11px] text-muted-foreground">{pool.id} · {kindLabel(pool.kind)} · {roleLabel(pool.role)}</p></div></div>
      <PoolHealth health={pool.health} />
    </CardHeader>
    <CardContent className="grid gap-5 pt-4 sm:grid-cols-2">
      <section aria-label="파일 시스템 용량" className="min-w-0">{pool.capacity_known ? <><div className="mb-2 flex items-baseline justify-between gap-2"><span className="text-xs font-medium text-muted-foreground">파일 시스템 용량</span><span className="text-xs tabular-nums">{formatBytes(pool.capacity.used_bytes)} / {formatBytes(pool.capacity.total_bytes)}</span></div><progress className="storage-progress" value={ratio} max={1} aria-label={`사용량 ${(ratio * 100).toFixed(0)}%`} /><div className="mt-2 flex justify-between text-[11px] text-muted-foreground"><span>{(ratio * 100).toFixed(1)}% 사용</span><span>{formatBytes(pool.capacity.available_bytes)} 여유</span></div></> : <div className="flex items-baseline justify-between gap-2"><span className="text-xs font-medium text-muted-foreground">파일 시스템 용량</span><span className="text-xs text-muted-foreground">용량 알 수 없음</span></div>}</section>
      <div className="grid grid-cols-2 gap-3">
        <MetricValue icon={<Gauge className="h-3.5 w-3.5" />} label="Recorder 읽기" value={formatRate(pool.throughput.read_bytes_per_second)} />
        <MetricValue icon={<Waves className="h-3.5 w-3.5" />} label="Recorder 쓰기" value={formatRate(pool.throughput.write_bytes_per_second)} />
        <MetricValue icon={<Layers3 className="h-3.5 w-3.5" />} label="수집 버퍼" value={`${formatBytes(pool.buffer.used_bytes)} / ${formatBytes(pool.buffer.capacity_bytes)}`} />
        <MetricValue icon={<Database className="h-3.5 w-3.5" />} label="저장 대기열" value={`${pool.queue.objects}개 · ${formatBytes(pool.queue.bytes)}`} />
      </div>
      <div className="flex min-w-0 flex-wrap items-center justify-between gap-x-4 gap-y-2 border-t border-border/70 pt-3 text-xs sm:col-span-2">
        <CeilingSummary pool={pool} />
        <span className="text-muted-foreground">가장 오래 대기 {formatDuration(pool.queue.oldest_age_seconds)} · 기록기 {pool.writers.active}/{pool.writers.limit}</span>
        <Link to="/storage/$poolId" params={{ poolId: pool.id }} className="ml-auto inline-flex items-center gap-1 font-medium text-primary">상세 보기 <ArrowRight className="h-3.5 w-3.5" /></Link>
      </div>
    </CardContent>
  </Card>
}

export function StoragePoolDetailPage() {
  const { poolId } = useParams({ from: '/storage/$poolId' })
  const [window, setWindow] = useState<StorageMetricWindow>('1h')
  const pools = useQuery(storagePoolsQuery)
  const pool = pools.data?.items.find(item => item.id === poolId)
  const metrics = useQuery(storageMetricsQuery(poolId, window, !pools.isLoading && !pools.error && Boolean(pool)))

  return <div className="page-enter">
    <PageHeading eyebrow="저장소 풀" title={pool?.display_name ?? poolId} description={pool ? `${pool.id} · ${kindLabel(pool.kind)} · ${roleLabel(pool.role)}` : '저장 풀 상태와 최근 측정값'} actions={<><Select aria-label="측정 기간" value={window} onValueChange={value => setWindow(value as StorageMetricWindow)} className="w-28"><SelectItem value="1h">최근 1시간</SelectItem><SelectItem value="6h">최근 6시간</SelectItem><SelectItem value="24h">최근 24시간</SelectItem></Select><Link to="/storage"><Button variant="outline"><ArrowLeft className="h-4 w-4" />저장소 목록</Button></Link></>} />
    {pools.isLoading ? <LoadingState label="저장 풀을 불러오는 중입니다" /> : pools.error ? <ErrorState message={errorMessage(pools.error)} retry={() => void pools.refetch()} /> : !pool ? <EmptyState title="저장 풀을 찾을 수 없습니다." description="저장소 목록에서 사용할 수 있는 풀을 확인하세요." /> : <>
      <PoolDetailSummary pool={pool} />
      <div className="mt-5 flex items-center justify-between gap-3"><h2 className="text-base font-semibold">최근 측정 추이</h2>{metrics.data && <span className="text-xs text-muted-foreground">{metrics.data.sample_interval_seconds}초 간격</span>}</div>
      <div className="mt-3 grid gap-4 xl:grid-cols-2">
        {metrics.isLoading ? <Card><CardContent className="pt-5"><LoadingState label="측정값을 불러오는 중입니다" /></CardContent></Card> : metrics.error ? <div className="xl:col-span-2"><ErrorState message={errorMessage(metrics.error)} retry={() => void metrics.refetch()} /></div> : metrics.data?.items.length ? <>
          <Card><CardContent className="pt-5"><MetricChart title="Recorder 읽기/쓰기" samples={metrics.data.items} kind="throughput" pool={pool} /></CardContent></Card>
          <Card><CardContent className="pt-5"><MetricChart title="수집 버퍼/저장 대기열" samples={metrics.data.items} kind="backlog" /></CardContent></Card>
        </> : <div className="xl:col-span-2"><EmptyState title="표시할 측정 기록이 없습니다." description="새 측정값이 수집되면 읽기·쓰기와 대기 상태의 추이가 표시됩니다." /></div>}
      </div>
    </>}
  </div>
}

function PoolDetailSummary({ pool }: { pool: StoragePool }) {
  const ratio = boundedRatio(pool.capacity.usage_ratio)
  return <div className="grid gap-4 xl:grid-cols-[1.1fr_1fr]">
    <Card><CardHeader className="flex-row items-center justify-between"><CardTitle className="flex items-center gap-2"><HardDrive className="h-4 w-4 text-primary" />풀 상태</CardTitle><PoolHealth health={pool.health} /></CardHeader><CardContent className="space-y-4"><div>{pool.capacity_known ? <><div className="flex justify-between gap-3 text-xs"><span className="text-muted-foreground">파일 시스템 사용량</span><span className="tabular-nums">{formatBytes(pool.capacity.used_bytes)} / {formatBytes(pool.capacity.total_bytes)}</span></div><progress className="storage-progress mt-2" value={ratio} max={1} aria-label={`용량 사용량 ${(ratio * 100).toFixed(0)}%`} /><p className="mt-1 text-right text-[11px] text-muted-foreground">{formatBytes(pool.capacity.available_bytes)} 여유 · {(ratio * 100).toFixed(1)}% 사용</p></> : <p className="text-sm text-muted-foreground">파일 시스템 용량: 용량 알 수 없음</p>}</div><dl className="grid grid-cols-2 gap-4 border-t border-border pt-4 sm:grid-cols-4">{[['유형', pool.kind], ['역할', roleLabel(pool.role)], ['총 읽기', formatBytes(pool.throughput.read_bytes_total)], ['총 쓰기', formatBytes(pool.throughput.write_bytes_total)]].map(([label, value]) => <div key={label}><dt className="text-[11px] text-muted-foreground">{label}</dt><dd className="mt-1 break-words text-sm font-medium tabular-nums">{value}</dd></div>)}</dl></CardContent></Card>
    <Card><CardHeader><CardTitle className="flex items-center gap-2"><Server className="h-4 w-4 text-primary" />기록기와 대기 상태</CardTitle></CardHeader><CardContent className="grid grid-cols-2 gap-x-5 gap-y-4 sm:grid-cols-3">{[
      ['Recorder 읽기', formatRate(pool.throughput.read_bytes_per_second)], ['Recorder 쓰기', formatRate(pool.throughput.write_bytes_per_second)], ['읽기 대기 시간', `${pool.throughput.read_latency_ms.toFixed(1)} ms`], ['쓰기 대기 시간', `${pool.throughput.write_latency_ms.toFixed(1)} ms`],
      ['수집 버퍼', `${formatBytes(pool.buffer.used_bytes)} / ${formatBytes(pool.buffer.capacity_bytes)}`], ['저장 대기열', `${pool.queue.objects}개 · ${formatBytes(pool.queue.bytes)}`], ['가장 오래 대기', formatDuration(pool.queue.oldest_age_seconds)],
      ['활성 기록기', `${pool.writers.active} / ${pool.writers.limit}`], ['저장 오류 누계', pool.errors_total.toLocaleString('ko-KR')],
    ].map(([label, value]) => <div key={label} className="min-w-0"><p className="text-[11px] text-muted-foreground">{label}</p><p className="mt-1 truncate text-sm font-semibold tabular-nums" title={value}>{value}</p></div>)}<div className="col-span-2 border-t border-border pt-3 sm:col-span-3"><CeilingSummary pool={pool} /></div></CardContent></Card>
  </div>
}

function MetricValue({ icon, label, value }: { icon: ReactNode; label: string; value: string }) { return <div className="min-w-0"><div className="flex items-center gap-1.5 text-[10px] text-muted-foreground">{icon}{label}</div><p className="mt-1 truncate text-xs font-semibold tabular-nums" title={value}>{value}</p></div> }
function PoolHealth({ health }: { health: string }) {
  const appearance = health === 'healthy' ? { label: '정상', tone: 'green' as const, icon: ShieldCheck } : health === 'degraded' ? { label: '저하', tone: 'amber' as const, icon: TriangleAlert } : health === 'unavailable' ? { label: '사용 불가', tone: 'red' as const, icon: TriangleAlert } : { label: health || '알 수 없음', tone: 'neutral' as const, icon: Timer }
  const Icon = appearance.icon
  return <Badge tone={appearance.tone}><Icon className="h-3 w-3" />{appearance.label}</Badge>
}
function CeilingSummary({ pool }: { pool: StoragePool }) {
  const ceiling = pool.estimated_ceiling
  const source = ceiling.source
  if (source === 'unknown' || (!ceiling.read_bytes_per_second && !ceiling.write_bytes_per_second)) return <span className="inline-flex items-center gap-1.5 text-xs text-muted-foreground"><Gauge className="h-3.5 w-3.5" />예상 상한: 알 수 없음</span>
  const sourceLabel = source === 'observed' ? '관측 기반 예상 상한' : source === 'configured' ? '설정된 상한' : source === 'benchmarked' ? '측정된 상한' : '예상 상한'
  const parts = [ceiling.read_bytes_per_second ? `읽기 ${formatRate(ceiling.read_bytes_per_second)}` : '', ceiling.write_bytes_per_second ? `쓰기 ${formatRate(ceiling.write_bytes_per_second)}` : ''].filter(Boolean)
  return <span className="inline-flex flex-wrap items-center gap-x-1.5 text-xs"><Gauge className="h-3.5 w-3.5 text-muted-foreground" /><span className="text-muted-foreground">{sourceLabel}:</span><span className="font-medium tabular-nums">{parts.join(' · ')}</span></span>
}
function boundedRatio(value: number) { return Number.isFinite(value) ? Math.min(1, Math.max(0, value)) : 0 }
function formatRate(value: number) { return `${formatBytes(value)}/s` }
function formatDuration(seconds: number) { return Number.isFinite(seconds) && seconds >= 0 ? seconds < 60 ? `${seconds.toFixed(1)}초` : `${Math.floor(seconds / 60)}분 ${Math.floor(seconds % 60)}초` : '—' }
function roleLabel(role: string) { return role === 'primary' ? '주 저장 풀' : role }
function kindLabel(kind: string) { return kind === 'local' ? '로컬' : kind }
