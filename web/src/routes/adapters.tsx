import { Link } from '@tanstack/react-router'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { ArrowRight, CircleAlert, Download, Fingerprint, PackageOpen, Radio, RefreshCw, Trash2, Upload } from 'lucide-react'
import { adaptersAPI, pluginsAPI } from '@/api'
import { adaptersQuery, pluginsQuery, qk } from '@/api/queries'
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

export function AdaptersPage() {
  const query = useQuery(adaptersQuery)
  const registryQuery = useQuery(pluginsQuery)
  const client = useQueryClient()
  const { toast } = useToast()
  const reconcile = useMutation({
    mutationFn: adaptersAPI.reconcile,
    onSuccess: async result => {
      await client.invalidateQueries({ queryKey: qk.adapters })
      if (result.state === 'rejected') toast('어댑터 검색을 마쳤습니다.', `유효하지 않은 후보 ${result.rejected_count}개를 건너뛰었습니다.`, 'info')
      else if (result.state === 'failed') toast('어댑터 검색을 완료하지 못했습니다.', '현재 활성 어댑터는 유지됩니다. 잠시 후 다시 시도해 주세요.', 'error')
      else if (result.rejected_count > 0) toast('유효한 어댑터 세대를 활성화했습니다.', `유효하지 않은 후보 ${result.rejected_count}개를 건너뛰었습니다.`, 'info')
      else toast(result.state === 'activated' ? '새 어댑터 세대를 활성화했습니다.' : '어댑터가 최신 상태입니다.')
    },
    onError: error => toast('어댑터 검색 실패', errorMessage(error), 'error'),
  })
  const actions = <div className="flex flex-wrap gap-2"><Button variant="outline" disabled={reconcile.isPending} onClick={() => reconcile.mutate()}><RefreshCw className={`h-4 w-4 ${reconcile.isPending ? 'animate-spin' : ''}`} />어댑터 다시 검색</Button><Button variant="ghost" onClick={() => void query.refetch()}><RefreshCw className="h-4 w-4" />새로고침</Button></div>
  return <div className="page-enter space-y-8">
    <PageHeading eyebrow="외부 연결" title="어댑터" description="플랫폼별 원본 탐색은 별도 adapter process가 담당합니다. Registry에서 source plugin을 설치하면 검증 후 새 adapter generation에 반영됩니다. 진행 중인 녹화는 시작 당시 generation을 계속 사용합니다." actions={actions} />
    {query.isLoading ? <LoadingState /> : query.error ? <ErrorState message={errorMessage(query.error)} retry={() => void query.refetch()} /> : query.data?.length ? <div className="overflow-hidden rounded-lg border border-border bg-card"><div className="grid grid-cols-[minmax(180px,1.5fr)_minmax(110px,.7fr)_minmax(120px,.8fr)_minmax(130px,.8fr)_80px] gap-3 border-b border-border bg-muted/50 px-4 py-2.5 text-[10px] font-semibold text-muted-foreground max-md:hidden"><span>어댑터</span><span>상태</span><span>프로토콜</span><span>기능</span><span className="text-right">세대</span></div><div className="divide-y divide-border">{query.data.map(adapter => <AdapterRow key={adapter.status.id} adapter={adapter} />)}</div></div> : <EmptyState title="설치된 어댑터가 없습니다." description={adapterEmptyDescription(registryQuery.data?.state)} />}
    <PluginRegistrySection />
  </div>
}

function adapterEmptyDescription(state?: PluginRegistryStatus['state']): string {
  const operator = '운영자 executable 가져오기는 기본적으로 비활성화되어 있습니다. 관리자는 Runtime Host에서 IR_ALLOW_OPERATOR_PLUGINS=1을 설정해야 사용할 수 있습니다.'
  if (state === 'ready') return `아래 공식 Plugin Registry에서 source plugin을 찾아 설치하세요. ${operator}`
  if (state === 'unavailable') return `Plugin Registry에 연결할 수 없습니다. Registry 새로고침으로 다시 시도하세요. 이미 설치된 plugin은 계속 사용할 수 있습니다. ${operator}`
  if (state === 'not_configured') return `Plugin Registry가 비활성화되었거나 URL이 구성되지 않았습니다. catalog URL을 설정한 뒤 사용할 수 있습니다. ${operator}`
  return `아래 Plugin Registry의 상태를 확인한 뒤 source plugin을 설치할 수 있습니다. ${operator}`
}

function PluginRegistrySection() {
  const query = useQuery(pluginsQuery)
  const client = useQueryClient()
  const { toast } = useToast()
  const invalidate = async () => {
    await Promise.all([client.invalidateQueries({ queryKey: qk.plugins }), client.invalidateQueries({ queryKey: qk.adapters })])
  }
  const refresh = useMutation({
    mutationFn: pluginsAPI.refresh,
    onSuccess: async status => { await invalidate(); toast(status.state === 'ready' ? 'Plugin Registry를 새로 고쳤습니다.' : 'Registry에 연결할 수 없습니다.', status.state === 'ready' ? undefined : '설치된 어댑터는 계속 사용할 수 있습니다.', status.state === 'ready' ? 'success' : 'info') },
    onError: async error => { await client.invalidateQueries({ queryKey: qk.plugins }); toast('Registry 새로 고침 실패', errorMessage(error), 'error') },
  })
  const install = useMutation({
    mutationFn: ({ id }: { id: string; type: 'source' | 'storage' }) => pluginsAPI.install(id),
    onSuccess: async (_result, plugin) => { await invalidate(); toast(plugin.type === 'storage' ? 'Storage provider 실행 파일을 설치했습니다.' : '플러그인을 설치하고 새 어댑터 세대를 활성화했습니다.', plugin.type === 'storage' ? '저장소 페이지에서 설정하고 별도로 활성화해야 기본 저장소가 바뀝니다.' : undefined) },
    onError: error => toast('플러그인 설치 실패', errorMessage(error), 'error'),
  })
  const update = useMutation({
    mutationFn: ({ id }: { id: string; type: 'source' | 'storage' }) => pluginsAPI.update(id),
    onSuccess: async (_result, plugin) => { await invalidate(); toast(plugin.type === 'storage' ? 'Storage provider 실행 파일을 업데이트했습니다.' : '플러그인을 업데이트하고 새 어댑터 세대를 활성화했습니다.', plugin.type === 'storage' ? '기본 저장소는 변경되지 않았습니다. 저장소 페이지에서 적용할 수 있습니다.' : '진행 중인 녹화는 기존 어댑터 세대를 계속 사용합니다.') },
    onError: error => toast('플러그인 업데이트 실패', errorMessage(error), 'error'),
  })
  const uninstall = useMutation({
    mutationFn: ({ id }: { id: string; type: 'source' | 'storage' }) => pluginsAPI.uninstall(id),
    onSuccess: async (_result, plugin) => { await invalidate(); toast(plugin.type === 'storage' ? 'Storage provider 설치 파일을 제거했습니다.' : '플러그인을 제거하고 새 어댑터 세대를 활성화했습니다.', plugin.type === 'storage' ? '현재 세대나 진행 중인 녹화가 참조하는 파일은 보존됩니다.' : '기존 녹화가 참조하는 파일은 안전하게 보존됩니다.') },
    onError: error => toast('플러그인 제거 실패', errorMessage(error), 'error'),
  })
  const busy = refresh.isPending || install.isPending || update.isPending || uninstall.isPending
  const status = query.data as PluginRegistryStatus | undefined
  const sourcePlugins = status?.plugins.filter(plugin => plugin.type !== 'storage') ?? []
  const storagePlugins = status?.plugins.filter(plugin => plugin.type === 'storage') ?? []
  const notConfigured = status?.state === 'not_configured'
  return <section aria-labelledby="plugin-registry-title" className="overflow-hidden rounded-lg border border-border bg-card">
    <div className="flex flex-wrap items-center justify-between gap-3 border-b border-border px-4 py-3">
      <div className="flex items-center gap-2"><PackageOpen className="h-4 w-4 text-primary" /><div><h2 id="plugin-registry-title" className="text-sm font-semibold">Plugin Registry</h2><p className="mt-0.5 text-xs text-muted-foreground">승인된 source adapter와 storage provider 실행 파일을 내려받습니다.</p></div></div>
      <Button variant="outline" disabled={busy || notConfigured} onClick={() => refresh.mutate()}><RefreshCw className={`h-4 w-4 ${refresh.isPending ? 'animate-spin' : ''}`} />Registry 새로고침</Button>
    </div>
    {query.isPending ? <div className="p-4"><LoadingState /></div> : query.error ? <div className="p-4"><ErrorState message={errorMessage(query.error)} retry={() => void query.refetch()} /></div> : <>
      {notConfigured && <div role="status" className="flex items-start gap-2 border-b border-border bg-muted/30 px-4 py-3 text-sm text-muted-foreground"><CircleAlert className="mt-0.5 h-4 w-4 shrink-0" />Plugin Registry가 비활성화되었거나 URL이 구성되지 않았습니다. 설치된 plugin은 계속 사용할 수 있습니다. Registry를 사용하려면 Runtime Host에 HTTPS catalog URL을 설정하세요.</div>}
      {status?.state === 'unavailable' && <div role="status" className="flex items-start gap-2 border-b border-border bg-muted/30 px-4 py-3 text-sm text-muted-foreground"><CircleAlert className="mt-0.5 h-4 w-4 shrink-0" />Registry에 연결할 수 없습니다. 이미 설치된 어댑터는 계속 사용할 수 있습니다. 설치된 storage provider도 계속 사용할 수 있습니다.</div>}
      <PluginGroup title="Source adapters" description="설치·업데이트하면 검증 후 새 adapter generation에 적용됩니다." plugins={sourcePlugins} busy={busy} onInstall={plugin => install.mutate(plugin)} onUpdate={plugin => update.mutate(plugin)} onUninstall={plugin => uninstall.mutate(plugin)} registryState={status?.state} />
      <div className="border-t border-border">
        <PluginGroup title="Storage providers" description="설치·업데이트는 실행 파일을 검증해 보관할 뿐 기본 저장소를 바꾸지 않습니다. 설정 및 별도 활성화는 저장소 페이지에서 진행합니다." plugins={storagePlugins} busy={busy} onInstall={plugin => install.mutate(plugin)} onUpdate={plugin => update.mutate(plugin)} onUninstall={plugin => uninstall.mutate(plugin)} registryState={status?.state} storage />
        <div className="px-4 pb-4"><Link to="/storage" className="text-xs font-medium text-primary hover:underline">저장소 설정 보기 <ArrowRight className="inline h-3 w-3" /></Link></div>
      </div>
    </>}
  </section>
}

type RegistryPlugin = PluginRegistryStatus['plugins'][number]
function PluginGroup({ title, description, plugins, busy, onInstall, onUpdate, onUninstall, registryState, storage = false }: {
  title: string; description: string; plugins: RegistryPlugin[]; busy: boolean;
  onInstall: (plugin: RegistryPlugin) => void; onUpdate: (plugin: RegistryPlugin) => void;
  onUninstall: (plugin: RegistryPlugin) => void; registryState?: PluginRegistryStatus['state']; storage?: boolean
}) {
  return <section aria-label={title}>
    <div className="px-4 pb-2 pt-4"><h3 className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">{title}</h3><p className="mt-1 text-xs text-muted-foreground">{description}</p></div>
    {plugins.length ? <div className="divide-y divide-border">{plugins.map(plugin => <div key={plugin.id} className="flex flex-wrap items-center justify-between gap-3 px-4 py-3">
      <div className="min-w-0"><div className="flex items-center gap-2"><span className="truncate text-sm font-medium">{plugin.name}</span>{plugin.installed && <Badge tone="green">설치됨</Badge>}</div><p className="mt-1 truncate font-mono text-[11px] text-muted-foreground">{plugin.id} · {plugin.installed ? `설치 ${plugin.installed_version ?? '버전 미상'}` : '미설치'}{plugin.available_version ? ` · 사용 가능 ${plugin.available_version}` : ''}</p><div className="mt-1.5"><PluginTrustBadges trust={plugin.trust} warning compact /></div></div>
      <div className="flex flex-wrap gap-2">{!plugin.installed && plugin.available_version && <Button size="sm" disabled={busy || registryState !== 'ready'} onClick={() => onInstall(plugin)}><Download className="h-3.5 w-3.5" />설치</Button>}{plugin.installed && plugin.update_available && <Button size="sm" variant="outline" disabled={busy || registryState !== 'ready'} onClick={() => onUpdate(plugin)}><Upload className="h-3.5 w-3.5" />업데이트</Button>}{plugin.installed && <Button size="sm" variant="ghost" disabled={busy} onClick={() => onUninstall(plugin)}><Trash2 className="h-3.5 w-3.5" />제거</Button>}</div>
    </div>)}</div> : <p className="px-4 pb-4 text-sm text-muted-foreground">{registryState === 'unavailable' ? `Registry에 연결할 수 없어 새 ${storage ? 'storage provider' : 'source adapter'} 목록을 표시하지 못했습니다.` : registryState === 'not_configured' ? `Registry가 비활성화되어 새 ${storage ? 'storage provider' : 'source adapter'} 목록을 표시하지 못했습니다.` : `등록된 ${storage ? 'storage provider' : 'source adapter'}가 없습니다.`}</p>}
  </section>
}

function AdapterRow({ adapter }: { adapter: Adapter }) {
  const descriptor = adapter.descriptor
  const caps = descriptor?.capabilities ?? []
  return <Link to="/adapters/$adapterId" params={{ adapterId: adapter.status.id }} search={{ tab: undefined }} className="grid min-w-0 grid-cols-[minmax(180px,1.5fr)_minmax(110px,.7fr)_minmax(120px,.8fr)_minmax(130px,.8fr)_80px] items-center gap-3 px-4 py-3 transition-colors hover:bg-muted/30 max-md:grid-cols-[1fr_auto] max-md:gap-y-2"><span className="flex min-w-0 items-center gap-3"><AdapterMark adapter={adapter} size="md" /><span className="min-w-0"><span className="block truncate text-sm font-semibold" title={descriptor?.name ?? adapter.status.name ?? adapter.status.id}>{descriptor?.name ?? adapter.status.name ?? adapter.status.id}</span><span className="mt-0.5 block truncate font-mono text-[11px] text-muted-foreground">{adapter.status.id} · {descriptor?.version ?? adapter.status.version ?? '—'}</span><span className="mt-1 block"><PluginTrustBadges trust={adapter.trust} warning compact /></span></span></span><span><StatusBadge state={adapter.status.state} />{adapter.status.error && <span className="mt-1 flex items-center gap-1 text-[10px] text-muted-foreground"><CircleAlert className="h-3 w-3" />상세 상태 참조</span>}</span><span className="text-xs text-muted-foreground">v{descriptor?.protocol_version ?? adapter.status.protocol_version ?? '—'}</span><span className="flex flex-wrap gap-1">{caps.length ? caps.slice(0, 2).map(cap => <Badge key={cap} tone="blue">{cap}</Badge>) : <span className="text-xs text-muted-foreground">기본 기능</span>}</span><span className="text-right text-xs tabular-nums text-muted-foreground">{adapter.status.generation ?? '—'}<ArrowRight className="ml-2 inline h-3.5 w-3.5" /></span><span className="col-span-2 flex gap-2 text-[11px] text-muted-foreground md:hidden"><Radio className="h-3 w-3" />{descriptor?.media_types?.join(', ') || '미디어 형식 미보고'}<span>·</span><Fingerprint className="h-3 w-3" />재시작 {adapter.status.restart_attempts ?? 0}회</span></Link>
}
