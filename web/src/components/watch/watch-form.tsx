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
      setIntervalError('확인 주기는 2초에서 3,600초 사이의 정수여야 합니다.')
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
      <CardHeader><CardTitle>Watch 설정</CardTitle><p className="text-xs leading-5 text-muted-foreground">방송이 시작되면 자동으로 녹화합니다. 비활성화하거나 Watch를 삭제해도 이미 시작된 녹화는 중지되지 않습니다.</p></CardHeader>
      <CardContent className="grid gap-4 sm:grid-cols-2">
        <label className="space-y-2"><span className="text-sm font-medium">녹화 제목 <span className="text-xs text-muted-foreground">(선택)</span></span><Input value={title} onChange={event => setTitle(event.target.value)} maxLength={256} placeholder="자동 녹화로 생성된 녹화의 제목" /></label>
        <label className="space-y-2"><span className="text-sm font-medium">방송 확인 주기 (초)</span><Input type="number" min={2} max={3600} step={1} value={interval} onChange={event => { setInterval(event.target.value === '' ? 0 : Number(event.target.value)); setIntervalError('') }} aria-invalid={Boolean(intervalError)} aria-describedby={intervalError ? 'watch-interval-error' : undefined} />{intervalError && <span id="watch-interval-error" className="block text-xs text-destructive" role="alert">{intervalError}</span>}</label>
        <label className="flex cursor-pointer items-start gap-3 rounded-md border border-border p-3 sm:col-span-2"><input type="checkbox" className="mt-0.5 h-4 w-4 accent-primary" checked={previewMode === 'segment'} onChange={event => setPreviewMode(event.target.checked ? 'segment' : 'disabled')} /><span><span className="block text-sm font-medium">{t('watch.form.previewTitle')}</span><span className="mt-1 block text-xs leading-5 text-muted-foreground">{t('watch.form.previewHelp')}</span></span></label>
        {canBrowse && <div className="space-y-3 sm:col-span-2"><div className="flex flex-wrap items-center justify-between gap-2"><div><p className="text-sm font-medium">리소스 선택 <span className="text-xs text-muted-foreground">(선택)</span></p><p className="mt-1 text-xs text-muted-foreground">어댑터가 리소스 탐색을 지원합니다.</p></div><Button type="button" variant="outline" size="sm" onClick={() => setBrowseOpen(open => !open)} aria-expanded={browseOpen}>{browseOpen ? '탐색 닫기' : '리소스 찾아보기'}<ChevronDown className="h-3.5 w-3.5" /></Button></div>
          {resource && <p className="text-xs text-muted-foreground">선택한 리소스: <span className="font-medium text-foreground">{resourceLabel(resource)}</span><Button type="button" variant="link" size="sm" className="ml-1 h-auto p-0" onClick={() => setResource(undefined)}>해제</Button></p>}
          {browseOpen && <div className="rounded-md border border-border p-3"><ResourceBreadcrumbs resource={browseParent} onNavigate={setBrowseParent} /><div className="mt-3 grid gap-2 sm:grid-cols-[1fr_180px_auto]"><Input value={search} onChange={event => setSearch(event.target.value)} onKeyDown={event => { if (event.key === 'Enter') runResourceSearch() }} placeholder="리소스 검색 (비우면 목록)" aria-label="리소스 검색" /><Input value={resourceType} onChange={event => setResourceType(event.target.value)} placeholder="리소스 유형" aria-label="리소스 유형" /><Button type="button" variant="outline" onClick={runResourceSearch}><Search className="h-4 w-4" />검색</Button></div>
            {resources.isLoading ? <p className="py-4 text-center text-xs text-muted-foreground">리소스를 불러오는 중입니다.</p> : resources.error ? <p role="alert" className="py-3 text-xs text-destructive">리소스 목록을 불러오지 못했습니다. 입력 양식은 계속 사용할 수 있습니다.</p> : items.length ? <div className="mt-2 divide-y divide-border">{items.map((item, index) => <div key={`${item.resource_type}:${item.resource_id}:${index}`} className="flex min-w-0 items-center gap-3 py-2"><button type="button" className="min-w-0 flex-1 text-left" onClick={() => setBrowseParent({ resource_type: item.resource_type, resource_id: item.resource_id, ...(item.parent ? { parent: item.parent } : {}) })}><span className="block truncate text-sm font-medium">{item.display_name ?? item.resource_id}</span><span className="block truncate text-[11px] text-muted-foreground">{item.resource_type} / {item.resource_id}</span></button><Button type="button" variant={resource?.resource_id === item.resource_id && resource.resource_type === item.resource_type ? 'secondary' : 'outline'} size="sm" onClick={() => setResource({ resource_type: item.resource_type, resource_id: item.resource_id, ...(item.parent ? { parent: item.parent } : {}) })}>{resource?.resource_id === item.resource_id && resource.resource_type === item.resource_type ? '선택됨' : '선택'}</Button></div>)}</div> : resources.data ? <p className="py-4 text-center text-xs text-muted-foreground">표시할 리소스가 없습니다.</p> : <p className="py-4 text-center text-xs text-muted-foreground">검색하거나 목록을 불러오세요.</p>}
            {resources.hasNextPage && <Button type="button" variant="ghost" size="sm" className="mt-2" onClick={() => void resources.fetchNextPage()} disabled={resources.isFetchingNextPage}>{resources.isFetchingNextPage ? '불러오는 중…' : '더 보기'}</Button>}
          </div>}
        </div>}
      </CardContent>
    </Card>
    <Card><CardHeader><CardTitle>입력 및 비밀 값</CardTitle><p className="text-xs leading-5 text-muted-foreground">입력 이름과 설명은 어댑터가 제공합니다. 비밀 값은 API에서 다시 표시되지 않습니다.</p></CardHeader><CardContent><SchemaForm
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
