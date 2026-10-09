import { useEffect, useMemo, useRef, useState } from 'react'
import { getCoreRowModel, flexRender, useReactTable, type ColumnDef } from '@tanstack/react-table'
import { Link, useNavigate, useSearch } from '@tanstack/react-router'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { ChevronRight, Search, SlidersHorizontal, Trash2, X } from 'lucide-react'
import { recordingsAPI } from '@/api'
import { adaptersQuery, qk } from '@/api/queries'
import { formatBytes, formatDate, formatDuration } from '@/lib/utils'
import { localDateBoundary, localDateInputValue } from '@/lib/recordings'
import type { RecordingQuery } from '@/api'
import { recordingStates, type RecordingListItem } from '@/types/api'
import { PageHeading } from '@/components/page-heading'
import { StatusBadge } from '@/components/status-badge'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Confirm } from '@/components/ui/confirm'
import { Input } from '@/components/ui/input'
import { Select, SelectItem } from '@/components/ui/select'
import { Card } from '@/components/ui/card'
import { ErrorState, LoadingState, EmptyState } from '@/components/query-state'
import { useToast } from '@/components/ui/use-toast'
import { RecordingListIdentity, RecordingTitle } from '@/components/recording/list-identity'
import { errorMessage } from '@/lib/errors'
import { useDebouncedValue } from '@/hooks/use-debounced-value'
import { integrityStatusLabel, recordingStateLabel } from '@/lib/labels'
import { useI18n } from '@/i18n/provider'

type SearchState = { q?: string; state?: string; adapter?: string; resource_type?: string; started_after?: string; started_before?: string; has_gaps?: string; integrity?: string; tag?: string; sort?: string; limit?: number; cursor?: string }
function buildColumns(t: ReturnType<typeof useI18n>['t']): ColumnDef<RecordingListItem>[] {
  return [
    { accessorKey: 'state', header: t('recordings.column.state'), cell: ({ row }) => <StatusBadge state={row.original.state} /> },
    { accessorKey: 'title', header: t('recordings.column.title'), cell: ({ row }) => <div className="max-w-[270px]"><RecordingTitle recordingId={row.original.id} title={row.original.title} preview={row.original.preview} adapterId={row.original.adapter_id} adapterName={row.original.adapter_name} /></div> },
    { accessorKey: 'adapter_id', header: t('recordings.column.source'), cell: ({ row }) => <RecordingListIdentity item={row.original} /> },
    { accessorKey: 'started_at', header: t('recordings.column.started'), cell: ({ row }) => <span className="whitespace-nowrap text-xs text-muted-foreground">{formatDate(row.original.started_at)}</span> },
    { accessorKey: 'duration_seconds', header: t('recordings.column.duration'), cell: ({ row }) => <span className="tabular-nums">{formatDuration(row.original.duration_seconds)}</span> },
    { accessorKey: 'archive_size_bytes', header: t('recordings.column.archiveSize'), cell: ({ row }) => <span className="tabular-nums">{formatBytes(row.original.archive_size_bytes)}</span> },
    { accessorKey: 'segment_count', header: t('recordings.column.segments'), cell: ({ row }) => <span className="tabular-nums">{row.original.segment_count ?? 0}</span> },
    { accessorKey: 'gap_count', header: t('recordings.column.gaps'), cell: ({ row }) => row.original.gap_count ? <Badge tone="amber">{row.original.gap_count}</Badge> : <span className="text-muted-foreground">0</span> },
    { accessorKey: 'integrity', header: t('recordings.column.integrity'), cell: ({ row }) => <StatusBadge state={row.original.integrity} /> },
    { accessorKey: 'tags', header: t('recordings.column.tags'), cell: ({ row }) => <div className="flex max-w-32 flex-wrap gap-1">{row.original.tags?.slice(0, 2).map(tag => <Badge key={tag}>{tag}</Badge>)}</div> },
  ]
}

export function RecordingsPage() {
  const search = useSearch({ from: '/recordings' }) as SearchState
  const navigate = useNavigate({ from: '/recordings' }); const client = useQueryClient(); const { toast } = useToast()
  const { t } = useI18n()
  const [searchText, setSearchText] = useState(search.q ?? '')
  const debouncedSearchText = useDebouncedValue(searchText, 300)
  const routeSearchValue = search.q ?? ''
  const lastRouteSearch = useRef(routeSearchValue)
  const routeSyncPending = useRef(false)
  const [filterOpen, setFilterOpen] = useState(false)
  useEffect(() => {
    if (lastRouteSearch.current === routeSearchValue) return
    lastRouteSearch.current = routeSearchValue
    routeSyncPending.current = true
    setSearchText(routeSearchValue)
  }, [routeSearchValue])
  useEffect(() => {
    if (routeSyncPending.current) {
      if (debouncedSearchText === routeSearchValue) routeSyncPending.current = false
      return
    }
    if (debouncedSearchText !== searchText) return
    if (debouncedSearchText === routeSearchValue) return
    lastRouteSearch.current = debouncedSearchText
    void navigate({ search: previous => ({ ...previous, q: debouncedSearchText || undefined, cursor: undefined }) })
  }, [debouncedSearchText, routeSearchValue, searchText, navigate])
  const query: RecordingQuery = { ...search }
  const records = useQuery({ queryKey: qk.recordings(query), queryFn: () => recordingsAPI.list(query), staleTime: 5000, refetchInterval: queryState => search.state === 'recording' || queryState.state.data?.items.some(item => item.state === 'recording') ? 5000 : 25_000, refetchIntervalInBackground: false })
  const adapters = useQuery(adaptersQuery)
  const remove = useMutation({ mutationFn: recordingsAPI.remove, onSuccess: (_, id) => { toast(t('recordings.deleteAction')); void client.invalidateQueries({ queryKey: ['recordings'] }); void client.invalidateQueries({ queryKey: qk.dashboard }); void client.removeQueries({ queryKey: qk.recording(id) }) }, onError: error => toast(t('recordings.delete'), errorMessage(error), 'error') })
  const set = (patch: Partial<SearchState>) => { void navigate({ search: previous => ({ ...previous, ...patch, cursor: undefined }) }) }
  const clearFilters = () => { setSearchText(''); void navigate({ search: { sort: '-started_at', limit: 25 } }) }
  const columns = useMemo(() => buildColumns(t), [t])
  const table = useReactTable({ data: records.data?.items ?? [], columns, getCoreRowModel: getCoreRowModel(), manualPagination: true, pageCount: -1 })
  const activeFilterCount = Object.entries(search).filter(([key, value]) => !['cursor', 'limit', 'sort'].includes(key) && Boolean(value)).length
  const goNext = () => { if (!records.data?.next_cursor) return; void navigate({ search: previous => ({ ...previous, cursor: records.data?.next_cursor }) }) }
  return <div className="page-enter"><PageHeading eyebrow={t('recordings.eyebrow')} title={t('recordings.title')} description={t('recordings.description')} actions={<><Badge tone="neutral">{t('recordings.total', { count: records.data?.total ?? '—' })}</Badge><Link to="/new"><Button><span className="text-lg leading-none">+</span>{t('nav.newRecording')}</Button></Link></>} />
    <Card className="mb-4 p-2.5"><div className="flex flex-col gap-2.5 xl:flex-row xl:flex-nowrap xl:items-center"><div className="relative min-w-0 flex-1 xl:min-w-[180px]"><Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" /><Input value={searchText} onChange={event => { routeSyncPending.current = false; setSearchText(event.target.value) }} placeholder={t('recordings.search')} aria-label={t('recordings.searchLabel')} className="pl-9" /></div><div className="flex min-w-0 flex-wrap items-center gap-2 xl:flex-nowrap"><div className="w-full sm:w-[126px]"><Select value={search.state ?? '__all'} onValueChange={value => set({ state: value === '__all' ? undefined : value })} aria-label={t('recordings.filter.state')}><SelectItem value="__all">{t('recordings.filter.allStates')}</SelectItem>{recordingStates.map(value => <SelectItem key={value} value={value}>{recordingStateLabel(value)}</SelectItem>)}</Select></div><div className="w-full sm:w-[150px]"><Select value={search.adapter ?? '__all'} onValueChange={value => set({ adapter: value === '__all' ? undefined : value })} aria-label={t('recordings.filter.source')}><SelectItem value="__all">{t('recordings.filter.allSources')}</SelectItem>{adapters.data?.filter(item => item.descriptor).map(item => <SelectItem key={item.status.id} value={item.status.id}>{item.descriptor?.name ?? item.status.id}</SelectItem>)}</Select></div><div className="w-full sm:w-[120px]"><Select value={search.has_gaps ?? '__all'} onValueChange={value => set({ has_gaps: value === '__all' ? undefined : value })} aria-label={t('recordings.filter.gaps')}><SelectItem value="__all">{t('recordings.filter.gapsAny')}</SelectItem><SelectItem value="true">{t('recordings.filter.hasGaps')}</SelectItem><SelectItem value="false">{t('recordings.filter.noGaps')}</SelectItem></Select></div><Button className="shrink-0" variant={filterOpen ? 'secondary' : 'outline'} size="sm" onClick={() => setFilterOpen(!filterOpen)}><SlidersHorizontal className="h-4 w-4" />{t('recordings.filter.more')}{activeFilterCount > 0 && <Badge tone="blue">{activeFilterCount}</Badge>}</Button><div className="w-full sm:w-[140px]"><Select value={search.sort ?? '-started_at'} onValueChange={value => set({ sort: value })} aria-label={t('recordings.sort.label')}><SelectItem value="-started_at">{t('recordings.sort.latest')}</SelectItem><SelectItem value="started_at">{t('recordings.sort.oldest')}</SelectItem><SelectItem value="-created_at">{t('recordings.sort.addedLatest')}</SelectItem><SelectItem value="created_at">{t('recordings.sort.addedOldest')}</SelectItem><SelectItem value="-duration">{t('recordings.sort.longest')}</SelectItem><SelectItem value="duration">{t('recordings.sort.shortest')}</SelectItem><SelectItem value="-size">{t('recordings.sort.largest')}</SelectItem><SelectItem value="size">{t('recordings.sort.smallest')}</SelectItem></Select></div></div></div>
      {filterOpen && <div className="mt-2 grid gap-2 border-t border-border pt-3 sm:grid-cols-2 xl:grid-cols-6"><label className="space-y-1 text-xs font-medium text-muted-foreground">{t('recordings.filter.integrity')}<select aria-label={t('recordings.filter.integrity')} className="focus-ring h-9 w-full rounded-md border border-input bg-background px-3 text-sm text-foreground" value={search.integrity ?? '__all'} onChange={event => set({ integrity: event.target.value === '__all' ? undefined : event.target.value })}><option value="__all">{t('recordings.filter.allStates')}</option>{['unknown', 'verifying', 'verified', 'degraded', 'failed'].map(value => <option key={value} value={value}>{integrityStatusLabel(value)}</option>)}</select></label><label className="space-y-1 text-xs font-medium text-muted-foreground">{t('recordings.filter.tag')}<Input aria-label={t('recordings.filter.tag')} value={search.tag ?? ''} onChange={event => set({ tag: event.target.value || undefined })} placeholder={t('recordings.filter.tag')} /></label><label className="space-y-1 text-xs font-medium text-muted-foreground">{t('recordings.filter.resourceType')}<Input aria-label={t('recordings.filter.resourceType')}  value={search.resource_type ?? ''} onChange={event => set({ resource_type: event.target.value || undefined })} placeholder={t('recordings.filter.resourceTypePlaceholder')} /></label><label className="space-y-1 text-xs font-medium text-muted-foreground">{t('recordings.filter.startedAfter')}<Input aria-label={t('recordings.filter.startedAfter')}  type="date" value={localDateInputValue(search.started_after)} onChange={event => set({ started_after: localDateBoundary(event.target.value) })} /></label><label className="space-y-1 text-xs font-medium text-muted-foreground">{t('recordings.filter.startedBefore')}<Input aria-label={t('recordings.filter.startedBefore')}  type="date" value={localDateInputValue(search.started_before)} onChange={event => set({ started_before: localDateBoundary(event.target.value, true) })} /></label><div className="flex items-end"><Button variant="ghost" size="sm" onClick={clearFilters}><X className="h-3.5 w-3.5" />{t('recordings.filter.clear')}</Button></div></div>}
    </Card>
    {records.isLoading ? <LoadingState /> : records.error ? <ErrorState message={errorMessage(records.error)} retry={() => void records.refetch()} /> : !records.data?.items.length ? <EmptyState title={t('recordings.empty.title')} description={t('recordings.empty.description')} /> : <>
      <div className="hidden overflow-hidden rounded-lg border border-border bg-card lg:block"><div className="overflow-x-auto"><table className="w-full min-w-[1180px] text-left text-xs"><thead className="bg-muted/60 text-[10px] uppercase tracking-wide text-muted-foreground"><tr>{table.getHeaderGroups()[0]?.headers.map(header => <th key={header.id} className="px-3 py-3 font-semibold">{flexRender(header.column.columnDef.header, header.getContext())}</th>)}<th className="px-3 py-3 text-right font-semibold">{t('recordings.column.actions')}</th></tr></thead><tbody>{table.getRowModel().rows.map(row => <tr key={row.id} className="border-t border-border/70 transition-colors hover:bg-muted/30">{row.getVisibleCells().map(cell => <td key={cell.id} className="px-3 py-3">{flexRender(cell.column.columnDef.cell, cell.getContext())}</td>)}<td className="px-3 py-3 text-right"><Confirm trigger={<Button variant="ghost" size="icon" aria-label={`${row.original.title || row.original.id} ${t('recordings.delete')}`} disabled={remove.isPending || row.original.state === 'recording'}><Trash2 className="h-4 w-4 text-muted-foreground hover:text-destructive" /></Button>} title={t('recordings.deleteTitle')} description={t('recordings.deleteDescription')} confirmLabel={t('recordings.deleteAction')} destructive onConfirm={() => remove.mutate(row.original.id)} disabled={remove.isPending || row.original.state === 'recording'} /></td></tr>)}</tbody></table></div></div>
      <div className="space-y-2 lg:hidden">{records.data.items.map(item => <MobileRecording key={item.id} item={item} onDelete={() => remove.mutate(item.id)} busy={remove.isPending} />)}</div>
      <div className="mt-4 flex flex-wrap items-center justify-between gap-3 text-xs text-muted-foreground"><span>{t('recordings.pageCount', { total: records.data.total, current: records.data.items.length })}</span><div className="flex gap-2"><Button variant="outline" size="sm" onClick={goNext} disabled={!records.data.next_cursor}><span>{t('recordings.next')}</span><ChevronRight className="h-4 w-4" /></Button></div></div>
    </>}
  </div>
}
function MobileRecording({ item, onDelete, busy }: { item: RecordingListItem; onDelete: () => void; busy: boolean }) { const adapters = useQuery(adaptersQuery); const adapter = adapters.data?.find(candidate => candidate.status.id === item.adapter_id); const { t } = useI18n(); return <Card className="p-3.5"><div className="flex items-start justify-between gap-2"><div className="min-w-0 flex-1"><RecordingTitle recordingId={item.id} title={item.title} preview={item.preview} adapterId={item.adapter_id} adapterName={item.adapter_name} adapter={adapter} /><div className="mt-2 flex min-w-0 items-center gap-2"><span className="min-w-0 truncate text-[11px] text-muted-foreground">{item.adapter_name || item.adapter_id} · {formatDate(item.started_at)}</span></div><p className="mt-1 truncate text-[11px] text-muted-foreground">{item.resource_type && item.resource_id ? `${item.resource_type} / ${item.resource_id}` : '—'}</p></div><StatusBadge state={item.state} /></div><div className="mt-3 grid grid-cols-3 gap-2 border-t border-border pt-3 text-xs"><span>{formatDuration(item.duration_seconds)}</span><span>{formatBytes(item.archive_size_bytes)}</span><span>{t('recordings.segmentCount', { count: item.segment_count })}</span></div><div className="mt-3 flex items-center justify-between"><div className="flex gap-1">{item.gap_count ? <Badge tone="amber">{t('recordings.gapsCount', { count: item.gap_count })}</Badge> : <Badge>{t('recordings.gapsNone')}</Badge>}<StatusBadge state={item.integrity} /></div><Confirm trigger={<Button variant="ghost" size="sm" aria-label={t('recordings.delete')} disabled={busy || item.state === 'recording'}><Trash2 className="h-4 w-4 text-destructive" /></Button>} title={t('recordings.deleteTitle')} description={t('recordings.deleteDescription')} confirmLabel={t('recordings.deleteAction')} destructive onConfirm={onDelete} disabled={busy || item.state === 'recording'} /></div></Card> }
