import { useRef, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Link, useNavigate } from '@tanstack/react-router'
import { ArrowLeft } from 'lucide-react'
import { adaptersAPI, watchesAPI, type WatchMutationBody } from '@/api'
import { adaptersQuery, dashboardQuery, qk } from '@/api/queries'
import { AdapterMark } from '@/components/adapter/adapter-mark'
import { PageHeading } from '@/components/page-heading'
import { StatusBadge } from '@/components/status-badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent } from '@/components/ui/card'
import { EmptyState, ErrorState, LoadingState } from '@/components/query-state'
import { WatchForm } from '@/components/watch/watch-form'
import { watchCapableAdapters } from '@/lib/watches'
import { errorMessage } from '@/lib/errors'
import { useToast } from '@/components/ui/use-toast'
import { useI18n } from '@/i18n/provider'

export function WatchNewPage() {
  const [adapterId, setAdapterId] = useState('')
  const adapters = useQuery(adaptersQuery)
  const client = useQueryClient()
  const navigate = useNavigate()
  const { toast } = useToast()
  const { t } = useI18n()
  const pendingBody = useRef<WatchMutationBody | undefined>(undefined)
  const selected = adapters.data?.find(adapter => adapter.status.id === adapterId)
  const schema = useQuery({
    queryKey: qk.schema(adapterId),
    queryFn: () => adaptersAPI.schema(adapterId),
    enabled: Boolean(selected?.descriptor), retry: false, staleTime: 60_000,
  })
  const create = useMutation({
    mutationFn: () => {
      const body = pendingBody.current
      pendingBody.current = undefined
      if (!body) throw new Error(t('watch.new.schemaFailed'))
      return watchesAPI.create(body)
    },
    gcTime: 0,
    onSuccess: async watch => {
      toast(t('watch.new.created'))
      await Promise.all([
        client.invalidateQueries({ queryKey: qk.watches }),
        client.invalidateQueries({ queryKey: dashboardQuery.queryKey }),
      ])
      await navigate({ to: '/watches/$watchId', params: { watchId: watch.id } })
    },
    onError: () => toast(t('watch.new.createFailed'), t('watch.new.retry'), 'error'),
  })
  const eligible = watchCapableAdapters(adapters.data)

  return <div className="page-enter">
    <PageHeading title={t('watch.new.title')} actions={<Link to="/watches"><Button variant="outline"><ArrowLeft className="h-4 w-4" />{t('watch.new.back')}</Button></Link>} />
    {!adapterId && <section><h2 className="mb-3 text-base font-semibold">{t('watch.new.adapter')}</h2>{adapters.isLoading ? <LoadingState /> : adapters.error ? <ErrorState message={errorMessage(adapters.error)} retry={() => void adapters.refetch()} /> : eligible.length ? <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">{eligible.map(adapter => <button key={adapter.status.id} type="button" onClick={() => setAdapterId(adapter.status.id)} className="focus-ring rounded-lg border border-border bg-card p-4 text-left transition hover:border-primary/50"><div className="flex items-center gap-3"><AdapterMark adapter={adapter} size="md" /><span className="min-w-0 flex-1"><span className="block truncate font-semibold" title={adapter.descriptor?.name}>{adapter.descriptor?.name}</span><span className="mt-0.5 block truncate text-xs text-muted-foreground">{adapter.status.id} · v{adapter.descriptor?.version}</span></span><StatusBadge state={adapter.status.state} /></div></button>)}</div> : <EmptyState title={t('watch.new.noAdapters')} />}</section>}
    {selected && <div className="mt-4"><Card className="mb-4"><CardContent className="flex flex-wrap items-center gap-3 py-3"><AdapterMark adapter={selected} adapterId={selected.status.id} showName size="sm" /><StatusBadge state={selected.status.state} /><Button type="button" variant="ghost" size="sm" className="ml-auto" onClick={() => setAdapterId('')} disabled={create.isPending}>{t('watch.new.changeAdapter')}</Button></CardContent></Card>{schema.isLoading ? <LoadingState label={t('watch.new.loadingSchema')} /> : schema.error || !schema.data ? <ErrorState message={schema.error ? errorMessage(schema.error) : t('watch.new.schemaFailed')} retry={() => void schema.refetch()} /> : <WatchForm key={selected.status.id} adapter={selected} schema={schema.data.input_schema} submitLabel={create.isPending ? t('watch.new.creating') : t('watch.new.create')} busy={create.isPending} onSubmit={value => { pendingBody.current = value; create.mutate() }} />}{create.isPending && <p role="status" className="mt-3 text-sm text-muted-foreground">{t('watch.new.saving')}</p>}</div>}
  </div>
}
