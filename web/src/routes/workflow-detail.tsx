import { useState } from 'react'
import { Link, useNavigate, useParams } from '@tanstack/react-router'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { ArrowLeft } from 'lucide-react'
import { workflowsAPI } from '@/api'
import { accountAuthorizationQuery, adaptersQuery, qk } from '@/api/queries'
import { SchemaForm } from '@/components/schema-form'
import { PageHeading } from '@/components/page-heading'
import { StatusBadge } from '@/components/status-badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Confirm } from '@/components/ui/confirm'
import { EmptyState, ErrorState, LoadingState } from '@/components/query-state'
import { useToast } from '@/components/ui/use-toast'
import { resourceLabel } from '@/lib/utils'
import { errorMessage } from '@/lib/errors'
import { WorkflowPromptPanel } from '@/components/workflow/prompt-panel'
import type { RecordingDetail, Schema, WorkflowProgress } from '@/types/api'
import { AdapterMark } from '@/components/adapter/adapter-mark'
import { useI18n } from '@/i18n/provider'

export function WorkflowDetailPage() {
  const { workflowId } = useParams({ from: '/workflows/$workflowId' })
  const { t } = useI18n()
  const client = useQueryClient()
  const navigate = useNavigate()
  const { toast } = useToast()
  const workflow = useQuery({ queryKey: qk.workflow(workflowId), queryFn: () => workflowsAPI.get(workflowId), retry: false })
  const authorization = useQuery(accountAuthorizationQuery)
  const adapters = useQuery(adaptersQuery)
  const [errorText, setErrorText] = useState('')
  const cancel = useMutation({ mutationFn: () => workflowsAPI.cancel(workflowId), onSuccess: async () => { toast(t('workflow.detail.cancelled')); await client.invalidateQueries({ queryKey: qk.workflows }); await client.invalidateQueries({ queryKey: qk.workflowHistory }); await navigate({ to: '/workflows' }) }, onError: error => toast(t('workflow.detail.cancelFailed'), errorMessage(error), 'error') })
  const proceed = useMutation({ mutationFn: (body: { values: Record<string, unknown>; secrets: Record<string, string>; persist_fields: string[] }) => workflowsAPI.continue(workflowId, body), onSuccess: async (result: WorkflowProgress | RecordingDetail) => {
    if ('id' in result) { toast(t('workflow.detail.startedRecording')); await client.invalidateQueries({ queryKey: qk.dashboard }); await client.invalidateQueries({ queryKey: ['recordings'] }); await navigate({ to: '/recordings/$recordingId', params: { recordingId: result.id } }); return }
    setErrorText(''); client.setQueryData(qk.workflow(workflowId), result); await client.invalidateQueries({ queryKey: qk.workflows }); await client.invalidateQueries({ queryKey: qk.workflowHistory }); toast(t('workflow.detail.updated'))
  }, onError: error => setErrorText(errorMessage(error)) })
  if (workflow.isLoading) return <LoadingState />
  if (workflow.error || !workflow.data) return <div className="page-enter"><ErrorState message={workflow.error ? errorMessage(workflow.error) : t('workflow.detail.missing')} retry={() => void workflow.refetch()} /><Link to="/workflows"><Button variant="outline" className="mt-3"><ArrowLeft className="h-4 w-4" />{t('workflow.detail.back')}</Button></Link></div>
  const item = workflow.data
  const challengeSchema: Schema = item.challenge?.schema ?? { fields: [] }
  const prompt = item.challenge?.prompt
  const submit = (result: { values: Record<string, unknown>; secrets: Record<string, string>; persistFields: string[] }) => proceed.mutate({ values: result.values, secrets: result.secrets, persist_fields: result.persistFields })
  const terminalPrompt = prompt?.type === 'complete' || prompt?.type === 'error'
  const canPersist = authorization.data?.permissions?.includes('plugin.configure') ?? false
  const requiresPersistence = challengeSchema.fields.some(field => field.persistence?.mode === 'required')
  const adapter = adapters.data?.find(candidate => candidate.status.id === item.adapter_id)
  return <div className="page-enter">
    <PageHeading title={t('workflow.detail.title')} actions={<><Link to="/workflows"><Button variant="outline"><ArrowLeft className="h-4 w-4" />{t('workflow.detail.back')}</Button></Link><Confirm trigger={<Button variant="destructive">{t('workflow.detail.cancel')}</Button>} title={t('workflow.detail.cancelTitle')} description={t('workflow.detail.cancelDescription')} confirmLabel={t('workflow.detail.cancel')} destructive onConfirm={() => cancel.mutate()} disabled={cancel.isPending || terminalPrompt} /></>} />
    <div className="grid gap-4 xl:grid-cols-[minmax(0,1fr)_320px]">
      <Card><CardHeader><CardTitle>{prompt?.title ?? t('workflow.detail.inputNeeded')}</CardTitle></CardHeader><CardContent>{prompt && <WorkflowPromptPanel prompt={prompt} />}{challengeSchema.fields.length && !terminalPrompt && !canPersist && requiresPersistence ? <p role="status" className="rounded-md bg-muted p-3 text-sm">{t('workflow.detail.adminRequired')}</p> : challengeSchema.fields.length && !terminalPrompt ? <SchemaForm key={`${workflowId}-${item.state}`} schema={challengeSchema} mode="challenge" allowPersistence={canPersist} submitLabel={t('workflow.detail.continue')} onSubmit={submit} busy={proceed.isPending} /> : item.state === 'resolved' || prompt?.type === 'complete' ? <EmptyState title={t('workflow.detail.complete')} /> : terminalPrompt ? null : <EmptyState title={t('workflow.detail.noInputs')} description={t('workflow.detail.noInputsHelp')} />}{errorText && <p role="alert" className="mt-4 rounded-md bg-destructive/10 p-3 text-sm text-destructive">{errorText}</p>}{proceed.isPending && <p role="status" className="mt-3 text-xs text-muted-foreground">{t('workflow.detail.continuing')}</p>}</CardContent></Card>
      <Card><CardHeader><CardTitle>{t('workflow.detail.status')}</CardTitle></CardHeader><CardContent><div className="mb-3"><AdapterMark adapter={adapter} adapterId={item.adapter_id} showName size="sm" /></div><StatusBadge state={item.state} /><dl className="mt-4 space-y-3">{[[t('workflow.detail.resource'), item.resource ? resourceLabel(item.resource) : t('workflow.detail.waitingResource')], [t('workflow.detail.id'), item.workflow_id]].map(([key, value]) => <div key={key}><dt className="text-[11px] text-muted-foreground">{key}</dt><dd className="mt-1 break-all text-xs font-medium">{value}</dd></div>)}</dl><details className="mt-4 border-t border-border pt-3"><summary className="cursor-pointer text-xs font-medium">{t('workflow.detail.security')}</summary><p className="mt-2 text-xs leading-5 text-muted-foreground">{t('workflow.detail.securityHelp')}</p></details></CardContent></Card>
    </div>
  </div>
}
