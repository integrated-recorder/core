import { useMemo } from 'react'
import { ExternalLink, Workflow } from 'lucide-react'
import type { WorkflowPrompt } from '@/types/api'
import { safeExternalURL } from '@/lib/urls'
import { useI18n } from '@/i18n/provider'

export function WorkflowPromptPanel({ prompt }: { prompt: WorkflowPrompt }) {
  const { t } = useI18n()
  const data = useMemo(() => prompt.data && typeof prompt.data === 'object' && !Array.isArray(prompt.data) ? prompt.data as Record<string, unknown> : {}, [prompt.data])
  const url = safeExternalURL(typeof data.url === 'string' ? data.url : '')
  const message = prompt.message || prompt.title || ''
  if (prompt.type === 'navigate' || prompt.type === 'action') {
    const label = prompt.type === 'navigate' ? t('workflow.prompt.openPage') : typeof data.label === 'string' && data.label.trim() ? data.label : t('workflow.prompt.continue')
    return <section className="mb-4 rounded-md bg-blue-50 p-3 text-sm dark:bg-blue-950/20" aria-label={prompt.type === 'navigate' ? t('workflow.prompt.openPage') : t('workflow.prompt.continue')}>
      <div className="flex gap-2"><Workflow className="mt-0.5 h-4 w-4 shrink-0 text-primary" /><div className="min-w-0"><p>{message || (prompt.type === 'navigate' ? t('workflow.prompt.externalHelp') : t('workflow.prompt.actionHelp'))}</p>
        {url ? <a className="mt-2 inline-flex items-center gap-1 font-medium text-primary underline" href={url} target="_blank" rel="noopener noreferrer">{label}<ExternalLink className="h-3.5 w-3.5" /></a> : prompt.type === 'action' && <p className="mt-2 text-xs text-muted-foreground">{t('workflow.prompt.noSafeURL')}</p>}
      </div></div>
    </section>
  }
  if (prompt.type === 'display' || prompt.type === 'status' || prompt.type === 'prompt' || prompt.type === 'secret_prompt') return <div className="mb-4 flex gap-2 rounded-md bg-muted p-3 text-sm" role="status"><Workflow className="mt-0.5 h-4 w-4 shrink-0 text-primary" /><span>{message || t(promptFallback[prompt.type])}</span></div>
  if (prompt.type === 'complete') return <div className="mb-4 rounded-md bg-emerald-50 p-3 text-sm text-emerald-900 dark:bg-emerald-950/30 dark:text-emerald-100" role="status">{message || t('workflow.prompt.completed')}</div>
  if (prompt.type === 'error') return <div className="mb-4 rounded-md bg-destructive/10 p-3 text-sm text-destructive" role="alert">{message || t('workflow.prompt.failed')}</div>
  return <div className="mb-4 rounded-md bg-muted p-3 text-sm" role="status">{message || t('workflow.prompt.unsupported')}</div>
}

const promptFallback: Record<string, 'workflow.prompt.enterRequested' | 'workflow.prompt.enterSecret' | 'workflow.prompt.information' | 'workflow.prompt.status'> = {
  prompt: 'workflow.prompt.enterRequested',
  secret_prompt: 'workflow.prompt.enterSecret',
  display: 'workflow.prompt.information',
  status: 'workflow.prompt.status',
}
