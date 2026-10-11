import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Check, Download, RotateCcw, Search, Sparkles } from 'lucide-react'
import { runtimeUpdateAPI } from '@/api'
import { qk, runtimeUpdateQuery } from '@/api/queries'
import { errorMessage } from '@/lib/errors'
import { formatDateTime } from '@/lib/formatting'
import { useI18n } from '@/i18n/provider'
import type { TranslationKey } from '@/i18n/catalog'
import type { RuntimeGenerationSummary, RuntimeReleaseSummary, RuntimeUpdateStatus } from '@/types/api'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { ErrorState, LoadingState } from '@/components/query-state'

type RuntimeUpdateAction = 'check' | 'stage' | 'activate' | 'rollback'

const verificationKeys: Record<RuntimeUpdateStatus['verification_state'], TranslationKey> = {
  unknown: 'runtime.verification.unknown', not_checked: 'runtime.verification.not_checked', checking: 'runtime.verification.checking', verified: 'runtime.verification.verified', failed: 'runtime.verification.failed',
}
const generationKeys: Record<string, TranslationKey> = {
  staging: 'runtime.generation.staging', verified: 'runtime.generation.verified', ready: 'runtime.generation.ready', active: 'runtime.generation.active', draining: 'runtime.generation.draining', retired: 'runtime.generation.retired', failed: 'runtime.generation.failed',
}
const channelKeys: Record<string, TranslationKey> = { stable: 'runtime.channel.stable', prerelease: 'runtime.channel.prerelease', development: 'runtime.channel.development' }
const unavailableKeys: Record<string, TranslationKey> = {
  development_build: 'runtime.unavailable.development_build', source_unavailable: 'runtime.unavailable.source_unavailable', unsupported_platform: 'runtime.unavailable.unsupported_platform',
  trust_key_unavailable: 'runtime.unavailable.trust_key_unavailable', updates_disabled: 'runtime.unavailable.updates_disabled', host_update_required: 'runtime.unavailable.host_update_required',
}
const failureKeys: Record<string, TranslationKey> = {
  update_unavailable: 'runtime.failure.update_unavailable', update_check_failed: 'runtime.failure.update_check_failed', no_update_available: 'runtime.failure.no_update_available', operation_conflict: 'runtime.failure.operation_conflict',
  verification_failed: 'runtime.failure.verification_failed', candidate_not_ready: 'runtime.failure.candidate_not_ready', release_incompatible: 'runtime.failure.release_incompatible', stage_failed: 'runtime.failure.stage_failed',
  activation_failed: 'runtime.failure.activation_failed', rollback_unavailable: 'runtime.failure.rollback_unavailable', rollback_failed: 'runtime.failure.rollback_failed', internal_error: 'runtime.failure.internal_error',
}

export function RuntimeUpdates() {
  const { t } = useI18n()
  const client = useQueryClient()
  const status = useQuery({ ...runtimeUpdateQuery, enabled: true })
  const action = useMutation({
    mutationFn: (operation: RuntimeUpdateAction) => runtimeUpdateAPI[operation](),
    onSuccess: result => { client.setQueryData(qk.runtimeUpdate, result) },
  })
  const value = status.data
  const unavailableReason = value?.update_unavailable_reason
  const unavailable = unavailableReason ? t(unavailableKeys[unavailableReason] ?? 'runtime.unavailable.unknown') : undefined
  const developmentBuild = value?.application.version === 'dev'
  const canUpdate = Boolean(value && !unavailable && !developmentBuild)
  const pending = action.isPending
  const perform = (operation: RuntimeUpdateAction) => action.mutate(operation)

  return <div role="tabpanel" id="settings-panel-updates" aria-labelledby="settings-tab-updates" tabIndex={0} className="space-y-4">
    <Card>
      <CardHeader><CardTitle className="flex items-center gap-2"><Sparkles className="h-4 w-4 text-primary" />{t('runtime.title')}</CardTitle><p className="text-xs leading-5 text-muted-foreground">{t('runtime.description')}</p></CardHeader>
      <CardContent>
        {status.isLoading ? <LoadingState /> : status.error || !value ? <ErrorState message={status.error ? errorMessage(status.error) : t('runtime.statusUnavailable')} retry={() => void status.refetch()} /> : <div className="space-y-5">
          <div className="grid gap-3 sm:grid-cols-2">
            <IdentityCard title={t('runtime.currentApplication')} identity={value.application} />
            <IdentityCard title={t('runtime.host')} identity={value.host} />
          </div>

          <div className="grid gap-3 lg:grid-cols-3">
            <ReleaseCard title={t('runtime.availableVersion')} release={value.available_release} empty={t('runtime.noReleaseChecked')} />
            <ReleaseCard title={t('runtime.stagedVersion')} release={value.staged_release} empty={t('runtime.noCandidate')} />
            <ReleaseCard title={t('runtime.rollbackVersion')} release={value.previous_release} empty={t('runtime.noPrevious')} />
          </div>

          <div className="flex flex-wrap items-center gap-2">
            <Badge tone={value.verification_state === 'verified' ? 'green' : value.verification_state === 'failed' ? 'red' : value.verification_state === 'checking' ? 'amber' : 'neutral'}>{t('runtime.verificationLabel')} · {t(verificationKeys[value.verification_state])}</Badge>
            {value.updates_available && <Badge tone="blue">{t('runtime.updatesAvailable')}</Badge>}
            {value.last_failure_code && <span className="text-xs text-destructive">{t(failureKeys[value.last_failure_code] ?? 'runtime.failure.unknown')}</span>}
          </div>

          {unavailable && <p className="rounded-md border border-amber-500/30 bg-amber-500/5 p-3 text-xs leading-5 text-amber-800 dark:text-amber-200">{unavailable}</p>}
          {!unavailable && developmentBuild && <p className="rounded-md border border-muted bg-muted/40 p-3 text-xs leading-5 text-muted-foreground">{t('runtime.unavailable.development_build')}</p>}
          {action.error && <p role="alert" className="rounded-md border border-destructive/30 bg-destructive/5 p-3 text-xs leading-5 text-destructive">{errorMessage(action.error)}</p>}

          <div className="flex flex-wrap gap-2">
            <Button variant="outline" size="sm" onClick={() => perform('check')} disabled={!canUpdate || pending}><Search className="h-3.5 w-3.5" />{t('runtime.check')}</Button>
            <Button variant="outline" size="sm" onClick={() => perform('stage')} disabled={!canUpdate || !value.available_release || pending}><Download className="h-3.5 w-3.5" />{t('runtime.stage')}</Button>
            <Button size="sm" onClick={() => perform('activate')} disabled={!canUpdate || !value.staged_release || value.verification_state !== 'verified' || pending}><Check className="h-3.5 w-3.5" />{t('runtime.activate')}</Button>
            <Button variant="outline" size="sm" onClick={() => perform('rollback')} disabled={!canUpdate || !value.previous_release || pending}><RotateCcw className="h-3.5 w-3.5" />{t('runtime.rollback')}</Button>
          </div>
          {pending && <p role="status" className="text-xs text-muted-foreground">{t('runtime.pending')}</p>}
          <p className="text-xs leading-5 text-muted-foreground">{t('runtime.continuity')}</p>
        </div>}
      </CardContent>
    </Card>

    {value && <Card>
      <CardHeader><CardTitle>{t('runtime.runningGenerations')}</CardTitle><p className="text-xs text-muted-foreground">{t('runtime.runningGenerationsHelp')}</p></CardHeader>
      <CardContent><GenerationList active={value.active_generations ?? []} draining={value.draining_generations ?? []} activeControl={value.active_control} defaultEngine={value.default_engine} /></CardContent>
    </Card>}
    {value?.handover_diagnostic && <HandoverDiagnostic diagnostic={value.handover_diagnostic} />}
  </div>
}

const knownDiagnosticPhases = new Set(['prepare_target', 'handover'])
const knownDiagnosticReasons = new Set(['target_start_timeout', 'target_not_ready', 'capability_mismatch', 'ipc_unavailable', 'process_exit', 'storage_binding_failed', 'unknown'])

function HandoverDiagnostic({ diagnostic }: { diagnostic: NonNullable<RuntimeUpdateStatus['handover_diagnostic']> }) {
  const { t } = useI18n()
  const phaseKey: TranslationKey = knownDiagnosticPhases.has(diagnostic.phase) ? `diagnostic.phase.${diagnostic.phase}` as TranslationKey : 'diagnostic.phase.prepare_target'
  const knownReason = knownDiagnosticReasons.has(diagnostic.reason_code) ? diagnostic.reason_code : 'unknown'
  const reasonKey: TranslationKey = `diagnostic.reason.${knownReason}` as TranslationKey
  const safeIdentifier = (value: string) => /^[A-Za-z0-9._:-]{1,128}$/.test(value) ? value : '—'
  const safeVersion = /^[A-Za-z0-9.+_-]{1,80}$/.test(diagnostic.target_version) ? diagnostic.target_version : '—'
  return <Card>
    <CardHeader><CardTitle>{t('diagnostic.title')}</CardTitle><p className="text-xs leading-5 text-muted-foreground">{t('diagnostic.help')}</p></CardHeader>
    <CardContent>
      <dl className="grid gap-x-5 gap-y-3 text-xs sm:grid-cols-2 lg:grid-cols-3">
        <DiagnosticField label={t('diagnostic.phase')} value={t(phaseKey)} />
        <DiagnosticField label={t('diagnostic.reason')} value={`${t(reasonKey)} · ${knownReason}`} />
        <DiagnosticField label={t('diagnostic.targetVersion')} value={safeVersion} />
        <DiagnosticField label={t('diagnostic.occurredAt')} value={formatDateTime(diagnostic.occurred_at)} />
        <DiagnosticField label={t('diagnostic.sourceGeneration')} value={safeIdentifier(diagnostic.source_generation_id)} />
        <DiagnosticField label={t('diagnostic.targetGeneration')} value={safeIdentifier(diagnostic.target_generation_id)} />
        <DiagnosticField label={t('diagnostic.recording')} value={safeIdentifier(diagnostic.recording_id)} />
        <DiagnosticField label={t('diagnostic.reconcileState')} value={t(diagnostic.reconcile_state === 'pending' ? 'diagnostic.reconcile.pending' : 'diagnostic.reconcile.succeeded')} />
        <DiagnosticField label={t('diagnostic.retryable')} value={diagnostic.retryable ? t('diagnostic.yes') : t('diagnostic.no')} />
        <DiagnosticField label={t('diagnostic.recoverable')} value={diagnostic.recoverable ? t('diagnostic.yes') : t('diagnostic.no')} />
        <DiagnosticField label={t('diagnostic.ownership')} value={diagnostic.ownership_retained ? t('diagnostic.ownerRetained') : t('diagnostic.no')} />
        {diagnostic.resolved_at && <DiagnosticField label={t('diagnostic.resolvedAt')} value={formatDateTime(diagnostic.resolved_at)} />}
      </dl>
    </CardContent>
  </Card>
}

function DiagnosticField({ label, value }: { label: string; value: string }) {
  return <div className="min-w-0"><dt className="text-muted-foreground">{label}</dt><dd className="mt-1 break-all font-medium">{value}</dd></div>
}

function IdentityCard({ title, identity }: { title: string; identity: RuntimeUpdateStatus['application'] }) {
  const { t } = useI18n()
  return <div className="min-w-0 rounded-md border border-border p-3"><p className="text-xs text-muted-foreground">{title}</p><p className="mt-1 truncate text-base font-semibold" title={identity.version}>{identity.version}</p><p className="mt-1 break-all font-mono text-[11px] text-muted-foreground">{identity.commit}</p><p className="mt-1 text-xs text-muted-foreground">{t('runtime.channelProtocol', { channel: t(channelKeys[identity.release_channel] ?? 'runtime.channel.other'), version: identity.runtime_protocol_version })}</p></div>
}

function ReleaseCard({ title, release, empty }: { title: string; release?: RuntimeReleaseSummary; empty: string }) {
	const { t } = useI18n()
	return <div className="min-w-0 rounded-md border border-border p-3"><p className="text-xs text-muted-foreground">{title}</p>{release ? <><p className="mt-1 truncate text-sm font-semibold" title={release.version}>{release.version}</p><p className="mt-1 break-all font-mono text-[11px] text-muted-foreground">{release.commit}</p><p className="mt-1 text-xs text-muted-foreground">{t('runtime.channelBuild', { channel: t(channelKeys[release.release_channel] ?? 'runtime.channel.other'), time: release.build_time })}</p>{release.notes_summary && <div className="mt-2 border-t border-border pt-2"><p className="text-[11px] font-medium text-muted-foreground">{t('runtime.releaseNotes')}</p><p className="mt-1 line-clamp-3 whitespace-normal text-xs leading-5 text-foreground">{release.notes_summary}</p></div>}</> : <p className="mt-2 text-xs text-muted-foreground">{empty}</p>}</div>
}

function GenerationList({ active, draining, activeControl, defaultEngine }: {
  active: RuntimeGenerationSummary[]; draining: RuntimeGenerationSummary[]
  activeControl?: RuntimeGenerationSummary; defaultEngine?: RuntimeGenerationSummary
}) {
  const { t } = useI18n()
  const stateName = (state: string) => t(generationKeys[state] ?? 'runtime.generation.unknown')
  const rows = [...active.map(item => ({ ...item, displayState: stateName(item.state) })), ...draining.map(item => ({ ...item, displayState: stateName(item.state) }))]
  for (const item of [activeControl, defaultEngine]) {
    if (item && !rows.some(row => row.id === item.id)) rows.push({ ...item, displayState: stateName(item.state) })
  }
  if (!rows.length) return <p className="text-sm text-muted-foreground">{t('runtime.noGenerations')}</p>
  return <div className="divide-y divide-border">{rows.map(item => <div key={item.id} className="flex flex-wrap items-center gap-x-3 gap-y-1 py-2.5 text-xs"><span className="min-w-0 flex-1 truncate font-medium">{item.version} <span className="font-normal text-muted-foreground">· {item.displayState}</span></span><span className="text-muted-foreground">{t('runtime.recordings', { count: item.active_recordings })}</span><Badge tone={item.id === defaultEngine?.id ? 'blue' : item.state === 'draining' ? 'amber' : 'neutral'}>{item.id === defaultEngine?.id ? t('runtime.defaultEngine') : stateName(item.state)}</Badge></div>)}</div>
}
