import { useEffect, useRef, useState, type FormEvent } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useNavigate } from '@tanstack/react-router'
import { AlertTriangle, ArrowLeft, ArrowRight, Check, CheckCircle2, HardDrive, LoaderCircle, RefreshCw, Server } from 'lucide-react'
import { adaptersAPI, authAPI, dashboardAPI, setupAPI } from '@/api'
import { installationQuery, qk, sessionQuery } from '@/api/queries'
import { AdapterMark } from '@/components/adapter/adapter-mark'
import { AppBrand } from '@/components/layout/app-brand'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { useI18n } from '@/i18n/provider'
import type { TranslationKey } from '@/i18n/catalog'
import { formatBytes } from '@/lib/utils'
import { passwordLengthStatus } from '@/lib/credentials'
import type { Adapter, InstallationStatus, SetupStorageTest } from '@/types/api'

type SetupStep = 'welcome' | 'administrator' | 'storage' | 'adapters' | 'review' | 'complete'
type VisibleSetupStep = SetupStep | 'resume' | 'recovery'

const stepList: { id: Exclude<SetupStep, 'complete'>; label: TranslationKey }[] = [
  { id: 'welcome', label: 'setup.step.welcome' },
  { id: 'administrator', label: 'setup.step.administrator' },
  { id: 'storage', label: 'setup.step.storage' },
  { id: 'adapters', label: 'setup.step.adapters' },
  { id: 'review', label: 'setup.step.review' },
]

const stepHeadingKeys: Record<VisibleSetupStep, TranslationKey> = {
  welcome: 'setup.heading.welcome', administrator: 'setup.heading.administrator', storage: 'setup.heading.storage',
  adapters: 'setup.heading.adapters', review: 'setup.heading.review', complete: 'setup.heading.complete',
  resume: 'setup.heading.resume', recovery: 'setup.heading.recovery',
}

const stepDescriptionKeys: Record<Exclude<VisibleSetupStep, 'welcome'>, TranslationKey> = {
  administrator: 'setup.description.administrator', storage: 'setup.description.storage',
  adapters: 'setup.description.adapters', review: 'setup.description.review', complete: 'setup.description.complete',
  resume: 'setup.description.resume', recovery: 'setup.description.recovery',
}

const adapterStateKeys: Record<string, TranslationKey> = {
  running: 'setup.adapterState.ready', ready: 'setup.adapterState.ready', configuration_required: 'setup.adapterState.configurationRequired',
  disabled: 'setup.adapterState.disabled', unavailable: 'setup.adapterState.unavailable', failed: 'setup.adapterState.failed',
  rejected: 'setup.adapterState.rejected', restarting: 'setup.adapterState.restarting', starting: 'setup.adapterState.starting',
  stopped: 'setup.adapterState.stopped', unknown: 'setup.adapterState.unknown',
}

function isAuthenticatedSession(session: { auth_enabled: boolean; authenticated: boolean } | undefined, authDisabled: boolean) {
  return authDisabled || session?.authenticated === true
}

function setupPasswordError(password: string, confirmation: string): TranslationKey | undefined {
  const length = passwordLengthStatus(password)
  if (length === 'too_short') return 'setup.error.passwordTooShort'
  if (length === 'too_long') return 'setup.error.passwordTooLong'
  if (password !== confirmation) return 'setup.error.passwordMismatch'
  return undefined
}

function initialStep(status?: InstallationStatus): SetupStep {
  if (status?.state === 'setup_in_progress') return 'storage'
  if (status?.administrator_configured) return 'storage'
  return 'welcome'
}

function releaseChannelKey(channel: string): TranslationKey {
  if (channel === 'stable') return 'setup.releaseChannel.stable'
  if (channel === 'prerelease') return 'setup.releaseChannel.prerelease'
  if (channel === 'development') return 'setup.releaseChannel.development'
  return 'setup.releaseChannel.other'
}

export function SetupPage() {
  const { t } = useI18n()
  const client = useQueryClient()
  const navigate = useNavigate()
  const installation = useQuery(installationQuery)
  // This request initializes the existing CSRF mechanism before credential submission.
  const session = useQuery(sessionQuery)
  const status = installation.data
  const authDisabled = status?.auth_disabled ?? false
  const authenticated = isAuthenticatedSession(session.data, authDisabled)
  const [step, setStep] = useState<SetupStep>(() => initialStep(status))
  const administratorForm = useRef<HTMLFormElement>(null)
  const resumeForm = useRef<HTMLFormElement>(null)
  const [formError, setFormError] = useState<TranslationKey | undefined>()
  const [isBootstrapping, setIsBootstrapping] = useState(false)
  const [isLoggingIn, setIsLoggingIn] = useState(false)
  const [beginError, setBeginError] = useState<TranslationKey | undefined>()
  const [storageResult, setStorageResult] = useState<SetupStorageTest>()

  const begin = useMutation({
    mutationFn: setupAPI.begin,
    onSuccess: result => { client.setQueryData(qk.installation, result); setBeginError(undefined); setStep('storage') },
    onError: () => setBeginError('setup.error.beginFailed'),
  })
  const infoQuery = useQuery({ queryKey: qk.info, queryFn: dashboardAPI.info, enabled: authenticated && status?.state === 'setup_in_progress' })
  const adaptersQuery = useQuery({ queryKey: qk.adapters, queryFn: adaptersAPI.list, enabled: authenticated && status?.state === 'setup_in_progress' })
  const storageTest = useMutation({ mutationFn: setupAPI.storageTest, onSuccess: setStorageResult })
  const completion = useMutation({
    mutationFn: setupAPI.complete,
    onSuccess: result => { client.setQueryData(qk.installation, result); setStep('complete') },
  })

  const resumeLogin = Boolean(status?.administrator_configured && !authenticated && !status.auth_disabled)
  const incompleteButAdminExists = status?.state === 'uninitialized' && status.administrator_configured
  const visibleStep: VisibleSetupStep = status?.state === 'recovery_required'
    ? 'recovery'
    : resumeLogin
      ? 'resume'
      : incompleteButAdminExists && authenticated
        ? 'resume'
        : status?.state === 'setup_in_progress' && authenticated
          ? step === 'welcome' || step === 'administrator' ? 'storage' : step
          : step

  useEffect(() => {
    // Show the first-success screen after setup. Redirect ready installations visiting /setup.
    if (status?.state === 'ready' && step !== 'complete') void navigate({ to: '/' })
  }, [navigate, status?.state, step])

  const submitAdministrator = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault()
    setFormError(undefined)
    const data = new FormData(event.currentTarget)
    const login = String(data.get('username') ?? '').trim()
    const password = String(data.get('password') ?? '')
    const confirmation = String(data.get('password_confirmation') ?? '')
    if (!login) return setFormError('login.usernameRequired')
    const passwordError = setupPasswordError(password, confirmation)
    if (passwordError) return setFormError(passwordError)
    if (isBootstrapping) return
    setIsBootstrapping(true)
    try {
      const result = await authAPI.bootstrap(login, password)
      client.setQueryData(qk.session, result)
      setFormError(undefined)
      try {
        const next = await setupAPI.begin()
        client.setQueryData(qk.installation, next)
        setBeginError(undefined)
        setStep('storage')
      } catch {
        setBeginError('setup.error.beginFailed')
        await client.invalidateQueries({ queryKey: qk.installation })
      }
    } catch {
      setFormError('setup.error.bootstrapFailed')
    } finally {
      setIsBootstrapping(false)
    }
  }

  const submitResumeLogin = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault()
    setFormError(undefined)
    const data = new FormData(event.currentTarget)
    const login = String(data.get('username') ?? '').trim()
    const password = String(data.get('password') ?? '')
    if (!login) return setFormError('login.usernameRequired')
    if (!password) return setFormError('setup.error.passwordRequired')
    if (isLoggingIn) return
    setIsLoggingIn(true)
    try {
      const result = await authAPI.login(login, password)
      client.setQueryData(qk.session, result)
      setFormError(undefined)
    } catch {
      setFormError('setup.error.loginFailed')
    } finally {
      setIsLoggingIn(false)
    }
  }

  if (installation.error || session.error) {
    return <SetupFrame><SetupError title={t('setup.error.unavailableTitle')} message={t('setup.error.unavailableMessage')} onRetry={() => { void installation.refetch(); void session.refetch() }} /></SetupFrame>
  }
  if (installation.isLoading || !status || session.isLoading) {
    return <SetupFrame><div className="flex items-center gap-3 text-sm text-muted-foreground" role="status"><LoaderCircle className="h-4 w-4 animate-spin" />{t('setup.loading')}</div></SetupFrame>
  }

  if (visibleStep === 'recovery') {
    return <SetupFrame><section className="rounded-xl border border-amber-300/70 bg-card p-6 shadow-sm sm:p-8" role="alert">
      <div className="grid h-11 w-11 place-items-center rounded-full bg-amber-500/10 text-amber-700 dark:text-amber-300"><AlertTriangle className="h-5 w-5" /></div>
      <h1 className="mt-5 text-xl font-semibold">{t('setup.error.unavailableTitle')}</h1>
      <p className="mt-2 text-sm leading-6 text-muted-foreground">{t('setup.error.recoveryMessage')}</p>
      {status.diagnostic_code && <p className="mt-4 text-xs text-muted-foreground">{t('setup.diagnosticCode')}: <code className="rounded bg-muted px-1.5 py-1 font-mono">{status.diagnostic_code}</code></p>}
    </section></SetupFrame>
  }

  if (visibleStep === 'resume') {
    if (resumeLogin) return <SetupFrame><section className="mx-auto w-full max-w-md rounded-xl border border-border bg-card p-6 shadow-sm sm:p-8">
      <Badge tone="blue">{t('setup.resume.badge')}</Badge>
      <h1 className="mt-4 text-2xl font-semibold">{t('setup.resume.loginTitle')}</h1>
      <p className="mt-2 text-sm leading-6 text-muted-foreground">{t('setup.resume.loginDescription')}</p>
      <form ref={resumeForm} className="mt-6 space-y-4" onSubmit={submitResumeLogin}>
        <div className="space-y-2"><Label htmlFor="resume-username">{t('setup.resume.usernameLabel')}</Label><Input id="resume-username" type="text" name="username" autoComplete="username" autoCapitalize="none" spellCheck={false} autoCorrect="off" required /></div>
        <div className="space-y-2"><Label htmlFor="resume-password">{t('setup.resume.passwordLabel')}</Label><Input id="resume-password" type="password" name="password" autoComplete="current-password" required /></div>
        {formError && <p className="text-sm text-destructive" role="alert">{t(formError)}</p>}
        <Button type="submit" className="w-full" disabled={isLoggingIn}>{isLoggingIn ? t('setup.resume.loggingIn') : t('setup.resume.loginAction')}<ArrowRight className="h-4 w-4" /></Button>
      </form>
    </section></SetupFrame>
    return <SetupFrame><section className="mx-auto w-full max-w-md rounded-xl border border-border bg-card p-6 shadow-sm sm:p-8">
      <Badge tone="amber">{t('setup.resume.badge')}</Badge>
      <h1 className="mt-4 text-2xl font-semibold">{t('setup.resume.continueTitle')}</h1>
      <p className="mt-2 text-sm leading-6 text-muted-foreground">{t('setup.resume.continueDescription')}</p>
      {beginError && <p role="alert" className="mt-4 rounded-md bg-destructive/10 p-3 text-sm text-destructive">{t(beginError)}</p>}
      <Button className="mt-6 w-full" onClick={() => begin.mutate()} disabled={begin.isPending}>{begin.isPending ? t('setup.resume.preparing') : t('setup.resume.continueAction')}<ArrowRight className="h-4 w-4" /></Button>
    </section></SetupFrame>
  }

  if (visibleStep === 'complete') return <SetupFrame><section className="mx-auto w-full max-w-xl rounded-xl border border-border bg-card p-6 text-center shadow-sm sm:p-9">
    <div className="mx-auto grid h-14 w-14 place-items-center rounded-full bg-emerald-500/10 text-emerald-600"><CheckCircle2 className="h-7 w-7" /></div>
    <h1 className="mt-5 text-2xl font-semibold">{t('setup.complete.title')}</h1>
    <p className="mt-2 text-sm text-muted-foreground">{t('setup.complete.description')}</p>
    <div className="mt-6 grid gap-2 rounded-lg bg-muted/50 p-4 text-left text-sm sm:grid-cols-2">
      <span className="text-muted-foreground">{t('setup.complete.storage')}</span><span>{t('setup.complete.normal')}</span>
      <span className="text-muted-foreground">{t('setup.complete.administrator')}</span><span>{t('setup.complete.configured')}</span>
      <span className="text-muted-foreground">{t('setup.complete.runtime')}</span><span>{t('setup.complete.normal')}</span>
    </div>
    <Button className="mt-6 w-full" onClick={() => navigate({ to: '/' })}>{t('setup.complete.open')}<ArrowRight className="h-4 w-4" /></Button>
  </section></SetupFrame>

  const activeIndex = Math.max(0, stepList.findIndex(item => item.id === visibleStep))
  const adapters = adaptersQuery.data ?? []
  const adapterWarnings = adapters.filter(adapter => !['ready', 'running'].includes(adapter.status.state))

  return <SetupFrame>
    <div className="mx-auto flex w-full max-w-5xl flex-col gap-4">
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-base">{t('setup.progressTitle')}</CardTitle>
          <CardDescription>{t('setup.progressDescription')}</CardDescription>
        </CardHeader>
        <CardContent className="pt-1">
          <nav aria-label={t('setup.progressAria')}>
            <ol className="flex w-full items-start">
              {stepList.map((item, index) => {
                const current = item.id === visibleStep
                const complete = index < activeIndex
                return <li key={item.id} aria-current={current ? 'step' : undefined} data-step-state={current ? 'current' : complete ? 'complete' : 'upcoming'} className="relative min-w-0 flex-1">
                  {index < stepList.length - 1 && <span aria-hidden="true" data-step-connector="true" className={`pointer-events-none absolute left-[calc(50%_+_12px)] top-3 z-0 h-px w-[calc(100%_-_24px)] ${index < activeIndex ? 'bg-emerald-600' : 'bg-border'}`} />}
                  <span data-step-indicator="true" className="relative z-10 flex min-w-0 flex-col items-center text-center">
                    <span data-step-marker="true" aria-hidden="true" className={`relative z-10 grid h-6 w-6 shrink-0 place-items-center rounded-full border text-xs ${complete ? 'border-emerald-600 bg-emerald-600 text-white' : current ? 'border-primary bg-card font-semibold text-primary' : 'border-border bg-card text-muted-foreground'}`}>{complete ? <Check className="h-3.5 w-3.5" /> : index + 1}</span>
                    <span className={`mt-2 max-w-full break-words text-[10px] leading-tight sm:text-xs ${current ? 'font-semibold text-primary' : complete ? 'text-foreground' : 'text-muted-foreground'}`}>{t(item.label)}</span>
                  </span>
                </li>
              })}
            </ol>
          </nav>
        </CardContent>
      </Card>
      <section className="min-w-0">
        <Card className="overflow-hidden">
          <CardHeader className="border-b border-border bg-muted/20"><CardTitle className="text-lg">{t(stepHeadingKeys[visibleStep])}</CardTitle>{visibleStep !== 'welcome' && <p className="text-sm text-muted-foreground">{t(stepDescriptionKeys[visibleStep])}</p>}</CardHeader>
          <CardContent className="py-5">
            {visibleStep === 'welcome' && <div>
              <div className="flex justify-end"><Button onClick={() => status.state === 'uninitialized' && !status.auth_disabled ? setStep('administrator') : begin.mutate()} disabled={begin.isPending}>{begin.isPending ? t('setup.welcome.starting') : t('setup.welcome.start')}<ArrowRight className="h-4 w-4" /></Button></div>
            </div>}

            {visibleStep === 'administrator' && <form ref={administratorForm} id="owner-bootstrap-form" name="owner-bootstrap" method="post" autoComplete="on" className="w-full max-w-3xl space-y-5" onSubmit={submitAdministrator}>
              <div>
                <p className="text-sm leading-6 text-muted-foreground">{t('setup.owner.description')}</p>
              </div>
              <div className="space-y-2"><Label htmlFor="setup-username">{t('setup.owner.usernameLabel')}</Label><Input id="setup-username" type="text" name="username" autoComplete="username" autoCapitalize="none" spellCheck={false} autoCorrect="off" required /></div>
              <div className="grid gap-4 sm:grid-cols-2">
                <div className="space-y-2"><Label htmlFor="setup-password">{t('setup.owner.passwordLabel')}</Label><Input id="setup-password" type="password" name="password" autoComplete="new-password" required /></div>
                <div className="space-y-2"><Label htmlFor="setup-password-confirm">{t('setup.owner.confirmLabel')}</Label><Input id="setup-password-confirm" type="password" name="password_confirmation" autoComplete="new-password" required /></div>
              </div>
              <p className="text-xs text-muted-foreground">{t('setup.owner.passwordHelp')}</p>
              {formError && <p role="alert" className="rounded-md bg-destructive/10 p-3 text-sm text-destructive">{t(formError)}</p>}
              <div className="flex flex-col-reverse gap-2 sm:flex-row sm:justify-between">
                <Button type="button" variant="outline" onClick={() => setStep('welcome')}><ArrowLeft className="h-4 w-4" />{t('setup.owner.previous')}</Button>
                <Button type="submit" disabled={isBootstrapping || session.isLoading}>{isBootstrapping ? t('setup.owner.pending') : t('setup.owner.submit')}<ArrowRight className="h-4 w-4" /></Button>
              </div>
            </form>}

            {visibleStep === 'storage' && <div className="space-y-5">
              <div className="grid gap-3 sm:grid-cols-2">
                <DiagnosticValue icon={<HardDrive className="h-4 w-4" />} label={t('setup.storage.default')} value={storageResult ? storageResult.capacity_known && storageResult.free_bytes !== undefined ? t('setup.storage.capacityAvailable', { bytes: formatBytes(storageResult.free_bytes) }) : t('setup.storage.capacityUnknown') : t('setup.storage.capacityNotChecked')} />
                <DiagnosticValue icon={<Server className="h-4 w-4" />} label={t('setup.storage.runtime')} value={infoQuery.data ? `${infoQuery.data.version} · ${t(releaseChannelKey(status.release_channel))}` : infoQuery.isLoading ? t('common.loading') : status.version} />
              </div>
              <div className="rounded-lg border border-border p-4">
                <h3 className="text-sm font-semibold">{t('setup.storage.testTitle')}</h3>
                <p className="mt-1 text-xs leading-5 text-muted-foreground">{t('setup.storage.testDescription')}</p>
                {storageResult && <StorageTestResult result={storageResult} />}
                {storageTest.error && <p role="alert" className="mt-3 text-sm text-destructive">{t('setup.error.storageTestFailed')}</p>}
                <div className="mt-4 flex flex-wrap items-center gap-3"><Button variant="outline" onClick={() => storageTest.mutate()} disabled={storageTest.isPending}>{storageTest.isPending ? <LoaderCircle className="h-4 w-4 animate-spin" /> : <RefreshCw className="h-4 w-4" />}{storageTest.isPending ? t('setup.storage.testing') : storageResult ? t('setup.storage.retest') : t('setup.storage.runTest')}</Button></div>
              </div>
              {beginError && <p role="alert" className="text-sm text-destructive">{t(beginError)}</p>}
              <div className="flex flex-col-reverse gap-2 sm:flex-row sm:justify-between">
                <Button variant="outline" onClick={() => setStep('welcome')}><ArrowLeft className="h-4 w-4" />{t('setup.owner.previous')}</Button>
                <Button onClick={() => setStep('adapters')} disabled={storageTest.isPending || !storageResult || storageResult.status === 'error'}>{t('setup.storage.next')}<ArrowRight className="h-4 w-4" /></Button>
              </div>
            </div>}

            {visibleStep === 'adapters' && <div>
              <p className="text-sm leading-6 text-muted-foreground">{t('setup.adapters.description')}</p>
              {adaptersQuery.isLoading && <p className="mt-4 text-sm text-muted-foreground" role="status">{t('setup.adapters.loading')}</p>}
              {adaptersQuery.error && <div className="mt-4"><SetupError title={t('setup.adapters.errorTitle')} message={t('setup.error.adaptersFailed')} onRetry={() => void adaptersQuery.refetch()} /></div>}
              {!adaptersQuery.isLoading && !adaptersQuery.error && <div className="mt-4 space-y-2">{adapters.length ? adapters.map(adapter => <AdapterReadiness key={adapter.status.id} adapter={adapter} />) : <p className="rounded-lg border border-amber-300/60 bg-amber-50/60 p-4 text-sm text-amber-900 dark:border-amber-900 dark:bg-amber-950/20 dark:text-amber-200">{t('setup.adapters.empty')}</p>}</div>}
              {adapterWarnings.length > 0 && <p className="mt-4 flex items-start gap-2 rounded-lg bg-amber-500/10 p-3 text-xs leading-5 text-muted-foreground"><AlertTriangle className="mt-0.5 h-4 w-4 shrink-0 text-amber-600" />{t('setup.adapters.warning')}</p>}
              {adapters.length > 0 && adapterWarnings.length === adapters.length && <p className="mt-2 text-xs text-muted-foreground">{t('setup.adapters.noneReady')}</p>}
              <div className="mt-7 flex flex-col-reverse gap-2 sm:flex-row sm:justify-between">
                <Button variant="outline" onClick={() => setStep('storage')}><ArrowLeft className="h-4 w-4" />{t('setup.adapters.previous')}</Button>
                <Button onClick={() => setStep('review')} disabled={adaptersQuery.isLoading}>{t('setup.adapters.next')}<ArrowRight className="h-4 w-4" /></Button>
              </div>
            </div>}

            {visibleStep === 'review' && <div>
              <div className="grid gap-3 sm:grid-cols-2">
                <ReviewValue label={t('setup.review.administrator')} value={status.administrator_configured || authDisabled ? authDisabled ? t('setup.review.authDisabled') : t('setup.review.configured') : t('setup.review.checkNeeded')} />
                <ReviewValue label={t('setup.review.storage')} value={storageResult?.status === 'ready' ? t('setup.storage.passed') : storageResult?.status === 'warning' ? t('setup.review.warning') : t('setup.review.notRun')} />
                <ReviewValue label={t('setup.review.adapters')} value={adapters.length ? t('setup.review.adapterCount', { count: adapters.length }) : t('setup.review.noAdapters')} />
                <ReviewValue label={t('setup.review.releaseChannel')} value={status.release_channel ? t(releaseChannelKey(status.release_channel)) : '—'} />
              </div>
              <p className="mt-5 rounded-lg bg-muted/40 p-4 text-sm leading-6 text-muted-foreground">{t('setup.review.automaticWork')}</p>
              {completion.error && <p className="mt-4 rounded-md bg-destructive/10 p-3 text-sm text-destructive" role="alert">{t('setup.error.completeFailed')}</p>}
              <div className="mt-7 flex flex-col-reverse gap-2 sm:flex-row sm:justify-between">
                <Button variant="outline" onClick={() => setStep('adapters')}><ArrowLeft className="h-4 w-4" />{t('setup.review.previous')}</Button>
                <Button onClick={() => completion.mutate()} disabled={completion.isPending || !authenticated}>{completion.isPending ? t('setup.review.completing') : t('setup.review.complete')}<Check className="h-4 w-4" /></Button>
              </div>
            </div>}
          </CardContent>
        </Card>
      </section>
    </div>
  </SetupFrame>
}

function SetupFrame({ children }: { children: React.ReactNode }) {
  return <main className="min-h-screen bg-background px-4 py-5 sm:px-6 sm:py-7"><div className="mx-auto mb-5 flex w-full max-w-5xl items-center justify-between"><AppBrand variant="mobile" /></div>{children}</main>
}

function SetupError({ title, message, onRetry }: { title: string; message: string; onRetry: () => void }) {
  const { t } = useI18n()
  return <div className="rounded-lg border border-destructive/30 bg-destructive/5 p-4"><div className="flex items-start gap-3"><AlertTriangle className="mt-0.5 h-4 w-4 shrink-0 text-destructive" /><div className="min-w-0"><h2 className="text-sm font-semibold">{title}</h2><p className="mt-1 text-sm text-muted-foreground">{message}</p><Button variant="outline" size="sm" className="mt-3" onClick={onRetry}>{t('common.retry')}</Button></div></div></div>
}

function DiagnosticValue({ icon, label, value }: { icon: React.ReactNode; label: string; value: string }) {
  return <div className="flex min-w-0 items-start gap-3 rounded-lg border border-border p-4"><span className="mt-0.5 text-primary">{icon}</span><div className="min-w-0"><p className="text-xs text-muted-foreground">{label}</p><p className="mt-1 truncate text-sm font-medium" title={value}>{value}</p></div></div>
}

function StorageTestResult({ result }: { result: SetupStorageTest }) {
  const { t } = useI18n()
  const tone = result.status === 'ready' ? 'green' : result.status === 'warning' ? 'amber' : 'red'
  const label = result.status === 'ready' ? t('setup.storage.passed') : result.status === 'warning' ? t('setup.review.warning') : t('setup.storage.failed')
  const capacity = result.capacity_known && result.free_bytes !== undefined ? formatBytes(result.free_bytes) : t('setup.storage.capacityUnknown')
  const write = result.write_test === 'passed' ? t('setup.storage.passed') : t('setup.storage.failed')
  const durability = result.durability_test === 'passed' ? t('setup.storage.passed') : t('setup.storage.failed')
  return <div className="mt-4 rounded-lg border border-border bg-muted/20 p-3" role="status">
    <div className="flex flex-wrap items-center justify-between gap-2"><p className="text-sm font-medium">{t('setup.storage.resultTitle')}</p><Badge tone={tone}>{label}</Badge></div>
    <div className="mt-3 grid gap-2 text-xs sm:grid-cols-3">
      <p><span className="text-muted-foreground">{t('setup.storage.available')}</span><br /><span className="mt-0.5 inline-block font-medium">{capacity}</span></p>
      <p><span className="text-muted-foreground">{t('setup.storage.writeTest')}</span><br /><span className="mt-0.5 inline-block font-medium">{write}</span></p>
      <p><span className="text-muted-foreground">{t('setup.storage.durabilityTest')}</span><br /><span className="mt-0.5 inline-block font-medium">{durability}</span></p>
    </div>
    {!result.capacity_known && <p className="mt-2 text-xs leading-5 text-amber-700 dark:text-amber-300">{t('setup.storage.capacityWarning')}</p>}
    {result.diagnostic_code && <p className="mt-2 text-[11px] text-muted-foreground">{t('setup.diagnosticCode')}: <code>{result.diagnostic_code}</code></p>}
  </div>
}

function AdapterReadiness({ adapter }: { adapter: Adapter }) {
  const { t } = useI18n()
  const state = adapter.status.state
  const stateKey = adapterStateKeys[state] ?? 'setup.adapterState.unknown'
  const tone = state === 'ready' || state === 'running' ? 'green' : state === 'disabled' ? 'neutral' : state === 'failed' || state === 'rejected' ? 'red' : 'amber'
  return <div className="flex min-w-0 items-center justify-between gap-3 rounded-lg border border-border px-3 py-2.5"><div className="flex min-w-0 items-center gap-3"><AdapterMark adapter={adapter} size="sm" /><div className="min-w-0"><p className="truncate text-sm font-medium">{adapter.descriptor?.name ?? adapter.status.name ?? adapter.status.id}</p><p className="truncate text-xs text-muted-foreground">{adapter.status.id}</p></div></div><Badge tone={tone}>{t(stateKey)}</Badge></div>
}

function ReviewValue({ label, value }: { label: string; value: string }) {
  return <div className="rounded-lg border border-border p-4"><p className="text-xs text-muted-foreground">{label}</p><p className="mt-1 text-sm font-medium">{value}</p></div>
}
