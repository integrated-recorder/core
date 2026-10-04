import { useEffect, useState, type FormEvent } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useNavigate } from '@tanstack/react-router'
import { AlertTriangle, ArrowLeft, ArrowRight, Check, CheckCircle2, Clipboard, HardDrive, LoaderCircle, LockKeyhole, RefreshCw, ShieldCheck, Server, Wifi } from 'lucide-react'
import { adaptersAPI, authAPI, dashboardAPI, setupAPI } from '@/api'
import { installationQuery, qk, sessionQuery } from '@/api/queries'
import { AppBrand } from '@/components/layout/app-brand'
import { AdapterMark } from '@/components/adapter/adapter-mark'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { errorMessage } from '@/lib/errors'
import { adapterStateLabel } from '@/lib/labels'
import { formatBytes } from '@/lib/utils'
import type { Adapter, InstallationStatus, SetupStorageTest } from '@/types/api'

type SetupStep = 'welcome' | 'administrator' | 'storage' | 'adapters' | 'review' | 'complete'
const stepList: { id: SetupStep; label: string }[] = [
  { id: 'welcome', label: '시작' }, { id: 'administrator', label: '관리자' }, { id: 'storage', label: '저장소' }, { id: 'adapters', label: '어댑터' }, { id: 'review', label: '완료 확인' },
]

function isAuthenticatedSession(session: { auth_enabled: boolean; authenticated: boolean } | undefined, authDisabled: boolean) {
  return authDisabled || session?.authenticated === true
}

function setupPasswordError(password: string, confirmation: string) {
  const bytes = new TextEncoder().encode(password).length
  if (bytes < 12) return '비밀번호는 UTF-8 기준 12바이트 이상이어야 합니다.'
  if (bytes > 72) return '비밀번호는 UTF-8 기준 72바이트 이하여야 합니다.'
  if (password !== confirmation) return '비밀번호 확인이 일치하지 않습니다.'
  return ''
}

function initialStep(status?: InstallationStatus): SetupStep {
  if (status?.state === 'setup_in_progress') return 'storage'
  if (status?.administrator_configured) return 'storage'
  return 'welcome'
}

export function SetupPage() {
  const client = useQueryClient()
  const navigate = useNavigate()
  const installation = useQuery(installationQuery)
  // This request also initializes the existing CSRF mechanism before credential submission.
  const session = useQuery(sessionQuery)
  const status = installation.data
  const authDisabled = status?.auth_disabled ?? false
  const authenticated = isAuthenticatedSession(session.data, authDisabled)
  const [step, setStep] = useState<SetupStep>(() => initialStep(status))
  const [setupCode, setSetupCode] = useState('')
  const [password, setPassword] = useState('')
  const [confirmation, setConfirmation] = useState('')
  const [formError, setFormError] = useState('')
  const [beginError, setBeginError] = useState('')
  const [storageResult, setStorageResult] = useState<SetupStorageTest>()
  const [copied, setCopied] = useState(false)

  const bootstrapping = useMutation({
    mutationFn: () => authAPI.bootstrap(setupCode, password),
    onSuccess: async result => {
      client.setQueryData(qk.session, result)
      setSetupCode('')
      setPassword('')
      setConfirmation('')
      setFormError('')
      try {
        const next = await setupAPI.begin()
        client.setQueryData(qk.installation, next)
        setBeginError('')
        setStep('storage')
      } catch (error) {
        setBeginError(errorMessage(error))
        await client.invalidateQueries({ queryKey: qk.installation })
      }
    },
    onError: () => setFormError('Setup code를 확인하거나 서버 연결 후 다시 시도하세요.'),
  })
  const loggingIn = useMutation({
    mutationFn: () => authAPI.login(password),
    onSuccess: result => { client.setQueryData(qk.session, result); setPassword(''); setFormError('') },
    onError: error => setFormError(errorMessage(error)),
  })
  const begin = useMutation({
    mutationFn: setupAPI.begin,
    onSuccess: result => { client.setQueryData(qk.installation, result); setBeginError(''); setStep('storage') },
    onError: error => setBeginError(errorMessage(error)),
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
  const visibleStep: SetupStep | 'resume' | 'recovery' = status?.state === 'recovery_required'
    ? 'recovery'
    : resumeLogin
      ? 'resume'
      : incompleteButAdminExists && authenticated
        ? 'resume'
        : status?.state === 'setup_in_progress' && authenticated
          ? step === 'welcome' || step === 'administrator' ? 'storage' : step
          : step

  useEffect(() => {
    // A successful completion should show the first-success screen and let the
    // user enter the application explicitly. A previously-ready installation
    // that visits /setup still redirects to the dashboard.
    if (status?.state === 'ready' && step !== 'complete') void navigate({ to: '/' })
  }, [navigate, status?.state, step])

  const submitAdministrator = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault()
    setFormError('')
    if (!setupCode.trim()) return setFormError('Setup code를 입력하세요.')
    const passwordError = setupPasswordError(password, confirmation)
    if (passwordError) return setFormError(passwordError)
    bootstrapping.mutate()
  }
  const submitResumeLogin = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault()
    setFormError('')
    if (!password) return setFormError('관리자 비밀번호를 입력하세요.')
    loggingIn.mutate()
  }
  const copySetupCommand = async () => {
    try {
      await navigator.clipboard.writeText('docker compose exec archiver runtime-host setup-code')
      setCopied(true)
      window.setTimeout(() => setCopied(false), 1800)
    } catch {
      setCopied(false)
    }
  }

  if (installation.isLoading || !status || session.isLoading) return <SetupFrame><div className="flex items-center gap-3 text-sm text-muted-foreground" role="status"><LoaderCircle className="h-4 w-4 animate-spin" />설치 상태를 확인하고 있습니다…</div></SetupFrame>
  if (installation.error || session.error) return <SetupFrame><SetupError title="설치 상태를 확인할 수 없습니다" message="Runtime Host와 인증 API 연결을 확인한 뒤 다시 시도하세요." onRetry={() => { void installation.refetch(); void session.refetch() }} /></SetupFrame>
  if (visibleStep === 'recovery') return <SetupFrame><section className="rounded-xl border border-amber-300/70 bg-card p-6 shadow-sm sm:p-8" role="alert"><div className="grid h-11 w-11 place-items-center rounded-full bg-amber-500/10 text-amber-700 dark:text-amber-300"><AlertTriangle className="h-5 w-5" /></div><h1 className="mt-5 text-xl font-semibold">설치 상태를 확인할 수 없습니다</h1><p className="mt-2 text-sm leading-6 text-muted-foreground">녹화 데이터는 수정하지 않았습니다. 관리자에게 설치 복구를 요청하세요.</p>{status.diagnostic_code && <p className="mt-4 text-xs text-muted-foreground">진단 코드: <code className="rounded bg-muted px-1.5 py-1 font-mono">{status.diagnostic_code}</code></p>}</section></SetupFrame>

  if (visibleStep === 'resume') {
    if (resumeLogin) return <SetupFrame><section className="mx-auto w-full max-w-md rounded-xl border border-border bg-card p-6 shadow-sm sm:p-8"><Badge tone="blue">설정 이어하기</Badge><h1 className="mt-4 text-2xl font-semibold">관리자 로그인</h1><p className="mt-2 text-sm leading-6 text-muted-foreground">관리자 계정은 이미 설정되어 있습니다. 로그인하면 설치의 나머지 단계를 이어갈 수 있습니다.</p><form className="mt-6 space-y-4" onSubmit={submitResumeLogin}><div className="space-y-2"><Label htmlFor="resume-password">관리자 비밀번호</Label><Input id="resume-password" type="password" value={password} onChange={event => setPassword(event.target.value)} autoComplete="current-password" required /></div>{formError && <p className="text-sm text-destructive" role="alert">{formError}</p>}<Button type="submit" className="w-full" disabled={loggingIn.isPending}>{loggingIn.isPending ? '로그인 중…' : '로그인하고 이어하기'}<ArrowRight className="h-4 w-4" /></Button></form></section></SetupFrame>
    return <SetupFrame><section className="mx-auto w-full max-w-md rounded-xl border border-border bg-card p-6 shadow-sm sm:p-8"><Badge tone="amber">설정 이어하기</Badge><h1 className="mt-4 text-2xl font-semibold">설치 준비를 계속합니다</h1><p className="mt-2 text-sm leading-6 text-muted-foreground">관리자 계정이 준비되어 있습니다. 다음 단계를 안전하게 시작합니다.</p>{beginError && <p role="alert" className="mt-4 rounded-md bg-destructive/10 p-3 text-sm text-destructive">{beginError}</p>}<Button className="mt-6 w-full" onClick={() => begin.mutate()} disabled={begin.isPending}>{begin.isPending ? '준비 중…' : '설정 계속하기'}<ArrowRight className="h-4 w-4" /></Button></section></SetupFrame>
  }

  if (visibleStep === 'complete') return <SetupFrame><section className="mx-auto w-full max-w-xl rounded-xl border border-border bg-card p-6 text-center shadow-sm sm:p-9"><div className="mx-auto grid h-14 w-14 place-items-center rounded-full bg-emerald-500/10 text-emerald-600"><CheckCircle2 className="h-7 w-7" /></div><h1 className="mt-5 text-2xl font-semibold">Integrated Recorder가 준비되었습니다</h1><p className="mt-2 text-sm text-muted-foreground">설정이 안전하게 저장되었습니다. 이제 보관 작업을 시작할 수 있습니다.</p><div className="mt-6 grid gap-2 rounded-lg bg-muted/50 p-4 text-left text-sm sm:grid-cols-2"><span className="text-muted-foreground">저장소</span><span>정상</span><span className="text-muted-foreground">관리자 계정</span><span>설정됨</span><span className="text-muted-foreground">Runtime</span><span>정상</span></div><Button className="mt-6 w-full" onClick={() => navigate({ to: '/' })}>Recorder 열기<ArrowRight className="h-4 w-4" /></Button></section></SetupFrame>

  const activeIndex = Math.max(0, stepList.findIndex(item => item.id === visibleStep))
  const adapters = adaptersQuery.data ?? []
  const adapterWarnings = adapters.filter(adapter => !['ready', 'running'].includes(adapter.status.state))

  return <SetupFrame>
    <div className="mx-auto grid w-full max-w-5xl gap-6 lg:grid-cols-[minmax(0,1fr)_280px]">
      <section className="min-w-0">
        <div className="mb-5 flex items-center gap-3"><div className="grid h-10 w-10 place-items-center rounded-lg bg-primary/10 text-primary"><ShieldCheck className="h-5 w-5" /></div><div><p className="text-xs font-semibold uppercase tracking-[.14em] text-primary">첫 설치</p><p className="text-sm text-muted-foreground">몇 단계만 완료하면 Recorder를 사용할 수 있습니다.</p></div></div>
        <Card className="overflow-hidden">
          <CardHeader className="border-b border-border bg-muted/20 px-5 py-5 sm:px-7"><CardTitle className="text-xl">{stepHeading(visibleStep)}</CardTitle><CardDescription>{stepDescription(visibleStep)}</CardDescription></CardHeader>
          <CardContent className="px-5 py-6 sm:px-7 sm:py-7">
            {visibleStep === 'welcome' && <div><div className="rounded-xl bg-[#101c2e] p-5 text-white sm:p-7"><AppBrand variant="login" inverse /><h1 className="mt-8 text-2xl font-semibold sm:text-3xl">처음 몇 가지만 설정하면<br className="hidden sm:block" /> 바로 시작할 수 있습니다.</h1><p className="mt-3 max-w-xl text-sm leading-6 text-white/65">관리자 계정을 만들고 저장소와 어댑터 상태를 확인합니다. 설정이 끝나기 전에는 자동 녹화와 보존 작업이 시작되지 않습니다.</p></div><div className="mt-6 flex flex-wrap gap-3 text-xs text-muted-foreground"><span className="inline-flex items-center gap-1.5"><LockKeyhole className="h-3.5 w-3.5" />안전한 관리자 접근</span><span className="inline-flex items-center gap-1.5"><HardDrive className="h-3.5 w-3.5" />저장소 검사</span><span className="inline-flex items-center gap-1.5"><Wifi className="h-3.5 w-3.5" />어댑터 확인</span></div><div className="mt-7 flex justify-end"><Button onClick={() => status.claim_required && !status.auth_disabled ? setStep('administrator') : begin.mutate()} disabled={begin.isPending}>{begin.isPending ? '준비 중…' : '시작하기'}<ArrowRight className="h-4 w-4" /></Button></div></div>}

            {visibleStep === 'administrator' && <form className="max-w-xl space-y-5" onSubmit={submitAdministrator}><div><p className="text-sm leading-6 text-muted-foreground">Setup code는 서버에서 한 번만 확인할 수 있습니다. 코드 자체는 이 화면의 입력란에만 제출하며 주소에 넣지 않습니다.</p><div className="mt-4 flex flex-col gap-2 rounded-lg border border-border bg-muted/30 p-3 sm:flex-row sm:items-center sm:justify-between"><code className="break-all text-xs">docker compose exec archiver runtime-host setup-code</code><Button type="button" variant="outline" size="sm" className="shrink-0" onClick={() => void copySetupCommand()}><Clipboard className="h-3.5 w-3.5" />{copied ? '복사됨' : '명령 복사'}</Button></div><p className="mt-2 text-xs text-muted-foreground">직접 실행 환경에서는 `runtime-host setup-code` 명령을 실행하세요.</p></div><div className="space-y-2"><Label htmlFor="setup-code">Setup code</Label><Input id="setup-code" type="password" value={setupCode} onChange={event => setSetupCode(event.target.value)} autoComplete="one-time-code" spellCheck={false} required /></div><div className="grid gap-4 sm:grid-cols-2"><div className="space-y-2"><Label htmlFor="setup-password">관리자 비밀번호</Label><Input id="setup-password" type="password" value={password} onChange={event => setPassword(event.target.value)} autoComplete="new-password" minLength={12} maxLength={72} required /></div><div className="space-y-2"><Label htmlFor="setup-password-confirm">비밀번호 확인</Label><Input id="setup-password-confirm" type="password" value={confirmation} onChange={event => setConfirmation(event.target.value)} autoComplete="new-password" maxLength={72} required /></div></div><p className="text-xs text-muted-foreground">비밀번호는 UTF-8 기준 12–72바이트여야 합니다.</p>{formError && <p role="alert" className="rounded-md bg-destructive/10 p-3 text-sm text-destructive">{formError}</p>}<div className="flex flex-col-reverse gap-2 sm:flex-row sm:justify-between"><Button type="button" variant="outline" onClick={() => setStep('welcome')}><ArrowLeft className="h-4 w-4" />이전</Button><Button type="submit" disabled={bootstrapping.isPending || session.isLoading}>{bootstrapping.isPending ? '관리자 계정 설정 중…' : '관리자 계정 만들기'}<ArrowRight className="h-4 w-4" /></Button></div></form>}

            {visibleStep === 'storage' && <div className="space-y-5"><div className="grid gap-3 sm:grid-cols-2"><DiagnosticValue icon={<HardDrive className="h-4 w-4" />} label="기본 저장소" value={storageResult ? storageResult.capacity_known && storageResult.free_bytes !== undefined ? `${formatBytes(storageResult.free_bytes)} 사용 가능` : '용량 알 수 없음' : '용량 확인 전'} /><DiagnosticValue icon={<Server className="h-4 w-4" />} label="Runtime" value={infoQuery.data ? `${infoQuery.data.version} · ${status.release_channel}` : infoQuery.isLoading ? '확인 중…' : status.version} /></div><div className="rounded-lg border border-border p-4"><h3 className="text-sm font-semibold">저장소 쓰기·내구성 검사</h3><p className="mt-1 text-xs leading-5 text-muted-foreground">보관 데이터와 별도의 임시 진단 객체로 쓰기, 읽기, 범위 읽기, 삭제를 확인합니다.</p>{storageResult && <StorageTestResult result={storageResult} />}{storageTest.error && <p role="alert" className="mt-3 text-sm text-destructive">{errorMessage(storageTest.error)}</p>}<div className="mt-4 flex flex-wrap items-center gap-3"><Button variant="outline" onClick={() => storageTest.mutate()} disabled={storageTest.isPending}>{storageTest.isPending ? <LoaderCircle className="h-4 w-4 animate-spin" /> : <RefreshCw className="h-4 w-4" />}{storageTest.isPending ? '저장소 검사 중…' : storageResult ? '다시 검사' : '저장소 검사 실행'}</Button></div></div>{beginError && <p role="alert" className="text-sm text-destructive">{beginError}</p>}<div className="flex flex-col-reverse gap-2 sm:flex-row sm:justify-between"><Button variant="outline" onClick={() => setStep('welcome')}><ArrowLeft className="h-4 w-4" />이전</Button><Button onClick={() => setStep('adapters')} disabled={storageTest.isPending || !storageResult || storageResult.status === 'error'}>계속<ArrowRight className="h-4 w-4" /></Button></div></div>}

            {visibleStep === 'adapters' && <div><p className="text-sm leading-6 text-muted-foreground">설치된 어댑터의 실제 실행 상태를 확인했습니다. 개별 어댑터가 준비되지 않아도 설치를 마친 뒤 설정할 수 있습니다.</p>{adaptersQuery.isLoading && <p className="mt-4 text-sm text-muted-foreground" role="status">어댑터 상태를 확인하고 있습니다…</p>}{adaptersQuery.error && <div className="mt-4"><SetupError title="어댑터 상태를 확인하지 못했습니다" message={errorMessage(adaptersQuery.error)} onRetry={() => void adaptersQuery.refetch()} /></div>}{!adaptersQuery.isLoading && !adaptersQuery.error && <div className="mt-4 space-y-2">{adapters.length ? adapters.map(adapter => <AdapterReadiness key={adapter.status.id} adapter={adapter} />) : <p className="rounded-lg border border-amber-300/60 bg-amber-50/60 p-4 text-sm text-amber-900 dark:border-amber-900 dark:bg-amber-950/20 dark:text-amber-200">사용 가능한 어댑터가 없습니다. 설치를 완료한 뒤 어댑터를 추가하거나 설정할 수 있습니다.</p>}</div>}{adapterWarnings.length > 0 && <p className="mt-4 flex items-start gap-2 rounded-lg bg-amber-500/10 p-3 text-xs leading-5 text-muted-foreground"><AlertTriangle className="mt-0.5 h-4 w-4 shrink-0 text-amber-600" />일부 어댑터에 설정이나 확인이 필요합니다. 설치 완료 후 어댑터 화면에서 계속 관리할 수 있습니다.</p>}{adapters.length > 0 && adapterWarnings.length === adapters.length && <p className="mt-2 text-xs text-muted-foreground">현재 실행 가능한 어댑터가 없습니다. 어댑터를 준비한 뒤 방송 감지를 사용할 수 있습니다.</p>}<div className="mt-7 flex flex-col-reverse gap-2 sm:flex-row sm:justify-between"><Button variant="outline" onClick={() => setStep('storage')}><ArrowLeft className="h-4 w-4" />이전</Button><Button onClick={() => setStep('review')} disabled={adaptersQuery.isLoading}>계속<ArrowRight className="h-4 w-4" /></Button></div></div>}

            {visibleStep === 'review' && <div><div className="grid gap-3 sm:grid-cols-2"><ReviewValue label="관리자 계정" value={status.administrator_configured || authDisabled ? authDisabled ? '인증 비활성화' : '설정됨' : '확인 필요'} /><ReviewValue label="저장소 검사" value={storageResult?.status === 'ready' ? '정상' : storageResult?.status === 'warning' ? '경고 확인됨' : '실행되지 않음'} /><ReviewValue label="어댑터" value={adapters.length ? `${adapters.length}개 확인` : '설치된 어댑터 없음'} /><ReviewValue label="Release channel" value={status.release_channel || '—'} /></div><p className="mt-5 rounded-lg bg-muted/40 p-4 text-sm leading-6 text-muted-foreground">설치를 완료하면 설정에 따라 Watch 감시와 자동 보존 작업이 시작됩니다. 서버를 다시 시작할 필요가 없습니다.</p>{completion.error && <p className="mt-4 rounded-md bg-destructive/10 p-3 text-sm text-destructive" role="alert">설치를 완료하지 못했습니다. {errorMessage(completion.error)}</p>}<div className="mt-7 flex flex-col-reverse gap-2 sm:flex-row sm:justify-between"><Button variant="outline" onClick={() => setStep('adapters')}><ArrowLeft className="h-4 w-4" />이전</Button><Button onClick={() => completion.mutate()} disabled={completion.isPending || !authenticated}>{completion.isPending ? '최종 확인 중…' : '설치 완료'}<Check className="h-4 w-4" /></Button></div></div>}
          </CardContent>
        </Card>
      </section>
      <aside className="space-y-4 lg:pt-[3.4rem]"><Card><CardHeader className="pb-3"><CardTitle>설치 진행 상황</CardTitle><CardDescription>완료 전에는 자동 운영이 시작되지 않습니다.</CardDescription></CardHeader><CardContent><ol aria-label="설치 단계" className="space-y-1">{stepList.map((item, index) => { const current = item.id === visibleStep; const complete = index < activeIndex; return <li key={item.id}><div aria-current={current ? 'step' : undefined} className={`flex items-center gap-3 rounded-md px-2.5 py-2 text-sm ${current ? 'bg-primary/10 font-medium text-primary' : 'text-muted-foreground'}`}><span className={`grid h-6 w-6 shrink-0 place-items-center rounded-full border text-xs ${complete ? 'border-emerald-600 bg-emerald-600 text-white' : current ? 'border-primary' : 'border-border'}`}>{complete ? <Check className="h-3.5 w-3.5" /> : index + 1}</span>{item.label}</div></li> })}</ol></CardContent></Card><p className="px-1 text-xs leading-5 text-muted-foreground">{status.version ? `버전 ${status.version}` : '버전 정보 확인 중'}{status.release_channel ? ` · ${status.release_channel}` : ''}</p></aside>
    </div>
  </SetupFrame>
}

function stepHeading(step: SetupStep | 'resume' | 'recovery') {
  const headings: Record<SetupStep | 'resume' | 'recovery', string> = { welcome: 'Integrated Recorder 시작하기', administrator: '관리자 계정 설정', storage: '저장소 확인', adapters: '어댑터 확인', review: '마지막으로 확인', complete: '설치 완료', resume: '설정 이어하기', recovery: '설치 상태 확인 필요' }
  return headings[step]
}
function stepDescription(step: SetupStep | 'resume' | 'recovery') {
  const descriptions: Record<SetupStep | 'resume' | 'recovery', string> = { welcome: '몇 가지 확인을 마치면 보관 작업을 시작할 수 있습니다.', administrator: '서버를 관리할 관리자 계정을 안전하게 만듭니다.', storage: '보관 데이터를 안전하게 저장할 수 있는지 검사합니다.', adapters: '연결할 플랫폼 어댑터의 준비 상태를 확인합니다.', review: '설치 상태를 확인하고 정상 운영을 시작합니다.', complete: '설치가 완료되었습니다.', resume: '이전에 시작한 설치를 이어갑니다.', recovery: '설치 상태에 문제가 있습니다.' }
  return descriptions[step]
}

function SetupFrame({ children }: { children: React.ReactNode }) {
  return <main className="min-h-screen bg-background px-4 py-6 sm:px-6 sm:py-9"><div className="mx-auto mb-7 flex w-full max-w-5xl items-center justify-between"><AppBrand variant="mobile" /><span className="hidden text-xs text-muted-foreground sm:block">안전한 최초 설치</span></div>{children}</main>
}

function SetupError({ title, message, onRetry }: { title: string; message: string; onRetry: () => void }) {
  return <div className="rounded-lg border border-destructive/30 bg-destructive/5 p-4"><div className="flex items-start gap-3"><AlertTriangle className="mt-0.5 h-4 w-4 shrink-0 text-destructive" /><div className="min-w-0"><h2 className="text-sm font-semibold">{title}</h2><p className="mt-1 text-sm text-muted-foreground">{message}</p><Button variant="outline" size="sm" className="mt-3" onClick={onRetry}>다시 시도</Button></div></div></div>
}

function DiagnosticValue({ icon, label, value }: { icon: React.ReactNode; label: string; value: string }) {
  return <div className="flex min-w-0 items-start gap-3 rounded-lg border border-border p-4"><span className="mt-0.5 text-primary">{icon}</span><div className="min-w-0"><p className="text-xs text-muted-foreground">{label}</p><p className="mt-1 truncate text-sm font-medium" title={value}>{value}</p></div></div>
}

function StorageTestResult({ result }: { result: SetupStorageTest }) {
  const tone = result.status === 'ready' ? 'green' : result.status === 'warning' ? 'amber' : 'red'
  const label = result.status === 'ready' ? '정상' : result.status === 'warning' ? '경고' : '오류'
  return <div className="mt-4 rounded-lg border border-border bg-muted/20 p-3" role="status"><div className="flex flex-wrap items-center justify-between gap-2"><p className="text-sm font-medium">기본 저장소 진단</p><Badge tone={tone}>{label}</Badge></div><div className="mt-3 grid gap-2 text-xs sm:grid-cols-3"><p><span className="text-muted-foreground">사용 가능</span><br /><span className="mt-0.5 inline-block font-medium">{result.capacity_known && result.free_bytes !== undefined ? formatBytes(result.free_bytes) : '알 수 없음'}</span></p><p><span className="text-muted-foreground">쓰기 테스트</span><br /><span className="mt-0.5 inline-block font-medium">{result.write_test === 'passed' ? '정상' : '실패'}</span></p><p><span className="text-muted-foreground">내구성 테스트</span><br /><span className="mt-0.5 inline-block font-medium">{result.durability_test === 'passed' ? '정상' : '실패'}</span></p></div>{!result.capacity_known && <p className="mt-2 text-xs leading-5 text-amber-700 dark:text-amber-300">저장소가 사용 가능 용량을 제공하지 않아 용량은 알 수 없습니다. 쓰기와 내구성 검사는 정상입니다.</p>}{result.diagnostic_code && <p className="mt-2 text-[11px] text-muted-foreground">진단 코드: <code>{result.diagnostic_code}</code></p>}</div>
}

function AdapterReadiness({ adapter }: { adapter: Adapter }) {
  const state = adapter.status.state
  const stateLabel = state === 'running' ? '준비됨' : state === 'configuration_required' ? '설정 필요' : ['ready', 'disabled', 'unavailable', 'failed', 'rejected', 'restarting', 'starting', 'stopped', 'unknown'].includes(state) ? adapterStateLabel(state) : '확인 필요'
  const tone = state === 'ready' || state === 'running' ? 'green' : state === 'disabled' ? 'neutral' : state === 'failed' || state === 'rejected' ? 'red' : 'amber'
  return <div className="flex min-w-0 items-center justify-between gap-3 rounded-lg border border-border px-3 py-2.5"><div className="flex min-w-0 items-center gap-3"><AdapterMark adapter={adapter} size="sm" /><div className="min-w-0"><p className="truncate text-sm font-medium">{adapter.descriptor?.name ?? adapter.status.name ?? adapter.status.id}</p><p className="truncate text-xs text-muted-foreground">{adapter.status.id}</p></div></div><Badge tone={tone}>{stateLabel}</Badge></div>
}

function ReviewValue({ label, value }: { label: string; value: string }) {
  return <div className="rounded-lg border border-border p-4"><p className="text-xs text-muted-foreground">{label}</p><p className="mt-1 text-sm font-medium">{value}</p></div>
}
