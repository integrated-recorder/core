import { useState, type FormEvent } from 'react'
import { Link } from '@tanstack/react-router'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { KeyRound, RefreshCw, UserPlus } from 'lucide-react'
import { accountAPI } from '@/api'
import { APIError, invalidateCSRFToken } from '@/api/client'
import { accountSessionsQuery, qk, usersQuery } from '@/api/queries'
import { useI18n } from '@/i18n/provider'
import type { AccountUser } from '@/types/api'
import { formatDate } from '@/lib/utils'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Confirm } from '@/components/ui/confirm'
import { Input } from '@/components/ui/input'
import { Badge } from '@/components/ui/badge'
import { EmptyState, ErrorState, LoadingState } from '@/components/query-state'
import { useToast } from '@/components/ui/use-toast'
import { errorMessage } from '@/lib/errors'

export function AccountSecurityPanel({ account, section = 'security' }: { account: AccountUser; section?: 'security' | 'sessions' }) {
  const { t } = useI18n()
  if (section === 'sessions') return <div className="page-enter"><SessionsCard /></div>
  return <div className="space-y-4">
    <Card>
      <CardContent className="pt-4"><dl className="grid gap-4 sm:grid-cols-3"><ProfileValue label={t('account.login')} value={account.login} /><ProfileValue label={t('account.role')} value={account.role === 'owner' ? t('account.role.owner') : t('account.role.member')} /><ProfileValue label={t('account.status')} value={account.status === 'active' ? t('account.status.active') : t('account.status.disabled')} /></dl></CardContent>
    </Card>
    <PasswordCard />
    <Link to="/account/sessions" className="block rounded-lg border border-border px-4 py-3 text-sm font-medium hover:bg-muted/50">{t('account.sessions')} →</Link>
    <Card><CardHeader><CardTitle>{t('account.localRecovery')}</CardTitle></CardHeader><CardContent className="space-y-2 text-sm text-muted-foreground"><p>{t('account.localRecoveryHelp')}</p><code className="block rounded bg-muted px-3 py-2 text-xs">docker compose exec -it archiver runtime-host reset-password &lt;login&gt;</code></CardContent></Card>
  </div>
}

function ProfileValue({ label, value }: { label: string; value: string }) {
  return <div><dt className="text-xs text-muted-foreground">{label}</dt><dd className="mt-1 text-sm font-medium">{value}</dd></div>
}

function PasswordCard() {
  const { t } = useI18n()
  const { toast } = useToast()
  const [formError, setFormError] = useState('')
  const [isChanging, setIsChanging] = useState(false)
  const submit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault()
    setFormError('')
    const form = event.currentTarget
    const data = new FormData(form)
    const currentPassword = String(data.get('current_password') ?? '')
    const newPassword = String(data.get('new_password') ?? '')
    const confirmation = String(data.get('password_confirmation') ?? '')
    if (newPassword !== confirmation) { setFormError(t('account.passwordsMismatch')); return }
    if (isChanging) return
    setIsChanging(true)
    try {
      await accountAPI.changePassword({ current_password: currentPassword, new_password: newPassword })
      form.reset()
      invalidateCSRFToken()
      toast(t('account.passwordChanged'))
      window.dispatchEvent(new CustomEvent('ir:unauthorized'))
    } catch (error) {
      setFormError(error instanceof APIError && error.errorCode === 'credentials_invalid' ? t('account.credentialsRejected') : errorMessage(error))
    } finally {
      setIsChanging(false)
    }
  }
  return <Card>
    <CardHeader><CardTitle className="flex items-center gap-2"><KeyRound className="h-4 w-4 text-primary" />{t('account.changePassword')}</CardTitle><p className="text-xs text-muted-foreground">{t('account.passwordPolicy')}</p></CardHeader>
    <CardContent><form className="max-w-xl space-y-3" onSubmit={submit}>
      <label className="block space-y-1.5 text-sm">{t('account.currentPassword')}<Input type="password" name="current_password" autoComplete="current-password" minLength={12} maxLength={72} required /></label>
      <label className="block space-y-1.5 text-sm">{t('account.newPassword')}<Input type="password" name="new_password" autoComplete="new-password" minLength={12} maxLength={72} required /></label>
      <label className="block space-y-1.5 text-sm">{t('account.confirmPassword')}<Input type="password" name="password_confirmation" autoComplete="new-password" minLength={12} maxLength={72} required /></label>
      {formError && <p role="alert" className="text-sm text-destructive">{formError}</p>}
      <Button type="submit" disabled={isChanging}>{t('account.changePassword')}</Button>
    </form></CardContent>
  </Card>
}

function SessionsCard() {
  const client = useQueryClient()
  const { t } = useI18n()
  const { toast } = useToast()
  const sessions = useQuery(accountSessionsQuery)
  const refresh = () => client.invalidateQueries({ queryKey: qk.accountSessions })
  const revoke = useMutation({ mutationFn: accountAPI.revokeSession, onSuccess: async (_result, sessionID) => {
    const selected = sessions.data?.items.find(item => item.id === sessionID)
    if (selected?.current) {
      invalidateCSRFToken()
      window.dispatchEvent(new CustomEvent('ir:unauthorized'))
    } else {
      toast(t('account.sessionRevoked'))
      await refresh()
    }
  }, onError: error => toast(t('account.sessionActionFailed'), errorMessage(error), 'error') })
  const revokeOthers = useMutation({ mutationFn: accountAPI.revokeOthers, onSuccess: async result => { toast(t('account.sessionsRevoked', { count: result.revoked_count })); await refresh() }, onError: error => toast(t('account.sessionActionFailed'), errorMessage(error), 'error') })
  const currentCount = sessions.data?.items.filter(item => !item.current).length ?? 0
  return <Card>
    <CardHeader className="flex flex-row items-start justify-between gap-3"><div><p className="text-xs text-muted-foreground">{t('account.sessionsHelp')}</p></div><div className="flex gap-2"><Button variant="outline" size="sm" onClick={() => void refresh()} aria-label={t('account.refreshSessions')}><RefreshCw className="h-4 w-4" /></Button><Button variant="outline" size="sm" disabled={!currentCount || revokeOthers.isPending} onClick={() => revokeOthers.mutate()}>{t('account.revokeOthers')}</Button></div></CardHeader>
    <CardContent>{sessions.isLoading ? <LoadingState /> : sessions.error ? <ErrorState message={errorMessage(sessions.error)} retry={() => void sessions.refetch()} /> : sessions.data?.items.length ? <ul className="divide-y divide-border">{sessions.data.items.map(session => <li key={session.id} className="flex flex-wrap items-center justify-between gap-3 py-3"><div className="flex items-center gap-2"><span className="text-sm">{t('account.sessionExpires', { date: formatDate(session.expires_at) })}</span>{session.current && <Badge tone="green">{t('account.currentSession')}</Badge>}</div><Button size="sm" variant="outline" disabled={revoke.isPending} onClick={() => revoke.mutate(session.id)}>{t('account.revoke')}</Button></li>)}</ul> : <EmptyState title={t('account.noSessions')} />}</CardContent>
  </Card>
}

export function UsersManagementPanel({ currentUser }: { currentUser: AccountUser }) {
  const client = useQueryClient()
  const { t } = useI18n()
  const { toast } = useToast()
  const users = useQuery(usersQuery)
  const [login, setLogin] = useState('')
  const [password, setPassword] = useState('')
  const [confirmation, setConfirmation] = useState('')
  const [formError, setFormError] = useState('')
  const invalidate = () => client.invalidateQueries({ queryKey: qk.users })
  const create = useMutation({ mutationFn: accountAPI.createUser, onSuccess: async () => { setLogin(''); setPassword(''); setConfirmation(''); toast(t('account.userCreated')); await invalidate() }, onError: error => setFormError(errorMessage(error)) })
  const toggle = useMutation({ mutationFn: ({ id, enabled }: { id: string; enabled: boolean }) => accountAPI.setUserEnabled(id, enabled), onSuccess: async user => { toast(user.status === 'active' ? t('account.userEnabled') : t('account.userDisabled')); await invalidate() }, onError: error => toast(t('account.userActionFailed'), errorMessage(error), 'error') })
  const submit = (event: React.FormEvent) => {
    event.preventDefault(); setFormError('')
    if (password !== confirmation) { setFormError(t('account.passwordsMismatch')); return }
    create.mutate({ login: login.trim(), password })
  }
  return <Card>
    <CardHeader><p className="text-xs text-muted-foreground">{t('account.usersHelp')}</p></CardHeader>
    <CardContent className="space-y-5">
      <form className="grid gap-3 rounded-lg border border-border p-4 sm:grid-cols-2" onSubmit={submit}>
        <label className="space-y-1.5 text-sm">{t('account.login')}<Input autoComplete="off" minLength={1} maxLength={64} required value={login} onChange={event => setLogin(event.target.value)} /></label>
        <label className="space-y-1.5 text-sm">{t('account.initialPassword')}<Input type="password" autoComplete="new-password" minLength={12} maxLength={72} required value={password} onChange={event => setPassword(event.target.value)} /></label>
        <label className="space-y-1.5 text-sm">{t('account.confirmPassword')}<Input type="password" autoComplete="new-password" minLength={12} maxLength={72} required value={confirmation} onChange={event => setConfirmation(event.target.value)} /></label>
        <div className="flex items-end"><Button type="submit" disabled={create.isPending}><UserPlus className="h-4 w-4" />{t('account.createUser')}</Button></div>
        <p className="text-xs text-muted-foreground sm:col-span-2">{t('account.passwordPolicy')}</p>
        {formError && <p role="alert" className="text-sm text-destructive sm:col-span-2">{formError}</p>}
      </form>
      {users.isLoading ? <LoadingState /> : users.error ? <ErrorState message={errorMessage(users.error)} retry={() => void users.refetch()} /> : <ul className="divide-y divide-border">{users.data?.items.map(user => <li key={user.user_id} className="flex flex-wrap items-center justify-between gap-3 py-3"><div><p className="text-sm font-medium">{user.login} <span className="text-xs text-muted-foreground">· {user.role === 'owner' ? t('account.role.owner') : t('account.role.member')}</span></p><p className="mt-1 text-xs text-muted-foreground">{user.status === 'active' ? t('account.status.active') : t('account.status.disabled')}</p></div>{user.user_id !== currentUser.user_id && (user.status === 'active' ? <Confirm trigger={<Button size="sm" variant="destructive">{t('account.disable')}</Button>} title={t('account.disableConfirmTitle')} description={t('account.disableConfirmBody', { login: user.login })} confirmLabel={t('account.disable')} destructive onConfirm={() => toggle.mutate({ id: user.user_id, enabled: false })} disabled={toggle.isPending} /> : <Button size="sm" variant="outline" disabled={toggle.isPending} onClick={() => toggle.mutate({ id: user.user_id, enabled: true })}>{t('account.enable')}</Button>)}</li>)}</ul>}
    </CardContent>
  </Card>
}
