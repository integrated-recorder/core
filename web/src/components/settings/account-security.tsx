import { useEffect, useMemo, useState, type FormEvent } from 'react'
import { Link } from '@tanstack/react-router'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { KeyRound, RefreshCw, UserPlus } from 'lucide-react'
import { accountAPI } from '@/api'
import { APIError, invalidateCSRFToken } from '@/api/client'
import { accountAuthorizationQuery, accountSessionsQuery, qk, rolesQuery, usersQuery } from '@/api/queries'
import { useI18n } from '@/i18n/provider'
import type { AccountRole, AccountUser, PermissionID } from '@/types/api'
import { passwordLengthStatus } from '@/lib/credentials'
import { formatDate } from '@/lib/utils'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Confirm } from '@/components/ui/confirm'
import { Input } from '@/components/ui/input'
import { Badge } from '@/components/ui/badge'
import { EmptyState, ErrorState, LoadingState } from '@/components/query-state'
import { useToast } from '@/components/ui/use-toast'
import { errorMessage } from '@/lib/errors'
import type { TranslationKey } from '@/i18n/catalog'

export function AccountSecurityPanel({ account, section = 'security' }: { account: AccountUser; section?: 'security' | 'sessions' }) {
  const { t } = useI18n()
  if (section === 'sessions') return <div className="page-enter"><SessionsCard /></div>
  return <div className="space-y-4">
    <Card><CardContent className="pt-4"><dl className="grid gap-4 sm:grid-cols-3"><ProfileValue label={t('account.login')} value={account.login} /><ProfileValue label={t('account.role')} value={(account.roles ?? []).map(role => displayRoleName(role, t)).join(', ')} /><ProfileValue label={t('account.status')} value={account.status === 'active' ? t('account.status.active') : t('account.status.disabled')} /></dl></CardContent></Card>
    <RenameLoginCard account={account} />
    <PasswordCard account={account} />
    <Link to="/account/sessions" className="block rounded-lg border border-border px-4 py-3 text-sm font-medium hover:bg-muted/50">{t('account.sessions')} →</Link>
    <Card><CardHeader><CardTitle>{t('account.localRecovery')}</CardTitle></CardHeader><CardContent className="space-y-2 text-sm text-muted-foreground"><p>{t('account.localRecoveryHelp')}</p><code className="block rounded bg-muted px-3 py-2 text-xs">runtime-host reset-password &lt;login&gt;</code></CardContent></Card>
  </div>
}

function ProfileValue({ label, value }: { label: string; value: string }) {
  return <div><dt className="text-xs text-muted-foreground">{label}</dt><dd className="mt-1 text-sm font-medium">{value}</dd></div>
}

function RenameLoginCard({ account }: { account: AccountUser }) {
  const { t } = useI18n()
  const { toast } = useToast()
  const [formError, setFormError] = useState('')
  const [pending, setPending] = useState(false)
  const submit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault()
    setFormError('')
    const data = new FormData(event.currentTarget)
    const login = String(data.get('new_username') ?? '').trim()
    const currentPassword = String(data.get('current_password') ?? '')
    if (!login) { setFormError(t('login.usernameRequired')); return }
    if (pending) return
    setPending(true)
    try {
      await accountAPI.renameLogin({ login, current_password: currentPassword })
      toast(t('account.loginChanged'))
      invalidateCSRFToken()
      window.dispatchEvent(new CustomEvent('ir:unauthorized'))
    } catch (error) {
      setFormError(error instanceof APIError && error.errorCode === 'current_credentials_invalid' ? t('account.credentialsRejected') : errorMessage(error))
    } finally { setPending(false) }
  }
  return <Card><CardHeader><CardTitle>{t('account.renameLogin')}</CardTitle><p className="text-xs text-muted-foreground">{t('account.renameLoginHelp')}</p></CardHeader><CardContent><form className="max-w-xl space-y-3" onSubmit={submit}>
    <label className="block space-y-1.5 text-sm">{t('account.login')}<Input type="text" name="username" value={account.login} autoComplete="username" readOnly /></label>
    <label className="block space-y-1.5 text-sm">{t('account.newLogin')}<Input type="text" name="new_username" autoComplete="username" autoCapitalize="none" autoCorrect="off" spellCheck={false} required /></label>
    <label className="block space-y-1.5 text-sm">{t('account.currentPassword')}<Input type="password" name="current_password" autoComplete="current-password" required /></label>
    {formError && <p role="alert" className="text-sm text-destructive">{formError}</p>}
    <Button type="submit" disabled={pending}>{pending ? t('account.savingLogin') : t('account.renameLogin')}</Button>
  </form></CardContent></Card>
}

function PasswordCard({ account }: { account: AccountUser }) {
  const { t } = useI18n()
  const { toast } = useToast()
  const [formError, setFormError] = useState('')
  const [isChanging, setIsChanging] = useState(false)
  const submit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault()
    setFormError('')
    const data = new FormData(event.currentTarget)
    const currentPassword = String(data.get('current_password') ?? '')
    const newPassword = String(data.get('new_password') ?? '')
    const confirmation = String(data.get('password_confirmation') ?? '')
    if (passwordLengthStatus(newPassword) !== 'valid') { setFormError(t(passwordLengthStatus(newPassword) === 'too_short' ? 'setup.error.passwordTooShort' : 'setup.error.passwordTooLong')); return }
    if (newPassword !== confirmation) { setFormError(t('account.passwordsMismatch')); return }
    if (isChanging) return
    setIsChanging(true)
    try {
      await accountAPI.changePassword({ current_password: currentPassword, new_password: newPassword })
      invalidateCSRFToken()
      toast(t('account.passwordChanged'))
      window.dispatchEvent(new CustomEvent('ir:unauthorized'))
    } catch (error) {
      setFormError(error instanceof APIError && error.errorCode === 'credentials_invalid' ? t('account.credentialsRejected') : errorMessage(error))
    } finally { setIsChanging(false) }
  }
  return <Card>
    <CardHeader><CardTitle className="flex items-center gap-2"><KeyRound className="h-4 w-4 text-primary" />{t('account.changePassword')}</CardTitle><p className="text-xs text-muted-foreground">{t('account.passwordPolicy')}</p></CardHeader>
    <CardContent><form className="max-w-xl space-y-3" onSubmit={submit}>
      <label className="block space-y-1.5 text-sm">{t('account.login')}<Input type="text" name="username" value={account.login} autoComplete="username" readOnly /></label>
      <label className="block space-y-1.5 text-sm">{t('account.currentPassword')}<Input type="password" name="current_password" autoComplete="current-password" required /></label>
      <label className="block space-y-1.5 text-sm">{t('account.newPassword')}<Input type="password" name="new_password" autoComplete="new-password" required /></label>
      <label className="block space-y-1.5 text-sm">{t('account.confirmPassword')}<Input type="password" name="password_confirmation" autoComplete="new-password" required /></label>
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
    if (selected?.current) { invalidateCSRFToken(); window.dispatchEvent(new CustomEvent('ir:unauthorized')) }
    else { toast(t('account.sessionRevoked')); await refresh() }
  }, onError: error => toast(t('account.sessionActionFailed'), errorMessage(error), 'error') })
  const revokeOthers = useMutation({ mutationFn: accountAPI.revokeOthers, onSuccess: async result => { toast(t('account.sessionsRevoked', { count: result.revoked_count })); await refresh() }, onError: error => toast(t('account.sessionActionFailed'), errorMessage(error), 'error') })
  const currentCount = sessions.data?.items.filter(item => !item.current).length ?? 0
  return <Card>
    <CardHeader className="flex flex-row items-start justify-between gap-3"><div><p className="text-xs text-muted-foreground">{t('account.sessionsHelp')}</p></div><div className="flex gap-2"><Button variant="outline" size="sm" onClick={() => void refresh()} aria-label={t('account.refreshSessions')}><RefreshCw className="h-4 w-4" /></Button><Button variant="outline" size="sm" disabled={!currentCount || revokeOthers.isPending} onClick={() => revokeOthers.mutate()}>{t('account.revokeOthers')}</Button></div></CardHeader>
    <CardContent>{sessions.isLoading ? <LoadingState /> : sessions.error ? <ErrorState message={errorMessage(sessions.error)} retry={() => void sessions.refetch()} /> : sessions.data?.items.length ? <ul className="divide-y divide-border">{sessions.data.items.map(session => <li key={session.id} className="flex flex-wrap items-center justify-between gap-3 py-3"><div className="flex items-center gap-2"><span className="text-sm">{t('account.sessionExpires', { date: formatDate(session.expires_at) })}</span>{session.current && <Badge tone="green">{t('account.currentSession')}</Badge>}</div><Button size="sm" variant="outline" disabled={revoke.isPending} onClick={() => revoke.mutate(session.id)}>{t('account.revoke')}</Button></li>)}</ul> : <EmptyState title={t('account.noSessions')} />}</CardContent>
  </Card>
}

const permissionLabels: Record<string, TranslationKey> = {
  'recording.read': 'rbac.permission.recording.read', 'recording.create': 'rbac.permission.recording.create', 'recording.stop': 'rbac.permission.recording.stop', 'recording.delete': 'rbac.permission.recording.delete', 'recording.export': 'rbac.permission.recording.export', 'recording.integrity.run': 'rbac.permission.recording.integrity', 'recording.annotate': 'rbac.permission.recording.annotate', 'recording.preview.manage': 'rbac.permission.recording.preview', 'recording.workflow.manage': 'rbac.permission.recording.workflow',
  'watch.read': 'rbac.permission.watch.read', 'watch.manage': 'rbac.permission.watch.manage', 'watch.check': 'rbac.permission.watch.check', 'plugin.read': 'rbac.permission.plugin.read', 'plugin.manage': 'rbac.permission.plugin.manage', 'plugin.configure': 'rbac.permission.plugin.configure', 'storage.read': 'rbac.permission.storage.read', 'storage.manage': 'rbac.permission.storage.manage', 'system.read': 'rbac.permission.system.read', 'system.settings.manage': 'rbac.permission.system.settings', 'system.update.manage': 'rbac.permission.system.update', 'system.diagnostics.read': 'rbac.permission.system.diagnostics', 'user.read': 'rbac.permission.user.read', 'user.create': 'rbac.permission.user.create', 'user.manage': 'rbac.permission.user.manage', 'role.read': 'rbac.permission.role.read', 'role.manage': 'rbac.permission.role.manage', 'audit.read': 'rbac.permission.audit.read',
}

function displayRoleName(id: string, t: (key: TranslationKey) => string): string {
  const keys: Record<string, TranslationKey> = { owner: 'rbac.role.owner', administrator: 'rbac.role.administrator', operator: 'rbac.role.operator', viewer: 'rbac.role.viewer' }
  return keys[id] ? t(keys[id]) : id
}

export function UsersManagementPanel({ currentUser }: { currentUser: AccountUser }) {
  const client = useQueryClient()
  const { t } = useI18n()
  const { toast } = useToast()
  const authorization = useQuery(accountAuthorizationQuery)
  const can = (permission: string) => authorization.data?.permissions?.includes(permission) ?? false
  const users = useQuery({ ...usersQuery, enabled: can('user.read') || can('user.create') || can('user.manage') })
  const roles = useQuery({ ...rolesQuery, enabled: can('role.read') || can('role.manage') || can('user.create') || can('user.manage') })
  const [formError, setFormError] = useState('')
  const [createRoles, setCreateRoles] = useState<string[]>(['operator'])
  const [roleName, setRoleName] = useState('')
  const [rolePermissions, setRolePermissions] = useState<PermissionID[]>([])
  const [editingRole, setEditingRole] = useState<AccountRole>()
  const actorIsOwner = authorization.data?.roles.includes('owner') ?? false
  const invalidate = async () => { await Promise.all([client.invalidateQueries({ queryKey: qk.users }), client.invalidateQueries({ queryKey: qk.roles }), client.invalidateQueries({ queryKey: qk.accountAuthorization })]) }
  const create = useMutation({ mutationFn: accountAPI.createUser, onSuccess: async () => { toast(t('account.userCreated')); await invalidate() }, onError: error => setFormError(errorMessage(error)) })
  const toggle = useMutation({ mutationFn: ({ id, enabled }: { id: string; enabled: boolean }) => accountAPI.setUserEnabled(id, enabled), onSuccess: async user => { toast(user.status === 'active' ? t('account.userEnabled') : t('account.userDisabled')); await invalidate() }, onError: error => toast(t('account.userActionFailed'), errorMessage(error), 'error') })
  const setRoles = useMutation({ mutationFn: ({ id, assigned }: { id: string; assigned: string[] }) => accountAPI.setUserRoles(id, assigned), onSuccess: async () => { toast(t('account.rolesSaved')); await invalidate() }, onError: error => toast(t('account.userActionFailed'), errorMessage(error), 'error') })
  const createRole = useMutation({ mutationFn: accountAPI.createRole, onSuccess: async role => { setRoleName(''); setRolePermissions([]); setEditingRole(undefined); toast(t('rbac.roleCreated')); await invalidate(); setEditingRole(role) }, onError: error => toast(t('rbac.roleSaveFailed'), errorMessage(error), 'error') })
  const updateRole = useMutation({ mutationFn: ({ id, name, permissions }: { id: string; name: string; permissions: PermissionID[] }) => accountAPI.updateRole(id, { name, permissions }), onSuccess: async role => { toast(t('rbac.roleSaved')); await invalidate(); setEditingRole(role) }, onError: error => toast(t('rbac.roleSaveFailed'), errorMessage(error), 'error') })
  const deleteRole = useMutation({ mutationFn: accountAPI.deleteRole, onSuccess: async () => { setEditingRole(undefined); toast(t('rbac.roleDeleted')); await invalidate() }, onError: error => toast(t('rbac.roleSaveFailed'), errorMessage(error), 'error') })
  const submitUser = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault()
    setFormError('')
    const data = new FormData(event.currentTarget)
    const login = String(data.get('username') ?? '').trim()
    const password = String(data.get('password') ?? '')
    const confirmation = String(data.get('password_confirmation') ?? '')
    if (!login) { setFormError(t('login.usernameRequired')); return }
    const length = passwordLengthStatus(password)
    if (length !== 'valid') { setFormError(t(length === 'too_short' ? 'setup.error.passwordTooShort' : 'setup.error.passwordTooLong')); return }
    if (password !== confirmation) { setFormError(t('account.passwordsMismatch')); return }
    create.mutate({ login, password, roles: createRoles.length ? createRoles : ['operator'] })
  }
  const submitRole = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault()
    if (editingRole) updateRole.mutate({ id: editingRole.id, name: roleName, permissions: rolePermissions })
    else createRole.mutate({ name: roleName, permissions: rolePermissions })
  }
  useEffect(() => {
    if (editingRole) { setRoleName(editingRole.name); setRolePermissions(editingRole.permissions) }
    else { setRoleName(''); setRolePermissions([]) }
  }, [editingRole])
  const groupedPermissions = useMemo(() => {
    const groups = new Map<string, PermissionID[]>()
    for (const permission of roles.data?.permissions ?? []) {
      if (permission.startsWith('account.') || permission === 'user.preferences.self') continue
      const group = permission.split('.')[0]
      groups.set(group, [...(groups.get(group) ?? []), permission])
    }
    return [...groups.entries()]
  }, [roles.data?.permissions])
  return <div className="space-y-4">
    {(can('user.create') || can('user.manage')) && <Card><CardHeader><p className="text-xs text-muted-foreground">{t('account.usersHelp')}</p></CardHeader><CardContent className="space-y-5">
      {can('user.create') && <form className="grid gap-3 rounded-lg border border-border p-4 sm:grid-cols-2" onSubmit={submitUser}>
        <label className="space-y-1.5 text-sm">{t('account.login')}<Input type="text" name="username" autoComplete="username" autoCapitalize="none" autoCorrect="off" spellCheck={false} required /></label>
        <label className="space-y-1.5 text-sm">{t('account.initialPassword')}<Input type="password" name="password" autoComplete="new-password" required /></label>
        <label className="space-y-1.5 text-sm">{t('account.confirmPassword')}<Input type="password" name="password_confirmation" autoComplete="new-password" required /></label>
        {roles.data && <fieldset className="space-y-1"><legend className="text-sm">{t('rbac.assignedRoles')}</legend>{roles.data.roles.filter(role => actorIsOwner || role.id !== 'owner').map(role => <label key={role.id} className="flex items-center gap-2 text-sm"><input type="checkbox" name="roles" checked={createRoles.includes(role.id)} onChange={event => setCreateRoles(current => event.target.checked ? [...new Set([...current, role.id])] : current.filter(id => id !== role.id))} />{displayRoleName(role.id, t)}{!role.built_in && <span className="text-muted-foreground">{role.name}</span>}</label>)}</fieldset>}
        <div className="flex items-end"><Button type="submit" disabled={create.isPending}><UserPlus className="h-4 w-4" />{t('account.createUser')}</Button></div>
        <p className="text-xs text-muted-foreground sm:col-span-2">{t('account.passwordPolicy')}</p>
        {formError && <p role="alert" className="text-sm text-destructive sm:col-span-2">{formError}</p>}
      </form>}
      {can('user.read') || can('user.manage') ? users.isLoading ? <LoadingState /> : users.error ? <ErrorState message={errorMessage(users.error)} retry={() => void users.refetch()} /> : <ul className="divide-y divide-border">{users.data?.items.map(user => <li key={user.user_id} className="space-y-3 py-3"><div className="flex flex-wrap items-center justify-between gap-3"><div><p className="text-sm font-medium">{user.login} {user.user_id === currentUser.user_id && <span className="text-xs text-muted-foreground">· {t('account.self')}</span>}</p><div className="mt-1 flex flex-wrap gap-1">{(user.roles ?? []).map(role => <Badge key={role} tone={role === 'owner' ? 'amber' : 'neutral'}>{displayRoleName(role, t)}</Badge>)}</div><p className="mt-1 text-xs text-muted-foreground">{user.status === 'active' ? t('account.status.active') : t('account.status.disabled')}</p></div>{can('user.manage') && user.user_id !== currentUser.user_id && (user.status === 'active' ? <Confirm trigger={<Button size="sm" variant="destructive">{t('account.disable')}</Button>} title={t('account.disableConfirmTitle')} description={t('account.disableConfirmBody', { login: user.login })} confirmLabel={t('account.disable')} destructive onConfirm={() => toggle.mutate({ id: user.user_id, enabled: false })} disabled={toggle.isPending} /> : <Button size="sm" variant="outline" disabled={toggle.isPending} onClick={() => toggle.mutate({ id: user.user_id, enabled: true })}>{t('account.enable')}</Button>)}</div>
        {can('user.manage') && roles.data && <details><summary className="cursor-pointer text-xs text-muted-foreground">{t('rbac.editUserRoles')}</summary><fieldset className="mt-2 flex flex-wrap gap-x-4 gap-y-2">{roles.data.roles.filter(role => actorIsOwner || role.id !== 'owner').map(role => <label key={role.id} className="flex items-center gap-2 text-sm"><input type="checkbox" name={`roles-${user.user_id}`} checked={user.roles.includes(role.id)} onChange={event => { const assigned = event.target.checked ? [...new Set([...user.roles, role.id])] : user.roles.filter(id => id !== role.id); setRoles.mutate({ id: user.user_id, assigned }) }} />{displayRoleName(role.id, t)}</label>)}</fieldset></details>}
      </li>)}</ul> : null}
    </CardContent></Card>}
    {can('role.read') && <Card><CardHeader><CardTitle>{t('rbac.roles')}</CardTitle><p className="text-xs text-muted-foreground">{t('rbac.rolesHelp')}</p></CardHeader><CardContent className="space-y-4">
      {roles.isLoading ? <LoadingState /> : roles.error ? <ErrorState message={errorMessage(roles.error)} retry={() => void roles.refetch()} /> : <div className="grid gap-2 sm:grid-cols-2">{roles.data?.roles.map(role => <button type="button" key={role.id} onClick={() => role.built_in ? undefined : setEditingRole(role)} className={`rounded-lg border p-3 text-left ${editingRole?.id === role.id ? 'border-primary bg-primary/5' : 'border-border'}`}><span className="flex items-center gap-2 text-sm font-medium">{role.built_in ? displayRoleName(role.id, t) : role.name}{role.built_in && <Badge>{t('rbac.builtIn')}</Badge>}</span><span className="mt-1 block text-xs text-muted-foreground">{t('rbac.permissionCount', { count: role.permissions.length })} · {t('rbac.assignedCount', { count: users.data?.items.filter(user => user.roles.includes(role.id)).length ?? 0 })}</span></button>)}</div>}
      {can('role.manage') && <form className="space-y-3 rounded-lg border border-border p-4" onSubmit={submitRole}>
        <label className="block space-y-1.5 text-sm">{t('rbac.roleName')}<Input value={roleName} onChange={event => setRoleName(event.target.value)} maxLength={64} required disabled={editingRole?.built_in} /></label>
        {editingRole?.built_in ? <p className="text-xs text-muted-foreground">{t('rbac.builtInHelp')}</p> : <div className="grid gap-3 sm:grid-cols-2">{groupedPermissions.map(([group, permissions]) => <fieldset key={group} className="rounded-md border border-border p-3"><legend className="px-1 text-sm font-medium">{t(`rbac.domain.${group}` as TranslationKey)}</legend><div className="space-y-2">{permissions.map(permission => <label key={permission} className="flex items-center gap-2 text-xs"><input type="checkbox" checked={rolePermissions.includes(permission)} onChange={event => setRolePermissions(current => event.target.checked ? [...current, permission] : current.filter(item => item !== permission))} />{t(permissionLabels[permission] ?? ('rbac.permission.unknown' as TranslationKey))}</label>)}</div></fieldset>)}</div>}
        <div className="flex flex-wrap gap-2">{editingRole ? <><Button type="submit" disabled={updateRole.isPending}>{t('rbac.saveRole')}</Button><Button type="button" variant="outline" onClick={() => setEditingRole(undefined)}>{t('common.cancel')}</Button><Confirm trigger={<Button type="button" variant="destructive">{t('rbac.deleteRole')}</Button>} title={t('rbac.deleteConfirmTitle')} description={t('rbac.deleteConfirmBody')} confirmLabel={t('rbac.deleteRole')} destructive onConfirm={() => deleteRole.mutate(editingRole.id)} disabled={deleteRole.isPending} /></> : <Button type="submit" disabled={createRole.isPending}>{t('rbac.createRole')}</Button>}</div>
      </form>}
    </CardContent></Card>}
  </div>
}
