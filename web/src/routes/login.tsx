import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useNavigate } from '@tanstack/react-router'
import { authAPI } from '@/api'
import { qk, sessionQuery } from '@/api/queries'
import { APIError } from '@/api/client'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { AppBrand } from '@/components/layout/app-brand'
import { useI18n } from '@/i18n/provider'

export function LoginPage() {
  const { isLoading, error } = useQuery(sessionQuery)
  const [login, setLogin] = useState('')
  const [password, setPassword] = useState('')
  const [formError, setFormError] = useState('')
  const client = useQueryClient()
  const navigate = useNavigate()
  const { t } = useI18n()
  const mutation = useMutation({ mutationFn: () => { const account = login.trim(); return account ? authAPI.login(password, account) : authAPI.login(password) }, onSuccess: async result => { client.setQueryData(qk.session, result); await client.invalidateQueries({ queryKey: qk.session }); await navigate({ to: '/' }) }, onError: error => { const message = error instanceof APIError ? error.message : t('login.failed'); setFormError(message) } })
  const submit = (event: React.FormEvent) => { event.preventDefault(); setFormError(''); if (!password) return setFormError(t('login.passwordRequired')); mutation.mutate() }

  return <main className="grid min-h-screen place-items-center bg-background px-4 py-8"><section className="w-full max-w-sm rounded-xl border border-border bg-card p-6 sm:p-8"><AppBrand variant="login" className="mb-7" /><h1 className="mb-5 text-xl font-semibold tracking-tight">{t('login.title')}</h1>{error && <p role="alert" className="mb-4 rounded-md bg-destructive/10 p-3 text-sm text-destructive">{t('login.unavailable')}</p>}<form className="space-y-4" onSubmit={submit}><div className="space-y-2"><Label htmlFor="login">{t('login.account')}</Label><Input id="login" autoComplete="username" value={login} onChange={event => setLogin(event.target.value)} /></div><div className="space-y-2"><Label htmlFor="password">{t('login.password')}</Label><Input id="password" type="password" value={password} onChange={event => setPassword(event.target.value)} autoComplete="current-password" required /></div>{formError && <p role="alert" className="rounded-md bg-destructive/10 p-3 text-sm text-destructive">{formError}</p>}<Button type="submit" className="w-full" disabled={mutation.isPending || isLoading}>{mutation.isPending ? t('login.pending') : t('login.submit')}</Button></form></section></main>
}
