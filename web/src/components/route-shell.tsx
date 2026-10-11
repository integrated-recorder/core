import { Suspense } from 'react'
import { Outlet, useRouterState } from '@tanstack/react-router'
import { AppShell } from '@/components/layout/app-shell'
import { Button } from '@/components/ui/button'
import { useI18n } from '@/i18n/provider'

export function RootComponent() {
  const pathname = useRouterState({ select: state => state.location.pathname })
  const { t } = useI18n()
  return <Suspense fallback={<div className="grid min-h-screen place-items-center bg-background p-6 text-sm text-muted-foreground" role="status">{t('common.loading')}</div>}>{pathname === '/login' || pathname === '/setup' ? <Outlet /> : <AppShell />}</Suspense>
}

export function RootError({ reset }: { reset: () => void }) {
  const { t } = useI18n()
  return <main className="grid min-h-screen place-items-center bg-background p-5"><section className="w-full max-w-lg rounded-xl border border-border bg-card p-6 text-center" role="alert"><h1 className="text-lg font-semibold">{t('common.loadFailed')}</h1><p className="mt-2 text-sm text-muted-foreground">{t('common.checkConnection')}</p><Button className="mt-5" onClick={reset}>{t('common.retry')}</Button></section></main>
}
