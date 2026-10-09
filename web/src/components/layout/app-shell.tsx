import { useEffect, useRef, useState } from 'react'
import { Link, Outlet, useNavigate, useRouterState } from '@tanstack/react-router'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Bell, ChevronDown, Command, HardDrive, LayoutDashboard, LogOut, Menu, Moon, Search, Settings2, Sun, Video, X, Cable, Plus, Radio, Workflow } from 'lucide-react'
import { authAPI, productAPI, userPreferencesAPI } from '@/api'
import { invalidateCSRFToken } from '@/api/client'
import { adaptersQuery, dashboardQuery, notificationsQuery, qk, workflowsQuery } from '@/api/queries'
import { formatBytes, formatDate, humanize } from '@/lib/utils'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover'
import { useToast } from '@/components/ui/use-toast'
import { errorMessage } from '@/lib/errors'
import { AppBrand } from '@/components/layout/app-brand'
import { statusLabel } from '@/lib/labels'
import { AdapterMark } from '@/components/adapter/adapter-mark'
import type { ApiSession, Notification } from '@/types/api'
import { useI18n } from '@/i18n/provider'

const primaryNavigation = [
  { to: '/', key: 'nav.dashboard', icon: LayoutDashboard }, { to: '/recordings', key: 'nav.recordings', icon: Video },
] as const
const managementNavigation = [
  { to: '/adapters', key: 'nav.sources', icon: Cable }, { to: '/storage', key: 'nav.storage', icon: HardDrive },
  { to: '/watches', key: 'nav.automation', icon: Radio }, { to: '/workflows', key: 'nav.workflows', icon: Workflow },
  { to: '/settings', key: 'nav.settings', icon: Settings2 },
] as const

export function AppShell() {
  const location = useRouterState({ select: state => state.location })
  const navigate = useNavigate()
  const [mobileOpen, setMobileOpen] = useState(false)
  const [search, setSearch] = useState('')
  const [searchTerm, setSearchTerm] = useState('')
  const [searchOpen, setSearchOpen] = useState(false)
  const searchInput = useRef<HTMLInputElement>(null)
  const { preferences, setPreferences, t } = useI18n()
  const theme = document.documentElement.classList.contains('dark') ? 'dark' : 'light'
  const client = useQueryClient()
  const { toast } = useToast()
  const dashboardQueryResult = useQuery(dashboardQuery)
  const dashboard = dashboardQueryResult.data
  const adapters = useQuery(adaptersQuery)
  const { data: notifications } = useQuery(notificationsQuery)
  const { data: workflows } = useQuery(workflowsQuery)
  const searchQuery = useQuery({ queryKey: ['global-search', searchTerm], queryFn: () => productAPI.search(searchTerm), enabled: searchTerm.length >= 2, staleTime: 10_000 })
  const notificationItems = [...new Map((notifications?.items ?? []).map(item => [item.id, item])).values()]
  const unread = notificationItems.filter(item => !item.read).length
  const logout = useMutation({ mutationFn: authAPI.logout, onSuccess: async () => {
    invalidateCSRFToken()
    client.setQueryData<ApiSession>(qk.session, current => current ? { ...current, authenticated: false, csrf_token: undefined, expires_at: undefined } : undefined)
    await navigate({ to: '/login' })
  }, onError: error => toast(t('shell.logoutFailed'), errorMessage(error), 'error') })
  const saveTheme = useMutation({ mutationFn: (theme: 'light' | 'dark') => userPreferencesAPI.update({ ...preferences, theme }), onSuccess: result => { client.setQueryData(qk.userPreferences, result); setPreferences(result); toast(t('settings.saved')) }, onError: error => toast(t('settings.saveFailed'), errorMessage(error), 'error') })
  const markAll = useMutation({ mutationFn: productAPI.markAllRead, onMutate: async () => { await client.cancelQueries({ queryKey: qk.notifications }); const previous = client.getQueryData<{ items: Notification[] }>(qk.notifications); client.setQueryData<{ items: Notification[] }>(qk.notifications, current => current ? { items: current.items.map(item => ({ ...item, read: true })) } : current); return { previous } }, onError: (error, _variables, context) => { if (context?.previous) client.setQueryData(qk.notifications, context.previous); toast(t('errors.generic'), errorMessage(error), 'error') }, onSettled: () => client.invalidateQueries({ queryKey: qk.notifications }) })
  const markOne = useMutation({ mutationFn: productAPI.markRead, onMutate: async id => { await client.cancelQueries({ queryKey: qk.notifications }); const previous = client.getQueryData<{ items: Notification[] }>(qk.notifications); client.setQueryData<{ items: Notification[] }>(qk.notifications, current => current ? { items: current.items.map(item => item.id === id ? { ...item, read: true } : item) } : current); return { previous } }, onError: (error, _id, context) => { if (context?.previous) client.setQueryData(qk.notifications, context.previous); toast(t('errors.generic'), errorMessage(error), 'error') }, onSettled: () => client.invalidateQueries({ queryKey: qk.notifications }) })
  const setDisplayTheme = (next: 'light' | 'dark') => saveTheme.mutate(next)
  useEffect(() => {
    const timer = window.setTimeout(() => setSearchTerm(search.trim()), 250)
    return () => window.clearTimeout(timer)
  }, [search])
  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === 'k') {
        event.preventDefault()
        searchInput.current?.focus()
        setSearchOpen(true)
      }
    }
    window.addEventListener('keydown', onKeyDown)
    return () => window.removeEventListener('keydown', onKeyDown)
  }, [])
  const normalizedPath = location.pathname
  const sidebar = <>
    <div className="flex h-[4.4rem] min-w-0 items-center gap-3 border-b border-border px-4"><AppBrand variant="sidebar" className="min-w-0 flex-1" /><Button variant="ghost" size="icon" className="shrink-0 lg:hidden" aria-label={t('shell.closeMenu')} onClick={() => setMobileOpen(false)}><X className="h-4 w-4" /></Button></div>
    <nav id="primary-navigation" className="space-y-1 px-3 py-5" aria-label={t('nav.primary')}>{primaryNavigation.map(item => { const Icon = item.icon; const active = item.to === '/' ? normalizedPath === '/' : normalizedPath === item.to || normalizedPath.startsWith(`${item.to}/`); return <Link key={item.to} to={item.to} onClick={() => setMobileOpen(false)} className={`focus-ring flex h-10 items-center gap-3 rounded-md px-3 text-sm font-medium transition-colors ${active ? 'bg-accent text-accent-foreground' : 'text-muted-foreground hover:bg-muted hover:text-foreground'}`} aria-current={active ? 'page' : undefined}><Icon className="h-[17px] w-[17px] shrink-0" />{t(item.key)}</Link> })}
      <Link to="/new" onClick={() => setMobileOpen(false)} aria-current={normalizedPath === '/new' ? 'page' : undefined} className={`focus-ring mt-3 flex h-11 items-center gap-2 rounded-lg bg-primary px-3 text-sm font-semibold text-primary-foreground shadow-sm transition hover:brightness-95 focus-visible:ring-primary ${normalizedPath === '/new' ? 'ring-2 ring-primary ring-offset-2 ring-offset-card' : ''}`}><Plus className="h-4 w-4" />{t('nav.newRecording')}</Link>
    </nav>
    <div className="px-3 pb-3"><h2 className="px-3 pb-2 text-[10px] font-semibold uppercase tracking-wide text-muted-foreground">{t('nav.settingsGroup')}</h2><nav aria-label={t('nav.settingsLinks')} className="space-y-1">{managementNavigation.map(item => { const Icon = item.icon; const active = normalizedPath === item.to || normalizedPath.startsWith(`${item.to}/`); const pendingWorkflows = item.to === '/workflows' ? workflows?.filter(workflow => workflow.in_progress).length ?? 0 : 0; return <Link key={item.to} to={item.to} onClick={() => setMobileOpen(false)} className={`focus-ring flex h-9 items-center gap-3 rounded-md px-3 text-xs font-medium transition-colors ${active ? 'bg-accent text-accent-foreground' : 'text-muted-foreground hover:bg-muted hover:text-foreground'}`} aria-current={active ? 'page' : undefined}><Icon className="h-4 w-4 shrink-0" />{t(item.key)}{pendingWorkflows > 0 && <span className="ml-auto rounded-full bg-primary/10 px-2 py-0.5 text-[10px] text-primary">{pendingWorkflows}</span>}</Link> })}</nav></div>
    <Link to="/storage" className="border-t border-border px-5 py-4 hover:bg-muted/60"><div className="mb-2 flex items-center justify-between text-[11px] font-medium text-muted-foreground"><span className="flex items-center gap-1.5"><HardDrive className="h-3.5 w-3.5" />{t('shell.archive')}</span><span>{dashboard ? dashboard.archive_bytes_known === false ? '—' : formatBytes(dashboard.archive_bytes) : t('shell.loading')}</span></div>{dashboard && <progress className="storage-progress" value={dashboard.filesystem_used_bytes} max={dashboard.filesystem_total_bytes || 1} aria-label={t('shell.filesystemUsage')} />}<p className="mt-1 text-[10px] text-muted-foreground">{dashboard ? `${formatBytes(dashboard.filesystem_free_bytes)} ${t('shell.free')}` : t('shell.storageStatus')}</p></Link>
  </>
  const resultItems = searchQuery.data?.results ?? []
  return <div className="min-h-screen bg-background text-foreground">
    {mobileOpen && <button aria-label={t('shell.closeMenu')} className="fixed inset-0 z-30 bg-black/40 lg:hidden" onClick={() => setMobileOpen(false)} />}
    <aside className={`fixed inset-y-0 left-0 z-40 flex w-[248px] flex-col border-r border-border bg-card transition-transform lg:translate-x-0 lg:visible ${mobileOpen ? 'translate-x-0 visible' : '-translate-x-full max-lg:invisible'}`}>{sidebar}</aside>
    <div className="lg:pl-[248px]">
      <header className="sticky top-0 z-20 flex h-[4.4rem] items-center gap-3 border-b border-border bg-background/95 px-4 backdrop-blur md:px-7">
        <Button variant="ghost" size="icon" className="lg:hidden" aria-label={t('shell.openMenu')} aria-expanded={mobileOpen} aria-controls="primary-navigation" onClick={() => setMobileOpen(true)}><Menu className="h-5 w-5" /></Button>
        <div className="relative w-full max-w-[560px]"><Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" /><input ref={searchInput} value={search} onChange={event => setSearch(event.target.value)} onFocus={() => setSearchOpen(true)} onBlur={() => window.setTimeout(() => setSearchOpen(false), 150)} onKeyDown={event => { if (event.key === 'Escape') { setSearch(''); setSearchTerm(''); setSearchOpen(false) } }} placeholder={t('shell.search')} aria-label={t('shell.searchLabel')} className="focus-ring h-9 w-full rounded-md border border-border bg-card pl-9 pr-16 text-sm placeholder:text-muted-foreground" /><kbd className="absolute right-2.5 top-1/2 -translate-y-1/2 rounded border border-border px-1.5 py-0.5 text-[10px] text-muted-foreground"><Command className="inline h-3 w-3" /> K</kbd>{searchOpen && search.trim().length >= 2 && <div className="absolute left-0 right-0 top-11 z-50 max-h-[min(70vh,28rem)] overflow-auto rounded-lg border border-border bg-popover p-1 shadow-xl">{searchTerm !== search.trim() || searchQuery.isLoading ? <p className="p-3 text-sm text-muted-foreground">{t('shell.searching')}</p> : searchQuery.error ? <p className="p-3 text-sm text-destructive">{t('shell.searchFailed')}</p> : resultItems.length ? resultItems.map((item, index) => { const href = item.type === 'recording' ? `/recordings/${item.id ?? ''}` : item.type === 'adapter' ? `/adapters/${item.id ?? item.adapter_id ?? ''}` : item.type === 'workflow' ? `/workflows/${item.workflow_id ?? item.id ?? ''}` : item.adapter_id ? `/adapters/${item.adapter_id}?tab=resources` : '/adapters'; return <Link key={`${item.type}-${item.id ?? item.workflow_id ?? item.resource_id}-${index}`} to={href} onClick={() => { setSearch(''); setSearchTerm('') }} className="flex items-center gap-3 rounded-md px-3 py-2.5 hover:bg-accent">{item.type === 'adapter' || item.adapter_id ? <><AdapterMark adapter={adapters.data?.find(adapter => adapter.status.id === (item.adapter_id ?? item.id))} adapterId={item.adapter_id ?? item.id} size="xs" /><Badge tone="neutral">{item.type === 'adapter' ? t('shell.searchType.source') : item.type === 'resource' ? t('shell.searchType.resource') : t('shell.searchType.workflow')}</Badge></> : <Badge tone="neutral">{item.type === 'recording' ? t('shell.searchType.recording') : humanize(item.type)}</Badge>}<span className="min-w-0 flex-1 truncate text-sm">{item.title ?? item.name ?? item.display_name ?? item.id ?? item.workflow_id ?? item.resource_id}</span><span className="shrink-0 text-xs text-muted-foreground">{item.state ? statusLabel(item.state) : ''}</span></Link> }) : <p className="p-3 text-sm text-muted-foreground">{t('shell.searchEmpty')}</p>}</div>}</div>
        <div className="ml-auto flex shrink-0 items-center gap-1.5">
          <div className="hidden items-center gap-2 rounded-full border border-border px-2.5 py-1.5 text-xs text-muted-foreground sm:flex"><span className={`h-1.5 w-1.5 rounded-full ${dashboardQueryResult.isError ? 'bg-destructive' : dashboardQueryResult.isLoading ? 'bg-amber-500' : 'bg-emerald-500'}`} />{dashboardQueryResult.isError ? t('shell.apiError') : dashboardQueryResult.isLoading ? t('shell.apiChecking') : t('shell.apiConnected')}</div>
          <Popover><PopoverTrigger asChild><Button variant="ghost" size="icon" className="relative" aria-label={`${t('shell.notifications')}${unread ? ` ${t('shell.unread', { count: unread })}` : ''}`}><Bell className="h-[18px] w-[18px]" />{unread > 0 && <span className="absolute right-1 top-1 h-2 w-2 rounded-full bg-primary ring-2 ring-background" />}</Button></PopoverTrigger><PopoverContent align="end" className="w-[min(22rem,calc(100vw-2rem))] p-0"><div className="flex items-center justify-between border-b border-border px-4 py-3"><div><p className="text-sm font-semibold">{t('shell.notifications')}</p><p className="text-xs text-muted-foreground">{t('shell.unread', { count: unread })}</p></div><Button variant="ghost" size="sm" onClick={() => markAll.mutate()} disabled={!unread || markAll.isPending}>{t('shell.markAllRead')}</Button></div><div className="max-h-80 overflow-auto">{notificationItems.length ? notificationItems.slice(0, 12).map(item => <button key={item.id} onClick={() => !item.read && markOne.mutate(item.id)} disabled={markOne.isPending && markOne.variables === item.id} className="flex w-full gap-3 border-b border-border/70 px-4 py-3 text-left hover:bg-muted/60 disabled:opacity-70"><span className={`mt-1 h-2 w-2 shrink-0 rounded-full ${item.read ? 'bg-muted-foreground/30' : 'bg-primary'}`} /><span className="min-w-0"><span className="block text-sm">{humanize(item.type)}</span><span className="mt-1 block text-xs text-muted-foreground">{formatDate(item.at)}</span></span></button>) : <p className="p-5 text-center text-sm text-muted-foreground">{t('shell.noNotifications')}</p>}</div></PopoverContent></Popover>
          <Button variant="ghost" size="icon" aria-label={t('settings.theme')} onClick={() => setDisplayTheme(theme === 'dark' ? 'light' : 'dark')}>{theme === 'dark' ? <Sun className="h-[18px] w-[18px]" /> : <Moon className="h-[18px] w-[18px]" />}</Button>
          <Popover><PopoverTrigger asChild><Button variant="ghost" className="gap-2 px-2"><span className="grid h-7 w-7 place-items-center rounded-full bg-primary/10 text-xs font-bold text-primary">IR</span><span className="hidden sm:inline">{t('shell.admin')}</span><ChevronDown className="hidden h-3.5 w-3.5 sm:inline" /></Button></PopoverTrigger><PopoverContent align="end" className="w-48 p-1"><Button variant="ghost" className="w-full justify-start" onClick={() => logout.mutate()} disabled={logout.isPending}><LogOut className="h-4 w-4" />{t('shell.logout')}</Button></PopoverContent></Popover>
        </div>
      </header>
      <main id="main-content" className="mx-auto w-full max-w-[1680px] px-4 py-6 md:px-7 md:py-8"><Outlet /></main>
    </div>
  </div>
}
