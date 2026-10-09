import { useEffect, useRef, useState } from 'react'
import { Link, Outlet, useNavigate, useRouterState } from '@tanstack/react-router'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Bell, ChevronDown, Command, HardDrive, LayoutDashboard, LogOut, Menu, Moon, Search, Settings2, Sun, Video, Workflow, X, Cable, Plus, Radio } from 'lucide-react'
import { authAPI, productAPI } from '@/api'
import { invalidateCSRFToken } from '@/api/client'
import { adaptersQuery, dashboardQuery, notificationsQuery, qk, workflowsQuery } from '@/api/queries'
import { formatBytes, formatDate, humanize } from '@/lib/utils'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover'
import { useToast } from '@/components/ui/use-toast'
import { errorMessage } from '@/lib/errors'
import { applyResolvedTheme, resolveTheme, subscribeThemePreference, type ThemePreference } from '@/lib/theme'
import { AppBrand } from '@/components/layout/app-brand'
import { statusLabel } from '@/lib/labels'
import { AdapterMark } from '@/components/adapter/adapter-mark'
import type { ApiSession, Notification } from '@/types/api'

const navigation = [
  { to: '/', label: '대시보드', icon: LayoutDashboard }, { to: '/recordings', label: '녹화', icon: Video },
  { to: '/new', label: '새 녹화', icon: Plus }, { to: '/adapters', label: '어댑터', icon: Cable },
  { to: '/watches', label: '자동 녹화', icon: Radio }, { to: '/workflows', label: '워크플로', icon: Workflow },
  { to: '/storage', label: '저장소', icon: HardDrive }, { to: '/settings', label: '설정', icon: Settings2 },
] as const

export function AppShell() {
  const location = useRouterState({ select: state => state.location })
  const navigate = useNavigate()
  const [mobileOpen, setMobileOpen] = useState(false)
  const [search, setSearch] = useState('')
  const [searchTerm, setSearchTerm] = useState('')
  const [searchOpen, setSearchOpen] = useState(false)
  const searchInput = useRef<HTMLInputElement>(null)
  const [theme, setTheme] = useState<'light' | 'dark'>(() => document.documentElement.classList.contains('dark') ? 'dark' : 'light')
  const [themePreference, setThemePreference] = useState<ThemePreference>(() => {
    const stored = localStorage.getItem('ir-theme')
    return stored === 'light' || stored === 'dark' || stored === 'system' ? stored : 'system'
  })
  const client = useQueryClient()
  const { toast } = useToast()
  const dashboardQueryResult = useQuery(dashboardQuery)
  const dashboard = dashboardQueryResult.data
  const adapters = useQuery(adaptersQuery)
  const { data: notifications } = useQuery(notificationsQuery)
  const { data: workflows } = useQuery(workflowsQuery)
  const settings = useQuery({ queryKey: qk.settings, queryFn: productAPI.settings, staleTime: 60_000 })
  const searchQuery = useQuery({ queryKey: ['global-search', searchTerm], queryFn: () => productAPI.search(searchTerm), enabled: searchTerm.length >= 2, staleTime: 10_000 })
  const notificationItems = [...new Map((notifications?.items ?? []).map(item => [item.id, item])).values()]
  const unread = notificationItems.filter(item => !item.read).length
  const logout = useMutation({ mutationFn: authAPI.logout, onSuccess: async () => {
    invalidateCSRFToken()
    client.setQueryData<ApiSession>(qk.session, current => current ? { ...current, authenticated: false, csrf_token: undefined, expires_at: undefined } : undefined)
    await navigate({ to: '/login' })
  }, onError: error => toast('로그아웃 실패', errorMessage(error), 'error') })
  const saveTheme = useMutation({ mutationFn: (next: ThemePreference) => productAPI.saveSettings({ ui: { theme: next } }), onSuccess: result => { client.setQueryData(qk.settings, result); toast('테마 설정을 저장했습니다.') }, onError: error => toast('테마 저장 실패', errorMessage(error), 'error') })
  const markAll = useMutation({ mutationFn: productAPI.markAllRead, onMutate: async () => { await client.cancelQueries({ queryKey: qk.notifications }); const previous = client.getQueryData<{ items: Notification[] }>(qk.notifications); client.setQueryData<{ items: Notification[] }>(qk.notifications, current => current ? { items: current.items.map(item => ({ ...item, read: true })) } : current); return { previous } }, onError: (error, _variables, context) => { if (context?.previous) client.setQueryData(qk.notifications, context.previous); toast('알림을 읽음 처리하지 못했습니다.', errorMessage(error), 'error') }, onSettled: () => client.invalidateQueries({ queryKey: qk.notifications }) })
  const markOne = useMutation({ mutationFn: productAPI.markRead, onMutate: async id => { await client.cancelQueries({ queryKey: qk.notifications }); const previous = client.getQueryData<{ items: Notification[] }>(qk.notifications); client.setQueryData<{ items: Notification[] }>(qk.notifications, current => current ? { items: current.items.map(item => item.id === id ? { ...item, read: true } : item) } : current); return { previous } }, onError: (error, _id, context) => { if (context?.previous) client.setQueryData(qk.notifications, context.previous); toast('알림을 읽음 처리하지 못했습니다.', errorMessage(error), 'error') }, onSettled: () => client.invalidateQueries({ queryKey: qk.notifications }) })
  const setDisplayTheme = (next: 'light' | 'dark') => { const preference: ThemePreference = next; setThemePreference(preference); setTheme(next); applyResolvedTheme(next); localStorage.setItem('ir-theme', preference); saveTheme.mutate(preference, { onError: () => { const previous = themePreference; const resolved = resolveTheme(previous, matchMedia('(prefers-color-scheme: dark)').matches); setThemePreference(previous); setTheme(resolved); applyResolvedTheme(resolved); localStorage.setItem('ir-theme', previous) } }) }
  useEffect(() => {
    const timer = window.setTimeout(() => setSearchTerm(search.trim()), 250)
    return () => window.clearTimeout(timer)
  }, [search])
  useEffect(() => {
    const preference = settings.data?.settings.ui.theme ?? themePreference
    setThemePreference(preference)
    localStorage.setItem('ir-theme', preference)
    return subscribeThemePreference(preference, resolved => {
      setTheme(resolved)
      applyResolvedTheme(resolved)
    })
  }, [settings.data?.settings.ui.theme, themePreference])
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
    <div className="flex h-[4.4rem] min-w-0 items-center gap-3 border-b border-border px-4"><AppBrand variant="sidebar" className="min-w-0 flex-1" /><Button variant="ghost" size="icon" className="shrink-0 lg:hidden" aria-label="메뉴 닫기" onClick={() => setMobileOpen(false)}><X className="h-4 w-4" /></Button></div>
    <nav id="primary-navigation" className="flex-1 space-y-1 px-3 py-5" aria-label="주 메뉴">{navigation.map(item => { const Icon = item.icon; const active = item.to === '/' ? normalizedPath === '/' : normalizedPath === item.to || normalizedPath.startsWith(`${item.to}/`); const pendingWorkflows = workflows?.filter(workflow => workflow.in_progress).length ?? 0; return <Link key={item.to} to={item.to} onClick={() => setMobileOpen(false)} className={`focus-ring flex h-10 items-center gap-3 rounded-md px-3 text-sm font-medium transition-colors ${active ? 'bg-accent text-accent-foreground' : 'text-muted-foreground hover:bg-muted hover:text-foreground'}`} aria-current={active ? 'page' : undefined}><Icon className="h-[17px] w-[17px] shrink-0" />{item.label}{item.to === '/workflows' && pendingWorkflows > 0 && <span className="ml-auto rounded-full bg-primary/10 px-2 py-0.5 text-[10px] text-primary">{pendingWorkflows}</span>}</Link> })}</nav>
    <Link to="/storage" className="border-t border-border px-5 py-4 hover:bg-muted/60"><div className="mb-2 flex items-center justify-between text-[11px] font-medium text-muted-foreground"><span className="flex items-center gap-1.5"><HardDrive className="h-3.5 w-3.5" />보관 데이터</span><span>{dashboard ? dashboard.archive_bytes_known === false ? '—' : formatBytes(dashboard.archive_bytes) : '불러오는 중'}</span></div>{dashboard && <progress className="storage-progress" value={dashboard.filesystem_used_bytes} max={dashboard.filesystem_total_bytes || 1} aria-label="파일 시스템 사용량" />}<p className="mt-1 text-[10px] text-muted-foreground">{dashboard ? `${formatBytes(dashboard.filesystem_free_bytes)} 여유` : '저장소 상태'}</p></Link>
  </>
  const resultItems = searchQuery.data?.results ?? []
  return <div className="min-h-screen bg-background text-foreground">
    {mobileOpen && <button aria-label="메뉴 닫기" className="fixed inset-0 z-30 bg-black/40 lg:hidden" onClick={() => setMobileOpen(false)} />}
    <aside className={`fixed inset-y-0 left-0 z-40 flex w-[248px] flex-col border-r border-border bg-card transition-transform lg:translate-x-0 lg:visible ${mobileOpen ? 'translate-x-0 visible' : '-translate-x-full max-lg:invisible'}`}>{sidebar}</aside>
    <div className="lg:pl-[248px]">
      <header className="sticky top-0 z-20 flex h-[4.4rem] items-center gap-3 border-b border-border bg-background/95 px-4 backdrop-blur md:px-7">
        <Button variant="ghost" size="icon" className="lg:hidden" aria-label="메뉴 열기" aria-expanded={mobileOpen} aria-controls="primary-navigation" onClick={() => setMobileOpen(true)}><Menu className="h-5 w-5" /></Button>
        <div className="relative w-full max-w-[560px]"><Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" /><input ref={searchInput} value={search} onChange={event => setSearch(event.target.value)} onFocus={() => setSearchOpen(true)} onBlur={() => window.setTimeout(() => setSearchOpen(false), 150)} onKeyDown={event => { if (event.key === 'Escape') { setSearch(''); setSearchTerm(''); setSearchOpen(false) } }} placeholder="전체 검색..." aria-label="전체 검색" className="focus-ring h-9 w-full rounded-md border border-border bg-card pl-9 pr-16 text-sm placeholder:text-muted-foreground" /><kbd className="absolute right-2.5 top-1/2 -translate-y-1/2 rounded border border-border px-1.5 py-0.5 text-[10px] text-muted-foreground"><Command className="inline h-3 w-3" /> K</kbd>{searchOpen && search.trim().length >= 2 && <div className="absolute left-0 right-0 top-11 z-50 max-h-[min(70vh,28rem)] overflow-auto rounded-lg border border-border bg-popover p-1 shadow-xl">{searchTerm !== search.trim() || searchQuery.isLoading ? <p className="p-3 text-sm text-muted-foreground">검색 중…</p> : searchQuery.error ? <p className="p-3 text-sm text-destructive">검색을 완료하지 못했습니다.</p> : resultItems.length ? resultItems.map((item, index) => { const href = item.type === 'recording' ? `/recordings/${item.id ?? ''}` : item.type === 'adapter' ? `/adapters/${item.id ?? item.adapter_id ?? ''}` : item.type === 'workflow' ? `/workflows/${item.workflow_id ?? item.id ?? ''}` : item.adapter_id ? `/adapters/${item.adapter_id}?tab=resources` : '/adapters'; return <Link key={`${item.type}-${item.id ?? item.workflow_id ?? item.resource_id}-${index}`} to={href} onClick={() => { setSearch(''); setSearchTerm('') }} className="flex items-center gap-3 rounded-md px-3 py-2.5 hover:bg-accent">{item.type === 'adapter' || item.adapter_id ? <><AdapterMark adapter={adapters.data?.find(adapter => adapter.status.id === (item.adapter_id ?? item.id))} adapterId={item.adapter_id ?? item.id} size="xs" /><Badge tone="neutral">{item.type === 'adapter' ? '어댑터' : item.type === 'resource' ? '리소스' : '워크플로'}</Badge></> : <Badge tone="neutral">{item.type === 'recording' ? '녹화' : humanize(item.type)}</Badge>}<span className="min-w-0 flex-1 truncate text-sm">{item.title ?? item.name ?? item.display_name ?? item.id ?? item.workflow_id ?? item.resource_id}</span><span className="shrink-0 text-xs text-muted-foreground">{item.state ? statusLabel(item.state) : ''}</span></Link> }) : <p className="p-3 text-sm text-muted-foreground">결과가 없습니다.</p>}</div>}</div>
        <div className="ml-auto flex shrink-0 items-center gap-1.5">
          <div className="hidden items-center gap-2 rounded-full border border-border px-2.5 py-1.5 text-xs text-muted-foreground sm:flex"><span className={`h-1.5 w-1.5 rounded-full ${dashboardQueryResult.isError ? 'bg-destructive' : dashboardQueryResult.isLoading ? 'bg-amber-500' : 'bg-emerald-500'}`} />{dashboardQueryResult.isError ? 'API 연결 오류' : dashboardQueryResult.isLoading ? 'API 확인 중' : 'API 연결됨'}</div>
          <Popover><PopoverTrigger asChild><Button variant="ghost" size="icon" className="relative" aria-label={`알림${unread ? ` ${unread}개 읽지 않음` : ''}`}><Bell className="h-[18px] w-[18px]" />{unread > 0 && <span className="absolute right-1 top-1 h-2 w-2 rounded-full bg-primary ring-2 ring-background" />}</Button></PopoverTrigger><PopoverContent align="end" className="w-[min(22rem,calc(100vw-2rem))] p-0"><div className="flex items-center justify-between border-b border-border px-4 py-3"><div><p className="text-sm font-semibold">알림</p><p className="text-xs text-muted-foreground">{unread}개 읽지 않음</p></div><Button variant="ghost" size="sm" onClick={() => markAll.mutate()} disabled={!unread || markAll.isPending}>모두 읽음</Button></div><div className="max-h-80 overflow-auto">{notificationItems.length ? notificationItems.slice(0, 12).map(item => <button key={item.id} onClick={() => !item.read && markOne.mutate(item.id)} disabled={markOne.isPending && markOne.variables === item.id} className="flex w-full gap-3 border-b border-border/70 px-4 py-3 text-left hover:bg-muted/60 disabled:opacity-70"><span className={`mt-1 h-2 w-2 shrink-0 rounded-full ${item.read ? 'bg-muted-foreground/30' : 'bg-primary'}`} /><span className="min-w-0"><span className="block text-sm">{humanize(item.type)}</span><span className="mt-1 block text-xs text-muted-foreground">{formatDate(item.at)}</span></span></button>) : <p className="p-5 text-center text-sm text-muted-foreground">새 알림이 없습니다.</p>}</div></PopoverContent></Popover>
          <Button variant="ghost" size="icon" aria-label="테마 전환" onClick={() => setDisplayTheme(theme === 'dark' ? 'light' : 'dark')}>{theme === 'dark' ? <Sun className="h-[18px] w-[18px]" /> : <Moon className="h-[18px] w-[18px]" />}</Button>
          <Popover><PopoverTrigger asChild><Button variant="ghost" className="gap-2 px-2"><span className="grid h-7 w-7 place-items-center rounded-full bg-primary/10 text-xs font-bold text-primary">IR</span><span className="hidden sm:inline">관리자</span><ChevronDown className="hidden h-3.5 w-3.5 sm:inline" /></Button></PopoverTrigger><PopoverContent align="end" className="w-48 p-1"><Button variant="ghost" className="w-full justify-start" onClick={() => logout.mutate()} disabled={logout.isPending}><LogOut className="h-4 w-4" />로그아웃</Button></PopoverContent></Popover>
        </div>
      </header>
      <main id="main-content" className="mx-auto w-full max-w-[1680px] px-4 py-6 md:px-7 md:py-8"><Outlet /></main>
    </div>
  </div>
}
