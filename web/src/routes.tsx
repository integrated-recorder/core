import { lazy } from 'react'
import { createRootRouteWithContext, createRoute, createRouter, redirect } from '@tanstack/react-router'
import type { QueryClient } from '@tanstack/react-query'
import { RootComponent, RootError } from '@/components/route-shell'
import { accountAuthorizationQuery, installationQuery, sessionQuery } from '@/api/queries'
import { installationRouteRedirect } from '@/lib/installation-routing'
const DashboardPage = lazy(() => import('@/routes/dashboard').then(module => ({ default: module.DashboardPage })))
const RecordingsPage = lazy(() => import('@/routes/recordings').then(module => ({ default: module.RecordingsPage })))
const RecordingDetailPage = lazy(() => import('@/routes/recording-detail').then(module => ({ default: module.RecordingDetailPage })))
const NewRecordingPage = lazy(() => import('@/routes/new-recording').then(module => ({ default: module.NewRecordingPage })))
const AdaptersPage = lazy(() => import('@/routes/adapters').then(module => ({ default: module.AdaptersPage })))
const PluginsPage = lazy(() => import('@/routes/adapters').then(module => ({ default: module.PluginsPage })))
const AdapterDetailPage = lazy(() => import('@/routes/adapter-detail').then(module => ({ default: module.AdapterDetailPage })))
const WorkflowsPage = lazy(() => import('@/routes/workflows').then(module => ({ default: module.WorkflowsPage })))
const WorkflowDetailPage = lazy(() => import('@/routes/workflow-detail').then(module => ({ default: module.WorkflowDetailPage })))
const SettingsPage = lazy(() => import('@/routes/settings').then(module => ({ default: module.SettingsPage })))
const WatchesPage = lazy(() => import('@/routes/watches').then(module => ({ default: module.WatchesPage })))
const WatchNewPage = lazy(() => import('@/routes/watch-new').then(module => ({ default: module.WatchNewPage })))
const WatchDetailPage = lazy(() => import('@/routes/watch-detail').then(module => ({ default: module.WatchDetailPage })))
const StoragePage = lazy(() => import('@/routes/storage').then(module => ({ default: module.StoragePage })))
const StorageLocationsPage = lazy(() => import('@/routes/storage').then(module => ({ default: module.StorageLocationsPage })))
const StoragePoolDetailPage = lazy(() => import('@/routes/storage').then(module => ({ default: module.StoragePoolDetailPage })))
const LoginPage = lazy(() => import('@/routes/login').then(module => ({ default: module.LoginPage })))
const SetupPage = lazy(() => import('@/routes/setup').then(module => ({ default: module.SetupPage })))
import { NotFoundPage } from '@/routes/not-found'
import { isRecordingState } from '@/types/api'

type RouterContext = { queryClient: QueryClient }
type RecordRouteSearch = { q?: string; state?: string; adapter?: string; resource_type?: string; started_after?: string; started_before?: string; has_gaps?: string; integrity?: string; tag?: string; sort?: string; limit?: number; cursor?: string }
const rootRoute = createRootRouteWithContext<RouterContext>()({
  beforeLoad: async ({ context, location }) => {
    const path = location.pathname
    const installation = await context.queryClient.ensureQueryData(installationQuery)
    const bootstrapCompatibility = path === '/login' && (location.search as Record<string, unknown>).mode === 'bootstrap'
    if (path === '/setup') {
      const destination = installationRouteRedirect(path, installation, false)
      if (destination) throw redirect({ to: destination })
      return
    }
    if (installation.state !== 'ready' && !bootstrapCompatibility) throw redirect({ to: '/setup' })
    if (bootstrapCompatibility) throw redirect({ to: '/setup' })
    const session = await context.queryClient.ensureQueryData(sessionQuery)
    const destination = installationRouteRedirect(path, installation, installation.auth_disabled || session.authenticated)
    if (destination) throw redirect({ to: destination })
    if (session.auth_enabled && session.authenticated) {
      const required = path === '/' || path === '/recordings' || path.startsWith('/recordings/') ? [['recording.read']]
        : path === '/new' ? [['recording.create']]
        : path === '/sources' || path.startsWith('/sources/') ? [['recording.create']]
          : path === '/plugins' || path.startsWith('/plugins/') ? [['plugin.read', 'plugin.manage']]
            : path === '/watches' || path.startsWith('/watches/') ? [['watch.read']]
              : path === '/storage/locations' ? [['storage.manage']]
                  : path === '/storage/advanced' ? [['system.settings.manage']]
                : path === '/storage/retention' ? [['recording.delete', 'system.settings.manage']]
                  : path === '/storage' || path.startsWith('/storage/') ? [['storage.read', 'storage.manage']]
                : path === '/system/users' ? [['user.read', 'user.create', 'user.manage', 'role.read', 'role.manage']]
                  : path === '/system/updates' ? [['system.update.manage']]
                    : path === '/system/diagnostics' ? [['system.diagnostics.read']]
                      : path === '/system/audit' ? [['audit.read']]
                        : path === '/system' ? [['system.read', 'system.settings.manage', 'system.diagnostics.read']]
                          : path.startsWith('/workflows') ? [['recording.workflow.manage']]
                              : []
      if (required.length) {
        const authorization = await context.queryClient.ensureQueryData(accountAuthorizationQuery)
        if (!required.every(group => group.some(permission => authorization.permissions.includes(permission)))) throw redirect({ to: '/account/security' })
      }
    }
  },
  component: RootComponent,
  notFoundComponent: NotFoundPage,
  errorComponent: RootError,
})
const loginRoute = createRoute({ getParentRoute: () => rootRoute, path: '/login', validateSearch: (search: Record<string, unknown>): { mode?: 'bootstrap' } => search.mode === 'bootstrap' ? { mode: 'bootstrap' } : {}, beforeLoad: async ({ context }) => {
  const session = await context.queryClient.ensureQueryData(sessionQuery)
  if (session.authenticated) throw redirect({ to: '/' })
}, component: LoginPage })
const setupRoute = createRoute({ getParentRoute: () => rootRoute, path: '/setup', component: SetupPage })
const dashboardRoute = createRoute({ getParentRoute: () => rootRoute, path: '/', component: DashboardPage })
const recordingsRoute = createRoute({ getParentRoute: () => rootRoute, path: '/recordings', validateSearch: (search: Record<string, unknown>): RecordRouteSearch => ({
  q: typeof search.q === 'string' ? search.q : undefined, state: isRecordingState(search.state) ? search.state : undefined,
  adapter: typeof search.adapter === 'string' ? search.adapter : undefined, resource_type: typeof search.resource_type === 'string' ? search.resource_type : undefined,
  started_after: typeof search.started_after === 'string' ? search.started_after : undefined, started_before: typeof search.started_before === 'string' ? search.started_before : undefined,
  has_gaps: typeof search.has_gaps === 'string' ? search.has_gaps : undefined, integrity: typeof search.integrity === 'string' ? search.integrity : undefined,
  tag: typeof search.tag === 'string' ? search.tag : undefined, sort: typeof search.sort === 'string' ? search.sort : '-started_at',
  limit: typeof search.limit === 'number' ? search.limit : 25, cursor: typeof search.cursor === 'string' ? search.cursor : undefined,
}), component: RecordingsPage })
const recordingRoute = createRoute({ getParentRoute: () => rootRoute, path: '/recordings/$recordingId', component: RecordingDetailPage })
const newRoute = createRoute({ getParentRoute: () => rootRoute, path: '/new', component: NewRecordingPage })
const adaptersRoute = createRoute({ getParentRoute: () => rootRoute, path: '/adapters', beforeLoad: () => { throw redirect({ to: '/sources' }) } })
const legacyAdapterRoute = createRoute({ getParentRoute: () => rootRoute, path: '/adapters/$adapterId', validateSearch: (search: Record<string, unknown>) => ({ tab: search.tab === 'settings' || search.tab === 'configuration' || search.tab === 'resources' || search.tab === 'workflows' ? search.tab as 'settings' | 'configuration' | 'resources' | 'workflows' : undefined }), beforeLoad: ({ params, search }) => { throw redirect({ to: '/sources/$adapterId', params, search }) } })
const sourcesRoute = createRoute({ getParentRoute: () => rootRoute, path: '/sources', component: AdaptersPage })
const adapterRoute = createRoute({ getParentRoute: () => rootRoute, path: '/sources/$adapterId', validateSearch: (search: Record<string, unknown>) => ({ tab: search.tab === 'configuration' || search.tab === 'settings' || search.tab === 'resources' || search.tab === 'workflows' ? search.tab as 'configuration' | 'settings' | 'resources' | 'workflows' : undefined }), component: AdapterDetailPage })
const pluginsRoute = createRoute({ getParentRoute: () => rootRoute, path: '/plugins', component: PluginsPage })
const workflowsRoute = createRoute({ getParentRoute: () => rootRoute, path: '/workflows', component: WorkflowsPage })
const workflowRoute = createRoute({ getParentRoute: () => rootRoute, path: '/workflows/$workflowId', component: WorkflowDetailPage })
const settingsRoute = createRoute({ getParentRoute: () => rootRoute, path: '/settings', beforeLoad: () => { throw redirect({ to: '/account/preferences' }) } })
const preferencesRoute = createRoute({ getParentRoute: () => rootRoute, path: '/account/preferences', component: () => <SettingsPage section="preferences" /> })
const accountRoute = createRoute({ getParentRoute: () => rootRoute, path: '/account/security', component: () => <SettingsPage section="security" /> })
const sessionsRoute = createRoute({ getParentRoute: () => rootRoute, path: '/account/sessions', component: () => <SettingsPage section="sessions" /> })
const systemRoute = createRoute({ getParentRoute: () => rootRoute, path: '/system', component: () => <SettingsPage section="runtime" /> })
const systemUsersRoute = createRoute({ getParentRoute: () => rootRoute, path: '/system/users', component: () => <SettingsPage section="users" /> })
const systemUpdatesRoute = createRoute({ getParentRoute: () => rootRoute, path: '/system/updates', component: () => <SettingsPage section="updates" /> })
const systemDiagnosticsRoute = createRoute({ getParentRoute: () => rootRoute, path: '/system/diagnostics', component: () => <SettingsPage section="diagnostics" /> })
const systemAuditRoute = createRoute({ getParentRoute: () => rootRoute, path: '/system/audit', component: () => <SettingsPage section="audit" /> })
const watchesRoute = createRoute({ getParentRoute: () => rootRoute, path: '/watches', component: WatchesPage })
const watchNewRoute = createRoute({ getParentRoute: () => rootRoute, path: '/watches/new', component: WatchNewPage })
const watchDetailRoute = createRoute({ getParentRoute: () => rootRoute, path: '/watches/$watchId', component: WatchDetailPage })
const storageRoute = createRoute({ getParentRoute: () => rootRoute, path: '/storage', component: StoragePage })
const storageLocationsRoute = createRoute({ getParentRoute: () => rootRoute, path: '/storage/locations', component: StorageLocationsPage })
const storageRetentionRoute = createRoute({ getParentRoute: () => rootRoute, path: '/storage/retention', component: () => <SettingsPage section="retention" /> })
const storageAdvancedRoute = createRoute({ getParentRoute: () => rootRoute, path: '/storage/advanced', component: () => <SettingsPage section="advanced-storage" /> })
const storagePoolRoute = createRoute({ getParentRoute: () => rootRoute, path: '/storage/$poolId', component: StoragePoolDetailPage })

const routeTree = rootRoute.addChildren([loginRoute, setupRoute, dashboardRoute, recordingsRoute, recordingRoute, newRoute, adaptersRoute, legacyAdapterRoute, sourcesRoute, adapterRoute, pluginsRoute, workflowsRoute, workflowRoute, watchesRoute, watchNewRoute, watchDetailRoute, storageRoute, storageLocationsRoute, storageRetentionRoute, storageAdvancedRoute, storagePoolRoute, settingsRoute, preferencesRoute, accountRoute, sessionsRoute, systemRoute, systemUsersRoute, systemUpdatesRoute, systemDiagnosticsRoute, systemAuditRoute])
export const router = createRouter({ routeTree, context: { queryClient: undefined! }, defaultPreload: 'intent', defaultPreloadStaleTime: 0 })
declare module '@tanstack/react-router' { interface Register { router: typeof router } }
