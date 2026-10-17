import { fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { createMemoryHistory, createRootRoute, createRoute, createRouter, Outlet, RouterProvider } from '@tanstack/react-router'
import { accountAPI, authAPI, dashboardAPI, productAPI, runtimeUpdateAPI, userPreferencesAPI } from '@/api'
import { ToastProvider } from '@/components/ui/toast'
import { APIError } from '@/api/client'
import { configureFormatPreferences } from '@/lib/formatting'
import type { RuntimeUpdateStatus, StorageSettings, SystemSettings } from '@/types/api'
import type { UserPreferences } from '@/types/api'
import { SettingsPage, type SettingsSection } from './settings'
import { I18nProvider } from '@/i18n/provider'

const storageDefaults: StorageSettings = {
  ingest_memory: { global_buffer_bytes: 1024 ** 3, per_recording_buffer_bytes: 768 * 1024 ** 2, max_payload_bytes: 512 * 1024 ** 2 },
  queue_writer: { pending_queue_capacity: 128, writer_concurrency: 1 },
  failure_handling: { persist_attempts: 5, retry_initial_backoff_ms: 100, retry_max_backoff_ms: 800 },
  observability: { sampling_interval_ms: 5000, metrics_retention_ms: 24 * 60 * 60 * 1000 },
}
const settingsDefaults: SystemSettings = {
  settings: { ui: { theme: 'light' }, integrity: { concurrency: 1 }, retention: { enabled: false, completed_after_days: 30 }, storage: storageDefaults },
  effective_storage: storageDefaults,
  restart_required: [],
}
const storageInfo = { archive_root: '', filesystem_total_bytes: 0, filesystem_used_bytes: 0, filesystem_available_bytes: 0, recordings_bytes: 0, recording_count: 0, segment_count: 0, init_segment_count: 0, manifest_count: 0 }
const systemInfo = { version: 'test', commit: 'test', go_version: 'go', goos: 'linux', goarch: 'amd64', started_at: '2026-09-29T00:00:00Z', uptime_seconds: 1, export_available: false }
const runtimeIdentity = { version: '1.0.0', commit: 'abcdef0123456789', build_time: '2026-09-28T12:00:00Z', release_channel: 'stable', runtime_protocol_version: 1 }
const updateStatus: RuntimeUpdateStatus = {
  host: runtimeIdentity,
  application: runtimeIdentity,
  active_control: { id: 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', version: '1.0.0', commit: runtimeIdentity.commit, installed_at: runtimeIdentity.build_time, state: 'active', active_recordings: 0 },
  default_engine: { id: 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb', version: '1.0.0', commit: runtimeIdentity.commit, installed_at: runtimeIdentity.build_time, state: 'active', active_recordings: 2 },
  active_generations: [{ id: 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb', version: '1.0.0', commit: runtimeIdentity.commit, installed_at: runtimeIdentity.build_time, state: 'active', active_recordings: 2 }],
  draining_generations: [],
  available_release: { version: '1.1.0', commit: '123456789abcdef0', build_time: '2026-09-29T12:00:00Z', release_channel: 'stable', notes_summary: '변경 사항 <img src=x onerror=alert(1)>' },
  previous_release: { version: '0.9.0', commit: '1234567abcdef012', build_time: '2026-09-20T12:00:00Z', release_channel: 'stable' },
  verification_state: 'not_checked', updates_available: true,
}

function settingsRouter(section: SettingsSection) {
  const root = createRootRoute({ component: Outlet })
  const paths = ['/test-settings', '/account/preferences', '/account/security', '/account/sessions', '/system', '/system/users', '/system/updates', '/system/diagnostics', '/system/audit', '/storage', '/storage/locations', '/storage/retention', '/storage/advanced'] as const
  const routes = paths.map(path => createRoute({ getParentRoute: () => root, path, component: path === '/test-settings' ? () => <SettingsPage section={section} /> : () => null }))
  return createRouter({ routeTree: root.addChildren(routes), history: createMemoryHistory({ initialEntries: ['/test-settings'] }) })
}
function renderSettings(section: SettingsSection = 'preferences') {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } })
  return render(<QueryClientProvider client={client}><ToastProvider><RouterProvider router={settingsRouter(section)} /></ToastProvider></QueryClientProvider>)
}
function renderLocalizedSettings(section: SettingsSection = 'preferences') {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } })
  return render(<QueryClientProvider client={client}><I18nProvider><ToastProvider><RouterProvider router={settingsRouter(section)} /></ToastProvider></I18nProvider></QueryClientProvider>)
}

beforeEach(() => {
  vi.stubGlobal('scrollTo', vi.fn())
  vi.spyOn(accountAPI, 'self').mockResolvedValue({ user_id: 'usr-00000000000000000000000000000001', login: 'owner', roles: ['owner'], role: 'owner', status: 'active', created_at: '2026-10-01T00:00:00Z' })
  vi.spyOn(accountAPI, 'authorization').mockResolvedValue({ user_id: 'usr-00000000000000000000000000000001', login: 'owner', roles: ['owner'], permissions: ['user.read', 'user.create', 'user.manage', 'role.read', 'role.manage', 'system.read', 'system.settings.manage'] })
  vi.spyOn(accountAPI, 'roles').mockResolvedValue({ roles: [{ id: 'owner', name: 'Owner', built_in: true, permissions: [] }, { id: 'operator', name: 'Operator', built_in: true, permissions: [] }, { id: 'viewer', name: 'Viewer', built_in: true, permissions: [] }], permissions: [] })
  vi.spyOn(accountAPI, 'sessions').mockResolvedValue({ items: [{ id: 'opaque-session-id', expires_at: '2026-10-12T00:00:00Z', current: true }] })
  vi.spyOn(accountAPI, 'users').mockResolvedValue({ items: [{ user_id: 'usr-00000000000000000000000000000001', login: 'owner', roles: ['owner'], role: 'owner', status: 'active', created_at: '2026-10-01T00:00:00Z' }] })
  vi.spyOn(accountAPI, 'changePassword').mockResolvedValue(undefined)
  vi.spyOn(accountAPI, 'revokeSession').mockResolvedValue(undefined)
  vi.spyOn(accountAPI, 'revokeOthers').mockResolvedValue({ revoked_count: 0 })
  vi.spyOn(accountAPI, 'createUser').mockResolvedValue({ user_id: 'usr-00000000000000000000000000000002', login: 'member', roles: ['operator'], role: 'operator', status: 'active', created_at: '2026-10-01T00:00:00Z' })
  vi.spyOn(accountAPI, 'setUserEnabled').mockResolvedValue({ user_id: 'usr-00000000000000000000000000000002', login: 'member', roles: ['operator'], role: 'operator', status: 'disabled', created_at: '2026-10-01T00:00:00Z' })
  vi.spyOn(productAPI, 'settings').mockResolvedValue(structuredClone(settingsDefaults))
  vi.spyOn(productAPI, 'saveSettings').mockResolvedValue(structuredClone(settingsDefaults))
  vi.spyOn(userPreferencesAPI, 'get').mockResolvedValue({ locale: 'system', theme: 'system', timezone: 'system' })
  vi.spyOn(userPreferencesAPI, 'update').mockImplementation(async preferences => preferences)
  vi.spyOn(productAPI, 'retentionCandidates').mockResolvedValue({ enabled: false, candidate_count: 0, candidates: [] })
  vi.spyOn(dashboardAPI, 'storage').mockResolvedValue(storageInfo)
  vi.spyOn(dashboardAPI, 'info').mockResolvedValue(systemInfo)
  vi.spyOn(runtimeUpdateAPI, 'status').mockResolvedValue(structuredClone(updateStatus))
  vi.spyOn(runtimeUpdateAPI, 'check').mockResolvedValue(structuredClone(updateStatus))
  vi.spyOn(runtimeUpdateAPI, 'stage').mockResolvedValue({ ...structuredClone(updateStatus), staged_release: updateStatus.available_release, verification_state: 'verified' })
  vi.spyOn(runtimeUpdateAPI, 'activate').mockResolvedValue({ ...structuredClone(updateStatus), application: { ...runtimeIdentity, version: '1.1.0', commit: '123456789abcdef0' }, staged_release: undefined, verification_state: 'verified' })
  vi.spyOn(runtimeUpdateAPI, 'rollback').mockResolvedValue({ ...structuredClone(updateStatus), application: runtimeIdentity, staged_release: undefined })
  vi.stubGlobal('matchMedia', vi.fn(() => ({ matches: false, addEventListener: vi.fn(), removeEventListener: vi.fn() })))
})
afterEach(() => { vi.restoreAllMocks(); vi.unstubAllGlobals(); document.documentElement.lang = 'ko-KR'; configureFormatPreferences('ko-KR', 'system') })

describe('storage ingest settings UI', () => {
  it('loads MiB/seconds values, shows effective values separately, and sends bytes/milliseconds to the API', async () => {
    const saved = structuredClone(settingsDefaults)
    saved.settings.storage.observability.sampling_interval_ms = 10_000
    saved.effective_storage.observability.sampling_interval_ms = 5_000
    saved.restart_required = ['storage.observability.sampling_interval_ms']
    vi.mocked(productAPI.saveSettings).mockResolvedValue(saved)
    renderSettings('advanced-storage')

    expect(await screen.findByRole('heading', { name: '고급 수집·저장 설정' })).toBeInTheDocument()
    await waitFor(() => expect(accountAPI.self).toHaveBeenCalled())
    await waitFor(() => expect(productAPI.settings).toHaveBeenCalled())
    expect(await screen.findByLabelText('전체 버퍼 한도')).toHaveValue(1024)
    expect(screen.getByLabelText('녹화별 버퍼 한도')).toHaveValue(768)
    expect(screen.getByLabelText('단일 페이로드 최대 크기')).toHaveValue(512)
    expect(screen.getByLabelText('측정 간격')).toHaveValue(5)
    const retention = screen.getByLabelText('측정 기록 보존 기간')
    expect(retention).toHaveValue(24)
    expect(Number(retention.getAttribute('min'))).toBeCloseTo(5 / 60, 6)
    expect(screen.getByText('관측 상한 계산에는 최소 5분의 비유휴 기록과 방향별 20개 샘플이 필요합니다.')).toBeInTheDocument()
    expect(screen.getByLabelText('총 저장 시도 횟수')).toHaveValue(5)
    expect(screen.getByText('최초 저장 시도를 포함한 최대 시도 횟수입니다.')).toBeInTheDocument()
    expect(screen.getByLabelText('저장 작업 동시 실행 수')).toBeDisabled()
    expect(screen.getByText(/높은 동시성이 항상 빠른 것은 아니며/)).toBeInTheDocument()
    expect(within(screen.getByTestId('effective-storage-settings')).getByText('5초')).toBeInTheDocument()

    fireEvent.change(screen.getByLabelText('측정 간격'), { target: { value: '10' } })
    fireEvent.click(screen.getByRole('button', { name: '수집·저장 설정 저장' }))
    await waitFor(() => expect(productAPI.saveSettings).toHaveBeenCalledWith({ storage: {
      ...storageDefaults,
      observability: { sampling_interval_ms: 10_000, metrics_retention_ms: 86_400_000 },
    } }))
    expect(await screen.findByText(/서버를 재시작해야 적용됩니다/)).toBeInTheDocument()
    expect(within(screen.getByTestId('effective-storage-settings')).getByText('5초')).toBeInTheDocument()
    expect(screen.getByLabelText('측정 간격')).toHaveValue(10)

    fireEvent.change(screen.getByLabelText('측정 간격'), { target: { value: '60' } })
    expect(Number(retention.getAttribute('min'))).toBeCloseTo(1 / 3, 6)

    fireEvent.change(screen.getByLabelText('측정 간격'), { target: { value: '7' } })
    expect(Number(retention.getAttribute('min'))).toBeCloseTo(301_000 / 3_600_000, 6)
  })

  it('shows an authoritative server validation error for invalid related limits', async () => {
    vi.mocked(productAPI.saveSettings).mockRejectedValue(new APIError(400, 'untrusted backend detail', undefined, 'invalid_argument'))
    renderSettings('advanced-storage')
    await screen.findByRole('heading', { name: '고급 수집·저장 설정' })
    await waitFor(() => expect(productAPI.settings).toHaveBeenCalled())
    fireEvent.change(await screen.findByLabelText('녹화별 버퍼 한도'), { target: { value: '1100' } })
    fireEvent.click(screen.getByRole('button', { name: '수집·저장 설정 저장' }))
    expect(await screen.findByRole('alert')).toHaveTextContent('입력 값을 확인한 뒤 다시 시도하세요.')
    expect(screen.getByRole('alert')).not.toHaveTextContent('untrusted backend detail')
  })
})

describe('runtime update settings UI', () => {
  it('shows a cleaned previous release as rollback-ready without a draining label', async () => {
    vi.mocked(runtimeUpdateAPI.status).mockResolvedValue({
      ...structuredClone(updateStatus),
      active_generations: [structuredClone(updateStatus.default_engine!)],
      draining_generations: [],
      previous_release: { version: 'vPrev', commit: '123456789abcdef0123456789abcdef012345678', build_time: '2026-09-20T12:00:00Z', release_channel: 'stable' },
    })
    renderSettings('updates')

    expect(await screen.findByText('vPrev')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: '이전 버전으로 롤백' })).toBeEnabled()
    fireEvent.click(screen.getByText('진단 정보', { selector: 'summary' }))
    expect(screen.queryByText('정리 중')).not.toBeInTheDocument()
  })

  it('continues to show a real draining generation as cleaning up', async () => {
    vi.mocked(runtimeUpdateAPI.status).mockResolvedValue({
      ...structuredClone(updateStatus),
      draining_generations: [{ id: 'cccccccc-cccc-cccc-cccc-cccccccccccc', version: 'vDraining', commit: '123456789abcdef0123456789abcdef012345678', installed_at: '2026-09-20T12:00:00Z', state: 'draining', active_recordings: 0 }],
    })
    renderSettings('updates')

    expect(await screen.findByText('vDraining')).toBeInTheDocument()
    fireEvent.click(screen.getByText('진단 정보', { selector: 'summary' }))
    expect(screen.getAllByText('정리 중').length).toBeGreaterThan(0)
  })

  it('renders only stable, sanitized handover diagnostics', async () => {
    const diagnostic = {
      recording_id: 'recording-123', phase: 'prepare_target', reason_code: 'target_start_timeout',
      source_generation_id: 'generation-old', target_generation_id: 'generation-new', target_version: '1.2.3',
      occurred_at: '2026-10-10T01:02:03Z', reconcile_state: 'pending' as const, retryable: true, recoverable: true,
      ownership_retained: true, raw_error: 'token=super-secret private-path=/srv/archive',
    } as NonNullable<RuntimeUpdateStatus['handover_diagnostic']>
    vi.mocked(runtimeUpdateAPI.status).mockResolvedValue({ ...structuredClone(updateStatus), handover_diagnostic: diagnostic })
    renderSettings('updates')
    expect(await screen.findByRole('heading', { name: '최근 세대 전환 진단' })).toBeInTheDocument()
    expect(screen.getByText(/가장 최근 진단 하나만 보관합니다/)).toBeInTheDocument()
    expect(screen.getByText('대상 시작 시간 초과 · target_start_timeout')).toBeInTheDocument()
    expect(screen.getByText('generation-old')).toBeInTheDocument()
    expect(screen.getByText('generation-new')).toBeInTheDocument()
    expect(screen.queryByText(/super-secret|private-path/)).not.toBeInTheDocument()
  })

  it('loads release state, runs update actions and refreshes the displayed status', async () => {
    const { container } = renderSettings('updates')
    expect(await screen.findByRole('heading', { name: '애플리케이션 업데이트' })).toBeInTheDocument()
    await waitFor(() => expect(runtimeUpdateAPI.status).toHaveBeenCalledTimes(1))
    expect(screen.getAllByText('1.0.0').length).toBeGreaterThan(0)
    expect(screen.getByText('1.1.0')).toBeInTheDocument()
    expect(screen.getByText('2개 녹화')).toBeInTheDocument()
    expect(screen.getByText('변경 사항 <img src=x onerror=alert(1)>')).toBeInTheDocument()
    expect(container.querySelector('img[src="x"]')).toBeNull()

    fireEvent.click(screen.getByRole('button', { name: '업데이트 확인' }))
    await waitFor(() => expect(runtimeUpdateAPI.check).toHaveBeenCalledTimes(1))
    expect(screen.queryByText('현재 최신 버전입니다.')).not.toBeInTheDocument()
    expect(screen.getByText('1.1.0')).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: '다운로드 및 검증' }))
    expect(await screen.findByText('검증 상태 · 검증 완료')).toBeInTheDocument()
    await waitFor(() => expect(runtimeUpdateAPI.stage).toHaveBeenCalledTimes(1))

    const activate = screen.getByRole('button', { name: '활성화' })
    expect(activate).toBeEnabled()
    fireEvent.click(activate)
    await waitFor(() => expect(runtimeUpdateAPI.activate).toHaveBeenCalledTimes(1))
    expect(screen.getAllByText('1.1.0').length).toBeGreaterThan(0)

    fireEvent.click(screen.getByRole('button', { name: '이전 버전으로 롤백' }))
    await waitFor(() => expect(runtimeUpdateAPI.rollback).toHaveBeenCalledTimes(1))
  })

  it('shows an informational status when a successful check finds no newer release', async () => {
    vi.mocked(runtimeUpdateAPI.check).mockResolvedValue({
      ...structuredClone(updateStatus), available_release: undefined, updates_available: false,
      last_failure_code: undefined, verification_state: 'verified',
    })
    renderSettings('updates')
    await screen.findByText('1.1.0')

    fireEvent.click(screen.getByRole('button', { name: '업데이트 확인' }))

    expect(await screen.findByText('현재 최신 버전입니다.')).toBeInTheDocument()
    expect(screen.getByRole('status')).toHaveTextContent('현재 최신 버전입니다.')
    expect(screen.queryByRole('alert')).not.toBeInTheDocument()
    expect(screen.queryByText('요청한 항목을 찾을 수 없습니다.')).not.toBeInTheDocument()
    expect(screen.queryByText('새 버전을 확인하지 않았습니다.')).not.toBeInTheDocument()
    expect(screen.getByText('새 버전 없음')).toBeInTheDocument()
  })

  it('shows a release candidate returned by a successful check', async () => {
    vi.mocked(runtimeUpdateAPI.status).mockResolvedValue({
      ...structuredClone(updateStatus), available_release: undefined, updates_available: false,
      verification_state: 'not_checked',
    })
    vi.mocked(runtimeUpdateAPI.check).mockResolvedValue(structuredClone(updateStatus))
    renderSettings('updates')
    expect(await screen.findByText('새 버전을 확인하지 않았습니다.')).toBeInTheDocument()

    fireEvent.click(screen.getByRole('button', { name: '업데이트 확인' }))

    expect(await screen.findByText('1.1.0')).toBeInTheDocument()
    expect(screen.queryByText('현재 최신 버전입니다.')).not.toBeInTheDocument()
    expect(screen.queryByRole('alert')).not.toBeInTheDocument()
  })

  it('clears the up-to-date result when a later check discovers an update', async () => {
    const current = {
      ...structuredClone(updateStatus), available_release: undefined, updates_available: false,
      last_failure_code: undefined, verification_state: 'verified' as const,
    }
    vi.mocked(runtimeUpdateAPI.check)
      .mockResolvedValueOnce(current)
      .mockResolvedValueOnce(structuredClone(updateStatus))
    renderSettings('updates')
    await screen.findByText('1.1.0')
    const check = screen.getByRole('button', { name: '업데이트 확인' })

    fireEvent.click(check)
    expect(await screen.findByText('현재 최신 버전입니다.')).toBeInTheDocument()
    fireEvent.click(check)

    await waitFor(() => expect(runtimeUpdateAPI.check).toHaveBeenCalledTimes(2))
    await waitFor(() => expect(screen.queryByText('현재 최신 버전입니다.')).not.toBeInTheDocument())
    expect(screen.getByText('1.1.0')).toBeInTheDocument()
  })

  it('shows the up-to-date result in English when en-US is selected', async () => {
    vi.spyOn(authAPI, 'session').mockResolvedValue({ auth_enabled: true, authenticated: true, needs_bootstrap: false })
    vi.mocked(userPreferencesAPI.get).mockResolvedValue({ locale: 'en-US', theme: 'system', timezone: 'system' })
    vi.mocked(runtimeUpdateAPI.check).mockResolvedValue({
      ...structuredClone(updateStatus), available_release: undefined, updates_available: false,
      last_failure_code: undefined, verification_state: 'verified',
    })
    renderLocalizedSettings('updates')
    const check = await screen.findByRole('button', { name: 'Check for updates' })
    fireEvent.click(check)

    expect(await screen.findByText('You’re up to date.')).toBeInTheDocument()
    expect(screen.getByText('No newer release.')).toBeInTheDocument()
  })

  it('shows update action errors', async () => {
    vi.mocked(runtimeUpdateAPI.check).mockRejectedValue(new APIError(502, 'private backend details'))
    renderSettings('updates')
    await screen.findByText('1.1.0')
    fireEvent.click(screen.getByRole('button', { name: '업데이트 확인' }))
    expect(await screen.findByRole('alert')).toHaveTextContent('서버가 요청을 처리하지 못했습니다.')
    expect(screen.getByRole('alert')).not.toHaveTextContent('private backend details')
  })

  it('shows unavailable reason and disables update operations for a development build', async () => {
    vi.mocked(runtimeUpdateAPI.status).mockResolvedValue({ ...structuredClone(updateStatus), application: { ...runtimeIdentity, version: 'dev', commit: 'unknown', build_time: 'unknown', release_channel: 'development' }, update_unavailable_reason: 'development_build' })
    renderSettings('updates')
    expect(await screen.findByText('개발 빌드에서는 애플리케이션 업데이트를 사용할 수 없습니다.')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: '업데이트 확인' })).toBeDisabled()
    expect(screen.getByRole('button', { name: '다운로드 및 검증' })).toBeDisabled()
    expect(screen.getByRole('button', { name: '활성화' })).toBeDisabled()
    expect(screen.getByRole('button', { name: '이전 버전으로 롤백' })).toBeDisabled()
  })
})

describe('per-user preferences', () => {
  it('switches settings UI and save toast to the saved locale', async () => {
    vi.spyOn(authAPI, 'session').mockResolvedValue({ auth_enabled: true, authenticated: true, needs_bootstrap: false })
    vi.mocked(userPreferencesAPI.get).mockResolvedValue({ locale: 'en-US', theme: 'system', timezone: 'system' })
    renderLocalizedSettings()
    expect(await screen.findByRole('combobox', { name: 'Language' })).toBeInTheDocument()
    const locale = await screen.findByRole('combobox', { name: 'Language' })
    fireEvent.change(locale, { target: { value: 'ko-KR' } })
    fireEvent.click(screen.getByRole('button', { name: 'Save preferences' }))
    expect(await screen.findByText('환경 설정을 저장했습니다.')).toBeInTheDocument()
    await waitFor(() => expect(document.documentElement.lang).toBe('ko-KR'))
    expect(screen.getByRole('heading', { name: '환경설정' })).toBeInTheDocument()
  })

  it('applies en-US to document lang immediately after saving from ko-KR', async () => {
    vi.spyOn(authAPI, 'session').mockResolvedValue({ auth_enabled: true, authenticated: true, needs_bootstrap: false })
    vi.mocked(userPreferencesAPI.get).mockResolvedValue({ locale: 'ko-KR', theme: 'system', timezone: 'system' })
    vi.mocked(userPreferencesAPI.update).mockImplementation(async preferences => preferences)
    renderLocalizedSettings()
    expect(await screen.findByRole('combobox', { name: '언어' })).toBeInTheDocument()
    const locale = await screen.findByRole('combobox', { name: '언어' })
    fireEvent.click(screen.getByRole('button', { name: '환경 설정 저장' }))
    await waitFor(() => expect(userPreferencesAPI.update).toHaveBeenCalledTimes(1))
    await waitFor(() => expect(screen.getByRole('button', { name: '환경 설정 저장' })).toBeEnabled())
    fireEvent.change(locale, { target: { value: 'en-US' } })
    fireEvent.click(screen.getByRole('button', { name: '환경 설정 저장' }))
    await waitFor(() => expect(userPreferencesAPI.update).toHaveBeenLastCalledWith({ locale: 'en-US', theme: 'system', timezone: 'system' }, expect.objectContaining({ client: expect.anything() })))
    await waitFor(() => expect(document.documentElement.lang).toBe('en-US'))
    expect(screen.getByRole('heading', { name: 'Preferences' })).toBeInTheDocument()
  })

  it('loads and persists locale, theme, and time zone together', async () => {
    const saved: UserPreferences = { locale: 'ko-KR', theme: 'dark', timezone: 'Asia/Seoul' }
    vi.mocked(userPreferencesAPI.update).mockResolvedValue(saved)
    renderSettings()
    expect(await screen.findByRole('combobox', { name: '언어' })).toBeInTheDocument()
    fireEvent.change(await screen.findByRole('combobox', { name: '언어' }), { target: { value: 'ko-KR' } })
    fireEvent.change(await screen.findByRole('combobox', { name: '테마' }), { target: { value: 'dark' } })
    fireEvent.change(await screen.findByLabelText('시간대'), { target: { value: 'Asia/Seoul' } })
    fireEvent.click(screen.getByRole('button', { name: '환경 설정 저장' }))
    await waitFor(() => expect(userPreferencesAPI.update).toHaveBeenCalledWith(saved, expect.objectContaining({ client: expect.anything() })))
    expect(await screen.findByText('환경 설정을 저장했습니다.')).toBeInTheDocument()
    expect(productAPI.saveSettings).not.toHaveBeenCalledWith(expect.objectContaining({ ui: expect.anything() }))
  })

  it('rejects invalid IANA time zones before sending the update', async () => {
    renderSettings()
    await screen.findByRole('combobox', { name: '언어' })
    fireEvent.change(await screen.findByLabelText('시간대'), { target: { value: 'Mars/Olympus' } })
    fireEvent.click(screen.getByRole('button', { name: '환경 설정 저장' }))
    expect(await screen.findByRole('alert')).toHaveTextContent('환경 설정 값을 확인하세요.')
    expect(userPreferencesAPI.update).not.toHaveBeenCalled()
  })
})

describe('account and security settings', () => {
  it('shows account identity and local recovery guidance without user administration', async () => {
    renderSettings('security')
    await waitFor(() => expect(accountAPI.self).toHaveBeenCalled())
    expect(await screen.findByRole('heading', { name: '계정 정보' })).toBeInTheDocument()
    expect(await screen.findByText('owner')).toBeInTheDocument()
    expect(screen.getByRole('link', { name: '활성 세션 →' })).toHaveAttribute('href', '/account/sessions')
    expect(screen.queryByRole('heading', { name: '사용자 관리' })).not.toBeInTheDocument()
    expect(screen.getByText(/runtime-host reset-password/)).toBeInTheDocument()
  })

  it('forces the browser back to login after a successful password change', async () => {
    const unauthorized = vi.fn()
    window.addEventListener('ir:unauthorized', unauthorized)
    renderSettings('security')
    const currentPassword = (await screen.findAllByLabelText('현재 비밀번호')).at(-1)!
    const newPassword = screen.getByLabelText('새 비밀번호')
    const confirmation = screen.getAllByLabelText('새 비밀번호 확인')[0]!
    expect(currentPassword).toHaveAttribute('name', 'current_password')
    expect(currentPassword).toHaveAttribute('autocomplete', 'current-password')
    expect(newPassword).toHaveAttribute('name', 'new_password')
    expect(newPassword).toHaveAttribute('autocomplete', 'new-password')
    expect(confirmation).toHaveAttribute('name', 'password_confirmation')
    expect(confirmation).toHaveAttribute('autocomplete', 'new-password')
    const setNativeValue = (input: HTMLInputElement, value: string) => {
      const setter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')?.set
      if (!setter) throw new Error('native input value setter is unavailable')
      setter.call(input, value)
    }
    setNativeValue(currentPassword as HTMLInputElement, 'old-password-123')
    setNativeValue(newPassword as HTMLInputElement, 'new-password-123')
    setNativeValue(confirmation as HTMLInputElement, 'new-password-123')
    fireEvent.submit(currentPassword.closest('form')!)
    await waitFor(() => expect(accountAPI.changePassword).toHaveBeenCalledWith({ current_password: 'old-password-123', new_password: 'new-password-123' }))
    await waitFor(() => expect(unauthorized).toHaveBeenCalledTimes(1))
    window.removeEventListener('ir:unauthorized', unauthorized)
  })

  it('keeps owner user administration on its dedicated route', async () => {
    renderSettings('users')
    expect(await screen.findByRole('heading', { name: '사용자 관리' })).toBeInTheDocument()
    expect(await screen.findByRole('button', { name: '사용자 생성' })).toBeInTheDocument()
  })

  it('hides owner user management from a member account', async () => {
    vi.mocked(accountAPI.self).mockResolvedValue({ user_id: 'usr-00000000000000000000000000000002', login: 'member', roles: ['operator'], role: 'operator', status: 'active', created_at: '2026-10-01T00:00:00Z' })
    vi.mocked(accountAPI.authorization).mockResolvedValue({ user_id: 'usr-00000000000000000000000000000002', login: 'member', roles: ['operator'], permissions: ['recording.read', 'recording.create'] })
    renderSettings('security')
    expect(await screen.findByRole('heading', { name: '계정 정보' })).toBeInTheDocument()
    expect(screen.queryByRole('heading', { name: '사용자 관리' })).not.toBeInTheDocument()
    expect(accountAPI.users).not.toHaveBeenCalled()
  })
})
