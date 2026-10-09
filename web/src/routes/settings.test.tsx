import { fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { authAPI, dashboardAPI, productAPI, runtimeUpdateAPI, userPreferencesAPI } from '@/api'
import { ToastProvider } from '@/components/ui/toast'
import { APIError } from '@/api/client'
import type { RuntimeUpdateStatus, StorageSettings, SystemSettings } from '@/types/api'
import type { UserPreferences } from '@/types/api'
import { SettingsPage } from './settings'
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

function renderSettings() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } })
  return render(<QueryClientProvider client={client}><ToastProvider><SettingsPage /></ToastProvider></QueryClientProvider>)
}
function renderLocalizedSettings() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } })
  return render(<QueryClientProvider client={client}><I18nProvider><ToastProvider><SettingsPage /></ToastProvider></I18nProvider></QueryClientProvider>)
}
function openStorageTab() { fireEvent.click(screen.getByRole('tab', { name: '저장소' })) }

beforeEach(() => {
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
afterEach(() => { vi.restoreAllMocks(); vi.unstubAllGlobals() })

describe('storage ingest settings UI', () => {
  it('loads MiB/seconds values, shows effective values separately, and sends bytes/milliseconds to the API', async () => {
    const saved = structuredClone(settingsDefaults)
    saved.settings.storage.observability.sampling_interval_ms = 10_000
    saved.effective_storage.observability.sampling_interval_ms = 5_000
    saved.restart_required = ['storage.observability.sampling_interval_ms']
    vi.mocked(productAPI.saveSettings).mockResolvedValue(saved)
    renderSettings()
    openStorageTab()

    expect(await screen.findByRole('heading', { name: '고급 수집·저장 설정' })).toBeInTheDocument()
    expect(screen.getByLabelText('전체 버퍼 한도')).toHaveValue(1024)
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
    renderSettings()
    openStorageTab()
    await screen.findByRole('heading', { name: '고급 수집·저장 설정' })
    fireEvent.change(screen.getByLabelText('녹화별 버퍼 한도'), { target: { value: '1100' } })
    fireEvent.click(screen.getByRole('button', { name: '수집·저장 설정 저장' }))
    expect(await screen.findByRole('alert')).toHaveTextContent('입력 값을 확인한 뒤 다시 시도하세요.')
    expect(screen.getByRole('alert')).not.toHaveTextContent('untrusted backend detail')
  })
})

describe('runtime update settings UI', () => {
  function openUpdatesTab() { fireEvent.click(screen.getByRole('tab', { name: '업데이트' })) }

  it('renders only stable, sanitized handover diagnostics', async () => {
    const diagnostic = {
      recording_id: 'recording-123', phase: 'prepare_target', reason_code: 'target_start_timeout',
      source_generation_id: 'generation-old', target_generation_id: 'generation-new', target_version: '1.2.3',
      occurred_at: '2026-10-10T01:02:03Z', reconcile_state: 'pending' as const, retryable: true, recoverable: true,
      ownership_retained: true, raw_error: 'token=super-secret private-path=/srv/archive',
    } as NonNullable<RuntimeUpdateStatus['handover_diagnostic']>
    vi.mocked(runtimeUpdateAPI.status).mockResolvedValue({ ...structuredClone(updateStatus), handover_diagnostic: diagnostic })
    renderSettings()
    openUpdatesTab()
    expect(await screen.findByRole('heading', { name: '최근 세대 전환 진단' })).toBeInTheDocument()
    expect(screen.getByText('대상 시작 시간 초과 · target_start_timeout')).toBeInTheDocument()
    expect(screen.getByText('generation-old')).toBeInTheDocument()
    expect(screen.getByText('generation-new')).toBeInTheDocument()
    expect(screen.queryByText(/super-secret|private-path/)).not.toBeInTheDocument()
  })

  it('loads release state, runs update actions and refreshes the displayed status', async () => {
    const { container } = renderSettings()
    openUpdatesTab()
    expect(await screen.findByRole('heading', { name: '애플리케이션 업데이트' })).toBeInTheDocument()
    await waitFor(() => expect(runtimeUpdateAPI.status).toHaveBeenCalledTimes(1))
    expect(screen.getAllByText('1.0.0').length).toBeGreaterThan(0)
    expect(screen.getByText('1.1.0')).toBeInTheDocument()
    expect(screen.getByText('2개 녹화')).toBeInTheDocument()
    expect(screen.getByText('변경 사항 <img src=x onerror=alert(1)>')).toBeInTheDocument()
    expect(container.querySelector('img[src="x"]')).toBeNull()

    fireEvent.click(screen.getByRole('button', { name: '업데이트 확인' }))
    await waitFor(() => expect(runtimeUpdateAPI.check).toHaveBeenCalledTimes(1))
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

  it('shows update action errors', async () => {
    vi.mocked(runtimeUpdateAPI.check).mockRejectedValue(new APIError(502, 'private backend details'))
    renderSettings()
    openUpdatesTab()
    await screen.findByText('1.1.0')
    fireEvent.click(screen.getByRole('button', { name: '업데이트 확인' }))
    expect(await screen.findByRole('alert')).toHaveTextContent('서버가 요청을 처리하지 못했습니다.')
    expect(screen.getByRole('alert')).not.toHaveTextContent('private backend details')
  })

  it('shows unavailable reason and disables update operations for a development build', async () => {
    vi.mocked(runtimeUpdateAPI.status).mockResolvedValue({ ...structuredClone(updateStatus), application: { ...runtimeIdentity, version: 'dev', commit: 'unknown', build_time: 'unknown', release_channel: 'development' }, update_unavailable_reason: 'development_build' })
    renderSettings()
    openUpdatesTab()
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
    expect(await screen.findByRole('heading', { name: 'User preferences' })).toBeInTheDocument()
    const locale = await screen.findByRole('combobox', { name: 'Language' })
    fireEvent.change(locale, { target: { value: 'ko-KR' } })
    fireEvent.click(screen.getByRole('button', { name: 'Save preferences' }))
    expect(await screen.findByText('환경 설정을 저장했습니다.')).toBeInTheDocument()
    await waitFor(() => expect(document.documentElement.lang).toBe('ko-KR'))
    expect(screen.getByRole('heading', { name: '사용자 환경' })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: '환경 설정 저장' })).toBeInTheDocument()
  })

  it('loads and persists locale, theme, and time zone together', async () => {
    const saved: UserPreferences = { locale: 'ko-KR', theme: 'dark', timezone: 'Asia/Seoul' }
    vi.mocked(userPreferencesAPI.update).mockResolvedValue(saved)
    renderSettings()
    expect(await screen.findByRole('heading', { name: '사용자 환경' })).toBeInTheDocument()
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
    await screen.findByRole('heading', { name: '사용자 환경' })
    fireEvent.change(await screen.findByLabelText('시간대'), { target: { value: 'Mars/Olympus' } })
    fireEvent.click(screen.getByRole('button', { name: '환경 설정 저장' }))
    expect(await screen.findByRole('alert')).toHaveTextContent('환경 설정 값을 확인하세요.')
    expect(userPreferencesAPI.update).not.toHaveBeenCalled()
  })
})
