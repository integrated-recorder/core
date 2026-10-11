import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import * as ts from 'typescript'
import { I18nProvider } from '@/i18n/provider'
import { SetupPage } from './setup'

const mocks = vi.hoisted(() => ({
  status: vi.fn(), session: vi.fn(), bootstrap: vi.fn(), login: vi.fn(), begin: vi.fn(), storageTest: vi.fn(), complete: vi.fn(),
  info: vi.fn(), adapters: vi.fn(), navigate: vi.fn(),
}))
vi.mock('@/api', async importOriginal => {
  const actual = await importOriginal<typeof import('@/api')>()
  return {
    ...actual,
    authAPI: { ...actual.authAPI, session: mocks.session, bootstrap: mocks.bootstrap, login: mocks.login },
    setupAPI: { status: mocks.status, begin: mocks.begin, storageTest: mocks.storageTest, complete: mocks.complete },
    dashboardAPI: { ...actual.dashboardAPI, info: mocks.info },
    adaptersAPI: { ...actual.adaptersAPI, list: mocks.adapters },
  }
})
vi.mock('@tanstack/react-router', async importOriginal => ({ ...await importOriginal<typeof import('@tanstack/react-router')>(), useNavigate: () => mocks.navigate }))

const status = (state: 'uninitialized' | 'setup_in_progress' | 'ready' | 'recovery_required', overrides: Record<string, unknown> = {}) => ({
  state, administrator_configured: state !== 'uninitialized', claim_required: state === 'uninitialized', recovery_required: state === 'recovery_required',
  auth_disabled: false, version: '1.2.3', release_channel: 'stable', ...overrides,
})
const session = (authenticated: boolean) => ({ auth_enabled: true, authenticated, needs_bootstrap: !authenticated, csrf_token: authenticated ? 'session-csrf' : 'initial-csrf' })
const adapterReady = [{ status: { id: 'owncast', name: 'Owncast', state: 'running' }, descriptor: { id: 'owncast', name: 'Owncast' } }]
const storagePassed = { status: 'ready' as const, capacity_known: true, free_bytes: 4_000_000_000, write_test: 'passed' as const, durability_test: 'passed' as const }
const storageCapacityUnknown = { status: 'warning' as const, capacity_known: false, write_test: 'passed' as const, durability_test: 'passed' as const }

function renderSetup() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } })
  return render(<QueryClientProvider client={client}><I18nProvider><SetupPage /></I18nProvider></QueryClientProvider>)
}

function setBrowserLocale(locale: string) {
  Object.defineProperty(navigator, 'languages', { configurable: true, value: [locale] })
  Object.defineProperty(navigator, 'language', { configurable: true, value: locale })
}

beforeEach(() => {
  setBrowserLocale('ko-KR')
  vi.clearAllMocks()
  mocks.status.mockResolvedValue(status('uninitialized'))
  mocks.session.mockResolvedValue(session(false))
  mocks.bootstrap.mockResolvedValue(session(true))
  mocks.login.mockResolvedValue(session(true))
  mocks.begin.mockResolvedValue(status('setup_in_progress'))
  mocks.storageTest.mockResolvedValue(storagePassed)
  mocks.complete.mockResolvedValue(status('ready'))
  mocks.info.mockResolvedValue({ version: '1.2.3' })
  mocks.adapters.mockResolvedValue(adapterReady)
})

describe('first-run setup flow', () => {
  it('resolves unauthenticated setup to en-US from browser locale', async () => {
    setBrowserLocale('en-US')
    renderSetup()
    expect(await screen.findByRole('heading', { name: 'Get started with Integrated Recorder' })).toBeInTheDocument()
    expect(screen.getByText('Complete a few setup steps to get started.')).toBeInTheDocument()
    await waitFor(() => expect(document.documentElement.lang).toBe('en-US'))
    expect(document.body.textContent).not.toMatch(/[\uac00-\ud7a3]/)
  })

  it('resolves unauthenticated setup to ko-KR from browser locale', async () => {
    renderSetup()
    expect(await screen.findByRole('heading', { name: 'Integrated Recorder 시작하기' })).toBeInTheDocument()
    expect(screen.getByText('처음 몇 가지만 설정하면 바로 시작할 수 있습니다.')).toBeInTheDocument()
    await waitFor(() => expect(document.documentElement.lang).toBe('ko-KR'))
  })

  it('keeps setup UI text in translation keys', () => {
    const source = readFileSync(resolve(process.cwd(), 'src/routes/setup.tsx'), 'utf8')
    const file = ts.createSourceFile('setup.tsx', source, ts.ScriptTarget.Latest, true, ts.ScriptKind.TSX)
    const visibleLiterals: string[] = []
    const visit = (node: ts.Node) => {
      if (ts.isJsxText(node) && /[\p{L}\p{N}]/u.test(node.getText(file))) visibleLiterals.push(node.getText(file).trim())
      ts.forEachChild(node, visit)
    }
    visit(file)
    expect(visibleLiterals).toEqual([])
  })

  it('submits the one-time code, creates the administrator session, then begins setup', async () => {
    renderSetup()
    fireEvent.click(await screen.findByRole('button', { name: '시작하기' }))
    expect(await screen.findByText(/로컬 콘솔과 container logs/)).toBeInTheDocument()
    expect(screen.getByText('docker compose logs archiver')).toBeInTheDocument()
    fireEvent.change(screen.getByLabelText('설치 코드'), { target: { value: 'test-only-code' } })
    fireEvent.change(screen.getByLabelText('관리자 비밀번호'), { target: { value: 'a-strong-passphrase' } })
    fireEvent.change(screen.getByLabelText('비밀번호 확인'), { target: { value: 'a-strong-passphrase' } })
    fireEvent.click(screen.getByRole('button', { name: '관리자 계정 만들기' }))
    await waitFor(() => expect(mocks.bootstrap).toHaveBeenCalledWith('test-only-code', 'a-strong-passphrase'))
    await waitFor(() => expect(mocks.begin).toHaveBeenCalledTimes(1))
    expect(await screen.findByRole('heading', { name: '저장소 확인' })).toBeInTheDocument()
    expect(screen.queryByDisplayValue('test-only-code')).not.toBeInTheDocument()
  })

  it('rejects password mismatch before submitting the bootstrap code', async () => {
    renderSetup()
    fireEvent.click(await screen.findByRole('button', { name: '시작하기' }))
    fireEvent.change(screen.getByLabelText('설치 코드'), { target: { value: 'test-only-code' } })
    fireEvent.change(screen.getByLabelText('관리자 비밀번호'), { target: { value: 'a-strong-passphrase' } })
    fireEvent.change(screen.getByLabelText('비밀번호 확인'), { target: { value: 'different-passphrase' } })
    fireEvent.click(screen.getByRole('button', { name: '관리자 계정 만들기' }))
    expect(await screen.findByRole('alert')).toHaveTextContent('비밀번호 확인이 일치하지 않습니다.')
    expect(mocks.bootstrap).not.toHaveBeenCalled()
  })

  it('shows a bootstrap error without leaving setup or exposing a filesystem path', async () => {
    mocks.bootstrap.mockRejectedValueOnce(new Error('Setup code was rejected'))
    renderSetup()
    fireEvent.click(await screen.findByRole('button', { name: '시작하기' }))
    fireEvent.change(screen.getByLabelText('설치 코드'), { target: { value: 'bad-code' } })
    fireEvent.change(screen.getByLabelText('관리자 비밀번호'), { target: { value: 'a-strong-passphrase' } })
    fireEvent.change(screen.getByLabelText('비밀번호 확인'), { target: { value: 'a-strong-passphrase' } })
    fireEvent.click(screen.getByRole('button', { name: '관리자 계정 만들기' }))
    expect(await screen.findByRole('alert')).toHaveTextContent('설치 코드 또는 서버 연결을 확인한 뒤 다시 시도하세요.')
    expect(screen.queryByText('Setup code was rejected')).not.toBeInTheDocument()
    expect(document.body.textContent).not.toContain('/data/')
    expect(mocks.begin).not.toHaveBeenCalled()
  })

  it('resumes an authenticated setup_in_progress installation at storage diagnostics', async () => {
    mocks.status.mockResolvedValue(status('setup_in_progress'))
    mocks.session.mockResolvedValue(session(true))
    renderSetup()
    expect(await screen.findByRole('heading', { name: '저장소 확인' })).toBeInTheDocument()
    expect(screen.queryByLabelText('설치 코드')).not.toBeInTheDocument()
    expect(mocks.bootstrap).not.toHaveBeenCalled()
  })

  it('lets the user retry a failed storage test', async () => {
    mocks.status.mockResolvedValue(status('setup_in_progress'))
    mocks.session.mockResolvedValue(session(true))
    mocks.storageTest.mockRejectedValueOnce(new Error('storage probe unavailable')).mockResolvedValueOnce(storagePassed)
    renderSetup()
    fireEvent.click(await screen.findByRole('button', { name: '저장소 검사 실행' }))
    expect(await screen.findByRole('alert')).toHaveTextContent('저장소 검사를 완료하지 못했습니다. 저장소 설정을 확인한 뒤 다시 시도하세요.')
    fireEvent.click(screen.getByRole('button', { name: '저장소 검사 실행' }))
    expect(await screen.findByText('기본 저장소 진단')).toBeInTheDocument()
    expect(screen.getAllByText('정상').length).toBeGreaterThan(0)
  })

  it('shows unknown provider capacity as a warning and still permits setup completion', async () => {
    mocks.status.mockResolvedValue(status('setup_in_progress'))
    mocks.session.mockResolvedValue(session(true))
    mocks.storageTest.mockResolvedValue(storageCapacityUnknown)
    renderSetup()
    fireEvent.click(await screen.findByRole('button', { name: '저장소 검사 실행' }))
    await waitFor(() => expect(mocks.storageTest).toHaveBeenCalledTimes(1))
    expect(await screen.findByText(/저장소가 사용 가능 용량을 제공하지 않아 용량은 알 수 없습니다/)).toBeInTheDocument()
    expect(screen.getAllByText('용량 알 수 없음').length).toBeGreaterThan(0)
    fireEvent.click(screen.getByRole('button', { name: '계속' }))
    fireEvent.click(await screen.findByRole('button', { name: '계속' }))
    fireEvent.click(await screen.findByRole('button', { name: '설치 완료' }))
    await waitFor(() => expect(mocks.complete).toHaveBeenCalledTimes(1))
  })

  it('shows adapter warnings while allowing installation to continue', async () => {
    mocks.status.mockResolvedValue(status('setup_in_progress'))
    mocks.session.mockResolvedValue(session(true))
    mocks.adapters.mockResolvedValue([{ status: { id: 'owncast', name: 'Owncast', state: 'failed' } }])
    renderSetup()
    fireEvent.click(await screen.findByRole('button', { name: '저장소 검사 실행' }))
    expect(await screen.findByText('기본 저장소 진단')).toBeInTheDocument()
    fireEvent.click(await screen.findByRole('button', { name: '계속' }))
    expect(await screen.findByText(/일부 어댑터에 설정이나 확인이 필요합니다/)).toBeInTheDocument()
    expect(screen.getByText('Owncast')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: '계속' })).toBeEnabled()
  })

  it('completes installation and opens the dashboard without restarting', async () => {
    mocks.status.mockResolvedValue(status('setup_in_progress'))
    mocks.session.mockResolvedValue(session(true))
    renderSetup()
    fireEvent.click(await screen.findByRole('button', { name: '저장소 검사 실행' }))
    expect(await screen.findByText('기본 저장소 진단')).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: '계속' }))
    fireEvent.click(await screen.findByRole('button', { name: '계속' }))
    fireEvent.click(await screen.findByRole('button', { name: '설치 완료' }))
    expect(await screen.findByRole('heading', { name: 'Integrated Recorder가 준비되었습니다' })).toBeInTheDocument()
    expect(mocks.complete).toHaveBeenCalledTimes(1)
    fireEvent.click(screen.getByRole('button', { name: 'Recorder 열기' }))
    expect(mocks.navigate).toHaveBeenCalledWith({ to: '/' })
  })

  it('shows a fail-closed recovery screen with only a safe diagnostic code', async () => {
    mocks.status.mockResolvedValue(status('recovery_required', { diagnostic_code: 'INSTALLATION_STATE_INVALID' }))
    renderSetup()
    expect(await screen.findByRole('heading', { name: '설치 상태를 확인할 수 없습니다' })).toBeInTheDocument()
    expect(screen.getByText('INSTALLATION_STATE_INVALID')).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /초기화|reset/i })).not.toBeInTheDocument()
    expect(mocks.complete).not.toHaveBeenCalled()
  })
})
