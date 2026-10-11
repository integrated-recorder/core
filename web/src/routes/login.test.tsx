import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { LoginPage } from './login'
import { ToastProvider } from '@/components/ui/toast'

const mocks = vi.hoisted(() => ({ session: vi.fn(), login: vi.fn(), logout: vi.fn(), navigate: vi.fn() }))
vi.mock('@/api', async importOriginal => {
  const actual = await importOriginal<typeof import('@/api')>()
  return { ...actual, authAPI: { ...actual.authAPI, session: mocks.session, login: mocks.login, logout: mocks.logout } }
})
vi.mock('@tanstack/react-router', async importOriginal => ({ ...await importOriginal<typeof import('@tanstack/react-router')>(), useNavigate: () => mocks.navigate }))

function renderLogin() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } })
  return render(<QueryClientProvider client={client}><ToastProvider><LoginPage /></ToastProvider></QueryClientProvider>)
}

beforeEach(() => {
  vi.clearAllMocks()
  mocks.session.mockResolvedValue({ auth_enabled: true, authenticated: false, needs_bootstrap: false })
  mocks.login.mockResolvedValue({ auth_enabled: true, authenticated: true, needs_bootstrap: false, csrf_token: 'login-csrf' })
})

describe('normal administrator login', () => {
  it('submits only a password and navigates into the application', async () => {
    renderLogin()
    expect(screen.getByRole('heading', { name: '로그인' })).toBeInTheDocument()
    expect(screen.queryByText('Archive Live.')).not.toBeInTheDocument()
    expect(screen.queryByText(/HttpOnly|브라우저에 저장되지 않습니다/)).not.toBeInTheDocument()
    expect(screen.getByLabelText('계정 이름')).toBeInTheDocument()
    fireEvent.change(await screen.findByLabelText('비밀번호'), { target: { value: 'a-strong-passphrase' } })
    fireEvent.click(screen.getByRole('button', { name: '로그인' }))
    await waitFor(() => expect(mocks.login).toHaveBeenCalledWith('a-strong-passphrase'))
    expect(mocks.navigate).toHaveBeenCalledWith({ to: '/' })
    expect(screen.queryByLabelText('초기화 토큰')).not.toBeInTheDocument()
    expect(screen.queryByText(/bootstrap-token/)).not.toBeInTheDocument()
  })
})
