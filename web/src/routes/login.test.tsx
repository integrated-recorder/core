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

function setNativeInputValue(input: HTMLInputElement, value: string) {
  const setter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')?.set
  if (!setter) throw new Error('native input value setter is unavailable')
  setter.call(input, value)
}

beforeEach(() => {
  vi.clearAllMocks()
  mocks.session.mockResolvedValue({ auth_enabled: true, authenticated: false, needs_bootstrap: false })
  mocks.login.mockResolvedValue({ auth_enabled: true, authenticated: true, needs_bootstrap: false, csrf_token: 'login-csrf' })
})

describe('normal administrator login', () => {
  it('renders a conventional credential form and submits browser-populated values', async () => {
    renderLogin()
    expect(screen.getByRole('heading', { name: '로그인' })).toBeInTheDocument()
    expect(screen.queryByText('Archive Live.')).not.toBeInTheDocument()
    expect(screen.queryByText(/HttpOnly|브라우저에 저장되지 않습니다/)).not.toBeInTheDocument()
    const username = screen.getByLabelText('계정 이름') as HTMLInputElement
    const password = await screen.findByLabelText('비밀번호') as HTMLInputElement
    expect(username).toHaveAttribute('type', 'text')
    expect(username).toHaveAttribute('id', 'login')
    expect(username).toHaveAttribute('name', 'username')
    expect(username).toHaveAttribute('autocomplete', 'username')
    expect(username).toHaveAttribute('autocapitalize', 'none')
    expect(username).toHaveAttribute('spellcheck', 'false')
    expect(password).toHaveAttribute('type', 'password')
    expect(password).toHaveAttribute('name', 'password')
    expect(password).toHaveAttribute('autocomplete', 'current-password')
    expect(password).toBeRequired()
    const form = username.closest('form')
    expect(form).not.toBeNull()
    expect(password.closest('form')).toBe(form)
    expect(form).toContainElement(screen.getByRole('button', { name: '로그인' }))
    await waitFor(() => expect(screen.getByRole('button', { name: '로그인' })).toBeEnabled())

    // Set native DOM values without dispatching input/change, as AutoFill may do.
    setNativeInputValue(username, 'alice')
    setNativeInputValue(password, 'correct-password')
    fireEvent.submit(form!)
    await waitFor(() => expect(mocks.login).toHaveBeenCalledWith('alice', 'correct-password'))
    expect(mocks.navigate).toHaveBeenCalledWith({ to: '/' })
    expect(screen.queryByLabelText('초기화 토큰')).not.toBeInTheDocument()
    expect(screen.queryByText(/bootstrap-token/)).not.toBeInTheDocument()
  })

  it('rejects a blank username rather than assuming an Owner login', async () => {
    renderLogin()
    const username = screen.getByLabelText('계정 이름') as HTMLInputElement
    const password = await screen.findByLabelText('비밀번호') as HTMLInputElement
    await waitFor(() => expect(screen.getByRole('button', { name: '로그인' })).toBeEnabled())
    setNativeInputValue(password, 'a-strong-passphrase')
    fireEvent.submit(username.closest('form')!)
    expect(await screen.findByRole('alert')).toHaveTextContent('로그인 이름을 입력하세요.')
    expect(mocks.login).not.toHaveBeenCalled()
    expect(mocks.navigate).not.toHaveBeenCalled()
  })
})
