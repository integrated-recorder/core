import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { render, screen } from '@testing-library/react'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { HTTPSSettings } from './https-settings'
import { I18nProvider } from '@/i18n/provider'

const api = vi.hoisted(() => ({
  status: vi.fn(),
  enableSelfSigned: vi.fn(),
  enableACME: vi.fn(),
  enableCertificate: vi.fn(),
  enableSaved: vi.fn(),
  disable: vi.fn(),
}))

vi.mock('@/api', async importOriginal => ({
  ...await importOriginal<typeof import('@/api')>(),
  runtimeHTTPSAPI: api,
}))

const emptyStatus = {
  enabled: false,
  configured: false,
  listening: false,
  certificate_ready: false,
  issuance_failed: false,
  redirect_http: false,
  redirect_active: false,
  http_recovery_available: true,
  port: 8443,
  http_port: 8080,
  self_signed: false,
  acme: false,
}

function renderSettings(status: unknown) {
  api.status.mockResolvedValue(status)
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  return render(<QueryClientProvider client={client}><I18nProvider><HTTPSSettings /></I18nProvider></QueryClientProvider>)
}

describe('HTTPS settings initial state', () => {
  beforeEach(() => vi.clearAllMocks())

  it.each([null, undefined, 'invalid', { unexpected: true }, [null, 42]])('renders safely when the backend host list is malformed: %s', async hosts => {
    renderSettings({ ...emptyStatus, hosts })
    expect(await screen.findByText(/HTTPS disabled|HTTPS 꺼짐/i)).toBeInTheDocument()
    expect(screen.getByPlaceholderText('recorder.example.com, 192.168.1.20')).toBeInTheDocument()
    expect(screen.queryByRole('alert')).not.toBeInTheDocument()
  })

  it('allows the first self-signed configuration to start with no hosts', async () => {
    renderSettings({ ...emptyStatus, hosts: [] })
    expect(await screen.findByText(/HTTPS disabled|HTTPS 꺼짐/i)).toBeInTheDocument()
    expect(screen.getByPlaceholderText('recorder.example.com, 192.168.1.20')).toHaveValue('')
  })

  it('renders saved HTTPS settings safely if a response omits its host list', async () => {
    renderSettings({ ...emptyStatus, configured: true, redirect_http: true, hosts: null })
    expect(await screen.findByRole('button', { name: /re-enable saved HTTPS settings|저장된 HTTPS 설정 다시 켜기/i })).toBeInTheDocument()
    expect(screen.getByPlaceholderText('recorder.example.com, 192.168.1.20')).toHaveValue('')
  })
})
