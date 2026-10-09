import { describe, expect, it, vi } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { WatchForm } from './watch-form'
import { WatchDeleteAction } from './watch-delete-action'
import type { Adapter, Schema, WatchView } from '@/types/api'

const adapter: Adapter = {
  status: { id: 'owncast', state: 'ready' },
  descriptor: { id: 'owncast', name: 'Owncast', version: '1.0', protocol_version: 1, capabilities: ['watch'], input_schema: { fields: [] }, configuration_schema: { fields: [] }, media_types: ['HLS'] },
}
const schema: Schema = { fields: [
  { key: 'source_url', label: '방송 주소', control: 'text', required: true },
  { key: 'access_token', label: '접근 토큰', control: 'secret' },
] }
function wrapper(children: React.ReactNode) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  return <QueryClientProvider client={client}>{children}</QueryClientProvider>
}

describe('Watch form', () => {
  it('defaults new watches to preview enabled and submits secrets separately', async () => {
    const user = userEvent.setup()
    const onSubmit = vi.fn()
    render(wrapper(<WatchForm adapter={adapter} schema={schema} submitLabel="Watch 등록" onSubmit={onSubmit} />))
    expect(screen.getByLabelText('방송 확인 주기 (초)')).toHaveValue(10)
    expect(screen.getByRole('checkbox', { name: /장면 미리보기 생성/ })).toBeChecked()
    await user.type(screen.getByLabelText(/방송 주소/), 'https://stream.example/live')
    await user.type(screen.getByLabelText('접근 토큰'), 'private-token-value')
    await user.click(screen.getByRole('button', { name: 'Watch 등록' }))
    await waitFor(() => expect(onSubmit).toHaveBeenCalledTimes(1))
    expect(onSubmit).toHaveBeenCalledWith(expect.objectContaining({
      input: { source_url: 'https://stream.example/live' },
      input_secrets: { access_token: 'private-token-value' },
      check_interval_seconds: 10,
      preview_mode: 'segment',
    }))
    expect(onSubmit.mock.calls[0]?.[0]).not.toHaveProperty('clear_resource')
    expect(screen.queryByText('private-token-value')).not.toBeInTheDocument()
  })

  it('rejects intervals outside 2–3600 seconds before calling the API handler', async () => {
    const user = userEvent.setup()
    const onSubmit = vi.fn()
    render(wrapper(<WatchForm adapter={adapter} schema={schema} submitLabel="저장" onSubmit={onSubmit} />))
    await user.type(screen.getByLabelText(/방송 주소/), 'https://stream.example/live')
    await user.clear(screen.getByLabelText('방송 확인 주기 (초)'))
    await user.type(screen.getByLabelText('방송 확인 주기 (초)'), '1')
    await user.click(screen.getByRole('button', { name: '저장' }))
    expect(await screen.findByRole('alert')).toHaveTextContent('2초에서 3,600초')
    expect(onSubmit).not.toHaveBeenCalled()
  })

  it('shows configured-only state without populating secret values into an edit form', async () => {
    const user = userEvent.setup()
    const onSubmit = vi.fn()
    const watch: WatchView = {
      id: 'watch-1', adapter_id: 'owncast', adapter_name: 'Owncast', input: { source_url: 'https://stream.example/live', access_token: 'must-not-render' },
      enabled: true, preview_mode: 'disabled', check_interval_seconds: 10, state: 'offline', created_at: '2026-09-28T00:00:00Z', updated_at: '2026-09-28T00:00:00Z',
      input_secret_configured: { access_token: true },
    }
    render(wrapper(<WatchForm adapter={adapter} schema={schema} watch={watch} submitLabel="저장" onSubmit={onSubmit} />))
    expect(screen.getByLabelText('접근 토큰')).toHaveValue('')
    expect(screen.getByLabelText('접근 토큰')).toHaveAttribute('placeholder', '설정된 값이 있습니다 · 새 값 입력 시 교체')
    expect(screen.queryByDisplayValue('must-not-render')).not.toBeInTheDocument()
    await user.click(screen.getByRole('button', { name: '저장' }))
    await waitFor(() => expect(onSubmit).toHaveBeenCalledWith(expect.objectContaining({ input: { source_url: 'https://stream.example/live' }, input_secrets: {} })))
  })

  it('preserves an existing explicit preview opt-out', async () => {
    const user = userEvent.setup()
    const onSubmit = vi.fn()
    const watch: WatchView = {
      id: 'watch-preview-off', adapter_id: 'owncast', input: { source_url: 'https://stream.example/live' },
      enabled: true, preview_mode: 'disabled', check_interval_seconds: 10, state: 'offline',
      created_at: '2026-09-28T00:00:00Z', updated_at: '2026-09-28T00:00:00Z', input_secret_configured: {},
    }
    render(wrapper(<WatchForm adapter={adapter} schema={schema} watch={watch} submitLabel="저장" onSubmit={onSubmit} />))
    expect(screen.getByRole('checkbox', { name: /장면 미리보기 생성/ })).not.toBeChecked()
    await user.click(screen.getByRole('button', { name: '저장' }))
    await waitFor(() => expect(onSubmit).toHaveBeenCalledWith(expect.objectContaining({ preview_mode: 'disabled' })))
  })

  it('sends clear_resource only when an existing resource is explicitly cleared', async () => {
    const user = userEvent.setup()
    const onSubmit = vi.fn()
    const browseAdapter: Adapter = { ...adapter, descriptor: { ...adapter.descriptor!, capabilities: ['watch', 'resource_browse'] } }
    const watch: WatchView = {
      id: 'watch-resource', adapter_id: 'owncast', resource: { resource_type: 'channel', resource_id: 'demo' },
      input: { source_url: 'https://stream.example/live' }, enabled: true, preview_mode: 'disabled', check_interval_seconds: 10,
      state: 'offline', created_at: '2026-09-28T00:00:00Z', updated_at: '2026-09-28T00:00:00Z', input_secret_configured: {},
    }
    const view = render(wrapper(<WatchForm adapter={browseAdapter} schema={schema} watch={watch} submitLabel="저장" onSubmit={onSubmit} />))
    await user.click(screen.getByRole('button', { name: '해제' }))
    await user.click(screen.getByRole('button', { name: '저장' }))
    await waitFor(() => expect(onSubmit).toHaveBeenCalledTimes(1))
    expect(onSubmit.mock.calls[0]?.[0]).toMatchObject({ clear_resource: true })
    expect(onSubmit.mock.calls[0]?.[0]).not.toHaveProperty('resource')

    view.unmount()
    const unchangedSubmit = vi.fn()
    render(wrapper(<WatchForm adapter={browseAdapter} schema={schema} watch={watch} submitLabel="저장" onSubmit={unchangedSubmit} />))
    await user.click(screen.getByRole('button', { name: '저장' }))
    await waitFor(() => expect(unchangedSubmit).toHaveBeenCalledTimes(1))
    expect(unchangedSubmit.mock.calls[0]?.[0]).not.toHaveProperty('clear_resource')
    expect(unchangedSubmit.mock.calls[0]?.[0]).toMatchObject({ resource: watch.resource })
  })

  it('only offers resource browsing when the adapter declares that capability', async () => {
    const browseAdapter: Adapter = { ...adapter, descriptor: { ...adapter.descriptor!, capabilities: ['watch', 'resource_browse'] } }
    const view = render(wrapper(<WatchForm adapter={browseAdapter} schema={schema} submitLabel="등록" onSubmit={vi.fn()} />))
    expect(screen.getByRole('button', { name: '리소스 찾아보기' })).toBeInTheDocument()
    view.unmount()
    render(wrapper(<WatchForm adapter={adapter} schema={schema} submitLabel="등록" onSubmit={vi.fn()} />))
    expect(screen.queryByRole('button', { name: '리소스 찾아보기' })).not.toBeInTheDocument()
  })
})

describe('Watch deletion confirmation', () => {
  it('states that existing recordings remain and an active recording keeps running', async () => {
    const user = userEvent.setup()
    render(<WatchDeleteAction onDelete={vi.fn()} />)
    await user.click(screen.getByRole('button', { name: '삭제' }))
    const dialog = screen.getByRole('alertdialog')
    expect(dialog).toHaveTextContent('이 Watch로 만들어진 녹화는 삭제되지 않으며')
    expect(dialog).toHaveTextContent('현재 진행 중인 녹화도 중지되지 않습니다')
  })
})
