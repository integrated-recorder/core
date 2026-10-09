import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'
import { SchemaForm } from './schema-form'
import type { Schema } from '@/types/api'

const schema: Schema = { fields: [
  { key: 'enabled', control: 'boolean', label: 'Enabled' },
  { key: 'detail', control: 'text', label: 'Detail', visible_when: { field: 'enabled', truthy: true } },
  { key: 'secret', control: 'secret', label: 'Secret', required: true },
] }

describe('SchemaForm', () => {
  it('renders conditional fields and submits generic schema values', async () => {
    const onSubmit = vi.fn()
    render(<SchemaForm schema={schema} onSubmit={onSubmit} submitLabel="Continue" />)
    expect(document.querySelector('[aria-label="필수"]')).toBeInTheDocument()
    expect(screen.queryByLabelText('Detail')).not.toBeInTheDocument()
    fireEvent.click(screen.getByRole('checkbox', { name: /Enabled/ }))
    fireEvent.change(screen.getByLabelText('Detail'), { target: { value: 'opaque' } })
    fireEvent.change(screen.getByLabelText(/Secret/), { target: { value: 'private-value' } })
    fireEvent.click(screen.getByRole('button', { name: 'Continue' }))
    await waitFor(() => expect(onSubmit).toHaveBeenCalledWith(expect.objectContaining({ values: { enabled: true, detail: 'opaque' }, secrets: { secret: 'private-value' } })))
    expect(document.body).not.toHaveTextContent('private-value')
  })

  it('keeps configured blank secrets unchanged and supports explicit removal', async () => {
    const onSubmit = vi.fn()
    render(<SchemaForm schema={{ fields: [schema.fields[2]!] }} mode="config" initialSecrets={{ secret: { configured: true } }} onSubmit={onSubmit} submitLabel="Save" />)
    expect(screen.getByLabelText(/Secret/)).toHaveAttribute('type', 'password')
    expect(screen.getByLabelText(/Secret/)).toHaveAttribute('placeholder', expect.stringContaining('설정된 값'))
    fireEvent.click(screen.getByRole('button', { name: 'Save' }))
    await waitFor(() => expect(onSubmit).toHaveBeenLastCalledWith(expect.objectContaining({ secrets: {}, clearSecrets: [] })))
    fireEvent.click(screen.getByRole('button', { name: '명시적으로 삭제' }))
    fireEvent.click(screen.getByRole('button', { name: 'Save' }))
    await waitFor(() => expect(onSubmit).toHaveBeenLastCalledWith(expect.objectContaining({ secrets: {}, clearSecrets: ['secret'] })))
  })

  it('restores object-valued multi-select choices and keeps explicit false values', async () => {
    const onSubmit = vi.fn()
    render(<SchemaForm schema={{ fields: [
      { key: 'enabled', control: 'boolean', label: 'Enabled' },
      { key: 'formats', control: 'multi-select', label: 'Formats', options: [{ value: { mode: 'source', ids: [1, 2] }, label: 'Source' }] },
      { key: 'action', control: 'action', label: 'Refresh' },
      { key: 'display', control: 'status', label: 'Status' },
    ] }} initialValues={{ enabled: false, formats: [{ ids: [1, 2], mode: 'source' }] }} onSubmit={onSubmit} />)
    expect(screen.getByRole('checkbox', { name: /Enabled 사용/ })).not.toBeChecked()
    expect(screen.getByRole('checkbox', { name: 'Source' })).toBeChecked()
    fireEvent.click(screen.getByRole('button', { name: '저장' }))
    await waitFor(() => expect(onSubmit).toHaveBeenCalledWith(expect.objectContaining({ values: { enabled: false, formats: [{ mode: 'source', ids: [1, 2] }] } })))
  })

  it('shows inherited values and submits explicit local override removal', async () => {
    const onSubmit = vi.fn()
    render(<SchemaForm schema={{ fields: [{ key: 'quality', control: 'text', label: 'Quality' }] }} mode="config" initialValues={{ quality: 'source' }} storedValues={{ quality: 'source' }} valueSources={{ quality: 'alpha' }} onSubmit={onSubmit} />)
    fireEvent.click(screen.getByRole('button', { name: /상속\/기본값 사용/ }))
    fireEvent.click(screen.getByRole('button', { name: '저장' }))
    await waitFor(() => expect(onSubmit).toHaveBeenCalledWith(expect.objectContaining({ values: {}, clearValues: ['quality'] })))
  })
})
