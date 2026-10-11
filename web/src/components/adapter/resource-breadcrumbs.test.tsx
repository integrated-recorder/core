import { fireEvent, render, screen } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'
import { ResourceBreadcrumbs } from './resource-breadcrumbs'
import type { ResourceRef } from '@/types/api'

const resource: ResourceRef = {
  resource_type: 'gamma', resource_id: '3',
  parent: { resource_type: 'beta', resource_id: '2', parent: { resource_type: 'alpha', resource_id: '1' } },
}

describe('ResourceBreadcrumbs', () => {
  it('offers root, ancestors, and parent navigation using opaque references', () => {
    const onNavigate = vi.fn()
    render(<ResourceBreadcrumbs resource={resource} onNavigate={onNavigate} />)
    fireEvent.click(screen.getByRole('button', { name: '상위' }))
    expect(onNavigate).toHaveBeenCalledWith(resource.parent)
    fireEvent.click(screen.getByRole('button', { name: 'alpha / 1' }))
    expect(onNavigate).toHaveBeenLastCalledWith(resource.parent?.parent)
    fireEvent.click(screen.getByRole('button', { name: '루트' }))
    expect(onNavigate).toHaveBeenLastCalledWith(undefined)
  })
})
