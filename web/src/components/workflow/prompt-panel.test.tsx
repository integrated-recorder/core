import { describe, expect, it } from 'vitest'
import { render, screen } from '@testing-library/react'
import { WorkflowPromptPanel } from './prompt-panel'
import { safeExternalURL } from '@/lib/urls'
import type { WorkflowPrompt } from '@/types/api'

describe('workflow prompt parity', () => {
  it.each(['prompt', 'secret_prompt', 'display', 'status'] as const)('renders %s as informational copy', type => {
    render(<WorkflowPromptPanel prompt={{ type, message: 'Enter a response' }} />)
    expect(screen.getByRole('status')).toHaveTextContent('Enter a response')
  })

  it('renders navigate and action links with safe labels and link isolation', () => {
    const navigate: WorkflowPrompt = { type: 'navigate', data: { url: 'https://example.test/verify' } }
    const action: WorkflowPrompt = { type: 'action', data: { url: 'https://example.test/continue', label: 'Continue verification' } }
    const view = render(<><WorkflowPromptPanel prompt={navigate} /><WorkflowPromptPanel prompt={action} /></>)
    const navigateLink = screen.getByRole('link', { name: /외부 페이지 열기/ })
    const actionLink = screen.getByRole('link', { name: /Continue verification/ })
    expect(navigateLink).toHaveAttribute('target', '_blank')
    expect(navigateLink).toHaveAttribute('rel', 'noopener noreferrer')
    expect(actionLink).toHaveAttribute('href', 'https://example.test/continue')
    view.unmount()
  })

  it('shows complete/error as terminal status panels and unknown types safely fall back', () => {
    const { rerender } = render(<WorkflowPromptPanel prompt={{ type: 'complete', message: 'Done' }} />)
    expect(screen.getByRole('status')).toHaveTextContent('Done')
    rerender(<WorkflowPromptPanel prompt={{ type: 'error', message: 'Rejected' }} />)
    expect(screen.getByRole('alert')).toHaveTextContent('Rejected')
    rerender(<WorkflowPromptPanel prompt={{ type: 'future-type', message: 'Future info' }} />)
    expect(screen.getByRole('status')).toHaveTextContent('Future info')
  })

  it.each(['javascript:alert(1)', 'data:text/html,test', 'file:///etc/passwd', 'https://user:pass@example.test/', 'http://'])('rejects unsafe URL %s', value => {
    expect(safeExternalURL(value)).toBe('')
  })

  it('accepts ordinary HTTP and HTTPS URLs with a hostname', () => {
    expect(safeExternalURL('https://example.test/a')).toBe('https://example.test/a')
    expect(safeExternalURL('http://localhost:8080/path')).toBe('http://localhost:8080/path')
  })

  it('does not render a link for an unsafe action URL', () => {
    render(<WorkflowPromptPanel prompt={{ type: 'action', data: { url: 'https://user:pass@example.test/' } }} />)
    expect(screen.queryByRole('link')).not.toBeInTheDocument()
    expect(screen.getByText('열 수 있는 외부 링크가 없습니다.')).toBeInTheDocument()
  })
})
