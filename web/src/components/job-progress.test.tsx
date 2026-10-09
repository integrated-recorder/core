import { describe, expect, it } from 'vitest'
import { render, screen } from '@testing-library/react'
import { JobProgress } from './job-progress'
import { jobProgressPercent } from '@/lib/job-progress'

describe('job progress display', () => {
  it('shows determinate progress only with a known positive total', () => {
    const progress = { current: 24, total: 80, percent: 30, indeterminate: false, phase: 'verifying', unit: 'objects' }
    render(<JobProgress label="Integrity verification" progress={progress} />)
    expect(screen.getByRole('progressbar', { name: 'Integrity verification' })).toHaveAttribute('aria-valuenow', '30')
    expect(screen.getByText('30%')).toBeInTheDocument()
    expect(screen.getByText(/24 \/ 80 객체/)).toBeInTheDocument()
  })

  it('keeps unknown totals indeterminate and omits percentage', () => {
    expect(jobProgressPercent({ current: 24, indeterminate: true, phase: 'writing', unit: 'bytes' })).toBeUndefined()
    render(<JobProgress label="Export" progress={{ current: 24, indeterminate: true, phase: 'writing', unit: 'bytes' }} />)
    const bar = screen.getByRole('progressbar', { name: 'Export' })
    expect(bar).not.toHaveAttribute('aria-valuenow')
    expect(bar).toHaveAttribute('aria-valuetext', '진행률 계산 중')
    expect(screen.queryByText(/%/)).not.toBeInTheDocument()
  })

  it('clamps malformed percentage into accessible range', () => {
    expect(jobProgressPercent({ current: 1, total: 10, percent: 140, indeterminate: false, phase: 'reading', unit: 'segments' })).toBe(100)
  })
})
