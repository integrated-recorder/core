import { fireEvent, render, screen } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'
import { ExportRow, JobTerminalMessage } from './recording-detail'
import type { ExportJob } from '@/types/api'

const job = (state: ExportJob['state']): ExportJob => ({ id: 'export-1', recording_id: 'recording-1', state, format: 'mkv' })

describe('export row action semantics', () => {
  it('cancels active jobs without presenting archive deletion copy', () => {
    const action = vi.fn()
    render(<ExportRow item={job('running')} onDelete={action} deleting={false} />)
    fireEvent.click(screen.getByRole('button', { name: '내보내기 작업 취소' }))
    expect(action).toHaveBeenCalledOnce()
    expect(screen.queryByRole('alertdialog')).not.toBeInTheDocument()
  })

  it('requires confirmation to delete a terminal export projection', () => {
    const action = vi.fn()
    render(<ExportRow item={job('failed')} onDelete={action} deleting={false} />)
    fireEvent.click(screen.getByRole('button', { name: '내보내기 항목 삭제' }))
    expect(screen.getByRole('alertdialog')).toHaveTextContent('원본 녹화 보관 데이터에는 영향이 없습니다.')
    fireEvent.click(screen.getByRole('button', { name: '내보내기 삭제' }))
    expect(action).toHaveBeenCalledOnce()
  })

  it.each(['integrity', 'export'])('labels restart-recovered %s failure as interrupted', kind => {
    render(kind === 'export'
      ? <ExportRow item={{ ...job('failed'), error_code: 'interrupted_by_restart' }} onDelete={() => undefined} deleting={false} />
      : <JobTerminalMessage state="failed" errorCode="interrupted_by_restart" />)

    expect(screen.getByText('작업이 재시작 과정에서 중단되었습니다.')).toBeInTheDocument()
    expect(screen.queryByText('서버가 요청을 처리하지 못했습니다. 잠시 후 다시 시도하세요.')).not.toBeInTheDocument()
  })
})
