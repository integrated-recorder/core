import { describe, expect, it } from 'vitest'
import { APIError } from '@/api/client'
import { errorMessage } from './errors'

describe('errorMessage', () => {
  it('localizes stable codes and never exposes unknown technical details', () => {
    expect(errorMessage(new Error('private backend detail'))).toContain('요청을 완료하지 못했습니다')
    expect(errorMessage(new APIError(400, 'private backend detail', 'req-1', 'archive_index_invalid_cursor'))).toContain('올바르지 않습니다')
    expect(errorMessage(new APIError(400, 'private backend detail', 'req-1', 'preferences_invalid'))).toContain('환경 설정 값을 확인')
    expect(errorMessage(new APIError(409, 'credential=secret', 'req-1'))).toContain('상태가 변경')
    expect(errorMessage(new APIError(500, 'credential=secret', 'req-1', 'unknown_private_detail'))).toContain('서버가 요청을 처리하지 못했습니다')
    expect(errorMessage(new APIError(409, 'credential=secret', 'req-1'))).not.toContain('credential')
    expect(errorMessage({ secret: 'never show' })).toContain('요청을 완료하지 못했습니다')
    expect(errorMessage({ secret: 'never show' })).not.toContain('never show')
  })
})
