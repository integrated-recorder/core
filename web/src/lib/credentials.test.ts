import { describe, expect, it } from 'vitest'
import { passwordByteLength, passwordLengthStatus } from './credentials'

describe('password byte policy', () => {
  it('counts UTF-8 bytes rather than JavaScript characters', () => {
    expect(passwordByteLength('가'.repeat(4))).toBe(12)
    expect(passwordLengthStatus('가'.repeat(4))).toBe('valid')
    expect(passwordLengthStatus('가'.repeat(24))).toBe('valid')
    expect(passwordLengthStatus('가'.repeat(25))).toBe('too_long')
    expect(passwordLengthStatus('가'.repeat(23) + 'ab')).toBe('valid')
  })
})
