export type PasswordLength = 'valid' | 'too_short' | 'too_long'

export function passwordByteLength(password: string): number {
  return new TextEncoder().encode(password).length
}

export function passwordLengthStatus(password: string): PasswordLength {
  const bytes = passwordByteLength(password)
  if (bytes < 12) return 'too_short'
  if (bytes > 72) return 'too_long'
  return 'valid'
}
