import { describe, expect, it } from 'vitest'
import type { InstallationStatus } from '@/types/api'
import { installationRouteRedirect } from './installation-routing'

function status(state: InstallationStatus['state']): InstallationStatus {
  return { state, administrator_configured: state !== 'uninitialized', recovery_required: state === 'recovery_required', auth_disabled: false, version: 'dev', release_channel: 'development' }
}

describe('installation route policy', () => {
  it('sends a fresh installation to setup before checking authentication', () => {
    expect(installationRouteRedirect('/', status('uninitialized'), false)).toBe('/setup')
    expect(installationRouteRedirect('/', status('setup_in_progress'), true)).toBe('/setup')
  })

  it('sends ready installations to login or the application according to session state', () => {
    expect(installationRouteRedirect('/', status('ready'), false)).toBe('/login')
    expect(installationRouteRedirect('/', status('ready'), true)).toBeUndefined()
  })

  it('keeps setup available until ready and then returns to the application', () => {
    expect(installationRouteRedirect('/setup', status('setup_in_progress'), true)).toBeUndefined()
    expect(installationRouteRedirect('/setup', status('ready'), true)).toBe('/')
  })

  it('sends legacy bootstrap login URLs to setup', () => {
    expect(installationRouteRedirect('/login', status('uninitialized'), false, true)).toBe('/setup')
  })

  it('does not turn recovery-required into a normal login or setup flow', () => {
    expect(installationRouteRedirect('/', status('recovery_required'), false)).toBe('/setup')
    expect(installationRouteRedirect('/setup', status('recovery_required'), false)).toBeUndefined()
  })
})
