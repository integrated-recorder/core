import { expect, test } from '@playwright/test'
import type { Locator } from '@playwright/test'

test.use({ locale: 'ko-KR' })

const ownerID = 'usr-00000000000000000000000000000001'
const ownerPermissions = [
  'recording.read', 'recording.create', 'recording.stop', 'recording.delete', 'recording.export', 'recording.integrity.run',
  'recording.annotate', 'recording.preview.manage', 'recording.workflow.manage', 'watch.read', 'watch.manage', 'watch.check',
  'plugin.read', 'plugin.manage', 'plugin.configure', 'storage.read', 'storage.manage', 'system.read', 'system.settings.manage',
  'system.update.manage', 'system.diagnostics.read', 'user.read', 'user.create', 'user.manage', 'role.read', 'role.manage', 'audit.read',
]
const readyStatus = { state: 'ready', administrator_configured: true, claim_required: false, recovery_required: false, auth_disabled: false, version: '1.2.3', release_channel: 'stable' }
const emptyDashboard = { active_recordings_count: 0, completed_last_24h: 0, interrupted_last_24h: 0, recordings_total: 0, segments_total: 0, gaps_total: 0, archive_bytes: 0, filesystem_total_bytes: 0, filesystem_free_bytes: 0, filesystem_used_bytes: 0, integrity: {}, adapters: {}, export_available: false, recent_recordings: [], active_recordings: [] }
const defaultPreferences = { locale: 'ko-KR', theme: 'system', timezone: 'Asia/Seoul' }
const user = (login = 'test-admin') => ({ user_id: ownerID, login, roles: ['owner'], status: 'active', created_at: '2026-10-17T00:00:00Z' })
const authorization = (login = 'test-admin') => ({ user_id: ownerID, login, roles: ['owner'], permissions: ownerPermissions })
const roles = [
  { id: 'owner', name: 'Owner', built_in: true, permissions: ownerPermissions },
  { id: 'administrator', name: 'Administrator', built_in: true, permissions: ownerPermissions.filter(permission => permission !== 'owner.root') },
  { id: 'operator', name: 'Operator', built_in: true, permissions: ['recording.read', 'recording.create', 'recording.stop'] },
  { id: 'viewer', name: 'Viewer', built_in: true, permissions: ['recording.read'] },
]

async function setNativeValue(locator: Locator, value: string) {
  await locator.evaluate((element, next) => {
    const setter = Object.getOwnPropertyDescriptor(element.ownerDocument.defaultView!.HTMLInputElement.prototype, 'value')?.set
    if (!setter) throw new Error('native input value setter is unavailable')
    setter.call(element, next)
  }, value)
}

test('WebKit login uses the native required username/password pair and submits browser values', async ({ page }) => {
  let authenticated = false
  await page.route('**/api/**', async route => {
    const request = route.request()
    const path = new URL(request.url()).pathname
    if (path === '/api/setup/status') return route.fulfill({ json: readyStatus })
    if (path === '/api/auth/session') return route.fulfill({ json: { auth_enabled: true, authenticated, needs_bootstrap: false, csrf_token: authenticated ? 'session-csrf' : '' } })
    if (path === '/api/auth/login') {
      expect(request.postDataJSON()).toEqual({ login: 'webkit-user', password: 'browser-filled-secret' })
      authenticated = true
      return route.fulfill({ json: { auth_enabled: true, authenticated: true, needs_bootstrap: false, csrf_token: 'session-csrf' } })
    }
    if (path === '/api/dashboard') return route.fulfill({ json: { active_recordings_count: 0, completed_last_24h: 0, interrupted_last_24h: 0, recordings_total: 0, segments_total: 0, gaps_total: 0, archive_bytes: 0, filesystem_total_bytes: 0, filesystem_free_bytes: 0, filesystem_used_bytes: 0, integrity: {}, adapters: {}, export_available: false, recent_recordings: [], active_recordings: [] } })
    if (path === '/api/notifications') return route.fulfill({ json: { items: [] } })
    if (path === '/api/resolve-workflows') return route.fulfill({ json: [] })
    return route.fulfill({ json: [] })
  })

  await page.goto('/login')
  const form = page.locator('form')
  const username = form.locator('input[name="username"]')
  const password = form.locator('input[name="password"]')
  await expect(username).toHaveAttribute('type', 'text')
  await expect(username).toHaveAttribute('autocomplete', 'username')
  await expect(username).toHaveAttribute('autocapitalize', 'none')
  await expect(username).toHaveAttribute('spellcheck', 'false')
  await expect(username).toHaveJSProperty('required', true)
  await expect(password).toHaveAttribute('type', 'password')
  await expect(password).toHaveAttribute('autocomplete', 'current-password')
  await expect(password).toHaveJSProperty('required', true)
  await setNativeValue(username, 'webkit-user')
  await setNativeValue(password, 'browser-filled-secret')
  await form.locator('button[type="submit"]').click()
  await expect(page).toHaveURL('/')
})

test('WebKit bootstrap uses an editable username and native UTF-8 password constraints', async ({ page }) => {
  let authenticated = false
  let state = 'uninitialized'
  await page.route('**/api/**', async route => {
    const request = route.request()
    const path = new URL(request.url()).pathname
    if (path === '/api/setup/status') return route.fulfill({ json: { state, administrator_configured: state !== 'uninitialized', claim_required: state === 'uninitialized', recovery_required: false, auth_disabled: false, version: '1.2.3', release_channel: 'stable' } })
    if (path === '/api/auth/session') return route.fulfill({ json: { auth_enabled: true, authenticated, needs_bootstrap: !authenticated, csrf_token: authenticated ? 'session-csrf' : 'setup-csrf' } })
    if (path === '/api/auth/bootstrap') {
      expect(request.postDataJSON()).toEqual({ login: 'chosen-admin', token: 'one-time-code', password: 'é'.repeat(6) })
      authenticated = true
      return route.fulfill({ json: { auth_enabled: true, authenticated: true, needs_bootstrap: false, csrf_token: 'session-csrf' } })
    }
    if (path === '/api/setup/begin') {
      state = 'setup_in_progress'
      return route.fulfill({ json: { state, administrator_configured: true, claim_required: false, recovery_required: false, auth_disabled: false, version: '1.2.3', release_channel: 'stable' } })
    }
    if (path === '/api/system/info') return route.fulfill({ json: { version: '1.2.3', commit: 'test', go_version: 'go', goos: 'darwin', goarch: 'arm64', started_at: new Date().toISOString(), uptime_seconds: 1, export_available: true } })
    if (path === '/api/adapters') return route.fulfill({ json: [] })
    return route.fulfill({ json: [] })
  })

  await page.goto('/setup')
  await page.getByRole('button', { name: '시작하기' }).click()
  const form = page.locator('form').filter({ has: page.getByRole('button', { name: '관리자 계정 만들기' }) })
  const username = form.locator('[name="username"]')
  const password = form.locator('[name="password"]')
  const confirmation = form.locator('[name="password_confirmation"]')
  await expect(username).toHaveValue('')
  await expect(username).toHaveAttribute('autocomplete', 'username')
  await expect(username).not.toHaveAttribute('readonly')
  await expect(password).toHaveAttribute('autocomplete', 'new-password')
  await expect(password).not.toHaveAttribute('minlength')
  await expect(password).not.toHaveAttribute('maxlength')
  await expect(confirmation).toHaveAttribute('autocomplete', 'new-password')
  await setNativeValue(username, 'chosen-admin')
  await setNativeValue(form.locator('[name="setup_code"]'), 'one-time-code')
  // Six two-byte characters satisfy the server's 12-byte rule; HTML minlength would miscount them.
  await setNativeValue(password, 'é'.repeat(6))
  await setNativeValue(confirmation, 'é'.repeat(6))
  await form.locator('button[type="submit"]').click()
  await expect(page.getByRole('heading', { name: '저장소 확인' })).toBeVisible()
})

test('WebKit setup resume submits an editable username with current-password semantics', async ({ page }) => {
  let authenticated = false
  await page.route('**/api/**', async route => {
    const request = route.request()
    const path = new URL(request.url()).pathname
    if (path === '/api/setup/status') return route.fulfill({ json: { ...readyStatus, state: 'setup_in_progress' } })
    if (path === '/api/auth/session') return route.fulfill({ json: { auth_enabled: true, authenticated, needs_bootstrap: false, csrf_token: 'resume-csrf' } })
    if (path === '/api/auth/login') {
      expect(request.postDataJSON()).toEqual({ login: 'chosen-admin', password: 'current-secret' })
      authenticated = true
      return route.fulfill({ json: { auth_enabled: true, authenticated: true, needs_bootstrap: false, csrf_token: 'session-csrf' } })
    }
    if (path === '/api/system/info') return route.fulfill({ json: { version: '1.2.3', commit: 'test', go_version: 'go', goos: 'darwin', goarch: 'arm64', started_at: new Date().toISOString(), uptime_seconds: 1, export_available: true } })
    if (path === '/api/adapters') return route.fulfill({ json: [] })
    return route.fulfill({ json: [] })
  })

  await page.goto('/setup')
  const form = page.locator('form')
  const username = form.locator('[name="username"]')
  const password = form.locator('[name="password"]')
  await expect(username).toHaveAttribute('autocomplete', 'username')
  await expect(username).not.toHaveAttribute('readonly')
  await expect(password).toHaveAttribute('autocomplete', 'current-password')
  await setNativeValue(username, 'chosen-admin')
  await setNativeValue(password, 'current-secret')
  await form.locator('button[type="submit"]').click()
  await expect(page.getByRole('heading', { name: '저장소 확인' })).toBeVisible()
})

test('WebKit account credential forms preserve username context and submit browser-filled values', async ({ page }) => {
  let authenticated = true
  let accountLogin = 'chosen-admin'
  const patches: Array<{ path: string; body: unknown }> = []
  await page.route('**/api/**', async route => {
    const request = route.request()
    const path = new URL(request.url()).pathname
    if (path === '/api/setup/status') return route.fulfill({ json: readyStatus })
    if (path === '/api/auth/session') return route.fulfill({ json: { auth_enabled: true, authenticated, needs_bootstrap: false, csrf_token: authenticated ? 'account-csrf' : '' } })
    if (path === '/api/dashboard') return route.fulfill({ json: emptyDashboard })
    if (path === '/api/adapters' || path === '/api/resolve-workflows') return route.fulfill({ json: [] })
    if (path === '/api/user/preferences') return route.fulfill({ json: defaultPreferences })
    if (path === '/api/notifications') return route.fulfill({ json: { items: [] } })
    if (path === '/api/user/authorization') return route.fulfill({ json: authorization(accountLogin) })
    if (path === '/api/user' && request.method() === 'GET') return route.fulfill({ json: user(accountLogin) })
    if (path === '/api/user' && request.method() === 'PATCH') {
      const body = request.postDataJSON() as { login: string; current_password: string }
      patches.push({ path, body })
      expect(body).toEqual({ login: 'new-admin-name', current_password: 'current-secret' })
      accountLogin = body.login
      authenticated = false
      return route.fulfill({ status: 204, body: '' })
    }
    if (path === '/api/user/password' && request.method() === 'POST') {
      const body = request.postDataJSON() as { current_password: string; new_password: string }
      patches.push({ path, body })
      expect(body).toEqual({ current_password: 'current-secret', new_password: 'new-secret-1234' })
      authenticated = false
      return route.fulfill({ status: 204, body: '' })
    }
    return route.fulfill({ json: { items: [] } })
  })

  await page.goto('/account/security')
  const renameForm = page.locator('form').filter({ has: page.getByRole('button', { name: '로그인 이름 변경' }) })
  const renameUsername = renameForm.locator('[name="username"]')
  await expect(renameUsername).toHaveValue('chosen-admin')
  await expect(renameUsername).toHaveAttribute('autocomplete', 'username')
  await expect(renameUsername).toHaveAttribute('readonly')
  await expect(renameForm.locator('[name="new_username"]')).toHaveAttribute('autocomplete', 'username')
  await expect(renameForm.locator('[name="current_password"]')).toHaveAttribute('autocomplete', 'current-password')
  await setNativeValue(renameForm.locator('[name="new_username"]'), 'new-admin-name')
  await setNativeValue(renameForm.locator('[name="current_password"]'), 'current-secret')
  await renameForm.locator('button[type="submit"]').click()
  await expect(page).toHaveURL('/login')
  expect(patches[0]).toEqual({ path: '/api/user', body: { login: 'new-admin-name', current_password: 'current-secret' } })
})

test('WebKit password change includes readonly username context and invalidates the session', async ({ page }) => {
  let authenticated = true
  await page.route('**/api/**', async route => {
    const request = route.request()
    const path = new URL(request.url()).pathname
    if (path === '/api/setup/status') return route.fulfill({ json: readyStatus })
    if (path === '/api/auth/session') return route.fulfill({ json: { auth_enabled: true, authenticated, needs_bootstrap: false, csrf_token: authenticated ? 'account-csrf' : '' } })
    if (path === '/api/dashboard') return route.fulfill({ json: emptyDashboard })
    if (path === '/api/adapters' || path === '/api/resolve-workflows') return route.fulfill({ json: [] })
    if (path === '/api/user/preferences') return route.fulfill({ json: defaultPreferences })
    if (path === '/api/notifications') return route.fulfill({ json: { items: [] } })
    if (path === '/api/user/authorization') return route.fulfill({ json: authorization() })
    if (path === '/api/user' && request.method() === 'GET') return route.fulfill({ json: user() })
    if (path === '/api/user/password' && request.method() === 'POST') {
      expect(request.postDataJSON()).toEqual({ current_password: 'current-secret', new_password: 'new-secret-1234' })
      authenticated = false
      return route.fulfill({ status: 204, body: '' })
    }
    return route.fulfill({ json: { items: [] } })
  })

  await page.goto('/account/security')
  const form = page.locator('form').filter({ has: page.getByRole('button', { name: '비밀번호 변경' }) })
  const username = form.locator('[name="username"]')
  await expect(username).toHaveValue('test-admin')
  await expect(username).toHaveAttribute('autocomplete', 'username')
  await expect(username).toHaveAttribute('readonly')
  await expect(form.locator('[name="current_password"]')).toHaveAttribute('autocomplete', 'current-password')
  await expect(form.locator('[name="new_password"]')).toHaveAttribute('autocomplete', 'new-password')
  await expect(form.locator('[name="password_confirmation"]')).toHaveAttribute('autocomplete', 'new-password')
  await setNativeValue(form.locator('[name="current_password"]'), 'current-secret')
  await setNativeValue(form.locator('[name="new_password"]'), 'new-secret-1234')
  await setNativeValue(form.locator('[name="password_confirmation"]'), 'new-secret-1234')
  await form.locator('button[type="submit"]').click()
  await expect(page).toHaveURL('/login')
})

test('WebKit secondary-user creation submits browser-populated credentials through FormData', async ({ page }) => {
  let created: unknown
  await page.route('**/api/**', async route => {
    const request = route.request()
    const path = new URL(request.url()).pathname
    if (path === '/api/setup/status') return route.fulfill({ json: readyStatus })
    if (path === '/api/auth/session') return route.fulfill({ json: { auth_enabled: true, authenticated: true, needs_bootstrap: false, csrf_token: 'account-csrf' } })
    if (path === '/api/dashboard') return route.fulfill({ json: emptyDashboard })
    if (path === '/api/adapters' || path === '/api/resolve-workflows') return route.fulfill({ json: [] })
    if (path === '/api/user/preferences') return route.fulfill({ json: defaultPreferences })
    if (path === '/api/notifications') return route.fulfill({ json: { items: [] } })
    if (path === '/api/user/authorization') return route.fulfill({ json: authorization() })
    if (path === '/api/user') return route.fulfill({ json: user() })
    if (path === '/api/users' && request.method() === 'GET') return route.fulfill({ json: { items: [user()] } })
    if (path === '/api/roles') return route.fulfill({ json: { roles, permissions: ownerPermissions } })
    if (path === '/api/users' && request.method() === 'POST') {
      created = request.postDataJSON()
      expect(created).toEqual({ login: 'second-user', password: 'generated-secret-1234', roles: ['operator'] })
      return route.fulfill({ status: 201, json: { ...user('second-user'), user_id: 'usr-00000000000000000000000000000002', roles: ['operator'] } })
    }
    return route.fulfill({ json: { items: [] } })
  })

  await page.goto('/system/users')
  const form = page.locator('form').filter({ has: page.getByRole('button', { name: '사용자 생성' }) })
  await expect(form.locator('[name="username"]')).toHaveAttribute('autocomplete', 'username')
  await expect(form.locator('[name="password"]')).toHaveAttribute('autocomplete', 'new-password')
  await expect(form.locator('[name="password_confirmation"]')).toHaveAttribute('autocomplete', 'new-password')
  await setNativeValue(form.locator('[name="username"]'), 'second-user')
  await setNativeValue(form.locator('[name="password"]'), 'generated-secret-1234')
  await setNativeValue(form.locator('[name="password_confirmation"]'), 'generated-secret-1234')
  await form.locator('button[type="submit"]').click()
  await expect.poll(() => created).toEqual({ login: 'second-user', password: 'generated-secret-1234', roles: ['operator'] })
})
