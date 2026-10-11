import { expect, test } from '@playwright/test'

// These mock-flow assertions use the Korean system-resolved catalog. Keep the
// browser locale explicit instead of relying on the machine running Playwright.
test.use({ locale: 'ko-KR' })

const storageSettings = {
  ingest_memory: { global_buffer_bytes: 1073741824, per_recording_buffer_bytes: 805306368, max_payload_bytes: 536870912 },
  queue_writer: { pending_queue_capacity: 128, writer_concurrency: 1 },
  failure_handling: { persist_attempts: 5, retry_initial_backoff_ms: 100, retry_max_backoff_ms: 800 },
  observability: { sampling_interval_ms: 5000, metrics_retention_ms: 86400000 },
}
function settingsResponse() {
  return { settings: { ui: { theme: 'light' as const }, integrity: { concurrency: 1 }, retention: { enabled: false, completed_after_days: 30 }, storage: structuredClone(storageSettings) }, effective_storage: structuredClone(storageSettings), restart_required: [] }
}
function dashboardResponse() {
  return { active_recordings_count: 0, completed_last_24h: 0, interrupted_last_24h: 0, recordings_total: 0, segments_total: 0, gaps_total: 0, archive_bytes: 0, filesystem_total_bytes: 0, filesystem_free_bytes: 0, filesystem_used_bytes: 0, integrity: {}, adapters: {}, export_available: false, recent_recordings: [], active_recordings: [] }
}

test('first-run setup claims the installation, runs diagnostics, and completes without exposing a token path', async ({ page }) => {
  const testOnlySetupCode = 'in-memory-one-time-test-code'
  let installationState: 'uninitialized' | 'setup_in_progress' | 'ready' = 'uninitialized'
  let administratorConfigured = false
  let authenticated = false
  let bootstrapCSRF = ''
  let beginCSRF = ''
  const requestedURLs: string[] = []
  page.on('request', request => requestedURLs.push(request.url()))
  await page.route('**/api/**', async route => {
    const request = route.request()
    const path = new URL(request.url()).pathname
    if (path === '/api/setup/status') {
      await route.fulfill({ json: { state: installationState, administrator_configured: administratorConfigured, claim_required: !administratorConfigured, recovery_required: false, auth_disabled: false, version: '1.2.3', release_channel: 'stable' } })
    } else if (path === '/api/auth/session') {
      await route.fulfill({ json: { auth_enabled: true, authenticated, needs_bootstrap: !administratorConfigured, csrf_token: authenticated ? 'csrf-after-bootstrap' : 'csrf-before-bootstrap' } })
    } else if (path === '/api/auth/bootstrap') {
      bootstrapCSRF = request.headers()['x-csrf-token'] ?? ''
      expect(request.postDataJSON()).toEqual({ token: testOnlySetupCode, password: 'a-strong-passphrase' })
      administratorConfigured = true
      authenticated = true
      await route.fulfill({ json: { auth_enabled: true, authenticated: true, needs_bootstrap: false, csrf_token: 'csrf-after-bootstrap' } })
    } else if (path === '/api/setup/begin') {
      beginCSRF = request.headers()['x-csrf-token'] ?? ''
      expect(request.postDataJSON()).toEqual({})
      installationState = 'setup_in_progress'
      await route.fulfill({ json: { state: installationState, administrator_configured: true, claim_required: false, recovery_required: false, auth_disabled: false, version: '1.2.3', release_channel: 'stable' } })
    } else if (path === '/api/setup/storage-test') {
      await route.fulfill({ json: { status: 'ready', free_bytes: 4000000000, write_test: 'passed', durability_test: 'passed' } })
    } else if (path === '/api/system/storage') {
      await route.fulfill({ json: { filesystem_total_bytes: 5000000000, filesystem_used_bytes: 1000000000, filesystem_available_bytes: 4000000000, recordings_bytes: 0, recording_count: 0, segment_count: 0, init_segment_count: 0, manifest_count: 0 } })
    } else if (path === '/api/system/info') {
      await route.fulfill({ json: { version: '1.2.3', commit: 'test', go_version: 'go', goos: 'linux', goarch: 'amd64', started_at: new Date().toISOString(), uptime_seconds: 1, export_available: true } })
    } else if (path === '/api/adapters') {
      await route.fulfill({ json: [{ status: { id: 'hls', name: 'HLS', state: 'running' }, descriptor: { id: 'hls', name: 'HLS', version: '1', protocol_version: 1, capabilities: [], input_schema: { fields: [] }, configuration_schema: { fields: [] }, media_types: [] } }] })
    } else if (path === '/api/setup/complete') {
      expect(request.headers()['x-csrf-token']).toBe('csrf-after-bootstrap')
      installationState = 'ready'
      await route.fulfill({ json: { state: installationState, administrator_configured: true, claim_required: false, recovery_required: false, auth_disabled: false, version: '1.2.3', release_channel: 'stable' } })
    } else if (path === '/api/dashboard') {
      await route.fulfill({ json: dashboardResponse() })
    } else if (path === '/api/notifications') {
      await route.fulfill({ json: { items: [] } })
    } else if (path === '/api/resolve-workflows') {
      await route.fulfill({ json: [] })
    } else if (path === '/api/settings') {
      await route.fulfill({ json: settingsResponse() })
    } else {
      await route.fulfill({ status: 404, json: { error: 'unexpected test request' } })
    }
  })

  await page.goto('/')
  await expect(page).toHaveURL('/setup')
  await expect(page.locator('html')).toHaveAttribute('lang', 'ko-KR')
  await expect(page.getByRole('heading', { name: 'Integrated Recorder 시작하기' })).toBeVisible()
  await expect(page.getByText(/docker compose exec archiver runtime-host setup-code/)).toHaveCount(0)
  await page.getByRole('button', { name: '시작하기' }).click()
  await expect(page.getByText('docker compose logs archiver', { exact: true })).toBeVisible()
  await expect(page.getByText(/runtime-host setup-code/)).toBeVisible()
  await page.getByLabel(/Setup code|설치 코드/).fill(testOnlySetupCode)
  await page.getByLabel('관리자 비밀번호').fill('a-strong-passphrase')
  await page.getByLabel('비밀번호 확인').fill('a-strong-passphrase')
  await page.getByRole('button', { name: '관리자 계정 만들기' }).click()
  await expect(page.getByRole('heading', { name: '저장소 확인' })).toBeVisible()
  await page.getByRole('button', { name: '저장소 검사 실행' }).click()
  await expect(page.getByText('기본 저장소 진단')).toBeVisible()
  await page.getByRole('button', { name: '계속' }).click()
  await expect(page.getByRole('heading', { name: '어댑터 확인' })).toBeVisible()
  await expect(page.getByText('HLS', { exact: false }).first()).toBeVisible()
  await page.getByRole('button', { name: '계속' }).click()
  await expect(page.getByRole('heading', { name: '마지막으로 확인' })).toBeVisible()
  await page.getByRole('button', { name: '설치 완료' }).click()
  await expect(page.getByRole('heading', { name: 'Integrated Recorder가 준비되었습니다' })).toBeVisible()
  await page.getByRole('button', { name: 'Recorder 열기' }).click()
  await expect(page).toHaveURL('/')
  await expect(page.getByRole('heading', { name: '대시보드' })).toBeVisible()
  expect(bootstrapCSRF).toBe('csrf-before-bootstrap')
  expect(beginCSRF).toBe('csrf-after-bootstrap')
  expect(requestedURLs.some(url => url.includes(testOnlySetupCode))).toBe(false)
  expect(requestedURLs.some(url => url.includes('bootstrap-token') || url.includes('/data/'))).toBe(false)
  for (const viewport of [{ width: 1440, height: 900 }, { width: 1024, height: 768 }, { width: 768, height: 1024 }, { width: 390, height: 844 }]) {
    await page.setViewportSize(viewport)
    await expect(page.getByRole('heading', { name: '대시보드' })).toBeVisible()
    const width = await page.evaluate(() => ({ body: document.body.scrollWidth, viewport: window.innerWidth }))
    expect(width.body).toBeLessThanOrEqual(width.viewport)
  }
  const menuButton = page.getByRole('button', { name: '메뉴 열기' })
  await expect(menuButton).toHaveAttribute('aria-expanded', 'false')
  await menuButton.click()
  await expect(menuButton).toHaveAttribute('aria-expanded', 'true')
  await expect(page.getByRole('navigation', { name: '주 메뉴' })).toBeVisible()
})

test('administrator can log in, use the application, and log out', async ({ page }) => {
  let authenticated = false
  let logoutCSRF = ''
  await page.route('**/api/**', async route => {
    const request = route.request()
    const path = new URL(request.url()).pathname
    if (path === '/api/setup/status') {
      await route.fulfill({ json: { state: 'ready', administrator_configured: true, claim_required: false, recovery_required: false, auth_disabled: false, version: '1.2.3', release_channel: 'stable' } })
    } else if (path === '/api/auth/session') {
      await route.fulfill({ json: { auth_enabled: true, authenticated, needs_bootstrap: false, csrf_token: authenticated ? 'session-csrf' : '' } })
    } else if (path === '/api/auth/login') {
      expect(request.postDataJSON()).toEqual({ password: 'a-strong-passphrase' })
      authenticated = true
      await route.fulfill({ json: { auth_enabled: true, authenticated: true, needs_bootstrap: false, csrf_token: 'session-csrf' } })
    } else if (path === '/api/auth/logout') {
      logoutCSRF = request.headers()['x-csrf-token'] ?? ''
      authenticated = false
      await route.fulfill({ status: 204, body: '' })
    } else if (path === '/api/dashboard') {
      await route.fulfill({ json: dashboardResponse() })
    } else if (path === '/api/system/storage') {
      await route.fulfill({ json: { filesystem_total_bytes: 0, filesystem_used_bytes: 0, filesystem_available_bytes: 0, recordings_bytes: 0, recording_count: 0, segment_count: 0, init_segment_count: 0, manifest_count: 0 } })
    } else if (path === '/api/adapters') {
      await route.fulfill({ json: [] })
    } else if (path === '/api/resolve-workflows') {
      await route.fulfill({ json: [] })
    } else if (path === '/api/notifications') {
      await route.fulfill({ json: { items: [] } })
    } else if (path === '/api/settings') {
      await route.fulfill({ json: settingsResponse() })
    } else {
      await route.fulfill({ status: 404, json: { error: 'unexpected test request' } })
    }
  })

  await page.goto('/login')
  await page.getByLabel('비밀번호').fill('a-strong-passphrase')
  await page.getByRole('button', { name: '로그인' }).click()
  await expect(page).toHaveURL('/')
  await expect(page.getByRole('heading', { name: '대시보드' })).toBeVisible()
  await page.getByRole('button', { name: /관리자/ }).click()
  await page.getByRole('button', { name: '로그아웃' }).click()
  await expect(page).toHaveURL('/login')
  expect(logoutCSRF).toBe('session-csrf')
})

test('login stays compact at desktop and mobile widths in both locales', async ({ browser }, testInfo) => {
  for (const locale of ['ko-KR', 'en-US'] as const) {
    const context = await browser.newContext({ baseURL: 'http://127.0.0.1:4173', locale })
    const page = await context.newPage()
    await page.route('**/api/**', async route => {
      const path = new URL(route.request().url()).pathname
      if (path === '/api/setup/status') {
        await route.fulfill({ json: { state: 'ready', administrator_configured: true, claim_required: false, recovery_required: false, auth_disabled: false } })
      } else if (path === '/api/auth/session') {
        await route.fulfill({ json: { auth_enabled: true, authenticated: false, needs_bootstrap: false, csrf_token: '' } })
      } else {
        await route.fulfill({ status: 404, json: { error: 'unexpected test request' } })
      }
    })

    for (const viewport of [{ name: 'desktop', width: 1440, height: 900 }, { name: 'mobile', width: 390, height: 844 }]) {
      await page.setViewportSize({ width: viewport.width, height: viewport.height })
      await page.goto('/login')
      await expect(page.locator('html')).toHaveAttribute('lang', locale)
      await expect(page.getByText('Integrated Recorder', { exact: true })).toBeVisible()
      await expect(page.getByRole('heading', { name: locale === 'ko-KR' ? '로그인' : 'Sign in' })).toBeVisible()
      await expect(page.getByLabel(locale === 'ko-KR' ? '비밀번호' : 'Password')).toBeVisible()
      await expect(page.getByRole('button', { name: locale === 'ko-KR' ? '로그인' : 'Sign in' })).toBeVisible()
      expect(await page.locator('body').innerText()).not.toMatch(/Archive Live|Preserve Source|HttpOnly|브라우저에 저장되지 않습니다/)
      const dimensions = await page.evaluate(() => ({ body: document.body.scrollWidth, viewport: window.innerWidth }))
      expect(dimensions.body).toBeLessThanOrEqual(dimensions.viewport)
      await page.screenshot({ path: testInfo.outputPath(`login-${locale}-${viewport.name}.png`), fullPage: true })
    }
    await context.close()
  }
})
