import { expect, test } from '@playwright/test'
import { randomUUID } from 'node:crypto'
import { readFileSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'

const selectedOwnerLogin = 'rc8-owner-test'
const initialPassword = 'A7bc-defghijk-lmnopq'
const dataDir = process.env.IR_SETUP_E2E_DATA_DIR
const restartFile = process.env.IR_SETUP_E2E_RESTART_FILE

if (!dataDir || !restartFile) {
  throw new Error('The real-product setup E2E runner did not provide its private test inputs.')
}

function persistedInstallation() {
  return JSON.parse(readFileSync(join(dataDir!, 'runtime', 'installation.json'), 'utf8')) as {
    installation_id: string
    state: string
  }
}

async function requestHostRestart() {
  const requestID = randomUUID()
  writeFileSync(restartFile!, JSON.stringify({ action: 'restart', request_id: requestID }), { mode: 0o600 })
  await expect.poll(async () => {
    try {
      const response = JSON.parse(readFileSync(`${restartFile}.response`, 'utf8')) as { request_id: string; phase: string }
      return response.request_id === requestID ? response.phase : 'waiting'
    } catch {
      return 'waiting'
    }
  }, { timeout: 90_000, message: 'Runtime Host should restart as a separate product process' }).toBe('ready')

  await expect.poll(async () => {
    try { return (await fetch('http://127.0.0.1:4173/healthz')).ok } catch { return false }
  }, { timeout: 30_000 }).toBe(true)
}

test('setup stepper geometry adapts to locale, color scheme, and viewport', async ({ browser }, testInfo) => {
  const viewports = [
    { name: 'desktop', width: 1440, height: 900 },
    { name: 'mobile', width: 390, height: 844 },
    { name: 'compact-mobile', width: 320, height: 720 },
  ]

  for (const locale of ['en-US', 'ko-KR'] as const) {
    for (const colorScheme of ['light', 'dark'] as const) {
      const context = await browser.newContext({ baseURL: 'http://127.0.0.1:4173', locale, colorScheme })
      const page = await context.newPage()
      await page.goto('/')
      await expect(page).toHaveURL(/\/setup$/)
      await expect(page.locator('html')).toHaveAttribute('lang', locale)
      await expect.poll(() => page.locator('html').evaluate(element => element.classList.contains('dark'))).toBe(colorScheme === 'dark')
      if (locale === 'en-US') {
        await expect(page.getByRole('heading', { name: 'Get started with Integrated Recorder' })).toBeVisible()
        await expect(page.getByText('Complete a few checks before starting archive operations.')).toHaveCount(0)
        expect(await page.locator('body').innerText()).not.toMatch(/[\uac00-\ud7a3]/)
      } else {
        await expect(page.getByRole('heading', { name: 'Integrated Recorder 시작하기' })).toBeVisible()
        await expect(page.getByText('몇 가지 확인을 마치면 보관 작업을 시작할 수 있습니다.')).toHaveCount(0)
        expect(await page.locator('body').innerText()).toMatch(/[\uac00-\ud7a3]/)
      }

      await page.getByRole('button', { name: locale === 'en-US' ? 'Get started' : '시작하기' }).click()
      const stepper = page.getByRole('navigation', { name: locale === 'en-US' ? 'Setup steps' : '설치 단계' })
      await expect(stepper.locator('[aria-current="step"] [data-step-indicator] > span:last-child')).toHaveText(locale === 'en-US' ? 'Administrator' : '관리자')
      await expect(stepper.locator('li[data-step-state="complete"]')).toHaveCount(1)
      await expect(stepper.locator('[data-step-connector="true"]')).toHaveCount(4)

      for (const viewport of viewports) {
        await page.setViewportSize({ width: viewport.width, height: viewport.height })
        const geometry = await stepper.evaluate(element => {
          const markers = Array.from(element.querySelectorAll<HTMLElement>('[data-step-marker="true"]'))
          const connectors = Array.from(element.querySelectorAll<HTMLElement>('[data-step-connector="true"]'))
          return {
            nav: element.getBoundingClientRect().toJSON(),
            markers: markers.map(marker => {
              const rect = marker.getBoundingClientRect()
              return { left: rect.left, right: rect.right, centerY: rect.top + rect.height / 2, zIndex: getComputedStyle(marker).zIndex, background: getComputedStyle(marker).backgroundColor }
            }),
            connectors: connectors.map(connector => {
              const rect = connector.getBoundingClientRect()
              return { left: rect.left, right: rect.right, centerY: rect.top + rect.height / 2, height: rect.height, zIndex: getComputedStyle(connector).zIndex }
            }),
          }
        })
        expect(geometry.markers).toHaveLength(5)
        expect(geometry.connectors).toHaveLength(4)
        expect(geometry.nav.left).toBeGreaterThanOrEqual(0)
        expect(geometry.nav.right).toBeLessThanOrEqual(viewport.width)
        for (let index = 0; index < geometry.connectors.length; index += 1) {
          const marker = geometry.markers[index]
          const nextMarker = geometry.markers[index + 1]
          const connector = geometry.connectors[index]
          expect(connector.left).toBeCloseTo(marker.right, 0)
          expect(connector.right).toBeCloseTo(nextMarker.left, 0)
          expect(Math.abs(connector.centerY - marker.centerY)).toBeLessThanOrEqual(0.5)
          expect(connector.height).toBe(1)
          expect(Number(connector.zIndex)).toBeLessThan(Number(marker.zIndex))
          expect(marker.background).not.toBe('rgba(0, 0, 0, 0)')
          expect(marker.background).not.toBe('transparent')
        }
        expect(geometry.markers.every((marker, index) => index === 0 || marker.left > geometry.markers[index - 1].left)).toBe(true)
        const dimensions = await page.evaluate(() => ({ body: document.body.scrollWidth, viewport: window.innerWidth }))
        expect(dimensions.body).toBeLessThanOrEqual(dimensions.viewport)
        await page.screenshot({ path: testInfo.outputPath(`setup-${locale}-${colorScheme}-${viewport.name}.png`) })
      }
      await context.close()
    }
  }
})

test('fresh installation completes through product APIs and remains ready after Host restart', async ({ browser }) => {
  const context = await browser.newContext({ baseURL: 'http://127.0.0.1:4173', locale: 'ko-KR' })
  const page = await context.newPage()
  let requestURLLeak = false
  let bootstrapLogin = ''
  let bootstrapHasOnlyCredentials = false
  let bootstrapResponseStatus = 0
  let bootstrapResponseBody = ''
  let bootstrapRequestOrigin = ''
  let bootstrapRequestHost = ''
  const requestChecks: Promise<void>[] = []
  let responseBodyLeak = false
  const responseChecks: Promise<void>[] = []
  page.on('request', request => {
    const url = request.url()
    if (url.includes(dataDir!)) requestURLLeak = true
    if (request.method() === 'POST' && new URL(url).pathname === '/api/auth/bootstrap') {
      const payload = request.postDataJSON() as Record<string, unknown> | null
      bootstrapLogin = String(payload?.login ?? '')
      bootstrapHasOnlyCredentials = Boolean(payload && Object.keys(payload).sort().join(',') === 'login,password')
      requestChecks.push(request.allHeaders().then(headers => {
        bootstrapRequestOrigin = headers.origin ?? ''
        bootstrapRequestHost = new URL(request.url()).host
      }))
    }
  })
  page.on('response', response => {
    if (new URL(response.url()).pathname === '/api/auth/bootstrap') {
      bootstrapResponseStatus = response.status()
      responseChecks.push(response.text().then(body => { bootstrapResponseBody = body }).catch(() => undefined))
    }
    responseChecks.push(response.body().then(body => {
      if (body.includes(dataDir!)) responseBodyLeak = true
    }).catch(() => undefined))
  })

  const initial = persistedInstallation()
  expect(initial.state).toBe('uninitialized')
  expect(initial.installation_id).toMatch(/^[a-f0-9]{32}$/)
  const installationID = initial.installation_id

  const initialStatusResponse = await page.request.get('/api/setup/status')
  expect(initialStatusResponse.ok()).toBeTruthy()
  const initialStatusBody = await initialStatusResponse.text()
  const initialStatus = JSON.parse(initialStatusBody)
  expect(initialStatus.state).toBe('uninitialized')
  expect(initialStatus.recovery_required).toBe(false)

  await page.goto('/')
  await expect(page).toHaveURL(/\/setup$/)
  await expect(page.getByRole('heading', { name: 'Integrated Recorder 시작하기' })).toBeVisible()

  await page.getByRole('button', { name: '시작하기' }).click()
  const setupUsername = page.getByLabel('계정')
  const setupPassword = page.getByLabel('관리자 비밀번호')
  const setupConfirmation = page.getByLabel('비밀번호 확인')
  const ownerForm = setupUsername.locator('xpath=ancestor::form')
  await expect(ownerForm).toHaveAttribute('id', 'owner-bootstrap-form')
  await expect(ownerForm).toHaveAttribute('name', 'owner-bootstrap')
  await expect(ownerForm).toHaveAttribute('method', 'post')
  await expect(ownerForm).toHaveAttribute('autocomplete', 'on')
  await expect(setupUsername).toHaveAttribute('name', 'username')
  await expect(setupUsername).toHaveAttribute('id', 'setup-username')
  await expect(setupUsername).toHaveAttribute('autocomplete', 'username')
  await expect(setupUsername).not.toHaveAttribute('readonly')
  await expect(setupPassword).toHaveAttribute('name', 'password')
  await expect(setupPassword).toHaveAttribute('id', 'setup-password')
  await expect(setupPassword).toHaveAttribute('autocomplete', 'new-password')
  await expect(setupConfirmation).toHaveAttribute('name', 'password_confirmation')
  await expect(setupConfirmation).toHaveAttribute('id', 'setup-password-confirm')
  await expect(setupConfirmation).toHaveAttribute('autocomplete', 'new-password')
  for (const [field, value] of [[setupUsername, selectedOwnerLogin], [setupPassword, initialPassword], [setupConfirmation, initialPassword]] as const) {
    await field.evaluate((element, nativeValue) => {
      const setter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')?.set
      if (!setter) throw new Error('native input value setter is unavailable')
      setter.call(element as HTMLInputElement, nativeValue)
    }, value)
  }
  const submittedFormValues = await ownerForm.evaluate(form => {
    const data = new FormData(form as HTMLFormElement)
    return ['username', 'password', 'password_confirmation'].map(name => data.get(name))
  })
  expect(submittedFormValues).toEqual([selectedOwnerLogin, initialPassword, initialPassword])
  await page.getByRole('button', { name: '관리자 계정 만들기' }).click()

  await expect.poll(() => bootstrapResponseStatus).not.toBe(0)
  await Promise.all([...requestChecks, ...responseChecks])
  expect(bootstrapResponseStatus, `bootstrap API response ${bootstrapResponseStatus}: ${bootstrapResponseBody}; origin=${bootstrapRequestOrigin}; host=${bootstrapRequestHost}`).toBe(200)
  await expect(page.getByRole('heading', { name: '저장소 확인' })).toBeVisible()
  const setupInProgress = persistedInstallation()
  expect(setupInProgress.state).toBe('setup_in_progress')
  expect(setupInProgress.installation_id).toBe(installationID)
  await requestHostRestart()
  await page.reload()
  await expect(page.getByRole('heading', { name: '저장소 확인' })).toBeVisible()
  const resumedStatusResponse = await page.request.get('/api/setup/status')
  expect(resumedStatusResponse.ok()).toBeTruthy()
  const resumedStatusBody = await resumedStatusResponse.text()
  const resumedStatus = JSON.parse(resumedStatusBody)
  expect(resumedStatus.state).toBe('setup_in_progress')
  const resumed = persistedInstallation()
  expect(resumed.state).toBe('setup_in_progress')
  expect(resumed.installation_id).toBe(installationID)
  const resumedSessionResponse = await page.request.get('/api/auth/session')
  expect(resumedSessionResponse.ok()).toBeTruthy()
  const resumedSessionBody = await resumedSessionResponse.text()
  expect(JSON.parse(resumedSessionBody).authenticated).toBe(true)

  await page.getByRole('button', { name: '저장소 검사 실행' }).click()
  await expect(page.getByText('기본 저장소 진단')).toBeVisible()
  await expect(page.getByText('쓰기 테스트')).toBeVisible()
  await expect(page.getByText('내구성 테스트')).toBeVisible()
  await page.getByRole('button', { name: '계속' }).click()

  await expect(page.getByRole('heading', { name: '어댑터 확인' })).toBeVisible()
  const adapterResponse = await page.request.get('/api/adapters')
  expect(adapterResponse.ok()).toBeTruthy()
  const adapters = await adapterResponse.json() as Array<{ descriptor?: { id?: string }; status?: { id?: string } }>
  expect(adapters.some(adapter => adapter.descriptor?.id === 'hls' || adapter.status?.id === 'hls')).toBe(true)
  await page.getByRole('button', { name: '계속' }).click()

  await expect(page.getByRole('heading', { name: '마지막으로 확인' })).toBeVisible()
  await page.getByRole('button', { name: '설치 완료' }).click()
  await expect(page.getByRole('heading', { name: 'Integrated Recorder가 준비되었습니다' })).toBeVisible()
  await page.getByRole('button', { name: 'Recorder 열기' }).click()
  await expect(page).toHaveURL('http://127.0.0.1:4173/')
  await expect(page.getByRole('heading', { name: '대시보드' })).toBeVisible()
  const initialOwner = await page.request.get('/api/user').then(response => response.json()) as { user_id: string; login: string; roles: string[] }
  expect(initialOwner.login).toBe(selectedOwnerLogin)
  expect(initialOwner.roles).toContain('owner')
  expect(bootstrapLogin).toBe(selectedOwnerLogin)

  const dashboardResponse = await page.request.get('/api/dashboard')
  expect(dashboardResponse.ok()).toBeTruthy()
  const readyResponse = await page.request.get('/api/setup/status')
  expect(readyResponse.ok()).toBeTruthy()
  const readyStatus = await readyResponse.json()
  expect(readyStatus.state).toBe('ready')
  const readyOnDisk = persistedInstallation()
  expect(readyOnDisk.state).toBe('ready')
  expect(readyOnDisk.installation_id).toBe(installationID)

  await requestHostRestart()
  const afterRestartStatusResponse = await page.request.get('/api/setup/status')
  expect(afterRestartStatusResponse.ok()).toBeTruthy()
  const afterRestartStatus = await afterRestartStatusResponse.json()
  expect(afterRestartStatus.state).toBe('ready')
  const afterRestart = persistedInstallation()
  expect(afterRestart.state).toBe('ready')
  expect(afterRestart.installation_id).toBe(installationID)

  const sessionResponse = await page.request.get('/api/auth/session')
  expect(sessionResponse.ok()).toBeTruthy()
  expect((await sessionResponse.json()).authenticated).toBe(true)

  await page.goto('/setup')
  await expect(page).toHaveURL('http://127.0.0.1:4173/')
  await expect(page.getByRole('heading', { name: '대시보드' })).toBeVisible()
  const postRestartDashboard = await page.request.get('/api/dashboard')
  expect(postRestartDashboard.ok()).toBeTruthy()

  // The setup-selected identity is the browser credential pair after the
  // installation is complete; login must not rely on the old literal owner.
  const initialSession = await page.request.get('/api/auth/session').then(response => response.json()) as { csrf_token: string }
  const logout = await page.request.post('/api/auth/logout', { headers: { 'X-CSRF-Token': initialSession.csrf_token }, data: {} })
  expect(logout.status()).toBe(204)
  await page.goto('/login')
  await page.getByLabel('계정 이름').fill(selectedOwnerLogin)
  await page.getByLabel('비밀번호').fill(initialPassword)
  await page.getByRole('button', { name: '로그인' }).click()
  await expect(page).toHaveURL('/')
  expect((await page.request.get('/api/user').then(response => response.json())).login).toBe(selectedOwnerLogin)
  const blankLogin = await page.request.post('/api/auth/login', { data: { login: '', password: initialPassword } })
  expect(blankLogin.status()).toBe(401)

  // Exercise self-service login rename and its session invalidation through
  // the native account form. The old login is rejected after the redirect.
  await page.goto('/account/security')
  await page.getByLabel('새 로그인 이름').fill('rc8-owner-renamed')
  await page.getByLabel('현재 비밀번호').first().fill(initialPassword)
  await page.getByRole('button', { name: '로그인 이름 변경' }).click()
  await expect(page).toHaveURL('/login')
  expect((await page.request.post('/api/auth/login', { data: { login: selectedOwnerLogin, password: initialPassword } })).status()).toBe(401)
  await page.getByLabel('계정 이름').fill('rc8-owner-renamed')
  await page.getByLabel('비밀번호').fill(initialPassword)
  await page.getByRole('button', { name: '로그인' }).click()
  await expect(page).toHaveURL('/')
  const renamedOwner = await page.request.get('/api/user').then(response => response.json()) as { user_id: string; login: string }
  expect(renamedOwner).toMatchObject({ user_id: initialOwner.user_id, login: 'rc8-owner-renamed' })

  // A second account is created through the real account-management form.
  await page.goto('/system/users')
  await page.getByLabel('로그인 이름').fill('rc8-operator-test')
  await page.getByLabel('초기 비밀번호').fill('rc8-operator-password-2026')
  await page.getByLabel('비밀번호 확인').last().fill('rc8-operator-password-2026')
  await page.getByRole('button', { name: '사용자 생성' }).click()
  await expect(page.getByText(/rc8-operator-test/)).toBeVisible()
  const operator = await page.request.get('/api/users').then(response => response.json()) as { items: { user_id: string; login: string; roles: string[] }[] }
  const operatorUser = operator.items.find(user => user.login === 'rc8-operator-test')!
  expect(operatorUser.roles).toContain('operator')
  const ownerSession = await page.request.get('/api/auth/session').then(response => response.json()) as { csrf_token: string }
  const ownerUserID = renamedOwner.user_id
  const ownerCannotDemoteSelf = await page.request.patch(`/api/users/${ownerUserID}/roles`, {
    headers: { 'X-CSRF-Token': ownerSession.csrf_token }, data: { roles: ['operator'] },
  })
  expect(ownerCannotDemoteSelf.status()).toBe(409)

  const operatorContext = await browser.newContext({ baseURL: 'http://127.0.0.1:4173', locale: 'ko-KR' })
  const operatorPage = await operatorContext.newPage()
  await operatorPage.goto('/login')
  await operatorPage.getByLabel('계정 이름').fill('rc8-operator-test')
  await operatorPage.getByLabel('비밀번호').fill('rc8-operator-password-2026')
  await operatorPage.getByRole('button', { name: '로그인' }).click()
  await expect(operatorPage).toHaveURL('/')
  expect((await operatorPage.request.get('/api/dashboard')).status()).toBe(200)
  expect((await operatorPage.request.get('/api/users')).status()).toBe(403)
  const operatorSession = await operatorPage.request.get('/api/auth/session').then(response => response.json()) as { csrf_token: string }
  const recordingAdmission = await operatorPage.request.post('/api/recordings', { headers: { 'X-CSRF-Token': operatorSession.csrf_token }, data: {} })
  expect(recordingAdmission.status()).toBe(400)

  // Custom permissions are resolved from durable role state on each request.
  const createdRoleResponse = await page.request.post('/api/roles', {
    headers: { 'X-CSRF-Token': ownerSession.csrf_token },
    data: { name: 'Diagnostics reader', permissions: ['system.diagnostics.read'] },
  })
  expect(createdRoleResponse.status()).toBe(201)
  const customRole = await createdRoleResponse.json() as { id: string }
  const roleAssignResponse = await page.request.patch(`/api/users/${operatorUser.user_id}/roles`, {
    headers: { 'X-CSRF-Token': ownerSession.csrf_token },
    data: { roles: ['viewer', customRole.id] },
  })
  expect(roleAssignResponse.ok()).toBeTruthy()
  await operatorContext.close()
  const assignedOperatorContext = await browser.newContext({ baseURL: 'http://127.0.0.1:4173', locale: 'ko-KR' })
  const assignedOperatorPage = await assignedOperatorContext.newPage()
  await assignedOperatorPage.goto('/login')
  await assignedOperatorPage.getByLabel('계정 이름').fill('rc8-operator-test')
  await assignedOperatorPage.getByLabel('비밀번호').fill('rc8-operator-password-2026')
  await assignedOperatorPage.getByRole('button', { name: '로그인' }).click()
  await expect(assignedOperatorPage).toHaveURL('/')
  const beforeRoleEdit = await assignedOperatorPage.request.get('/api/user/authorization').then(response => response.json()) as { roles: string[]; permissions: string[] }
  expect(beforeRoleEdit.roles).toEqual(expect.arrayContaining(['viewer', customRole.id]))
  expect(beforeRoleEdit.permissions).toContain('system.diagnostics.read')
  expect((await assignedOperatorPage.request.get('/api/logs?limit=1')).status()).toBe(200)
  const updateRoleResponse = await page.request.put(`/api/roles/${customRole.id}`, {
    headers: { 'X-CSRF-Token': ownerSession.csrf_token },
    data: { name: 'Diagnostics reader', permissions: ['storage.read'] },
  })
  expect(updateRoleResponse.ok()).toBeTruthy()
  const afterRoleEdit = await assignedOperatorPage.request.get('/api/user/authorization').then(response => response.json()) as { roles: string[]; permissions: string[] }
  expect(afterRoleEdit.roles).toEqual(expect.arrayContaining(['viewer', customRole.id]))
  expect(afterRoleEdit.permissions).not.toContain('system.diagnostics.read')
  expect(afterRoleEdit.permissions).toContain('storage.read')
  expect((await assignedOperatorPage.request.get('/api/logs?limit=1')).status()).toBe(403)
  expect((await assignedOperatorPage.request.get('/api/system/storage')).status()).toBe(200)
  await assignedOperatorContext.close()

  const changedPassword = 'first-run-product-e2e-password-changed-2026'
  await page.goto('/account/security')
  await page.getByLabel('현재 비밀번호').last().fill(initialPassword)
  await page.getByLabel('새 비밀번호', { exact: true }).fill(changedPassword)
  await page.getByLabel('새 비밀번호 확인').fill(changedPassword)
  await page.getByRole('button', { name: '비밀번호 변경' }).last().click()
  await expect(page).toHaveURL('/login')
  expect((await page.request.post('/api/auth/login', { data: { login: 'rc8-owner-renamed', password: initialPassword } })).status()).toBe(401)
  await page.getByLabel('계정 이름').fill('rc8-owner-renamed')
  await page.getByLabel('비밀번호').fill(changedPassword)
  await page.getByRole('button', { name: '로그인' }).click()
  await expect(page).toHaveURL('/')

  await Promise.all(responseChecks)
  expect(requestURLLeak).toBe(false)
  expect(responseBodyLeak).toBe(false)
  expect(bootstrapHasOnlyCredentials).toBe(true)
  expect(bootstrapLogin).toBe(selectedOwnerLogin)
  await context.close()
})
