import { expect, test } from '@playwright/test'
import { randomUUID } from 'node:crypto'
import { readFileSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'

const setupCode = process.env.IR_SETUP_E2E_CODE
const dataDir = process.env.IR_SETUP_E2E_DATA_DIR
const restartFile = process.env.IR_SETUP_E2E_RESTART_FILE

if (!setupCode || !dataDir || !restartFile) {
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

test('fresh setup resolves browser locale before authentication', async ({ browser }) => {
  for (const locale of ['en-US', 'ko-KR'] as const) {
    const context = await browser.newContext({ baseURL: 'http://127.0.0.1:4173', locale })
    const page = await context.newPage()
    await page.goto('/')
    await expect(page).toHaveURL(/\/setup$/)
    await expect(page.locator('html')).toHaveAttribute('lang', locale)
    if (locale === 'en-US') {
      await expect(page.getByRole('heading', { name: 'Get started with Integrated Recorder' })).toBeVisible()
      expect(await page.locator('body').innerText()).not.toMatch(/[\uac00-\ud7a3]/)
    } else {
      await expect(page.getByRole('heading', { name: 'Integrated Recorder 시작하기' })).toBeVisible()
      expect(await page.locator('body').innerText()).toMatch(/[\uac00-\ud7a3]/)
    }
    await context.close()
  }
})

test('fresh installation completes through product APIs and remains ready after Host restart', async ({ browser }) => {
  const context = await browser.newContext({ baseURL: 'http://127.0.0.1:4173', locale: 'ko-KR' })
  const page = await context.newPage()
  let requestURLLeak = false
  let disallowedRequestBodyLeak = false
  let bootstrapBodyContainsCode = false
  let responseBodyLeak = false
  const responseChecks: Promise<void>[] = []
  page.on('request', request => {
    const url = request.url()
    if (url.includes(setupCode!) || url.includes('bootstrap-token') || url.includes(dataDir!)) requestURLLeak = true
    const body = request.postData() ?? ''
    if (request.method() === 'POST' && new URL(url).pathname === '/api/auth/bootstrap') {
      bootstrapBodyContainsCode ||= body.includes(setupCode!)
    } else if (body.includes(setupCode!)) {
      disallowedRequestBodyLeak = true
    }
  })
  page.on('response', response => {
    responseChecks.push(response.body().then(body => {
      if (body.toString('utf8').includes(setupCode!)) responseBodyLeak = true
    }).catch(() => undefined))
  })

  const initial = persistedInstallation()
  expect(initial.state).toBe('uninitialized')
  expect(initial.installation_id).toMatch(/^[a-f0-9]{32}$/)
  const installationID = initial.installation_id

  const initialStatusResponse = await page.request.get('/api/setup/status')
  expect(initialStatusResponse.ok()).toBeTruthy()
  const initialStatusBody = await initialStatusResponse.text()
  expect(initialStatusBody.includes(setupCode!)).toBe(false)
  const initialStatus = JSON.parse(initialStatusBody)
  expect(initialStatus.state).toBe('uninitialized')
  expect(initialStatus.claim_required).toBe(true)
  expect(initialStatus.recovery_required).toBe(false)

  await page.goto('/')
  await expect(page).toHaveURL(/\/setup$/)
  await expect(page.getByRole('heading', { name: 'Integrated Recorder 시작하기' })).toBeVisible()

  await page.getByRole('button', { name: '시작하기' }).click()
  await page.getByLabel('설치 코드').fill(setupCode)
  expect((await page.locator('body').innerText()).includes(setupCode!)).toBe(false)
  await page.getByLabel('관리자 비밀번호').fill('first-run-product-e2e-password-2026')
  await page.getByLabel('비밀번호 확인').fill('first-run-product-e2e-password-2026')
  await page.getByRole('button', { name: '관리자 계정 만들기' }).click()

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
  expect(resumedStatusBody.includes(setupCode!)).toBe(false)
  const resumedStatus = JSON.parse(resumedStatusBody)
  expect(resumedStatus.state).toBe('setup_in_progress')
  const resumed = persistedInstallation()
  expect(resumed.state).toBe('setup_in_progress')
  expect(resumed.installation_id).toBe(installationID)
  const resumedSessionResponse = await page.request.get('/api/auth/session')
  expect(resumedSessionResponse.ok()).toBeTruthy()
  const resumedSessionBody = await resumedSessionResponse.text()
  expect(resumedSessionBody.includes(setupCode!)).toBe(false)
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

  const dashboardResponse = await page.request.get('/api/dashboard')
  expect(dashboardResponse.ok()).toBeTruthy()
  const readyResponse = await page.request.get('/api/setup/status')
  expect(readyResponse.ok()).toBeTruthy()
  const readyStatus = await readyResponse.json()
  expect(readyStatus.state).toBe('ready')
  expect(readyStatus.claim_required).toBe(false)
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

  expect((await page.locator('body').innerText()).includes(setupCode!)).toBe(false)
  await Promise.all(responseChecks)
  expect(requestURLLeak).toBe(false)
  expect(disallowedRequestBodyLeak).toBe(false)
  expect(bootstrapBodyContainsCode).toBe(true)
  expect(responseBodyLeak).toBe(false)
  await context.close()
})
