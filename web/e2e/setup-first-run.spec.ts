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

test('fresh installation completes through product APIs and remains ready after Host restart', async ({ page }) => {
  const requests: Array<{ method: string; url: string }> = []
  page.on('request', request => requests.push({ method: request.method(), url: request.url() }))

  const initial = persistedInstallation()
  expect(initial.state).toBe('uninitialized')
  expect(initial.installation_id).toMatch(/^[a-f0-9]{32}$/)
  const installationID = initial.installation_id

  const initialStatusResponse = await page.request.get('/api/setup/status')
  expect(initialStatusResponse.ok()).toBeTruthy()
  const initialStatus = await initialStatusResponse.json()
  expect(initialStatus.state).toBe('uninitialized')
  expect(initialStatus.claim_required).toBe(true)
  expect(initialStatus.recovery_required).toBe(false)

  await page.goto('/')
  await expect(page).toHaveURL(/\/setup$/)
  await expect(page.getByRole('heading', { name: /Integrated Recorder 시작하기/ })).toBeVisible()

  await page.getByRole('button', { name: '시작하기' }).click()
  await page.getByLabel('Setup code').fill(setupCode)
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
  const resumedStatus = await resumedStatusResponse.json()
  expect(resumedStatus.state).toBe('setup_in_progress')
  const resumed = persistedInstallation()
  expect(resumed.state).toBe('setup_in_progress')
  expect(resumed.installation_id).toBe(installationID)
  const resumedSessionResponse = await page.request.get('/api/auth/session')
  expect(resumedSessionResponse.ok()).toBeTruthy()
  expect((await resumedSessionResponse.json()).authenticated).toBe(true)

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

  const requestsAvoidSetupSecrets = requests.every(({ url }) => !url.includes(setupCode!) && !url.includes('bootstrap-token') && !url.includes(dataDir!))
  expect(requestsAvoidSetupSecrets).toBe(true)
  expect(requests.some(({ method, url }) => method === 'POST' && url.endsWith('/api/auth/bootstrap'))).toBe(true)
  expect(requests.some(({ method, url }) => method === 'POST' && url.endsWith('/api/setup/begin'))).toBe(true)
  expect(requests.some(({ method, url }) => method === 'POST' && url.endsWith('/api/setup/storage-test'))).toBe(true)
  expect(requests.some(({ method, url }) => method === 'POST' && url.endsWith('/api/setup/complete'))).toBe(true)
})
