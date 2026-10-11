import { expect, test, type Page } from '@playwright/test'
import { readFileSync } from 'node:fs'
import { join } from 'node:path'

const dataDir = process.env.IR_E2E_DATA_DIR
if (!dataDir) throw new Error('IR_E2E_DATA_DIR was not provided by Playwright config')

// The test selectors below intentionally exercise the Korean catalog while
// the user's saved preference is `system`.
test.use({ locale: 'ko-KR' })

test('actual Go backend: Watch detects live, records segments, and resumes monitoring', async ({ page }) => {
  const sourceURL = readFileSync(join(dataDir!, 'e2e-source-url'), 'utf8').trim()
  const consoleErrors: string[] = []
  const pageErrors: string[] = []
  const watchCreateFailures: string[] = []
  let watchCreateBody: Record<string, unknown> | undefined
  page.on('console', message => { if (message.type() === 'error') consoleErrors.push(message.text()) })
  page.on('pageerror', error => pageErrors.push(error.message))
  page.on('response', async response => {
    if (response.request().method() !== 'POST' || !response.url().endsWith('/api/watches')) return
    if (response.ok()) return
    watchCreateFailures.push(`HTTP ${response.status()}: ${await response.text()}`)
  })
  page.on('request', request => {
    if (request.method() === 'POST' && new URL(request.url()).pathname === '/api/watches') {
      watchCreateBody = request.postDataJSON() as Record<string, unknown>
    }
  })

  await login(page)
  await page.goto('/watches/new')
  await expect(page.getByRole('heading', { name: 'Watch 등록' })).toBeVisible()
  await page.getByRole('button', { name: /Workflow Fixture/ }).click()
  await page.getByLabel('Fixture source URL').fill(sourceURL)
  await page.getByLabel('녹화 제목').fill('Browser E2E automatic Watch')
  await page.getByLabel('방송 확인 주기 (초)').fill('2')
  const previewToggle = page.getByRole('checkbox', { name: '장면 미리보기 생성' })
  await expect(previewToggle).toBeChecked()
  await page.getByRole('button', { name: '자동 녹화 등록' }).click()
  await expect.poll(() => watchCreateFailures.length > 0 || /\/watches\/[a-f0-9]{32}$/.test(page.url()), { timeout: 10_000 }).toBe(true)
  if (watchCreateFailures.length) throw new Error(`Watch creation failed: ${watchCreateFailures.join('; ')}`)
  expect(watchCreateBody).toMatchObject({ preview_mode: 'segment' })
  await expect(page).toHaveURL(/\/watches\/[a-f0-9]{32}$/)
  const watchID = page.url().split('/').at(-1)!
  await expect(page.getByRole('heading', { name: 'Browser E2E automatic Watch' })).toBeVisible()

  await expect.poll(async () => (await getJSON<WatchWire>(page, `/api/watches/${watchID}`)).state, { timeout: 15_000 }).toBe('offline')
  expect((await getJSON<WatchWire>(page, `/api/watches/${watchID}`)).input_secret_configured).toEqual({})

  await setSource(page, sourceURL, true, false)
  await expect.poll(async () => (await getJSON<WatchWire>(page, `/api/watches/${watchID}`)).state, { timeout: 20_000 }).toBe('recording')
  const liveWatch = await getJSON<WatchWire>(page, `/api/watches/${watchID}`)
  expect(liveWatch.current_recording_id).toMatch(/^[a-f0-9]{32}$/)
  const recordingID = liveWatch.current_recording_id!
  await expect.poll(async () => page.getByText('녹화 중', { exact: true }).first().isVisible(), { timeout: 10_000 }).toBe(true)
  await expect.poll(async () => segmentCount(await getJSON<RecordingWire>(page, `/api/recordings/${recordingID}`)), { timeout: 20_000 }).toBeGreaterThan(0)
  const activeRecording = await getJSON<RecordingWire>(page, `/api/recordings/${recordingID}`)
  expect(activeRecording.state).toBe('recording')
  expect(segmentCount(activeRecording)).toBeGreaterThan(0)

  await setSource(page, sourceURL, false, true)
  await expect.poll(async () => (await getJSON<WatchWire>(page, `/api/watches/${watchID}`)).state, { timeout: 30_000 }).toBe('offline')
  const endedWatch = await getJSON<WatchWire>(page, `/api/watches/${watchID}`)
  expect(endedWatch.current_recording_id).toBeFalsy()
  const retainedRecording = await getJSON<RecordingWire>(page, `/api/recordings/${recordingID}`)
  expect(['completed', 'stopped']).toContain(retainedRecording.state)
  expect(segmentCount(retainedRecording)).toBeGreaterThan(0)
  const history = await getJSON<{ items: { id: string }[] }>(page, `/api/watches/${watchID}/recordings?limit=20`)
  expect(history.items.some(item => item.id === recordingID)).toBe(true)

  await page.goto('/watches')
  await expect(page.getByRole('heading', { name: '자동 녹화' })).toBeVisible()
  await expect(page.getByText('Browser E2E automatic Watch')).toBeVisible()
  await expect(page.getByText('오프라인', { exact: true }).first()).toBeVisible()
  expect(consoleErrors).toEqual([])
  expect(pageErrors).toEqual([])
})

type WatchWire = {
  id: string
  state: string
  current_recording_id?: string
  input_secret_configured: Record<string, boolean>
}
type RecordingWire = {
  id: string
  state: string
  segment_count?: number
  tracks?: Record<string, { segments?: unknown[] }>
}
type SessionWire = { needs_bootstrap: boolean; authenticated?: boolean }

async function login(page: Page) {
  await page.goto('/login')
  const session = await getJSON<SessionWire>(page, '/api/auth/session')
  if (session.authenticated) {
    await page.goto('/')
    return
  }
  if (session.needs_bootstrap) {
    const token = readFileSync(join(dataDir!, 'security', 'bootstrap-token'), 'utf8').trim()
    await page.goto('/login?mode=bootstrap')
    await page.getByLabel('초기화 토큰').fill(token)
    await page.getByLabel('관리자 비밀번호').fill('browser-e2e-strong-password')
    await page.getByLabel('비밀번호 확인').fill('browser-e2e-strong-password')
    await page.getByRole('button', { name: '서버 초기화' }).click()
  } else {
    await page.getByLabel('관리자 비밀번호').fill('browser-e2e-strong-password')
    await page.getByRole('button', { name: '로그인' }).click()
  }
  await expect(page).toHaveURL('/')
}

async function setSource(page: Page, sourceURL: string, online: boolean, endlist: boolean) {
  const response = await page.request.get(`${sourceURL}/e2e/set-online?value=${online}&endlist=${endlist}`)
  expect(response.status()).toBe(204)
}

async function getJSON<T>(page: Page, path: string): Promise<T> {
  const response = await page.request.get(path)
  if (!response.ok()) throw new Error(`GET ${path}: ${response.status()}`)
  return await response.json() as T
}

function segmentCount(recording: RecordingWire): number {
  if (typeof recording.segment_count === 'number') return recording.segment_count
  return Object.values(recording.tracks ?? {}).reduce((count, track) => count + (track.segments?.length ?? 0), 0)
}
