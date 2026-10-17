import { expect, test, type Page } from '@playwright/test'
import { readFileSync } from 'node:fs'
import { join } from 'node:path'

const dataDir = process.env.IR_E2E_DATA_DIR
if (!dataDir) throw new Error('IR_E2E_DATA_DIR was not provided by Playwright config')

type TimedEventWire = { event_archive_ordinal: number; event_type: string; source_event_id?: string; presentation_offset_ms: number; message?: { text?: string } }
type TimedEventPageWire = { items: TimedEventWire[]; archive_revision: number; timeline_revision: number }

async function timedEventPage(page: Page, recordingID: string) {
  const response = await page.request.get(`/api/recordings/${encodeURIComponent(recordingID)}/timed-events?limit=100&tail=1`)
  expect(response.status()).toBe(200)
  return response.json() as Promise<TimedEventPageWire>
}

test('actual adapter child streams, reconnects, archives, and replays generic timed events', async ({ page }) => {
  test.setTimeout(120_000)
  const sourceURL = readFileSync(join(dataDir, 'e2e-source-url'), 'utf8').trim()
  await page.goto('/login')
  await page.getByLabel(/Account name|계정 이름/).fill('owner')
  await page.getByLabel(/Password|비밀번호/).fill('browser-e2e-strong-password')
  await page.getByRole('button', { name: /Sign in|로그인/ }).click()
  await expect(page).toHaveURL('/')

  await page.goto('/new')
  await page.getByRole('button', { name: /Timed Event Fixture/ }).click()
  await page.getByRole('button', { name: /Next|다음/ }).click()
  await page.getByLabel(/Fixture source URL/).fill(sourceURL)
  await page.getByRole('checkbox').uncheck()
  await page.getByRole('button', { name: /Next|다음/ }).click()
  await page.getByRole('button', { name: /Start recording|녹화 시작/ }).click()
  await expect(page).toHaveURL(/\/recordings\/[^/]+$/)
  const recordingID = page.url().split('/').at(-1)!

  await page.getByRole('tab', { name: /Events|이벤트/ }).click()
  await expect.poll(async () => (await timedEventPage(page, recordingID)).items.length, { timeout: 20_000 }).toBe(10)
  const beforeProcessRestart = await timedEventPage(page, recordingID)
  expect(beforeProcessRestart.archive_revision).toBeGreaterThanOrEqual(4)
  expect(beforeProcessRestart.timeline_revision).toBeGreaterThan(0)
  expect(beforeProcessRestart.items.filter(item => item.source_event_id === 'fixture-message-1')).toHaveLength(1)
  expect(beforeProcessRestart.items.filter(item => item.event_type === 'source.sequence')).toHaveLength(1)
  expect(beforeProcessRestart.items.filter(item => item.event_type === 'custom.example')).toHaveLength(1)
  expect(beforeProcessRestart.items.filter(item => item.message?.text === 'source-less chat message')).toHaveLength(1)
  expect(beforeProcessRestart.items.map(item => item.event_type)).toEqual(expect.arrayContaining([
    'chat.message', 'chat.edit', 'chat.delete', 'chat.system', 'support.donation', 'support.subscription', 'custom.example',
  ]))
  await expect(page.getByText('fixture chat message')).toBeVisible()
  await expect(page.getByText('source-less chat message')).toBeVisible()
  await expect(page.getByText('custom.example')).toBeVisible()
  await expect(page.getByRole('button', { name: /fixture chat message/ })).toBeDisabled()

  const adapterBefore = await page.request.get('/api/adapters/timed-event-fixture')
  expect(adapterBefore.status()).toBe(200)
  const adapterGenerationBefore = (await adapterBefore.json() as { status: { generation: number } }).status.generation
  const auth = await page.request.get('/api/auth/session')
  expect(auth.status()).toBe(200)
  const csrfToken = (await auth.json() as { csrf_token: string }).csrf_token
  const restarted = await page.request.post('/api/adapters/timed-event-fixture/restart', { headers: { 'X-CSRF-Token': csrfToken }, data: {} })
  expect(restarted.status()).toBe(200)
  expect((await restarted.json() as { status: { generation: number } }).status.generation).toBeGreaterThan(adapterGenerationBefore)
  await expect.poll(async () => (await timedEventPage(page, recordingID)).items.length, { timeout: 20_000 }).toBe(11)
  const afterProcessRestart = await timedEventPage(page, recordingID)
  expect(afterProcessRestart.items.filter(item => item.source_event_id === 'fixture-message-1')).toHaveLength(1)
  expect(afterProcessRestart.items.filter(item => item.event_type === 'source.sequence')).toHaveLength(1)
  expect(afterProcessRestart.items.filter(item => item.source_event_id === 'fixture-message-3')).toHaveLength(1)

  await page.getByRole('button', { name: /Stop|중지/ }).first().click()
  await page.getByRole('alertdialog').getByRole('button', { name: /Stop|중지/ }).click()
  await expect.poll(async () => (await page.request.get(`/api/recordings/${encodeURIComponent(recordingID)}`)).json().then((body: { state: string }) => body.state)).toBe('stopped')
  await expect.poll(async () => page.locator('#vod-player video').evaluate(video => video.readyState)).toBeGreaterThan(0)
  const seekEvent = page.getByRole('button', { name: /fixture chat message/ })
  await expect(seekEvent).toBeEnabled()
  const offsetSeconds = beforeProcessRestart.items.find(item => item.source_event_id === 'fixture-message-1')!.presentation_offset_ms / 1000
  await page.locator('#vod-player video').evaluate(video => { (video as HTMLVideoElement).currentTime = 1 })
  await seekEvent.click()
  await expect.poll(async () => page.locator('#vod-player video').evaluate(video => (video as HTMLVideoElement).currentTime)).toBeCloseTo(offsetSeconds, 1)

  const archive = await page.request.get(`/api/recordings/${encodeURIComponent(recordingID)}/archive/index`)
  expect(archive.status()).toBe(200)
  const archiveBody = await archive.json() as { entries: { path: string }[] }
  expect(archiveBody.entries.some(entry => entry.path.includes('timed-events/batches/'))).toBe(true)
  expect(archiveBody.entries.some(entry => entry.path.includes('timed-events/identities/'))).toBe(true)
})
