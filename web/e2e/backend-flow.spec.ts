import { expect, test, type Locator, type Page } from '@playwright/test'
import { createHash } from 'node:crypto'
import { mkdirSync, readFileSync } from 'node:fs'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'

const dataDir = process.env.IR_E2E_DATA_DIR
if (!dataDir) throw new Error('IR_E2E_DATA_DIR was not provided by Playwright config')

type RecordingWire = {
  state: string
  segment_count?: number
  tracks?: Record<string, { segments?: unknown[] }>
}
type UserPreferencesWire = { locale: string; theme: string; timezone: string }
type IntegrityWire = { status: string; objects_total: number; objects_corrupt: number }
type TagsWire = { tags: string[] }
type ArchiveIndexWire = { entries: { path: string }[] }
type RecordingPageWire = { items: RecordingListWire[] }
type RecordingListWire = { id: string; adapter_id: string; adapter_name: string; state: string; tags: string[] }
type StorageSettingsWire = {
  ingest_memory: { global_buffer_bytes: number; per_recording_buffer_bytes: number; max_payload_bytes: number }
  queue_writer: { pending_queue_capacity: number; writer_concurrency: number }
  failure_handling: { persist_attempts: number; retry_initial_backoff_ms: number; retry_max_backoff_ms: number }
  observability: { sampling_interval_ms: number; metrics_retention_ms: number }
}
type PreviewWire = { mode: string; state: string; available: boolean; frame_count: number; image_archive_ordinal?: number }
type PreviewFramesWire = { state: string; available: boolean; frame_count: number; items: { archive_ordinal: number; frame_time_seconds: number; generated_at: string }[] }
type MetadataWire = { current?: { title?: string | null; description?: string | null; observed_at: string }; items: { title?: string | null; description?: string | null; observed_at: string }[]; truncated: boolean }
type CSPViolation = { effectiveDirective: string; violatedDirective: string; blockedURI: string; sourceFile: string }

test.describe.configure({ mode: 'serial' })

test('actual Go backend: direct HLS capture, VOD, management, and delete', async ({ page }) => {
  test.setTimeout(180_000)
  await forceHlsJSPlayback(page)
  const sourceURL = readFileSync(join(dataDir, 'e2e-source-url'), 'utf8').trim()
  const manifestURL = `${sourceURL}/hls/stream.m3u8`
  const expectedSegmentSHA256 = createHash('sha256').update(readFileSync(join(dataDir, 'e2e-source-segment'))).digest('hex')
  const requests: string[] = []
  const cspViolations: CSPViolation[] = []
  const expectedAPIResponseFailures: { path: string; status: number }[] = []
  const consoleErrors: string[] = []
  const pageErrors: string[] = []
  const staticAssetFailures: string[] = []
  const unexpectedRequestFailures: string[] = []
  await page.exposeBinding('__recordCspViolation', (_source, violation: CSPViolation) => cspViolations.push(violation))
  await page.addInitScript(() => {
    document.addEventListener('securitypolicyviolation', event => {
      const recordViolation = (window as Window & { __recordCspViolation?: (violation: CSPViolation) => Promise<void> }).__recordCspViolation
      if (recordViolation) void recordViolation({ effectiveDirective: event.effectiveDirective, violatedDirective: event.violatedDirective, blockedURI: event.blockedURI, sourceFile: event.sourceFile })
    })
  })
  page.on('console', message => {
    if (message.type() !== 'error') return
    const status = Number(message.text().match(/status of (\d+)/)?.[1])
    const expectedIndex = expectedAPIResponseFailures.findIndex(failure => failure.status === status)
    if ([401, 404, 501].includes(status) && expectedIndex >= 0) {
      expectedAPIResponseFailures.splice(expectedIndex, 1)
      return
    }
    consoleErrors.push(message.text())
  })
  page.on('pageerror', error => pageErrors.push(error.message))
  page.on('request', request => requests.push(new URL(request.url()).pathname))
  page.on('requestfailed', request => {
    const failure = request.failure()?.errorText ?? 'unknown failure'
    if (failure !== 'net::ERR_ABORTED') unexpectedRequestFailures.push(`${request.url()}: ${failure}`)
  })
  page.on('response', response => {
    const path = new URL(response.url()).pathname
    if (path.startsWith('/static/ui/') && response.status() >= 400) staticAssetFailures.push(`${path}: ${response.status()}`)
    if (path.startsWith('/api/') && [401, 404, 501].includes(response.status())) expectedAPIResponseFailures.push({ path, status: response.status() })
  })
  const loginPage = await page.goto('/login')
  const cspHeader = loginPage?.headers()['content-security-policy'] ?? ''
  expect(cspHeader).toContain("script-src 'self'")
  expect(cspHeader).not.toContain("script-src 'unsafe-inline'")
  expect(await page.locator('script:not([src])').count()).toBe(0)
  await assertResponsive(page)
  await page.setViewportSize({ width: 1440, height: 900 })
  await expect(page.getByLabel(/Administrator password|관리자 비밀번호/)).toBeVisible()
  await page.getByLabel(/Administrator password|관리자 비밀번호/).fill('browser-e2e-strong-password')
  await page.getByRole('button', { name: /Sign in|로그인/ }).click()
  await expect(page).toHaveURL('/')
  await setLocaleForCurrentUser(page, 'ko-KR')
  await expect(page.getByRole('heading', { name: '대시보드' })).toBeVisible()
  const primaryNav = page.getByRole('navigation', { name: /Main navigation|주 메뉴/ })
  await expect(primaryNav.getByRole('link', { name: /Dashboard|대시보드/ })).toBeVisible()
  await expect(primaryNav.getByRole('link', { name: /^(Recordings|녹화)$/ })).toBeVisible()
  await expect(primaryNav.getByRole('link', { name: /^(New recording|새 녹화)$/ })).toHaveAttribute('href', '/new')
  const settingsNav = page.getByRole('navigation', { name: /Settings navigation|설정 메뉴/ })
  await expect(settingsNav.getByRole('link', { name: /Source plugins|소스 플러그인/ })).toBeVisible()
  await expect(settingsNav.getByRole('link', { name: /Workflows|워크플로/ })).toBeVisible()
  expect(requests.some(path => path.includes('bootstrap-token'))).toBe(false)

  for (const route of ['/', '/recordings', '/new', '/adapters', '/adapters/hls', '/workflows', '/settings', '/storage', '/storage/local-primary']) {
    await page.goto(route)
    await assertResponsive(page)
    await assertNoCSPViolations(cspViolations)
  }
  await page.goto('/storage')
  await expect(page.getByRole('heading', { name: '저장소', exact: true })).toBeVisible()
  await expect(page.getByText('local-primary', { exact: false }).first()).toBeVisible()
  await page.goto('/storage/local-primary')
  await expect(page.getByRole('heading', { name: '기본 보관 저장소' })).toBeVisible()
  const noMetricHistory = page.getByText('표시할 측정 기록이 없습니다.').first()
  const throughputChart = page.getByTestId('metric-chart-throughput')
  await expect.poll(async () => (await noMetricHistory.count()) > 0 || (await throughputChart.count()) > 0).toBe(true)
  if (await noMetricHistory.count()) await expect(noMetricHistory).toBeVisible()
  else await expect(throughputChart.locator('svg')).toBeVisible()
  await page.goto('/recordings')
  const recordingState = page.getByRole('combobox', { name: '상태 필터' })
  const recordingAdapter = page.getByRole('combobox', { name: '소스 플러그인 필터' })
  await exerciseNativeSelect(recordingState)
  await exerciseNativeSelect(recordingAdapter)
  await page.goto('/settings')
  await expect(page.getByText('커밋', { exact: true })).toBeVisible()
  const settingsSelects = page.locator('#settings-panel-preferences select')
  await expect(settingsSelects).toHaveCount(3)
  const localeSelect = page.locator('#user-locale')
  await localeSelect.selectOption('ko-KR')
  const koreanPreferencesResponsePromise = page.waitForResponse(response => response.url().endsWith('/api/user/preferences') && response.request().method() === 'PUT')
  await page.getByRole('button', { name: /환경 설정 저장|Save preferences/ }).click()
  const koreanPreferencesResponse = await koreanPreferencesResponsePromise
  expect(koreanPreferencesResponse.status()).toBe(200)
  expect(await koreanPreferencesResponse.json()).toMatchObject({ locale: 'ko-KR' })
  await expect(page.locator('html')).toHaveAttribute('lang', 'ko-KR')
  await localeSelect.selectOption('en-US')
  const englishPreferencesResponsePromise = page.waitForResponse(response => response.url().endsWith('/api/user/preferences') && response.request().method() === 'PUT')
  await page.getByRole('button', { name: /환경 설정 저장|Save preferences/ }).click()
  const englishPreferencesResponse = await englishPreferencesResponsePromise
  const englishPreferencesBody = await englishPreferencesResponse.json() as UserPreferencesWire
  expect(englishPreferencesResponse.status(), JSON.stringify(englishPreferencesBody)).toBe(200)
  expect(englishPreferencesBody).toMatchObject({ locale: 'en-US' })
  await expect.poll(() => page.locator('html').getAttribute('lang'), {
    message: `After locale selector en-US save, PUT /api/user/preferences returned ${JSON.stringify(englishPreferencesBody)}`,
  }).toBe('en-US')
  await localeSelect.selectOption('system')
  await page.getByRole('button', { name: /Save preferences|환경 설정 저장/ }).click()
  await expect(page.locator('html')).toHaveAttribute('lang', 'en-US')
  const themeSelect = page.locator('#user-theme')
  const originalTheme = await themeSelect.inputValue()
  await themeSelect.selectOption(originalTheme === 'dark' ? 'light' : 'dark')
  await page.getByRole('button', { name: /Save preferences|환경 설정 저장/ }).click()
  await themeSelect.selectOption(originalTheme)
  await page.getByRole('button', { name: /Save preferences|환경 설정 저장/ }).click()
  await exerciseNativeSelect(themeSelect)
  const concurrencySelect = settingsSelects.nth(2)
  const originalConcurrency = await concurrencySelect.inputValue()
  const alternateConcurrency = originalConcurrency === '1' ? '2' : '1'
  await concurrencySelect.selectOption(alternateConcurrency)
  await concurrencySelect.selectOption(originalConcurrency)
  await exerciseNativeSelect(concurrencySelect)
  await exerciseTopbarPopovers(page)
  await assertNoCSPViolations(cspViolations)
  // The remaining assertions use the Korean UI; set an explicit preference
  // after verifying that `system` follows Playwright's en-US browser locale.
  await setLocaleForCurrentUser(page, 'ko-KR')
  await page.goto('/new')
  await page.getByRole('button', { name: /HLS/ }).click()
  await page.getByRole('button', { name: /입력 설정|Next|다음/ }).click()
  await page.getByLabel('HLS manifest URL').fill(manifestURL)
  await page.getByLabel(/Recording title|녹화 제목/).fill('Browser E2E HLS capture')
  const previewToggle = page.getByRole('checkbox', { name: /장면 미리보기 생성|Generate scene previews/ })
  await expect(previewToggle).toBeChecked()
  await previewToggle.uncheck()
  await page.getByRole('button', { name: /입력 확인|Next|다음/ }).click()
  let recordingStartBody = ''
  page.on('request', request => {
    if (new URL(request.url()).pathname === '/api/recordings' && request.method() === 'POST') recordingStartBody = request.postData() ?? ''
  })
  await page.getByRole('button', { name: /녹화 시작|Start recording/ }).click()
  expect(JSON.parse(recordingStartBody)).toMatchObject({ preview_mode: 'disabled' })
  await expect(page).toHaveURL(/\/recordings\/[^/]+$/)
  const recordingID = page.url().split('/').at(-1)!

  await expect.poll(async () => segmentCount(await getJSON<RecordingWire>(page, `/api/recordings/${encodeURIComponent(recordingID)}`))).toBeGreaterThan(0)
  const activeRecording = await getJSON<RecordingWire>(page, `/api/recordings/${encodeURIComponent(recordingID)}`)
  expect(activeRecording.state).toBe('recording')
  await expect(page.getByRole('group', { name: '녹화 중 표시 방식' })).toBeVisible()
  await expect(page.getByRole('button', { name: '실시간 HLS' })).toHaveAttribute('aria-pressed', 'true')
  const liveVideo = page.locator('#vod-player video')
  await expect(liveVideo).toBeVisible()
  const liveMaster = await page.request.get(`/api/recordings/${encodeURIComponent(recordingID)}/play/live/master.m3u8`)
  expect(liveMaster.status()).toBe(200)
  const liveMasterText = await liveMaster.text()
  expect(liveMasterText).not.toContain('#EXT-X-ENDLIST')
  const liveVariantPath = playlistURI(liveMasterText)
  const liveVariant = await page.request.get(new URL(liveVariantPath, page.url()).toString())
  expect(liveVariant.status()).toBe(200)
  const firstLiveSnapshot = parseMediaPlaylist(await liveVariant.text())
  expect(firstLiveSnapshot.text).not.toContain('#EXT-X-ENDLIST')
  expect(firstLiveSnapshot.uris.length).toBeGreaterThan(0)
  expect(firstLiveSnapshot.text).not.toContain('#EXT-X-GAP')

  const liveSegment = await page.request.get(new URL(firstLiveSnapshot.uris[0]!, page.url()).toString())
  expect(liveSegment.status()).toBe(200)
  const liveSegmentBytes = await liveSegment.body()
  expect(liveSegmentBytes.length).toBeGreaterThan(0)
  expect(createHash('sha256').update(liveSegmentBytes).digest('hex')).toBe(expectedSegmentSHA256)
  expect(Number(liveSegment.headers()['content-length'] ?? liveSegmentBytes.length)).toBe(liveSegmentBytes.length)

  await decodeAndAssertPlayback(page, liveVideo)
  const secondLiveVariant = await page.request.get(new URL(liveVariantPath, page.url()).toString())
  expect(secondLiveVariant.status()).toBe(200)
  const secondLiveSnapshot = parseMediaPlaylist(await secondLiveVariant.text())
  expect(secondLiveSnapshot.text).not.toContain('#EXT-X-ENDLIST')
  assertOverlappingLiveIdentity(firstLiveSnapshot, secondLiveSnapshot)
  expect((await getJSON<RecordingWire>(page, `/api/recordings/${encodeURIComponent(recordingID)}`)).state).toBe('recording')

  const livePlaylist = await page.request.get(`/api/recordings/${encodeURIComponent(recordingID)}/play/live/tracks/main/playlist.m3u8`)
  expect(livePlaylist.status()).toBe(200)
  expect(await livePlaylist.text()).not.toContain('#EXT-X-ENDLIST')
  await page.getByRole('button', { name: '중지', exact: true }).click()
  await page.getByRole('alertdialog').getByRole('button', { name: '중지', exact: true }).click()
  await expect.poll(async () => (await getJSON<RecordingWire>(page, `/api/recordings/${encodeURIComponent(recordingID)}`)).state).toBe('stopped')

  await expect.poll(() => requests.some(path => path === `/api/recordings/${recordingID}/play/master.m3u8`)).toBe(true)
  const vod = await readVOD(page, recordingID)
  expect(vod.masterStatus).toBe(200)
  expect(vod.playlistStatus).toBe(200)
  expect(vod.segmentStatus).toBe(200)
  expect(vod.segmentSHA256).toBe(expectedSegmentSHA256)
  expect(vod.playlistText).toContain('#EXT-X-ENDLIST')
  const vodDuration = playlistDuration(vod.playlistText)
  expect(vodDuration).toBeGreaterThan(1)
  const vodVideo = page.locator('#vod-player video')
  await expect(vodVideo).toBeVisible()
  await decodeAndAssertPlayback(page, vodVideo)
  await seekAndAssertPlayback(vodVideo, 0.25, vodDuration)
  await seekAndAssertPlayback(vodVideo, vodDuration / 2, vodDuration)
  await seekAndAssertPlayback(vodVideo, Math.max(0.25, vodDuration - 0.5), vodDuration)

  await page.getByRole('button', { name: '편집' }).click()
  await page.getByLabel('태그 목록').fill('browser-e2e, source-preserved')
  await page.getByRole('button', { name: '저장', exact: true }).click()
  await expect(page.getByText('browser-e2e', { exact: true })).toBeVisible()
  expect((await getJSON<TagsWire>(page, `/api/recordings/${recordingID}/tags`)).tags).toEqual(['browser-e2e', 'source-preserved'])

  await page.getByRole('button', { name: '무결성 검사' }).click()
  await expect.poll(async () => (await getJSON<IntegrityWire>(page, `/api/recordings/${recordingID}/integrity`)).status).toBe('verified')
  const integrity = await getJSON<IntegrityWire>(page, `/api/recordings/${recordingID}/integrity`)
  expect(integrity.objects_total).toBeGreaterThan(0)
  expect(integrity.objects_corrupt).toBe(0)

  await page.getByRole('tab', { name: '보관 데이터 목록' }).click()
  await expect(page.getByText('recording.json', { exact: true })).toBeVisible()
  const archive = await getJSON<ArchiveIndexWire>(page, `/api/recordings/${recordingID}/archive/index`)
  expect(archive.entries.some((entry: { path: string }) => entry.path.endsWith('.ts'))).toBe(true)

  const exportResponse = await page.request.get(`/api/recordings/${recordingID}/exports`)
  expect(exportResponse.status()).toBe(501)
  await expect(page.getByText('이 서버에서 내보내기를 사용할 수 없습니다.')).toBeVisible()

  const pageWire = await getJSON<RecordingPageWire>(page, '/api/v2/recordings?state=stopped&limit=25')
  const listItem = pageWire.items.find(item => item.id === recordingID)
  expect(listItem).toMatchObject({ id: recordingID, adapter_id: 'hls', adapter_name: 'HLS', state: 'stopped', tags: ['browser-e2e', 'source-preserved'] })
  expect('adapter' in listItem).toBe(false)
  expect('resource' in listItem).toBe(false)
  await page.goto('/recordings?state=stopped')
  await expect(page.getByRole('link', { name: 'Browser E2E HLS capture' }).first()).toBeVisible()
  await expect(page.getByRole('row').filter({ hasText: 'Browser E2E HLS capture' }).getByText('중지됨', { exact: true })).toBeVisible()
  await expect(
    page.getByRole('row').filter({ hasText: 'Browser E2E HLS capture' }).getByText('HLS', { exact: true }),
  ).toBeVisible()
  await page.getByRole('link', { name: /Browser E2E HLS capture/ }).click()
  await expect(page).toHaveURL(new RegExp(`/recordings/${recordingID}$`))

  await page.goto('/storage/local-primary')
  await expect.poll(async () => {
    const response = await page.request.get('/api/storage/pools/local-primary/metrics?window=1h')
    if (!response.ok()) return 0
    const body = await response.json() as { items?: unknown[] }
    return body.items?.length ?? 0
  }, { timeout: 20_000 }).toBeGreaterThan(0)
  await expect(page.getByTestId('metric-chart-throughput').locator('svg')).toBeVisible()
  await expect(page.getByTestId('metric-chart-backlog').locator('svg')).toBeVisible()
  await expect(page.getByRole('heading', { name: 'Recorder 읽기/쓰기', exact: true })).toBeVisible()
  await assertResponsive(page)

  await page.goto(`/recordings/${recordingID}`)
  await expect(page.getByRole('heading', { name: 'Browser E2E HLS capture' })).toBeVisible()

  await page.getByRole('button', { name: '삭제' }).click()
  await expect(page.getByRole('alertdialog')).toContainText('되돌릴 수 없습니다')
  await assertNoCSPViolations(cspViolations)
  expect(consoleErrors).toEqual([])
  expect(pageErrors).toEqual([])
  expect(staticAssetFailures).toEqual([])
  expect(unexpectedRequestFailures).toEqual([])
  expect(expectedAPIResponseFailures).toEqual([])
  await page.getByRole('alertdialog').getByRole('button', { name: '보관 데이터 삭제' }).click()
  await expect(page).toHaveURL(/\/recordings(?:\?.*)?$/)
  await expect(page.getByText('Browser E2E HLS capture')).toHaveCount(0)
  expect((await page.request.get(`/api/recordings/${recordingID}`)).status()).toBe(404)

  for (const path of ['/login', '/', '/recordings', '/recordings/demo', '/new', '/adapters', '/adapters/hls', '/workflows', '/workflows/demo', '/settings', '/storage', '/storage/local-primary']) {
    const response = await page.request.get(path, { headers: { accept: 'text/html' } })
    expect(response.status(), `${path} should be served as an SPA route`).toBe(200)
    expect(response.headers()['content-type']).toContain('text/html')
  }
  for (const path of ['/foo', '/recordings/a/b', '/api/unknown', '/static/unknown']) {
    const response = await page.request.get(path, { headers: { accept: 'text/html' } })
    const expectedStatus = path === '/static/unknown' ? 404 : 403
    expect(response.status(), `${path} must fail closed before SPA fallback`).toBe(expectedStatus)
    expect(response.headers()['content-type'] ?? '').not.toContain('text/html')
    if (expectedStatus === 403) expect(await response.text()).toContain('permission denied')
  }
  await assertNoCSPViolations(cspViolations)
  expect(consoleErrors).toEqual([])
  expect(pageErrors).toEqual([])
  expect(staticAssetFailures).toEqual([])
  expect(unexpectedRequestFailures).toEqual([])
  expect(expectedAPIResponseFailures).toEqual([])
})

test('actual browser HLS decode: active live playback and terminal VOD seeks', async ({ page }) => {
  test.setTimeout(180_000)
  const sourceURL = readFileSync(join(dataDir, 'e2e-source-url'), 'utf8').trim()
  const manifestURL = `${sourceURL}/hls/stream.m3u8`
  const expectedSegmentSHA256 = createHash('sha256').update(readFileSync(join(dataDir, 'e2e-source-segment'))).digest('hex')
  const fatalPlaybackErrors: string[] = []
  await forceHlsJSPlayback(page)
  page.on('console', message => {
    if (message.type() === 'error' && /hls(?:\.js)?.*(?:fatal|error)|fatal.*hls/i.test(message.text())) fatalPlaybackErrors.push(message.text())
  })

  await login(page)
  await page.goto('/new')
  await page.getByRole('button', { name: /HLS/ }).click()
  await page.getByRole('button', { name: /입력 설정|Next|다음/ }).click()
  await page.getByLabel('HLS manifest URL').fill(manifestURL)
  await page.getByLabel('녹화 제목').fill('Browser HLS decode E2E')
  const previewToggle = page.getByRole('checkbox', { name: /장면 미리보기 생성|Generate scene previews/ })
  await expect(previewToggle).toBeChecked()
  await previewToggle.uncheck()
  await page.getByRole('button', { name: /입력 확인|Next|다음/ }).click()
  await page.getByRole('button', { name: /녹화 시작|Start recording/ }).click()
  await expect(page).toHaveURL(/\/recordings\/[a-f0-9]{32}$/)
  const recordingID = page.url().split('/').at(-1)!
  const recordingPath = `/api/recordings/${encodeURIComponent(recordingID)}`

  let activeRecording: RecordingWire | undefined
  await expect.poll(async () => {
    activeRecording = await getJSON<RecordingWire>(page, recordingPath)
    return activeRecording.state === 'recording' ? await segmentCount(activeRecording) : 0
  }, { timeout: 30_000 }).toBeGreaterThan(0)
  expect(activeRecording?.state).toBe('recording')

  const liveMasterPath = `${recordingPath}/play/live/master.m3u8`
  const liveMaster = await page.request.get(liveMasterPath)
  expect(liveMaster.status()).toBe(200)
  const liveMasterText = await liveMaster.text()
  expect(liveMasterText).toContain('#EXTM3U')
  expect(liveMasterText).not.toContain('#EXT-X-ENDLIST')
  const liveVariant = await page.request.get(new URL(playlistURI(liveMasterText), liveMaster.url()).toString())
  expect(liveVariant.status()).toBe(200)
  const firstLiveSnapshot = parseMediaPlaylist(await liveVariant.text())
  expect(firstLiveSnapshot.text).not.toContain('#EXT-X-ENDLIST')
  expect(firstLiveSnapshot.uris.length).toBeGreaterThan(0)
  expect(firstLiveSnapshot.uris.length).toBeLessThanOrEqual(12)
  expect(firstLiveSnapshot.text).not.toContain('#EXT-X-GAP')

  const liveSegment = await page.request.get(new URL(firstLiveSnapshot.uris[0]!, liveVariant.url()).toString())
  expect(liveSegment.status()).toBe(200)
  const liveBytes = await liveSegment.body()
  expect(liveBytes.length).toBeGreaterThan(0)
  expect(createHash('sha256').update(liveBytes).digest('hex')).toBe(expectedSegmentSHA256)
  expect(Number(liveSegment.headers()['content-length'] ?? liveBytes.length)).toBe(liveBytes.length)

  const liveVideo = page.locator('#vod-player video')
  await expect(liveVideo).toBeVisible()
  await decodeAndAssertPlayback(page, liveVideo)
  const secondLiveVariant = await page.request.get(new URL(playlistURI(liveMasterText), liveMaster.url()).toString())
  expect(secondLiveVariant.status()).toBe(200)
  const secondLiveSnapshot = parseMediaPlaylist(await secondLiveVariant.text())
  expect(secondLiveSnapshot.text).not.toContain('#EXT-X-ENDLIST')
  assertOverlappingLiveIdentity(firstLiveSnapshot, secondLiveSnapshot)
  expect((await getJSON<RecordingWire>(page, recordingPath)).state).toBe('recording')
  expect(fatalPlaybackErrors).toEqual([])

  await page.getByRole('button', { name: /중지|Stop recording/, exact: true }).click()
  await page.getByRole('alertdialog').getByRole('button', { name: /중지|Stop recording/, exact: true }).click()
  await expect.poll(async () => (await getJSON<RecordingWire>(page, recordingPath)).state).toBe('stopped')

  const vodMasterPath = `/api/recordings/${encodeURIComponent(recordingID)}/play/master.m3u8`
  const vodMaster = await page.request.get(vodMasterPath)
  expect(vodMaster.status()).toBe(200)
  const vodMasterText = await vodMaster.text()
  const vodVariant = await page.request.get(new URL(playlistURI(vodMasterText), vodMaster.url()).toString())
  expect(vodVariant.status()).toBe(200)
  const vodPlaylistText = await vodVariant.text()
  expect(vodPlaylistText).toContain('#EXT-X-ENDLIST')
  const vodDuration = playlistDuration(vodPlaylistText)
  expect(vodDuration).toBeGreaterThan(1)
  const vodSegmentPath = vodPlaylistText.split(/\r?\n/).map(line => line.trim()).find(line => line && !line.startsWith('#'))
  expect(vodSegmentPath).toBeTruthy()
  const vodSegment = await page.request.get(new URL(vodSegmentPath!, vodVariant.url()).toString())
  expect(vodSegment.status()).toBe(200)
  const vodBytes = await vodSegment.body()
  expect(vodBytes.length).toBeGreaterThan(0)
  expect(createHash('sha256').update(vodBytes).digest('hex')).toBe(expectedSegmentSHA256)

  const vodVideo = page.locator('#vod-player video')
  await expect(vodVideo).toBeVisible()
  await decodeAndAssertPlayback(page, vodVideo)
  await seekAndAssertPlayback(vodVideo, 0.25, vodDuration)
  await seekAndAssertPlayback(vodVideo, vodDuration / 2, vodDuration)
  await seekAndAssertPlayback(vodVideo, Math.max(0.25, vodDuration - 0.5), vodDuration)
  expect(fatalPlaybackErrors).toEqual([])
})

test('actual Go backend: locale preference save updates the current document language', async ({ page }) => {
  await login(page)
  await page.goto('/settings')
  const localeSelect = page.locator('#user-locale')
  await expect(localeSelect).toHaveValue('ko-KR')
  const previous = await getJSON<UserPreferencesWire>(page, '/api/user/preferences')
  expect(previous.locale).toBe('ko-KR')
  await localeSelect.selectOption('en-US')
  const responsePromise = page.waitForResponse(response => response.url().endsWith('/api/user/preferences') && response.request().method() === 'PUT')
  await page.getByRole('button', { name: /Save preferences|환경 설정 저장/ }).click()
  const response = await responsePromise
  const body = await response.json() as UserPreferencesWire
  const submitted = response.request().postDataJSON() as UserPreferencesWire
  expect(response.status(), JSON.stringify(body)).toBe(200)
  expect(submitted).toEqual({ ...previous, locale: 'en-US' })
  expect(body).toEqual(submitted)
  await expect.poll(() => page.locator('html').getAttribute('lang'), {
    message: `After locale selector en-US save, PUT /api/user/preferences returned ${JSON.stringify(body)}`,
  }).toBe('en-US')
  await expect(page.getByRole('navigation', { name: /Main navigation|주 메뉴/ }).getByRole('link', { name: /^(Dashboard|대시보드)$/ })).toBeVisible()
})

test('actual Go backend: metadata fixture changes are archived and rendered as a timeline', async ({ page }) => {
  test.setTimeout(120_000)
  await login(page)
  const sourceURL = readFileSync(join(dataDir, 'e2e-source-url'), 'utf8').trim()
  await page.goto('/new')
  await page.getByRole('button', { name: /Metadata Fixture/ }).click()
  await page.getByRole('button', { name: /입력 설정|Next|다음/ }).click()
  await page.getByLabel('Fixture source URL').fill(sourceURL)
  await page.getByLabel('녹화 제목').fill('Metadata timeline E2E')
  await page.getByRole('button', { name: /입력 확인|Next|다음/ }).click()
  await page.getByRole('button', { name: /녹화 시작|Start recording/ }).click()
  await expect(page).toHaveURL(/\/recordings\/[a-f0-9]{32}$/)
  const recordingID = page.url().split('/').at(-1)!
  const metadataPath = `/api/recordings/${encodeURIComponent(recordingID)}/metadata`
  await expect.poll(async () => (await getJSON<MetadataWire>(page, metadataPath)).current?.title, { timeout: 15_000 }).toBe('Fixture broadcast title')
  const changed = await page.request.get(`${new URL(sourceURL).origin}/e2e/set-metadata?title=${encodeURIComponent('Changed fixture source title')}&description=${encodeURIComponent('Updated fixture description')}`)
  expect(changed.status()).toBe(204)
  await expect.poll(async () => (await getJSON<MetadataWire>(page, metadataPath)).current?.title, { timeout: 45_000 }).toBe('Changed fixture source title')
  const history = await getJSON<MetadataWire>(page, metadataPath)
  expect(history.items.map(item => item.title)).toEqual(['Fixture broadcast title', 'Changed fixture source title'])
  expect(history.items.map(item => item.description)).toEqual(['Initial fixture description', 'Updated fixture description'])
  await page.reload()
  await expect(page.getByRole('heading', { name: '방송 메타데이터' })).toBeVisible()
  await expect(page.getByRole('list').getByText('Updated fixture description', { exact: true })).toBeVisible()
  await page.getByRole('button', { name: '중지', exact: true }).click()
  await page.getByRole('alertdialog').getByRole('button', { name: '중지', exact: true }).click()
  await expect.poll(async () => (await getJSON<RecordingWire>(page, `/api/recordings/${encodeURIComponent(recordingID)}`)).state).toBe('stopped')
})

test('actual Go backend: storage ingest settings persist, show restart state, and reject invalid values', async ({ page }) => {
  await login(page)
  await page.goto('/settings')
  await page.getByRole('tab', { name: '저장소' }).click()
  await expect(page.getByRole('heading', { name: '고급 수집·저장 설정' })).toBeVisible()
  await expect(page.getByLabel('총 저장 시도 횟수')).toHaveValue('5')
  await expect(page.getByText('최초 저장 시도를 포함한 최대 시도 횟수입니다.')).toBeVisible()
  const before = await getJSON<{ settings: { storage: StorageSettingsWire } }>(page, '/api/settings')
  expect(before.settings.storage.observability.sampling_interval_ms).toBe(5000)
  expect(before.settings.storage.failure_handling.persist_attempts).toBe(5)

  await page.getByLabel('측정 간격').fill('10')
  await page.getByRole('button', { name: '수집·저장 설정 저장' }).click()
  await expect(page.getByRole('status').filter({ hasText: '재시작해야 적용됩니다' })).toBeVisible()
  await expect(page.getByTestId('effective-storage-settings').getByText('5초')).toBeVisible()
  await expect(page.getByLabel('측정 간격')).toHaveValue('10')

  const stored = await getJSON<{ settings: { storage: StorageSettingsWire }; effective_storage: StorageSettingsWire; restart_required: string[] }>(page, '/api/settings')
  expect(stored.settings.storage.observability.sampling_interval_ms).toBe(10_000)
  expect(stored.effective_storage.observability.sampling_interval_ms).toBe(5_000)
  expect(stored.restart_required).toContain('storage.observability.sampling_interval_ms')

  await page.getByLabel('측정 간격').fill('60')
  const minimumRetentionHours = Number(await page.getByLabel('측정 기록 보존 기간').getAttribute('min'))
  expect(minimumRetentionHours).toBeCloseTo(20 / 60, 6)

  const session = await getJSON<{ csrf_token: string }>(page, '/api/auth/session')
  const unreachableObservability: StorageSettingsWire = {
    ...stored.settings.storage,
    observability: { sampling_interval_ms: 60_000, metrics_retention_ms: 300_000 },
  }
  const unreachable = await page.request.put('/api/settings', { headers: { 'X-CSRF-Token': session.csrf_token }, data: { storage: unreachableObservability } })
  expect(unreachable.status()).toBe(400)
  const unreachableBody = await unreachable.json() as { error?: unknown; message?: unknown }
  const unreachableMessage = typeof unreachableBody.error === 'string' ? unreachableBody.error : unreachableBody.message
  expect(unreachableMessage).toContain('at least 20 minutes')
  const afterRejectedUpdate = await getJSON<{ settings: { storage: StorageSettingsWire } }>(page, '/api/settings')
  expect(afterRejectedUpdate.settings.storage.observability.sampling_interval_ms).toBe(10_000)
  expect(afterRejectedUpdate.settings.storage.observability.metrics_retention_ms).toBe(86_400_000)

  const invalidStorage: StorageSettingsWire = {
    ...stored.settings.storage,
    ingest_memory: { ...stored.settings.storage.ingest_memory, global_buffer_bytes: 64 * 1024 * 1024, per_recording_buffer_bytes: 65 * 1024 * 1024, max_payload_bytes: 32 * 1024 * 1024 },
  }
  const invalid = await page.request.put('/api/settings', { headers: { 'X-CSRF-Token': session.csrf_token }, data: { storage: invalidStorage } })
  expect(invalid.status()).toBe(400)
  const body = await invalid.json() as { error?: unknown; message?: unknown }
  const message = typeof body.error === 'string' ? body.error : body.message
  expect(typeof message).toBe('string')
  expect((message as string).length).toBeGreaterThan(0)
})

async function assertNoCSPViolations(violations: CSPViolation[]) {
  await expect.poll(() => violations).toEqual([])
}

async function exerciseNativeSelect(select: Locator) {
  await expect(select).toBeVisible()
  const originalValue = await select.inputValue()
  await select.click()
  await select.press('ArrowDown')
  await select.press('Escape')
  if (await select.inputValue() !== originalValue) await select.selectOption(originalValue)
}

async function exerciseTopbarPopovers(page: Page) {
  for (const viewport of [{ width: 1440, height: 900 }, { width: 390, height: 844 }]) {
    await page.setViewportSize(viewport)
    for (const trigger of [page.getByRole('button', { name: /^(Notifications|알림)$/ }), page.getByRole('button', { name: /IR/ }).last()]) {
      await trigger.click()
      const content = page.getByRole('dialog').last()
      await expect(content).toBeVisible()
      const bounds = await content.boundingBox()
      expect(bounds).not.toBeNull()
      expect(bounds!.x).toBeGreaterThanOrEqual(0)
      expect(bounds!.y).toBeGreaterThanOrEqual(0)
      expect(bounds!.x + bounds!.width).toBeLessThanOrEqual(viewport.width + 1)
      expect(bounds!.y + bounds!.height).toBeLessThanOrEqual(viewport.height + 1)
      await page.keyboard.press('Escape')
      await expect(content).toBeHidden()
    }
  }
  await page.setViewportSize({ width: 1440, height: 900 })
}

test('actual Go backend: opt-in segment previews, live/recent frames, storyboard seek, and screenshots', async ({ page }) => {
  const sourceURL = readFileSync(join(dataDir, 'e2e-source-url'), 'utf8').trim()
  const manifestURL = `${sourceURL}/hls/stream.m3u8`
  const screenshots = fileURLToPath(new URL('../../artifacts/preview-frame-index/', import.meta.url))
  const consoleErrors: string[] = []
  const pageErrors: string[] = []
  const requestFailures: string[] = []
  const expectedUnavailableResponses: { path: string; status: number }[] = []
  const unexpectedHTTPResponses: { path: string; status: number }[] = []
  page.on('console', message => { if (message.type() === 'error') consoleErrors.push(message.text()) })
  page.on('pageerror', error => pageErrors.push(error.message))
  page.on('requestfailed', request => {
    const reason = request.failure()?.errorText ?? 'unknown failure'
    if (reason !== 'net::ERR_ABORTED') requestFailures.push(`${request.url()}: ${reason}`)
  })
  page.on('response', response => {
    if (response.status() < 400) return
    const path = new URL(response.url()).pathname
    if (response.status() === 501 && /^\/api\/recordings\/[^/]+\/exports$/.test(path)) {
      expectedUnavailableResponses.push({ path, status: response.status() })
      return
    }
    unexpectedHTTPResponses.push({ path, status: response.status() })
  })
  mkdirSync(screenshots, { recursive: true })
  await page.setViewportSize({ width: 1440, height: 900 })
  await login(page)
  await page.goto('/new')
  await page.getByRole('button', { name: /HLS/ }).click()
  await page.getByRole('button', { name: /입력 설정|Next|다음/ }).click()
  await page.getByLabel('HLS manifest URL').fill(manifestURL)
  await page.getByLabel('녹화 제목').fill('Preview Frame Index E2E')
  const previewToggle = page.getByRole('checkbox', { name: /장면 미리보기 생성|Generate scene previews/ })
  await expect(previewToggle).toBeVisible()
  await expect(previewToggle).toBeChecked()
  await page.getByRole('button', { name: /입력 확인|Next|다음/ }).click()
  let startBody = ''
  page.on('request', request => {
    if (new URL(request.url()).pathname === '/api/recordings' && request.method() === 'POST') startBody = request.postData() ?? ''
  })
  await page.getByRole('button', { name: /녹화 시작|Start recording/ }).click()
  await expect(page).toHaveURL(/\/recordings\/[^/]+$/)
  const recordingID = page.url().split('/').at(-1)!
  expect(JSON.parse(startBody)).toMatchObject({ preview_mode: 'segment' })
  await expect(page.getByRole('button', { name: '현재 미리보기' })).toBeVisible()
  await page.getByRole('button', { name: '현재 미리보기' }).click()

  let summary: PreviewWire | undefined
  await expect.poll(async () => {
    const response = await page.request.get(`/api/recordings/${encodeURIComponent(recordingID)}`)
    if (!response.ok()) return 0
    const recording = await response.json() as { preview?: PreviewWire }
    summary = recording.preview
    return summary?.state ?? ''
  }, { timeout: 15_000 }).not.toBe('')
  expect(summary?.state, 'preview extraction must be available in the FFmpeg-capable E2E fixture').not.toBe('unavailable')
  expect(summary?.mode).toBe('segment')
  let recent: PreviewFramesWire | undefined
  await expect.poll(async () => {
    const response = await page.request.get(`/api/recordings/${encodeURIComponent(recordingID)}/previews?sampling=recent&limit=48`)
    expect(response.status()).toBe(200)
    recent = await response.json() as PreviewFramesWire
    return recent?.items.length ?? 0
  }, { timeout: 45_000 }).toBeGreaterThan(0)
  let latestOrdinal = summary?.image_archive_ordinal ?? recent?.items.at(-1)?.archive_ordinal ?? 0
  await expect.poll(async () => {
    const response = await page.request.get(`/api/recordings/${encodeURIComponent(recordingID)}`)
    if (!response.ok()) return 0
    const recording = await response.json() as { preview?: PreviewWire }
    latestOrdinal = recording.preview?.image_archive_ordinal ?? 0
    return latestOrdinal
  }, { timeout: 15_000 }).toBeGreaterThan(0)
  const liveImage = page.locator(`#vod-player img[src^="/api/recordings/${recordingID}/previews/"]`).last()
  await expect.poll(() => liveImage.evaluate(image => ({ complete: (image as HTMLImageElement).complete, width: (image as HTMLImageElement).naturalWidth }))).toMatchObject({ complete: true })
  await expect.poll(() => liveImage.evaluate(image => (image as HTMLImageElement).naturalWidth)).toBeGreaterThan(0)
  await page.locator('#vod-player').scrollIntoViewIfNeeded()
  await page.screenshot({ path: join(screenshots, '01-live-recording-detail.png') })

  await page.goto('/recordings')
  await expect(page.getByRole('link', { name: 'Preview Frame Index E2E' })).toBeVisible()
  await expectPreviewImage(page, recordingID)
  await page.screenshot({ path: join(screenshots, '03-recordings-list.png') })
  await page.goto('/')
  await expect(page.getByRole('heading', { name: '대시보드' })).toBeVisible()
  await expectPreviewImage(page, recordingID)
  await page.screenshot({ path: join(screenshots, '04-dashboard.png') })
  await page.goto(`/recordings/${recordingID}`)
  await page.getByRole('button', { name: '중지', exact: true }).click()
  await page.getByRole('alertdialog').getByRole('button', { name: '중지', exact: true }).click()
  await expect.poll(async () => (await getJSON<{ state: string }>(page, `/api/recordings/${encodeURIComponent(recordingID)}`)).state).toBe('stopped')
  const uniformResponse = await page.request.get(`/api/recordings/${encodeURIComponent(recordingID)}/previews?sampling=uniform&limit=48`)
  expect(uniformResponse.status()).toBe(200)
  const uniform = await uniformResponse.json() as PreviewFramesWire
  expect(uniform.items.length).toBeGreaterThan(0)
  expect(new Set(uniform.items.map(item => item.archive_ordinal)).size).toBe(uniform.items.length)
  await expect(page.locator('#vod-player video')).toHaveAttribute('controls', '')
  const storyboardButtons = page.locator('section[aria-labelledby="preview-heading"] button[aria-label$="위치로 이동"]')
  await expect(storyboardButtons).toHaveCount(uniform.items.length)
  const video = page.locator('#vod-player video')
  await expect.poll(() => video.evaluate(element => (element as HTMLVideoElement).readyState)).toBeGreaterThanOrEqual(2)
  await scrollPreviewSectionIntoView(page)
  const storyboardImages = page.locator('section[aria-labelledby="preview-heading"] img')
  for (let i = 0; i < await storyboardImages.count(); i++) {
    await expect.poll(() => storyboardImages.nth(i).evaluate(image => (image as HTMLImageElement).naturalWidth)).toBeGreaterThan(0)
  }
  const target = uniform.items[Math.floor(uniform.items.length / 2)]!
  const wasPaused = await video.evaluate(element => (element as HTMLVideoElement).paused)
  await storyboardButtons.nth(Math.floor(uniform.items.length / 2)).click()
  await expect.poll(() => video.evaluate(element => (element as HTMLVideoElement).currentTime)).toBeCloseTo(target.frame_time_seconds, 0)
  expect(await video.evaluate(element => (element as HTMLVideoElement).paused)).toBe(wasPaused)
  await page.screenshot({ path: join(screenshots, '02-stopped-recording-storyboard.png') })

  const frameImage = page.locator(`section[aria-labelledby="preview-heading"] img[src^="/api/recordings/${recordingID}/previews/"]`).first()
  await expect.poll(() => frameImage.evaluate(image => ({ complete: (image as HTMLImageElement).complete, width: (image as HTMLImageElement).naturalWidth }))).toMatchObject({ complete: true })
  await expect.poll(() => frameImage.evaluate(image => (image as HTMLImageElement).naturalWidth)).toBeGreaterThan(0)
  await page.getByRole('button', { name: '삭제' }).click()
  await page.getByRole('alertdialog').getByRole('button', { name: '보관 데이터 삭제' }).click()
  const expectedUnavailableConsoleErrors = consoleErrors.filter(message => message.includes('status of 501 (Not Implemented)'))
  const unexpectedConsoleErrors = consoleErrors.filter(message => !message.includes('status of 501 (Not Implemented)'))
  expect(expectedUnavailableResponses.length).toBeGreaterThan(0)
  expect(expectedUnavailableConsoleErrors.length).toBe(expectedUnavailableResponses.length)
  expect(unexpectedHTTPResponses).toEqual([])
  expect(unexpectedConsoleErrors).toEqual([])
  expect(pageErrors).toEqual([])
  expect(requestFailures).toEqual([])
})

async function expectPreviewImage(page: Page, recordingID: string) {
  const image = page.locator(`img[src^="/api/recordings/${encodeURIComponent(recordingID)}/previews/"]`).first()
  await expect.poll(() => image.evaluate(node => ({ complete: (node as HTMLImageElement).complete, width: (node as HTMLImageElement).naturalWidth }))).toMatchObject({ complete: true })
  await expect.poll(() => image.evaluate(node => (node as HTMLImageElement).naturalWidth)).toBeGreaterThan(0)
}

async function scrollPreviewSectionIntoView(page: Page) {
  await page.locator('section[aria-labelledby="preview-heading"]').evaluate(section => {
    const top = window.scrollY + section.getBoundingClientRect().top
    window.scrollTo(0, Math.max(0, top - 620))
  })
}

test('actual workflow adapter: challenge, secret, action URL, continue, cancel, and history', async ({ page }) => {
  const manifestURL = `${readFileSync(join(dataDir, 'e2e-source-url'), 'utf8').trim()}/hls/stream.m3u8`
  const ephemeralSecret = 'ephemeral-browser-secret'
  let continuationBody = ''
  page.on('request', request => {
    if (new URL(request.url()).pathname.endsWith('/continue')) continuationBody = request.postData() ?? ''
  })
  await login(page)
  await page.goto('/new')
  await page.getByRole('button', { name: /Workflow Fixture/ }).click()
  await page.getByRole('button', { name: /입력 설정|Next|다음/ }).click()
  await page.getByLabel('Fixture source URL').fill(manifestURL)
  await page.getByRole('button', { name: /입력 확인|Next|다음/ }).click()
  await page.getByRole('button', { name: /녹화 시작|Start recording/ }).click()
  await expect(page).toHaveURL(/\/workflows\/[^/]+$/)
  const firstWorkflowID = page.url().split('/').at(-1)!

  const actionLink = page.getByRole('link', { name: 'Open fixture action' })
  await expect(actionLink).toHaveAttribute('href', 'https://example.test/confirm')
  await expect(actionLink).toHaveAttribute('target', '_blank')
  await expect(actionLink).toHaveAttribute('rel', /noopener/)
  await expect(actionLink).toHaveAttribute('rel', /noreferrer/)
  await assertResponsive(page)
  await page.getByLabel('Fixture answer').fill('continue')
  await page.getByLabel('Fixture secret').fill(ephemeralSecret)
  const persistSecret = page.getByRole('checkbox', { name: /다음에도 저장/ })
  await expect(persistSecret).toBeVisible()
  await persistSecret.check()
  await expect(persistSecret).toBeChecked()
  await persistSecret.uncheck()
  await expect(persistSecret).not.toBeChecked()
  await page.getByRole('button', { name: '계속 진행' }).click()
  await expect.poll(() => continuationBody).not.toBe('')
  const submitted = JSON.parse(continuationBody) as { secrets?: Record<string, string>; persist_fields?: string[] }
  expect(submitted.secrets?.session_value).toBe(ephemeralSecret)
  expect(submitted.persist_fields ?? []).not.toContain('session_value')
  await expect(page).toHaveURL(/\/recordings\/[^/]+$/)
  const recordingID = page.url().split('/').at(-1)!
  await expect.poll(async () => segmentCount(await getJSON<RecordingWire>(page, `/api/recordings/${recordingID}`))).toBeGreaterThan(0)
  expect(JSON.stringify(await getJSON(page, '/api/workflow-history'))).not.toContain(ephemeralSecret)
  expect(JSON.stringify(await getJSON(page, '/api/logs?limit=100'))).not.toContain(ephemeralSecret)
  await page.getByRole('button', { name: '중지', exact: true }).click()
  await page.getByRole('alertdialog').getByRole('button', { name: '중지', exact: true }).click()
  await expect.poll(async () => (await getJSON<RecordingWire>(page, `/api/recordings/${recordingID}`)).state).toBe('stopped')

  await page.goto(`/workflows/${firstWorkflowID}`)
  await expect(page.getByRole('heading', { name: '워크플로를 찾을 수 없습니다' })).toBeVisible()

  await page.goto('/new')
  await page.getByRole('button', { name: /Workflow Fixture/ }).click()
  await page.getByRole('button', { name: /입력 설정|Next|다음/ }).click()
  await page.getByLabel('Fixture source URL').fill(manifestURL)
  await page.getByRole('button', { name: /입력 확인|Next|다음/ }).click()
  await page.getByRole('button', { name: /녹화 시작|Start recording/ }).click()
  await expect(page).toHaveURL(/\/workflows\/[^/]+$/)
  await page.getByRole('button', { name: '취소' }).click()
  await page.getByRole('alertdialog').getByRole('button', { name: '워크플로 취소' }).click()
  await expect(page).toHaveURL('/workflows')
  await expect(page.getByText(/Workflow Fixture · 취소됨/)).toBeVisible()
})

async function login(page: Page) {
  await page.goto('/login')
  await page.getByLabel(/Administrator password|관리자 비밀번호/).fill('browser-e2e-strong-password')
  await page.getByRole('button', { name: /Sign in|로그인/ }).click()
  await expect(page).toHaveURL('/')
  await setLocaleForCurrentUser(page, 'ko-KR')
}

async function setLocaleForCurrentUser(page: Page, locale: 'ko-KR' | 'en-US') {
  const session = await getJSON<{ csrf_token: string }>(page, '/api/auth/session')
  const current = await getJSON<UserPreferencesWire>(page, '/api/user/preferences')
  const response = await page.request.put('/api/user/preferences', {
    headers: { 'X-CSRF-Token': session.csrf_token },
    data: { ...current, locale },
  })
  expect(response.status()).toBe(200)
  await page.reload()
  await expect(page.locator('html')).toHaveAttribute('lang', locale)
}

async function assertResponsive(page: Page) {
  for (const viewport of [
    { width: 390, height: 844 }, { width: 430, height: 932 }, { width: 768, height: 1024 },
    { width: 1024, height: 768 }, { width: 1440, height: 900 },
  ]) {
    await page.setViewportSize(viewport)
    const width = await page.evaluate(() => ({ body: document.body.scrollWidth, viewport: window.innerWidth }))
    expect(width.body).toBeLessThanOrEqual(width.viewport)
  }
}

async function getJSON<T>(page: Page, path: string): Promise<T> {
  return page.evaluate(async url => {
    const response = await fetch(url)
    if (!response.ok) throw new Error(`GET ${url}: ${response.status}`)
    return await response.json() as T
  }, path)
}

async function segmentCount(recording: RecordingWire): Promise<number> {
  if (typeof recording.segment_count === 'number') return recording.segment_count
  return Object.values(recording.tracks ?? {}).reduce((count, track) => count + (track.segments?.length ?? 0), 0)
}

function playlistURI(text: string): string {
  const uri = text.split(/\r?\n/).map(line => line.trim()).find(line => line && !line.startsWith('#'))
  if (!uri) throw new Error('HLS master playlist had no variant URI')
  return uri
}

function parseMediaPlaylist(text: string) {
  const match = /^#EXT-X-MEDIA-SEQUENCE:(\d+)$/m.exec(text)
  if (!match) throw new Error('HLS media playlist had no media sequence')
  return {
    text,
    mediaSequence: Number(match[1]),
    uris: text.split(/\r?\n/).map(line => line.trim()).filter(line => line && !line.startsWith('#')),
  }
}

function assertOverlappingLiveIdentity(first: ReturnType<typeof parseMediaPlaylist>, second: ReturnType<typeof parseMediaPlaylist>) {
  const firstSequences = new Map(first.uris.map((uri, index) => [uri, first.mediaSequence + index]))
  const overlap = second.uris.filter(uri => firstSequences.has(uri))
  expect(overlap.length, 'live reloads should retain at least one media URI').toBeGreaterThan(0)
  for (const uri of overlap) expect(second.mediaSequence + second.uris.indexOf(uri)).toBe(firstSequences.get(uri))
}

function playlistDuration(text: string): number {
  return text.split(/\r?\n/).reduce((total, line) => {
    const match = /^#EXTINF:([\d.]+)(?:,|$)/.exec(line.trim())
    return total + (match ? Number(match[1]) : 0)
  }, 0)
}

async function decodeAndAssertPlayback(page: Page, video: Locator) {
  expect(await video.evaluate(node => (node as HTMLVideoElement).canPlayType('application/vnd.apple.mpegurl'))).toBe('')
  expect(await page.evaluate(() => typeof MediaSource !== 'undefined')).toBe(true)
  await video.evaluate(node => { (node as HTMLVideoElement).muted = true })
  await expect.poll(() => video.evaluate(node => (node as HTMLVideoElement).readyState), { timeout: 30_000 }).toBeGreaterThanOrEqual(2)
  await expect.poll(() => video.evaluate(node => (node as HTMLVideoElement).videoWidth), { timeout: 30_000 }).toBeGreaterThan(0)
  await expect.poll(() => video.evaluate(node => (node as HTMLVideoElement).videoHeight), { timeout: 30_000 }).toBeGreaterThan(0)
  await expect(page.getByText(/Playback unavailable|재생할 수 없습니다/)).toHaveCount(0)
  const initialTime = await video.evaluate(node => (node as HTMLVideoElement).currentTime)
  await video.evaluate(node => (node as HTMLVideoElement).play())
  await expect.poll(() => video.evaluate(node => (node as HTMLVideoElement).currentTime), { timeout: 15_000 }).toBeGreaterThan(initialTime + 0.2)
  await expect(page.getByText(/Playback unavailable|재생할 수 없습니다/)).toHaveCount(0)
}

async function forceHlsJSPlayback(page: Page) {
  await page.addInitScript(() => {
    const nativeCanPlayType = HTMLMediaElement.prototype.canPlayType
    Object.defineProperty(HTMLMediaElement.prototype, 'canPlayType', {
      configurable: true,
      value(this: HTMLMediaElement, type: string) {
        if (type === 'application/vnd.apple.mpegurl') return ''
        return nativeCanPlayType.call(this, type)
      },
    })
  })
}

async function seekAndAssertPlayback(video: Locator, requestedTime: number, duration: number) {
  const target = Math.min(Math.max(0, requestedTime), Math.max(0, duration - 0.25))
  await video.evaluate((node, time) => {
    const element = node as HTMLVideoElement
    element.pause()
    element.currentTime = time
  }, target)
  await expect.poll(() => video.evaluate((node, expected) => {
    const element = node as HTMLVideoElement
    return !element.seeking && Math.abs(element.currentTime - expected) <= 0.5 && element.readyState >= 2
  }, target), { timeout: 20_000 }).toBe(true)
}

async function readVOD(page: Page, recordingID: string) {
  return page.evaluate(async id => {
    const base = `/api/recordings/${encodeURIComponent(id)}/play/`
    const master = await fetch(`${base}master.m3u8`)
    const masterText = await master.text()
    const trackPath = masterText.split(/\r?\n/).find(line => line && !line.startsWith('#'))
    if (!trackPath) throw new Error('master playlist had no track path')
    const playlist = await fetch(new URL(trackPath, `${location.origin}${base}`).toString())
    const playlistText = await playlist.text()
    const segmentPath = playlistText.split(/\r?\n/).find(line => line && !line.startsWith('#'))
    if (!segmentPath) throw new Error('track playlist had no media path')
    const segment = await fetch(new URL(segmentPath, `${location.origin}${base}`).toString())
    return {
      masterStatus: master.status,
      playlistStatus: playlist.status,
      playlistText,
      segmentStatus: segment.status,
      segmentSHA256: await crypto.subtle.digest('SHA-256', await segment.arrayBuffer()).then(digest =>
        [...new Uint8Array(digest)].map(byte => byte.toString(16).padStart(2, '0')).join(''),
      ),
    }
  }, recordingID)
}
