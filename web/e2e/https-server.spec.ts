import { expect, test } from '@playwright/test'
import { readFileSync } from 'node:fs'
import { join } from 'node:path'

const httpsURL = readFileSync(join(process.env.IR_E2E_DATA_DIR!, 'e2e-https-url'), 'utf8').trim()
const httpsPort = new URL(httpsURL).port

test('Runtime Host direct HTTPS disable and saved reactivation preserve browser access and Secure session', async ({ page, context }) => {
  const httpURL = 'http://127.0.0.1:4173'
  await page.goto(`${httpURL}/login`)
  await expect(page.getByRole('heading', { name: /sign in|로그인/i })).toBeVisible()
  await page.locator('input[name="username"]').fill('owner')
  await page.locator('input[name="password"]').fill('browser-e2e-strong-password')
  await page.locator('form button[type="submit"]').click()
  await expect(page).toHaveURL(`${httpURL}/`)

  const unconfiguredResponse = await page.request.get(`${httpURL}/api/runtime/https`)
  expect(unconfiguredResponse.ok()).toBe(true)
  expect(await unconfiguredResponse.json()).toMatchObject({ enabled: false, configured: false, hosts: [], redirect_active: false })
  await page.goto(`${httpURL}/system`)
  await expect(page.getByRole('heading', { name: /system|시스템/i })).toBeVisible()
  await page.getByPlaceholder('recorder.example.com, 192.168.1.20').fill('localhost, 127.0.0.1')
  await page.getByRole('button', { name: /자체 서명 인증서 생성·교체|Create or rotate self-signed certificate/i }).click()
  await expect(page.getByText(/HTTPS 사용 가능|HTTPS ready/i)).toBeVisible()
  await expect(page.getByText(/자체 서명 인증서는 browser 경고가 표시됩니다|Browsers warn for self-signed certificates/i)).toBeVisible()
  await page.goto(`${httpURL}/system`)
  await expect(page).toHaveURL(`${httpsURL}/system`)

  const cookies = await context.cookies(httpsURL)
  const session = cookies.find(cookie => cookie.name === 'ir_session')
  const csrf = cookies.find(cookie => cookie.name === 'ir_csrf')
  expect(session?.secure).toBe(true)
  expect(session?.httpOnly).toBe(true)
  expect(csrf?.secure).toBe(true)
  expect(csrf?.httpOnly).toBe(false)

  const response = await page.request.get(`${httpsURL}/api/runtime/https`)
  expect(response.ok()).toBe(true)
  const status = await response.json()
  expect(status).toMatchObject({ enabled: true, configured: true, listening: true, certificate_ready: true, redirect_http: true, redirect_active: true, http_recovery_available: true, self_signed: true, http_port: 4173, port: Number(httpsPort) })
  expect(JSON.stringify(status)).not.toContain('PRIVATE KEY')

  expect(csrf?.value).toBeTruthy()
  const assetPath = await page.locator('script[src^="/static/"]').first().getAttribute('src')
  expect(assetPath).toBeTruthy()
  for (const path of ['/', '/system', assetPath!, '/api/setup/status', '/api/runtime/https']) {
    const redirected = await page.request.get(`${httpURL}${path}`, { maxRedirects: 0 })
    expect(redirected.status(), `${path} should use the same HTTP transition policy`).toBe(307)
    expect(redirected.headers()['cache-control']).toBe('no-store')
    const location = redirected.headers().location
    expect(location).toBeTruthy()
    if (location) expect(new URL(location).pathname).toBe(new URL(path, 'http://localhost').pathname)
  }
  const health = await page.request.get(`${httpURL}/healthz`, { maxRedirects: 0 })
  expect(health.ok()).toBe(true)
  expect(health.headers().location).toBeUndefined()
  await page.goto(`${httpURL}/system`)
  await expect(page).toHaveURL(`${httpsURL}/system`)

  const sessionValue = session?.value
  await page.goto(`${httpsURL}/system`)
  await page.getByRole('button', { name: /disable HTTPS|HTTPS 끄기/i }).click()
  await expect(page).toHaveURL(`${httpURL}/system`)
  await expect(page.getByRole('heading', { name: /system|시스템/i })).toBeVisible()
  const httpCookies = await context.cookies('http://127.0.0.1:4173')
  const httpSession = httpCookies.find(cookie => cookie.name === 'ir_session')
  const httpCsrf = httpCookies.find(cookie => cookie.name === 'ir_csrf')
  expect(httpSession?.value).toBe(sessionValue)
  expect(httpSession?.secure).toBe(false)
  expect(httpSession?.httpOnly).toBe(true)
  expect(httpCsrf?.secure).toBe(false)
  await page.reload()
  await expect(page).toHaveURL('http://127.0.0.1:4173/system')
  await expect(page.getByRole('heading', { name: /system|시스템/i })).toBeVisible()
  const nonHTMLSystem = await page.request.get(`${httpURL}/system`, { headers: { Accept: 'application/json' } })
  expect(nonHTMLSystem.status()).toBe(404)
  const disabledResponse = await page.request.get(`${httpURL}/api/runtime/https`)
  expect(disabledResponse.ok()).toBe(true)
  const disabled = await disabledResponse.json()
  expect(disabled).toMatchObject({ enabled: false, configured: true, redirect_http: true, redirect_active: false })

  await page.getByRole('button', { name: /Re-enable saved HTTPS settings|저장된 HTTPS 설정 다시 켜기/i }).click()
  await expect(page).toHaveURL(`${httpsURL}/system`)
  const reenabledCookies = await context.cookies(httpsURL)
  const reenabledSession = reenabledCookies.find(cookie => cookie.name === 'ir_session')
  const reenabledCsrf = reenabledCookies.find(cookie => cookie.name === 'ir_csrf')
  expect(reenabledSession?.value).toBe(sessionValue)
  expect(reenabledSession?.secure).toBe(true)
  expect(reenabledSession?.httpOnly).toBe(true)
  expect(reenabledCsrf?.secure).toBe(true)

  await page.goto(`${httpURL}/system`)
  await expect(page).toHaveURL(`${httpsURL}/system`)
  const reactivatedRedirect = await page.request.get(`${httpURL}/static/ui/assets/missing.js`, { maxRedirects: 0 })
  expect(reactivatedRedirect.status()).toBe(307)
  expect(reactivatedRedirect.headers()['cache-control']).toBe('no-store')
  const finalStatus = await page.request.get(`${httpsURL}/api/runtime/https`)
  expect(await finalStatus.json()).toMatchObject({ enabled: true, configured: true, redirect_active: true })
})
