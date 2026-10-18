import { expect, test } from '@playwright/test'
import { readFileSync } from 'node:fs'
import { join } from 'node:path'

const httpsURL = readFileSync(join(process.env.IR_E2E_DATA_DIR!, 'e2e-https-url'), 'utf8').trim()

test('Runtime Host direct HTTPS serves the browser UI and Secure authenticated session', async ({ page, context }) => {
  await page.goto(`${httpsURL}/login`)
  await expect(page.getByRole('heading', { name: /sign in|로그인/i })).toBeVisible()
  await page.locator('input[name="username"]').fill('owner')
  await page.locator('input[name="password"]').fill('browser-e2e-strong-password')
  await page.locator('form button[type="submit"]').click()
  await expect(page).toHaveURL(`${httpsURL}/`)

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
  expect(status).toMatchObject({ enabled: true, listening: true, certificate_ready: true, self_signed: true })
  expect(JSON.stringify(status)).not.toContain('PRIVATE KEY')
})
