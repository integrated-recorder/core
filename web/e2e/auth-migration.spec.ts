import { expect, test } from '@playwright/test'
import { readFileSync } from 'node:fs'
import { join } from 'node:path'

const dataDir = process.env.IR_SETUP_E2E_DATA_DIR
if (!dataDir) throw new Error('The native auth migration runner did not provide its isolated data directory.')

const ownerPassword = 'legacy-owner-password'
const memberPassword = 'legacy-member-password'

test('Runtime Host startup migrates v1 owner/member identities without changing credentials or privilege', async ({ browser }) => {
  const store = JSON.parse(readFileSync(join(dataDir!, 'security', 'users.json'), 'utf8')) as {
    version: number
    users: { user_id: string; login: string; roles: string[]; status: string; password_hash: string }[]
  }
  expect(store.version).toBe(2)
  expect(store.users).toHaveLength(2)
  expect(store.users).toEqual(expect.arrayContaining([
    expect.objectContaining({ user_id: 'usr-0123456789abcdef0123456789abcdef', login: 'legacy-owner', roles: ['owner'], status: 'active', password_hash: '$2a$04$vsPhwLYUKLGceXLMce3b4u6NzXaeZLPteH4NawlbAZo4O48EQBRjO' }),
    expect.objectContaining({ user_id: 'usr-1123456789abcdef0123456789abcdef', login: 'legacy-member', roles: ['operator'], status: 'active', password_hash: '$2a$04$7l/aOZSOTo1VQrMyFmyzAOvAZhCAzzvbPEBq7Wcbk8H7JfmadfMaO' }),
  ]))
  const installation = JSON.parse(readFileSync(join(dataDir!, 'runtime', 'installation.json'), 'utf8')) as { state: string }
  expect(installation.state).toBe('ready')

  const ownerContext = await browser.newContext({ baseURL: 'http://127.0.0.1:4173', locale: 'en-US' })
  const ownerPage = await ownerContext.newPage()
  await ownerPage.goto('/login')
  await ownerPage.getByLabel('Account name').fill('legacy-owner')
  await ownerPage.getByLabel('Password').fill(ownerPassword)
  await ownerPage.getByRole('button', { name: 'Sign in' }).click()
  await expect(ownerPage).toHaveURL('/')
  const owner = await ownerPage.request.get('/api/user').then(response => response.json())
  expect(owner).toMatchObject({
    user_id: 'usr-0123456789abcdef0123456789abcdef', login: 'legacy-owner', roles: ['owner'], status: 'active',
  })

  const memberContext = await browser.newContext({ baseURL: 'http://127.0.0.1:4173', locale: 'en-US' })
  const memberPage = await memberContext.newPage()
  await memberPage.goto('/login')
  await memberPage.getByLabel('Account name').fill('legacy-member')
  await memberPage.getByLabel('Password').fill(memberPassword)
  await memberPage.getByRole('button', { name: 'Sign in' }).click()
  await expect(memberPage).toHaveURL('/')
  const member = await memberPage.request.get('/api/user').then(response => response.json())
  expect(member).toMatchObject({
    user_id: 'usr-1123456789abcdef0123456789abcdef', login: 'legacy-member', roles: ['operator'], status: 'active',
  })
  expect((await memberPage.request.get('/api/dashboard')).status()).toBe(200)
  expect((await memberPage.request.get('/api/users')).status()).toBe(403)

  await ownerContext.close()
  await memberContext.close()
})
