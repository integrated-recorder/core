import { expect, test } from '@playwright/test'
import { readFile } from 'node:fs/promises'
import { resolve } from 'node:path'

async function applyPlaybackCSP(page: import('@playwright/test').Page): Promise<void> {
  await page.route('**/e2e/peer-playback-webkit.html', route => route.continue({
    headers: {
      ...route.request().headers(),
      'Content-Security-Policy': "default-src 'none'; base-uri 'none'; connect-src 'self' blob: ws://127.0.0.1:4174; font-src 'self'; img-src 'self' data:; media-src 'self' blob:; script-src 'self'; style-src 'self'; worker-src 'self' blob:; frame-ancestors 'none'; form-action 'self'",
    },
  }))
}

test('WebKit plays and seeks encrypted peer HLS media through MSE', async ({ page }) => {
  await applyPlaybackCSP(page)
  await page.goto('/e2e/peer-playback-webkit.html')
  const result = page.locator('#result')
  await expect.poll(async () => {
    try { return JSON.parse(await result.textContent() ?? '{}').stage } catch { return 'running' }
  }).toBe('ready')
  await page.getByRole('button', { name: 'Play encrypted fixture' }).click()
  await expect.poll(async () => {
    try { return JSON.parse(await result.textContent() ?? '{}').stage } catch { return 'running' }
  }, { timeout: 20_000 }).toBe('complete')
  const report = JSON.parse(await result.textContent() ?? '{}') as {
    error?: string
    secure_context?: boolean
    webcrypto?: boolean
    hls_mse?: boolean
    fragment_decrypted?: boolean
    mse_playback?: boolean
    seek_success?: boolean
    encrypted_objects?: number
    duration?: number
    seek_seconds?: number
  }
  expect(report.error).toBeUndefined()
  expect(report.secure_context).toBe(true)
  expect(report.webcrypto).toBe(true)
  expect(report.hls_mse).toBe(true)
  expect(report.fragment_decrypted).toBe(true)
  expect(report.mse_playback).toBe(true)
  expect(report.seek_success).toBe(true)
  expect(report.encrypted_objects).toBe(4)
  expect(report.duration).toBeGreaterThan(2.5)
  expect(report.seek_seconds).toBeGreaterThan(2)
})

test('WebKit opens the Go-issued Passkey playback envelope only for its bound scope', async ({ page }) => {
  await applyPlaybackCSP(page)
  const vector = await readFile(resolve(process.cwd(), '../protocol/cryptoarchive-v1/session-key-envelope-test-vector.json'), 'utf8')
  await page.route('**/session-key-envelope-test-vector.json', route => route.fulfill({
    status: 200,
    contentType: 'application/json',
    body: vector,
  }))
  await page.goto('/e2e/peer-playback-webkit.html')
  const result = await page.evaluate(async () => {
    const testVector = await fetch('/session-key-envelope-test-vector.json').then(response => response.json()) as {
      recipient_private_key_hex: string
      dek_hex: string
      binding: {
        archive_id: string
        snapshot_id: string
        key_epoch: string
        owner_subject_id: string
        grant_hash: string
        supplier_node_id: string
      }
      envelope: import('../src/identity/cryptoarchive').SessionDEKEnvelope
    }
    const cryptoArchive = await import('/src/identity/cryptoarchive.ts')
    const privateKey = Uint8Array.from(testVector.recipient_private_key_hex.match(/.{2}/gu) ?? [], pair => Number.parseInt(pair, 16))
    const expected = Uint8Array.from(testVector.dek_hex.match(/.{2}/gu) ?? [], pair => Number.parseInt(pair, 16))
    const binding = {
      archiveId: testVector.binding.archive_id,
      snapshotId: testVector.binding.snapshot_id,
      keyEpoch: testVector.binding.key_epoch,
      ownerSubjectId: testVector.binding.owner_subject_id,
      grantHash: testVector.binding.grant_hash,
      supplierNodeId: testVector.binding.supplier_node_id,
    }
    try {
      const opened = await cryptoArchive.openSessionDEKEnvelope(testVector.envelope, privateKey, binding)
      const keyMatches = opened.every((byte, index) => byte === expected[index])
      opened.fill(0)
      let wrongGrantRejected = false
      try {
        await cryptoArchive.openSessionDEKEnvelope(testVector.envelope, privateKey, { ...binding, grantHash: `sha256:${'c'.repeat(64)}` })
      } catch {
        wrongGrantRejected = true
      }
      return { secureContext: window.isSecureContext, keyMatches, wrongGrantRejected }
    } finally {
      privateKey.fill(0)
      expected.fill(0)
    }
  })
  expect(result).toEqual({ secureContext: true, keyMatches: true, wrongGrantRejected: true })
})
