import { expect, test } from '@playwright/test'

test('WebKit decodes the QR image generated for a schema-shaped Node enrollment payload', async ({ page }) => {
  const proof = {
    identity_id: `gi_${'A'.repeat(22)}`,
    previous_event_hash: `sha256:${'b'.repeat(64)}`,
    node_id: `nd_${'C'.repeat(22)}`,
    node_public_key: 'A'.repeat(43),
    enrollment_nonce: 'A'.repeat(43),
    node_proof: 'A'.repeat(86),
    expires_at: new Date(Date.now() + 60_000).toISOString(),
  }
  const payload = JSON.stringify(proof)
  await page.goto('/e2e/peer-playback-webkit.html')
  const result = await page.evaluate(async (json) => {
    const { roundTripEnrollmentQR } = await import('/e2e/node-enrollment-qr-browser.ts')
    return roundTripEnrollmentQR(json)
  }, payload)

  expect(result.dataURL).toMatch(/^data:image\/png;base64,/u)
  expect(result.decoded).toBe(payload)
  expect(JSON.parse(result.decoded)).toEqual(proof)
})
