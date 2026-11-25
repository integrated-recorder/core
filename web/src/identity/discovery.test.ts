import { webcrypto } from 'node:crypto'
import { readFile } from 'node:fs/promises'
import { resolve } from 'node:path'
import { x25519 } from '@noble/curves/ed25519.js'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { deriveRecoveryKeys } from './recovery'
import { openNodeAddressEnvelope, sealNodeAddressEnvelope, type NodeAddressContext } from './discovery'

const vectorPath = resolve(process.cwd(), '../protocol/discovery-v1/address-envelope-test-vector.json')

describe('discovery recipient-encrypted address envelope', () => {
  beforeEach(() => vi.stubGlobal('crypto', webcrypto as unknown as Crypto))
  afterEach(() => vi.unstubAllGlobals())

  it('opens the Go shared HPKE vector only for the bound recovery key and identity head', async () => {
    const vector = JSON.parse(await readFile(vectorPath, 'utf8')) as {
      test_only: boolean
      context: { global_identity_hint: string; identity_head_hash: string; node_id: string }
      recipient_private_key_hex: string
      plaintext_json: string
      address_envelope: string
      browser_address_envelope: string
    }
    expect(vector.test_only).toBe(true)
    const privateKey = Uint8Array.from(Buffer.from(vector.recipient_private_key_hex, 'hex'))
    const keys = { encryptionPrivateKey: privateKey, encryptionPublicKey: x25519.getPublicKey(privateKey) }
    const context: NodeAddressContext = {
      globalIdentityHint: vector.context.global_identity_hint,
      identityHeadHash: vector.context.identity_head_hash,
      nodeId: vector.context.node_id,
    }
    for (const envelope of [vector.address_envelope, vector.browser_address_envelope]) {
      const opened = await openNodeAddressEnvelope(envelope, keys, context)
      expect(JSON.stringify(opened)).toBe(vector.plaintext_json)
      await expect(openNodeAddressEnvelope(envelope, keys, {
        ...context, identityHeadHash: `sha256:${'b'.repeat(64)}`,
      })).rejects.toThrow()
    }
  })

  it('seals canonical address JSON without exposing it and round-trips with the recovery key', async () => {
    const recovery = await deriveRecoveryKeys(new Uint8Array(32).fill(0x17))
    const context: NodeAddressContext = {
      globalIdentityHint: 'gih_AAAAAAAAAAAAAAAAAAAAAA',
      identityHeadHash: `sha256:${'a'.repeat(64)}`,
      nodeId: 'nd_AAAAAAAAAAAAAAAAAAAAAA',
    }
    const payload = { addresses: ['wss://private-node.example.test/signal'] }
    try {
      const envelope = await sealNodeAddressEnvelope(recovery, context, payload)
      expect(envelope).not.toContain('private-node.example.test')
      expect(await openNodeAddressEnvelope(envelope, recovery, context)).toEqual(payload)
      await expect(openNodeAddressEnvelope(envelope, recovery, { ...context, nodeId: 'nd_BBBBBBBBBBBBBBBBBBBBBB' })).rejects.toThrow()
    } finally {
      recovery.destroy()
    }
  })
})
