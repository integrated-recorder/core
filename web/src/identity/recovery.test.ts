import { beforeEach, afterEach, describe, expect, it, vi } from 'vitest'
import { webcrypto } from 'node:crypto'
import { ed25519 } from '@noble/curves/ed25519.js'
import {
  createRecoveryMaterial,
  decodeRecoveryFile,
  deriveRecoveryKeys,
  encodeRecoveryFile,
  recoveryEntropyFromMnemonic,
  recoveryMnemonic,
  signRecoveryMessage,
  signIdentityGenesis,
  signIdentitySuccessor,
} from './recovery'

const vectorEntropy = new Uint8Array(32)
const vectorMnemonic = 'abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon art'

describe('global identity offline recovery', () => {
  beforeEach(() => vi.stubGlobal('crypto', webcrypto as unknown as Crypto))
  afterEach(() => vi.unstubAllGlobals())

  it('round-trips the same 256-bit entropy through a 24-word BIP-39 phrase', () => {
    expect(recoveryMnemonic(vectorEntropy)).toBe(vectorMnemonic)
    expect(recoveryEntropyFromMnemonic(vectorMnemonic)).toEqual(vectorEntropy)
    expect(recoveryEntropyFromMnemonic(`${vectorMnemonic.toUpperCase()}  `)).toEqual(vectorEntropy)
    expect(() => recoveryEntropyFromMnemonic(`${vectorMnemonic.slice(0, -3)}bad`)).toThrow()
  })

  it('round-trips the same entropy in the checksummed versioned binary recovery file', async () => {
    const file = await encodeRecoveryFile(vectorEntropy)
    expect(Buffer.from(file).toString('hex')).toBe('49525243010000000000000000000000000000000000000000000000000000000000000000901d12b3ef6adbcf44ebb376697cb159')
    expect(await decodeRecoveryFile(file)).toEqual(vectorEntropy)
    const corrupted = Uint8Array.from(file)
    corrupted[12] ^= 1
    await expect(decodeRecoveryFile(corrupted)).rejects.toThrow(/checksum/u)
    const unsupported = Uint8Array.from(file)
    unsupported[4] = 2
    await expect(decodeRecoveryFile(unsupported)).rejects.toThrow(/version/u)
    await expect(decodeRecoveryFile(file.subarray(0, file.length - 1))).rejects.toThrow(/length/u)
  })

  it('uses distinct HKDF labels and reproduces the public cross-language key vector', async () => {
    const keys = await deriveRecoveryKeys(vectorEntropy)
    try {
      expect(Buffer.from(keys.signingSeed).toString('hex')).toBe('4a42772013744ac49444cabe191141fb5a6051362dffc26dd9849051deb13fa3')
      expect(Buffer.from(keys.signingPublicKey).toString('base64url')).toBe('-purqCj6vOJ3KIQHs7yX2Jw39v94byHCppGn2aSEjIw')
      expect(Buffer.from(keys.encryptionPrivateKey).toString('hex')).toBe('624215aa9467ad5f2a32526cca20ff0e5faa5cba638f90ff13ce913f3df7718e')
      expect(Buffer.from(keys.encryptionPublicKey).toString('base64url')).toBe('z-xHbQnvbzfevTP7fJ0n7HZSJGSORyKXPpL7PRNdAlc')
      expect(keys.identityId).toBe('gi_FZHpkMERS415hhqLqd6caA')
      expect(keys.signerKeyId).toBe('ik_kQ-SpHzCPkqIJN1PafegM7LIw7TNIsLeIjH6TqpLUZU')

      const message = new TextEncoder().encode('test-only signed genesis proof')
      const signature = signRecoveryMessage(message, keys.signingSeed)
      expect(ed25519.verify(signature, message, keys.signingPublicKey)).toBe(true)
      const challenge = Buffer.alloc(32, 7).toString('base64url')
      const credentialID = Buffer.from('credential-for-test').toString('base64url')
      const genesis = signIdentityGenesis(keys, challenge, credentialID, new Date('2026-01-02T03:04:05.000Z'))
      const canonical = new TextDecoder().decode(genesis.canonicalEvent)
      expect(canonical).toContain(`"credential_id":"${credentialID}"`)
      expect(canonical.indexOf('event_version')).toBeLessThan(canonical.indexOf('identity_id'))
      const signedEventBytes = new Uint8Array([...new TextEncoder().encode('IR/identity-event/v1/sign\0'), ...genesis.canonicalEvent])
      expect(ed25519.verify(genesis.signature, signedEventBytes, keys.signingPublicKey)).toBe(true)
      const successor = signIdentitySuccessor(keys, 'credential_revoke', '1', `sha256:${'a'.repeat(64)}`, { credential_id: credentialID }, new Date('2026-01-02T03:05:05.000Z'))
      expect(new TextDecoder().decode(successor.canonicalEvent)).toContain('credential_revoke')
      expect(keys.signingSeed).not.toEqual(keys.encryptionPrivateKey)
    } finally {
      keys.destroy()
    }
    expect(keys.signingSeed.every(byte => byte === 0)).toBe(true)
    expect(keys.encryptionPrivateKey.every(byte => byte === 0)).toBe(true)
  })

  it('creates a recovery phrase and file from browser CSPRNG entropy only', async () => {
    const material = await createRecoveryMaterial()
    try {
      expect(material.entropy).toHaveLength(32)
      expect(material.mnemonic.trim().split(/\s+/u)).toHaveLength(24)
      expect(recoveryEntropyFromMnemonic(material.mnemonic)).toEqual(material.entropy)
      expect(await decodeRecoveryFile(material.file)).toEqual(material.entropy)
    } finally {
      material.entropy.fill(0)
      material.file.fill(0)
    }
  })

  it('rejects values that cannot be represented in canonical identity event JSON', async () => {
    const keys = await deriveRecoveryKeys(vectorEntropy)
    try {
      expect(() => signIdentitySuccessor(keys, 'credential_add', '1', `sha256:${'b'.repeat(64)}`, { unsupported: undefined as unknown as string })).toThrow(/unsupported value/u)
      expect(() => signIdentitySuccessor(keys, 'credential_add', '1', `sha256:${'b'.repeat(64)}`, { invalid_key: 'x', 'not-valid': 'x' })).toThrow(/property/u)
      expect(() => signIdentitySuccessor(keys, 'credential_add', '1', `sha256:${'b'.repeat(64)}`, { value: '\ud800' })).toThrow(/surrogate/u)
      expect(() => signIdentitySuccessor(keys, 'credential_add', '1', `sha256:${'b'.repeat(64)}`, { value: 1 } as unknown as Record<string, string>)).toThrow(/unsupported value/u)
    } finally { keys.destroy() }
  })
})
