import { ed25519, x25519 } from '@noble/curves/ed25519.js'
import { entropyToMnemonic, mnemonicToEntropy } from '@scure/bip39'
import { wordlist } from '@scure/bip39/wordlists/english.js'

const ENTROPY_BYTES = 32
const RECOVERY_MAGIC = new Uint8Array([0x49, 0x52, 0x52, 0x43]) // IRRC
const RECOVERY_VERSION = 1
const CHECKSUM_BYTES = 16
const RECOVERY_FILE_BYTES = RECOVERY_MAGIC.length + 1 + ENTROPY_BYTES + CHECKSUM_BYTES
const HKDF_SALT = new TextEncoder().encode('IR/recovery/v1/HKDF-SHA256/salt')
const SIGNING_INFO = new TextEncoder().encode('IR/recovery/v1/identity-signing-ed25519-seed')
const ENCRYPTION_INFO = new TextEncoder().encode('IR/recovery/v1/recovery-encryption-x25519-scalar')
const IDENTITY_ID_DOMAIN = new TextEncoder().encode('IR/global-identity-id/v1\0')
const IDENTITY_KEY_DOMAIN = new TextEncoder().encode('IR/identity-key/v1\0')
const IDENTITY_EVENT_SIGN_DOMAIN = new TextEncoder().encode('IR/identity-event/v1/sign\0')
const RECOVERY_CHECKSUM_DOMAIN = new TextEncoder().encode('IR/recovery-file/v1/check\0')

export type RecoveryMaterial = {
  entropy: Uint8Array
  mnemonic: string
  file: Uint8Array
}

export type RecoveryKeyMaterial = {
  identityId: string
  signerKeyId: string
  signingPublicKey: Uint8Array
  encryptionPublicKey: Uint8Array
  /** Secret seed bytes; keep only in memory and call destroy() when done. */
  signingSeed: Uint8Array
  /** Secret X25519 scalar bytes; keep only in memory and call destroy() when done. */
  encryptionPrivateKey: Uint8Array
  destroy: () => void
}

export type SignedIdentityEvent = {
  canonicalEvent: Uint8Array
  signature: Uint8Array
}

function cryptoAPI(): Crypto {
  const api = globalThis.crypto
  if (!api?.getRandomValues || !api.subtle) throw new Error('secure browser cryptography is unavailable')
  return api
}

function concat(...parts: Uint8Array[]): Uint8Array {
  const result = new Uint8Array(parts.reduce((length, part) => length + part.length, 0))
  let offset = 0
  for (const part of parts) {
    result.set(part, offset)
    offset += part.length
  }
  return result
}

function base64url(bytes: Uint8Array): string {
  let binary = ''
  for (let offset = 0; offset < bytes.length; offset += 0x8000) {
    binary += String.fromCharCode(...bytes.subarray(offset, Math.min(offset + 0x8000, bytes.length)))
  }
  return btoa(binary).replaceAll('+', '-').replaceAll('/', '_').replace(/=+$/u, '')
}

function equalBytes(left: Uint8Array, right: Uint8Array): boolean {
  if (left.length !== right.length) return false
  let diff = 0
  for (let index = 0; index < left.length; index += 1) diff |= left[index] ^ right[index]
  return diff === 0
}

function requireEntropy(entropy: Uint8Array): void {
  if (!(entropy instanceof Uint8Array) || entropy.length !== ENTROPY_BYTES) {
    throw new TypeError('recovery entropy must contain exactly 256 bits')
  }
}

export function generateRecoveryEntropy(): Uint8Array {
  return cryptoAPI().getRandomValues(new Uint8Array(ENTROPY_BYTES))
}

export function recoveryMnemonic(entropy: Uint8Array): string {
  requireEntropy(entropy)
  return entropyToMnemonic(entropy, wordlist)
}

export function recoveryEntropyFromMnemonic(mnemonic: string): Uint8Array {
  if (typeof mnemonic !== 'string') throw new TypeError('recovery phrase must be text')
  const normalized = mnemonic.trim().split(/\s+/u).join(' ').toLowerCase().normalize('NFKD')
  const entropy = mnemonicToEntropy(normalized, wordlist)
  requireEntropy(entropy)
  return Uint8Array.from(entropy)
}

/**
 * The versioned file contains exactly the same entropy as the BIP-39 phrase.
 * Its truncated SHA-256 checksum detects accidental corruption; it is not a
 * MAC, encryption, or protection against an attacker who can rewrite the file.
 */
export async function encodeRecoveryFile(entropy: Uint8Array): Promise<Uint8Array> {
  requireEntropy(entropy)
  const body = concat(RECOVERY_MAGIC, new Uint8Array([RECOVERY_VERSION]), entropy)
  const digest = new Uint8Array(await cryptoAPI().subtle.digest('SHA-256', concat(RECOVERY_CHECKSUM_DOMAIN, body)))
  return concat(body, digest.subarray(0, CHECKSUM_BYTES))
}

export async function decodeRecoveryFile(file: Uint8Array): Promise<Uint8Array> {
  if (!(file instanceof Uint8Array) || file.length !== RECOVERY_FILE_BYTES) {
    throw new TypeError('recovery file length is invalid')
  }
  if (!equalBytes(file.subarray(0, RECOVERY_MAGIC.length), RECOVERY_MAGIC)) {
    throw new TypeError('recovery file header is invalid')
  }
  if (file[RECOVERY_MAGIC.length] !== RECOVERY_VERSION) {
    throw new TypeError('recovery file version is unsupported')
  }
  const body = file.subarray(0, RECOVERY_FILE_BYTES - CHECKSUM_BYTES)
  const expected = new Uint8Array(await cryptoAPI().subtle.digest('SHA-256', concat(RECOVERY_CHECKSUM_DOMAIN, body)))
  if (!equalBytes(file.subarray(body.length), expected.subarray(0, CHECKSUM_BYTES))) {
    throw new TypeError('recovery file checksum is invalid')
  }
  return Uint8Array.from(file.subarray(RECOVERY_MAGIC.length + 1, body.length))
}

async function derive32(entropy: Uint8Array, info: Uint8Array): Promise<Uint8Array> {
  const key = await cryptoAPI().subtle.importKey('raw', entropy, 'HKDF', false, ['deriveBits'])
  const bits = await cryptoAPI().subtle.deriveBits(
    { name: 'HKDF', hash: 'SHA-256', salt: HKDF_SALT, info },
    key,
    256,
  )
  return new Uint8Array(bits)
}

export async function deriveRecoveryKeys(entropy: Uint8Array): Promise<RecoveryKeyMaterial> {
  requireEntropy(entropy)
  const signingSeed = await derive32(entropy, SIGNING_INFO)
  const encryptionPrivateKey = await derive32(entropy, ENCRYPTION_INFO)
  try {
    const signingPublicKey = ed25519.getPublicKey(signingSeed)
    const encryptionPublicKey = x25519.getPublicKey(encryptionPrivateKey)
    const idDigest = new Uint8Array(await cryptoAPI().subtle.digest('SHA-256', concat(IDENTITY_ID_DOMAIN, signingPublicKey)))
    const keyDigest = new Uint8Array(await cryptoAPI().subtle.digest('SHA-256', concat(IDENTITY_KEY_DOMAIN, signingPublicKey)))
    let destroyed = false
    return {
      identityId: `gi_${base64url(idDigest.subarray(0, 16))}`,
      signerKeyId: `ik_${base64url(keyDigest)}`,
      signingPublicKey,
      encryptionPublicKey,
      signingSeed,
      encryptionPrivateKey,
      destroy: () => {
        if (destroyed) return
        signingSeed.fill(0)
        encryptionPrivateKey.fill(0)
        destroyed = true
      },
    }
  } catch (error) {
    signingSeed.fill(0)
    encryptionPrivateKey.fill(0)
    throw error
  }
}

export async function createRecoveryMaterial(): Promise<RecoveryMaterial> {
  const entropy = generateRecoveryEntropy()
  try {
    return {
      entropy,
      mnemonic: recoveryMnemonic(entropy),
      file: await encodeRecoveryFile(entropy),
    }
  } catch (error) {
    entropy.fill(0)
    throw error
  }
}

export function signRecoveryMessage(message: Uint8Array, signingSeed: Uint8Array): Uint8Array {
  if (signingSeed.length !== 32) throw new TypeError('identity signing seed is invalid')
  return ed25519.sign(message, signingSeed)
}

export function signIdentityEvent(keys: RecoveryKeyMaterial, input: Record<string, unknown>): SignedIdentityEvent {
  const canonicalEvent = new TextEncoder().encode(canonicalIdentityJSON(input))
  const signature = signRecoveryMessage(concat(IDENTITY_EVENT_SIGN_DOMAIN, canonicalEvent), keys.signingSeed)
  return { canonicalEvent, signature }
}

export function signIdentityGenesis(keys: RecoveryKeyMaterial, challenge: string, credentialId: string, issuedAt = new Date()): SignedIdentityEvent {
  if (!challenge || !credentialId) throw new TypeError('identity genesis ceremony binding is missing')
  return signIdentityEvent(keys, {
    event_version: 'ir.identity.event/1',
    identity_id: keys.identityId,
    issued_at: issuedAt.toISOString(),
    operation: 'genesis',
    payload: {
      credential_id: credentialId,
      identity_format: 'global-identity/v1',
      recovery_encryption_public_key: base64url(keys.encryptionPublicKey),
      webauthn_challenge: challenge,
    },
    previous_event_hash: null,
    sequence: '0',
    signer_key_id: keys.signerKeyId,
    signer_public_key: base64url(keys.signingPublicKey),
  })
}

export function signIdentitySuccessor(
  keys: RecoveryKeyMaterial,
  operation: 'credential_add' | 'credential_revoke' | 'node_delegate' | 'node_revoke' | 'fork_resolve',
  sequence: string,
  previousEventHash: string,
  payload: Record<string, string>,
  issuedAt = new Date(),
): SignedIdentityEvent {
  if (!/^(0|[1-9][0-9]{0,19})$/u.test(sequence) || !/^sha256:[0-9a-f]{64}$/u.test(previousEventHash)) {
    throw new TypeError('identity history head is invalid')
  }
  return signIdentityEvent(keys, {
    event_version: 'ir.identity.event/1',
    identity_id: keys.identityId,
    issued_at: issuedAt.toISOString(),
    operation,
    payload,
    previous_event_hash: previousEventHash,
    sequence,
    signer_key_id: keys.signerKeyId,
    signer_public_key: base64url(keys.signingPublicKey),
  })
}

export function canonicalIdentityJSON(value: unknown): string {
  if (value === null) return 'null'
  if (typeof value === 'boolean') return JSON.stringify(value)
  if (typeof value === 'string') {
    for (let index = 0; index < value.length; index += 1) {
      const unit = value.charCodeAt(index)
      if (unit >= 0xd800 && unit <= 0xdbff) {
        const next = value.charCodeAt(index + 1)
        if (!(next >= 0xdc00 && next <= 0xdfff)) throw new TypeError('identity event contains an unpaired surrogate')
        index += 1
      } else if (unit >= 0xdc00 && unit <= 0xdfff) {
        throw new TypeError('identity event contains an unpaired surrogate')
      }
    }
    return JSON.stringify(value)
  }
  if (typeof value !== 'object') throw new TypeError('identity event contains an unsupported value')
  if (Array.isArray(value)) return `[${value.map(item => canonicalIdentityJSON(item)).join(',')}]`
  if (Object.getPrototypeOf(value) !== Object.prototype) throw new TypeError('identity event contains a non-plain object')
  const object = value as Record<string, unknown>
  const keys = Object.keys(object).sort()
  for (const key of keys) {
    if (!/^[a-z][a-z0-9_]*$/u.test(key)) throw new TypeError('identity event property is outside the protocol profile')
  }
  return `{${keys.map(key => `${JSON.stringify(key)}:${canonicalIdentityJSON(object[key])}`).join(',')}}`
}
