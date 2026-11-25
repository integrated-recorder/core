import { Aes256Gcm, CipherSuite, HkdfSha256 } from '@hpke/core'
import { DhkemX25519HkdfSha256 } from '@hpke/dhkem-x25519'
import { x25519 } from '@noble/curves/ed25519.js'
import { sha256 } from '@noble/hashes/sha2.js'
import { canonicalIdentityJSON } from './recovery'

export const NODE_ADDRESS_ENVELOPE_VERSION = 'ir.node-address-envelope/1'
const MAX_PLAINTEXT_BYTES = 8 * 1024
const MAX_ENVELOPE_BYTES = 16 * 1024
const KEY_HASH_DOMAIN = new TextEncoder().encode('IR/node-address-envelope/v1/recipient-key\0')
const INFO_DOMAIN = new TextEncoder().encode('IR/node-address-envelope/v1/hpke-info\0')
const AAD_DOMAIN = new TextEncoder().encode('IR/node-address-envelope/v1/hpke-aad\0')

export type NodeAddressContext = {
  globalIdentityHint: string
  identityHeadHash: string
  nodeId: string
}

export type NodeAddressEnvelope = {
  envelope_version: typeof NODE_ADDRESS_ENVELOPE_VERSION
  recipient_public_key_sha256: string
  kem: 'DHKEM(X25519,HKDF-SHA256)'
  kdf: 'HKDF-SHA256'
  aead: 'AES-256-GCM'
  encapsulation: string
  ciphertext: string
}

export type AddressEncryptionKey = {
  encryptionPrivateKey: Uint8Array
  encryptionPublicKey: Uint8Array
}

function concat(...parts: Uint8Array[]): Uint8Array {
  const result = new Uint8Array(parts.reduce((size, part) => size + part.length, 0))
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

function fromBase64url(value: string, expectedLength?: number): Uint8Array {
  if (!/^[A-Za-z0-9_-]+$/u.test(value)) throw new TypeError('node address envelope is invalid')
  const padded = value.replaceAll('-', '+').replaceAll('_', '/') + '='.repeat((4 - value.length % 4) % 4)
  const binary = atob(padded)
  const result = Uint8Array.from(binary, character => character.charCodeAt(0))
  if ((expectedLength !== undefined && result.length !== expectedLength) || base64url(result) !== value) {
    throw new TypeError('node address envelope is invalid')
  }
  return result
}

function equalBytes(left: Uint8Array, right: Uint8Array): boolean {
  if (left.length !== right.length) return false
  let difference = 0
  for (let index = 0; index < left.length; index += 1) difference |= left[index] ^ right[index]
  return difference === 0
}

function validateContext(context: NodeAddressContext): void {
  if (!/^gih_[A-Za-z0-9_-]{22}$/u.test(context.globalIdentityHint)
    || !/^sha256:[0-9a-f]{64}$/u.test(context.identityHeadHash)
    || !/^nd_[A-Za-z0-9_-]{22}$/u.test(context.nodeId)) {
    throw new TypeError('node address envelope context is invalid')
  }
}

function publicKeyHash(publicKey: Uint8Array): string {
  return `sha256:${Array.from(sha256(concat(KEY_HASH_DOMAIN, publicKey)), byte => byte.toString(16).padStart(2, '0')).join('')}`
}

function envelopeContext(context: NodeAddressContext, keyHash: string): Record<string, string> {
  return {
    envelope_version: NODE_ADDRESS_ENVELOPE_VERSION,
    global_identity_hint: context.globalIdentityHint,
    identity_head_hash: context.identityHeadHash,
    node_id: context.nodeId,
    recipient_public_key_sha256: keyHash,
  }
}

function suiteForDiscoveryEnvelope(): CipherSuite {
  return new CipherSuite({ kem: new DhkemX25519HkdfSha256(), kdf: new HkdfSha256(), aead: new Aes256Gcm() })
}

function validateEnvelope(value: unknown): NodeAddressEnvelope {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) throw new TypeError('node address envelope is invalid')
  const envelope = value as Partial<NodeAddressEnvelope>
  const fields = Object.keys(value).sort()
  const expected = ['aead', 'ciphertext', 'encapsulation', 'envelope_version', 'kdf', 'kem', 'recipient_public_key_sha256']
  if (fields.length !== expected.length || fields.some((field, index) => field !== expected[index])
    || envelope.envelope_version !== NODE_ADDRESS_ENVELOPE_VERSION
    || !/^sha256:[0-9a-f]{64}$/u.test(envelope.recipient_public_key_sha256 ?? '')
    || envelope.kem !== 'DHKEM(X25519,HKDF-SHA256)' || envelope.kdf !== 'HKDF-SHA256' || envelope.aead !== 'AES-256-GCM'
    || typeof envelope.encapsulation !== 'string' || typeof envelope.ciphertext !== 'string') {
    throw new TypeError('node address envelope is invalid')
  }
  fromBase64url(envelope.encapsulation, 32)
  const ciphertext = fromBase64url(envelope.ciphertext)
  if (ciphertext.length < 17 || ciphertext.length > MAX_PLAINTEXT_BYTES + 16) throw new TypeError('node address envelope is invalid')
  return envelope as NodeAddressEnvelope
}

/** Encrypts a small canonical JSON address/ICE document to the offline recovery key. */
export async function sealNodeAddressEnvelope(
  keys: AddressEncryptionKey,
  context: NodeAddressContext,
  payload: unknown,
): Promise<string> {
  validateContext(context)
  if (keys.encryptionPrivateKey.length !== 32 || keys.encryptionPublicKey.length !== 32) throw new TypeError('recovery key material is invalid')
  if (!equalBytes(x25519.getPublicKey(keys.encryptionPrivateKey), keys.encryptionPublicKey)) throw new TypeError('recovery key material is inconsistent')
  const plaintext = new TextEncoder().encode(canonicalIdentityJSON(payload))
  if (!plaintext.length || plaintext.length > MAX_PLAINTEXT_BYTES) throw new TypeError('node address payload is too large')
  let ciphertext: Uint8Array | undefined
  try {
    const keyHash = publicKeyHash(keys.encryptionPublicKey)
    const contextBytes = new TextEncoder().encode(canonicalIdentityJSON(envelopeContext(context, keyHash)))
    const suite = suiteForDiscoveryEnvelope()
    const recipient = await suite.kem.deserializePublicKey(keys.encryptionPublicKey)
    const sender = await suite.createSenderContext({ recipientPublicKey: recipient, info: concat(INFO_DOMAIN, contextBytes) })
    ciphertext = new Uint8Array(await sender.seal(plaintext, concat(AAD_DOMAIN, contextBytes)))
    const envelope: NodeAddressEnvelope = {
      envelope_version: NODE_ADDRESS_ENVELOPE_VERSION,
      recipient_public_key_sha256: keyHash,
      kem: 'DHKEM(X25519,HKDF-SHA256)',
      kdf: 'HKDF-SHA256',
      aead: 'AES-256-GCM',
      encapsulation: base64url(new Uint8Array(sender.enc)),
      ciphertext: base64url(ciphertext),
    }
    const encoded = new TextEncoder().encode(canonicalIdentityJSON(envelope))
    if (encoded.length > MAX_ENVELOPE_BYTES) throw new TypeError('node address envelope is too large')
    return base64url(encoded)
  } finally {
    plaintext.fill(0)
    ciphertext?.fill(0)
  }
}

/** Opens a signed-advertisement address envelope only in the recovery-key client. */
export async function openNodeAddressEnvelope(
  value: string,
  keys: AddressEncryptionKey,
  context: NodeAddressContext,
): Promise<unknown> {
  validateContext(context)
  if (value.length > MAX_ENVELOPE_BYTES * 2) throw new TypeError('node address envelope is invalid')
  const encoded = fromBase64url(value)
  if (encoded.length > MAX_ENVELOPE_BYTES) throw new TypeError('node address envelope is invalid')
  const text = new TextDecoder('utf-8', { fatal: true }).decode(encoded)
  const envelope = validateEnvelope(JSON.parse(text) as unknown)
  if (canonicalIdentityJSON(envelope) !== text) throw new TypeError('node address envelope is not canonical')
  if (keys.encryptionPrivateKey.length !== 32 || keys.encryptionPublicKey.length !== 32
    || !equalBytes(x25519.getPublicKey(keys.encryptionPrivateKey), keys.encryptionPublicKey)
    || envelope.recipient_public_key_sha256 !== publicKeyHash(keys.encryptionPublicKey)) {
    throw new TypeError('node address envelope recipient is not authorized')
  }
  const contextBytes = new TextEncoder().encode(canonicalIdentityJSON(envelopeContext(context, envelope.recipient_public_key_sha256)))
  const suite = suiteForDiscoveryEnvelope()
  const recipientKey = await suite.kem.deserializePrivateKey(keys.encryptionPrivateKey)
  const recipient = await suite.createRecipientContext({
    enc: fromBase64url(envelope.encapsulation, 32),
    recipientKey,
    info: concat(INFO_DOMAIN, contextBytes),
  })
  const plaintext = new Uint8Array(await recipient.open(fromBase64url(envelope.ciphertext), concat(AAD_DOMAIN, contextBytes)))
  if (!plaintext.length || plaintext.length > MAX_PLAINTEXT_BYTES) {
    plaintext.fill(0)
    throw new TypeError('node address payload is invalid')
  }
  try {
    const payloadText = new TextDecoder('utf-8', { fatal: true }).decode(plaintext)
    const payload = JSON.parse(payloadText) as unknown
    if (canonicalIdentityJSON(payload) !== payloadText) throw new TypeError('node address payload is not canonical')
    return payload
  } finally {
    plaintext.fill(0)
  }
}
