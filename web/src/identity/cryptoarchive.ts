import { Aes256Gcm, CipherSuite, HkdfSha256 } from '@hpke/core'
import { DhkemX25519HkdfSha256 } from '@hpke/dhkem-x25519'
import { sha256 } from '@noble/hashes/sha2.js'
import { ed25519, x25519 } from '@noble/curves/ed25519.js'
import { canonicalIdentityJSON, signRecoveryMessage, type RecoveryKeyMaterial } from './recovery'

export const CRYPTO_ARCHIVE_FORMAT_VERSION = 1
export const CRYPTO_ARCHIVE_CHUNK_SIZE = 64 * 1024
export const KEY_ENVELOPE_TYPE = 'ir.key-envelope/1'

const MAX_OBJECT_BYTES = 1 << 30
const HEADER_BYTES = 4 + 1 + 4 + 8 + 32
const OBJECT_MAGIC = new Uint8Array([0x49, 0x52, 0x41, 0x43])
const OBJECT_KEY_DOMAIN = new TextEncoder().encode('IR/archive-object-key/v1\0')
const CHUNK_AAD_DOMAIN = new TextEncoder().encode('IR/archive-chunk-aad/v1\0')
const ENVELOPE_INFO_DOMAIN = new TextEncoder().encode('IR/key-envelope/v1/hpke-info\0')
const ENVELOPE_AAD_DOMAIN = new TextEncoder().encode('IR/key-envelope/v1/hpke-aad\0')
const ENVELOPE_BINDING_DOMAIN = new TextEncoder().encode('IR/key-envelope/v1/binding\0')
const ENVELOPE_SIGN_DOMAIN = new TextEncoder().encode('IR/key-envelope/v1/sign\0')
const RECIPIENT_PUBLIC_DOMAIN = new TextEncoder().encode('IR/recipient-public-key/v1\0')
const RECIPIENT_ID_DOMAIN = new TextEncoder().encode('IR/recipient-key-id/v1\0')
const SESSION_DEK_INFO_DOMAIN = new TextEncoder().encode('IR/session-dek-envelope/v1/hpke-info\0')
const SESSION_DEK_AAD_DOMAIN = new TextEncoder().encode('IR/session-dek-envelope/v1/hpke-aad\0')
const UINT64_MAX = (1n << 64n) - 1n

export type CryptoObjectDescriptor = {
  archiveId: string
  coordinate: {
    sessionId: string
    trackId: string
    sourceEpoch: string
    discontinuitySequence: string
    sequence: string
    kind: 'media' | 'init'
  }
  objectType: 'media' | 'init'
  plaintextSize: number
  plaintextSha256: string
  keyEpoch: string
  ciphertextSize?: number
  ciphertextSha256?: string
}

export type CryptoObjectResult = {
  formatVersion: number
  plaintextSize: number
  plaintextSha256: string
  ciphertextSize: number
  ciphertextSha256: string
  chunkCount: number
}

export type KeyEnvelope = {
  envelope_version: typeof KEY_ENVELOPE_TYPE
  archive_id: string
  key_epoch: string
  recipient_key_id: string
  recipient_public_key_sha256: string
  kem: 'DHKEM(X25519,HKDF-SHA256)'
  kdf: 'HKDF-SHA256'
  aead: 'AES-256-GCM'
  encapsulation: string
  ciphertext: string
  signer_key_id: string
  binding_hash: string
  signature: string
}

export type SessionDEKEnvelope = {
  envelope_version: 'ir.session-dek-envelope/1'
  archive_id: string
  snapshot_id: string
  key_epoch: string
  owner_subject_id: string
  grant_hash: string
  supplier_node_id: string
  recipient_key_id: string
  recipient_public_key_sha256: string
  kem: 'DHKEM(X25519,HKDF-SHA256)'
  kdf: 'HKDF-SHA256'
  aead: 'AES-256-GCM'
  encapsulation: string
  ciphertext: string
}

export type SessionDEKBinding = {
  archiveId: string
  snapshotId: string
  keyEpoch: string
  ownerSubjectId: string
  grantHash: string
  supplierNodeId: string
}

export type SessionDEKKeyPair = {
  publicKey: Uint8Array
  privateKey: Uint8Array
  destroy: () => void
}

export type ArchiveStreamSink = (chunk: Uint8Array) => Promise<void>

type WireCoordinate = {
  session_id: string
  track_id: string
  source_epoch: string
  discontinuity_sequence: string
  sequence: string
  kind: 'media' | 'init'
}

type WireBinding = {
  archive_id: string
  object_coordinate: WireCoordinate
  object_type: 'media' | 'init'
  plaintext_size: string
  plaintext_sha256: string
  crypto_format_version: string
  key_epoch: string
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

function fromBase64url(value: string, expectedLength?: number): Uint8Array {
  if (!/^[A-Za-z0-9_-]+$/u.test(value)) throw new TypeError('archive cryptographic metadata is invalid')
  const padded = value.replaceAll('-', '+').replaceAll('_', '/') + '='.repeat((4 - value.length % 4) % 4)
  const binary = atob(padded)
  const bytes = Uint8Array.from(binary, (char) => char.charCodeAt(0))
  if ((expectedLength !== undefined && bytes.length !== expectedLength) || base64url(bytes) !== value) {
    throw new TypeError('archive cryptographic metadata is invalid')
  }
  return bytes
}

function hex(bytes: Uint8Array): string {
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, '0')).join('')
}

function decimal(value: string, allowZero = true): boolean {
  if (!/^(0|[1-9][0-9]{0,19})$/u.test(value)) return false
  try {
    const parsed = BigInt(value)
    return parsed <= UINT64_MAX && (allowZero || parsed > 0n)
  } catch {
    return false
  }
}

function validateDescriptor(value: CryptoObjectDescriptor): WireBinding {
  const { coordinate } = value
  if (!/^arc_[A-Za-z0-9_-]{22}$/u.test(value.archiveId)
    || !/^session-[0-9a-f]{64}$/u.test(coordinate.sessionId)
    || !/^[A-Za-z0-9._:-]{1,128}$/u.test(coordinate.trackId)
    || !decimal(coordinate.sourceEpoch)
    || !decimal(coordinate.discontinuitySequence)
    || !decimal(coordinate.sequence)
    || !['media', 'init'].includes(coordinate.kind)
    || value.objectType !== coordinate.kind
    || !Number.isSafeInteger(value.plaintextSize)
    || value.plaintextSize <= 0 || value.plaintextSize > MAX_OBJECT_BYTES
    || !/^sha256:[0-9a-f]{64}$/u.test(value.plaintextSha256)
    || !decimal(value.keyEpoch, false)
    || ((value.ciphertextSize === undefined) !== (value.ciphertextSha256 === undefined))
    || (value.ciphertextSize !== undefined && (!Number.isSafeInteger(value.ciphertextSize)
      || value.ciphertextSize < HEADER_BYTES + 16 || value.ciphertextSize > MAX_OBJECT_BYTES + Math.ceil(MAX_OBJECT_BYTES / CRYPTO_ARCHIVE_CHUNK_SIZE) * 16 + HEADER_BYTES
      || !/^sha256:[0-9a-f]{64}$/u.test(value.ciphertextSha256 ?? '')))) {
    throw new TypeError('archive object descriptor is invalid')
  }
  return {
    archive_id: value.archiveId,
    object_coordinate: {
      session_id: coordinate.sessionId,
      track_id: coordinate.trackId,
      source_epoch: coordinate.sourceEpoch,
      discontinuity_sequence: coordinate.discontinuitySequence,
      sequence: coordinate.sequence,
      kind: coordinate.kind,
    },
    object_type: value.objectType,
    plaintext_size: String(value.plaintextSize),
    plaintext_sha256: value.plaintextSha256,
    crypto_format_version: String(CRYPTO_ARCHIVE_FORMAT_VERSION),
    key_epoch: value.keyEpoch,
  }
}

function makeNonce(index: number): Uint8Array {
  if (!Number.isSafeInteger(index) || index < 0 || BigInt(index) > UINT64_MAX) throw new RangeError('archive chunk index is invalid')
  const nonce = new Uint8Array(12)
  new DataView(nonce.buffer).setBigUint64(4, BigInt(index), false)
  return nonce
}

function chunkAAD(binding: WireBinding, index: number, totalSize: number): Uint8Array {
  return concat(CHUNK_AAD_DOMAIN, new TextEncoder().encode(canonicalIdentityJSON({
    archive_id: binding.archive_id,
    object_coordinate: binding.object_coordinate,
    object_type: binding.object_type,
    plaintext_size: String(totalSize),
    plaintext_sha256: binding.plaintext_sha256,
    chunk_index: String(index),
    byte_offset: String(index * CRYPTO_ARCHIVE_CHUNK_SIZE),
    crypto_format_version: binding.crypto_format_version,
    key_epoch: binding.key_epoch,
  })))
}

async function deriveObjectCryptoKey(dek: Uint8Array, salt: Uint8Array, binding: WireBinding, usage: KeyUsage[]): Promise<CryptoKey> {
  if (dek.length !== 32 || salt.length !== 32) throw new TypeError('archive DEK or salt is invalid')
  const rawKey = await cryptoAPI().subtle.importKey('raw', dek, 'HKDF', false, ['deriveBits'])
  const info = concat(OBJECT_KEY_DOMAIN, new TextEncoder().encode(canonicalIdentityJSON(binding)))
  const bits = new Uint8Array(await cryptoAPI().subtle.deriveBits(
    { name: 'HKDF', hash: 'SHA-256', salt, info }, rawKey, 256,
  ))
  try {
    return await cryptoAPI().subtle.importKey('raw', bits, { name: 'AES-GCM' }, false, usage)
  } finally {
    bits.fill(0)
  }
}

class StreamBytes {
  private readonly reader: ReadableStreamDefaultReader<Uint8Array>
  private current = new Uint8Array()
  private offset = 0
  private ended = false

  constructor(stream: ReadableStream<Uint8Array>) {
    this.reader = stream.getReader()
  }

  async exact(length: number): Promise<Uint8Array> {
    const output = new Uint8Array(length)
    let written = 0
    while (written < length) {
      if (this.offset >= this.current.length) {
        if (this.ended) throw new TypeError('archive stream ended before its declared length')
        const next = await this.reader.read()
        if (next.done) {
          this.ended = true
          throw new TypeError('archive stream ended before its declared length')
        }
        this.current = next.value
        this.offset = 0
        if (this.current.length === 0) continue
      }
      const count = Math.min(length - written, this.current.length - this.offset)
      output.set(this.current.subarray(this.offset, this.offset + count), written)
      written += count
      this.offset += count
    }
    return output
  }

  async requireEOF(): Promise<void> {
    if (this.offset < this.current.length) throw new TypeError('archive stream has trailing bytes')
    if (this.ended) return
    while (true) {
      const next = await this.reader.read()
      if (next.done) {
        this.ended = true
        return
      }
      if (next.value.length !== 0) throw new TypeError('archive stream has trailing bytes')
    }
  }

  async cancel(): Promise<void> {
    try { await this.reader.cancel() } catch { /* preserve the original error */ }
  }

  release(): void { this.reader.releaseLock() }
}

/**
 * Encrypts with bounded 64-KiB chunks. The sink must be an unpublished staging
 * writer; discard its output if this function rejects, and publish only after
 * it returns the verified whole-object digest.
 */
export async function encryptArchiveObject(
  dek: Uint8Array,
  descriptor: CryptoObjectDescriptor,
  source: ReadableStream<Uint8Array>,
  sink: ArchiveStreamSink,
): Promise<CryptoObjectResult> {
  const binding = validateDescriptor(descriptor)
  const count = Math.ceil(descriptor.plaintextSize / CRYPTO_ARCHIVE_CHUNK_SIZE)
  const salt = cryptoAPI().getRandomValues(new Uint8Array(32))
  const key = await deriveObjectCryptoKey(dek, salt, binding, ['encrypt'])
  const reader = new StreamBytes(source)
  const plainHash = sha256.create()
  const cipherHash = sha256.create()
  const header = new Uint8Array(HEADER_BYTES)
  header.set(OBJECT_MAGIC, 0)
  header[4] = CRYPTO_ARCHIVE_FORMAT_VERSION
  const headerView = new DataView(header.buffer)
  headerView.setUint32(5, CRYPTO_ARCHIVE_CHUNK_SIZE, false)
  headerView.setBigUint64(9, BigInt(descriptor.plaintextSize), false)
  header.set(salt, 17)
  let cipherSize = header.length
  try {
    await sink(header)
    cipherHash.update(header)
    let remaining = descriptor.plaintextSize
    for (let index = 0; remaining > 0; index += 1) {
      const size = Math.min(CRYPTO_ARCHIVE_CHUNK_SIZE, remaining)
      const plain = await reader.exact(size)
      plainHash.update(plain)
      const encrypted = new Uint8Array(await cryptoAPI().subtle.encrypt(
        { name: 'AES-GCM', iv: makeNonce(index), additionalData: chunkAAD(binding, index, descriptor.plaintextSize), tagLength: 128 },
        key, plain,
      ))
      plain.fill(0)
      await sink(encrypted)
      cipherHash.update(encrypted)
      cipherSize += encrypted.length
      remaining -= size
    }
    await reader.requireEOF()
    const actualPlainHash = `sha256:${hex(plainHash.digest())}`
    if (actualPlainHash !== descriptor.plaintextSha256) throw new TypeError('archive plaintext does not match its declared identity')
    return {
      formatVersion: CRYPTO_ARCHIVE_FORMAT_VERSION,
      plaintextSize: descriptor.plaintextSize,
      plaintextSha256: actualPlainHash,
      ciphertextSize: cipherSize,
      ciphertextSha256: `sha256:${hex(cipherHash.digest())}`,
      chunkCount: count,
    }
  } catch (error) {
    await reader.cancel()
    throw error
  } finally {
    salt.fill(0)
    reader.release()
  }
}

/**
 * Decrypts into a staging sink with bounded memory. A complete-object hash or
 * ciphertext digest mismatch rejects the operation; callers must discard all
 * staged plaintext on any rejection.
 */
export async function decryptArchiveObject(
  dek: Uint8Array,
  descriptor: CryptoObjectDescriptor,
  source: ReadableStream<Uint8Array>,
  sink: ArchiveStreamSink,
): Promise<void> {
  const binding = validateDescriptor(descriptor)
  const reader = new StreamBytes(source)
  const cipherHash = sha256.create()
  const plainHash = sha256.create()
  let cipherSize = 0
  try {
    const header = await reader.exact(HEADER_BYTES)
    cipherSize += header.length
    cipherHash.update(header)
    const view = new DataView(header.buffer, header.byteOffset, header.byteLength)
    if (!header.subarray(0, 4).every((byte, index) => byte === OBJECT_MAGIC[index])
      || header[4] !== CRYPTO_ARCHIVE_FORMAT_VERSION
      || view.getUint32(5, false) !== CRYPTO_ARCHIVE_CHUNK_SIZE
      || view.getBigUint64(9, false) !== BigInt(descriptor.plaintextSize)) {
      throw new TypeError('archive ciphertext header is invalid')
    }
    const key = await deriveObjectCryptoKey(dek, header.subarray(17), binding, ['decrypt'])
    let remaining = descriptor.plaintextSize
    for (let index = 0; remaining > 0; index += 1) {
      const size = Math.min(CRYPTO_ARCHIVE_CHUNK_SIZE, remaining)
      const sealed = await reader.exact(size + 16)
      cipherHash.update(sealed)
      cipherSize += sealed.length
      let plain: Uint8Array
      try {
        plain = new Uint8Array(await cryptoAPI().subtle.decrypt(
          { name: 'AES-GCM', iv: makeNonce(index), additionalData: chunkAAD(binding, index, descriptor.plaintextSize), tagLength: 128 },
          key, sealed,
        ))
      } finally {
        sealed.fill(0)
      }
      if (plain.length !== size) {
        plain.fill(0)
        throw new TypeError('archive ciphertext chunk is invalid')
      }
      plainHash.update(plain)
      await sink(plain)
      remaining -= size
    }
    await reader.requireEOF()
    if (`sha256:${hex(plainHash.digest())}` !== descriptor.plaintextSha256) throw new TypeError('archive plaintext digest is invalid')
    if (descriptor.ciphertextSize !== undefined && (descriptor.ciphertextSize !== cipherSize
      || descriptor.ciphertextSha256 !== `sha256:${hex(cipherHash.digest())}`)) {
      throw new TypeError('archive ciphertext digest is invalid')
    }
  } catch (error) {
    await reader.cancel()
    throw error
  } finally {
    reader.release()
  }
}

export function recipientPublicKeyHash(publicKey: Uint8Array): string {
  return `sha256:${hex(sha256(concat(RECIPIENT_PUBLIC_DOMAIN, publicKey)))}`
}

export function recipientKeyId(publicKey: Uint8Array): string {
  return `rk_${base64url(sha256(concat(RECIPIENT_ID_DOMAIN, publicKey)))}`
}

type EnvelopeUnsigned = Omit<KeyEnvelope, 'binding_hash' | 'signature'>

function suiteForArchiveDEK(): CipherSuite {
  return new CipherSuite({ kem: new DhkemX25519HkdfSha256(), kdf: new HkdfSha256(), aead: new Aes256Gcm() })
}

/** Generate a one-playback X25519 key pair. Keep the private bytes in memory
 * and call destroy as soon as the DEK has been opened or playback is cleared.
 */
export function createSessionDEKKeyPair(): SessionDEKKeyPair {
  const privateKey = cryptoAPI().getRandomValues(new Uint8Array(32))
  const publicKey = x25519.getPublicKey(privateKey)
  let destroyed = false
  return {
    privateKey,
    publicKey,
    destroy: () => {
      if (destroyed) return
      privateKey.fill(0)
      publicKey.fill(0)
      destroyed = true
    },
  }
}

function sessionDEKContext(binding: SessionDEKBinding, recipientPublicKey: Uint8Array): Record<string, string> {
  return {
    envelope_version: 'ir.session-dek-envelope/1',
    archive_id: binding.archiveId,
    snapshot_id: binding.snapshotId,
    key_epoch: binding.keyEpoch,
    owner_subject_id: binding.ownerSubjectId,
    grant_hash: binding.grantHash,
    supplier_node_id: binding.supplierNodeId,
    recipient_key_id: recipientKeyId(recipientPublicKey),
    recipient_public_key_sha256: recipientPublicKeyHash(recipientPublicKey),
  }
}

/** Open a one-use DEK response returned through the authenticated and
 * capability-authorized Node-Key peer session. This is separate from the
 * durable, Owner-recovery-signed key envelope used for offline recovery.
 */
export async function openSessionDEKEnvelope(
  envelope: SessionDEKEnvelope,
  recipientPrivateKey: Uint8Array,
  expected: SessionDEKBinding,
): Promise<Uint8Array> {
  validateSessionDEKEnvelope(envelope)
  if (recipientPrivateKey.length !== 32 || envelope.archive_id !== expected.archiveId ||
    envelope.snapshot_id !== expected.snapshotId || envelope.key_epoch !== expected.keyEpoch ||
    envelope.owner_subject_id !== expected.ownerSubjectId || envelope.grant_hash !== expected.grantHash ||
    envelope.supplier_node_id !== expected.supplierNodeId) {
    throw new TypeError('session archive key envelope binding is not authorized')
  }
  const publicKey = x25519.getPublicKey(recipientPrivateKey)
  if (envelope.recipient_key_id !== recipientKeyId(publicKey) ||
    envelope.recipient_public_key_sha256 !== recipientPublicKeyHash(publicKey)) {
    throw new TypeError('session archive key envelope recipient is not authorized')
  }
  const contextBytes = new TextEncoder().encode(canonicalIdentityJSON(sessionDEKContext(expected, publicKey)))
  const suite = suiteForArchiveDEK()
  const privateKey = await suite.kem.deserializePrivateKey(recipientPrivateKey)
  const recipient = await suite.createRecipientContext({
    enc: fromBase64url(envelope.encapsulation, 32),
    recipientKey: privateKey,
    info: concat(SESSION_DEK_INFO_DOMAIN, contextBytes),
  })
  const dek = new Uint8Array(await recipient.open(fromBase64url(envelope.ciphertext, 48), concat(SESSION_DEK_AAD_DOMAIN, contextBytes)))
  if (dek.length !== 32) {
    dek.fill(0)
    throw new TypeError('session archive DEK envelope plaintext is invalid')
  }
  return dek
}

export function validateSessionDEKEnvelope(envelope: SessionDEKEnvelope): void {
  const expectedFields = [
    'aead', 'archive_id', 'ciphertext', 'encapsulation', 'envelope_version', 'grant_hash', 'kdf', 'kem',
    'key_epoch', 'owner_subject_id', 'recipient_key_id', 'recipient_public_key_sha256', 'snapshot_id',
    'supplier_node_id',
  ]
  const actualFields = Object.keys(envelope).sort()
  if (envelope.envelope_version !== 'ir.session-dek-envelope/1' ||
    actualFields.length !== expectedFields.length || actualFields.some((field, index) => field !== expectedFields[index]) ||
    !/^arc_[A-Za-z0-9_-]{22}$/u.test(envelope.archive_id) || !/^snap_[0-9a-f]{64}$/u.test(envelope.snapshot_id) ||
    !decimal(envelope.key_epoch, false) || !/^os_[A-Za-z0-9_-]{22}$/u.test(envelope.owner_subject_id) ||
    !/^sha256:[0-9a-f]{64}$/u.test(envelope.grant_hash) || !/^nd_[A-Za-z0-9_-]{22}$/u.test(envelope.supplier_node_id) ||
    !/^rk_[A-Za-z0-9_-]{43}$/u.test(envelope.recipient_key_id) ||
    !/^sha256:[0-9a-f]{64}$/u.test(envelope.recipient_public_key_sha256) ||
    envelope.kem !== 'DHKEM(X25519,HKDF-SHA256)' || envelope.kdf !== 'HKDF-SHA256' || envelope.aead !== 'AES-256-GCM') {
    throw new TypeError('session archive key envelope fields are invalid')
  }
  fromBase64url(envelope.encapsulation, 32)
  fromBase64url(envelope.ciphertext, 48)
}

function envelopeContext(unsigned: EnvelopeUnsigned): Record<string, string> {
  return {
    envelope_version: unsigned.envelope_version,
    archive_id: unsigned.archive_id,
    key_epoch: unsigned.key_epoch,
    recipient_key_id: unsigned.recipient_key_id,
    recipient_public_key_sha256: unsigned.recipient_public_key_sha256,
    signer_key_id: unsigned.signer_key_id,
  }
}

function envelopeBindingHash(unsigned: EnvelopeUnsigned): string {
  return `sha256:${hex(sha256(concat(ENVELOPE_BINDING_DOMAIN, new TextEncoder().encode(canonicalIdentityJSON(unsigned)))))}`
}

/** Wrap an archive DEK for the recovery recipient and sign all envelope fields. */
export async function wrapArchiveDEK(
  dek: Uint8Array,
  archiveId: string,
  keyEpoch: string,
  recipientPublicKey: Uint8Array,
  recoveryKeys: RecoveryKeyMaterial,
): Promise<KeyEnvelope> {
  if (dek.length !== 32 || !/^arc_[A-Za-z0-9_-]{22}$/u.test(archiveId) || !decimal(keyEpoch, false)
    || recipientPublicKey.length !== 32 || recoveryKeys.signingSeed.length !== 32 || recoveryKeys.signingPublicKey.length !== 32) {
    throw new TypeError('archive key envelope input is invalid')
  }
  const derivedSignerKeyId = `ik_${base64url(sha256(concat(new TextEncoder().encode('IR/identity-key/v1\0'), recoveryKeys.signingPublicKey)))}`
  const derivedSignerPublicKey = ed25519.getPublicKey(recoveryKeys.signingSeed)
  if (derivedSignerKeyId !== recoveryKeys.signerKeyId
    || !equalBytes(derivedSignerPublicKey, recoveryKeys.signingPublicKey)) {
    throw new TypeError('archive recovery key material is inconsistent')
  }
  const suite = suiteForArchiveDEK()
  const recipient = await suite.kem.deserializePublicKey(recipientPublicKey)
  const unsigned: EnvelopeUnsigned = {
    envelope_version: KEY_ENVELOPE_TYPE,
    archive_id: archiveId,
    key_epoch: keyEpoch,
    recipient_key_id: recipientKeyId(recipientPublicKey),
    recipient_public_key_sha256: recipientPublicKeyHash(recipientPublicKey),
    kem: 'DHKEM(X25519,HKDF-SHA256)',
    kdf: 'HKDF-SHA256',
    aead: 'AES-256-GCM',
    encapsulation: '',
    ciphertext: '',
    signer_key_id: recoveryKeys.signerKeyId,
  }
  const contextBytes = new TextEncoder().encode(canonicalIdentityJSON(envelopeContext(unsigned)))
  const sender = await suite.createSenderContext({
    recipientPublicKey: recipient,
    info: concat(ENVELOPE_INFO_DOMAIN, contextBytes),
  })
  const ciphertext = new Uint8Array(await sender.seal(dek, concat(ENVELOPE_AAD_DOMAIN, contextBytes)))
  unsigned.encapsulation = base64url(new Uint8Array(sender.enc))
  unsigned.ciphertext = base64url(ciphertext)
  ciphertext.fill(0)
  const bindingHash = envelopeBindingHash(unsigned)
  const signed = new TextEncoder().encode(canonicalIdentityJSON({ envelope: unsigned, binding_hash: bindingHash }))
  const signature = signRecoveryMessage(concat(ENVELOPE_SIGN_DOMAIN, signed), recoveryKeys.signingSeed)
  return { ...unsigned, binding_hash: bindingHash, signature: base64url(signature) }
}

export type OpenDEKExpectation = {
  archiveId: string
  keyEpoch: string
  signerKeyId: string
  signerPublicKey: Uint8Array
  recipientPrivateKey: Uint8Array
}

/** Verify the explicit authority signature before HPKE decapsulation. */
export async function openArchiveDEK(envelope: KeyEnvelope, expected: OpenDEKExpectation): Promise<Uint8Array> {
  validateEnvelope(envelope)
  const unsigned: EnvelopeUnsigned = {
    envelope_version: envelope.envelope_version,
    archive_id: envelope.archive_id,
    key_epoch: envelope.key_epoch,
    recipient_key_id: envelope.recipient_key_id,
    recipient_public_key_sha256: envelope.recipient_public_key_sha256,
    kem: envelope.kem,
    kdf: envelope.kdf,
    aead: envelope.aead,
    encapsulation: envelope.encapsulation,
    ciphertext: envelope.ciphertext,
    signer_key_id: envelope.signer_key_id,
  }
  const recipientPrivateKey = expected.recipientPrivateKey
  if (envelope.archive_id !== expected.archiveId || envelope.key_epoch !== expected.keyEpoch
    || envelope.signer_key_id !== expected.signerKeyId || recipientPrivateKey.length !== 32 || expected.signerPublicKey.length !== 32) {
    throw new TypeError('archive key envelope binding is not authorized')
  }
  const recipientPublicKey = x25519.getPublicKey(recipientPrivateKey)
  if (envelope.recipient_key_id !== recipientKeyId(recipientPublicKey)
    || envelope.recipient_public_key_sha256 !== recipientPublicKeyHash(recipientPublicKey)) {
    throw new TypeError('archive key envelope recipient is not authorized')
  }
  const signerDigest = sha256(concat(new TextEncoder().encode('IR/identity-key/v1\0'), expected.signerPublicKey))
  if (`ik_${base64url(signerDigest)}` !== expected.signerKeyId) throw new TypeError('archive envelope signer key ID is invalid')
  const bindingHash = envelopeBindingHash(unsigned)
  if (bindingHash !== envelope.binding_hash) throw new TypeError('archive key envelope binding hash is invalid')
  const signed = new TextEncoder().encode(canonicalIdentityJSON({ envelope: unsigned, binding_hash: bindingHash }))
  if (!ed25519.verify(fromBase64url(envelope.signature, 64), concat(ENVELOPE_SIGN_DOMAIN, signed), expected.signerPublicKey)) {
    throw new TypeError('archive key envelope authority signature is invalid')
  }
  const contextBytes = new TextEncoder().encode(canonicalIdentityJSON(envelopeContext(unsigned)))
  const suite = suiteForArchiveDEK()
  const recipientPrivate = await suite.kem.deserializePrivateKey(recipientPrivateKey)
  const recipient = await suite.createRecipientContext({
    enc: fromBase64url(envelope.encapsulation, 32),
    recipientKey: recipientPrivate,
    info: concat(ENVELOPE_INFO_DOMAIN, contextBytes),
  })
  const dek = new Uint8Array(await recipient.open(fromBase64url(envelope.ciphertext, 48), concat(ENVELOPE_AAD_DOMAIN, contextBytes)))
  if (dek.length !== 32) {
    dek.fill(0)
    throw new TypeError('archive DEK envelope plaintext is invalid')
  }
  return dek
}

export function validateEnvelope(envelope: KeyEnvelope): void {
  const expectedFields = [
    'aead', 'archive_id', 'binding_hash', 'ciphertext', 'encapsulation', 'envelope_version',
    'kdf', 'kem', 'key_epoch', 'recipient_key_id', 'recipient_public_key_sha256', 'signature', 'signer_key_id',
  ]
  const actualFields = Object.keys(envelope).sort()
  if (envelope.envelope_version !== KEY_ENVELOPE_TYPE || !/^arc_[A-Za-z0-9_-]{22}$/u.test(envelope.archive_id)
    || actualFields.length !== expectedFields.length || actualFields.some((field, index) => field !== expectedFields[index])
    || !decimal(envelope.key_epoch, false) || !/^rk_[A-Za-z0-9_-]{43}$/u.test(envelope.recipient_key_id)
    || !/^sha256:[0-9a-f]{64}$/u.test(envelope.recipient_public_key_sha256)
    || envelope.kem !== 'DHKEM(X25519,HKDF-SHA256)' || envelope.kdf !== 'HKDF-SHA256' || envelope.aead !== 'AES-256-GCM'
    || !/^ik_[A-Za-z0-9_-]{43}$/u.test(envelope.signer_key_id)
    || !/^sha256:[0-9a-f]{64}$/u.test(envelope.binding_hash)) {
    throw new TypeError('archive key envelope fields are invalid')
  }
  fromBase64url(envelope.encapsulation, 32)
  fromBase64url(envelope.ciphertext, 48)
  fromBase64url(envelope.signature, 64)
}

function equalBytes(left: Uint8Array, right: Uint8Array): boolean {
  if (left.length !== right.length) return false
  let difference = 0
  for (let index = 0; index < left.length; index += 1) difference |= left[index] ^ right[index]
  return difference === 0
}
