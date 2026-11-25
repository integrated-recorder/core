import { sha256 } from '@noble/hashes/sha2.js'
import type { ArchiveSnapshot, ReplicaManifest, VerifiedGlobalIdentityRecipient } from '@/types/api'
import { canonicalIdentityJSON, type RecoveryKeyMaterial } from './recovery'
import { decryptArchiveObject, openArchiveDEK, recipientKeyId, type CryptoObjectDescriptor, type KeyEnvelope } from './cryptoarchive'

const REKEY_DOMAIN = new TextEncoder().encode('IR/archive-rekey-history/v1\0')
const SNAPSHOT_CONTENT_V1 = 'IR/archive-snapshot/v1/content\0'
const SNAPSHOT_CONTENT_V2 = 'IR/archive-snapshot/v2/content\0'
const SNAPSHOT_PROVENANCE = 'IR/archive-snapshot/v1/provenance\0'
const MANIFEST_DOMAIN = 'IR/replica-manifest/v1\0'
const MAX_CIPHERTEXT_RANGE = 4 * 1024 * 1024

export type RekeyHistoryVerification = {
  archive_id: string
  previous_key_epoch: string
  new_key_epoch: string
  snapshot_count: string
  head_sequence: string
  head_snapshot_id: string
  bundle_set_commitment: string
  envelope_sha256: string
  recipient_key_id: string
}

export type RecipientRekeyEvidence = {
  archive_id: string
  previous_key_epoch: string
  new_key_epoch: string
  snapshot_count: string
  head_snapshot_id: string
  bundle_set_commitment: string
  envelope_sha256: string
  recipient_key_id: string
}

export type RekeyRangeFetcher = (
  snapshotID: string,
  keyEpoch: string,
  objectID: string,
  offset: number,
  length: number,
  signal: AbortSignal,
) => Promise<Uint8Array>

export type RekeyManifestLoader = (snapshotID: string, keyEpoch: string) => Promise<ReplicaManifest>

export type RekeyHistoryRecord = {
  snapshot: ArchiveSnapshot
  previousManifest: ReplicaManifest
  newManifest: ReplicaManifest
  envelope: KeyEnvelope
}

function concat(...parts: Uint8Array[]): Uint8Array {
  const result = new Uint8Array(parts.reduce((size, part) => size + part.byteLength, 0))
  let offset = 0
  for (const part of parts) {
    result.set(part, offset)
    offset += part.byteLength
  }
  return result
}

function hex(bytes: Uint8Array): string {
  return Array.from(bytes, byte => byte.toString(16).padStart(2, '0')).join('')
}

function decodeHex(value: string): Uint8Array {
  if (!/^(?:[0-9a-f]{2})+$/u.test(value)) throw new TypeError('archive digest encoding is invalid')
  return Uint8Array.from({ length: value.length / 2 }, (_, index) => Number.parseInt(value.slice(index * 2, index * 2 + 2), 16))
}

function digest(domain: string | Uint8Array, bytes: Uint8Array): string {
  const prefix = typeof domain === 'string' ? new TextEncoder().encode(domain) : domain
  return `sha256:${hex(sha256(concat(prefix, bytes)))}`
}

function utf8(value: string): Uint8Array {
  return new TextEncoder().encode(value)
}

function canonicalSnapshotJSON(value: unknown): string {
  if (value === null) return 'null'
  if (typeof value === 'boolean' || typeof value === 'string') {
    if (typeof value === 'string') {
      for (let index = 0; index < value.length; index += 1) {
        const unit = value.charCodeAt(index)
        if (unit >= 0xd800 && unit <= 0xdbff) {
          const next = value.charCodeAt(index + 1)
          if (!(next >= 0xdc00 && next <= 0xdfff)) throw new TypeError('snapshot contains invalid Unicode')
          index += 1
        } else if (unit >= 0xdc00 && unit <= 0xdfff) {
          throw new TypeError('snapshot contains invalid Unicode')
        }
      }
    }
    return JSON.stringify(value)
  }
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) throw new TypeError('snapshot contains a non-finite number')
    return JSON.stringify(value)
  }
  if (Array.isArray(value)) return `[${value.map(item => canonicalSnapshotJSON(item)).join(',')}]`
  if (typeof value !== 'object' || Object.getPrototypeOf(value) !== Object.prototype) {
    throw new TypeError('snapshot contains an unsupported value')
  }
  const object = value as Record<string, unknown>
  const keys = Object.keys(object).sort()
  return `{${keys.map(key => `${JSON.stringify(key)}:${canonicalSnapshotJSON(object[key])}`).join(',')}}`
}

function decodeBase64url(value: string, expectedLength: number): Uint8Array {
  if (!/^[A-Za-z0-9_-]+$/u.test(value)) throw new TypeError('verified identity key encoding is invalid')
  const binary = atob(value.replaceAll('-', '+').replaceAll('_', '/') + '='.repeat((4 - value.length % 4) % 4))
  const bytes = Uint8Array.from(binary, character => character.charCodeAt(0))
  if (bytes.byteLength !== expectedLength) throw new TypeError('verified identity key length is invalid')
  return bytes
}

function coordinateKey(value: ReplicaManifest['objects'][number]['coordinate']): string {
  const kind = value.kind || 'media'
  if (!/^session-[0-9a-f]{64}$/u.test(value.session_id) || !/^[A-Za-z0-9._:-]{1,128}$/u.test(value.track_id) ||
      !Number.isSafeInteger(value.source_epoch) || value.source_epoch < 0 ||
      !Number.isSafeInteger(value.discontinuity_sequence) || value.discontinuity_sequence < 0 ||
      !Number.isSafeInteger(value.sequence) || value.sequence < 0 || (kind !== 'media' && kind !== 'init')) {
    throw new TypeError('archive rekey coordinate is invalid')
  }
  return [value.session_id, value.track_id, value.source_epoch, value.discontinuity_sequence, value.sequence, kind].join('\0')
}

function snapshotCoordinateKey(value: ArchiveSnapshot['content']['objects'][number]['coordinate']): string {
  return coordinateKey({ ...value, kind: value.kind || 'media' })
}

function compareSnapshotObjects(
  left: ArchiveSnapshot['content']['objects'][number],
  right: ArchiveSnapshot['content']['objects'][number],
): number {
  const a = left.coordinate
  const b = right.coordinate
  if (a.track_id !== b.track_id) return a.track_id < b.track_id ? -1 : 1
  const aKind = a.kind || ''
  const bKind = b.kind || ''
  if (aKind !== bKind) return aKind < bKind ? -1 : 1
  if (aKind === 'media' && left.timeline_ordinal !== right.timeline_ordinal) return left.timeline_ordinal < right.timeline_ordinal ? -1 : 1
  if (a.source_epoch !== b.source_epoch) return a.source_epoch < b.source_epoch ? -1 : 1
  if (a.discontinuity_sequence !== b.discontinuity_sequence) return a.discontinuity_sequence < b.discontinuity_sequence ? -1 : 1
  if (a.sequence !== b.sequence) return a.sequence < b.sequence ? -1 : 1
  return 0
}

function equalBytes(left: Uint8Array, right: Uint8Array): boolean {
  if (left.byteLength !== right.byteLength) return false
  let difference = 0
  for (let index = 0; index < left.byteLength; index += 1) difference |= left[index] ^ right[index]
  return difference === 0
}

async function verifiedIdentity(value: VerifiedGlobalIdentityRecipient, now: Date): Promise<Uint8Array> {
  const verifiedAt = Date.parse(value.verified_at)
  const freshUntil = Date.parse(value.fresh_until)
  if (value.frozen || !/^gi_[A-Za-z0-9_-]{22}$/u.test(value.identity_id) ||
      !Number.isFinite(verifiedAt) || !Number.isFinite(freshUntil) || verifiedAt > now.getTime() ||
      now.getTime() - verifiedAt > 5 * 60 * 1000 || freshUntil <= now.getTime() || freshUntil > verifiedAt + 5 * 60 * 1000) {
    throw new TypeError('verified global identity is unavailable or stale')
  }
  const signingPublicKey = decodeBase64url(value.identity_signing_public_key, 32)
  const identityID = new Uint8Array(await globalThis.crypto.subtle.digest('SHA-256', concat(utf8('IR/global-identity-id/v1\0'), signingPublicKey)))
  if (`gi_${base64url(identityID.subarray(0, 16))}` !== value.identity_id) throw new TypeError('verified identity key does not match its ID')
  return signingPublicKey
}

function verifySnapshot(snapshot: ArchiveSnapshot, archiveID: string): void {
  if ((snapshot.schema_version !== 1 && snapshot.schema_version !== 2) || !Number.isSafeInteger(snapshot.sequence) || snapshot.sequence < 1 ||
      !/^snap_[0-9a-f]{64}$/u.test(snapshot.snapshot_id) || !/^sha256:[0-9a-f]{64}$/u.test(snapshot.content_hash) ||
      !/^sha256:[0-9a-f]{64}$/u.test(snapshot.provenance_hash) || snapshot.content.archive_id !== archiveID ||
      snapshot.content.archive_state !== 'sealed' || snapshot.content.capture_state !== 'closed' ||
      !Array.isArray(snapshot.content.objects) || !Array.isArray(snapshot.content.coverage) || !Array.isArray(snapshot.provenance)) {
    throw new TypeError('archive rekey snapshot is invalid or not sealed')
  }
  const contentHash = digest(snapshot.schema_version === 1 ? SNAPSHOT_CONTENT_V1 : SNAPSHOT_CONTENT_V2, utf8(canonicalSnapshotJSON(snapshot.content)))
  const provenanceHash = digest(SNAPSHOT_PROVENANCE, utf8(canonicalSnapshotJSON(snapshot.provenance)))
  if (contentHash !== snapshot.content_hash || `snap_${contentHash.slice('sha256:'.length)}` !== snapshot.snapshot_id || provenanceHash !== snapshot.provenance_hash) {
    throw new TypeError('archive rekey snapshot content or provenance hash is invalid')
  }
  const sessionID = snapshot.content.session.id
  if (!/^session-[0-9a-f]{64}$/u.test(sessionID)) throw new TypeError('archive rekey session identity is invalid')
  for (let index = 0; index < snapshot.content.objects.length; index += 1) {
    const object = snapshot.content.objects[index]
    if (!object) throw new TypeError('archive rekey snapshot object is invalid')
    const coordinate = snapshotCoordinateKey(object.coordinate)
    if (object.coordinate.session_id !== sessionID || (index > 0 && compareSnapshotObjects(snapshot.content.objects[index - 1]!, object) >= 0) ||
        !Number.isSafeInteger(object.timeline_ordinal) || object.timeline_ordinal < 0 ||
        !Number.isSafeInteger(object.plaintext_size) || object.plaintext_size < 1 || !/^sha256:[0-9a-f]{64}$/u.test(object.plaintext_sha256) ||
        (object.state !== 'present' && object.state !== 'conflict')) {
      throw new TypeError('archive rekey snapshot contains an invalid or noncanonical object')
    }
    if (coordinate.length === 0) throw new TypeError('archive rekey snapshot coordinate is invalid')
  }
}

function manifestHash(snapshot: ArchiveSnapshot, manifest: ReplicaManifest, expectedEpoch: string): string {
  if (manifest.replica_manifest_version !== 'ir.replica-manifest/1' || manifest.archive_id !== snapshot.content.archive_id ||
      manifest.snapshot_id !== snapshot.snapshot_id || manifest.key_epoch !== expectedEpoch || !Array.isArray(manifest.objects) || manifest.objects.length === 0) {
    throw new TypeError('archive rekey replica manifest binding is invalid')
  }
  const present = new Map(snapshot.content.objects.filter(object => object.state === 'present').map(object => [snapshotCoordinateKey(object.coordinate), object]))
  const seenCoordinates = new Set<string>()
  const seenIDs = new Set<string>()
  const objects = manifest.objects.map(object => {
    if (!/^ob_[A-Za-z0-9_-]{43}$/u.test(object.object_id) || !/^[1-9][0-9]{0,15}$/u.test(object.ciphertext_size) ||
        !/^sha256:[0-9a-f]{64}$/u.test(object.ciphertext_sha256)) {
      throw new TypeError('archive rekey manifest object is invalid')
    }
    const key = coordinateKey(object.coordinate)
    if (!present.has(key) || seenCoordinates.has(key) || seenIDs.has(object.object_id)) throw new TypeError('archive rekey manifest does not uniquely match snapshot objects')
    if (object.object_id !== `ob_${base64url(decodeHex(object.ciphertext_sha256.slice('sha256:'.length)))}`) {
      throw new TypeError('archive rekey object ID does not match its ciphertext digest')
    }
    seenCoordinates.add(key)
    seenIDs.add(object.object_id)
    return {
      session_id: object.coordinate.session_id,
      track_id: object.coordinate.track_id,
      source_epoch: String(object.coordinate.source_epoch),
      discontinuity_sequence: String(object.coordinate.discontinuity_sequence),
      sequence: String(object.coordinate.sequence),
      kind: object.coordinate.kind || '',
      object_id: object.object_id,
      ciphertext_size: object.ciphertext_size,
      ciphertext_sha256: object.ciphertext_sha256,
    }
  })
  if (seenCoordinates.size !== present.size) throw new TypeError('archive rekey manifest omits snapshot objects')
  const document = {
    replica_manifest_version: manifest.replica_manifest_version,
    archive_id: manifest.archive_id,
    snapshot_id: manifest.snapshot_id,
    key_epoch: manifest.key_epoch,
    objects,
  }
  return digest(MANIFEST_DOMAIN, utf8(canonicalIdentityJSON(document)))
}

async function verifyEncryptedObject(
  snapshot: ArchiveSnapshot,
  manifest: ReplicaManifest,
  object: ReplicaManifest['objects'][number],
  dek: Uint8Array,
  fetchRange: RekeyRangeFetcher,
  signal: AbortSignal,
): Promise<void> {
  const metadata = snapshot.content.objects.find(item => snapshotCoordinateKey(item.coordinate) === coordinateKey(object.coordinate))
  if (!metadata) throw new TypeError('archive rekey manifest references an unknown snapshot object')
  const ciphertextSize = Number(object.ciphertext_size)
  if (!Number.isSafeInteger(ciphertextSize) || ciphertextSize <= 0) throw new TypeError('archive rekey ciphertext size is invalid')
  const kind = object.coordinate.kind || 'media'
  const descriptor: CryptoObjectDescriptor = {
    archiveId: manifest.archive_id,
    coordinate: {
      sessionId: object.coordinate.session_id,
      trackId: object.coordinate.track_id,
      sourceEpoch: String(object.coordinate.source_epoch),
      discontinuitySequence: String(object.coordinate.discontinuity_sequence),
      sequence: String(object.coordinate.sequence),
      kind,
    },
    objectType: kind,
    plaintextSize: metadata.plaintext_size,
    plaintextSha256: metadata.plaintext_sha256,
    keyEpoch: manifest.key_epoch,
    ciphertextSize,
    ciphertextSha256: object.ciphertext_sha256,
  }
  let offset = 0
  const source = new ReadableStream<Uint8Array>({
    pull: async controller => {
      if (offset >= ciphertextSize) { controller.close(); return }
      const length = Math.min(MAX_CIPHERTEXT_RANGE, ciphertextSize - offset)
      const chunk = await fetchRange(snapshot.snapshot_id, manifest.key_epoch, object.object_id, offset, length, signal)
      if (chunk.byteLength !== length) throw new TypeError('archive rekey ciphertext range length is invalid')
      offset += length
      controller.enqueue(chunk)
    },
  })
  await decryptArchiveObject(dek, descriptor, source, async plaintext => { plaintext.fill(0) })
}

/**
 * Independently verifies a source-prepared rekey before a new controller
 * signs archive_rekey_complete. History is loaded one immutable snapshot at a
 * time so ciphertext is always range-bounded and large archives do not retain
 * the full snapshot/manifest chain in browser memory. The source's commitment
 * is recomputed from the full chain and both epoch manifests; every new-epoch
 * object is decrypted in bounded chunks and checked against its immutable
 * plaintext size/digest. Recovery keys never leave this browser.
 */
export async function verifyRecipientRekeyHistory(input: {
  archiveID: string
  previousEpoch: string
  newEpoch: string
  loadHistoryRecord: (snapshotID: string, expectedSequence: number, index: number, signal: AbortSignal) => Promise<RekeyHistoryRecord>
  envelope: KeyEnvelope
  sourceIdentity: VerifiedGlobalIdentityRecipient
  recipientIdentity: VerifiedGlobalIdentityRecipient
  recipientKeys: RecoveryKeyMaterial
  expected: RekeyHistoryVerification
  fetchCiphertextRange: RekeyRangeFetcher
  now?: Date
  signal?: AbortSignal
}): Promise<RecipientRekeyEvidence> {
  const now = input.now ?? new Date()
  const sourceSigningPublicKey = await verifiedIdentity(input.sourceIdentity, now)
  const recipientSigningPublicKey = await verifiedIdentity(input.recipientIdentity, now)
  const recipientEncryptionPublicKey = decodeBase64url(input.recipientIdentity.recovery_encryption_public_key, 32)
  if (input.recipientKeys.identityId !== input.recipientIdentity.identity_id ||
      !equalBytes(recipientSigningPublicKey, input.recipientKeys.signingPublicKey) ||
      !equalBytes(recipientEncryptionPublicKey, input.recipientKeys.encryptionPublicKey) ||
      input.recipientKeys.identityId === input.sourceIdentity.identity_id ||
      input.expected.archive_id !== input.archiveID || input.expected.previous_key_epoch !== input.previousEpoch ||
      input.expected.new_key_epoch !== input.newEpoch || !/^[1-9][0-9]{0,19}$/u.test(input.previousEpoch) ||
      !/^[1-9][0-9]{0,19}$/u.test(input.newEpoch) || BigInt(input.newEpoch) <= BigInt(input.previousEpoch)) {
    throw new TypeError('archive rekey identity or epoch context is invalid')
  }
  const snapshotCount = Number(input.expected.snapshot_count)
  const headSequence = Number(input.expected.head_sequence)
  if (!/^[1-9][0-9]{0,19}$/u.test(input.expected.snapshot_count) || !/^[1-9][0-9]{0,19}$/u.test(input.expected.head_sequence) ||
      !Number.isSafeInteger(snapshotCount) || snapshotCount > 100_000 || headSequence !== snapshotCount ||
      !Number.isSafeInteger(headSequence) || headSequence > 100_000 || typeof input.loadHistoryRecord !== 'function') {
    throw new TypeError('archive rekey history is incomplete')
  }
  const headSequenceFromExpected = headSequence
  if (!/^snap_[0-9a-f]{64}$/u.test(input.expected.head_snapshot_id)) throw new TypeError('archive rekey head is invalid')
  let expectedSnapshotID: string | undefined = input.expected.head_snapshot_id
  let commitment = sha256(concat(REKEY_DOMAIN, utf8(`begin\0${input.archiveID}`)))
  const newDEK = await openArchiveDEK(input.envelope, {
    archiveId: input.archiveID,
    keyEpoch: input.newEpoch,
    signerKeyId: `ik_${base64urlSHA(sourceSigningPublicKey)}`,
    signerPublicKey: sourceSigningPublicKey,
    recipientPrivateKey: input.recipientKeys.encryptionPrivateKey,
  })
  try {
    if (input.envelope.recipient_key_id !== recipientKeyId(input.recipientKeys.encryptionPublicKey) ||
        input.expected.recipient_key_id !== input.envelope.recipient_key_id ||
        input.expected.envelope_sha256 !== digest('', utf8(canonicalIdentityJSON(input.envelope)))) {
      throw new TypeError('archive rekey envelope differs from the recipient or source verification')
    }
    for (let index = 0; index < snapshotCount; index++) {
      if (input.signal?.aborted) throw input.signal.reason ?? new DOMException('rekey verification aborted', 'AbortError')
      if (!expectedSnapshotID) throw new TypeError('archive rekey snapshot history ended before its recorded sequence')
      const expectedSequence = headSequenceFromExpected - index
      const record = await input.loadHistoryRecord(expectedSnapshotID, expectedSequence, index, input.signal ?? new AbortController().signal)
      const snapshot = record?.snapshot
      const previousManifest = record?.previousManifest
      const newManifest = record?.newManifest
      const snapshotEnvelope = record?.envelope
      if (!snapshot || !previousManifest || !newManifest || !snapshotEnvelope) throw new TypeError('archive rekey history is incomplete')
      verifySnapshot(snapshot, input.archiveID)
      if (snapshot.sequence !== expectedSequence || expectedSequence < 1 ||
          snapshot.snapshot_id !== expectedSnapshotID ||
          (expectedSequence === 1 && Boolean(snapshot.previous_snapshot_id))) {
        throw new TypeError('archive rekey snapshot chain is incomplete or reordered')
      }
      expectedSnapshotID = snapshot.previous_snapshot_id
      const previousHash = manifestHash(snapshot, previousManifest, input.previousEpoch)
      const nextHash = manifestHash(snapshot, newManifest, input.newEpoch)
      if (digest('', utf8(canonicalIdentityJSON(snapshotEnvelope))) !== input.expected.envelope_sha256 ||
          canonicalIdentityJSON(snapshotEnvelope) !== canonicalIdentityJSON(input.envelope)) {
        throw new TypeError('archive rekey envelope changed within the snapshot history')
      }
      for (const object of newManifest.objects) {
        await verifyEncryptedObject(snapshot, newManifest, object, newDEK, input.fetchCiphertextRange, input.signal ?? new AbortController().signal)
      }
      commitment = sha256(concat(REKEY_DOMAIN, commitment, utf8(String(snapshot.sequence)), new Uint8Array([0]),
        utf8(snapshot.snapshot_id), new Uint8Array([0]), utf8(previousHash), new Uint8Array([0]), utf8(nextHash)))
    }
    if (expectedSnapshotID) throw new TypeError('archive rekey snapshot history contains an unverified predecessor')
    const bundleSetCommitment = `sha256:${hex(commitment)}`
    if (bundleSetCommitment !== input.expected.bundle_set_commitment) {
      throw new TypeError('archive rekey bundle commitment does not match the complete history')
    }
    return {
      archive_id: input.archiveID,
      previous_key_epoch: input.previousEpoch,
      new_key_epoch: input.newEpoch,
      snapshot_count: String(snapshotCount),
      head_snapshot_id: input.expected.head_snapshot_id,
      bundle_set_commitment: bundleSetCommitment,
      envelope_sha256: input.expected.envelope_sha256,
      recipient_key_id: input.expected.recipient_key_id,
    }
  } finally {
    newDEK.fill(0)
    sourceSigningPublicKey.fill(0)
  }
}

function base64urlSHA(publicKey: Uint8Array): string {
  const digestBytes = sha256(concat(utf8('IR/identity-key/v1\0'), publicKey))
  return base64url(digestBytes)
}

function base64url(bytes: Uint8Array): string {
  let binary = ''
  for (const byte of bytes) binary += String.fromCharCode(byte)
  return btoa(binary).replaceAll('+', '-').replaceAll('/', '_').replace(/=+$/u, '')
}
