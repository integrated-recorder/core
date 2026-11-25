import { webcrypto } from 'node:crypto'
import { sha256 } from '@noble/hashes/sha2.js'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import type { ArchiveSnapshot, ReplicaManifest, VerifiedGlobalIdentityRecipient } from '@/types/api'
import { encryptArchiveObject, wrapArchiveDEK, type CryptoObjectDescriptor, type KeyEnvelope } from './cryptoarchive'
import { deriveRecoveryKeys, canonicalIdentityJSON, type RecoveryKeyMaterial } from './recovery'
import { verifyRecipientRekeyHistory, type RekeyHistoryVerification } from './rekey-verification'

const ARCHIVE_ID = `arc_${'A'.repeat(22)}`
const SESSION_ID = `session-${'a'.repeat(64)}`
const NOW = new Date('2026-11-04T00:00:00.000Z')
const SNAPSHOT_CONTENT_DOMAIN = 'IR/archive-snapshot/v2/content\0'
const SNAPSHOT_PROVENANCE_DOMAIN = 'IR/archive-snapshot/v1/provenance\0'
const MANIFEST_DOMAIN = 'IR/replica-manifest/v1\0'
const REKEY_DOMAIN = 'IR/archive-rekey-history/v1\0'

function b64url(value: Uint8Array): string {
  return Buffer.from(value).toString('base64url')
}

function hash(value: Uint8Array): string {
  return `sha256:${Buffer.from(sha256(value)).toString('hex')}`
}

function canonicalSnapshotJSON(value: unknown): string {
  if (value === null || typeof value === 'boolean' || typeof value === 'string' || typeof value === 'number') return JSON.stringify(value)
  if (Array.isArray(value)) return `[${value.map(canonicalSnapshotJSON).join(',')}]`
  const object = value as Record<string, unknown>
  return `{${Object.keys(object).sort().map(key => `${JSON.stringify(key)}:${canonicalSnapshotJSON(object[key])}`).join(',')}}`
}

function stream(bytes: Uint8Array): ReadableStream<Uint8Array> {
  return new ReadableStream({
    start(controller) { controller.enqueue(bytes.slice()); controller.close() },
  })
}

function concat(...parts: Uint8Array[]): Uint8Array {
  const output = new Uint8Array(parts.reduce((length, part) => length + part.length, 0))
  let offset = 0
  for (const part of parts) { output.set(part, offset); offset += part.length }
  return output
}

function manifestHash(manifest: ReplicaManifest): string {
  const document = {
    replica_manifest_version: manifest.replica_manifest_version,
    archive_id: manifest.archive_id,
    snapshot_id: manifest.snapshot_id,
    key_epoch: manifest.key_epoch,
    objects: manifest.objects.map(object => ({
      session_id: object.coordinate.session_id,
      track_id: object.coordinate.track_id,
      source_epoch: String(object.coordinate.source_epoch),
      discontinuity_sequence: String(object.coordinate.discontinuity_sequence),
      sequence: String(object.coordinate.sequence),
      kind: object.coordinate.kind,
      object_id: object.object_id,
      ciphertext_size: object.ciphertext_size,
      ciphertext_sha256: object.ciphertext_sha256,
    })),
  }
  return hash(concat(new TextEncoder().encode(MANIFEST_DOMAIN), new TextEncoder().encode(canonicalIdentityJSON(document))))
}

function identity(keys: RecoveryKeyMaterial): VerifiedGlobalIdentityRecipient {
  return {
    identity_id: keys.identityId,
    head_hash: `sha256:${'b'.repeat(64)}`,
    verified_at: NOW.toISOString(),
    fresh_until: new Date(NOW.getTime() + 4 * 60 * 1000).toISOString(),
    frozen: false,
    identity_signing_public_key: b64url(keys.signingPublicKey),
    recovery_encryption_public_key: b64url(keys.encryptionPublicKey),
  }
}

async function fixture() {
  const sourceKeys = await deriveRecoveryKeys(new Uint8Array(32).fill(0x11))
  const recipientKeys = await deriveRecoveryKeys(new Uint8Array(32).fill(0x22))
  const plains = [new TextEncoder().encode('canonical media payload 1'), new TextEncoder().encode('canonical media payload 2')]
  const coordinates = plains.map((_, index) => ({
    session_id: SESSION_ID, track_id: 'main', source_epoch: 1, discontinuity_sequence: 1, sequence: index + 1, kind: 'media' as const,
  }))
  const provenance: unknown[] = []
  const makeSnapshot = (sequence: number, previousSnapshotID: string | undefined): ArchiveSnapshot => {
    const objects = coordinates.slice(0, sequence).map((coordinate, index) => ({
      coordinate,
      timeline_ordinal: index + 1,
      duration: 2.5,
      state: 'present' as const,
      plaintext_size: plains[index]!.length,
      plaintext_sha256: hash(plains[index]!),
    }))
    const content = {
      archive_id: ARCHIVE_ID,
      session: { id: SESSION_ID, adapter_id: 'hls' },
      capture_state: 'closed' as const,
      archive_state: 'sealed' as const,
      objects,
      coverage: [],
    }
    const contentHash = hash(concat(new TextEncoder().encode(SNAPSHOT_CONTENT_DOMAIN), new TextEncoder().encode(canonicalSnapshotJSON(content))))
    return {
      schema_version: 2,
      sequence,
      ...(previousSnapshotID ? { previous_snapshot_id: previousSnapshotID } : {}),
      snapshot_id: `snap_${contentHash.slice('sha256:'.length)}`,
      content_hash: contentHash,
      provenance_hash: hash(concat(new TextEncoder().encode(SNAPSHOT_PROVENANCE_DOMAIN), new TextEncoder().encode('[]'))),
      content,
      provenance,
    }
  }
  const snapshots = [makeSnapshot(1, undefined)]
  snapshots.push(makeSnapshot(2, snapshots[0]!.snapshot_id))
  const encrypt = async (snapshot: ArchiveSnapshot, keyEpoch: string, dek: Uint8Array) => {
    const encrypted = await Promise.all(snapshot.content.objects.map(async object => {
      const index = object.coordinate.sequence - 1
      const plain = plains[index]!
      const coordinate = object.coordinate
      const descriptor: CryptoObjectDescriptor = {
        archiveId: ARCHIVE_ID,
        coordinate: {
          sessionId: coordinate.session_id, trackId: coordinate.track_id, sourceEpoch: String(coordinate.source_epoch),
          discontinuitySequence: String(coordinate.discontinuity_sequence), sequence: String(coordinate.sequence), kind: coordinate.kind ?? 'media',
        },
        objectType: coordinate.kind ?? 'media', plaintextSize: plain.length, plaintextSha256: hash(plain), keyEpoch,
      }
      const chunks: Uint8Array[] = []
      const result = await encryptArchiveObject(dek, descriptor, stream(plain), async chunk => { chunks.push(chunk.slice()) })
      const ciphertext = concat(...chunks)
      return {
        ciphertext,
        object: {
          coordinate: { ...coordinate, kind: coordinate.kind ?? 'media' },
          object_id: `ob_${Buffer.from(sha256(ciphertext)).toString('base64url')}`,
          ciphertext_size: String(result.ciphertextSize),
          ciphertext_sha256: result.ciphertextSha256,
        },
      }
    }))
    return {
      ciphertexts: new Map(encrypted.map(item => [item.object.object_id, item.ciphertext])),
      manifest: {
        replica_manifest_version: 'ir.replica-manifest/1',
        archive_id: ARCHIVE_ID,
        snapshot_id: snapshot.snapshot_id,
        key_epoch: keyEpoch,
        objects: encrypted.map(item => item.object),
      } satisfies ReplicaManifest,
    }
  }
  const oldDEK = new Uint8Array(32).fill(0x33)
  const newDEK = new Uint8Array(32).fill(0x44)
  const encryptedHistory = await Promise.all(snapshots.map(async snapshot => ({
    snapshot,
    oldEncrypted: await encrypt(snapshot, '1', oldDEK),
    newEncrypted: await encrypt(snapshot, '2', newDEK),
  })))
  const bySnapshot = new Map(encryptedHistory.map(record => [record.snapshot.snapshot_id, record]))
  const envelope: KeyEnvelope = await wrapArchiveDEK(newDEK, ARCHIVE_ID, '2', recipientKeys.encryptionPublicKey, sourceKeys)
  let commitment = sha256(concat(new TextEncoder().encode(REKEY_DOMAIN), new TextEncoder().encode(`begin\0${ARCHIVE_ID}`)))
  for (const record of [...encryptedHistory].reverse()) {
    commitment = sha256(concat(new TextEncoder().encode(REKEY_DOMAIN), commitment, new TextEncoder().encode(String(record.snapshot.sequence)), new Uint8Array([0]),
      new TextEncoder().encode(record.snapshot.snapshot_id), new Uint8Array([0]), new TextEncoder().encode(manifestHash(record.oldEncrypted.manifest)), new Uint8Array([0]),
      new TextEncoder().encode(manifestHash(record.newEncrypted.manifest))))
  }
  const expected: RekeyHistoryVerification = {
    archive_id: ARCHIVE_ID,
    previous_key_epoch: '1',
    new_key_epoch: '2',
    snapshot_count: String(snapshots.length),
    head_sequence: String(snapshots.length),
    head_snapshot_id: snapshots.at(-1)!.snapshot_id,
    bundle_set_commitment: `sha256:${Buffer.from(commitment).toString('hex')}`,
    envelope_sha256: hash(new TextEncoder().encode(canonicalIdentityJSON(envelope))),
    recipient_key_id: envelope.recipient_key_id,
  }
  return { sourceKeys, recipientKeys, snapshots, bySnapshot, envelope, expected }
}

describe('recipient-side archive rekey verification', () => {
  beforeEach(() => vi.stubGlobal('crypto', webcrypto as unknown as Crypto))
  afterEach(() => vi.unstubAllGlobals())

  it('verifies the sealed snapshot, both manifests, recipient envelope and new epoch ciphertext', async () => {
    const value = await fixture()
    const evidence = await verifyRecipientRekeyHistory({
      archiveID: ARCHIVE_ID, previousEpoch: '1', newEpoch: '2',
      envelope: value.envelope, sourceIdentity: identity(value.sourceKeys), recipientIdentity: identity(value.recipientKeys),
      recipientKeys: value.recipientKeys, expected: value.expected, now: NOW,
      loadHistoryRecord: async (snapshotID, expectedSequence) => {
        const record = value.bySnapshot.get(snapshotID)
        expect(record?.snapshot.sequence).toBe(expectedSequence)
        if (!record) throw new Error('missing snapshot fixture')
        return { snapshot: record.snapshot, previousManifest: record.oldEncrypted.manifest, newManifest: record.newEncrypted.manifest, envelope: value.envelope }
      },
      fetchCiphertextRange: async (snapshotID, _epoch, objectID, offset, length) => {
        const record = value.bySnapshot.get(snapshotID)
        const ciphertext = record?.newEncrypted.ciphertexts.get(objectID)
        if (!ciphertext) throw new Error('missing ciphertext fixture')
        return ciphertext.slice(offset, offset + length)
      },
    })
    expect(evidence).toMatchObject({
      archive_id: ARCHIVE_ID,
      previous_key_epoch: '1',
      new_key_epoch: '2',
      snapshot_count: '2',
      head_snapshot_id: value.snapshots.at(-1)!.snapshot_id,
      bundle_set_commitment: value.expected.bundle_set_commitment,
      recipient_key_id: value.expected.recipient_key_id,
    })
    value.sourceKeys.destroy()
    value.recipientKeys.destroy()
  })

  it('rejects a wrong recipient, incomplete epoch history and modified ciphertext', async () => {
    const value = await fixture()
    const otherRecipient = await deriveRecoveryKeys(new Uint8Array(32).fill(0x55))
    const input = {
      archiveID: ARCHIVE_ID, previousEpoch: '1', newEpoch: '2',
      envelope: value.envelope, sourceIdentity: identity(value.sourceKeys), recipientIdentity: identity(value.recipientKeys),
      recipientKeys: value.recipientKeys, expected: value.expected, now: NOW,
      loadHistoryRecord: async (snapshotID: string) => {
        const record = value.bySnapshot.get(snapshotID)
        if (!record) throw new Error('missing snapshot fixture')
        return { snapshot: record.snapshot, previousManifest: record.oldEncrypted.manifest, newManifest: record.newEncrypted.manifest, envelope: value.envelope }
      },
      fetchCiphertextRange: async (snapshotID: string, _epoch: string, objectID: string, offset: number, length: number) => {
        const ciphertext = value.bySnapshot.get(snapshotID)?.newEncrypted.ciphertexts.get(objectID)
        if (!ciphertext) throw new Error('missing ciphertext fixture')
        return ciphertext.slice(offset, offset + length)
      },
    }
    await expect(verifyRecipientRekeyHistory({ ...input, recipientKeys: otherRecipient })).rejects.toThrow(/identity|recipient/u)
    await expect(verifyRecipientRekeyHistory({ ...input, expected: { ...value.expected, snapshot_count: '3', head_sequence: '3' } })).rejects.toThrow(/history|snapshot/u)
    await expect(verifyRecipientRekeyHistory({ ...input,
      fetchCiphertextRange: async (snapshotID: string, _epoch: string, objectID: string, offset: number, length: number) => {
        const ciphertext = value.bySnapshot.get(snapshotID)?.newEncrypted.ciphertexts.get(objectID)
        if (!ciphertext) throw new Error('missing ciphertext fixture')
        const chunk = ciphertext.slice(offset, offset + length)
        if (offset === 0) chunk[chunk.length - 1] ^= 1
        return chunk
      },
    })).rejects.toThrow()
    value.sourceKeys.destroy()
    value.recipientKeys.destroy()
    otherRecipient.destroy()
  })

  it('rejects a changed previous-epoch manifest even when new-epoch objects still decrypt', async () => {
    const value = await fixture()
    const input = {
      archiveID: ARCHIVE_ID, previousEpoch: '1', newEpoch: '2',
      envelope: value.envelope, sourceIdentity: identity(value.sourceKeys), recipientIdentity: identity(value.recipientKeys),
      recipientKeys: value.recipientKeys, expected: value.expected, now: NOW,
      loadHistoryRecord: async (snapshotID: string) => {
        const record = value.bySnapshot.get(snapshotID)
        if (!record) throw new Error('missing snapshot fixture')
        const previousManifest = structuredClone(record.oldEncrypted.manifest)
        previousManifest.objects[0]!.ciphertext_size = String(Number(previousManifest.objects[0]!.ciphertext_size) + 1)
        return { snapshot: record.snapshot, previousManifest, newManifest: record.newEncrypted.manifest, envelope: value.envelope }
      },
      fetchCiphertextRange: async (snapshotID: string, _epoch: string, objectID: string, offset: number, length: number) => {
        const ciphertext = value.bySnapshot.get(snapshotID)?.newEncrypted.ciphertexts.get(objectID)
        if (!ciphertext) throw new Error('missing ciphertext fixture')
        return ciphertext.slice(offset, offset + length)
      },
    }
    await expect(verifyRecipientRekeyHistory(input)).rejects.toThrow(/commitment/u)
    value.sourceKeys.destroy()
    value.recipientKeys.destroy()
  })
})
