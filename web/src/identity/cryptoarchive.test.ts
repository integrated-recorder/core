import { sha256 } from '@noble/hashes/sha2.js'
import { x25519 } from '@noble/curves/ed25519.js'
import { readFile } from 'node:fs/promises'
import { resolve } from 'node:path'
import { describe, expect, it } from 'vitest'
import { deriveRecoveryKeys } from './recovery'
import {
  CRYPTO_ARCHIVE_CHUNK_SIZE,
  decryptArchiveObject,
  encryptArchiveObject,
  openArchiveDEK,
  openSessionDEKEnvelope,
  recipientKeyId,
  wrapArchiveDEK,
  type KeyEnvelope,
  type SessionDEKBinding,
  type SessionDEKEnvelope,
  type CryptoObjectDescriptor,
} from './cryptoarchive'

function streamFrom(bytes: Uint8Array, splits: number[] = [bytes.length]): ReadableStream<Uint8Array> {
  return new ReadableStream<Uint8Array>({
    start(controller) {
      let offset = 0
      for (const size of splits) {
        if (offset >= bytes.length) break
        const end = Math.min(offset + size, bytes.length)
        controller.enqueue(bytes.slice(offset, end))
        offset = end
      }
      if (offset < bytes.length) controller.enqueue(bytes.slice(offset))
      controller.close()
    },
  })
}

function descriptor(plain: Uint8Array): CryptoObjectDescriptor {
  return {
    archiveId: 'arc_0123456789abcdefghijkl',
    coordinate: {
      sessionId: `session-${'a'.repeat(64)}`,
      trackId: 'track-0',
      sourceEpoch: '1',
      discontinuitySequence: '2',
      sequence: '42',
      kind: 'media',
    },
    objectType: 'media',
    plaintextSize: plain.length,
    plaintextSha256: `sha256:${Array.from(sha256(plain), byte => byte.toString(16).padStart(2, '0')).join('')}`,
    keyEpoch: '1',
  }
}

describe('archive cryptographic protocol', () => {
  it('opens the shared Go-generated Passkey playback envelope vector', async () => {
    const text = await readFile(resolve(process.cwd(), '../protocol/cryptoarchive-v1/session-key-envelope-test-vector.json'), 'utf8')
    const vector = JSON.parse(text) as {
      test_only_note: string
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
      envelope: SessionDEKEnvelope
    }
    expect(vector.test_only_note).toContain('never be used')
    const privateKey = Uint8Array.from(vector.recipient_private_key_hex.match(/.{2}/gu) ?? [], pair => Number.parseInt(pair, 16))
    const expectedDEK = Uint8Array.from(vector.dek_hex.match(/.{2}/gu) ?? [], pair => Number.parseInt(pair, 16))
    const binding: SessionDEKBinding = {
      archiveId: vector.binding.archive_id,
      snapshotId: vector.binding.snapshot_id,
      keyEpoch: vector.binding.key_epoch,
      ownerSubjectId: vector.binding.owner_subject_id,
      grantHash: vector.binding.grant_hash,
      supplierNodeId: vector.binding.supplier_node_id,
    }
    try {
      const opened = await openSessionDEKEnvelope(vector.envelope, privateKey, binding)
      expect(opened).toEqual(expectedDEK)
      opened.fill(0)

      await expect(openSessionDEKEnvelope(vector.envelope, privateKey, {
        ...binding,
        grantHash: `sha256:${'c'.repeat(64)}`,
      })).rejects.toThrow(/binding/u)
      const corrupted = { ...vector.envelope, ciphertext: `${vector.envelope.ciphertext.slice(0, -1)}A` }
      await expect(openSessionDEKEnvelope(corrupted, privateKey, binding)).rejects.toThrow()
    } finally {
      privateKey.fill(0)
      expectedDEK.fill(0)
    }
  })

  it('encrypts and decrypts bounded chunks without changing original bytes', async () => {
    const plain = new TextEncoder().encode('canonical media segment\0'.repeat(CRYPTO_ARCHIVE_CHUNK_SIZE / 24 + 31))
    const dek = new Uint8Array(32).fill(0x42)
    const encrypted: Uint8Array[] = []
    const result = await encryptArchiveObject(dek, descriptor(plain), streamFrom(plain, [3, 97, 5, 8193]), async chunk => {
      encrypted.push(chunk.slice())
    })
    const cipher = join(encrypted)
    expect(result.chunkCount).toBeGreaterThan(1)
    expect(result.ciphertextSize).toBe(cipher.length)
    expect(result.ciphertextSha256).toMatch(/^sha256:[0-9a-f]{64}$/u)

    const expected = descriptor(plain)
    expected.ciphertextSize = result.ciphertextSize
    expected.ciphertextSha256 = result.ciphertextSha256
    const restored: Uint8Array[] = []
    await decryptArchiveObject(dek, expected, streamFrom(cipher, [1, 4, 9, 17777]), async chunk => {
      restored.push(chunk.slice())
    })
    expect(Array.from(join(restored))).toEqual(Array.from(plain))
  })

  it('decrypts the shared Go-generated format vector byte-for-byte', async () => {
    const text = await readFile(resolve(process.cwd(), '../protocol/cryptoarchive-v1/object-test-vector.json'), 'utf8')
    const vector = JSON.parse(text) as {
      test_only_note: string
      dek_hex: string
      plaintext_base64: string
      ciphertext_base64: string
      descriptor: {
        archive_id: string
        coordinate: {
          session_id: string
          track_id: string
          source_epoch: string
          discontinuity_sequence: string
          sequence: string
          kind: 'media' | 'init'
        }
        object_type: 'media' | 'init'
        plaintext_size: number
        plaintext_sha256: string
        key_epoch: string
        ciphertext_size: number
        ciphertext_sha256: string
      }
    }
    expect(vector.test_only_note).toContain('never use')
    const dek = Uint8Array.from(vector.dek_hex.match(/.{2}/gu) ?? [], pair => Number.parseInt(pair, 16))
    const plain = Uint8Array.from(atob(vector.plaintext_base64), char => char.charCodeAt(0))
    const cipher = Uint8Array.from(atob(vector.ciphertext_base64), char => char.charCodeAt(0))
    const restored: Uint8Array[] = []
    await decryptArchiveObject(dek, {
      archiveId: vector.descriptor.archive_id,
      coordinate: {
        sessionId: vector.descriptor.coordinate.session_id,
        trackId: vector.descriptor.coordinate.track_id,
        sourceEpoch: vector.descriptor.coordinate.source_epoch,
        discontinuitySequence: vector.descriptor.coordinate.discontinuity_sequence,
        sequence: vector.descriptor.coordinate.sequence,
        kind: vector.descriptor.coordinate.kind,
      },
      objectType: vector.descriptor.object_type,
      plaintextSize: vector.descriptor.plaintext_size,
      plaintextSha256: vector.descriptor.plaintext_sha256,
      keyEpoch: vector.descriptor.key_epoch,
      ciphertextSize: vector.descriptor.ciphertext_size,
      ciphertextSha256: vector.descriptor.ciphertext_sha256,
    }, streamFrom(cipher, [5, 2, 1, 17]), async chunk => { restored.push(chunk.slice()) })
    expect(Array.from(join(restored))).toEqual(Array.from(plain))
  })

  it('decrypts the shared browser-generated vector in Go-compatible form', async () => {
    const text = await readFile(resolve(process.cwd(), '../protocol/cryptoarchive-v1/browser-object-test-vector.json'), 'utf8')
    const vector = JSON.parse(text) as {
      test_only_note: string
      dek_hex: string
      plaintext_base64: string
      ciphertext_base64: string
      descriptor: {
        archive_id: string
        coordinate: {
          session_id: string
          track_id: string
          source_epoch: string
          discontinuity_sequence: string
          sequence: string
          kind: 'media' | 'init'
        }
        object_type: 'media' | 'init'
        plaintext_size: number
        plaintext_sha256: string
        key_epoch: string
        ciphertext_size: number
        ciphertext_sha256: string
      }
    }
    expect(vector.test_only_note).toContain('never use')
    const dek = Uint8Array.from(vector.dek_hex.match(/.{2}/gu) ?? [], pair => Number.parseInt(pair, 16))
    const plain = Uint8Array.from(atob(vector.plaintext_base64), char => char.charCodeAt(0))
    const cipher = Uint8Array.from(atob(vector.ciphertext_base64), char => char.charCodeAt(0))
    const restored: Uint8Array[] = []
    await decryptArchiveObject(dek, {
      archiveId: vector.descriptor.archive_id,
      coordinate: {
        sessionId: vector.descriptor.coordinate.session_id,
        trackId: vector.descriptor.coordinate.track_id,
        sourceEpoch: vector.descriptor.coordinate.source_epoch,
        discontinuitySequence: vector.descriptor.coordinate.discontinuity_sequence,
        sequence: vector.descriptor.coordinate.sequence,
        kind: vector.descriptor.coordinate.kind,
      },
      objectType: vector.descriptor.object_type,
      plaintextSize: vector.descriptor.plaintext_size,
      plaintextSha256: vector.descriptor.plaintext_sha256,
      keyEpoch: vector.descriptor.key_epoch,
      ciphertextSize: vector.descriptor.ciphertext_size,
      ciphertextSha256: vector.descriptor.ciphertext_sha256,
    }, streamFrom(cipher), async chunk => { restored.push(chunk.slice()) })
    expect(Array.from(join(restored))).toEqual(Array.from(plain))
  })

  it('rejects altered chunks and does not accept a false source hash', async () => {
    const plain = new TextEncoder().encode('media bytes')
    const dek = new Uint8Array(32).fill(0x15)
    const encrypted: Uint8Array[] = []
    await encryptArchiveObject(dek, descriptor(plain), streamFrom(plain), async chunk => { encrypted.push(chunk.slice()) })
    const cipher = join(encrypted)
    cipher[cipher.length - 1] ^= 1
    const staged: Uint8Array[] = []
    await expect(decryptArchiveObject(dek, descriptor(plain), streamFrom(cipher), async chunk => { staged.push(chunk.slice()) }))
      .rejects.toThrow()

    const invalid = descriptor(plain)
    invalid.plaintextSha256 = `sha256:${'0'.repeat(64)}`
    const partialStage: Uint8Array[] = []
    await expect(encryptArchiveObject(dek, invalid, streamFrom(plain), async chunk => { partialStage.push(chunk.slice()) }))
      .rejects.toThrow(/declared identity/u)
    expect(partialStage.length).toBeGreaterThan(0)
    // The caller owns this staging sink and must discard it after rejection.
  })

  it('wraps and opens the DEK for the recovery key, binding archive, epoch, and signer', async () => {
    const recovery = await deriveRecoveryKeys(new Uint8Array(32))
    const dek = new Uint8Array(32).fill(0xc3)
    try {
      const envelope = await wrapArchiveDEK(dek, 'arc_0123456789abcdefghijkl', '4', recovery.encryptionPublicKey, recovery)
      expect(envelope.recipient_key_id).toBe(recipientKeyId(recovery.encryptionPublicKey))
      const restored = await openArchiveDEK(envelope, {
        archiveId: envelope.archive_id,
        keyEpoch: envelope.key_epoch,
        signerKeyId: recovery.signerKeyId,
        signerPublicKey: recovery.signingPublicKey,
        recipientPrivateKey: recovery.encryptionPrivateKey,
      })
      expect(restored).toEqual(dek)
      restored.fill(0)
      await expect(openArchiveDEK(envelope, {
        archiveId: envelope.archive_id,
        keyEpoch: '5',
        signerKeyId: recovery.signerKeyId,
        signerPublicKey: recovery.signingPublicKey,
        recipientPrivateKey: recovery.encryptionPrivateKey,
      })).rejects.toThrow()
      const forged = { ...envelope, signer_key_id: `ik_${'A'.repeat(43)}` }
      await expect(openArchiveDEK(forged, {
        archiveId: envelope.archive_id,
        keyEpoch: envelope.key_epoch,
        signerKeyId: recovery.signerKeyId,
        signerPublicKey: recovery.signingPublicKey,
        recipientPrivateKey: recovery.encryptionPrivateKey,
      })).rejects.toThrow()
    } finally {
      dek.fill(0)
      recovery.destroy()
    }
  })

  it('keeps the authority signing key independent from an ephemeral recipient key', async () => {
    const recovery = await deriveRecoveryKeys(new Uint8Array(32))
    const dek = new Uint8Array(32).fill(0x78)
    const ephemeralPrivate = new Uint8Array(32).fill(0x69)
    const ephemeralPublic = x25519.getPublicKey(ephemeralPrivate)
    try {
      const envelope = await wrapArchiveDEK(dek, 'arc_0123456789abcdefghijkl', '6', ephemeralPublic, recovery)
      expect(envelope.recipient_key_id).toBe(recipientKeyId(ephemeralPublic))
      const opened = await openArchiveDEK(envelope, {
        archiveId: envelope.archive_id,
        keyEpoch: envelope.key_epoch,
        signerKeyId: recovery.signerKeyId,
        signerPublicKey: recovery.signingPublicKey,
        recipientPrivateKey: ephemeralPrivate,
      })
      expect(Array.from(opened)).toEqual(Array.from(dek))
      opened.fill(0)
      await expect(openArchiveDEK(envelope, {
        archiveId: envelope.archive_id,
        keyEpoch: envelope.key_epoch,
        signerKeyId: recovery.signerKeyId,
        signerPublicKey: recovery.signingPublicKey,
        recipientPrivateKey: recovery.encryptionPrivateKey,
      })).rejects.toThrow(/recipient/u)
    } finally {
      dek.fill(0)
      ephemeralPrivate.fill(0)
      recovery.destroy()
    }
  })

  it('restores original content from ciphertext and a recovery-key envelope', async () => {
    const recovery = await deriveRecoveryKeys(new Uint8Array(32))
    const plain = new TextEncoder().encode('recovery-only archive restore proof\n'.repeat(2400))
    const dek = new Uint8Array(32).fill(0x31)
    const inputDescriptor = descriptor(plain)
    inputDescriptor.keyEpoch = '9'
    const ciphertextChunks: Uint8Array[] = []
    try {
      const result = await encryptArchiveObject(dek, inputDescriptor, streamFrom(plain, [211, 17, 65536]), async chunk => { ciphertextChunks.push(chunk.slice()) })
      const envelope = await wrapArchiveDEK(dek, inputDescriptor.archiveId, inputDescriptor.keyEpoch, recovery.encryptionPublicKey, recovery)
      dek.fill(0)
      const restoredDEK = await openArchiveDEK(envelope, {
        archiveId: inputDescriptor.archiveId,
        keyEpoch: inputDescriptor.keyEpoch,
        signerKeyId: recovery.signerKeyId,
        signerPublicKey: recovery.signingPublicKey,
        recipientPrivateKey: recovery.encryptionPrivateKey,
      })
      const output: Uint8Array[] = []
      await decryptArchiveObject(restoredDEK, {
        ...inputDescriptor,
        ciphertextSize: result.ciphertextSize,
        ciphertextSha256: result.ciphertextSha256,
      }, streamFrom(join(ciphertextChunks), [997, 13, 4096]), async chunk => { output.push(chunk.slice()) })
      expect(Array.from(join(output))).toEqual(Array.from(plain))
      restoredDEK.fill(0)
    } finally {
      dek.fill(0)
      recovery.destroy()
    }
  })

  it('opens the shared browser HPKE vector in the browser implementation', async () => {
    const text = await readFile(resolve(process.cwd(), '../protocol/cryptoarchive-v1/envelope-test-vector.json'), 'utf8')
    const vector = JSON.parse(text) as {
      test_only_note: string
      dek_hex: string
      archive_id: string
      key_epoch: string
      signer_key_id: string
      envelope: KeyEnvelope
    }
    expect(vector.test_only_note).toContain('MUST NEVER')
    const recovery = await deriveRecoveryKeys(new Uint8Array(32))
    try {
      expect(recovery.signerKeyId).toBe(vector.signer_key_id)
      const dek = await openArchiveDEK(vector.envelope, {
        archiveId: vector.archive_id,
        keyEpoch: vector.key_epoch,
        signerKeyId: vector.signer_key_id,
        signerPublicKey: recovery.signingPublicKey,
        recipientPrivateKey: recovery.encryptionPrivateKey,
      })
      expect(Array.from(dek)).toEqual(Array.from(Uint8Array.from(vector.dek_hex.match(/.{2}/gu) ?? [], pair => Number.parseInt(pair, 16))))
      dek.fill(0)
    } finally {
      recovery.destroy()
    }
  })

  it('opens the shared Go-generated HPKE vector in the browser implementation', async () => {
    const text = await readFile(resolve(process.cwd(), '../protocol/cryptoarchive-v1/go-envelope-test-vector.json'), 'utf8')
    const vector = JSON.parse(text) as {
      test_only_note: string
      dek_hex: string
      archive_id: string
      key_epoch: string
      signer_key_id: string
      envelope: KeyEnvelope
    }
    expect(vector.test_only_note).toContain('never use')
    const recovery = await deriveRecoveryKeys(new Uint8Array(32))
    try {
      const dek = await openArchiveDEK(vector.envelope, {
        archiveId: vector.archive_id,
        keyEpoch: vector.key_epoch,
        signerKeyId: vector.signer_key_id,
        signerPublicKey: recovery.signingPublicKey,
        recipientPrivateKey: recovery.encryptionPrivateKey,
      })
      expect(Array.from(dek)).toEqual(Array.from(Uint8Array.from(vector.dek_hex.match(/.{2}/gu) ?? [], pair => Number.parseInt(pair, 16))))
      dek.fill(0)
    } finally {
      recovery.destroy()
    }
  })
})

function join(chunks: Uint8Array[]): Uint8Array {
  const total = chunks.reduce((size, chunk) => size + chunk.length, 0)
  const result = new Uint8Array(total)
  let offset = 0
  for (const chunk of chunks) {
    result.set(chunk, offset)
    offset += chunk.length
  }
  return result
}
