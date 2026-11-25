import { webcrypto } from 'node:crypto'
import vector from '../../../protocol/authority-v1/test-vector.json'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import {
  acceptOwnerSubjectTransferOffer,
  capabilityGrantHash,
  createSignedOwnerSubjectTransferOffer,
  createCapabilityNonce,
  formatAuthorityTimestamp,
  signCapabilityGrant,
  signArchiveRekeyCompleteEvent,
  signOwnerAuthorityEvent,
  verifyAcceptedOwnerSubjectTransfer,
  validOwnerSubjectReplicationPolicy,
  type CapabilityGrant,
  type OwnerAuthorityEvent,
} from './authority'
import { deriveRecoveryKeys } from './recovery'
import { recipientKeyId } from './cryptoarchive'
import type { OwnerAuthoritySubject, VerifiedGlobalIdentityRecipient } from '@/types/api'
import type { RecipientRekeyEvidence } from './rekey-verification'

function decodeBase64url(value: string): Uint8Array {
  return Uint8Array.from(Buffer.from(value, 'base64url'))
}

describe('capability grant browser signing profile', () => {
  beforeEach(() => vi.stubGlobal('crypto', webcrypto as unknown as Crypto))
  afterEach(() => vi.unstubAllGlobals())

  it('matches the Go signature and hash vector exactly', async () => {
    const grant = vector.grant as CapabilityGrant
    const signed = signCapabilityGrant(grant, decodeBase64url(vector.root_seed))
    expect(signed.signature).toBe(vector.signature)
    expect(await capabilityGrantHash(grant)).toBe(vector.grant_hash)
  })

  it('matches the Go signed Archive claim event vector exactly', () => {
    const event = vector.archive_claim_event.event as OwnerAuthorityEvent
    expect(signOwnerAuthorityEvent(event, decodeBase64url(vector.root_seed)).signature).toBe(vector.archive_claim_event.signature)
  })

  it('creates a 256-bit nonce and refuses an invalid private seed', () => {
    expect(createCapabilityNonce()).toMatch(/^[A-Za-z0-9_-]{43}$/u)
    expect(() => signCapabilityGrant(vector.grant as CapabilityGrant, new Uint8Array(31))).toThrow(/seed/u)
  })

  it('keeps caller-selected numeric fields out of the canonical signed profile', () => {
    const invalid = { ...vector.grant, policy_epoch: 7 } as unknown as CapabilityGrant
    expect(() => signCapabilityGrant(invalid, decodeBase64url(vector.root_seed))).toThrow(/unsupported value/u)
  })

  it('formats OwnerSubject timestamps as canonical UTC seconds accepted by the Go protocol', () => {
    expect(formatAuthorityTimestamp(new Date('2026-11-02T14:00:04.287Z'))).toBe('2026-11-02T14:00:04Z')
  })

  it('accepts only canonical, bounded OwnerSubject replication policy values', () => {
    const valid = { minimum_replicas: '2', grace_period_seconds: '86400', capacity_limit_bytes: '1073741824', relocate_after_grace: true }
    expect(validOwnerSubjectReplicationPolicy(valid)).toBe(true)
    expect(validOwnerSubjectReplicationPolicy({ ...valid, minimum_replicas: '02' })).toBe(false)
    expect(validOwnerSubjectReplicationPolicy({ ...valid, minimum_replicas: '17' })).toBe(false)
    expect(validOwnerSubjectReplicationPolicy({ ...valid, grace_period_seconds: '2592001' })).toBe(false)
    expect(validOwnerSubjectReplicationPolicy({ ...valid, capacity_limit_bytes: '1125899906842625' })).toBe(false)
    expect(validOwnerSubjectReplicationPolicy({ ...valid, relocate_after_grace: 'true' as unknown as boolean })).toBe(false)
  })

  it('requires fresh heads and verifies both signatures in the dual-consent transfer exchange', async () => {
    const now = new Date('2026-11-02T14:00:04Z')
    const sourceKeys = await deriveRecoveryKeys(new Uint8Array(32).fill(0x21))
    const recipientKeys = await deriveRecoveryKeys(new Uint8Array(32).fill(0x42))
    const encoded = (value: Uint8Array) => Buffer.from(value).toString('base64url')
    const source = {
      identity_id: sourceKeys.identityId,
      head_hash: `sha256:${'a'.repeat(64)}`,
      verified_at: now.toISOString(), fresh_until: new Date(now.getTime() + 4 * 60_000).toISOString(), frozen: false,
      identity_signing_public_key: encoded(sourceKeys.signingPublicKey),
    }
    const recipient = {
      identity_id: recipientKeys.identityId,
      head_hash: `sha256:${'b'.repeat(64)}`,
      verified_at: now.toISOString(), fresh_until: new Date(now.getTime() + 4 * 60_000).toISOString(), frozen: false,
      identity_signing_public_key: encoded(recipientKeys.signingPublicKey),
    }
    try {
      const signedOffer = await createSignedOwnerSubjectTransferOffer({
        subjectID: `os_${'s'.repeat(22)}`, fromIdentity: source, recipient, oldPolicyEpoch: '7', signer: sourceKeys, now,
      })
      const accepted = await acceptOwnerSubjectTransferOffer({ signedOffer, source, recipient, signer: recipientKeys, now })
      expect(accepted.offer.transfer_id).toMatch(/^tr_[A-Za-z0-9_-]{22}$/u)
      expect(accepted.offer.new_policy_epoch).toBe('8')
      await expect(verifyAcceptedOwnerSubjectTransfer({ accepted, source, recipient, now })).resolves.toBeUndefined()

      await expect(acceptOwnerSubjectTransferOffer({
        signedOffer, source, recipient: { ...recipient, head_hash: `sha256:${'c'.repeat(64)}` }, signer: recipientKeys, now,
      })).rejects.toThrow(/bound/u)
      await expect(acceptOwnerSubjectTransferOffer({
        signedOffer: { ...signedOffer, signature: encoded(new Uint8Array(64)) }, source, recipient, signer: recipientKeys, now,
      })).rejects.toThrow(/signature/u)
      await expect(verifyAcceptedOwnerSubjectTransfer({
        accepted: { ...accepted, recipient_proof: encoded(new Uint8Array(64)) }, source, recipient, now,
      })).rejects.toThrow(/signature|consent/u)
      await expect(verifyAcceptedOwnerSubjectTransfer({ accepted, source: { ...source, head_hash: `sha256:${'c'.repeat(64)}` }, recipient, now })).rejects.toThrow(/source identity/u)
      await expect(acceptOwnerSubjectTransferOffer({ signedOffer, source, recipient, signer: recipientKeys, now: new Date(now.getTime() + 2 * 60 * 60_000) })).rejects.toThrow(/stale/u)
    } finally {
      sourceKeys.destroy()
      recipientKeys.destroy()
    }
  })
})

describe('recipient OwnerSubject rekey completion signing', () => {
  beforeEach(() => vi.stubGlobal('crypto', webcrypto as unknown as Crypto))
  afterEach(() => vi.unstubAllGlobals())

  it('signs only evidence covering every pending claimed archive for the current identity', async () => {
    const now = new Date('2026-11-04T00:00:00.000Z')
    const keys = await deriveRecoveryKeys(new Uint8Array(32).fill(0x6a))
    const encoded = (value: Uint8Array) => Buffer.from(value).toString('base64url')
    const identity: VerifiedGlobalIdentityRecipient = {
      identity_id: keys.identityId, head_hash: `sha256:${'a'.repeat(64)}`, verified_at: now.toISOString(),
      fresh_until: new Date(now.getTime() + 4 * 60_000).toISOString(), frozen: false,
      identity_signing_public_key: encoded(keys.signingPublicKey), recovery_encryption_public_key: encoded(keys.encryptionPublicKey),
    }
    const archiveID = `arc_${'A'.repeat(22)}`
    const subject: OwnerAuthoritySubject = {
      subject: {
        subject_id: `os_${'s'.repeat(22)}`, subject_type: 'person', controller: { kind: 'identity', id: keys.identityId },
        controller_policy: 'global_identity_recovery_root/1', policy_epoch: '2', created_at: now.toISOString(),
      },
      replication_policy: { minimum_replicas: '0', grace_period_seconds: '0', capacity_limit_bytes: '0', relocate_after_grace: false },
      archive_ids: [archiveID], rekey_required_archives: [archiveID], rekey_required_since_event_hash: `sha256:${'b'.repeat(64)}`,
      sequence: '4', head_hash: `sha256:${'c'.repeat(64)}`, frozen: false, fork_count: 0, grants: [],
    }
    const evidence: RecipientRekeyEvidence = {
      archive_id: archiveID, previous_key_epoch: '1', new_key_epoch: '2', snapshot_count: '3',
      head_snapshot_id: `snap_${'d'.repeat(64)}`, bundle_set_commitment: `sha256:${'e'.repeat(64)}`,
      envelope_sha256: `sha256:${'f'.repeat(64)}`, recipient_key_id: recipientKeyId(keys.encryptionPublicKey),
    }
    try {
      const signed = await signArchiveRekeyCompleteEvent({ identity, subject, signer: keys, evidence: [evidence], now })
      expect(signed.event.operation).toBe('archive_rekey_complete')
      expect(signed.event.payload).toEqual({ transfer_event_hash: subject.rekey_required_since_event_hash, archives: [evidence] })
      await expect(signArchiveRekeyCompleteEvent({ identity, subject, signer: keys, evidence: [] , now })).rejects.toThrow(/context|pending/u)
      await expect(signArchiveRekeyCompleteEvent({ identity, subject, signer: keys, evidence: [{ ...evidence, recipient_key_id: `rk_${'x'.repeat(43)}` }], now })).rejects.toThrow(/evidence/u)
      await expect(signArchiveRekeyCompleteEvent({ identity: { ...identity, frozen: true }, subject, signer: keys, evidence: [evidence], now })).rejects.toThrow(/frozen|stale/u)
    } finally {
      keys.destroy()
    }
  })
})
