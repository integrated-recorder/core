import { canonicalIdentityJSON, signRecoveryMessage } from './recovery'
import { ed25519 } from '@noble/curves/ed25519.js'
import { sha256 } from '@noble/hashes/sha2.js'
import { recipientKeyId } from './cryptoarchive'
import type { OwnerAuthoritySubject, OwnerSubjectReplicationPolicy, VerifiedGlobalIdentityRecipient } from '@/types/api'
import type { RecoveryKeyMaterial } from './recovery'
import type { RecipientRekeyEvidence } from './rekey-verification'

const SIGN_DOMAIN = new TextEncoder().encode('IR/capability-grant/v1/sign\0')
const HASH_DOMAIN = new TextEncoder().encode('IR/capability-grant/v1/hash\0')
const OWNER_AUTHORITY_EVENT_DOMAIN = new TextEncoder().encode('IR/owner-authority/v1/sign\0')
const OWNER_TRANSFER_OFFER_DOMAIN = new TextEncoder().encode('IR/owner-subject-transfer/v1/offer\0')
const OWNER_TRANSFER_ACCEPT_DOMAIN = new TextEncoder().encode('IR/owner-subject-transfer/v1/accept\0')

/** Owner-authority timestamps use the protocol's canonical UTC-second form. */
export function formatAuthorityTimestamp(value: Date): string {
  return value.toISOString().replace(/\.\d{3}Z$/u, 'Z')
}

export type AuthorityPrincipal = {
  kind: 'identity' | 'node'
  id: string
}

export type CapabilityGrant = {
  grant_version: 'ir.capability.grant/1'
  subject_id: string
  issuer: AuthorityPrincipal
  audience: AuthorityPrincipal
  archive_id: string
  snapshot_id?: string
  actions: string[]
  not_before: string
  expires_at: string
  delegation_depth: string
  nonce: string
  parent_grant_hash?: string
  policy_epoch: string
  issuer_key_id: string
}

export type SignedCapabilityGrant = {
  grant: CapabilityGrant
  signature: string
}

export type OwnerAuthorityEvent = {
  event_version: 'ir.owner-authority.event/1'
  identity_id: string
  identity_head_hash: string
  subject_id: string
  sequence: string
  previous_event_hash?: string
  operation: 'subject_create' | 'archive_claim' | 'grant_issue' | 'grant_revoke' | 'subject_transfer' | 'archive_rekey_complete' | 'replication_policy_set'
  issued_at: string
  signer: AuthorityPrincipal
  signer_key_id: string
  payload: Record<string, unknown>
}

export type SignedOwnerAuthorityEvent = {
  event: OwnerAuthorityEvent
  signature: string
}

export type OwnerSubjectTransferOffer = {
  transfer_version: 'ir.owner-subject.transfer/1'
  transfer_id: string
  subject_id: string
  from: AuthorityPrincipal
  to: AuthorityPrincipal
  old_policy_epoch: string
  new_policy_epoch: string
  old_identity_head_hash: string
  recipient_identity_head_hash: string
  nonce: string
  issued_at: string
  expires_at: string
}

export type SignedOwnerSubjectTransferOffer = {
  offer: OwnerSubjectTransferOffer
  signature: string
}

export type AcceptedOwnerSubjectTransfer = {
  offer: OwnerSubjectTransferOffer
  current_controller_proof: string
  recipient_proof: string
}

export type VerifiedTransferIdentity = {
  identity_id: string
  head_hash: string
  verified_at: string
  fresh_until: string
  frozen: boolean
  identity_signing_public_key: string
}

export type TransferRecoverySigner = {
  identityId: string
  signerKeyId: string
  signingSeed: Uint8Array
  signingPublicKey: Uint8Array
}

function concat(...parts: Uint8Array[]): Uint8Array {
  const result = new Uint8Array(parts.reduce((total, part) => total + part.length, 0))
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

function decodeBase64url(value: string, expectedLength: number): Uint8Array {
  if (!/^[A-Za-z0-9_-]+$/u.test(value)) throw new TypeError('transfer signature encoding is invalid')
  const bytes = Uint8Array.from(atob(value.replaceAll('-', '+').replaceAll('_', '/') + '='.repeat((4 - value.length % 4) % 4)), character => character.charCodeAt(0))
  if (bytes.length !== expectedLength || base64url(bytes) !== value) throw new TypeError('transfer signature encoding is invalid')
  return bytes
}

function validTransferOffer(offer: OwnerSubjectTransferOffer, now: Date): void {
  if (offer.transfer_version !== 'ir.owner-subject.transfer/1' || !/^tr_[A-Za-z0-9_-]{22}$/u.test(offer.transfer_id) ||
    !/^os_[A-Za-z0-9_-]{22}$/u.test(offer.subject_id) || offer.from.kind !== 'identity' || offer.to.kind !== 'identity' ||
    !/^gi_[A-Za-z0-9_-]{22}$/u.test(offer.from.id) || !/^gi_[A-Za-z0-9_-]{22}$/u.test(offer.to.id) || offer.from.id === offer.to.id ||
    !/^(0|[1-9][0-9]{0,8})$/u.test(offer.old_policy_epoch) || !/^(0|[1-9][0-9]{0,8})$/u.test(offer.new_policy_epoch) ||
    BigInt(offer.new_policy_epoch) !== BigInt(offer.old_policy_epoch) + 1n ||
    !/^sha256:[a-f0-9]{64}$/u.test(offer.old_identity_head_hash) || !/^sha256:[a-f0-9]{64}$/u.test(offer.recipient_identity_head_hash) ||
    !/^[A-Za-z0-9_-]{43}$/u.test(offer.nonce) || decodeBase64url(offer.nonce, 32).length !== 32) {
    throw new TypeError('transfer offer is invalid')
  }
  const issued = Date.parse(offer.issued_at)
  const expires = Date.parse(offer.expires_at)
  if (!Number.isFinite(issued) || !Number.isFinite(expires) || issued >= expires || expires - issued > 24 * 60 * 60 * 1000 ||
    issued > now.getTime() + 30_000 || expires <= now.getTime()) throw new TypeError('transfer offer is expired or invalid')
}

async function verifiedTransferIdentity(value: VerifiedTransferIdentity, now: Date): Promise<void> {
  const verifiedAt = Date.parse(value.verified_at)
  const freshUntil = Date.parse(value.fresh_until)
  if (value.frozen || !/^gi_[A-Za-z0-9_-]{22}$/u.test(value.identity_id) || !/^sha256:[a-f0-9]{64}$/u.test(value.head_hash) ||
    !Number.isFinite(verifiedAt) || !Number.isFinite(freshUntil) || verifiedAt > now.getTime() || now.getTime() - verifiedAt > 5 * 60 * 1000 ||
    freshUntil <= now.getTime() || freshUntil > verifiedAt + 5 * 60 * 1000) {
    throw new TypeError('verified transfer identity is unavailable or stale')
  }
  const signingPublicKey = decodeBase64url(value.identity_signing_public_key, 32)
  const identityDigest = new Uint8Array(await globalThis.crypto.subtle.digest(
    'SHA-256',
    concat(new TextEncoder().encode('IR/global-identity-id/v1\0'), signingPublicKey),
  ))
  if (`gi_${base64url(identityDigest.subarray(0, 16))}` !== value.identity_id) throw new TypeError('verified transfer identity key does not match its ID')
}

function transferBytes(domain: Uint8Array, value: unknown): Uint8Array {
  return concat(domain, new TextEncoder().encode(canonicalIdentityJSON(value)))
}

export async function createSignedOwnerSubjectTransferOffer(input: {
  subjectID: string
  fromIdentity: VerifiedTransferIdentity
  recipient: VerifiedTransferIdentity
  oldPolicyEpoch: string
  signer: TransferRecoverySigner
  now?: Date
}): Promise<SignedOwnerSubjectTransferOffer> {
  const now = input.now ?? new Date()
  await verifiedTransferIdentity(input.fromIdentity, now)
  await verifiedTransferIdentity(input.recipient, now)
  if (input.signer.identityId !== input.fromIdentity.identity_id || !/^os_[A-Za-z0-9_-]{22}$/u.test(input.subjectID) ||
    !/^(0|[1-9][0-9]{0,8})$/u.test(input.oldPolicyEpoch) || BigInt(input.oldPolicyEpoch) >= 999_999_999n || input.signer.signingSeed.length !== 32 ||
    !sameBytes(ed25519.getPublicKey(input.signer.signingSeed), input.signer.signingPublicKey) ||
    !sameBytes(input.signer.signingPublicKey, decodeBase64url(input.fromIdentity.identity_signing_public_key, 32))) {
    throw new TypeError('transfer signing context is invalid')
  }
  const offer: OwnerSubjectTransferOffer = {
    transfer_version: 'ir.owner-subject.transfer/1',
    transfer_id: `tr_${base64url(globalThis.crypto.getRandomValues(new Uint8Array(16)))}`,
    subject_id: input.subjectID,
    from: { kind: 'identity', id: input.fromIdentity.identity_id },
    to: { kind: 'identity', id: input.recipient.identity_id },
    old_policy_epoch: input.oldPolicyEpoch,
    new_policy_epoch: (BigInt(input.oldPolicyEpoch) + 1n).toString(),
    old_identity_head_hash: input.fromIdentity.head_hash,
    recipient_identity_head_hash: input.recipient.head_hash,
    nonce: base64url(globalThis.crypto.getRandomValues(new Uint8Array(32))),
    issued_at: formatAuthorityTimestamp(now),
    expires_at: formatAuthorityTimestamp(new Date(now.getTime() + 60 * 60 * 1000)),
  }
  validTransferOffer(offer, now)
  return { offer, signature: base64url(signRecoveryMessage(transferBytes(OWNER_TRANSFER_OFFER_DOMAIN, offer), input.signer.signingSeed)) }
}

async function verifySignedOwnerSubjectTransferOffer(
  signed: SignedOwnerSubjectTransferOffer,
  source: VerifiedTransferIdentity,
  now: Date,
): Promise<void> {
  await verifiedTransferIdentity(source, now)
  validTransferOffer(signed.offer, now)
  if (signed.offer.from.id !== source.identity_id || signed.offer.old_identity_head_hash !== source.head_hash ||
    !ed25519.verify(decodeBase64url(signed.signature, 64), transferBytes(OWNER_TRANSFER_OFFER_DOMAIN, signed.offer), decodeBase64url(source.identity_signing_public_key, 32))) {
    throw new TypeError('transfer offer signature or source identity does not match')
  }
}

export async function acceptOwnerSubjectTransferOffer(input: {
  signedOffer: SignedOwnerSubjectTransferOffer
  source: VerifiedTransferIdentity
  recipient: VerifiedTransferIdentity
  signer: TransferRecoverySigner
  now?: Date
}): Promise<AcceptedOwnerSubjectTransfer> {
  const now = input.now ?? new Date()
  await verifySignedOwnerSubjectTransferOffer(input.signedOffer, input.source, now)
  await verifiedTransferIdentity(input.recipient, now)
  const { offer } = input.signedOffer
  if (offer.to.id !== input.recipient.identity_id || offer.recipient_identity_head_hash !== input.recipient.head_hash ||
    input.signer.identityId !== input.recipient.identity_id || input.signer.signingSeed.length !== 32 ||
    !sameBytes(ed25519.getPublicKey(input.signer.signingSeed), input.signer.signingPublicKey) ||
    !sameBytes(decodeBase64url(input.recipient.identity_signing_public_key, 32), input.signer.signingPublicKey)) {
    throw new TypeError('transfer offer is not bound to the current recipient identity')
  }
  const acceptedCanonical = transferBytes(OWNER_TRANSFER_ACCEPT_DOMAIN, offer)
  const offerSignature = decodeBase64url(input.signedOffer.signature, 64)
  return {
    offer,
    current_controller_proof: base64url(offerSignature),
    recipient_proof: base64url(signRecoveryMessage(acceptedCanonical, input.signer.signingSeed)),
  }
}

function sameBytes(left: Uint8Array, right: Uint8Array): boolean {
  if (left.length !== right.length) return false
  let difference = 0
  for (let index = 0; index < left.length; index += 1) difference |= left[index] ^ right[index]
  return difference === 0
}

export async function verifyAcceptedOwnerSubjectTransfer(input: {
  accepted: AcceptedOwnerSubjectTransfer
  source: VerifiedTransferIdentity
  recipient: VerifiedTransferIdentity
  now?: Date
}): Promise<void> {
  const now = input.now ?? new Date()
  const { accepted } = input
  const signedOffer = { offer: accepted.offer, signature: accepted.current_controller_proof }
  await verifySignedOwnerSubjectTransferOffer(signedOffer, input.source, now)
  await verifiedTransferIdentity(input.recipient, now)
  if (accepted.offer.to.id !== input.recipient.identity_id || accepted.offer.recipient_identity_head_hash !== input.recipient.head_hash ||
    !ed25519.verify(decodeBase64url(accepted.recipient_proof, 64), transferBytes(OWNER_TRANSFER_ACCEPT_DOMAIN, accepted.offer), decodeBase64url(input.recipient.identity_signing_public_key, 32))) {
    throw new TypeError('recipient transfer consent is invalid or stale')
  }
}

/**
 * Sign one bounded capability grant with the recovery-derived identity key.
 * The private seed remains in caller memory and is never sent to the server.
 * The server must independently verify the chain against current identity
 * history and must not trust this function or its returned principal fields.
 */
export function signCapabilityGrant(grant: CapabilityGrant, signingSeed: Uint8Array): SignedCapabilityGrant {
  if (!(signingSeed instanceof Uint8Array) || signingSeed.length !== 32) {
    throw new TypeError('identity signing seed is invalid')
  }
  const canonical = new TextEncoder().encode(canonicalIdentityJSON(grant))
  const signature = signRecoveryMessage(concat(SIGN_DOMAIN, canonical), signingSeed)
  return { grant, signature: base64url(signature) }
}

/** Sign one durable OwnerSubject event while the recovery key remains in browser memory. */
export function signOwnerAuthorityEvent(event: OwnerAuthorityEvent, signingSeed: Uint8Array): SignedOwnerAuthorityEvent {
  if (!(signingSeed instanceof Uint8Array) || signingSeed.length !== 32) {
    throw new TypeError('identity signing seed is invalid')
  }
  const canonical = new TextEncoder().encode(canonicalIdentityJSON(event))
  const signature = signRecoveryMessage(concat(OWNER_AUTHORITY_EVENT_DOMAIN, canonical), signingSeed)
  return { event, signature: base64url(signature) }
}

/** Validate the bounded owner-signed M7 policy before it is serialized. */
export function validOwnerSubjectReplicationPolicy(policy: OwnerSubjectReplicationPolicy): boolean {
  const canonical = (value: string, max: bigint) => /^(0|[1-9][0-9]{0,19})$/u.test(value) && BigInt(value) <= max
  return Boolean(policy) && canonical(policy.minimum_replicas, 16n) &&
    canonical(policy.grace_period_seconds, 2_592_000n) &&
    canonical(policy.capacity_limit_bytes, 1_125_899_906_842_624n) &&
    typeof policy.relocate_after_grace === 'boolean'
}

/**
 * Sign the recipient's OwnerSubject rekey-complete acknowledgement only when
 * the locally recovered identity matches the fresh current identity, the
 * durable subject is controlled by that identity, and the evidence covers the
 * exact sorted pending archive set. Callers must obtain each evidence value
 * from verifyRecipientRekeyHistory before invoking this function.
 */
export async function signArchiveRekeyCompleteEvent(input: {
  identity: VerifiedGlobalIdentityRecipient
  subject: OwnerAuthoritySubject
  signer: RecoveryKeyMaterial
  evidence: RecipientRekeyEvidence[]
  now?: Date
}): Promise<SignedOwnerAuthorityEvent> {
  const now = input.now ?? new Date()
  const { identity, subject, signer } = input
  await verifiedTransferIdentity(identity, now)
  if (!Array.isArray(input.evidence)) throw new TypeError('recipient rekey evidence is invalid')
  const pending = [...(subject.rekey_required_archives ?? [])].sort()
  const claimed = [...subject.archive_ids].sort()
  const sortedEvidence = [...input.evidence].sort((left, right) => left.archive_id < right.archive_id ? -1 : left.archive_id > right.archive_id ? 1 : 0)
  if (!identity || identity.frozen || subject.frozen || subject.fork_count !== 0 || !subject.rekey_required_since_event_hash ||
      !/^sha256:[a-f0-9]{64}$/u.test(subject.rekey_required_since_event_hash) ||
      !/^sha256:[a-f0-9]{64}$/u.test(identity.head_hash) || !/^(0|[1-9][0-9]{0,19})$/u.test(subject.sequence) ||
      !/^sha256:[a-f0-9]{64}$/u.test(subject.head_hash) ||
      pending.some((archiveID, index) => !/^arc_[A-Za-z0-9_-]{22}$/u.test(archiveID) || (index > 0 && archiveID === pending[index - 1])) ||
      pending.length !== claimed.length || pending.some((archiveID, index) => archiveID !== claimed[index]) ||
      subject.subject.controller.kind !== 'identity' || subject.subject.controller.id !== identity.identity_id ||
      signer.identityId !== identity.identity_id || signer.signingSeed.length !== 32 || signer.encryptionPrivateKey.length !== 32 ||
      signer.signerKeyId !== `ik_${base64url(sha256(concat(new TextEncoder().encode('IR/identity-key/v1\0'), signer.signingPublicKey)))}` ||
      !sameBytes(ed25519.getPublicKey(signer.signingSeed), signer.signingPublicKey) ||
      !sameBytes(decodeBase64url(identity.identity_signing_public_key, 32), signer.signingPublicKey) ||
      !sameBytes(decodeBase64url(identity.recovery_encryption_public_key, 32), signer.encryptionPublicKey) ||
      input.evidence.length !== pending.length || pending.length === 0) {
    throw new TypeError('recipient rekey completion context is invalid')
  }
  const expectedKeyID = recipientKeyId(signer.encryptionPublicKey)
  const archives = sortedEvidence
  for (let index = 0; index < pending.length; index += 1) {
    const evidence = archives[index]
    if (!evidence || evidence.archive_id !== pending[index] || !/^arc_[A-Za-z0-9_-]{22}$/u.test(evidence.archive_id) ||
        !/^[1-9][0-9]{0,19}$/u.test(evidence.previous_key_epoch) || !/^[1-9][0-9]{0,19}$/u.test(evidence.new_key_epoch) ||
        BigInt(evidence.new_key_epoch) <= BigInt(evidence.previous_key_epoch) || !/^[1-9][0-9]{0,19}$/u.test(evidence.snapshot_count) ||
        !/^snap_[a-f0-9]{64}$/u.test(evidence.head_snapshot_id) || !/^sha256:[a-f0-9]{64}$/u.test(evidence.bundle_set_commitment) ||
        !/^sha256:[a-f0-9]{64}$/u.test(evidence.envelope_sha256) || evidence.recipient_key_id !== expectedKeyID) {
      throw new TypeError('recipient rekey evidence does not match the pending archive set')
    }
  }
  const event: OwnerAuthorityEvent = {
    event_version: 'ir.owner-authority.event/1',
    identity_id: identity.identity_id,
    identity_head_hash: identity.head_hash,
    subject_id: subject.subject.subject_id,
    sequence: (BigInt(subject.sequence) + 1n).toString(),
    previous_event_hash: subject.head_hash,
    operation: 'archive_rekey_complete',
    issued_at: formatAuthorityTimestamp(now),
    signer: { kind: 'identity', id: identity.identity_id },
    signer_key_id: signer.signerKeyId,
    payload: { transfer_event_hash: subject.rekey_required_since_event_hash, archives },
  }
  return signOwnerAuthorityEvent(event, signer.signingSeed)
}

/** Hash a grant body using the exact domain and canonical profile used by Go. */
export async function capabilityGrantHash(grant: CapabilityGrant): Promise<string> {
  const canonical = new TextEncoder().encode(canonicalIdentityJSON(grant))
  const digest = new Uint8Array(await globalThis.crypto.subtle.digest('SHA-256', concat(HASH_DOMAIN, canonical)))
  return `sha256:${Array.from(digest, (value) => value.toString(16).padStart(2, '0')).join('')}`
}

/** Generate a one-use 256-bit grant nonce without reusing recovery entropy. */
export function createCapabilityNonce(): string {
  const bytes = globalThis.crypto.getRandomValues(new Uint8Array(32))
  return base64url(bytes)
}
