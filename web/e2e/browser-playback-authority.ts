import { authAPI, globalIdentityAPI } from '../src/api/index'
import {
  capabilityGrantHash,
  createCapabilityNonce,
  formatAuthorityTimestamp,
  signOwnerAuthorityEvent,
  signCapabilityGrant,
} from '../src/identity/authority'
import { deriveRecoveryKeys, recoveryEntropyFromMnemonic } from '../src/identity/recovery'
import { createSessionDEKKeyPair, openSessionDEKEnvelope } from '../src/identity/cryptoarchive'
import { base64UrlBytes, serializeAssertionCredential, webAuthnRequestOptions } from '../src/identity/webauthn'

function base64url(bytes: Uint8Array): string {
  return btoa(String.fromCharCode(...bytes)).replaceAll('+', '-').replaceAll('/', '_').replace(/=+$/u, '')
}

/** Test-only browser bundle that uses the application's real signing code. */
export async function createPlaybackAuthority(phrase: string, audienceNodeID: string, peerNodeID: string) {
  // This separately bundled harness has its own API client module instance.
  // Prime its CSRF state through the real authenticated session endpoint.
  const localSession = await authAPI.session()
  if (!localSession.authenticated) throw new Error('local Owner session is unavailable')
  const entropy = recoveryEntropyFromMnemonic(phrase)
  const keys = await deriveRecoveryKeys(entropy)
  entropy.fill(0)
  try {
    const status = await globalIdentityAPI.status()
    const identity = status.identity
    if (!identity || identity.frozen) throw new Error('current identity is unavailable')
    const nodeStatus = await globalIdentityAPI.nodeStatus()
    const nodeID = nodeStatus.node?.node_id
    if (nodeID !== audienceNodeID || !identity.node_ids.includes(nodeID)) {
      throw new Error('local Node is not delegated by the current identity')
    }
    const listing = await globalIdentityAPI.ownerAuthoritySubjects()
    const subject = listing.subjects.find(item =>
      item.subject.controller.kind === 'identity' && item.subject.controller.id === identity.identity_id,
    )
    if (!subject) throw new Error('OwnerSubject fixture was not created')

    const archiveID = `arc_${base64url(crypto.getRandomValues(new Uint8Array(16)))}`
    const now = new Date()
    const issuedAt = formatAuthorityTimestamp(now)
    const claim = signOwnerAuthorityEvent({
      event_version: 'ir.owner-authority.event/1', identity_id: identity.identity_id,
      identity_head_hash: identity.head_hash, subject_id: subject.subject.subject_id,
      sequence: (BigInt(subject.sequence) + 1n).toString(), previous_event_hash: subject.head_hash,
      operation: 'archive_claim', issued_at: issuedAt,
      signer: { kind: 'identity', id: identity.identity_id }, signer_key_id: keys.signerKeyId,
      payload: { archive_id: archiveID },
    }, keys.signingSeed)
    await globalIdentityAPI.applyOwnerAuthorityEvent(claim)

    const claimed = await globalIdentityAPI.ownerAuthoritySubject(subject.subject.subject_id)
    const grant = {
      grant_version: 'ir.capability.grant/1' as const,
      subject_id: subject.subject.subject_id,
      issuer: { kind: 'identity' as const, id: identity.identity_id },
      audience: { kind: 'node' as const, id: nodeID },
      archive_id: archiveID,
      actions: ['archive.view'],
      not_before: issuedAt,
      expires_at: formatAuthorityTimestamp(new Date(now.getTime() + 60 * 60 * 1000)),
      delegation_depth: '0',
      nonce: createCapabilityNonce(),
      policy_epoch: claimed.subject.policy_epoch,
      issuer_key_id: keys.signerKeyId,
    }
    const signedGrant = signCapabilityGrant(grant, keys.signingSeed)
    const grantHash = await capabilityGrantHash(grant)
    const issue = signOwnerAuthorityEvent({
      event_version: 'ir.owner-authority.event/1', identity_id: identity.identity_id,
      identity_head_hash: identity.head_hash, subject_id: claimed.subject.subject_id,
      sequence: (BigInt(claimed.sequence) + 1n).toString(), previous_event_hash: claimed.head_hash,
      operation: 'grant_issue', issued_at: issuedAt,
      signer: { kind: 'identity', id: identity.identity_id }, signer_key_id: keys.signerKeyId,
      payload: { chain: [signedGrant] },
    }, keys.signingSeed)
    await globalIdentityAPI.applyOwnerAuthorityEvent(issue)
    return { identityID: identity.identity_id, ownerSubjectID: subject.subject.subject_id, archiveID, grantHash, nodeID: peerNodeID }
  } finally {
    keys.destroy()
  }
}

export async function openPlaybackEnvelope(
  authority: { identityID: string; ownerSubjectID: string; archiveID: string; grantHash: string; nodeID: string },
  snapshotID: string,
) {
  const keyPair = createSessionDEKKeyPair()
  try {
    const options = await globalIdentityAPI.loginOptions(base64UrlBytes(keyPair.publicKey)) as never
    const credential = await navigator.credentials.get({ publicKey: webAuthnRequestOptions(options) })
    if (!credential) throw new Error('Passkey assertion was cancelled')
    await globalIdentityAPI.loginFinish(serializeAssertionCredential(credential))
    const session = await globalIdentityAPI.session()
    if (!session.authenticated || !session.playback_key_bound || !session.global_csrf_token || session.identity_id !== authority.identityID) {
      throw new Error('Passkey playback session is not bound to the current identity')
    }
    const epochs = await globalIdentityAPI.sessionKeyEpochs(authority.nodeID, authority.ownerSubjectID, authority.archiveID, authority.grantHash, snapshotID)
    const epoch = epochs.key_epochs[0]
    if (epochs.node_id !== authority.nodeID || epochs.archive_id !== authority.archiveID || epochs.snapshot_id !== snapshotID || epoch !== '1') {
      throw new Error('peer key epochs do not match the authorized snapshot')
    }
    const envelope = await globalIdentityAPI.sessionDEKEnvelope(authority.nodeID, authority.ownerSubjectID, authority.archiveID, authority.grantHash, snapshotID, epoch, session.global_csrf_token)
    const dek = await openSessionDEKEnvelope(envelope, keyPair.privateKey, {
      archiveId: authority.archiveID, snapshotId: snapshotID, keyEpoch: epoch,
      ownerSubjectId: authority.ownerSubjectID, grantHash: authority.grantHash, supplierNodeId: authority.nodeID,
    })
    const opened = dek.length === 32 && dek.every(byte => byte === 0x39)
    dek.fill(0)
    const afterConsume = await globalIdentityAPI.session()
    return { opened, playbackKeyBoundAfterConsume: afterConsume.playback_key_bound, identityID: session.identity_id }
  } finally {
    keyPair.destroy()
  }
}
