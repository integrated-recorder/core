import { useEffect, useRef, useState } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { Fingerprint, KeyRound, Download, LogOut, UserPlus } from 'lucide-react'
import { globalIdentityAPI, peerArchivesAPI, peerSessionsAPI, recordingsAPI, replicaImportsAPI, replicaTombstonesAPI } from '@/api'
import { accountAuthorizationQuery, globalIdentityForkChoicesQuery, globalIdentityQuery, globalIdentityRegistryQuery, globalIdentitySessionQuery, nodeIdentityQuery, ownerAuthoritySubjectsQuery, peerSessionsQuery, qk, replicaLeasesQuery } from '@/api/queries'
import { useI18n } from '@/i18n/provider'
import type { TranslationKey } from '@/i18n/catalog'
import { canonicalIdentityJSON, createRecoveryMaterial, decodeRecoveryFile, deriveRecoveryKeys, recoveryEntropyFromMnemonic, signIdentityGenesis, signIdentitySuccessor, type RecoveryKeyMaterial, type RecoveryMaterial } from '@/identity/recovery'
import { base64UrlBytes, webAuthnChallenge, webAuthnCreationOptions, webAuthnRequestOptions, serializeAssertionCredential, serializeRegistrationCredential } from '@/identity/webauthn'
import type { GlobalIdentityForkChoice, GlobalIdentityHistoryBundle, NodeEnrollmentProof, OwnerAuthoritySubject, OwnerSubjectReplicationPolicy, ReplicaInboxOffer, ReplicaLeaseSummary } from '@/types/api'
import { errorMessage } from '@/lib/errors'
import { acceptOwnerSubjectTransferOffer, createCapabilityNonce, createSignedOwnerSubjectTransferOffer, formatAuthorityTimestamp, signArchiveRekeyCompleteEvent, signCapabilityGrant, signOwnerAuthorityEvent, validOwnerSubjectReplicationPolicy, verifyAcceptedOwnerSubjectTransfer, type AcceptedOwnerSubjectTransfer, type CapabilityGrant, type SignedOwnerSubjectTransferOffer } from '@/identity/authority'
import { wrapArchiveDEK, type KeyEnvelope } from '@/identity/cryptoarchive'
import { verifyRecipientRekeyHistory, type RecipientRekeyEvidence, type RekeyHistoryVerification } from '@/identity/rekey-verification'
import { matchesReplicaRetryScope, resumableReplicaRetryIntents, storedKeyReplicaRetryRequest, type ReplicaRetryIntent } from '@/identity/replica-retry'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { EnrollmentProofQRCode, EnrollmentProofQRScanner } from '@/components/settings/node-enrollment-qr'

function isSecurePasskeyContext(): boolean {
  return typeof window !== 'undefined' && window.isSecureContext && 'credentials' in navigator
}

function bytesToBase64URL(bytes: Uint8Array): string {
  return base64UrlBytes(bytes)
}

function decodeIdentityPublicKey(value: string): Uint8Array {
  if (!/^[A-Za-z0-9_-]{43}$/u.test(value)) throw new TypeError('identity recovery public key is invalid')
  const base64 = value.replaceAll('-', '+').replaceAll('_', '/') + '='
  const binary = atob(base64)
  const bytes = Uint8Array.from(binary, character => character.charCodeAt(0))
  if (bytes.length !== 32 || bytesToBase64URL(bytes) !== value) throw new TypeError('identity recovery public key is invalid')
  return bytes
}

let lastReplicaKeyEpoch = 0
function createReplicaKeyEpoch(): number {
  lastReplicaKeyEpoch = Math.max(Date.now(), lastReplicaKeyEpoch + 1)
  return lastReplicaKeyEpoch
}

function createReplicaLeaseID(): string {
  return `rl_${bytesToBase64URL(globalThis.crypto.getRandomValues(new Uint8Array(16)))}`
}

function bytesToBase64(bytes: Uint8Array): string {
  let binary = ''
  for (let offset = 0; offset < bytes.length; offset += 0x8000) {
    binary += String.fromCharCode(...bytes.subarray(offset, Math.min(offset + 0x8000, bytes.length)))
  }
  return btoa(binary)
}

function readFileText(file: File): Promise<string> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader()
    reader.onload = () => typeof reader.result === 'string' ? resolve(reader.result) : reject(new TypeError('file text is unavailable'))
    reader.onerror = () => reject(reader.error ?? new TypeError('file could not be read'))
    reader.readAsText(file)
  })
}

const MAX_NODE_ENROLLMENT_PROOF_BYTES = 16 * 1024
const MAX_REKEY_TRANSFER_FILE_BYTES = 128 * 1024
const rekeyPreparationFormat = 'ir.owner-subject-rekey-preparation/1'
const rekeyEvidenceFormat = 'ir.owner-subject-rekey-evidence/1'
type RekeyPreparationFile = {
  format: typeof rekeyPreparationFormat
  owner_subject_id: string
  archive_id: string
  source_node_id: string
  source_identity_id: string
  source_identity_head_hash: string
  recipient_identity_id: string
  recipient_node_id: string
  grant_hash: string
  view_grant_hash: string
  verification: RekeyHistoryVerification
  key_envelope: KeyEnvelope
}
type RekeyEvidenceFile = {
  format: typeof rekeyEvidenceFormat
  owner_subject_id: string
  source_identity_id: string
  recipient_identity_id: string
  archive_id: string
  evidence: RecipientRekeyEvidence
}

function parseJsonObjectFile(file: File): Promise<Record<string, unknown>> {
  if (file.size <= 0 || file.size > MAX_REKEY_TRANSFER_FILE_BYTES) throw new TypeError('owner transfer evidence file exceeds its size limit')
  return readFileText(file).then(text => {
    const value: unknown = JSON.parse(text)
    if (!value || typeof value !== 'object' || Array.isArray(value)) throw new TypeError('owner transfer evidence file is invalid')
    return value as Record<string, unknown>
  })
}

function hasExactFields(value: Record<string, unknown>, fields: readonly string[]): boolean {
  const keys = Object.keys(value).sort()
  const expected = [...fields].sort()
  return keys.length === expected.length && keys.every((key, index) => key === expected[index])
}

async function parseRekeyPreparationFile(file: File): Promise<RekeyPreparationFile> {
  const record = await parseJsonObjectFile(file)
  if (!hasExactFields(record, ['format', 'owner_subject_id', 'archive_id', 'source_node_id', 'source_identity_id', 'source_identity_head_hash', 'recipient_identity_id', 'recipient_node_id', 'grant_hash', 'view_grant_hash', 'verification', 'key_envelope']) ||
      record.format !== rekeyPreparationFormat || !record.verification || typeof record.verification !== 'object' || Array.isArray(record.verification) ||
      !record.key_envelope || typeof record.key_envelope !== 'object' || Array.isArray(record.key_envelope)) {
    throw new TypeError('owner transfer rekey preparation file is invalid')
  }
  const verification = record.verification as Record<string, unknown>
  if (!hasExactFields(verification, ['archive_id', 'previous_key_epoch', 'new_key_epoch', 'snapshot_count', 'head_sequence', 'head_snapshot_id', 'bundle_set_commitment', 'envelope_sha256', 'recipient_key_id'])) {
    throw new TypeError('owner transfer rekey verification is invalid')
  }
  return record as unknown as RekeyPreparationFile
}

async function parseRekeyEvidenceFile(file: File): Promise<RekeyEvidenceFile> {
  const record = await parseJsonObjectFile(file)
  if (!hasExactFields(record, ['format', 'owner_subject_id', 'source_identity_id', 'recipient_identity_id', 'archive_id', 'evidence']) ||
      record.format !== rekeyEvidenceFormat || !record.evidence || typeof record.evidence !== 'object' || Array.isArray(record.evidence)) {
    throw new TypeError('verified owner transfer evidence file is invalid')
  }
  const evidence = record.evidence as Record<string, unknown>
  if (!hasExactFields(evidence, ['archive_id', 'previous_key_epoch', 'new_key_epoch', 'snapshot_count', 'head_snapshot_id', 'bundle_set_commitment', 'envelope_sha256', 'recipient_key_id'])) {
    throw new TypeError('verified owner transfer evidence is invalid')
  }
  return record as unknown as RekeyEvidenceFile
}
const nodeEnrollmentProofFields = [
  'identity_id',
  'previous_event_hash',
  'node_id',
  'node_public_key',
  'enrollment_nonce',
  'node_proof',
  'expires_at',
] as const

function parseNodeEnrollmentProof(text: string): NodeEnrollmentProof {
  const value: unknown = JSON.parse(text)
  if (value === null || typeof value !== 'object' || Array.isArray(value)) throw new TypeError('node enrollment proof has an invalid shape')
  const record = value as Record<string, unknown>
  const keys = Object.keys(record)
  if (keys.length !== nodeEnrollmentProofFields.length || nodeEnrollmentProofFields.some(field => !keys.includes(field))) {
    throw new TypeError('node enrollment proof has an invalid shape')
  }
  for (const field of nodeEnrollmentProofFields) {
    if (typeof record[field] !== 'string' || record[field].length === 0) throw new TypeError('node enrollment proof has an invalid shape')
  }
  return record as unknown as NodeEnrollmentProof
}

export function GlobalIdentityPanel() {
  const { t } = useI18n()
  const client = useQueryClient()
  const authorization = useQuery(accountAuthorizationQuery)
  const mayManage = authorization.data?.permissions.includes('system.settings.manage') ?? false
  const mayReadRecordings = authorization.data?.permissions.includes('recording.read') ?? false
  const status = useQuery({ ...globalIdentityQuery, enabled: mayManage })
  const forkChoices = useQuery({ ...globalIdentityForkChoicesQuery, enabled: mayManage && Boolean(status.data?.identity?.frozen) })
  const registry = useQuery({ ...globalIdentityRegistryQuery, enabled: mayManage })
  const ownerAuthority = useQuery({ ...ownerAuthoritySubjectsQuery, enabled: mayManage && Boolean(status.data?.identity) })
  const session = useQuery({ ...globalIdentitySessionQuery, enabled: mayManage })
  const nodeIdentity = useQuery({ ...nodeIdentityQuery, enabled: mayManage })
  const replicaState = useQuery({ ...replicaLeasesQuery, enabled: mayManage && mayReadRecordings && Boolean(status.data?.identity) })
  const localNodeID = nodeIdentity.data?.node?.node_id
  const identityNodeIDs = status.data?.identity?.node_ids ?? []
  const peerTargets = identityNodeIDs.filter(nodeID => nodeID !== localNodeID)
  const ownNodeDelegated = Boolean(localNodeID && identityNodeIDs.includes(localNodeID))
  const peerSessions = useQuery({ ...peerSessionsQuery, enabled: mayManage && Boolean(status.data?.identity && ownNodeDelegated) })
  const [recoveryMaterial, setRecoveryMaterial] = useState<RecoveryMaterial | null>(null)
  const [recoveryPhrase, setRecoveryPhrase] = useState('')
  const [recoveryFile, setRecoveryFile] = useState<File | null>(null)
  const [enrollmentIdentityID, setEnrollmentIdentityID] = useState('')
  const [enrollmentHead, setEnrollmentHead] = useState('')
  const [enrollmentProof, setEnrollmentProof] = useState<NodeEnrollmentProof | null>(null)
  const [enrollmentProofText, setEnrollmentProofText] = useState('')
  const [historyFile, setHistoryFile] = useState<File | null>(null)
  const [forkSelections, setForkSelections] = useState<Record<string, string>>({})
  const [registryHistoryFile, setRegistryHistoryFile] = useState<File | null>(null)
  const [confirmedSaved, setConfirmedSaved] = useState(false)
  const [authoritySubjectID, setAuthoritySubjectID] = useState('')
  const [replicationPolicyDrafts, setReplicationPolicyDrafts] = useState<Record<string, OwnerSubjectReplicationPolicy>>({})
  const [externalPeerIdentityID, setExternalPeerIdentityID] = useState('')
  const [externalPeerNodeID, setExternalPeerNodeID] = useState('')
  const [authoritySyncSubjectID, setAuthoritySyncSubjectID] = useState('')
  const [transferRecipientID, setTransferRecipientID] = useState('')
  const [transferOfferFile, setTransferOfferFile] = useState<File | null>(null)
  const [transferAcceptanceFile, setTransferAcceptanceFile] = useState<File | null>(null)
  const [rekeyRecordingID, setRekeyRecordingID] = useState('')
  const [rekeySnapshot, setRekeySnapshot] = useState<Awaited<ReturnType<typeof recordingsAPI.archiveSnapshotIdentity>> | null>(null)
  const [rekeyGrantHash, setRekeyGrantHash] = useState('')
  const [rekeyRecipientIdentityID, setRekeyRecipientIdentityID] = useState('')
  const [rekeyRecipientNodeID, setRekeyRecipientNodeID] = useState('')
  const [rekeyViewGrantHash, setRekeyViewGrantHash] = useState('')
  const [rekeyPreviousEpoch, setRekeyPreviousEpoch] = useState('')
  const [rekeyPreparationFile, setRekeyPreparationFile] = useState<File | null>(null)
  const [rekeyEvidenceFiles, setRekeyEvidenceFiles] = useState<File[]>([])
  const [authorityAudienceKind, setAuthorityAudienceKind] = useState<'identity' | 'node'>('node')
  const [authorityAudienceID, setAuthorityAudienceID] = useState('')
  const [authorityArchiveID, setAuthorityArchiveID] = useState('')
  const [resolvedAuthorityArchiveID, setResolvedAuthorityArchiveID] = useState('')
  const [authoritySnapshotID, setAuthoritySnapshotID] = useState('')
  const [authorityRecordingID, setAuthorityRecordingID] = useState('')
  const [authorityActions, setAuthorityActions] = useState<string[]>(['archive.view'])
  const [authorityLifetimeDays, setAuthorityLifetimeDays] = useState('30')
  const [authorityDelegationDepth, setAuthorityDelegationDepth] = useState('0')
  const [replicaRecordingID, setReplicaRecordingID] = useState('')
  const [replicaSnapshot, setReplicaSnapshot] = useState<Awaited<ReturnType<typeof recordingsAPI.archiveSnapshotIdentity>> | null>(null)
  const [replicaSubjectID, setReplicaSubjectID] = useState('')
  const [replicaGrantHash, setReplicaGrantHash] = useState('')
  const [replicaManifest, setReplicaManifest] = useState<import('@/types/api').ReplicaManifest | null>(null)
  const [replicaProviderNodeID, setReplicaProviderNodeID] = useState('')
  const [replicaLease, setReplicaLease] = useState<import('@/types/api').ReplicaLeaseSummary | null>(null)
  const [replicaRetryIntent, setReplicaRetryIntent] = useState<ReplicaRetryIntent | null>(null)
  const [incomingReplicaSourceNodeID, setIncomingReplicaSourceNodeID] = useState('')
  const [incomingReplicaSubjectID, setIncomingReplicaSubjectID] = useState('')
  const [incomingReplicaArchiveID, setIncomingReplicaArchiveID] = useState('')
  const [incomingReplicaSnapshotID, setIncomingReplicaSnapshotID] = useState('')
  const [incomingReplicaLeaseID, setIncomingReplicaLeaseID] = useState('')
  const [incomingReplicaGrantHash, setIncomingReplicaGrantHash] = useState('')
  const [incomingReplicaOffers, setIncomingReplicaOffers] = useState<ReplicaInboxOffer[]>([])
  const [incomingReplicaOffersFetched, setIncomingReplicaOffersFetched] = useState(false)
  const [incomingReplicaResult, setIncomingReplicaResult] = useState<{ lease: ReplicaLeaseSummary; snapshot_id: string; objects: number } | null>(null)
  const [tombstoneRecordingID, setTombstoneRecordingID] = useState('')
  const [tombstoneArchiveID, setTombstoneArchiveID] = useState('')
  const [tombstoneSubjectID, setTombstoneSubjectID] = useState('')
  const [tombstoneGrantHash, setTombstoneGrantHash] = useState('')
  const [tombstoneResult, setTombstoneResult] = useState<import('@/types/api').ReplicaTombstoneResult | null>(null)
  const [pending, setPending] = useState(false)
  const [message, setMessage] = useState('')
  const [error, setError] = useState('')
  const mounted = useRef(true)
  const keysRef = useRef<RecoveryKeyMaterial | null>(null)

  useEffect(() => {
    mounted.current = true
    return () => {
      mounted.current = false
      keysRef.current?.destroy()
      keysRef.current = null
      recoveryMaterial?.entropy.fill(0)
      recoveryMaterial?.file.fill(0)
    }
  }, [recoveryMaterial])

  if (!mayManage) return null

  const clearRecovery = () => {
    keysRef.current?.destroy()
    keysRef.current = null
    setRecoveryPhrase('')
    setRecoveryFile(null)
  }

  const refresh = async () => {
    await Promise.all([
      client.invalidateQueries({ queryKey: qk.globalIdentity }),
      client.invalidateQueries({ queryKey: qk.globalIdentityForks }),
      client.invalidateQueries({ queryKey: qk.globalIdentityRegistry }),
      client.invalidateQueries({ queryKey: qk.globalIdentitySession }),
      client.invalidateQueries({ queryKey: qk.nodeIdentity }),
      client.invalidateQueries({ queryKey: qk.ownerAuthoritySubjects }),
      client.invalidateQueries({ queryKey: qk.peerSessions }),
    ])
  }

  const run = async (operation: () => Promise<void>) => {
    if (pending) return
    setPending(true)
    setError('')
    setMessage('')
    try { await operation() } catch (cause) {
      setError(errorMessage(cause))
      // An authority event may have committed locally while federated mirror
      // publication is pending. Refresh the durable local view so the next
      // action is signed against the current sequence/head.
      await client.invalidateQueries({ queryKey: qk.ownerAuthoritySubjects })
    } finally { if (mounted.current) setPending(false) }
  }

  const connectExternalPeer = () => run(async () => {
    const identityID = externalPeerIdentityID.trim()
    const nodeID = externalPeerNodeID.trim()
    if (!/^gi_[A-Za-z0-9_-]{22}$/u.test(identityID) || !/^nd_[A-Za-z0-9_-]{22}$/u.test(nodeID) || identityID === identity?.identity_id) {
      throw new Error(t('identity.peerCrossIdentityInvalid'))
    }
    await peerSessionsAPI.connect(identityID, nodeID)
    await client.invalidateQueries({ queryKey: qk.peerSessions })
    setExternalPeerIdentityID('')
    setExternalPeerNodeID('')
    setMessage(t('identity.peerConnected'))
  })

  const createMaterial = () => run(async () => {
    const material = await createRecoveryMaterial()
    setRecoveryMaterial(current => {
      current?.entropy.fill(0)
      current?.file.fill(0)
      return material
    })
    setConfirmedSaved(false)
  })

  const downloadRecoveryFile = () => {
    if (!recoveryMaterial) return
    const url = URL.createObjectURL(new Blob([recoveryMaterial.file], { type: 'application/octet-stream' }))
    const link = document.createElement('a')
    link.href = url
    link.download = 'integrated-recorder-recovery.irrc'
    link.click()
    URL.revokeObjectURL(url)
  }

  const registerInitialPasskey = () => run(async () => {
    if (!recoveryMaterial || !confirmedSaved) throw new Error(t('identity.saveRecoveryFirst'))
    const keys = await deriveRecoveryKeys(recoveryMaterial.entropy)
    keysRef.current = keys
    try {
      const options = await globalIdentityAPI.registrationOptions(keys.identityId)
      const credential = await navigator.credentials.create({ publicKey: webAuthnCreationOptions(options) })
      const serialized = serializeRegistrationCredential(credential)
      const signed = signIdentityGenesis(keys, webAuthnChallenge(options), String(serialized.rawId))
      await globalIdentityAPI.finishRegistration({
        credential: serialized,
        canonical_event: bytesToBase64URL(signed.canonicalEvent),
        signature: bytesToBase64URL(signed.signature),
      })
      setRecoveryMaterial(current => {
        current?.entropy.fill(0)
        current?.file.fill(0)
        return null
      })
      setConfirmedSaved(false)
      await refresh()
      setMessage(t('identity.created'))
    } finally {
      keys.destroy()
      if (keysRef.current === keys) keysRef.current = null
    }
  })

  const readRecoveryKeys = async (identityID: string): Promise<RecoveryKeyMaterial> => {
    let entropy: Uint8Array
    if (recoveryFile) entropy = await decodeRecoveryFile(new Uint8Array(await recoveryFile.arrayBuffer()))
    else entropy = recoveryEntropyFromMnemonic(recoveryPhrase)
    try {
      const keys = await deriveRecoveryKeys(entropy)
      if (keys.identityId !== identityID) {
        keys.destroy()
        throw new Error(t('identity.recoveryMismatch'))
      }
      return keys
    } finally { entropy.fill(0) }
  }

  const parseTransferOfferFile = async (file: File): Promise<SignedOwnerSubjectTransferOffer> => {
    if (file.size <= 0 || file.size > 64 * 1024) throw new Error(t('identity.transferFileInvalid'))
    try {
      const parsed: unknown = JSON.parse(await readFileText(file))
      if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) throw new TypeError('invalid')
      const record = parsed as Record<string, unknown>
      if (record.format !== 'ir.owner-subject-transfer-offer/1' || !record.signed_offer || typeof record.signed_offer !== 'object') throw new TypeError('invalid')
      return record.signed_offer as SignedOwnerSubjectTransferOffer
    } catch {
      throw new Error(t('identity.transferFileInvalid'))
    }
  }

  const parseTransferAcceptanceFile = async (file: File): Promise<AcceptedOwnerSubjectTransfer> => {
    if (file.size <= 0 || file.size > 64 * 1024) throw new Error(t('identity.transferFileInvalid'))
    try {
      const parsed: unknown = JSON.parse(await readFileText(file))
      if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) throw new TypeError('invalid')
      const record = parsed as Record<string, unknown>
      if (record.format !== 'ir.owner-subject-transfer-acceptance/1' || !record.accepted_transfer || typeof record.accepted_transfer !== 'object') throw new TypeError('invalid')
      return record.accepted_transfer as AcceptedOwnerSubjectTransfer
    } catch {
      throw new Error(t('identity.transferFileInvalid'))
    }
  }

  const createOwnerSubjectTransferOffer = (item: NonNullable<typeof ownerAuthority.data>['subjects'][number]) => run(async () => {
    const current = status.data?.identity
    const recipientID = transferRecipientID.trim()
    if (!current || current.frozen || item.frozen || item.subject.controller.kind !== 'identity' || item.subject.controller.id !== current.identity_id ||
      (item.rekey_required_archives?.length ?? 0) > 0 || !/^gi_[A-Za-z0-9_-]{22}$/u.test(recipientID) || recipientID === current.identity_id || !isSecurePasskeyContext()) {
      throw new Error(t('identity.transferUnavailable'))
    }
    const [source, recipient] = await Promise.all([
      globalIdentityAPI.verifiedRecipient(current.identity_id),
      globalIdentityAPI.verifiedRecipient(recipientID),
    ])
    if (source.head_hash !== current.head_hash || recipient.frozen || recipient.identity_id !== recipientID) throw new Error(t('identity.transferIdentityStale'))
    const keys = await readRecoveryKeys(current.identity_id)
    keysRef.current = keys
    try {
      const signedOffer = await createSignedOwnerSubjectTransferOffer({
        subjectID: item.subject.subject_id, fromIdentity: source, recipient,
        oldPolicyEpoch: item.subject.policy_epoch, signer: keys,
      })
      downloadJSON('integrated-recorder-owner-transfer-offer.json', { format: 'ir.owner-subject-transfer-offer/1', signed_offer: signedOffer })
      clearRecovery()
      setMessage(t('identity.transferOfferCreated'))
    } finally {
      keys.destroy()
      if (keysRef.current === keys) keysRef.current = null
    }
  })

  const resolveOwnerRekeyArchive = () => run(async () => {
    const recordingID = rekeyRecordingID.trim()
    if (!recordingID) throw new Error(t('identity.replicaRecordingRequired'))
    const snapshot = await recordingsAPI.archiveSnapshotIdentity(recordingID)
    setRekeySnapshot(snapshot)
    setRekeyGrantHash('')
    setRekeyViewGrantHash('')
    const existingEpochs = (replicaState.data?.leases ?? []).filter(item => item.archive_id === snapshot.archive_id)
      .map(item => Number(item.key_epoch)).filter(epoch => Number.isSafeInteger(epoch) && epoch > 0).sort((left, right) => right - left)
    if (existingEpochs[0] !== undefined) setRekeyPreviousEpoch(String(existingEpochs[0]))
    setMessage(t('identity.rekeyArchiveResolved', { archive: snapshot.archive_id, count: snapshot.object_count }))
  })

  const prepareOwnerRekeyHistory = () => run(async () => {
    const current = status.data?.identity
    const nodeID = localNodeID
    const subject = ownerAuthority.data?.subjects.find(item => item.subject.subject_id === authoritySubjectID) ?? controlledSubjects[0]
    const snapshot = rekeySnapshot
    const recipientID = rekeyRecipientIdentityID.trim()
    const recipientNodeID = rekeyRecipientNodeID.trim()
    const previousEpoch = Number(rekeyPreviousEpoch)
    if (!current || current.frozen || !nodeID || !ownNodeDelegated || !subject || subject.frozen || !snapshot ||
        !subject.archive_ids.includes(snapshot.archive_id) || subject.subject.controller.kind !== 'identity' || subject.subject.controller.id !== current.identity_id ||
        !/^gi_[A-Za-z0-9_-]{22}$/u.test(recipientID) || recipientID === current.identity_id ||
        !/^nd_[A-Za-z0-9_-]{22}$/u.test(recipientNodeID) || !Number.isSafeInteger(previousEpoch) || previousEpoch < 1 || previousEpoch >= Number.MAX_SAFE_INTEGER ||
        !isSecurePasskeyContext() || window.location.protocol !== 'https:') {
      throw new Error(t('identity.rekeyUnavailable'))
    }
    if (subject.rekey_required_archives?.includes(snapshot.archive_id)) throw new Error(t('identity.rekeyAlreadyPending'))
    const grant = subject.grants.find(item => item.grant_hash === rekeyGrantHash) ?? subject.grants.find(item =>
      item.archive_id === snapshot.archive_id && !item.snapshot_id && item.audience.kind === 'node' && item.audience.id === nodeID &&
      item.actions.includes('key.rotate') && item.actions.includes('archive.replicate') && !item.revoked && Date.parse(item.expires_at) > Date.now())
    const viewGrant = subject.grants.find(item => item.grant_hash === rekeyViewGrantHash) ?? subject.grants.find(item =>
      item.archive_id === snapshot.archive_id && !item.snapshot_id && item.audience.kind === 'node' && item.audience.id === recipientNodeID &&
      item.actions.includes('archive.view') && !item.revoked && Date.parse(item.expires_at) > Date.now())
    if (!grant || grant.archive_id !== snapshot.archive_id || grant.snapshot_id || grant.audience.kind !== 'node' || grant.audience.id !== nodeID ||
        !grant.actions.includes('key.rotate') || !grant.actions.includes('archive.replicate') || grant.revoked || Date.parse(grant.expires_at) <= Date.now() ||
        !viewGrant || viewGrant.archive_id !== snapshot.archive_id || viewGrant.snapshot_id || viewGrant.audience.kind !== 'node' ||
        viewGrant.audience.id !== recipientNodeID || !viewGrant.actions.includes('archive.view') || viewGrant.revoked || Date.parse(viewGrant.expires_at) <= Date.now()) {
      throw new Error(t('identity.rekeyGrantsRequired'))
    }
    const [sourceIdentity, recipient] = await Promise.all([
      globalIdentityAPI.verifiedRecipient(current.identity_id),
      globalIdentityAPI.verifiedRecipient(recipientID),
    ])
    if (sourceIdentity.head_hash !== current.head_hash || recipient.identity_id !== recipientID || recipient.frozen) throw new Error(t('identity.transferIdentityStale'))
    const keys = await readRecoveryKeys(current.identity_id)
    keysRef.current = keys
    const dek = globalThis.crypto.getRandomValues(new Uint8Array(32))
    const nextEpoch = Math.max(createReplicaKeyEpoch(), previousEpoch + 1)
    try {
      const envelope = await wrapArchiveDEK(dek, snapshot.archive_id, String(nextEpoch), decodeIdentityPublicKey(recipient.recovery_encryption_public_key), keys)
      const result = await recordingsAPI.rekeyArchiveHistory(rekeyRecordingID.trim(), {
        owner_subject_id: subject.subject.subject_id,
        grant_hash: grant.grant_hash,
        recipient_identity_id: recipientID,
        previous_key_epoch: previousEpoch,
        key_epoch: nextEpoch,
        data_key: bytesToBase64(dek),
        key_envelope: envelope,
      })
      const verification = result.verification
      if (verification.archive_id !== snapshot.archive_id || verification.previous_key_epoch !== String(previousEpoch) ||
          verification.new_key_epoch !== String(nextEpoch) || verification.head_snapshot_id !== snapshot.snapshot_id ||
          verification.recipient_key_id !== envelope.recipient_key_id) throw new TypeError('source rekey result does not match the selected archive')
      downloadJSON('integrated-recorder-owner-subject-rekey-preparation.json', {
        format: rekeyPreparationFormat,
        owner_subject_id: subject.subject.subject_id,
        archive_id: snapshot.archive_id,
        source_node_id: nodeID,
        source_identity_id: current.identity_id,
        source_identity_head_hash: current.head_hash,
        recipient_identity_id: recipientID,
        recipient_node_id: recipientNodeID,
        grant_hash: grant.grant_hash,
        view_grant_hash: viewGrant.grant_hash,
        verification,
        key_envelope: result.key_envelope,
      } satisfies RekeyPreparationFile)
      clearRecovery()
      setMessage(t('identity.rekeyPreparationReady'))
    } finally {
      dek.fill(0)
      keys.destroy()
      if (keysRef.current === keys) keysRef.current = null
    }
  })

  const verifyIncomingOwnerRekey = () => run(async () => {
    const current = status.data?.identity
    const localNodeID = nodeIdentity.data?.node?.node_id
    if (!current || current.frozen || !rekeyPreparationFile || !localNodeID || !isSecurePasskeyContext() || window.location.protocol !== 'https:') {
      throw new Error(t('identity.rekeyVerifyUnavailable'))
    }
    const preparation = await parseRekeyPreparationFile(rekeyPreparationFile)
    if (preparation.recipient_identity_id !== current.identity_id || preparation.recipient_node_id !== localNodeID ||
        !/^os_[A-Za-z0-9_-]{22}$/u.test(preparation.owner_subject_id) || preparation.verification.archive_id !== preparation.archive_id ||
        !/^nd_[A-Za-z0-9_-]{22}$/u.test(preparation.source_node_id) || !/^gi_[A-Za-z0-9_-]{22}$/u.test(preparation.source_identity_id) ||
        preparation.source_identity_id === current.identity_id || preparation.verification.new_key_epoch !== preparation.key_envelope.key_epoch) {
      throw new Error(t('identity.rekeyFileDoesNotMatch'))
    }
    const sessionToSource = peerSessions.data?.sessions.some(session => session.node_id === preparation.source_node_id && session.identity_id === preparation.source_identity_id)
    if (!sessionToSource) throw new Error(t('identity.rekeyPeerUnavailable'))
    const [source, recipientIdentity] = await Promise.all([
      globalIdentityAPI.verifiedRecipient(preparation.source_identity_id),
      globalIdentityAPI.verifiedRecipient(current.identity_id),
    ])
    if (source.head_hash !== preparation.source_identity_head_hash || recipientIdentity.head_hash !== current.head_hash) {
      throw new Error(t('identity.transferIdentityStale'))
    }
    const head = await peerArchivesAPI.head(preparation.source_node_id, preparation.owner_subject_id, preparation.archive_id, preparation.view_grant_hash)
    if (head.node_id !== preparation.source_node_id || head.snapshot.content.archive_id !== preparation.archive_id ||
        head.snapshot.snapshot_id !== preparation.verification.head_snapshot_id || head.snapshot.sequence !== Number(preparation.verification.head_sequence)) {
      throw new Error(t('identity.rekeyHeadMismatch'))
    }
    const headEnvelope = await peerArchivesAPI.keyEnvelope(preparation.source_node_id, preparation.owner_subject_id, preparation.archive_id,
      preparation.view_grant_hash, head.snapshot.snapshot_id, preparation.verification.new_key_epoch)
    if (canonicalIdentityJSON(headEnvelope) !== canonicalIdentityJSON(preparation.key_envelope)) throw new Error(t('identity.rekeyEnvelopeMismatch'))
    const keys = await readRecoveryKeys(current.identity_id)
    keysRef.current = keys
    try {
      setMessage(t('identity.rekeyVerificationRunning'))
      const evidence = await verifyRecipientRekeyHistory({
        archiveID: preparation.archive_id,
        previousEpoch: preparation.verification.previous_key_epoch,
        newEpoch: preparation.verification.new_key_epoch,
        envelope: preparation.key_envelope,
        sourceIdentity: source,
        recipientIdentity,
        recipientKeys: keys,
        expected: preparation.verification,
        now: new Date(),
        loadHistoryRecord: async (snapshotID, expectedSequence, index, signal) => {
          const snapshotResponse = index === 0
            ? head
            : await peerArchivesAPI.snapshot(preparation.source_node_id, preparation.owner_subject_id, preparation.archive_id, preparation.view_grant_hash, snapshotID)
          const snapshot = snapshotResponse.snapshot
          if (snapshotResponse.node_id !== preparation.source_node_id || snapshot.snapshot_id !== snapshotID || snapshot.sequence !== expectedSequence || snapshot.content.archive_id !== preparation.archive_id) {
            throw new TypeError('source archive snapshot does not match the committed predecessor chain')
          }
          const [previousManifest, newManifest, envelopeForSnapshot] = await Promise.all([
            peerArchivesAPI.manifest(preparation.source_node_id, preparation.owner_subject_id, preparation.archive_id, preparation.view_grant_hash, snapshotID, preparation.verification.previous_key_epoch),
            peerArchivesAPI.manifest(preparation.source_node_id, preparation.owner_subject_id, preparation.archive_id, preparation.view_grant_hash, snapshotID, preparation.verification.new_key_epoch),
            peerArchivesAPI.keyEnvelope(preparation.source_node_id, preparation.owner_subject_id, preparation.archive_id, preparation.view_grant_hash, snapshotID, preparation.verification.new_key_epoch),
          ])
          if (signal.aborted) throw signal.reason ?? new DOMException('rekey verification aborted', 'AbortError')
          return { snapshot, previousManifest, newManifest, envelope: envelopeForSnapshot }
        },
        fetchCiphertextRange: (snapshotID, keyEpoch, objectID, offset, length, signal) => peerArchivesAPI.ciphertextRange(
          preparation.source_node_id, preparation.owner_subject_id, preparation.archive_id, preparation.view_grant_hash,
          snapshotID, keyEpoch, objectID, offset, length, signal,
        ),
      })
      downloadJSON('integrated-recorder-owner-subject-rekey-evidence.json', {
        format: rekeyEvidenceFormat,
        owner_subject_id: preparation.owner_subject_id,
        source_identity_id: preparation.source_identity_id,
        recipient_identity_id: current.identity_id,
        archive_id: preparation.archive_id,
        evidence,
      } satisfies RekeyEvidenceFile)
      setMessage(t('identity.rekeyEvidenceVerified'))
    } finally {
      keys.destroy()
      if (keysRef.current === keys) keysRef.current = null
    }
  })

  const completeOwnerSubjectRekey = (subject: OwnerAuthoritySubject) => run(async () => {
    const current = status.data?.identity
    if (!current || current.frozen || subject.frozen || !rekeyEvidenceFiles.length || !isSecurePasskeyContext()) throw new Error(t('identity.rekeyCompleteUnavailable'))
    const files = rekeyEvidenceFiles
    if (files.length > 4096 || files.reduce((sum, file) => sum + file.size, 0) > 8 * 1024 * 1024) throw new Error(t('identity.rekeyFilesTooLarge'))
    const parsed = await Promise.all(files.map(parseRekeyEvidenceFile))
    const evidence = parsed.map(file => {
      if (file.owner_subject_id !== subject.subject.subject_id || file.recipient_identity_id !== current.identity_id ||
          file.archive_id !== file.evidence.archive_id) throw new Error(t('identity.rekeyFileDoesNotMatch'))
      return file.evidence
    })
    if (new Set(evidence.map(item => item.archive_id)).size !== evidence.length) throw new Error(t('identity.rekeyFilesDuplicate'))
    const identity = await globalIdentityAPI.verifiedRecipient(current.identity_id)
    if (identity.head_hash !== current.head_hash) throw new Error(t('identity.transferIdentityStale'))
    const keys = await readRecoveryKeys(current.identity_id)
    keysRef.current = keys
    try {
      const signed = await signArchiveRekeyCompleteEvent({ identity, subject, signer: keys, evidence })
      await globalIdentityAPI.applyOwnerAuthorityEvent(signed)
      clearRecovery()
      setRekeyEvidenceFiles([])
      await client.invalidateQueries({ queryKey: qk.ownerAuthoritySubjects })
      setMessage(t('identity.rekeyCompleted'))
    } finally {
      keys.destroy()
      if (keysRef.current === keys) keysRef.current = null
    }
  })

  const acceptIncomingOwnerSubjectTransferOffer = () => run(async () => {
    const current = status.data?.identity
    if (!current || current.frozen || !transferOfferFile || !isSecurePasskeyContext()) throw new Error(t('identity.transferUnavailable'))
    const signedOffer = await parseTransferOfferFile(transferOfferFile)
    const [source, recipient] = await Promise.all([
      globalIdentityAPI.verifiedRecipient(signedOffer.offer?.from?.id ?? ''),
      globalIdentityAPI.verifiedRecipient(current.identity_id),
    ])
    if (recipient.head_hash !== current.head_hash) throw new Error(t('identity.transferIdentityStale'))
    const keys = await readRecoveryKeys(current.identity_id)
    keysRef.current = keys
    try {
      const accepted = await acceptOwnerSubjectTransferOffer({ signedOffer, source, recipient, signer: keys })
      downloadJSON('integrated-recorder-owner-transfer-acceptance.json', { format: 'ir.owner-subject-transfer-acceptance/1', accepted_transfer: accepted })
      clearRecovery()
      setTransferOfferFile(null)
      setMessage(t('identity.transferAccepted'))
    } finally {
      keys.destroy()
      if (keysRef.current === keys) keysRef.current = null
    }
  })

  const applyOwnerSubjectTransfer = (item: NonNullable<typeof ownerAuthority.data>['subjects'][number]) => run(async () => {
    const current = status.data?.identity
    if (!current || current.frozen || item.frozen || item.subject.controller.kind !== 'identity' || item.subject.controller.id !== current.identity_id ||
      (item.rekey_required_archives?.length ?? 0) > 0 || !transferAcceptanceFile || !isSecurePasskeyContext()) throw new Error(t('identity.transferUnavailable'))
    const accepted = await parseTransferAcceptanceFile(transferAcceptanceFile)
    if (accepted.offer?.subject_id !== item.subject.subject_id || accepted.offer.from?.id !== current.identity_id ||
      accepted.offer.old_policy_epoch !== item.subject.policy_epoch || accepted.offer.old_identity_head_hash !== current.head_hash) {
      throw new Error(t('identity.transferDoesNotMatch'))
    }
    const recipient = await globalIdentityAPI.verifiedRecipient(accepted.offer.to?.id ?? '')
    const source = await globalIdentityAPI.verifiedRecipient(current.identity_id)
    if (source.head_hash !== current.head_hash) throw new Error(t('identity.transferIdentityStale'))
    await verifyAcceptedOwnerSubjectTransfer({ accepted, source, recipient })
    const keys = await readRecoveryKeys(current.identity_id)
    keysRef.current = keys
    try {
      const issuedAt = formatAuthorityTimestamp(new Date())
      const signedEvent = signOwnerAuthorityEvent({
        event_version: 'ir.owner-authority.event/1', identity_id: current.identity_id,
        identity_head_hash: current.head_hash, subject_id: item.subject.subject_id,
        sequence: (BigInt(item.sequence) + 1n).toString(), previous_event_hash: item.head_hash,
        operation: 'subject_transfer', issued_at: issuedAt,
        signer: item.subject.controller, signer_key_id: keys.signerKeyId,
        payload: { accepted_transfer: accepted },
      }, keys.signingSeed)
      await globalIdentityAPI.applyOwnerAuthorityEvent(signedEvent)
      clearRecovery()
      setTransferAcceptanceFile(null)
      setTransferRecipientID('')
      await refresh()
      setMessage(t('identity.transferApplied'))
    } finally {
      keys.destroy()
      if (keysRef.current === keys) keysRef.current = null
    }
  })

  const addPasskey = () => run(async () => {
    const identity = status.data?.identity
    if (!identity) throw new Error(t('identity.notInitialized'))
    const keys = await readRecoveryKeys(identity.identity_id)
    keysRef.current = keys
    try {
      const options = await globalIdentityAPI.registrationOptions(identity.identity_id)
      const credential = await navigator.credentials.create({ publicKey: webAuthnCreationOptions(options) })
      const serialized = serializeRegistrationCredential(credential)
      const signed = signIdentitySuccessor(keys, 'credential_add', (BigInt(identity.sequence) + 1n).toString(), identity.head_hash, {
        credential_id: String(serialized.rawId),
        webauthn_challenge: webAuthnChallenge(options),
      })
      await globalIdentityAPI.finishRegistration({ credential: serialized, canonical_event: bytesToBase64URL(signed.canonicalEvent), signature: bytesToBase64URL(signed.signature) })
      clearRecovery()
      await refresh()
      setMessage(t('identity.passkeyAdded'))
    } finally { keys.destroy(); if (keysRef.current === keys) keysRef.current = null }
  })

  const revokePasskey = (credentialID: string) => run(async () => {
    const identity = status.data?.identity
    if (!identity) throw new Error(t('identity.notInitialized'))
    if (identity.credential_count === 1 && !window.confirm(t('identity.revokeLastConfirm'))) return
    const keys = await readRecoveryKeys(identity.identity_id)
    keysRef.current = keys
    try {
      const signed = signIdentitySuccessor(keys, 'credential_revoke', (BigInt(identity.sequence) + 1n).toString(), identity.head_hash, { credential_id: credentialID })
      await globalIdentityAPI.revokeCredential(credentialID, { canonical_event: bytesToBase64URL(signed.canonicalEvent), signature: bytesToBase64URL(signed.signature) })
      clearRecovery()
      await refresh()
      setMessage(t('identity.passkeyRevoked'))
    } finally { keys.destroy(); if (keysRef.current === keys) keysRef.current = null }
  })

  const authenticatePasskey = () => run(async () => {
    const options = await globalIdentityAPI.loginOptions()
    const credential = await navigator.credentials.get({ publicKey: webAuthnRequestOptions(options) })
    await globalIdentityAPI.loginFinish(serializeAssertionCredential(credential))
    await refresh()
    setMessage(t('identity.authenticated'))
  })

  const createOwnerSubject = () => run(async () => {
    const identity = status.data?.identity
    if (!identity || identity.frozen) throw new Error(t('identity.notAvailableForDelegation'))
    const keys = await readRecoveryKeys(identity.identity_id)
    keysRef.current = keys
    try {
      const now = new Date()
        const timestamp = formatAuthorityTimestamp(now)
      const rawID = globalThis.crypto.getRandomValues(new Uint8Array(16))
      const subjectID = `os_${bytesToBase64URL(rawID)}`
      const subject = {
        subject_id: subjectID,
        subject_type: 'person' as const,
        controller: { kind: 'identity' as const, id: identity.identity_id },
        controller_policy: 'global_identity_recovery_root/1',
        policy_epoch: '1',
        created_at: timestamp,
      }
      const signed = signOwnerAuthorityEvent({
        event_version: 'ir.owner-authority.event/1',
        identity_id: identity.identity_id,
        identity_head_hash: identity.head_hash,
        subject_id: subjectID,
        sequence: '1',
        operation: 'subject_create',
        issued_at: timestamp,
        signer: { kind: 'identity', id: identity.identity_id },
        signer_key_id: keys.signerKeyId,
        payload: { subject },
      }, keys.signingSeed)
      await globalIdentityAPI.applyOwnerAuthorityEvent(signed)
      clearRecovery()
      await client.invalidateQueries({ queryKey: qk.ownerAuthoritySubjects })
      setMessage(t('identity.ownerSubjectCreated'))
    } finally {
      keys.destroy()
      if (keysRef.current === keys) keysRef.current = null
    }
  })

  const saveReplicationPolicy = (item: OwnerAuthoritySubject) => run(async () => {
    const identity = status.data?.identity
    const policy = replicationPolicyDrafts[item.subject.subject_id] ?? item.replication_policy ?? {
      minimum_replicas: '0', grace_period_seconds: '0', capacity_limit_bytes: '0', relocate_after_grace: false,
    }
    if (!identity || identity.frozen || item.frozen || item.subject.controller.kind !== 'identity' || item.subject.controller.id !== identity.identity_id) {
      throw new Error(t('identity.notAvailableForDelegation'))
    }
    if (!validOwnerSubjectReplicationPolicy(policy)) throw new Error(t('identity.replicationPolicyInvalid'))
    const keys = await readRecoveryKeys(identity.identity_id)
    keysRef.current = keys
    try {
      const issuedAt = formatAuthorityTimestamp(new Date())
      const signedEvent = signOwnerAuthorityEvent({
        event_version: 'ir.owner-authority.event/1', identity_id: identity.identity_id,
        identity_head_hash: identity.head_hash, subject_id: item.subject.subject_id,
        sequence: (BigInt(item.sequence) + 1n).toString(), previous_event_hash: item.head_hash,
        operation: 'replication_policy_set', issued_at: issuedAt,
        signer: item.subject.controller, signer_key_id: keys.signerKeyId,
        payload: { policy },
      }, keys.signingSeed)
      await globalIdentityAPI.applyOwnerAuthorityEvent(signedEvent)
      clearRecovery()
      setReplicationPolicyDrafts(current => {
        const next = { ...current }
        delete next[item.subject.subject_id]
        return next
      })
      await client.invalidateQueries({ queryKey: qk.ownerAuthoritySubjects })
      setMessage(t('identity.replicationPolicySaved'))
    } finally {
      keys.destroy()
      if (keysRef.current === keys) keysRef.current = null
    }
  })

  const syncOwnerAuthorityHistory = () => run(async () => {
    const subjectID = authoritySyncSubjectID.trim()
    if (!/^os_[A-Za-z0-9_-]{22}$/u.test(subjectID)) throw new Error(t('identity.authoritySubjectIDInvalid'))
    await globalIdentityAPI.syncOwnerAuthorityHistory(subjectID)
    await client.invalidateQueries({ queryKey: qk.ownerAuthoritySubjects })
    setAuthoritySubjectID(subjectID)
    setAuthoritySyncSubjectID('')
    setMessage(t('identity.authorityHistorySynced'))
  })

  const issueCapability = (item: NonNullable<typeof ownerAuthority.data>['subjects'][number]) => run(async () => {
    const identity = status.data?.identity
    if (!identity || identity.frozen || item.frozen || item.subject.controller.kind !== 'identity' || item.subject.controller.id !== identity.identity_id) {
      throw new Error(t('identity.notAvailableForDelegation'))
    }
    const audienceID = authorityAudienceID.trim()
    const archiveID = authorityArchiveID.trim()
    const lifetime = Number(authorityLifetimeDays)
    const delegationDepth = Number(authorityDelegationDepth)
    if (!audienceID || !archiveID || authorityActions.length === 0 || !Number.isInteger(lifetime) || lifetime < 1 || lifetime > 366 || !Number.isInteger(delegationDepth) || delegationDepth < 0 || delegationDepth > 8) {
      throw new Error(t('identity.capabilityInvalid'))
    }
    if (authorityAudienceKind === 'identity' ? !/^gi_[A-Za-z0-9_-]{22}$/u.test(audienceID) : !/^nd_[A-Za-z0-9_-]{22}$/u.test(audienceID)) {
      throw new Error(t('identity.capabilityAudienceInvalid'))
    }
    if (!/^arc_[A-Za-z0-9_-]{22}$/u.test(archiveID) || (authoritySnapshotID.trim() && !/^snap_[a-f0-9]{64}$/u.test(authoritySnapshotID.trim()))) {
      throw new Error(t('identity.capabilityArchiveInvalid'))
    }
    if (!item.archive_ids.includes(archiveID)) throw new Error(t('identity.archiveClaimRequired'))
    if (item.rekey_required_archives?.includes(archiveID)) throw new Error(t('identity.rekeyRequired'))
    if (audienceID === identity.identity_id) throw new Error(t('identity.capabilityAudienceInvalid'))

    const keys = await readRecoveryKeys(identity.identity_id)
    keysRef.current = keys
    try {
      const now = new Date()
      const issuedAt = formatAuthorityTimestamp(now)
      const grant: CapabilityGrant = {
        grant_version: 'ir.capability.grant/1', subject_id: item.subject.subject_id,
        issuer: item.subject.controller, audience: { kind: authorityAudienceKind, id: audienceID },
        archive_id: archiveID, ...(authoritySnapshotID.trim() ? { snapshot_id: authoritySnapshotID.trim() } : {}),
        actions: [...authorityActions].sort(), not_before: issuedAt,
        expires_at: formatAuthorityTimestamp(new Date(now.getTime() + lifetime * 24 * 60 * 60 * 1000)),
        delegation_depth: String(delegationDepth), nonce: createCapabilityNonce(),
        policy_epoch: item.subject.policy_epoch, issuer_key_id: keys.signerKeyId,
      }
      const signedGrant = signCapabilityGrant(grant, keys.signingSeed)
      const signedEvent = signOwnerAuthorityEvent({
        event_version: 'ir.owner-authority.event/1', identity_id: identity.identity_id,
        identity_head_hash: identity.head_hash, subject_id: item.subject.subject_id,
        sequence: (BigInt(item.sequence) + 1n).toString(), previous_event_hash: item.head_hash,
        operation: 'grant_issue', issued_at: issuedAt,
        signer: item.subject.controller, signer_key_id: keys.signerKeyId,
        payload: { chain: [signedGrant] },
      }, keys.signingSeed)
      await globalIdentityAPI.applyOwnerAuthorityEvent(signedEvent)
      clearRecovery()
      await client.invalidateQueries({ queryKey: qk.ownerAuthoritySubjects })
      setMessage(t('identity.capabilityIssued'))
    } finally {
      keys.destroy()
      if (keysRef.current === keys) keysRef.current = null
    }
  })

  const claimArchive = (item: NonNullable<typeof ownerAuthority.data>['subjects'][number]) => run(async () => {
    const identity = status.data?.identity
    const archiveID = authorityArchiveID.trim()
    if (!identity || identity.frozen || item.frozen || item.subject.controller.kind !== 'identity' || item.subject.controller.id !== identity.identity_id) {
      throw new Error(t('identity.notAvailableForDelegation'))
    }
    if (!/^arc_[A-Za-z0-9_-]{22}$/u.test(archiveID) || resolvedAuthorityArchiveID !== archiveID) throw new Error(t('identity.capabilityArchiveInvalid'))
    if (item.archive_ids.includes(archiveID)) throw new Error(t('identity.archiveAlreadyClaimed'))

    const keys = await readRecoveryKeys(identity.identity_id)
    keysRef.current = keys
    try {
      const issuedAt = formatAuthorityTimestamp(new Date())
      const signedEvent = signOwnerAuthorityEvent({
        event_version: 'ir.owner-authority.event/1', identity_id: identity.identity_id,
        identity_head_hash: identity.head_hash, subject_id: item.subject.subject_id,
        sequence: (BigInt(item.sequence) + 1n).toString(), previous_event_hash: item.head_hash,
        operation: 'archive_claim', issued_at: issuedAt,
        signer: item.subject.controller, signer_key_id: keys.signerKeyId,
        payload: { archive_id: archiveID },
      }, keys.signingSeed)
      await globalIdentityAPI.applyOwnerAuthorityEvent(signedEvent)
      clearRecovery()
      await client.invalidateQueries({ queryKey: qk.ownerAuthoritySubjects })
      setMessage(t('identity.archiveClaimed'))
    } finally {
      keys.destroy()
      if (keysRef.current === keys) keysRef.current = null
    }
  })

  const resolveArchiveSnapshot = () => run(async () => {
    const recordingID = authorityRecordingID.trim()
    if (!recordingID) throw new Error(t('identity.capabilityRecordingRequired'))
    const resolved = await recordingsAPI.archiveSnapshotIdentity(recordingID)
    setAuthorityArchiveID(resolved.archive_id)
    setResolvedAuthorityArchiveID(resolved.archive_id)
    setAuthoritySnapshotID(resolved.snapshot_id)
    setMessage(t('identity.capabilitySnapshotResolved', { sequence: resolved.sequence }))
  })

  const resolveReplicaSnapshot = () => run(async () => {
    const recordingID = replicaRecordingID.trim()
    if (!recordingID) throw new Error(t('identity.replicaRecordingRequired'))
    const resolved = await recordingsAPI.archiveSnapshotIdentity(recordingID)
    setReplicaSnapshot(resolved)
    setReplicaManifest(null)
    setReplicaLease(null)
    setReplicaRetryIntent(null)
    setReplicaGrantHash('')
    setMessage(t('identity.replicaSnapshotResolved', { count: resolved.object_count }))
  })

  const prepareEncryptedReplica = () => run(async () => {
    const identity = status.data?.identity
    const nodeID = localNodeID
    const providerNodeID = replicaProviderNodeID
    const recordingID = replicaRecordingID.trim()
    const subject = controlledSubjects.find(item => item.subject.subject_id === replicaSubjectID) ?? controlledSubjects[0]
    const snapshot = replicaSnapshot
    if (!identity || identity.frozen || !nodeID || !ownNodeDelegated || !providerNodeID || !peerTargets.includes(providerNodeID) || !snapshot || !recordingID || !isSecurePasskeyContext() || window.location.protocol !== 'https:') {
      throw new Error(t('identity.replicaPrerequisites'))
    }
    const grantHash = replicaGrantHash || usableReplicaGrants[0]?.grant_hash
    const grant = subject?.grants.find(item => item.grant_hash === grantHash)
    if (!subject || subject.frozen || !grant || grant.revoked || Date.parse(grant.expires_at) <= Date.now() ||
      grant.archive_id !== snapshot.archive_id || (grant.snapshot_id && grant.snapshot_id !== snapshot.snapshot_id) ||
      grant.audience.kind !== 'node' || grant.audience.id !== nodeID || !grant.actions.includes('archive.replicate')) {
      throw new Error(t('identity.replicaGrantRequired'))
    }
    const retryScope = {
      identityID: identity.identity_id,
      nodeID,
      recordingID,
      archiveID: snapshot.archive_id,
      snapshotID: snapshot.snapshot_id,
      ownerSubjectID: subject.subject.subject_id,
      grantHash: grant.grant_hash,
      providerNodeID,
    }
    if (matchesReplicaRetryScope(replicaRetryIntent, retryScope)) {
      let result
      try {
        result = await recordingsAPI.createReplicaOffer(recordingID, storedKeyReplicaRetryRequest(replicaRetryIntent))
      } catch (cause) {
        await client.invalidateQueries({ queryKey: qk.replicaLeases })
        throw cause
      }
      setReplicaManifest(result.manifest ?? null)
      setReplicaLease(result.lease)
      clearRecovery()
      await client.invalidateQueries({ queryKey: qk.replicaLeases })
      setMessage(t(result.reused ? 'identity.replicaOfferReused' : 'identity.replicaOfferCreated', { id: result.lease.lease_id }))
      return
    }
    const keys = await readRecoveryKeys(identity.identity_id)
    keysRef.current = keys
    const dek = globalThis.crypto.getRandomValues(new Uint8Array(32))
    const leaseID = createReplicaLeaseID()
    try {
      const recipient = decodeIdentityPublicKey(identity.recovery_encryption_public_key)
      const keyEpoch = createReplicaKeyEpoch()
      const intent: ReplicaRetryIntent = { ...retryScope, leaseID, keyEpoch }
      setReplicaRetryIntent(intent)
      const envelope = await wrapArchiveDEK(dek, snapshot.archive_id, keyEpoch.toString(), recipient, keys)
      let result
      try {
        result = await recordingsAPI.createReplicaOffer(recordingID, {
          owner_subject_id: subject.subject.subject_id,
          grant_hash: grant.grant_hash,
          key_epoch: keyEpoch,
          data_key: bytesToBase64(dek),
          key_envelope: envelope,
          lease_id: leaseID,
          provider_node_id: providerNodeID,
        })
      } catch (cause) {
        // A failed response can follow a durable encryption/offer commit. Refresh
        // the projection before offering a new key/lease attempt to the user.
        await client.invalidateQueries({ queryKey: qk.replicaLeases })
        throw cause
      }
      setReplicaManifest(result.manifest ?? null)
      setReplicaLease(result.lease)
      clearRecovery()
      await client.invalidateQueries({ queryKey: qk.replicaLeases })
      setMessage(t(result.reused ? 'identity.replicaOfferReused' : 'identity.replicaOfferCreated', { id: result.lease.lease_id }))
    } finally {
      dek.fill(0)
      keys.destroy()
      if (keysRef.current === keys) keysRef.current = null
    }
  })

  const resetReplicaRetry = () => {
    setReplicaRetryIntent(null)
    setReplicaManifest(null)
    setReplicaLease(null)
    setMessage(t('identity.replicaRetryReset'))
  }

  const importEncryptedReplica = () => run(async () => {
    const sourceNodeID = incomingReplicaSourceNodeID.trim()
    const archiveID = incomingReplicaArchiveID.trim()
    const snapshotID = incomingReplicaSnapshotID.trim()
    const leaseID = incomingReplicaLeaseID.trim()
    const subject = controlledSubjects.find(item => item.subject.subject_id === incomingReplicaSubjectID) ?? controlledSubjects[0]
    const grant = subject?.grants.find(item => item.grant_hash === incomingReplicaGrantHash) ?? usableIncomingReplicaGrants[0]
    if (!identity || identity.frozen || !ownNodeDelegated || !localNodeID || !peerTargets.includes(sourceNodeID) || !archiveID || !snapshotID || !leaseID || !isSecurePasskeyContext() || window.location.protocol !== 'https:') {
      throw new Error(t('identity.replicaImportPrerequisites'))
    }
    if (!subject || subject.frozen || !grant || grant.revoked || Date.parse(grant.expires_at) <= Date.now() ||
      grant.archive_id !== archiveID || (grant.snapshot_id && grant.snapshot_id !== snapshotID) ||
      grant.audience.kind !== 'node' || grant.audience.id !== localNodeID || !grant.actions.includes('archive.replicate')) {
      throw new Error(t('identity.replicaGrantRequired'))
    }
    const result = await replicaImportsAPI.importFromPeer({
      source_node_id: sourceNodeID,
      owner_subject_id: subject.subject.subject_id,
      grant_hash: grant.grant_hash,
      archive_id: archiveID,
      snapshot_id: snapshotID,
      lease_id: leaseID,
    })
    setIncomingReplicaResult(result)
    setMessage(t('identity.replicaImported', { count: result.objects, id: result.lease.lease_id }))
  })

  const findIncomingReplicaOffers = () => run(async () => {
    const sourceNodeID = incomingReplicaSourceNodeID.trim()
    const subject = controlledSubjects.find(item => item.subject.subject_id === incomingReplicaSubjectID) ?? controlledSubjects[0]
    const archiveID = incomingReplicaArchiveID || subject?.archive_ids[0] || ''
    if (!identity || identity.frozen || !ownNodeDelegated || !localNodeID || !peerTargets.includes(sourceNodeID) || !subject ||
      !subject.archive_ids.includes(archiveID) || !isSecurePasskeyContext() || window.location.protocol !== 'https:') {
      throw new Error(t('identity.replicaImportPrerequisites'))
    }
    setIncomingReplicaArchiveID(archiveID)
    const result = await replicaImportsAPI.offers(sourceNodeID, subject.subject.subject_id, archiveID)
    setIncomingReplicaOffers(result.offers)
    setIncomingReplicaOffersFetched(true)
    setIncomingReplicaSnapshotID('')
    setIncomingReplicaLeaseID('')
    setIncomingReplicaGrantHash('')
    setIncomingReplicaResult(null)
    setMessage(t('identity.replicaOffersFound', { count: result.offers.length }))
  })

  const resolveTombstoneArchive = () => run(async () => {
    const recordingID = tombstoneRecordingID.trim()
    if (!recordingID) throw new Error(t('identity.tombstoneRecordingRequired'))
    const resolved = await recordingsAPI.archiveSnapshotIdentity(recordingID)
    setTombstoneArchiveID(resolved.archive_id)
    setTombstoneSubjectID('')
    setTombstoneGrantHash('')
    setTombstoneResult(null)
    setMessage(t('identity.tombstoneArchiveResolved', { archive: resolved.archive_id }))
  })

  const submitReplicaTombstone = () => run(async () => {
    const subject = selectedTombstoneSubject
    const grant = usableTombstoneGrants.find(item => item.grant_hash === tombstoneGrantHash) ?? usableTombstoneGrants[0]
    if (!identity || identity.frozen || !ownNodeDelegated || !subject || !grant || !tombstoneArchiveID || window.location.protocol !== 'https:') {
      throw new Error(t('identity.tombstonePrerequisites'))
    }
    if (!window.confirm(t('identity.tombstoneConfirm'))) return
    const result = await replicaTombstonesAPI.submit({
      owner_subject_id: subject.subject.subject_id,
      archive_id: tombstoneArchiveID,
      grant_hash: grant.grant_hash,
      reason_code: 'owner_requested',
    })
    setTombstoneResult(result)
    await client.invalidateQueries({ queryKey: qk.replicaLeases })
    await client.invalidateQueries({ queryKey: qk.ownerAuthoritySubjects })
    setMessage(t('identity.tombstoneRecorded'))
  })

  const revokeCapability = (item: NonNullable<typeof ownerAuthority.data>['subjects'][number], grantHash: string) => run(async () => {
    const identity = status.data?.identity
    if (!identity || identity.frozen || item.frozen || item.subject.controller.kind !== 'identity' || item.subject.controller.id !== identity.identity_id) {
      throw new Error(t('identity.notAvailableForDelegation'))
    }
    if (!window.confirm(t('identity.capabilityRevokeConfirm'))) return
    const keys = await readRecoveryKeys(identity.identity_id)
    keysRef.current = keys
    try {
      const issuedAt = formatAuthorityTimestamp(new Date())
      const signedEvent = signOwnerAuthorityEvent({
        event_version: 'ir.owner-authority.event/1', identity_id: identity.identity_id,
        identity_head_hash: identity.head_hash, subject_id: item.subject.subject_id,
        sequence: (BigInt(item.sequence) + 1n).toString(), previous_event_hash: item.head_hash,
        operation: 'grant_revoke', issued_at: issuedAt,
        signer: item.subject.controller, signer_key_id: keys.signerKeyId,
        payload: { grant_hash: grantHash },
      }, keys.signingSeed)
      await globalIdentityAPI.applyOwnerAuthorityEvent(signedEvent)
      clearRecovery()
      await client.invalidateQueries({ queryKey: qk.ownerAuthoritySubjects })
      setMessage(t('identity.capabilityRevoked'))
    } finally {
      keys.destroy()
      if (keysRef.current === keys) keysRef.current = null
    }
  })

  const logout = () => run(async () => {
    const csrf = session.data?.global_csrf_token
    if (!csrf) throw new Error(t('identity.sessionUnavailable'))
    await globalIdentityAPI.logout(csrf)
    await refresh()
    setMessage(t('identity.loggedOut'))
  })

  const createNodeIdentity = () => run(async () => {
    await globalIdentityAPI.createNodeIdentity()
    await client.invalidateQueries({ queryKey: qk.nodeIdentity })
    setMessage(t('identity.nodeCreated'))
  })

  const issueEnrollmentProof = () => run(async () => {
    const identity = status.data?.identity
    const identityID = identity?.identity_id ?? enrollmentIdentityID.trim()
    const previousHash = identity?.head_hash ?? enrollmentHead.trim()
    if (!identityID || !previousHash) throw new Error(t('identity.enrollmentContextRequired'))
    if (identity?.frozen) throw new Error(t('identity.forkFrozen', { count: identity.fork_count }))
    const proof = await globalIdentityAPI.enrollmentChallenge(identityID, previousHash)
    setEnrollmentProof(proof)
    setEnrollmentProofText(JSON.stringify(proof, null, 2))
    setMessage(t('identity.enrollmentProofReady'))
  })

  const importEnrollmentProofText = (text: string, source: 'file' | 'qr') => {
    if (new TextEncoder().encode(text).byteLength > MAX_NODE_ENROLLMENT_PROOF_BYTES) {
      setError(t('identity.enrollmentProofFileTooLarge'))
      return
    }
    let proof: NodeEnrollmentProof
    try { proof = parseNodeEnrollmentProof(text) } catch {
      setError(t('identity.enrollmentProofInvalid'))
      return
    }
    setError('')
    setEnrollmentProofText(JSON.stringify(proof, null, 2))
    setMessage(source === 'qr' ? t('identity.enrollmentProofScanned') : t('identity.enrollmentProofFileLoaded'))
  }

  const importEnrollmentProofFile = (file: File) => run(async () => {
    if (file.size > MAX_NODE_ENROLLMENT_PROOF_BYTES) {
      setError(t('identity.enrollmentProofFileTooLarge'))
      return
    }
    let proof: NodeEnrollmentProof
    try { proof = parseNodeEnrollmentProof(await readFileText(file)) } catch {
      setError(t('identity.enrollmentProofInvalid'))
      return
    }
    importEnrollmentProofText(JSON.stringify(proof), 'file')
  })

  const downloadJSON = (name: string, value: unknown) => {
    const url = URL.createObjectURL(new Blob([JSON.stringify(value, null, 2)], { type: 'application/json' }))
    const link = document.createElement('a')
    link.href = url
    link.download = name
    link.click()
    URL.revokeObjectURL(url)
  }

  const delegateNode = () => run(async () => {
    const identity = status.data?.identity
    if (!identity || identity.frozen) throw new Error(t('identity.notAvailableForDelegation'))
    let proof: NodeEnrollmentProof
    try { proof = parseNodeEnrollmentProof(enrollmentProofText) } catch { throw new Error(t('identity.enrollmentProofInvalid')) }
    if (!proof || proof.identity_id !== identity.identity_id || proof.previous_event_hash !== identity.head_hash || !proof.node_id || !proof.node_public_key || !proof.enrollment_nonce || !proof.node_proof || !Number.isFinite(Date.parse(proof.expires_at)) || Date.parse(proof.expires_at) <= Date.now()) {
      throw new Error(t('identity.enrollmentProofStale'))
    }
    const keys = await readRecoveryKeys(identity.identity_id)
    keysRef.current = keys
    try {
      const signed = signIdentitySuccessor(keys, 'node_delegate', (BigInt(identity.sequence) + 1n).toString(), identity.head_hash, {
        enrollment_nonce: proof.enrollment_nonce,
        node_id: proof.node_id,
        node_proof: proof.node_proof,
        node_public_key: proof.node_public_key,
      })
      await globalIdentityAPI.delegateNode({ canonical_event: bytesToBase64URL(signed.canonicalEvent), signature: bytesToBase64URL(signed.signature) })
      clearRecovery()
      setEnrollmentProof(null)
      setEnrollmentProofText('')
      await refresh()
      setMessage(t('identity.nodeDelegated', { id: proof.node_id }))
    } finally { keys.destroy(); if (keysRef.current === keys) keysRef.current = null }
  })

  const revokeNode = (nodeID: string) => run(async () => {
    const identity = status.data?.identity
    if (!identity || identity.frozen) throw new Error(t('identity.notAvailableForDelegation'))
    if (!window.confirm(t('identity.revokeNodeConfirm', { id: nodeID }))) return
    const keys = await readRecoveryKeys(identity.identity_id)
    keysRef.current = keys
    try {
      const signed = signIdentitySuccessor(keys, 'node_revoke', (BigInt(identity.sequence) + 1n).toString(), identity.head_hash, { node_id: nodeID })
      await globalIdentityAPI.revokeNode(nodeID, { canonical_event: bytesToBase64URL(signed.canonicalEvent), signature: bytesToBase64URL(signed.signature) })
      clearRecovery()
      await refresh()
      setMessage(t('identity.nodeRevoked', { id: nodeID }))
    } finally { keys.destroy(); if (keysRef.current === keys) keysRef.current = null }
  })

  const resolveFork = (choice: GlobalIdentityForkChoice) => run(async () => {
    const identity = status.data?.identity
    const selectedHead = forkSelections[choice.fork_id]
    if (!identity || !selectedHead) throw new Error(t('identity.forkSelectionRequired'))
    if (!window.confirm(t('identity.forkResolveConfirm', { head: selectedHead }))) return
    const selectedSequence = selectedHead === choice.first_head_hash ? choice.first_sequence : selectedHead === choice.second_head_hash ? choice.second_sequence : ''
    if (!selectedSequence) throw new Error(t('identity.forkSelectionRequired'))
    const keys = await readRecoveryKeys(identity.identity_id)
    keysRef.current = keys
    try {
      const signed = signIdentitySuccessor(keys, 'fork_resolve', (BigInt(selectedSequence) + 1n).toString(), selectedHead, {
        fork_id: choice.fork_id,
        previous_event_hash: choice.previous_event_hash,
        first_head_hash: choice.first_head_hash,
        second_head_hash: choice.second_head_hash,
        selected_head_hash: selectedHead,
      })
      await globalIdentityAPI.resolveFork({ canonical_event: bytesToBase64URL(signed.canonicalEvent), signature: bytesToBase64URL(signed.signature) })
      clearRecovery()
      setForkSelections(current => { const next = { ...current }; delete next[choice.fork_id]; return next })
      await refresh()
      setMessage(t('identity.forkResolved'))
    } finally { keys.destroy(); if (keysRef.current === keys) keysRef.current = null }
  })

  const exportIdentityHistory = () => run(async () => {
    const bundle = await globalIdentityAPI.exportHistory()
    downloadJSON('integrated-recorder-identity-history.json', bundle)
    setMessage(t('identity.historyExported'))
  })

  const publishIdentityHistory = () => run(async () => {
    const result = await globalIdentityAPI.publishHistory()
    setMessage(t('identity.historyPublished', { count: result.providers.filter(provider => provider.success).length }))
  })

  const importIdentityHistory = () => run(async () => {
    if (!historyFile) throw new Error(t('identity.historyFileRequired'))
    if (historyFile.size > 6 * 1024 * 1024) throw new Error(t('identity.historyFileTooLarge'))
    let bundle: GlobalIdentityHistoryBundle
    try { bundle = JSON.parse(await readFileText(historyFile)) as GlobalIdentityHistoryBundle } catch { throw new Error(t('identity.historyFileInvalid')) }
    if (!bundle || bundle.format !== 'ir.identity.history/1' || !Array.isArray(bundle.events)) throw new Error(t('identity.historyFileInvalid'))
    await globalIdentityAPI.importHistory(bundle)
    setHistoryFile(null)
    await refresh()
    setMessage(t('identity.historyImported'))
  })

  const importRegistryHistory = () => run(async () => {
    if (!registryHistoryFile) throw new Error(t('identity.registryFileRequired'))
    if (registryHistoryFile.size > 6 * 1024 * 1024) throw new Error(t('identity.historyFileTooLarge'))
    let bundle: GlobalIdentityHistoryBundle
    try { bundle = JSON.parse(await readFileText(registryHistoryFile)) as GlobalIdentityHistoryBundle } catch { throw new Error(t('identity.historyFileInvalid')) }
    if (!bundle || bundle.format !== 'ir.identity.history/1' || !Array.isArray(bundle.events)) throw new Error(t('identity.historyFileInvalid'))
    try {
      await globalIdentityAPI.importRegistryHistory(bundle)
      setRegistryHistoryFile(null)
      setMessage(t('identity.registryImported'))
    } finally {
      await client.invalidateQueries({ queryKey: qk.globalIdentityRegistry })
    }
  })

  if (status.isLoading) return <Card><CardContent className="pt-5 text-sm text-muted-foreground">{t('common.loading')}</CardContent></Card>
  if (status.error) return <Card><CardContent className="pt-5 text-sm text-destructive">{errorMessage(status.error)}</CardContent></Card>
  const identity = status.data?.identity
  const controlledSubjects = ownerAuthority.data?.subjects.filter(item => item.subject.controller.kind === 'identity' && item.subject.controller.id === identity?.identity_id) ?? []
  const selectedAuthoritySubject = controlledSubjects.find(item => item.subject.subject_id === authoritySubjectID) ?? controlledSubjects[0]
  const usableRekeySourceGrants = selectedAuthoritySubject?.grants.filter(item => rekeySnapshot && !item.revoked && Date.parse(item.expires_at) > Date.now() &&
    item.archive_id === rekeySnapshot.archive_id && !item.snapshot_id && item.audience.kind === 'node' && item.audience.id === localNodeID &&
    item.actions.includes('key.rotate') && item.actions.includes('archive.replicate')) ?? []
  const usableRekeyViewGrants = selectedAuthoritySubject?.grants.filter(item => rekeySnapshot && !item.revoked && Date.parse(item.expires_at) > Date.now() &&
    item.archive_id === rekeySnapshot.archive_id && !item.snapshot_id && item.audience.kind === 'node' && item.audience.id === rekeyRecipientNodeID.trim() &&
    item.actions.includes('archive.view')) ?? []
  const selectedReplicaSubject = controlledSubjects.find(item => item.subject.subject_id === replicaSubjectID) ?? controlledSubjects[0]
  const usableReplicaGrants = selectedReplicaSubject?.grants.filter(item => replicaSnapshot && !item.revoked && Date.parse(item.expires_at) > Date.now() &&
    item.archive_id === replicaSnapshot.archive_id && (!item.snapshot_id || item.snapshot_id === replicaSnapshot.snapshot_id) &&
    item.audience.kind === 'node' && item.audience.id === localNodeID && item.actions.includes('archive.replicate')) ?? []
  const selectedReplicaGrant = usableReplicaGrants.find(item => item.grant_hash === replicaGrantHash) ?? usableReplicaGrants[0]
  const currentReplicaRetryScope = identity && localNodeID && selectedReplicaSubject && selectedReplicaGrant && replicaSnapshot ? {
    identityID: identity.identity_id,
    nodeID: localNodeID,
    recordingID: replicaRecordingID.trim(),
    archiveID: replicaSnapshot.archive_id,
    snapshotID: replicaSnapshot.snapshot_id,
    ownerSubjectID: selectedReplicaSubject.subject.subject_id,
    grantHash: selectedReplicaGrant.grant_hash,
    providerNodeID: replicaProviderNodeID,
  } : null
  const canRetryReplicaWithStoredKey = currentReplicaRetryScope !== null && matchesReplicaRetryScope(replicaRetryIntent, currentReplicaRetryScope)
  const durableReplicaRetryIntents = currentReplicaRetryScope
    ? resumableReplicaRetryIntents(currentReplicaRetryScope, replicaState.data?.leases ?? [])
    : []
  const durableRetryLeaseExists = Boolean(replicaRetryIntent && replicaState.data?.leases.some(item => item.lease_id === replicaRetryIntent.leaseID))
  const selectedIncomingReplicaSubject = controlledSubjects.find(item => item.subject.subject_id === incomingReplicaSubjectID) ?? controlledSubjects[0]
  const selectedIncomingReplicaArchiveID = selectedIncomingReplicaSubject && selectedIncomingReplicaSubject.archive_ids.includes(incomingReplicaArchiveID)
    ? incomingReplicaArchiveID : selectedIncomingReplicaSubject?.archive_ids[0] ?? ''
  const usableIncomingReplicaGrants = selectedIncomingReplicaSubject?.grants.filter(item => !item.revoked && Date.parse(item.expires_at) > Date.now() &&
    item.archive_id === incomingReplicaArchiveID.trim() && (!item.snapshot_id || item.snapshot_id === incomingReplicaSnapshotID.trim()) &&
    item.audience.kind === 'node' && item.audience.id === localNodeID && item.actions.includes('archive.replicate')) ?? []
  const tombstoneSubjects = controlledSubjects.filter(item => item.archive_ids.includes(tombstoneArchiveID))
  const selectedTombstoneSubject = tombstoneSubjects.find(item => item.subject.subject_id === tombstoneSubjectID) ?? tombstoneSubjects[0]
  const usableTombstoneGrants = selectedTombstoneSubject?.grants.filter(item => !item.revoked && Date.parse(item.expires_at) > Date.now() &&
    item.archive_id === tombstoneArchiveID && !item.snapshot_id && item.audience.kind === 'node' && item.audience.id === localNodeID &&
    item.actions.includes('archive.delete') && item.actions.includes('archive.replicate')) ?? []

  return <Card>
    <CardHeader><CardTitle className="flex items-center gap-2"><Fingerprint className="h-4 w-4 text-primary" />{t('identity.title')}</CardTitle><p className="text-xs text-muted-foreground">{t('identity.help')}</p></CardHeader>
    <CardContent className="space-y-4">
      {!isSecurePasskeyContext() && <p role="status" className="rounded-md border border-amber-500/40 bg-amber-500/5 p-3 text-sm">{t('identity.secureContextRequired')}</p>}
      <div className="space-y-3 rounded-md border border-border p-3">
        <p className="text-sm font-medium">{t('identity.nodeTitle')}</p>
        <p className="text-xs text-muted-foreground">{t('identity.nodeHelp')}</p>
        {nodeIdentity.error ? <p role="alert" className="text-sm text-destructive">{errorMessage(nodeIdentity.error)}</p> : nodeIdentity.data?.created && nodeIdentity.data.node ? <dl className="grid gap-2 text-sm"><div><dt className="text-xs text-muted-foreground">{t('identity.nodeID')}</dt><dd className="break-all font-mono">{nodeIdentity.data.node.node_id}</dd></div><div><dt className="text-xs text-muted-foreground">{t('identity.nodePublicKey')}</dt><dd className="break-all font-mono">{nodeIdentity.data.node.public_key}</dd></div></dl> : <Button type="button" variant="outline" disabled={pending || nodeIdentity.isLoading || !nodeIdentity.data} onClick={() => void createNodeIdentity()}>{t('identity.nodeCreate')}</Button>}
        {nodeIdentity.data?.created && !identity && <>
          <p className="text-xs text-muted-foreground">{t('identity.joinHelp')}</p>
          <label className="block space-y-1.5 text-sm">{t('identity.identityID')}<Input value={enrollmentIdentityID} onChange={event => setEnrollmentIdentityID(event.target.value)} autoComplete="off" /></label>
          <label className="block space-y-1.5 text-sm">{t('identity.historyHead')}<Input value={enrollmentHead} onChange={event => setEnrollmentHead(event.target.value)} autoComplete="off" /></label>
          <Button type="button" variant="outline" disabled={pending} onClick={() => void issueEnrollmentProof()}>{t('identity.createEnrollmentProof')}</Button>
        </>}
        {enrollmentProof && !identity && <div className="space-y-2 rounded-md bg-muted/40 p-3">
          <p className="text-xs text-muted-foreground">{t('identity.enrollmentProofHelp', { expires: new Date(enrollmentProof.expires_at).toLocaleString() })}</p>
          <EnrollmentProofQRCode payload={JSON.stringify(enrollmentProof)} imageLabel={t('identity.enrollmentProofQR')} preparingLabel={t('identity.enrollmentProofQRPreparing')} unavailableLabel={t('identity.enrollmentProofQRUnavailable')} />
          <textarea aria-label={t('identity.enrollmentProof')} readOnly value={enrollmentProofText} className="min-h-36 w-full rounded-md border border-input bg-background px-3 py-2 font-mono text-xs" />
          <Button type="button" variant="outline" onClick={() => downloadJSON('integrated-recorder-node-enrollment.json', enrollmentProof)}>{t('identity.downloadEnrollmentProof')}</Button>
        </div>}
        {nodeIdentity.data?.created && <div className="space-y-2 border-t border-border pt-3">
          <p className="text-xs text-muted-foreground">{identity ? t('identity.ownerEnrollmentHelp') : t('identity.importHistoryHelp')}</p>
          {identity && !identity.frozen && <>
            <Button type="button" size="sm" variant="outline" disabled={pending} onClick={() => void issueEnrollmentProof()}>{t('identity.createEnrollmentProof')}</Button>
            <EnrollmentProofQRScanner
              onScan={payload => importEnrollmentProofText(payload, 'qr')}
              startLabel={t('identity.scanEnrollmentProof')}
              stopLabel={t('identity.stopEnrollmentScan')}
              scanningLabel={t('identity.scanningEnrollmentProof')}
              unavailableLabel={t('identity.enrollmentScannerUnavailable')}
              secureContextLabel={t('identity.enrollmentScannerSecureContext')}
            />
            <label className="block space-y-1.5 text-sm">{t('identity.enrollmentProofFile')}<Input type="file" accept="application/json,.json" disabled={pending} onChange={event => { const file = event.target.files?.[0]; if (file) void importEnrollmentProofFile(file); event.currentTarget.value = '' }} /></label>
            <label className="block space-y-1.5 text-sm">{t('identity.enrollmentProof')}<textarea value={enrollmentProofText} onChange={event => setEnrollmentProofText(event.target.value)} className="min-h-28 w-full rounded-md border border-input bg-background px-3 py-2 font-mono text-xs" /></label>
            <Button type="button" variant="outline" disabled={pending || (!recoveryFile && !recoveryPhrase.trim())} onClick={() => void delegateNode()}>{t('identity.delegateNode')}</Button>
            {identity.node_ids.map(nodeID => <div key={nodeID} className="flex items-center justify-between gap-2 text-xs"><span className="break-all font-mono">{nodeID}</span><Button type="button" size="sm" variant="ghost" disabled={pending || (!recoveryFile && !recoveryPhrase.trim())} onClick={() => void revokeNode(nodeID)}>{t('identity.revokeNode')}</Button></div>)}
            <Button type="button" size="sm" variant="outline" disabled={pending || !identity || identity.frozen} onClick={() => void publishIdentityHistory()}>{t('identity.publishHistory')}</Button>
          </>}
          <label className="block space-y-1.5 text-sm">{t('identity.historyFile')}<Input type="file" accept="application/json,.json" onChange={event => setHistoryFile(event.target.files?.[0] ?? null)} /></label>
          <Button type="button" variant="outline" disabled={pending || !historyFile} onClick={() => void importIdentityHistory()}>{identity ? t('identity.syncHistory') : t('identity.joinIdentity')}</Button>
        </div>}
        {identity && ownNodeDelegated && <div className="space-y-3 border-t border-border pt-3">
          <div><p className="text-sm font-medium">{t('identity.peerSessions')}</p><p className="text-xs text-muted-foreground">{t('identity.peerSessionsHelp')}</p></div>
          {peerSessions.error && <p role="alert" className="text-xs text-destructive">{t('identity.peerUnavailable')}: {errorMessage(peerSessions.error)}</p>}
          {peerSessions.data?.sessions.length ? <ul className="space-y-2">{peerSessions.data.sessions.map(item => <li key={item.node_id} className="flex flex-wrap items-center justify-between gap-2 rounded bg-muted/40 p-2 text-xs">
            <span className="min-w-0"><span className="block break-all font-mono">{item.identity_id} · {item.node_id}</span><span className="text-muted-foreground">{t('identity.peerConnectedAt', { time: new Date(item.connected_at).toLocaleString() })}</span></span>
          </li>)}</ul> : !peerSessions.isLoading && !peerSessions.error ? <p className="text-xs text-muted-foreground">{t('identity.peerSessionsEmpty')}</p> : null}
          <div className="space-y-2 rounded-md border border-border p-3">
            <p className="text-xs text-muted-foreground">{t('identity.peerCrossIdentityHelp')}</p>
            <label className="block space-y-1.5 text-sm">{t('identity.peerRemoteIdentityID')}<Input autoComplete="off" spellCheck={false} value={externalPeerIdentityID} onChange={event => setExternalPeerIdentityID(event.target.value)} /></label>
            <label className="block space-y-1.5 text-sm">{t('identity.peerRemoteNodeID')}<Input autoComplete="off" spellCheck={false} value={externalPeerNodeID} onChange={event => setExternalPeerNodeID(event.target.value)} /></label>
            <Button type="button" size="sm" variant="outline" disabled={pending || !externalPeerIdentityID.trim() || !externalPeerNodeID.trim()} onClick={() => void connectExternalPeer()}>{t('identity.peerConnectExternal')}</Button>
          </div>
        </div>}
      </div>
      {identity ? <>
        {identity.frozen && <div className="space-y-3 rounded-md border border-destructive/40 bg-destructive/5 p-3">
          <p role="alert" className="text-sm">{t('identity.forkFrozen', { count: identity.fork_count })}</p>
          <div><p className="text-sm font-medium">{t('identity.forkResolveTitle')}</p><p className="text-xs text-muted-foreground">{t('identity.forkResolveHelp')}</p></div>
          <Button type="button" size="sm" variant="outline" disabled={pending} onClick={() => void exportIdentityHistory()}>{t('identity.exportHistory')}</Button>
          {forkChoices.error && <p role="alert" className="text-xs text-destructive">{errorMessage(forkChoices.error)}</p>}
          {forkChoices.data?.forks.map(choice => {
            const selected = forkSelections[choice.fork_id]
            return <div key={choice.fork_id} className="space-y-2 rounded border border-border bg-background p-3">
              <p className="break-all font-mono text-xs">{choice.fork_id}</p>
              <p className="break-all text-xs text-muted-foreground">{choice.previous_event_hash || t('identity.forkGenesis')} · {t('identity.forkHeads')}</p>
              <div className="grid gap-2 sm:grid-cols-2">
                {[{ head: choice.first_head_hash, sequence: choice.first_sequence, label: 'identity.forkBranchFirst' }, { head: choice.second_head_hash, sequence: choice.second_sequence, label: 'identity.forkBranchSecond' }].map(branch => <Button key={branch.head} type="button" size="sm" variant="outline" aria-pressed={selected === branch.head} className={selected === branch.head ? 'ring-2 ring-primary' : ''} disabled={pending} onClick={() => setForkSelections(current => ({ ...current, [choice.fork_id]: branch.head }))}>
                  {t(branch.label as TranslationKey)} · seq {branch.sequence}
                </Button>)}
              </div>
              <p className="break-all font-mono text-xs">{selected || t('identity.forkNoSelection')}</p>
              <Button type="button" size="sm" variant="outline" disabled={pending || !selected || (!recoveryFile && !recoveryPhrase.trim())} onClick={() => void resolveFork(choice)}>{t('identity.forkResolveApprove')}</Button>
            </div>
          })}
          {forkChoices.data?.forks.length === 0 && !forkChoices.isLoading && <p className="text-xs text-muted-foreground">{t('identity.forkNoResolvableChoice')}</p>}
        </div>}
        {!identity.frozen && <Button type="button" size="sm" variant="outline" disabled={pending} onClick={() => void exportIdentityHistory()}>{t('identity.exportHistory')}</Button>}
        <dl className="grid gap-3 rounded-md bg-muted/40 p-3 text-sm sm:grid-cols-3"><div><dt className="text-xs text-muted-foreground">{t('identity.identityID')}</dt><dd className="break-all font-mono">{identity.identity_id}</dd></div><div><dt className="text-xs text-muted-foreground">{t('identity.historySequence')}</dt><dd>{identity.sequence}</dd></div><div><dt className="text-xs text-muted-foreground">{t('identity.passkeyCount')}</dt><dd>{identity.credential_count}</dd></div></dl>
        {session.data?.authenticated ? <div className="flex flex-wrap items-center gap-2"><p className="text-sm">{t('identity.authenticatedAs', { id: session.data.identity_id ?? identity.identity_id })}</p><Button type="button" size="sm" variant="outline" disabled={pending} onClick={() => void logout()}><LogOut className="h-4 w-4" />{t('identity.logout')}</Button></div> : <Button type="button" variant="outline" disabled={!isSecurePasskeyContext() || pending} onClick={() => void authenticatePasskey()}><Fingerprint className="h-4 w-4" />{t('identity.authenticate')}</Button>}
        <div className="space-y-3 rounded-md border border-border p-3">
          <p className="text-sm font-medium">{t('identity.recoveryTitle')}</p><p className="text-xs text-muted-foreground">{t('identity.recoveryHelp')}</p>
          <label className="block space-y-1.5 text-sm">{t('identity.recoveryPhrase')}<textarea className="min-h-24 w-full rounded-md border border-input bg-background px-3 py-2 font-mono text-xs" value={recoveryPhrase} onChange={event => { setRecoveryPhrase(event.target.value); setRecoveryFile(null) }} autoComplete="off" spellCheck={false} /></label>
          <label className="block space-y-1.5 text-sm">{t('identity.recoveryFile')}<Input type="file" accept=".irrc,application/octet-stream" onChange={event => { setRecoveryFile(event.target.files?.[0] ?? null); if (event.target.files?.[0]) setRecoveryPhrase('') }} /></label>
          <div className="flex flex-wrap gap-2"><Button type="button" variant="outline" disabled={identity.frozen || !isSecurePasskeyContext() || pending || (!recoveryFile && !recoveryPhrase.trim())} onClick={() => void addPasskey()}><UserPlus className="h-4 w-4" />{t('identity.addPasskey')}</Button>{identity.credential_ids.map(id => <Button type="button" key={id} variant="ghost" size="sm" disabled={identity.frozen || pending || (!recoveryFile && !recoveryPhrase.trim())} onClick={() => void revokePasskey(id)}>{t('identity.revokePasskey')}</Button>)}</div>
        </div>
        <div className="space-y-3 rounded-md border border-border p-3">
          <div><p className="text-sm font-medium">{t('identity.replicaTitle')}</p><p className="text-xs text-muted-foreground">{t('identity.replicaHelp')}</p></div>
          <label className="block space-y-1.5 text-sm">{t('identity.replicaRecording')}<Input value={replicaRecordingID} onChange={event => { setReplicaRecordingID(event.target.value); setReplicaSnapshot(null); setReplicaManifest(null); setReplicaLease(null); setReplicaRetryIntent(null); setReplicaGrantHash('') }} autoComplete="off" spellCheck={false} /></label>
          <Button type="button" variant="outline" disabled={pending || !replicaRecordingID.trim()} onClick={() => void resolveReplicaSnapshot()}>{t('identity.replicaResolve')}</Button>
          {replicaSnapshot && <p className="break-all text-xs text-muted-foreground">{replicaSnapshot.archive_id} · {replicaSnapshot.snapshot_id} · {t('identity.replicaObjectCount', { count: replicaSnapshot.object_count })}</p>}
          <label className="block space-y-1.5 text-sm">{t('identity.replicaProvider')}<select className="w-full rounded-md border border-input bg-background px-3 py-2" value={replicaProviderNodeID} onChange={event => { setReplicaProviderNodeID(event.target.value); setReplicaLease(null); setReplicaRetryIntent(null); setReplicaManifest(null) }}><option value="">{t('identity.peerTargetSelect')}</option>{peerTargets.map(id => <option key={id} value={id}>{id}</option>)}</select></label>
          {controlledSubjects.length > 0 && <label className="block space-y-1.5 text-sm">{t('identity.capabilitySubject')}<select className="w-full rounded-md border border-input bg-background px-3 py-2" value={replicaSubjectID || controlledSubjects[0].subject.subject_id} onChange={event => { setReplicaSubjectID(event.target.value); setReplicaGrantHash(''); setReplicaManifest(null); setReplicaLease(null); setReplicaRetryIntent(null) }}>{controlledSubjects.map(item => <option key={item.subject.subject_id} value={item.subject.subject_id}>{item.subject.subject_id}</option>)}</select></label>}
          {usableReplicaGrants.length > 0 && <label className="block space-y-1.5 text-sm">{t('identity.replicaGrant')}<select className="w-full rounded-md border border-input bg-background px-3 py-2" value={replicaGrantHash || usableReplicaGrants[0].grant_hash} onChange={event => { setReplicaGrantHash(event.target.value); setReplicaManifest(null); setReplicaLease(null); setReplicaRetryIntent(null) }}>{usableReplicaGrants.map(item => <option key={item.grant_hash} value={item.grant_hash}>{item.grant_hash}</option>)}</select></label>}
          <Button type="button" variant="outline" disabled={pending || !replicaSnapshot || !replicaProviderNodeID || usableReplicaGrants.length === 0 || (!canRetryReplicaWithStoredKey && !recoveryFile && !recoveryPhrase.trim()) || window.location.protocol !== 'https:'} onClick={() => void prepareEncryptedReplica()}>{canRetryReplicaWithStoredKey ? t('identity.replicaRetry') : t('identity.replicaOffer')}</Button>
          {!canRetryReplicaWithStoredKey && durableReplicaRetryIntents.length > 0 && <div className="space-y-2 rounded-md border border-border p-3">
            <p className="text-xs text-muted-foreground">{t('identity.replicaRetryAfterReload')}</p>
            {durableReplicaRetryIntents.map(intent => <div key={intent.leaseID} className="flex flex-wrap items-center justify-between gap-2 text-xs">
              <span className="break-all font-mono">{intent.leaseID} · {intent.keyEpoch}</span>
              <Button type="button" size="sm" variant="outline" disabled={pending} onClick={() => { setReplicaRetryIntent(intent); setReplicaManifest(null); setReplicaLease(null) }}>{t('identity.replicaResumeOffer')}</Button>
            </div>)}
          </div>}
          {replicaRetryIntent && canRetryReplicaWithStoredKey && <p className="text-xs text-muted-foreground">{t('identity.replicaRetryHelp')}</p>}
          {replicaRetryIntent && canRetryReplicaWithStoredKey && !replicaLease && !durableRetryLeaseExists && replicaState.isSuccess && !replicaState.isFetching && <Button type="button" variant="ghost" disabled={pending} onClick={resetReplicaRetry}>{t('identity.replicaRetryFresh')}</Button>}
          {window.location.protocol !== 'https:' && <p className="text-xs text-amber-700 dark:text-amber-400">{t('identity.replicaHTTPSRequired')}</p>}
          {replicaManifest && <div role="status" className="rounded bg-muted/40 p-2 text-xs"><p>{t('identity.replicaEncrypted', { count: replicaManifest.objects.length, epoch: replicaManifest.key_epoch })}</p><p className="break-all font-mono">{replicaManifest.snapshot_id}</p></div>}
          {replicaLease && <div role="status" className="rounded bg-muted/40 p-2 text-xs"><p>{t('identity.replicaLeasePending', { status: replicaLease.status })}</p><p className="break-all font-mono">{replicaLease.lease_id} → {replicaLease.provider_node_id}</p></div>}
          {replicaLease && <p className="break-all text-xs text-muted-foreground">{t('identity.replicaTransferReference', { subject: selectedReplicaSubject?.subject.subject_id ?? '', grant: replicaGrantHash || usableReplicaGrants[0]?.grant_hash || '', archive: replicaLease.archive_id, snapshot: replicaLease.snapshot_id })}</p>}
          <div className="space-y-3 border-t border-border pt-3">
            <div><p className="text-sm font-medium">{t('identity.replicaImportTitle')}</p><p className="text-xs text-muted-foreground">{t('identity.replicaImportHelp')}</p></div>
            <label className="block space-y-1.5 text-sm">{t('identity.replicaSource')}<select className="w-full rounded-md border border-input bg-background px-3 py-2" value={incomingReplicaSourceNodeID} onChange={event => { setIncomingReplicaSourceNodeID(event.target.value); setIncomingReplicaOffers([]); setIncomingReplicaOffersFetched(false); setIncomingReplicaResult(null) }}><option value="">{t('identity.peerTargetSelect')}</option>{peerTargets.map(id => <option key={id} value={id}>{id}</option>)}</select></label>
            {controlledSubjects.length > 0 && <label className="block space-y-1.5 text-sm">{t('identity.capabilitySubject')}<select className="w-full rounded-md border border-input bg-background px-3 py-2" value={incomingReplicaSubjectID || controlledSubjects[0].subject.subject_id} onChange={event => { const subject = controlledSubjects.find(item => item.subject.subject_id === event.target.value); setIncomingReplicaSubjectID(event.target.value); setIncomingReplicaArchiveID(subject?.archive_ids[0] ?? ''); setIncomingReplicaOffers([]); setIncomingReplicaOffersFetched(false); setIncomingReplicaGrantHash(''); setIncomingReplicaResult(null) }}>{controlledSubjects.map(item => <option key={item.subject.subject_id} value={item.subject.subject_id}>{item.subject.subject_id}</option>)}</select></label>}
            <label className="block space-y-1.5 text-sm">{t('identity.replicaArchiveID')}<select className="w-full rounded-md border border-input bg-background px-3 py-2" value={selectedIncomingReplicaArchiveID} onChange={event => { setIncomingReplicaArchiveID(event.target.value); setIncomingReplicaOffers([]); setIncomingReplicaOffersFetched(false); setIncomingReplicaSnapshotID(''); setIncomingReplicaLeaseID(''); setIncomingReplicaResult(null); setIncomingReplicaGrantHash('') }}><option value="">{t('identity.replicaArchiveSelect')}</option>{(selectedIncomingReplicaSubject?.archive_ids ?? []).map(archiveID => <option key={archiveID} value={archiveID}>{archiveID}</option>)}</select></label>
            <Button type="button" variant="outline" disabled={pending || window.location.protocol !== 'https:' || !incomingReplicaSourceNodeID || !selectedIncomingReplicaArchiveID || !selectedIncomingReplicaSubject} onClick={() => void findIncomingReplicaOffers()}>{t('identity.replicaFindOffers')}</Button>
            {incomingReplicaOffers.length > 0 && <div className="space-y-2" role="list" aria-label={t('identity.replicaPendingOffers')}>{incomingReplicaOffers.map(offer => <div key={offer.lease_id} className="flex flex-col gap-2 rounded-md border border-border p-3 sm:flex-row sm:items-center sm:justify-between" role="listitem"><div className="min-w-0 text-xs"><p className="break-all font-mono">{offer.lease_id}</p><p className="break-all text-muted-foreground">{offer.snapshot_id} · {t('identity.replicaObjectCount', { count: offer.object_count })} · {offer.reserved_bytes} B</p></div><Button type="button" size="sm" variant="outline" onClick={() => { setIncomingReplicaArchiveID(offer.archive_id); setIncomingReplicaSnapshotID(offer.snapshot_id); setIncomingReplicaLeaseID(offer.lease_id); setIncomingReplicaGrantHash(offer.grant_hash); setIncomingReplicaResult(null) }}>{t('identity.replicaSelectOffer')}</Button></div>)}</div>}
            {incomingReplicaOffersFetched && incomingReplicaOffers.length === 0 && <p className="text-xs text-muted-foreground">{t('identity.replicaNoOffers')}</p>}
            <label className="block space-y-1.5 text-sm">{t('identity.replicaSnapshotID')}<Input value={incomingReplicaSnapshotID} onChange={event => { setIncomingReplicaSnapshotID(event.target.value); setIncomingReplicaResult(null); setIncomingReplicaGrantHash('') }} autoComplete="off" spellCheck={false} /></label>
            <label className="block space-y-1.5 text-sm">{t('identity.replicaLeaseID')}<Input value={incomingReplicaLeaseID} onChange={event => { setIncomingReplicaLeaseID(event.target.value); setIncomingReplicaResult(null) }} autoComplete="off" spellCheck={false} /></label>
            {usableIncomingReplicaGrants.length > 0 && <label className="block space-y-1.5 text-sm">{t('identity.replicaGrant')}<select className="w-full rounded-md border border-input bg-background px-3 py-2" value={incomingReplicaGrantHash || usableIncomingReplicaGrants[0].grant_hash} onChange={event => setIncomingReplicaGrantHash(event.target.value)}>{usableIncomingReplicaGrants.map(item => <option key={item.grant_hash} value={item.grant_hash}>{item.grant_hash}</option>)}</select></label>}
            <Button type="button" variant="outline" disabled={pending || window.location.protocol !== 'https:' || !incomingReplicaSourceNodeID || !incomingReplicaArchiveID.trim() || !incomingReplicaSnapshotID.trim() || !incomingReplicaLeaseID.trim() || usableIncomingReplicaGrants.length === 0} onClick={() => void importEncryptedReplica()}>{t('identity.replicaImport')}</Button>
            {window.location.protocol !== 'https:' && <p className="text-xs text-amber-700 dark:text-amber-400">{t('identity.replicaHTTPSRequired')}</p>}
            {incomingReplicaResult && <div role="status" className="rounded bg-muted/40 p-2 text-xs"><p>{t('identity.replicaImported', { count: incomingReplicaResult.objects, id: incomingReplicaResult.lease.lease_id })}</p><p>{incomingReplicaResult.lease.status} · {incomingReplicaResult.snapshot_id}</p></div>}
          </div>
        </div>
        <div className="space-y-3 border-t border-border pt-3">
          <div><p className="text-sm font-medium">{t('identity.tombstoneTitle')}</p><p className="text-xs text-muted-foreground">{t('identity.tombstoneHelp')}</p></div>
          <label className="block space-y-1.5 text-sm">{t('identity.tombstoneRecording')}<Input value={tombstoneRecordingID} onChange={event => { setTombstoneRecordingID(event.target.value); setTombstoneArchiveID(''); setTombstoneResult(null) }} autoComplete="off" spellCheck={false} /></label>
          <Button type="button" size="sm" variant="outline" disabled={pending || !tombstoneRecordingID.trim()} onClick={() => void resolveTombstoneArchive()}>{t('identity.tombstoneResolve')}</Button>
          {tombstoneArchiveID && <p className="break-all text-xs font-mono">{tombstoneArchiveID}</p>}
          {tombstoneSubjects.length > 0 && <label className="block space-y-1.5 text-sm">{t('identity.capabilitySubject')}<select className="w-full rounded-md border border-input bg-background px-3 py-2" value={selectedTombstoneSubject?.subject.subject_id ?? ''} onChange={event => { setTombstoneSubjectID(event.target.value); setTombstoneGrantHash(''); setTombstoneResult(null) }}>{tombstoneSubjects.map(item => <option key={item.subject.subject_id} value={item.subject.subject_id}>{item.subject.subject_id}</option>)}</select></label>}
          {usableTombstoneGrants.length > 0 && <label className="block space-y-1.5 text-sm">{t('identity.tombstoneCapability')}<select className="w-full rounded-md border border-input bg-background px-3 py-2" value={tombstoneGrantHash || usableTombstoneGrants[0].grant_hash} onChange={event => { setTombstoneGrantHash(event.target.value); setTombstoneResult(null) }}>{usableTombstoneGrants.map(item => <option key={item.grant_hash} value={item.grant_hash}>{item.grant_hash}</option>)}</select></label>}
          <Button type="button" variant="destructive" disabled={pending || !selectedTombstoneSubject || usableTombstoneGrants.length === 0 || window.location.protocol !== 'https:'} onClick={() => void submitReplicaTombstone()}>{t('identity.tombstoneSubmit')}</Button>
          {window.location.protocol !== 'https:' && <p className="text-xs text-amber-700 dark:text-amber-400">{t('identity.replicaHTTPSRequired')}</p>}
          {tombstoneResult && <div role="status" className="space-y-1 rounded bg-muted/40 p-2 text-xs"><p>{t('identity.tombstoneRecorded')}</p><p className="break-all font-mono">{tombstoneResult.archive_id} · epoch {tombstoneResult.deletion_epoch} · {tombstoneResult.tombstone_hash}</p><p>{t('identity.tombstoneNoPhysicalDelete')}</p>{tombstoneResult.peers.map(peer => <p key={peer.node_id} className="break-all">{peer.node_id} · {t(peer.status === 'durable_receipt' ? 'identity.tombstonePeerReceipt' : 'identity.tombstonePeerPending')}</p>)}</div>}
          {(replicaState.data?.tombstones ?? []).length > 0 && <div className="space-y-2 border-t border-border pt-3" aria-live="polite"><p className="text-sm font-medium">{t('identity.tombstoneHistory')}</p>{replicaState.data?.tombstones.map(tombstone => {
            const acknowledged = new Set(tombstone.acknowledged_nodes)
            const providers = [...new Set((replicaState.data?.leases ?? []).filter(item => item.archive_id === tombstone.archive_id && item.provider_node_id !== localNodeID && identityNodeIDs.includes(item.provider_node_id)).map(item => item.provider_node_id))].sort()
            return <div key={tombstone.archive_id} className="rounded border border-border p-2 text-xs">
              <p className="break-all font-mono">{tombstone.archive_id} · epoch {tombstone.deletion_epoch}{tombstone.frozen ? ` · ${t('identity.tombstoneFrozen')}` : ''}</p>
              {providers.length === 0 ? <p className="mt-1 text-muted-foreground">{t('identity.tombstoneNoReplicaTargets')}</p> : providers.map(nodeID => <p key={nodeID} className="mt-1 break-all">{nodeID} · {t(acknowledged.has(nodeID) ? 'identity.tombstonePeerReceipt' : 'identity.tombstonePeerPending')}</p>)}
            </div>
          })}</div>}
        </div>
        <div className="space-y-3 rounded-md border border-border p-3">
          <p className="text-sm font-medium">{t('identity.ownerSubjects')}</p>
          <p className="text-xs text-muted-foreground">{t('identity.ownerSubjectsHelp')}</p>
          <div className="space-y-2 rounded bg-muted/40 p-2">
            <p className="text-xs text-muted-foreground">{t('identity.syncAuthorityHistoryHelp')}</p>
            <label className="block space-y-1.5 text-xs">{t('identity.ownerSubjectID')}<Input value={authoritySyncSubjectID} onChange={event => setAuthoritySyncSubjectID(event.target.value)} autoComplete="off" spellCheck={false} /></label>
            <Button type="button" size="sm" variant="outline" disabled={pending || !/^os_[A-Za-z0-9_-]{22}$/u.test(authoritySyncSubjectID.trim())} onClick={() => void syncOwnerAuthorityHistory()}>{t('identity.syncAuthorityHistory')}</Button>
          </div>
          {controlledSubjects.length > 0 && <section className="space-y-3 rounded border border-border p-3" aria-labelledby="owner-rekey-prepare-title">
            <div><h3 id="owner-rekey-prepare-title" className="text-sm font-medium">{t('identity.rekeyPrepareTitle')}</h3><p className="mt-1 text-xs text-muted-foreground">{t('identity.rekeyPrepareHelp')}</p></div>
            <label className="block space-y-1.5 text-xs">{t('identity.capabilitySubject')}<select className="w-full rounded-md border border-input bg-background px-3 py-2" value={selectedAuthoritySubject?.subject.subject_id ?? ''} onChange={event => setAuthoritySubjectID(event.target.value)}>{controlledSubjects.map(item => <option key={item.subject.subject_id} value={item.subject.subject_id}>{item.subject.subject_id}</option>)}</select></label>
            <label className="block space-y-1.5 text-xs">{t('identity.rekeyRecording')}<Input value={rekeyRecordingID} onChange={event => { setRekeyRecordingID(event.target.value); setRekeySnapshot(null) }} autoComplete="off" spellCheck={false} /></label>
            <Button type="button" size="sm" variant="outline" disabled={pending || !rekeyRecordingID.trim()} onClick={() => void resolveOwnerRekeyArchive()}>{t('identity.rekeyArchiveResolve')}</Button>
            {rekeySnapshot && <>
              <p className="break-all text-xs font-mono">{rekeySnapshot.archive_id} · {rekeySnapshot.snapshot_id} · {t('identity.replicaObjectCount', { count: rekeySnapshot.object_count })}</p>
              <label className="block space-y-1.5 text-xs">{t('identity.rekeyPreviousEpoch')}<Input type="number" min="1" step="1" value={rekeyPreviousEpoch} onChange={event => setRekeyPreviousEpoch(event.target.value)} /></label>
              <label className="block space-y-1.5 text-xs">{t('identity.transferRecipient')}<Input value={rekeyRecipientIdentityID} onChange={event => setRekeyRecipientIdentityID(event.target.value)} autoComplete="off" spellCheck={false} placeholder="gi_…" /></label>
              <label className="block space-y-1.5 text-xs">{t('identity.rekeyRecipientNode')}<Input value={rekeyRecipientNodeID} onChange={event => { setRekeyRecipientNodeID(event.target.value); setRekeyViewGrantHash('') }} autoComplete="off" spellCheck={false} placeholder="nd_…" /></label>
              {usableRekeySourceGrants.length > 0 && <label className="block space-y-1.5 text-xs">{t('identity.rekeySourceGrant')}<select className="w-full rounded-md border border-input bg-background px-3 py-2" value={rekeyGrantHash || usableRekeySourceGrants[0]?.grant_hash} onChange={event => setRekeyGrantHash(event.target.value)}>{usableRekeySourceGrants.map(item => <option key={item.grant_hash} value={item.grant_hash}>{item.grant_hash}</option>)}</select></label>}
              {usableRekeyViewGrants.length > 0 && <label className="block space-y-1.5 text-xs">{t('identity.rekeyViewGrant')}<select className="w-full rounded-md border border-input bg-background px-3 py-2" value={rekeyViewGrantHash || usableRekeyViewGrants[0]?.grant_hash} onChange={event => setRekeyViewGrantHash(event.target.value)}>{usableRekeyViewGrants.map(item => <option key={item.grant_hash} value={item.grant_hash}>{item.grant_hash}</option>)}</select></label>}
              <Button type="button" size="sm" variant="outline" disabled={pending || !selectedAuthoritySubject || selectedAuthoritySubject.frozen || !rekeyPreviousEpoch || !/^gi_[A-Za-z0-9_-]{22}$/u.test(rekeyRecipientIdentityID.trim()) || !/^nd_[A-Za-z0-9_-]{22}$/u.test(rekeyRecipientNodeID.trim()) || usableRekeySourceGrants.length === 0 || usableRekeyViewGrants.length === 0 || (!recoveryFile && !recoveryPhrase.trim()) || window.location.protocol !== 'https:'} onClick={() => void prepareOwnerRekeyHistory()}>{t('identity.rekeyPrepare')}</Button>
            </>}
            {window.location.protocol !== 'https:' && <p className="text-xs text-amber-700 dark:text-amber-400">{t('identity.replicaHTTPSRequired')}</p>}
          </section>}
          <section className="space-y-3 rounded border border-border p-3" aria-labelledby="owner-rekey-verify-title">
            <div><h3 id="owner-rekey-verify-title" className="text-sm font-medium">{t('identity.rekeyVerifyTitle')}</h3><p className="mt-1 text-xs text-muted-foreground">{t('identity.rekeyVerifyHelp')}</p></div>
            <label className="block space-y-1.5 text-xs">{t('identity.rekeyPreparationFile')}<Input type="file" accept="application/json,.json" disabled={pending} onChange={event => setRekeyPreparationFile(event.target.files?.[0] ?? null)} /></label>
            <Button type="button" size="sm" variant="outline" disabled={pending || !rekeyPreparationFile || !localNodeID || !ownNodeDelegated || (!recoveryFile && !recoveryPhrase.trim()) || window.location.protocol !== 'https:'} onClick={() => void verifyIncomingOwnerRekey()}>{t('identity.rekeyVerify')}</Button>
            {window.location.protocol !== 'https:' && <p className="text-xs text-amber-700 dark:text-amber-400">{t('identity.replicaHTTPSRequired')}</p>}
          </section>
          <section className="space-y-3 rounded border border-border p-3" aria-labelledby="owner-transfer-title">
            <div><h3 id="owner-transfer-title" className="text-sm font-medium">{t('identity.transferTitle')}</h3><p className="mt-1 text-xs text-muted-foreground">{t('identity.transferHelp')}</p></div>
            {controlledSubjects.length > 0 && <>
              <label className="block space-y-1.5 text-xs">{t('identity.capabilitySubject')}<select className="w-full rounded-md border border-input bg-background px-3 py-2" value={selectedAuthoritySubject?.subject.subject_id ?? ''} onChange={event => setAuthoritySubjectID(event.target.value)}>{controlledSubjects.map(item => <option key={item.subject.subject_id} value={item.subject.subject_id}>{item.subject.subject_id}</option>)}</select></label>
              <label className="block space-y-1.5 text-xs">{t('identity.transferRecipient')}<Input value={transferRecipientID} onChange={event => setTransferRecipientID(event.target.value)} autoComplete="off" spellCheck={false} placeholder="gi_…" /></label>
              <Button type="button" size="sm" variant="outline" disabled={pending || !selectedAuthoritySubject || selectedAuthoritySubject.frozen || Boolean(selectedAuthoritySubject.rekey_required_archives?.length) || !/^gi_[A-Za-z0-9_-]{22}$/u.test(transferRecipientID.trim()) || (!recoveryFile && !recoveryPhrase.trim())} onClick={() => { if (selectedAuthoritySubject) void createOwnerSubjectTransferOffer(selectedAuthoritySubject) }}>{t('identity.transferCreateOffer')}</Button>
              <label className="block space-y-1.5 text-xs">{t('identity.transferAcceptanceFile')}<Input type="file" accept="application/json,.json" disabled={pending} onChange={event => setTransferAcceptanceFile(event.target.files?.[0] ?? null)} /></label>
              <Button type="button" size="sm" variant="outline" disabled={pending || !transferAcceptanceFile || !selectedAuthoritySubject || selectedAuthoritySubject.frozen || (!recoveryFile && !recoveryPhrase.trim())} onClick={() => { if (selectedAuthoritySubject) void applyOwnerSubjectTransfer(selectedAuthoritySubject) }}>{t('identity.transferApply')}</Button>
            </>}
            <label className="block space-y-1.5 text-xs">{t('identity.transferOfferFile')}<Input type="file" accept="application/json,.json" disabled={pending} onChange={event => setTransferOfferFile(event.target.files?.[0] ?? null)} /></label>
            <Button type="button" size="sm" variant="outline" disabled={pending || !transferOfferFile || (!recoveryFile && !recoveryPhrase.trim())} onClick={() => void acceptIncomingOwnerSubjectTransferOffer()}>{t('identity.transferAccept')}</Button>
            <p className="text-xs text-amber-700 dark:text-amber-400">{t('identity.transferRekeyWarning')}</p>
          </section>
          {ownerAuthority.error && <p role="alert" className="text-sm text-destructive">{errorMessage(ownerAuthority.error)}</p>}
          {ownerAuthority.data?.subjects.map(item => <div key={item.subject.subject_id} className="space-y-2 rounded bg-muted/40 p-2 text-xs">
            <p className="break-all font-mono">{item.subject.subject_id}</p>
            <p>{item.subject.controller.kind}: <span className="break-all font-mono">{item.subject.controller.id}</span> · epoch {item.subject.policy_epoch}</p>
            <p>{t('identity.claimedArchives')}: {item.archive_ids.length ? item.archive_ids.map(id => <span key={id} className="ml-1 break-all font-mono">{id}</span>) : t('identity.noClaimedArchives')}</p>
            {!!item.rekey_required_archives?.length && <p role="alert" className="text-amber-700 dark:text-amber-400">{t('identity.rekeyRequired')} {item.rekey_required_archives.map(id => <span key={id} className="ml-1 break-all font-mono">{id}</span>)}</p>}
            {item.subject.controller.kind === 'identity' && item.subject.controller.id === identity.identity_id && <section className="space-y-2 rounded border border-border p-2" aria-label={t('identity.replicationPolicyTitle')}>
              <div><p className="font-medium">{t('identity.replicationPolicyTitle')}</p><p className="text-muted-foreground">{t('identity.replicationPolicyHelp')}</p></div>
              {(() => {
                const policy = replicationPolicyDrafts[item.subject.subject_id] ?? item.replication_policy ?? { minimum_replicas: '0', grace_period_seconds: '0', capacity_limit_bytes: '0', relocate_after_grace: false }
                const updatePolicy = (patch: Partial<OwnerSubjectReplicationPolicy>) => setReplicationPolicyDrafts(current => ({ ...current, [item.subject.subject_id]: { ...policy, ...patch } }))
                return <>
                  <div className="grid gap-2 sm:grid-cols-3">
                    <label className="block space-y-1">{t('identity.replicationPolicyMinimum')}<Input type="number" min={0} max={16} step={1} value={policy.minimum_replicas} onChange={event => updatePolicy({ minimum_replicas: event.target.value })} /></label>
                    <label className="block space-y-1">{t('identity.replicationPolicyGrace')}<Input type="number" min={0} max={2592000} step={1} value={policy.grace_period_seconds} onChange={event => updatePolicy({ grace_period_seconds: event.target.value })} /></label>
                    <label className="block space-y-1">{t('identity.replicationPolicyCapacity')}<Input type="number" min={0} max={1125899906842624} step={1} value={policy.capacity_limit_bytes} onChange={event => updatePolicy({ capacity_limit_bytes: event.target.value })} /></label>
                  </div>
                  <label className="flex items-start gap-2"><input type="checkbox" checked={policy.relocate_after_grace} onChange={event => updatePolicy({ relocate_after_grace: event.target.checked })} /><span>{t('identity.replicationPolicyRelocate')}</span></label>
                  <Button type="button" size="sm" variant="outline" disabled={pending || identity.frozen || item.frozen || (!recoveryFile && !recoveryPhrase.trim()) || !validOwnerSubjectReplicationPolicy(policy)} onClick={() => void saveReplicationPolicy(item)}>{t('identity.replicationPolicySave')}</Button>
                </>
              })()}
            </section>}
            {!!item.rekey_required_archives?.length && item.subject.controller.kind === 'identity' && item.subject.controller.id === identity.identity_id && <section className="space-y-2 rounded border border-border p-2" aria-label={t('identity.rekeyCompleteTitle')}>
              <p className="font-medium">{t('identity.rekeyCompleteTitle')}</p>
              <p className="text-muted-foreground">{t('identity.rekeyCompleteHelp')}</p>
              <label className="block space-y-1.5">{t('identity.rekeyEvidenceFiles')}<Input type="file" accept="application/json,.json" multiple disabled={pending} onChange={event => setRekeyEvidenceFiles(Array.from(event.target.files ?? []))} /></label>
              <p>{t('identity.rekeyEvidenceCount', { count: rekeyEvidenceFiles.length, pending: item.rekey_required_archives.length })}</p>
              <Button type="button" size="sm" variant="outline" disabled={pending || !rekeyEvidenceFiles.length || !identity || identity.frozen || item.frozen || (!recoveryFile && !recoveryPhrase.trim()) || window.location.protocol !== 'https:'} onClick={() => void completeOwnerSubjectRekey(item)}>{t('identity.rekeyComplete')}</Button>
            </section>}
            {item.frozen && <p role="alert" className="text-destructive">{t('identity.forkFrozen', { count: item.fork_count })}</p>}
            {item.grants.map(grant => <div key={grant.grant_hash} className="flex flex-wrap items-center justify-between gap-2 border-t border-border pt-2">
              <span className="min-w-0 break-all font-mono">{grant.grant_hash} · {grant.audience.kind}:{grant.audience.id} · {grant.actions.join(', ')}{grant.revoked ? ` · ${t('identity.capabilityRevokedLabel')}` : ''}</span>
              {!grant.revoked && <Button type="button" size="sm" variant="ghost" disabled={pending || item.frozen || identity.frozen || (!recoveryFile && !recoveryPhrase.trim())} onClick={() => void revokeCapability(item, grant.grant_hash)}>{t('identity.capabilityRevoke')}</Button>}
            </div>)}
          </div>)}
          <Button type="button" variant="outline" disabled={pending || !identity || identity.frozen || (!recoveryFile && !recoveryPhrase.trim())} onClick={() => void createOwnerSubject()}>{t('identity.createOwnerSubject')}</Button>
          {ownerAuthority.data?.subjects.length ? <fieldset className="space-y-3 rounded-md border border-border p-3" disabled={pending || identity.frozen || (!recoveryFile && !recoveryPhrase.trim())}>
            <legend className="px-1 text-sm font-medium">{t('identity.capabilityIssue')}</legend>
            <label className="block space-y-1.5">{t('identity.capabilitySubject')}<select className="w-full rounded-md border border-input bg-background px-3 py-2" value={authoritySubjectID || controlledSubjects[0]?.subject.subject_id || ''} onChange={event => setAuthoritySubjectID(event.target.value)}>{controlledSubjects.map(item => <option key={item.subject.subject_id} value={item.subject.subject_id}>{item.subject.subject_id}</option>)}</select></label>
            <div className="grid gap-3 sm:grid-cols-2"><label className="block space-y-1.5">{t('identity.capabilityAudienceType')}<select className="w-full rounded-md border border-input bg-background px-3 py-2" value={authorityAudienceKind} onChange={event => setAuthorityAudienceKind(event.target.value === 'identity' ? 'identity' : 'node')}><option value="node">Node</option><option value="identity">Global Identity</option></select></label><label className="block space-y-1.5">{t('identity.capabilityAudience')}<Input value={authorityAudienceID} onChange={event => setAuthorityAudienceID(event.target.value)} autoComplete="off" spellCheck={false} /></label></div>
            <div className="grid gap-3 sm:grid-cols-[minmax(0,1fr)_auto] sm:items-end"><label className="block space-y-1.5">{t('identity.capabilityRecording')}<Input value={authorityRecordingID} onChange={event => setAuthorityRecordingID(event.target.value)} autoComplete="off" spellCheck={false} /></label><Button type="button" variant="outline" disabled={pending || !authorityRecordingID.trim()} onClick={() => void resolveArchiveSnapshot()}>{t('identity.resolveArchiveSnapshot')}</Button></div>
            <div className="grid gap-3 sm:grid-cols-2"><label className="block space-y-1.5">{t('identity.capabilityArchive')}<Input value={authorityArchiveID} onChange={event => { setAuthorityArchiveID(event.target.value); if (event.target.value.trim() !== resolvedAuthorityArchiveID) setResolvedAuthorityArchiveID('') }} placeholder="arc_…" autoComplete="off" spellCheck={false} /></label><label className="block space-y-1.5">{t('identity.capabilitySnapshot')}<Input value={authoritySnapshotID} onChange={event => setAuthoritySnapshotID(event.target.value)} placeholder="snap_…" autoComplete="off" spellCheck={false} /></label></div>
            <Button type="button" variant="outline" disabled={pending || !selectedAuthoritySubject || selectedAuthoritySubject.frozen || selectedAuthoritySubject.archive_ids.includes(authorityArchiveID.trim()) || resolvedAuthorityArchiveID !== authorityArchiveID.trim() || !/^arc_[A-Za-z0-9_-]{22}$/u.test(authorityArchiveID.trim())} onClick={() => { if (selectedAuthoritySubject) void claimArchive(selectedAuthoritySubject) }}>{t('identity.claimArchive')}</Button>
            <fieldset className="grid gap-2 sm:grid-cols-2"><legend className="mb-1">{t('identity.capabilityActions')}</legend>{['archive.view', 'archive.export', 'archive.replicate', 'archive.annotate', 'archive.manage', 'archive.delegate', 'archive.transfer', 'archive.delete', 'key.rotate'].map(action => <label key={action} className="flex items-center gap-2"><input type="checkbox" checked={authorityActions.includes(action)} onChange={event => setAuthorityActions(current => event.target.checked ? [...current, action] : current.filter(value => value !== action))} /><span>{t(`identity.capabilityAction.${action}` as TranslationKey)}</span></label>)}</fieldset>
            <div className="grid gap-3 sm:grid-cols-2"><label className="block space-y-1.5">{t('identity.capabilityLifetime')}<Input type="number" min={1} max={366} value={authorityLifetimeDays} onChange={event => setAuthorityLifetimeDays(event.target.value)} /></label><label className="block space-y-1.5">{t('identity.capabilityDelegationDepth')}<Input type="number" min={0} max={8} value={authorityDelegationDepth} onChange={event => setAuthorityDelegationDepth(event.target.value)} /></label></div>
            <Button type="button" variant="outline" disabled={pending || !selectedAuthoritySubject || selectedAuthoritySubject.frozen || selectedAuthoritySubject.rekey_required_archives?.includes(authorityArchiveID.trim()) || !selectedAuthoritySubject.archive_ids.includes(authorityArchiveID.trim())} onClick={() => { if (selectedAuthoritySubject) void issueCapability(selectedAuthoritySubject) }}>{t('identity.capabilityIssue')}</Button>
          </fieldset> : null}
        </div>
      </> : recoveryMaterial ? <>
        <p className="text-sm font-medium">{t('identity.recoveryWarning')}</p>
        <ol aria-label={t('identity.recoveryPhrase')} className="grid grid-cols-2 gap-x-4 gap-y-1 rounded-md border border-border bg-muted/30 p-4 font-mono text-xs sm:grid-cols-3 md:grid-cols-4">{recoveryMaterial.mnemonic.split(' ').map((word, index) => <li key={`${index}-${word}`}><span className="mr-2 text-muted-foreground">{index + 1}.</span>{word}</li>)}</ol>
        <Button type="button" variant="outline" onClick={downloadRecoveryFile}><Download className="h-4 w-4" />{t('identity.downloadRecovery')}</Button>
        <label className="flex items-start gap-2 text-sm"><input type="checkbox" checked={confirmedSaved} onChange={event => setConfirmedSaved(event.target.checked)} /><span>{t('identity.savedRecoveryConfirm')}</span></label>
        <Button type="button" disabled={!confirmedSaved || !isSecurePasskeyContext() || pending} onClick={() => void registerInitialPasskey()}><KeyRound className="h-4 w-4" />{t('identity.createPasskey')}</Button>
      </> : <Button type="button" disabled={pending} onClick={() => void createMaterial()}><KeyRound className="h-4 w-4" />{t('identity.create')}</Button>}
      <section aria-labelledby="remote-identity-registry-title" className="space-y-3 rounded-md border border-border p-3">
        <div>
          <h3 id="remote-identity-registry-title" className="text-sm font-medium">{t('identity.registryTitle')}</h3>
          <p className="mt-1 text-xs text-muted-foreground">{t('identity.registryHelp')}</p>
        </div>
        {registry.error ? <p role="alert" className="text-sm text-destructive">{errorMessage(registry.error)}</p> : null}
        {registry.isLoading ? <p className="text-xs text-muted-foreground">{t('common.loading')}</p> : null}
        {registry.data?.identities.length ? <ul className="space-y-2">{registry.data.identities.map(item => <li key={item.identity_id} className="rounded bg-muted/40 p-2 text-xs">
          <p className="break-all font-mono">{item.identity_id}</p>
          <p>{t('identity.registrySequence', { sequence: item.sequence })} · {new Date(item.verified_at).toLocaleString()}</p>
          <p className="break-all font-mono text-muted-foreground">{item.head_hash}</p>
          <p>{t(item.frozen ? 'identity.registryFrozen' : 'identity.registryActive', { count: item.fork_count })} · {t('identity.registryNodes', { count: item.node_ids.length })}</p>
        </li>)}</ul> : !registry.isLoading ? <p className="text-xs text-muted-foreground">{t('identity.registryEmpty')}</p> : null}
        <label className="block space-y-1.5 text-sm">{t('identity.registryFile')}<Input type="file" accept="application/json,.json" onChange={event => setRegistryHistoryFile(event.target.files?.[0] ?? null)} /></label>
        <Button type="button" variant="outline" disabled={pending || !registryHistoryFile} onClick={() => void importRegistryHistory()}>{t('identity.registryImport')}</Button>
      </section>
      {error && <p role="alert" className="text-sm text-destructive">{error}</p>}
      {message && <p role="status" className="text-sm text-emerald-700 dark:text-emerald-400">{message}</p>}
    </CardContent>
  </Card>
}
