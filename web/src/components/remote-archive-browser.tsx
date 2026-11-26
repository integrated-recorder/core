import { useCallback, useEffect, useRef, useState } from 'react'
import { useMutation, useQuery } from '@tanstack/react-query'
import { globalIdentityAPI, peerArchivesAPI } from '@/api'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { errorMessage } from '@/lib/errors'
import { useI18n } from '@/i18n/provider'
import { decodeRecoveryFileBlob, deriveRecoveryKeys, recoveryEntropyFromMnemonic } from '@/identity/recovery'
import { createSessionDEKKeyPair, openArchiveDEK, openSessionDEKEnvelope, type KeyEnvelope, type SessionDEKBinding } from '@/identity/cryptoarchive'
import { createPeerArchiveFragmentLoader, createPeerPlaybackPlan, decryptPeerPlaybackObject, type PeerPlaybackPlan } from '@/identity/peer-playback'
import { serializeAssertionCredential, webAuthnRequestOptions, base64UrlBytes } from '@/identity/webauthn'
import type { PeerArchiveCapabilities, PeerArchiveHead, ReplicaManifest } from '@/types/api'

const maxPeerArchiveHeadFanout = 16

type ArchiveGrant = PeerArchiveCapabilities['capabilities'][number]
type ResolvedArchiveHead = PeerArchiveHead & { peerIdentityID: string; checkedReplicaCount: number; unavailableReplicaCount: number }

function uniquePeerSessions(sessions: PeerArchiveCapabilities['sessions']) {
  const peers = new Map<string, PeerArchiveCapabilities['sessions'][number]>()
  for (const session of sessions) {
    const existing = peers.get(session.node_id)
    if (existing && existing.identity_id !== session.identity_id) {
      throw new Error('A connected Node ID is associated with multiple Global Identities.')
    }
    peers.set(session.node_id, session)
  }
  return [...peers.values()].sort((left, right) => left.node_id.localeCompare(right.node_id))
}

async function resolvePeerArchiveHead(
  sessions: PeerArchiveCapabilities['sessions'],
  grant: ArchiveGrant,
): Promise<ResolvedArchiveHead> {
  const peers = uniquePeerSessions(sessions)
  if (peers.length === 0) throw new Error('No connected archive replica is available.')
  if (peers.length > maxPeerArchiveHeadFanout) throw new Error('Too many connected archive replicas to verify safely.')

  const results = await Promise.allSettled(peers.map(async session => {
    const response = await peerArchivesAPI.head(session.node_id, grant.owner_subject_id, grant.archive_id, grant.grant_hash)
    if (response.node_id !== session.node_id) throw new TypeError('peer archive response identity does not match the requested Node')
    return { response, peerIdentityID: session.identity_id }
  }))
  const heads = results.flatMap(result => result.status === 'fulfilled' ? [result.value] : [])
  if (heads.length === 0) throw new Error('No connected archive replica returned a verified snapshot.')

  const first = heads[0]
  if (!first) throw new Error('No connected archive replica returned a verified snapshot.')
  const firstSnapshot = first.response.snapshot
  if (heads.some(item => item.response.snapshot.snapshot_id !== firstSnapshot.snapshot_id ||
    item.response.snapshot.content_hash !== firstSnapshot.content_hash ||
    item.response.snapshot.sequence !== firstSnapshot.sequence)) {
    throw new Error('Connected archive replicas disagree about the current immutable snapshot.')
  }
  return {
    ...first.response,
    peerIdentityID: first.peerIdentityID,
    checkedReplicaCount: heads.length,
    unavailableReplicaCount: results.length - heads.length,
  }
}

async function resolvePeerArchiveSnapshot(
  sessions: PeerArchiveCapabilities['sessions'],
  grant: ArchiveGrant,
  snapshotID: string,
  expectedSequence: number,
): Promise<ResolvedArchiveHead> {
  const peers = uniquePeerSessions(sessions)
  if (peers.length === 0 || peers.length > maxPeerArchiveHeadFanout) throw new Error('No connected archive replica is available.')

  const results = await Promise.allSettled(peers.map(async session => {
    const response = await peerArchivesAPI.snapshot(session.node_id, grant.owner_subject_id, grant.archive_id, grant.grant_hash, snapshotID)
    if (response.node_id !== session.node_id || response.snapshot.snapshot_id !== snapshotID ||
      response.snapshot.sequence !== expectedSequence || response.snapshot.content.archive_id !== grant.archive_id) {
      throw new TypeError('peer archive snapshot does not match its immutable predecessor reference')
    }
    return { response, peerIdentityID: session.identity_id }
  }))
  const snapshots = results.flatMap(result => result.status === 'fulfilled' ? [result.value] : [])
  if (snapshots.length === 0) throw new Error('No connected archive replica returned the requested historical snapshot.')
  const first = snapshots[0]
  if (!first) throw new Error('No connected archive replica returned the requested historical snapshot.')
  if (snapshots.some(item => item.response.snapshot.content_hash !== first.response.snapshot.content_hash ||
    item.response.snapshot.sequence !== first.response.snapshot.sequence)) {
    throw new Error('Connected archive replicas disagree about the immutable snapshot history.')
  }
  return { ...first.response, peerIdentityID: first.peerIdentityID, checkedReplicaCount: snapshots.length, unavailableReplicaCount: results.length - snapshots.length }
}

export function RemoteArchiveBrowser() {
  const { t } = useI18n()
  const [nodeID, setNodeID] = useState('')
  const [selectedSnapshot, setSelectedSnapshot] = useState<ResolvedArchiveHead | null>(null)
  const [snapshotLoading, setSnapshotLoading] = useState(false)
  const [grantHash, setGrantHash] = useState('')
  const [playbackKeyEpoch, setPlaybackKeyEpoch] = useState('')
  const [recoveryPhrase, setRecoveryPhrase] = useState('')
  const [recoveryFile, setRecoveryFile] = useState<File | null>(null)
  const [trackID, setTrackID] = useState('')
  const [playbackPlan, setPlaybackPlan] = useState<PeerPlaybackPlan | null>(null)
  const [playbackError, setPlaybackError] = useState('')
  const [playbackMessage, setPlaybackMessage] = useState('')
  const [peerRecoveryPending, setPeerRecoveryPending] = useState(false)
  const videoRef = useRef<HTMLVideoElement>(null)
  const hlsRef = useRef<{ destroy: () => void } | null>(null)
  const playlistURLRef = useRef('')
  const dekRef = useRef<Uint8Array | null>(null)
  const playbackScopeRef = useRef<{ identityID: string; nodeID: string; ownerSubjectID: string; archiveID: string; grantHash: string } | null>(null)
  const peerRecoveryScopeRef = useRef<{ identityID: string; nodeID: string; ownerSubjectID: string; archiveID: string; grantHash: string } | null>(null)
  const headRequestRef = useRef(0)
  const capabilities = useQuery({
    queryKey: ['peerArchives', 'capabilities'],
    queryFn: peerArchivesAPI.capabilities,
    staleTime: 0,
    refetchInterval: 5_000,
    refetchIntervalInBackground: false,
    retry: false,
  })
  const head = useMutation({
    mutationFn: (input: { sessions: PeerArchiveCapabilities['sessions']; grant: ArchiveGrant }) =>
      resolvePeerArchiveHead(input.sessions, input.grant),
  })
  const sessions = capabilities.data?.sessions ?? []
  const grants = capabilities.data?.capabilities ?? []
  const selectedGrant = grants.find(item => item.grant_hash === grantHash) ?? grants[0]
  const selectedNodeSessions = sessions.filter(session => session.node_id === nodeID)
  const selectedPeerConnected = Boolean(selectedSnapshot && selectedNodeSessions.length > 0 &&
    new Set(selectedNodeSessions.map(session => session.identity_id)).size === 1 &&
    selectedNodeSessions[0]?.identity_id === selectedSnapshot.peerIdentityID)
  const selectedGrantActive = Boolean(selectedGrant && grants.some(grant => grant.owner_subject_id === selectedGrant.owner_subject_id &&
    grant.archive_id === selectedGrant.archive_id && grant.grant_hash === selectedGrant.grant_hash && Date.parse(grant.expires_at) > Date.now()))
  const canStartPlayback = !capabilities.error && selectedPeerConnected && selectedGrantActive && Boolean(selectedSnapshot && nodeID)

  const clearPlayback = useCallback(() => {
    playbackScopeRef.current = null
    hlsRef.current?.destroy()
    hlsRef.current = null
    if (videoRef.current) {
      videoRef.current.pause()
      videoRef.current.removeAttribute('src')
      videoRef.current.load()
    }
    if (playlistURLRef.current) URL.revokeObjectURL(playlistURLRef.current)
    playlistURLRef.current = ''
    dekRef.current?.fill(0)
    dekRef.current = null
    peerRecoveryScopeRef.current = null
    setPeerRecoveryPending(false)
    setPlaybackPlan(null)
    setTrackID('')
    setPlaybackKeyEpoch('')
  }, [])

  useEffect(() => {
    const scope = playbackScopeRef.current
    if (!scope) {
      if (peerRecoveryPending) {
        if (capabilities.error) {
          setPlaybackError(t('identity.remotePlaybackAuthorizationUnavailable'))
          return
        }
        const pendingScope = peerRecoveryScopeRef.current
        const sessionsForPeer = pendingScope ? (capabilities.data?.sessions ?? []).filter(session => session.node_id === pendingScope.nodeID) : []
        const peerIdentityIDs = new Set(sessionsForPeer.map(session => session.identity_id))
        const peerReturned = Boolean(pendingScope && peerIdentityIDs.size === 1 && peerIdentityIDs.has(pendingScope.identityID))
        const capabilityStillActive = Boolean(pendingScope && (capabilities.data?.capabilities ?? []).some(grant =>
          grant.owner_subject_id === pendingScope.ownerSubjectID && grant.archive_id === pendingScope.archiveID &&
          grant.grant_hash === pendingScope.grantHash && Date.parse(grant.expires_at) > Date.now()))
        if (!capabilityStillActive) {
          setPeerRecoveryPending(false)
          peerRecoveryScopeRef.current = null
          setPlaybackError(t('identity.remotePlaybackGrantEnded'))
        } else if (peerIdentityIDs.size > 1 || (peerIdentityIDs.size === 1 && !peerIdentityIDs.has(pendingScope?.identityID ?? ''))) {
          setPeerRecoveryPending(false)
          peerRecoveryScopeRef.current = null
          setPlaybackError(t('identity.remotePlaybackPeerAmbiguous'))
        } else if (peerReturned) {
          setPeerRecoveryPending(false)
          peerRecoveryScopeRef.current = null
          setPlaybackError('')
          setPlaybackMessage(t('identity.remotePlaybackPeerReconnected'))
        }
      }
      return
    }

    const grantStillActive = (capabilities.data?.capabilities ?? []).some(grant =>
      grant.owner_subject_id === scope.ownerSubjectID && grant.archive_id === scope.archiveID &&
      grant.grant_hash === scope.grantHash && Date.parse(grant.expires_at) > Date.now())
    const peerStillConnected = (capabilities.data?.sessions ?? []).some(session =>
      session.identity_id === scope.identityID && session.node_id === scope.nodeID) &&
      new Set((capabilities.data?.sessions ?? []).filter(session => session.node_id === scope.nodeID).map(session => session.identity_id)).size === 1
    const hasPeerSessions = (capabilities.data?.sessions ?? []).some(session => session.node_id === scope.nodeID)
    if (capabilities.error) {
      clearPlayback()
      setPlaybackMessage('')
      setPeerRecoveryPending(false)
      setPlaybackError(t('identity.remotePlaybackAuthorizationUnavailable'))
    } else if (!grantStillActive) {
      clearPlayback()
      setPlaybackMessage('')
      setPeerRecoveryPending(false)
      setPlaybackError(t('identity.remotePlaybackGrantEnded'))
    } else if (!peerStillConnected) {
      clearPlayback()
      setPlaybackMessage('')
      if (hasPeerSessions) {
        setPeerRecoveryPending(false)
        setPlaybackError(t('identity.remotePlaybackPeerAmbiguous'))
      } else {
        peerRecoveryScopeRef.current = scope
        setPeerRecoveryPending(true)
        setPlaybackError(t('identity.remotePlaybackPeerDisconnected'))
      }
    }
  }, [capabilities.data, capabilities.dataUpdatedAt, capabilities.error, clearPlayback, peerRecoveryPending, playbackPlan, t])

  useEffect(() => () => {
    headRequestRef.current++
    hlsRef.current?.destroy()
    if (videoRef.current) { videoRef.current.pause(); videoRef.current.removeAttribute('src') }
    if (playlistURLRef.current) URL.revokeObjectURL(playlistURLRef.current)
    dekRef.current?.fill(0)
    dekRef.current = null
  }, [])

  const loadSnapshot = () => {
    if (!selectedGrant || sessions.length === 0) return
    clearPlayback()
    setSnapshotLoading(false)
    setSelectedSnapshot(null)
    setNodeID('')
    setPlaybackError('')
    setPlaybackMessage('')
    head.reset()
    const requestID = ++headRequestRef.current
    void head.mutateAsync({ sessions, grant: selectedGrant })
      .then(result => {
        if (requestID !== headRequestRef.current) return
        setSelectedSnapshot(result)
        setNodeID(result.node_id)
      })
      .catch(cause => {
        if (requestID !== headRequestRef.current) return
        const message = cause instanceof Error && cause.message.includes('disagree')
          ? t('identity.remoteArchiveReplicaConflict') : errorMessage(cause)
        setPlaybackError(message)
      })
  }

  const loadPreviousSnapshot = () => {
    const previousID = selectedSnapshot?.snapshot.previous_snapshot_id
    const expectedSequence = selectedSnapshot ? selectedSnapshot.snapshot.sequence - 1 : 0
    if (!previousID || expectedSequence < 1 || !selectedGrant || sessions.length === 0 || snapshotLoading) return
    clearPlayback()
    setPlaybackError('')
    setPlaybackMessage('')
    const requestID = ++headRequestRef.current
    setSnapshotLoading(true)
    void resolvePeerArchiveSnapshot(sessions, selectedGrant, previousID, expectedSequence)
      .then(result => {
        if (requestID !== headRequestRef.current) return
        setSelectedSnapshot(result)
        setNodeID(result.node_id)
      })
      .catch(cause => {
        if (requestID !== headRequestRef.current) return
        setPlaybackError(cause instanceof Error && cause.message.includes('disagree')
          ? t('identity.remoteArchiveReplicaConflict') : errorMessage(cause))
      })
      .finally(() => { if (requestID === headRequestRef.current) setSnapshotLoading(false) })
  }

  const installPlayback = async (dek: Uint8Array, epoch: string, verifiedManifest?: ReplicaManifest, keySource: 'recovery' | 'passkey' = 'recovery') => {
    const snapshot = selectedSnapshot?.snapshot
    const grant = selectedGrant
    if (!snapshot || !grant || !nodeID) throw new Error(t('identity.remotePlaybackUnavailable'))
    const manifest = verifiedManifest ?? await peerArchivesAPI.manifest(nodeID, grant.owner_subject_id, grant.archive_id, grant.grant_hash, snapshot.snapshot_id, epoch)
    const plan = createPeerPlaybackPlan(snapshot, manifest)
    const firstTrack = plan.tracks[0]
    if (!firstTrack) throw new TypeError('remote archive has no playable tracks')
    const video = videoRef.current
    if (!video) throw new Error(t('identity.remotePlaybackUnavailable'))
    const HlsModule = (await import('hls.js')).default
    if (!HlsModule.isSupported()) throw new Error(t('identity.remotePlaybackMSEUnsupported'))
    const scope = {
      nodeID,
      ownerSubjectID: grant.owner_subject_id,
      archiveID: grant.archive_id,
      grantHash: grant.grant_hash,
      snapshotID: snapshot.snapshot_id,
      keyEpoch: epoch,
    }
    const loadObject = (objectID: string, signal: AbortSignal) => {
      const object = plan.objects.get(objectID)
      if (!object) throw new TypeError('remote archive object is not in the verified manifest')
      return decryptPeerPlaybackObject(object, dek, (requestedID, offset, length, abortSignal) => {
        if (requestedID !== objectID) throw new TypeError('remote archive object reference changed')
        return peerArchivesAPI.ciphertextRange(scope.nodeID, scope.ownerSubjectID, scope.archiveID, scope.grantHash, scope.snapshotID, scope.keyEpoch, objectID, offset, length, abortSignal)
      }, signal)
    }
    const hls = new HlsModule({ enableWorker: true, lowLatencyMode: false, maxBufferLength: 20, fLoader: createPeerArchiveFragmentLoader(loadObject) })
    const playlistURL = URL.createObjectURL(new Blob([plan.playlist(firstTrack)], { type: 'application/vnd.apple.mpegurl' }))
    playlistURLRef.current = playlistURL
    dekRef.current = dek
    playbackScopeRef.current = {
      identityID: selectedSnapshot.peerIdentityID,
      nodeID,
      ownerSubjectID: grant.owner_subject_id,
      archiveID: grant.archive_id,
      grantHash: grant.grant_hash,
    }
    setPlaybackKeyEpoch(epoch)
    setPlaybackPlan(plan)
    setTrackID(firstTrack)
    hlsRef.current = hls
    hls.attachMedia(video)
    hls.on(HlsModule.Events.ERROR, (_event, data) => {
      if (data.fatal) {
        dekRef.current?.fill(0)
        dekRef.current = null
        setPlaybackError(t('identity.remotePlaybackFailed'))
      }
    })
    hls.loadSource(playlistURL)
    setPlaybackMessage(t(keySource === 'passkey' ? 'identity.remotePlaybackPasskeyReady' : 'identity.remotePlaybackReady'))
  }

  const startPlayback = async () => {
    if (!canStartPlayback || !nodeID || !selectedGrant || !selectedSnapshot) return
    clearPlayback()
    setPlaybackError('')
    setPlaybackMessage('')
    let entropy: Uint8Array | null = null
    let keys: Awaited<ReturnType<typeof deriveRecoveryKeys>> | null = null
    let dek: Uint8Array | null = null
    try {
      if (!window.isSecureContext || !globalThis.crypto?.subtle) throw new Error(t('identity.remotePlaybackNeedsHTTPS'))
      if (!recoveryFile && !recoveryPhrase.trim()) throw new Error(t('identity.remotePlaybackRecoveryRequired'))
      entropy = recoveryFile
        ? await decodeRecoveryFileBlob(recoveryFile)
        : recoveryEntropyFromMnemonic(recoveryPhrase)
      keys = await deriveRecoveryKeys(entropy)
      entropy.fill(0)
      entropy = null
      const matchingSessions = sessions.filter(item => item.node_id === nodeID)
      if (new Set(matchingSessions.map(item => item.identity_id)).size !== 1 ||
        matchingSessions[0]?.identity_id !== selectedSnapshot.peerIdentityID ||
        keys.identityId !== selectedSnapshot.peerIdentityID) {
        throw new Error(t('identity.remotePlaybackWrongRecovery'))
      }
      const snapshot = selectedSnapshot.snapshot
      const keyEpochResponse = await peerArchivesAPI.keyEpochs(nodeID, selectedGrant.owner_subject_id, selectedGrant.archive_id, selectedGrant.grant_hash, snapshot.snapshot_id)
      if (keyEpochResponse.node_id !== nodeID || keyEpochResponse.archive_id !== snapshot.content.archive_id || keyEpochResponse.snapshot_id !== snapshot.snapshot_id ||
        !Array.isArray(keyEpochResponse.key_epochs) || keyEpochResponse.key_epochs.length === 0 || keyEpochResponse.key_epochs.length > 64) {
        throw new Error(t('identity.remotePlaybackNoKeyEpoch'))
      }
      let manifest: ReplicaManifest | null = null
      let epoch = ''
      let previousEpoch = BigInt(Number.MAX_SAFE_INTEGER)
      for (const [index, candidate] of keyEpochResponse.key_epochs.entries()) {
        if (!/^[1-9][0-9]{0,19}$/u.test(candidate)) throw new TypeError('remote archive key epoch list is invalid')
        const candidateEpoch = BigInt(candidate)
        if (index > 0 && candidateEpoch >= previousEpoch) throw new TypeError('remote archive key epoch list is invalid')
        previousEpoch = candidateEpoch
        const [candidateManifest, envelope] = await Promise.all([
          peerArchivesAPI.manifest(nodeID, selectedGrant.owner_subject_id, selectedGrant.archive_id, selectedGrant.grant_hash, snapshot.snapshot_id, candidate),
          peerArchivesAPI.keyEnvelope(nodeID, selectedGrant.owner_subject_id, selectedGrant.archive_id, selectedGrant.grant_hash, snapshot.snapshot_id, candidate),
        ])
        if (candidateManifest.snapshot_id !== snapshot.snapshot_id || candidateManifest.archive_id !== snapshot.content.archive_id || candidateManifest.key_epoch !== candidate) {
          throw new TypeError('remote replica manifest binding is invalid')
        }
        try {
          dek = await openArchiveDEK(envelope as KeyEnvelope, {
            archiveId: snapshot.content.archive_id,
            keyEpoch: candidate,
            signerKeyId: keys.signerKeyId,
            signerPublicKey: keys.signingPublicKey,
            recipientPrivateKey: keys.encryptionPrivateKey,
          })
          manifest = candidateManifest
          epoch = candidate
          break
        } catch {
          dek?.fill(0)
          dek = null
        }
      }
      if (!manifest || !dek || !epoch) throw new Error(t('identity.remotePlaybackNoKeyEpoch'))
      await installPlayback(dek, epoch, manifest)
      dek = null
      setRecoveryPhrase('')
      setRecoveryFile(null)
    } catch (cause) {
      setPlaybackError(cause instanceof Error && cause.message.startsWith('remote archive')
        ? t('identity.remotePlaybackFailed') : errorMessage(cause))
      const failedDEK = dek as Uint8Array | null
      failedDEK?.fill(0)
      dek = null
    } finally {
      entropy?.fill(0)
      keys?.destroy()
    }
  }

  const startPlaybackWithPasskey = async () => {
    if (!canStartPlayback || !nodeID || !selectedGrant || !selectedSnapshot) return
    clearPlayback()
    setPlaybackError('')
    setPlaybackMessage('')
    let dek: Uint8Array | null = null
    let keyPair: ReturnType<typeof createSessionDEKKeyPair> | null = null
    try {
      if (!window.isSecureContext || !globalThis.crypto?.subtle) throw new Error(t('identity.remotePlaybackNeedsHTTPS'))
      const activeKeyPair = createSessionDEKKeyPair()
      keyPair = activeKeyPair
      const options = await globalIdentityAPI.loginOptions(base64UrlBytes(activeKeyPair.publicKey))
      const credential = await navigator.credentials.get({ publicKey: webAuthnRequestOptions(options) })
      await globalIdentityAPI.loginFinish(serializeAssertionCredential(credential))
      const passkeySession = await globalIdentityAPI.session()
      if (!passkeySession.authenticated || !passkeySession.identity_id || !passkeySession.global_csrf_token || !passkeySession.playback_key_bound) {
        throw new Error(t('identity.remotePlaybackPasskeyUnavailable'))
      }

      const snapshot = selectedSnapshot.snapshot
      const epochResponse = await globalIdentityAPI.sessionKeyEpochs(
        nodeID, selectedGrant.owner_subject_id, selectedGrant.archive_id, selectedGrant.grant_hash, snapshot.snapshot_id,
      )
      if (epochResponse.node_id !== nodeID || epochResponse.archive_id !== snapshot.content.archive_id ||
        epochResponse.snapshot_id !== snapshot.snapshot_id || !Array.isArray(epochResponse.key_epochs) ||
        epochResponse.key_epochs.length === 0 || epochResponse.key_epochs.length > 64) {
        throw new Error(t('identity.remotePlaybackNoKeyEpoch'))
      }
      let previousEpoch = BigInt(Number.MAX_SAFE_INTEGER)
      for (const [index, candidate] of epochResponse.key_epochs.entries()) {
        if (!/^[1-9][0-9]{0,19}$/u.test(candidate)) throw new TypeError('remote archive key epoch list is invalid')
        const parsedEpoch = BigInt(candidate)
        if (index > 0 && parsedEpoch >= previousEpoch) throw new TypeError('remote archive key epoch list is invalid')
        previousEpoch = parsedEpoch
      }

      const epoch = epochResponse.key_epochs[0]
      if (!epoch) throw new Error(t('identity.remotePlaybackNoKeyEpoch'))
      const [manifest, envelope] = await Promise.all([
        peerArchivesAPI.manifest(nodeID, selectedGrant.owner_subject_id, selectedGrant.archive_id, selectedGrant.grant_hash, snapshot.snapshot_id, epoch),
        globalIdentityAPI.sessionDEKEnvelope(
          nodeID, selectedGrant.owner_subject_id, selectedGrant.archive_id, selectedGrant.grant_hash,
          snapshot.snapshot_id, epoch, passkeySession.global_csrf_token,
        ),
      ])
      if (manifest.snapshot_id !== snapshot.snapshot_id || manifest.archive_id !== snapshot.content.archive_id || manifest.key_epoch !== epoch) {
        throw new TypeError('remote replica manifest binding is invalid')
      }
      const binding: SessionDEKBinding = {
        archiveId: snapshot.content.archive_id,
        snapshotId: snapshot.snapshot_id,
        keyEpoch: epoch,
        ownerSubjectId: selectedGrant.owner_subject_id,
        grantHash: selectedGrant.grant_hash,
        supplierNodeId: nodeID,
      }
      dek = await openSessionDEKEnvelope(envelope, activeKeyPair.privateKey, binding)
      await installPlayback(dek, epoch, manifest, 'passkey')
      dek = null
    } catch (cause) {
      setPlaybackError(cause instanceof Error && cause.message.startsWith('remote archive')
        ? t('identity.remotePlaybackFailed') : errorMessage(cause))
      const failedDEK = dek as Uint8Array | null
      failedDEK?.fill(0)
      dek = null
    } finally {
      keyPair?.destroy()
    }
  }

  const changeTrack = (value: string) => {
    if (!playbackPlan || !dekRef.current || !videoRef.current) return
    setPlaybackError('')
    hlsRef.current?.destroy()
    hlsRef.current = null
    if (playlistURLRef.current) URL.revokeObjectURL(playlistURLRef.current)
    const playlistURL = URL.createObjectURL(new Blob([playbackPlan.playlist(value)], { type: 'application/vnd.apple.mpegurl' }))
    playlistURLRef.current = playlistURL
    void import('hls.js').then(({ default: HlsModule }) => {
      if (!dekRef.current || !videoRef.current) return
      const selectedGrantNow = selectedGrant
      const snapshotNow = selectedSnapshot?.snapshot
      if (!selectedGrantNow || !snapshotNow) return
      const scope = { nodeID, ownerSubjectID: selectedGrantNow.owner_subject_id, archiveID: selectedGrantNow.archive_id, grantHash: selectedGrantNow.grant_hash, snapshotID: snapshotNow.snapshot_id, keyEpoch: playbackKeyEpoch }
      const loadObject = (objectID: string, signal: AbortSignal) => {
        const object = playbackPlan.objects.get(objectID)
        const currentDEK = dekRef.current
        if (!object || !currentDEK) throw new TypeError('remote archive object is unavailable')
        return decryptPeerPlaybackObject(object, currentDEK, (requestedID, offset, length, abortSignal) => {
          if (requestedID !== objectID) throw new TypeError('remote archive object reference changed')
          return peerArchivesAPI.ciphertextRange(scope.nodeID, scope.ownerSubjectID, scope.archiveID, scope.grantHash, scope.snapshotID, scope.keyEpoch, objectID, offset, length, abortSignal)
        }, signal)
      }
      if (!HlsModule.isSupported()) { setPlaybackError(t('identity.remotePlaybackMSEUnsupported')); return }
      const hls = new HlsModule({ enableWorker: true, lowLatencyMode: false, maxBufferLength: 20, fLoader: createPeerArchiveFragmentLoader(loadObject) })
      hlsRef.current = hls
      hls.attachMedia(videoRef.current)
      hls.on(HlsModule.Events.ERROR, (_event, data) => {
        if (data.fatal) {
          dekRef.current?.fill(0)
          dekRef.current = null
          setPlaybackError(t('identity.remotePlaybackFailed'))
        }
      })
      hls.loadSource(playlistURL)
    }).catch(() => setPlaybackError(t('identity.remotePlaybackFailed')))
    setTrackID(value)
  }

  if (capabilities.isLoading) return null
  if (capabilities.error && !playbackPlan) return <Card className="mb-4"><CardContent className="pt-5 text-sm text-destructive" role="alert">{errorMessage(capabilities.error)}</CardContent></Card>
  if (grants.length === 0 && !playbackPlan) {
    return playbackError
      ? <Card className="mb-4"><CardContent className="pt-5 text-sm text-destructive" role="alert">{playbackError}</CardContent></Card>
      : null
  }

  return <Card className="mb-4">
    <CardHeader><CardTitle>{t('identity.remoteArchiveBrowse')}</CardTitle><p className="text-xs text-muted-foreground">{t('identity.remoteArchiveHelp')}</p></CardHeader>
    <CardContent className="space-y-3">
      <div className="grid gap-3">
        <label className="block space-y-1.5 text-sm">{t('identity.remoteArchiveCapability')}<select className="flex h-9 w-full rounded-md border border-input bg-background px-3 py-1 text-sm" value={selectedGrant?.grant_hash ?? ''} onChange={event => { headRequestRef.current++; clearPlayback(); setSnapshotLoading(false); setSelectedSnapshot(null); setNodeID(''); setPlaybackError(''); setGrantHash(event.target.value); head.reset() }}>
          {grants.map(item => <option key={item.grant_hash} value={item.grant_hash}>{item.archive_id} · {item.owner_subject_id}</option>)}
        </select></label>
      </div>
      {sessions.length === 0 && <p className="text-xs text-muted-foreground">{t('identity.remoteArchiveNoConnectedReplicas')}</p>}
      <Button type="button" variant="outline" size="sm" disabled={sessions.length === 0 || !selectedGrant || head.isPending} onClick={loadSnapshot}>
        {head.isPending ? t('common.loading') : t('identity.remoteArchiveLoad')}
      </Button>
      {playbackError && !selectedSnapshot && <p role="alert" className="text-sm text-destructive">{playbackError}</p>}
      {selectedSnapshot && <>
        {selectedSnapshot.unavailableReplicaCount ? <p role="status" className="text-xs text-amber-700 dark:text-amber-300">{t('identity.remoteArchivePartialReplicas', { checked: selectedSnapshot.checkedReplicaCount, total: selectedSnapshot.checkedReplicaCount + selectedSnapshot.unavailableReplicaCount })}</p> : null}
        <div className="max-h-80 space-y-2 overflow-y-auto rounded-md border border-border bg-muted/30 p-3 text-xs">
          <p>{t('identity.remoteArchiveSequence', { sequence: selectedSnapshot.snapshot.sequence, count: selectedSnapshot.snapshot.content.objects.length })}</p>
          <p className="break-all font-mono">{selectedSnapshot.snapshot.snapshot_id}</p>
          <div className="flex flex-wrap gap-2">
            {selectedSnapshot.snapshot.previous_snapshot_id && <Button type="button" variant="outline" size="sm" disabled={snapshotLoading || head.isPending} onClick={loadPreviousSnapshot}>{snapshotLoading ? t('common.loading') : t('identity.remoteArchivePrevious')}</Button>}
            {head.data && selectedSnapshot.snapshot.snapshot_id !== head.data.snapshot.snapshot_id && <Button type="button" variant="outline" size="sm" disabled={snapshotLoading || head.isPending} onClick={loadSnapshot}>{t('identity.remoteArchiveLatest')}</Button>}
          </div>
          <ol aria-label={t('identity.remoteArchiveObjects')} className="space-y-1">
            {selectedSnapshot.snapshot.content.objects.slice(-24).map((object, index) => <li key={`${object.coordinate.session_id}-${object.coordinate.track_id}-${object.coordinate.source_epoch}-${object.coordinate.discontinuity_sequence}-${object.coordinate.sequence}-${index}`} className="rounded border border-border/60 px-2 py-1">
              <span className="font-mono">{object.coordinate.track_id} · {object.coordinate.source_epoch}:{object.coordinate.discontinuity_sequence}:{object.coordinate.sequence}</span>
              <span className="ml-2 text-muted-foreground">#{object.timeline_ordinal} · {object.duration.toFixed(2)}s</span>
            </li>)}
          </ol>
        </div>
        <div className="space-y-3 rounded-md border border-border p-3">
          <p className="text-sm font-medium">{t('identity.remotePlaybackTitle')}</p>
          <p className="text-xs text-muted-foreground">{t('identity.remotePlaybackHelp')}</p>
          <p className="text-xs text-muted-foreground">{t('identity.remotePlaybackPasskeyHelp')}</p>
          {playbackKeyEpoch && <p role="status" className="text-xs text-muted-foreground">{t('identity.remotePlaybackSelectedEpoch', { epoch: playbackKeyEpoch })}</p>}
          <label className="block space-y-1.5 text-sm">{t('identity.remotePlaybackRecoveryPhrase')}<textarea className="min-h-20 w-full rounded-md border border-input bg-background px-3 py-2 font-mono text-xs" value={recoveryPhrase} onChange={event => { setRecoveryPhrase(event.target.value); setRecoveryFile(null) }} autoComplete="off" spellCheck={false} /></label>
          <label className="block space-y-1.5 text-sm">{t('identity.remotePlaybackRecoveryFile')}<Input type="file" accept=".irrc,application/octet-stream" onChange={event => { setRecoveryFile(event.target.files?.[0] ?? null); if (event.target.files?.[0]) setRecoveryPhrase('') }} /></label>
          <div className="flex flex-wrap gap-2">
            <Button type="button" size="sm" disabled={Boolean(playbackPlan) || !canStartPlayback || head.isPending || (!recoveryFile && !recoveryPhrase.trim())} onClick={() => void startPlayback()}>{t('identity.remotePlaybackStart')}</Button>
            <Button type="button" size="sm" variant="outline" disabled={Boolean(playbackPlan) || !canStartPlayback || head.isPending} onClick={() => void startPlaybackWithPasskey()}>{t('identity.remotePlaybackPasskey')}</Button>
            {playbackPlan && <>
              {playbackPlan.tracks.length > 1 && <label className="flex items-center gap-2 text-sm">{t('identity.remotePlaybackTrack')}<select aria-label={t('identity.remotePlaybackTrack')} className="h-9 rounded-md border border-input bg-background px-3" value={trackID} onChange={event => changeTrack(event.target.value)}>{playbackPlan.tracks.map(track => <option key={track} value={track}>{track}</option>)}</select></label>}
              <Button type="button" size="sm" variant="outline" onClick={clearPlayback}>{t('identity.remotePlaybackStop')}</Button>
            </>}
          </div>
          {playbackMessage && <p role="status" className="text-xs text-muted-foreground">{playbackMessage}</p>}
          {playbackError && <p role="alert" className="text-sm text-destructive">{playbackError}</p>}
      <video ref={videoRef} controls playsInline hidden={!playbackPlan} className="aspect-video w-full rounded-md bg-black" />
        </div>
      </>}
    </CardContent>
  </Card>
}
