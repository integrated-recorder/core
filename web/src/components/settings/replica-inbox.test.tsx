// @vitest-environment-options {"url":"https://localhost"}
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { GlobalIdentityPanel } from './global-identity'

describe('replica offer inbox UI', () => {
  afterEach(() => { vi.restoreAllMocks(); vi.unstubAllGlobals() })

  it('finds scoped pending offers and only fills the explicit import form after selection', async () => {
    const identityID = `gi_${'I'.repeat(22)}`
    const sourceIdentityID = `gi_${'X'.repeat(22)}`
    const localNodeID = `nd_${'L'.repeat(22)}`
    const sourceNodeID = `nd_${'S'.repeat(22)}`
    const subjectID = `os_${'O'.repeat(22)}`
    const archiveID = `arc_${'A'.repeat(22)}`
    const snapshotID = `snap_${'B'.repeat(64)}`
    const leaseID = `rl_${'C'.repeat(22)}`
    const grantHash = `sha256:${'d'.repeat(64)}`
    const offerRequests: string[] = []
    const importRequests: string[] = []
    let ambiguousPeerSession = false
    const browserWindow = window
    Object.defineProperty(browserWindow, 'isSecureContext', { configurable: true, value: true })
    Object.defineProperty(navigator, 'credentials', { configurable: true, value: {} })
    vi.stubGlobal('window', new Proxy(browserWindow, {
      get(target, property) {
        if (property === 'isSecureContext') return true
        if (property === 'location') return { ...target.location, protocol: 'https:' }
        return Reflect.get(target, property, target)
      },
    }))

    vi.stubGlobal('fetch', vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const path = String(input)
      let payload: unknown
      if (path === '/api/global-identity/status') payload = {
        initialized: true,
        identity: { identity_id: identityID, sequence: '2', head_hash: `sha256:${'a'.repeat(64)}`, created_at: new Date().toISOString(), credential_count: 1, credential_ids: ['credential'], node_ids: [localNodeID, sourceNodeID], frozen: false, fork_count: 0 },
      }
      else if (path === '/api/global-identity/registry') payload = { identities: [] }
      else if (path === '/api/global-identity/session') payload = { authenticated: false }
      else if (path === '/api/node-identity/status') payload = { created: true, node: { node_id: localNodeID, public_key: 'AQID' } }
      else if (path === '/api/owner-authority/subjects') payload = {
        subjects: [{
          subject: { subject_id: subjectID, subject_type: 'person', controller: { kind: 'identity', id: sourceIdentityID }, controller_policy: 'global_identity_recovery_root/1', policy_epoch: '1', created_at: new Date().toISOString() },
          archive_ids: [archiveID], sequence: '2', head_hash: `sha256:${'b'.repeat(64)}`, frozen: false, fork_count: 0,
          grants: [{ grant_hash: grantHash, subject_id: subjectID, issuer: { kind: 'identity', id: sourceIdentityID }, audience: { kind: 'node', id: localNodeID }, archive_id: archiveID, snapshot_id: snapshotID, actions: ['archive.replicate'], not_before: new Date(Date.now() - 60_000).toISOString(), expires_at: new Date(Date.now() + 60_000).toISOString(), delegation_depth: '0', policy_epoch: '1', issuer_key_id: 'key', revoked: false }],
        }],
      }
      else if (path === '/api/peer-sessions') payload = { sessions: [
        { identity_id: sourceIdentityID, node_id: sourceNodeID, connected_at: new Date().toISOString() },
        ...(ambiguousPeerSession ? [{ identity_id: identityID, node_id: sourceNodeID, connected_at: new Date().toISOString() }] : []),
      ] }
      else if (path === '/api/replica-leases') payload = { leases: [], tombstones: [] }
      else if (path.startsWith(`/api/peer-archives/${sourceNodeID}/replica-offers?`)) {
        offerRequests.push(path)
        payload = { source_node_id: sourceNodeID, archive_id: archiveID, offers: [{ lease_id: leaseID, archive_id: archiveID, snapshot_id: snapshotID, owner_subject_id: subjectID, offer_hash: `sha256:${'e'.repeat(64)}`, key_epoch: '1', expires_at: new Date(Date.now() + 60_000).toISOString(), reserved_bytes: '1024', object_count: 1, grant_hash: grantHash }] }
      }
      else if (path === '/api/replica-imports') {
        importRequests.push(String(init?.body ?? ''))
        payload = { lease: { lease_id: leaseID, status: 'active' }, snapshot_id: snapshotID, objects: 1 }
      }
      else throw new Error(`unexpected API request: ${init?.method ?? 'GET'} ${path}`)
      return new Response(JSON.stringify(payload), { status: 200, headers: { 'Content-Type': 'application/json' } })
    }))

    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    client.setQueryData(['user', 'authorization'], { user_id: 'usr_owner', login: 'owner', roles: ['owner'], permissions: ['system.settings.manage', 'recording.read'] })
    render(<QueryClientProvider client={client}><GlobalIdentityPanel /></QueryClientProvider>)

    await screen.findByRole('option', { name: `${sourceIdentityID} · ${sourceNodeID}` })
    fireEvent.change(await screen.findByLabelText('복제본 원본 Node'), { target: { value: sourceNodeID } })
    expect(screen.getByLabelText('복제본 원본 Node')).toHaveDisplayValue(`${sourceIdentityID} · ${sourceNodeID}`)
    fireEvent.click(screen.getByRole('button', { name: '선택한 Node에서 대기 중인 Offer 찾기' }))

    expect(await screen.findByText(leaseID)).toBeInTheDocument()
    expect(offerRequests).toHaveLength(1)
    expect(offerRequests[0]).toContain(`owner_subject_id=${subjectID}`)
    expect(offerRequests[0]).toContain(`archive_id=${archiveID}`)
    expect(screen.getByLabelText('수신 Snapshot ID')).toHaveValue('')
    expect(screen.getByLabelText('Offer Lease ID')).toHaveValue('')
    expect(importRequests).toHaveLength(0)

    fireEvent.click(screen.getByRole('button', { name: '이 Offer 사용' }))
    await waitFor(() => {
      expect(screen.getByLabelText('수신 Snapshot ID')).toHaveValue(snapshotID)
      expect(screen.getByLabelText('Offer Lease ID')).toHaveValue(leaseID)
    })
    expect(importRequests).toHaveLength(0)
    fireEvent.click(screen.getByRole('button', { name: '서명 Offer 확인 및 암호문 가져오기' }))
    await waitFor(() => expect(importRequests).toHaveLength(1))
    expect(JSON.parse(importRequests[0])).toMatchObject({
      source_node_id: sourceNodeID,
      owner_subject_id: subjectID,
      grant_hash: grantHash,
      archive_id: archiveID,
      snapshot_id: snapshotID,
      lease_id: leaseID,
    })
    ambiguousPeerSession = true
    await client.invalidateQueries({ queryKey: ['peer-sessions'] })
    expect(await screen.findByRole('alert')).toHaveTextContent('같은 Node가 둘 이상의 인증 신원으로 연결되어')
  })
})
