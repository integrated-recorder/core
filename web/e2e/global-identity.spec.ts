import { expect, test } from '@playwright/test'
import { readFile } from 'node:fs/promises'
import { dirname, join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { build } from 'vite'

const webRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..')

async function buildPlaybackAuthorityTestBundle(): Promise<string> {
  const outputs = await build({
    configFile: false,
    root: webRoot,
    logLevel: 'error',
    resolve: { alias: { '@': resolve(webRoot, 'src') } },
    build: {
      write: false,
      emptyOutDir: false,
      lib: {
        entry: resolve(webRoot, 'e2e/browser-playback-authority.ts'),
        name: 'IRPlaybackAuthorityTest',
        formats: ['iife'],
        fileName: 'playback-authority-test',
      },
      rollupOptions: { output: { inlineDynamicImports: true } },
    },
  })
  const chunks = (Array.isArray(outputs) ? outputs : [outputs]).flatMap(output => output.output)
  const bundle = chunks.find(chunk => chunk.type === 'chunk')
  if (!bundle || bundle.type !== 'chunk') throw new Error('browser playback authorization test bundle was not built')
  return bundle.code
}

// This suite exercises WebAuthn. Use the localhost RP origin (not its IP
// address) and pin the Korean locale expected by its labels.
test.use({ baseURL: 'http://localhost:4173', locale: 'ko-KR' })

test('real Go backend: recoverable identity, independent Node enrollment and revocation, passkey recovery', async ({ page, context, browser }) => {
  test.setTimeout(90_000)
  const cdp = await context.newCDPSession(page)
  await cdp.send('WebAuthn.enable')
  const { authenticatorId } = await cdp.send('WebAuthn.addVirtualAuthenticator', {
    options: {
      protocol: 'ctap2',
      transport: 'internal',
      hasResidentKey: true,
      hasUserVerification: true,
      isUserVerified: true,
      automaticPresenceSimulation: true,
    },
  })

  const registrationBodies: Record<string, unknown>[] = []
  const identityFailures: { path: string; status: number; code?: string }[] = []
  const authorityFailures: { status: number; code?: string }[] = []
  const authorityEventSummaries: Record<string, unknown>[] = []
  page.on('request', request => {
    if (request.url().endsWith('/api/global-identity/registration/finish')) {
      registrationBodies.push(request.postDataJSON() as Record<string, unknown>)
    }
    if (request.url().endsWith('/api/owner-authority/events')) {
      const wire = request.postDataJSON() as { event?: Record<string, unknown> }
      if (wire.event) authorityEventSummaries.push(wire.event)
    }
  })
  page.on('response', async response => {
    const path = new URL(response.url()).pathname
    if ((path.startsWith('/api/global-identity/') || path.startsWith('/api/node-identity/')) && response.status() >= 400) {
      const body = await response.json().catch(() => ({})) as { error_code?: string; code?: string }
      identityFailures.push({ path, status: response.status(), code: body.error_code ?? body.code })
    }
    if (path.startsWith('/api/owner-authority/') && response.status() >= 400) {
      const body = await response.json().catch(() => ({})) as { error_code?: string; code?: string }
      authorityFailures.push({ status: response.status(), code: body.error_code ?? body.code })
    }
  })

  await page.goto('/login')
  await page.getByLabel(/Account name|계정 이름/).fill('owner')
  await page.getByLabel(/Password|비밀번호/).fill('browser-e2e-strong-password')
  await page.getByRole('button', { name: /Sign in|로그인/ }).click()
  await expect(page).toHaveURL('/')

  await page.goto('/account/security')
  await page.getByRole('button', { name: '글로벌 신원 만들기' }).click()
  const list = page.getByRole('list', { name: '24개 복구 단어' })
  await expect(list.locator('li')).toHaveCount(24)
  const phraseItems = await list.locator('li').allTextContents()
  const recoveryPhrase = phraseItems.map(item => item.replace(/^\d+\.\s*/u, '')).join(' ')
  expect(recoveryPhrase.trim().split(/\s+/u)).toHaveLength(24)
  await page.getByRole('checkbox').check()
  await page.getByRole('button', { name: '첫 Passkey 등록' }).click()
  if (identityFailures.length) throw new Error(`identity enrollment API failed: ${JSON.stringify(identityFailures)}`)
  await expect(page.getByText('글로벌 신원과 첫 Passkey를 만들었습니다.', { exact: false })).toBeVisible()
  await expect(page.getByText('gi_', { exact: false })).toBeVisible()
  await expect(page.getByRole('list', { name: '24개 복구 단어' })).toHaveCount(0)

  expect(registrationBodies).toHaveLength(1)
  const firstEvent = Buffer.from(String(registrationBodies[0].canonical_event), 'base64url').toString('utf8')
  expect(firstEvent).toContain('"operation":"genesis"')
  expect(firstEvent).not.toContain(recoveryPhrase)
  expect(JSON.stringify(registrationBodies[0])).not.toContain(recoveryPhrase)
  await page.getByRole('button', { name: '이 설치의 Node Key 만들기' }).click()

  // The local Recorder Node itself must be present in the signed identity
  // history before an archive-wide capability can authorize peer reads.
  await page.getByRole('button', { name: '일회용 등록 증명 발급' }).click()
  // The UI action intentionally runs asynchronously. Wait for its rendered
  // success state before reading the controlled textarea; click() only waits
  // for the DOM event, not the promise started by the React handler.
  await expect(page.getByText('5분 동안 유효한 등록 증명을 만들었습니다.', { exact: true })).toBeVisible()
  const localProofInput = page.getByLabel('Node 등록 증명 JSON')
  await expect(localProofInput).toHaveValue(/"node_id"\s*:/u)
  const localProofText = await localProofInput.inputValue()
  const localProof = JSON.parse(localProofText) as { node_id: string }
  await page.getByLabel('24개 복구 단어').fill(recoveryPhrase)
  await page.getByRole('button', { name: '복구 키로 Node 위임' }).click()
  await expect(page.getByText(new RegExp(`${localProof.node_id} Node를 신원 이력에 위임했습니다.`, 'u'))).toBeVisible()

  await page.getByRole('button', { name: 'Passkey로 글로벌 신원 인증' }).click()
  await expect(page.getByText(/현재 글로벌 신원 세션/u)).toBeVisible()
  const localSession = await page.request.get('/api/auth/session')
  expect((await localSession.json()).authenticated).toBe(true)

  // Exercise the browser assertion path used by online playback: the
  // one-use recipient public key is included in the login-options request,
  // the virtual authenticator signs its challenge, and the resulting session
  // reports that the matching private recipient key is bound to it.
  const playbackLogin = await page.evaluate(async () => {
    const keyPair = await crypto.subtle.generateKey({ name: 'X25519' }, false, ['deriveBits'])
    const recipient = new Uint8Array(await crypto.subtle.exportKey('raw', keyPair.publicKey))
    const base64url = (bytes: Uint8Array) => btoa(String.fromCharCode(...bytes)).replaceAll('+', '-').replaceAll('/', '_').replace(/=+$/u, '')
    const decodeBase64url = (value: string) => {
      const normalized = value.replaceAll('-', '+').replaceAll('_', '/')
      const binary = atob(normalized + '='.repeat((4 - normalized.length % 4) % 4))
      return Uint8Array.from(binary, character => character.charCodeAt(0)).buffer
    }
    const optionsResponse = await fetch('/api/global-identity/login/options', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ recipient_public_key: base64url(recipient) }),
    })
    if (!optionsResponse.ok) return { optionsStatus: optionsResponse.status, finishStatus: 0, playbackKeyBound: false }
    const options = await optionsResponse.json() as { publicKey: PublicKeyCredentialRequestOptions & { challenge: string; allowCredentials?: { id: string; type: PublicKeyCredentialType; transports?: AuthenticatorTransport[] }[] } }
    const publicKey = {
      ...options.publicKey,
      challenge: decodeBase64url(options.publicKey.challenge),
      allowCredentials: options.publicKey.allowCredentials?.map(item => ({ ...item, id: decodeBase64url(item.id) })),
    } as PublicKeyCredentialRequestOptions
    const credential = await navigator.credentials.get({ publicKey }) as PublicKeyCredential | null
    if (!credential || !(credential.response instanceof AuthenticatorAssertionResponse)) {
      return { optionsStatus: optionsResponse.status, finishStatus: 0, playbackKeyBound: false }
    }
    const assertion = credential.response
    const finishResponse = await fetch('/api/global-identity/login/finish', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        id: credential.id, rawId: base64url(new Uint8Array(credential.rawId)), type: credential.type,
        authenticatorAttachment: credential.authenticatorAttachment, clientExtensionResults: credential.getClientExtensionResults(),
        response: {
          authenticatorData: base64url(new Uint8Array(assertion.authenticatorData)),
          clientDataJSON: base64url(new Uint8Array(assertion.clientDataJSON)),
          signature: base64url(new Uint8Array(assertion.signature)),
          userHandle: assertion.userHandle ? base64url(new Uint8Array(assertion.userHandle)) : null,
        },
      }),
    })
    recipient.fill(0)
    const sessionResponse = await fetch('/api/global-identity/session')
    const session = await sessionResponse.json() as { authenticated?: boolean; playback_key_bound?: boolean }
    return { optionsStatus: optionsResponse.status, finishStatus: finishResponse.status, playbackKeyBound: Boolean(session.authenticated && session.playback_key_bound) }
  })
  expect(playbackLogin).toEqual({ optionsStatus: 200, finishStatus: 200, playbackKeyBound: true })

  // This test-only server has a separate account, Node Key and durable
  // identity store. Exercise the transfer workflow through the browser UI.
  const nodeBURL = (await readFile(join(process.env.IR_E2E_DATA_DIR!, 'e2e-node-b-url'), 'utf8')).trim()
  const nodeAPID = Number((await readFile(join(process.env.IR_E2E_DATA_DIR!, 'e2e-node-a-pid'), 'utf8')).trim())
  const nodeBPID = Number((await readFile(join(process.env.IR_E2E_DATA_DIR!, 'e2e-node-b-pid'), 'utf8')).trim())
  expect(nodeAPID).toBeGreaterThan(0)
  expect(nodeBPID).toBeGreaterThan(0)
  expect(nodeBPID).not.toBe(nodeAPID)
  const nodeBContext = await browser.newContext({ locale: 'ko-KR' })
  const nodeB = await nodeBContext.newPage()
  await nodeB.goto(`${nodeBURL}/login`)
  await expect(nodeB).toHaveURL(`${nodeBURL}/login`)
  await nodeB.locator('input[name="username"]').fill('owner', { timeout: 5_000 })
  await nodeB.getByLabel(/비밀번호/u).fill('browser-e2e-strong-password')
  await nodeB.getByRole('button', { name: /로그인/u }).click()
  await expect(nodeB).toHaveURL(`${nodeBURL}/`)
  await nodeB.goto(`${nodeBURL}/account/security`)
  await nodeB.getByRole('button', { name: '이 설치의 Node Key 만들기' }).click()
  await expect(nodeB.getByText('Node ID', { exact: true })).toBeVisible()

  const ownerStatusResponse = await page.request.get('/api/global-identity/status')
  expect(ownerStatusResponse.ok()).toBe(true)
  const ownerStatus = await ownerStatusResponse.json() as { identity: { identity_id: string; head_hash: string } }
  await nodeB.getByLabel('안정된 신원 ID').fill(ownerStatus.identity.identity_id)
  await nodeB.getByLabel('현재 이력 머리값').fill(ownerStatus.identity.head_hash)
  await nodeB.getByRole('button', { name: '일회용 등록 증명 발급' }).click()
  const proofText = await nodeB.getByLabel('Node 등록 증명 JSON').inputValue()
  const proof = JSON.parse(proofText) as { node_id: string }
  expect(proof.node_id).toMatch(/^nd_/u)

  await page.getByLabel('등록 증명 JSON 파일').setInputFiles({ name: 'node-enrollment.json', mimeType: 'application/json', buffer: Buffer.from(proofText) })
  await expect(page.getByLabel('Node 등록 증명 JSON')).toHaveValue(proofText)
  await page.getByLabel('24개 복구 단어').fill(recoveryPhrase)
  await page.getByRole('button', { name: '복구 키로 Node 위임' }).click()
  await expect(page.getByText(new RegExp(`${proof.node_id} Node를 신원 이력에 위임했습니다.`, 'u'))).toBeVisible()
  const delegatedHistoryDownload = page.waitForEvent('download')
  await page.getByRole('button', { name: '공개 신원 이력 다운로드' }).click()
  const delegatedHistoryPath = await (await delegatedHistoryDownload).path()
  if (!delegatedHistoryPath) throw new Error('delegated identity history download is unavailable')
  const delegatedHistory = await readFile(delegatedHistoryPath)
  await nodeB.getByLabel('서명된 신원 이력 JSON').setInputFiles({ name: 'identity-history.json', mimeType: 'application/json', buffer: delegatedHistory })
  await nodeB.getByRole('button', { name: '위임된 신원에 연결' }).click()
  await expect(nodeB.getByText('서명과 Node 위임을 검증한 신원 이력을 가져왔습니다.', { exact: false })).toBeVisible()
  let nodeBStatusResponse = await nodeB.request.get(`${nodeBURL}/api/global-identity/status`)
  expect((await nodeBStatusResponse.json()).identity.node_ids).toContain(proof.node_id)

  // Exercise the actual Control Plane session-key endpoints after a current
  // OwnerSubject claim and Node-scoped view grant. The peer itself is a
  // deterministic in-process fixture; WebAuthn, local authorization, route
  // checks, one-use recipient binding, HPKE opening, and response binding are
  // all real.
  await page.request.post('/__e2e/peer-playback/connect', { data: { node_id: proof.node_id } }).then(async response => {
    expect(response.status()).toBe(204)
  })
  const playbackTestBundle = await buildPlaybackAuthorityTestBundle()
  await page.route('**/__e2e/playback-authority.js', route => route.fulfill({
    status: 200,
    contentType: 'application/javascript; charset=utf-8',
    body: playbackTestBundle,
  }))
  // Load from the same origin so the application CSP stays intact; do not
  // weaken script-src to permit an inline test harness.
  await page.addScriptTag({ url: '/__e2e/playback-authority.js' })
  await page.getByLabel('24개 복구 단어').fill(recoveryPhrase)
  await page.getByRole('button', { name: 'OwnerSubject 만들기' }).click()
  await expect(page.getByText('서명된 이력에 OwnerSubject를 만들었습니다.', { exact: false })).toBeVisible()
  let playbackAuthority: { identityID: string; ownerSubjectID: string; archiveID: string; grantHash: string; nodeID: string }
  try {
    playbackAuthority = await page.evaluate(async ({ phrase, audienceNodeID, peerNodeID }) => {
      const harness = (window as unknown as { IRPlaybackAuthorityTest: { createPlaybackAuthority: (mnemonic: string, localNodeID: string, peerNodeID: string) => Promise<{ identityID: string; ownerSubjectID: string; archiveID: string; grantHash: string; nodeID: string }> } }).IRPlaybackAuthorityTest
      return harness.createPlaybackAuthority(phrase, audienceNodeID, peerNodeID)
    }, { phrase: recoveryPhrase, audienceNodeID: localProof.node_id, peerNodeID: proof.node_id })
  } catch (error) {
    throw new Error(`${error instanceof Error ? error.message : 'playback authority setup failed'}; authority API failures: ${JSON.stringify(authorityFailures)}`)
  }

  const headPath = `/api/peer-archives/${playbackAuthority.nodeID}/head?owner_subject_id=${playbackAuthority.ownerSubjectID}&archive_id=${playbackAuthority.archiveID}&grant_hash=${encodeURIComponent(playbackAuthority.grantHash)}`
  const remoteHeadResponse = await page.request.get(headPath)
  expect(remoteHeadResponse.status()).toBe(200)
  const remoteHead = await remoteHeadResponse.json() as { snapshot: { snapshot_id: string } }
  const openedPlaybackEnvelope = await page.evaluate(async ({ authority, snapshotID }) => {
    const harness = (window as unknown as { IRPlaybackAuthorityTest: { openPlaybackEnvelope: (input: typeof authority, snapshot: string) => Promise<{ opened: boolean; playbackKeyBoundAfterConsume: boolean; identityID: string }> } }).IRPlaybackAuthorityTest
    return harness.openPlaybackEnvelope(authority, snapshotID)
  }, { authority: playbackAuthority, snapshotID: remoteHead.snapshot.snapshot_id })
  expect(openedPlaybackEnvelope).toEqual({ opened: true, playbackKeyBoundAfterConsume: false, identityID: playbackAuthority.identityID })

  // Exercise the shipped remote-archive UI against the same real Control
  // Plane APIs and a deterministic encrypted archive fixture. The peer is an
  // in-process fixture, while authorization, key issuance, ciphertext range
  // retrieval, browser decryption, HLS playback, and seeking use production
  // code paths.
  await page.goto('/recordings')
  const ciphertextResponses: Array<{ path: string; status: number }> = []
  page.on('response', response => {
    const path = new URL(response.url()).pathname
    if (path.includes('/snapshots/') && path.includes('/objects/')) ciphertextResponses.push({ path, status: response.status() })
  })
  await page.getByRole('button', { name: '스냅샷 목록 불러오기' }).click()
  await expect(page.getByText(/스냅샷 1 · 객체 4개/u)).toBeVisible()
  await expect(page.getByText(remoteHead.snapshot.snapshot_id, { exact: true })).toBeVisible()
  await page.getByRole('button', { name: 'Passkey로 재생 키 받기' }).click()
  await expect(page.getByText('Passkey로 받은 임시 키를 브라우저에서 확인했습니다. 재생 버튼을 누르세요.', { exact: true })).toBeVisible()
  const video = page.locator('video')
  await expect(video).toBeVisible()
  const playInvocation = await video.evaluate(element => {
    const media = element as HTMLVideoElement
    media.muted = true
    void media.play().catch(() => { /* surfaced through media state or the UI's fatal playback alert */ })
    return { readyState: media.readyState, error: media.error?.code ?? 0 }
  })
  expect(playInvocation.error).toBe(0)
  await page.waitForFunction(() => {
    const alert = document.querySelector('[role="alert"]')?.textContent?.trim()
    const media = document.querySelector('video')
    return Boolean(alert) || Boolean(media && media.readyState >= HTMLMediaElement.HAVE_METADATA && media.duration > 2.5)
  }, undefined, { timeout: 15_000 })
  const playbackAlert = (await page.getByRole('alert').allTextContents()).join('; ')
  if (playbackAlert) throw new Error(`remote playback UI reported failure: ${playbackAlert}; ciphertext responses=${JSON.stringify(ciphertextResponses)}`)
  const metadataDuration = await video.evaluate(element => element.duration)
  expect(metadataDuration).toBeGreaterThan(2.5)
  const playedAndSought = await video.evaluate(async element => {
    const media = element as HTMLVideoElement
    await new Promise<void>((resolve, reject) => {
      const timeout = window.setTimeout(() => reject(new Error('remote fixture playback did not advance')), 5_000)
      const check = () => {
        if (media.currentTime >= 0.2) {
          window.clearTimeout(timeout)
          media.removeEventListener('timeupdate', check)
          resolve()
        }
      }
      media.addEventListener('timeupdate', check)
    })
    media.pause()
    const target = Math.min(2.2, media.duration - 0.2)
    const seeked = new Promise<void>(resolve => media.addEventListener('seeked', () => resolve(), { once: true }))
    media.currentTime = target
    await seeked
    return { duration: media.duration, currentTime: media.currentTime }
  })
  expect(playedAndSought.duration).toBeGreaterThan(2.5)
  expect(playedAndSought.currentTime).toBeGreaterThan(2)
  expect(ciphertextResponses).toHaveLength(4)
  expect(ciphertextResponses.every(response => response.status === 200)).toBe(true)
  expect(new Set(ciphertextResponses.map(response => response.path)).size).toBe(4)

  await page.goto('/account/security')
  await page.getByLabel('24개 복구 단어').fill(recoveryPhrase)
  page.once('dialog', dialog => dialog.accept())
  await page.locator('div.flex.items-center.justify-between.gap-2.text-xs')
    .filter({ hasText: proof.node_id })
    .getByRole('button', { name: '위임 폐기' })
    .click()
  await expect(page.getByText(new RegExp(`${proof.node_id} Node 위임을 폐기했습니다.`, 'u'))).toBeVisible()
  const revokedHistoryDownload = page.waitForEvent('download')
  await page.getByRole('button', { name: '공개 신원 이력 다운로드' }).click()
  const revokedHistoryPath = await (await revokedHistoryDownload).path()
  if (!revokedHistoryPath) throw new Error('revoked identity history download is unavailable')
  const revokedHistory = await readFile(revokedHistoryPath)
  await nodeB.getByLabel('서명된 신원 이력 JSON').setInputFiles({ name: 'identity-history.json', mimeType: 'application/json', buffer: revokedHistory })
  await nodeB.getByRole('button', { name: '서명된 이력 동기화' }).click()
  await expect(nodeB.getByText('서명과 Node 위임을 검증한 신원 이력을 가져왔습니다.', { exact: false })).toBeVisible()
  nodeBStatusResponse = await nodeB.request.get(`${nodeBURL}/api/global-identity/status`)
  const revokedNodeBIdentity = (await nodeBStatusResponse.json()).identity
  expect(revokedNodeBIdentity.node_ids).not.toContain(proof.node_id)
  expect(revokedNodeBIdentity.sequence).toBe('3')
  const nodeKeyBeforeRestartResponse = await nodeB.request.get(`${nodeBURL}/api/node-identity/status`)
  expect(nodeKeyBeforeRestartResponse.ok()).toBe(true)
  const nodeKeyBeforeRestart = (await nodeKeyBeforeRestartResponse.json() as { node: { node_id: string } }).node.node_id
  await nodeBContext.close()

  const restartResponse = await page.request.post('/__e2e/identity-peer/restart')
  expect(restartResponse.status()).toBe(204)
  await expect.poll(async () => {
    const pid = Number((await readFile(join(process.env.IR_E2E_DATA_DIR!, 'e2e-node-b-pid'), 'utf8').catch(() => '')).trim())
    return pid > 0 && pid !== nodeBPID
  }, { timeout: 10_000 }).toBe(true)
  const restartedNodeBURL = (await readFile(join(process.env.IR_E2E_DATA_DIR!, 'e2e-node-b-url'), 'utf8')).trim()
  const restartedNodeBContext = await browser.newContext({ locale: 'ko-KR' })
  const restartedNodeB = await restartedNodeBContext.newPage()
  await restartedNodeB.goto(`${restartedNodeBURL}/login`)
  await restartedNodeB.locator('input[name="username"]').fill('owner')
  await restartedNodeB.getByLabel(/비밀번호/u).fill('browser-e2e-strong-password')
  await restartedNodeB.getByRole('button', { name: /로그인/u }).click()
  await expect(restartedNodeB).toHaveURL(`${restartedNodeBURL}/`)
  const nodeKeyAfterRestartResponse = await restartedNodeB.request.get(`${restartedNodeBURL}/api/node-identity/status`)
  expect(nodeKeyAfterRestartResponse.ok()).toBe(true)
  const nodeKeyAfterRestart = (await nodeKeyAfterRestartResponse.json() as { node: { node_id: string } }).node.node_id
  expect(nodeKeyAfterRestart).toBe(nodeKeyBeforeRestart)
  const identityAfterRestartResponse = await restartedNodeB.request.get(`${restartedNodeBURL}/api/global-identity/status`)
  expect(identityAfterRestartResponse.ok()).toBe(true)
  const identityAfterRestart = (await identityAfterRestartResponse.json() as { identity: { sequence: string; node_ids: string[] } }).identity
  expect(identityAfterRestart.sequence).toBe('3')
  expect(identityAfterRestart.node_ids).not.toContain(proof.node_id)
  await restartedNodeBContext.close()

  // Recovery models a replacement device: remove the first virtual
  // authenticator before adding a fresh one with no credential for this RP.
  await cdp.send('WebAuthn.removeVirtualAuthenticator', { authenticatorId })
  const { authenticatorId: recoveryAuthenticatorId } = await cdp.send('WebAuthn.addVirtualAuthenticator', {
    options: {
      protocol: 'ctap2',
      transport: 'internal',
      hasResidentKey: true,
      hasUserVerification: true,
      isUserVerified: true,
      automaticPresenceSimulation: true,
    },
  })
  await page.getByLabel('24개 복구 단어').fill(recoveryPhrase)
  await page.getByRole('button', { name: '복구 키로 Passkey 추가' }).click()
  await expect.poll(async () => {
    if (await page.getByRole('alert').count()) return 'error'
    if (await page.getByText('복구 서명 이력을 추가하고 새 Passkey를 등록했습니다.', { exact: false }).count()) return 'success'
    return 'pending'
  }, { timeout: 15_000 }).not.toBe('pending')
  const addAlert = await page.getByRole('alert').allTextContents()
  if (addAlert.length) throw new Error(`Recovery-based Passkey add failed: ${addAlert.join('; ')}; API=${JSON.stringify(identityFailures)}`)
  await expect(page.getByText('복구 서명 이력을 추가하고 새 Passkey를 등록했습니다.', { exact: false })).toBeVisible()
  await expect(page.getByText('등록된 Passkey').locator('..')).toContainText('2')
  expect(registrationBodies).toHaveLength(2)
  const addEvent = Buffer.from(String(registrationBodies[1].canonical_event), 'base64url').toString('utf8')
  expect(addEvent).toContain('"operation":"credential_add"')
  expect(addEvent).not.toContain(recoveryPhrase)
  expect(JSON.stringify(registrationBodies[1])).not.toContain(recoveryPhrase)

  await page.getByLabel('24개 복구 단어').fill(recoveryPhrase)
  page.once('dialog', dialog => dialog.accept())
  await page.getByRole('button', { name: '이 Passkey 폐기' }).first().click()
  await expect(page.getByText('복구 서명 이력에 따라 Passkey를 폐기했습니다.', { exact: false })).toBeVisible()
  await expect(page.getByText('등록된 Passkey').locator('..')).toContainText('1')
  await expect(page.getByRole('button', { name: 'Passkey로 글로벌 신원 인증' })).toBeVisible()
  const localSessionAfterRevoke = await page.request.get('/api/auth/session')
  expect((await localSessionAfterRevoke.json()).authenticated).toBe(true)

  // The replacement authenticator must work as a fresh credential after the
  // original has been revoked; recovery is not complete at registration alone.
  const loggedOutIdentitySession = await page.request.get('/api/global-identity/session')
  expect((await loggedOutIdentitySession.json()).authenticated).toBe(false)
  await expect(page.getByRole('button', { name: 'Passkey로 글로벌 신원 인증' })).toBeVisible()
  await page.getByRole('button', { name: 'Passkey로 글로벌 신원 인증' }).click()
  await expect(page.getByText(/현재 글로벌 신원 세션/u)).toBeVisible()
  const restoredIdentity = await page.request.get('/api/global-identity/status')
  expect(restoredIdentity.ok()).toBe(true)
  expect((await restoredIdentity.json() as { identity: { identity_id: string } }).identity.identity_id).toBe(ownerStatus.identity.identity_id)

  await cdp.send('WebAuthn.removeVirtualAuthenticator', { authenticatorId: recoveryAuthenticatorId })
  await cdp.detach()
})
