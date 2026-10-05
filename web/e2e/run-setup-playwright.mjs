/* global AbortSignal, console, fetch */
import { spawn, spawnSync } from 'node:child_process'
import { chmodSync, copyFileSync, cpSync, existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync, renameSync } from 'node:fs'
import { join, resolve, dirname } from 'node:path'
import { fileURLToPath } from 'node:url'
import { clearInterval, setInterval } from 'node:timers'
import process from 'node:process'
import { setTimeout as delay } from 'node:timers/promises'

const e2eDir = dirname(fileURLToPath(import.meta.url))
const webDir = resolve(e2eDir, '..')
const repositoryRoot = resolve(webDir, '..')
const tempBase = existsSync('/private/tmp') ? '/private/tmp' : process.env.TMPDIR || '/tmp'
const root = mkdtempSync(join(tempBase, `ir-setup-e2e-${process.pid}-`))
const dataDir = join(root, 'data')
const binDir = join(root, 'bin')
const bundleDir = join(root, 'initial')
const adapterDir = join(root, 'adapters')
const controlFile = join(root, 'restart-request.json')
const responseFile = `${controlFile}.response`
const hostAddress = '127.0.0.1:4173'
const baseURL = `http://${hostAddress}`

for (const path of [dataDir, binDir, bundleDir, adapterDir]) mkdirSync(path, { mode: 0o700 })

let host
let playwright
let stopping = false
let restartBusy = false
let lastRequestID = ''
let restartPoll
let restartFailure
let hostLogs = ''
let setupCodeForLogCheck = ''
let hostLogContainsSetupCode = false
let hostLogBoundary = ''
let hostChildLeakDetected = false

function run(command, args, options = {}) {
  const result = spawnSync(command, args, {
    cwd: options.cwd ?? repositoryRoot,
    env: options.env ?? process.env,
    encoding: 'utf8',
    stdio: options.stdio ?? 'inherit',
    timeout: options.timeout ?? 5 * 60_000,
  })
  if (result.error || result.status !== 0) {
    const diagnostic = result.error?.message || `${command} exited with ${result.status ?? 'a signal'}`
    const output = [result.stdout, result.stderr].filter(Boolean).join('\n').slice(-8_000)
    throw new Error(`${diagnostic}${output ? `\n${output}` : ''}`)
  }
  return result
}

function build() {
  run('npm', ['run', 'build'], { cwd: webDir, timeout: 3 * 60_000 })
  const moduleCache = join(root, 'go-module-cache')
  const moduleCacheSource = spawnSync('go', ['env', 'GOMODCACHE'], { cwd: repositoryRoot, encoding: 'utf8' }).stdout.trim()
  if (moduleCacheSource && existsSync(moduleCacheSource)) cpSync(moduleCacheSource, moduleCache, { recursive: true })
  const goBuildEnv = {
    ...process.env,
    GOCACHE: process.env.GOCACHE || join(root, 'go-build-cache'),
    GOMODCACHE: process.env.GOMODCACHE || moduleCache,
  }
  const infoFlags = [
    '-X github.com/integrated-recorder/core/internal/buildinfo.version=0.0.0-setup-e2e',
    '-X github.com/integrated-recorder/core/internal/buildinfo.commit=eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee',
    '-X github.com/integrated-recorder/core/internal/buildinfo.buildTime=2026-10-01T00:00:00Z',
    '-X github.com/integrated-recorder/core/internal/buildinfo.releaseChannel=prerelease',
  ]
  const output = (name, packagePath, extra = '') => {
    const path = join(binDir, name)
    const flags = [...infoFlags, extra].filter(Boolean).join(' ')
    run('go', ['build', '-trimpath', '-o', path, '-ldflags', flags, packagePath], { env: goBuildEnv, timeout: 5 * 60_000 })
    chmodSync(path, 0o700)
    return path
  }

  const control = output('control-plane', './cmd/control-plane')
  const engine = output('recorder-engine', './cmd/recorder-engine')
  const adapter = output('integrated-recorder-adapter-hls', './cmd/adapters/hls')
  copyFileSync(control, join(bundleDir, 'control-plane'))
  copyFileSync(engine, join(bundleDir, 'recorder-engine'))
  chmodSync(join(bundleDir, 'control-plane'), 0o555)
  chmodSync(join(bundleDir, 'recorder-engine'), 0o555)
  chmodSync(bundleDir, 0o700)
  copyFileSync(adapter, join(adapterDir, 'integrated-recorder-adapter-hls'))
  chmodSync(join(adapterDir, 'integrated-recorder-adapter-hls'), 0o700)

  const bundledHLSPath = join(adapterDir, 'integrated-recorder-adapter-hls')
  const hostExtra = [
    `-X github.com/integrated-recorder/core/internal/runtimehost/bootstrap.defaultBundleDir=${bundleDir}`,
    `-X github.com/integrated-recorder/core/internal/runtimehost/bootstrap.defaultBundledHLSBinary=${bundledHLSPath}`,
  ].join(' ')
  return output('runtime-host', './cmd/runtime-host', hostExtra)
}

const productEnv = () => ({
  ...process.env,
  DATA_DIR: dataDir,
  ADDR: hostAddress,
  ADAPTER_DIR: adapterDir,
})

function startHost(binary) {
  const child = spawn(binary, [], { cwd: repositoryRoot, env: productEnv(), detached: process.platform !== 'win32', stdio: ['ignore', 'pipe', 'pipe'] })
  const collect = chunk => {
    const text = chunk.toString()
    if (setupCodeForLogCheck && `${hostLogBoundary}${text}`.includes(setupCodeForLogCheck)) hostLogContainsSetupCode = true
    if (setupCodeForLogCheck.length > 1) hostLogBoundary = `${hostLogBoundary}${text}`.slice(-(setupCodeForLogCheck.length - 1))
    hostLogs = (hostLogs + text).slice(-8_000)
  }
  child.stdout.on('data', collect)
  child.stderr.on('data', collect)
  child.once('error', () => { restartFailure = 'Runtime Host process failed to start.' })
  return child
}

function hostExited(child) {
  return child.exitCode !== null || child.signalCode !== null
}

async function waitForExit(child, timeoutMs) {
  if (hostExited(child)) return true
  return await Promise.race([
    new Promise(resolveExit => child.once('exit', () => resolveExit(true))),
    delay(timeoutMs).then(() => false),
  ])
}

function processGroupExists(processGroupID) {
  if (process.platform === 'win32') return false
  try {
    process.kill(-processGroupID, 0)
    return true
  } catch (error) {
    if (error?.code === 'ESRCH') return false
    if (error?.code === 'EPERM') return true
    throw error
  }
}

async function waitForProcessGroupExit(processGroupID, timeoutMs) {
  if (process.platform === 'win32') return true
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    if (!processGroupExists(processGroupID)) return true
    await delay(100)
  }
  return !processGroupExists(processGroupID)
}

async function stopHost(child, force = false) {
  if (!child) return
  if (!hostExited(child)) {
    try { child.kill(force ? 'SIGKILL' : 'SIGTERM') } catch { /* checked below */ }
    if (!await waitForExit(child, force ? 5_000 : 25_000)) {
      if (child.pid && process.platform !== 'win32') {
        try { process.kill(-child.pid, 'SIGKILL') } catch { /* process group already exited */ }
      }
      try { child.kill('SIGKILL') } catch { /* process already exited */ }
      if (!await waitForExit(child, 5_000)) throw new Error('Runtime Host process could not be stopped cleanly.')
    }
  }
  // Every product subprocess inherits this detached test Host's private
  // process group. A graceful Host exit must drain the group itself; detect
  // and fail on leftovers before force-cleaning them to avoid test leaks.
  let orphanedChildren = false
  if (child.pid && process.platform !== 'win32') {
    if (!await waitForProcessGroupExit(child.pid, 5_000)) {
      orphanedChildren = true
      hostChildLeakDetected = true
      try { process.kill(-child.pid, 'SIGKILL') } catch { /* process group exited concurrently */ }
      await waitForProcessGroupExit(child.pid, 5_000)
    }
  }
  return orphanedChildren
}

async function waitForHealth(child, timeoutMs = 45_000) {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    if (restartFailure) throw new Error(restartFailure)
    if (hostExited(child)) {
      const safeLog = hostLogs.replaceAll(root, '<private-e2e-dir>').replaceAll(dataDir, '<private-data-dir>')
      throw new Error(`Runtime Host exited before becoming healthy.${safeLog ? `\n${safeLog}` : ''}`)
    }
    try {
      const response = await fetch(`${baseURL}/healthz`, { signal: AbortSignal.timeout(1_000) })
      if (response.ok) return
    } catch { /* startup/restart window */ }
    await delay(100)
  }
  throw new Error('Runtime Host did not become healthy before the test timeout.')
}

function readSetupCode(binary) {
  const result = spawnSync(binary, ['setup-code'], { env: { ...process.env, DATA_DIR: dataDir }, encoding: 'utf8', timeout: 10_000 })
  if (result.error || result.status !== 0 || !result.stdout || !result.stdout.endsWith('\n') || result.stdout.trim().includes('\n')) {
    throw new Error('Runtime Host did not provide a first-run setup code.')
  }
  return result.stdout.trim()
}

async function restartHostFromTest(binary) {
  const child = host
  if (await stopHost(child)) throw new Error('Runtime Host left product child processes running after graceful restart.')
  host = startHost(binary)
  await waitForHealth(host)
}

function writeRestartResponse(response) {
  const temporary = `${responseFile}.tmp`
  writeFileSync(temporary, JSON.stringify(response), { mode: 0o600 })
  renameSync(temporary, responseFile)
}

async function processRestartRequests(binary) {
  restartPoll = setInterval(async () => {
    if (restartBusy || stopping || !existsSync(controlFile)) return
    restartBusy = true
    let request
    try {
      request = JSON.parse(readFileSync(controlFile, 'utf8'))
      if (!request || request.action !== 'restart' || typeof request.request_id !== 'string' || request.request_id === lastRequestID) return
      lastRequestID = request.request_id
      writeRestartResponse({ request_id: request.request_id, phase: 'restarting', ok: false })
      await restartHostFromTest(binary)
      writeRestartResponse({ request_id: request.request_id, phase: 'ready', ok: true })
    } catch {
      restartFailure = 'Runtime Host restart request failed.'
      try { writeRestartResponse({ request_id: request?.request_id ?? '', phase: 'failed', ok: false, error_code: 'runtime_host_restart_failed' }) } catch { /* test will fail by timeout */ }
    } finally {
      restartBusy = false
    }
  }, 100)
  restartPoll.unref()
}

async function main() {
  const hostBinary = build()
  host = startHost(hostBinary)
  await waitForHealth(host)
  const setupCode = readSetupCode(hostBinary)
  setupCodeForLogCheck = setupCode
  hostLogContainsSetupCode ||= hostLogs.includes(setupCode)
  await processRestartRequests(hostBinary)

  const playwrightCLI = join(webDir, 'node_modules', '@playwright', 'test', 'cli.js')
  if (!existsSync(playwrightCLI)) throw new Error('Playwright is not installed. Run npm ci in web/.')
  playwright = spawn(process.execPath, [playwrightCLI, 'test', '--config', 'playwright.setup.config.ts', ...process.argv.slice(2)], {
    cwd: webDir,
    env: {
      ...process.env,
      IR_SETUP_E2E_DATA_DIR: dataDir,
      IR_SETUP_E2E_CODE: setupCode,
      IR_SETUP_E2E_RESTART_FILE: controlFile,
    },
    stdio: 'inherit',
  })
  const result = await new Promise((resolveExit, reject) => {
    playwright.once('error', reject)
    playwright.once('exit', (code, signal) => resolveExit(code ?? (signal ? 1 : 0)))
  })
  if (hostLogContainsSetupCode) {
    console.error('Runtime Host logs unexpectedly contained the setup secret.')
    process.exitCode = 1
  } else if (hostChildLeakDetected) {
    console.error('Runtime Host left product child processes running after graceful shutdown; remaining processes were force-cleaned.')
    process.exitCode = 1
  } else {
    process.exitCode = result
  }
}

async function cleanup() {
  stopping = true
  if (restartPoll) clearInterval(restartPoll)
  if (playwright && !hostExited(playwright)) {
    try { playwright.kill('SIGTERM') } catch { /* best effort */ }
    await waitForExit(playwright, 10_000)
    if (!hostExited(playwright)) {
      try { playwright.kill('SIGKILL') } catch { /* best effort */ }
      await waitForExit(playwright, 5_000)
    }
  }
  let orphanedChildren = false
  try { orphanedChildren = await stopHost(host) } catch { await stopHost(host, true) }
  if (orphanedChildren || hostChildLeakDetected) {
    console.error('Runtime Host left product child processes running after graceful shutdown; remaining processes were force-cleaned.')
    process.exitCode = 1
  }
  try { rmSync(root, { recursive: true, force: true, maxRetries: 5, retryDelay: 50 }) } catch { /* isolated temp cleanup is best effort */ }
}

process.on('SIGINT', () => { process.exitCode = 130; void cleanup().finally(() => process.exit()) })
process.on('SIGTERM', () => { process.exitCode = 143; void cleanup().finally(() => process.exit()) })

try {
  await main()
} catch (error) {
  if (error instanceof Error) console.error(error.message)
  process.exitCode = 1
} finally {
  await cleanup()
}
