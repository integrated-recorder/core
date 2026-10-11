import { spawn, spawnSync } from 'node:child_process'
import { cpSync, existsSync } from 'node:fs'
import { dirname, resolve, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import process from 'node:process'
import { cleanupBuildDir, createBuildDir } from './temp-build.mjs'

const webDir = dirname(fileURLToPath(import.meta.url))
const repositoryRoot = resolve(webDir, '../..')
const dataDir = process.env.IR_E2E_DATA_DIR
if (!dataDir) throw new Error('IR_E2E_DATA_DIR must be set by Playwright config')

const buildDir = createBuildDir()
const serverBinary = join(buildDir, 'integrated-recorder-e2e-server')
const moduleCache = join(buildDir, 'go-mod-cache')
let child
let stopping = false
const cleanup = () => cleanupBuildDir(buildDir)
process.once('exit', cleanup)
const forwardSignal = signal => {
  if (stopping) return
  stopping = true
  child?.kill(signal)
}
process.on('SIGINT', () => forwardSignal('SIGINT'))
process.on('SIGTERM', () => forwardSignal('SIGTERM'))

function run(command, args, options) {
  return new Promise((resolveRun, rejectRun) => {
    const processChild = spawn(command, args, options)
    child = processChild
    processChild.once('error', rejectRun)
    processChild.once('exit', (code, signal) => resolveRun({ code: code ?? (signal ? 1 : 0), signal }))
  })
}

try {
  const moduleCacheSource = spawnSync('go', ['env', 'GOMODCACHE'], { encoding: 'utf8' }).stdout.trim()
  if (moduleCacheSource && existsSync(moduleCacheSource)) cpSync(moduleCacheSource, moduleCache, { recursive: true })
  const goEnvironment = { ...process.env, GOMODCACHE: moduleCache, GOCACHE: join(buildDir, 'go-build-cache') }
  const build = await run('go', ['build', '-o', serverBinary, './web/e2e/backend'], {
    cwd: repositoryRoot,
    env: goEnvironment,
    stdio: 'inherit',
  })
  child = undefined
  if (build.code !== 0 || stopping) {
    process.exitCode = build.code || 1
  } else {
    const server = await run(serverBinary, [], {
      cwd: repositoryRoot,
      env: { ...goEnvironment, DATA_DIR: dataDir, ADDR: '127.0.0.1:4173' },
      stdio: 'inherit',
    })
    child = undefined
    process.exitCode = server.code
  }
} finally {
  if (child) child.kill('SIGTERM')
  cleanup()
}
