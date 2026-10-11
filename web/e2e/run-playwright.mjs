import { spawn, spawnSync } from 'node:child_process'
import { mkdtempSync, rmSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { tmpdir } from 'node:os'
import { fileURLToPath } from 'node:url'
import process from 'node:process'

const webDir = dirname(fileURLToPath(import.meta.url))
const dataDir = mkdtempSync(join(tmpdir(), 'integrated-recorder-ui-e2e-'))
const build = spawnSync('npm', ['run', 'build'], { cwd: webDir, stdio: 'inherit' })
if (build.error || build.status !== 0) {
  try { rmSync(dataDir, { recursive: true, force: true, maxRetries: 5, retryDelay: 50 }) } catch { /* isolated test data cleanup is best effort */ }
  process.exit(build.status ?? 1)
}
const child = spawn('npx', ['playwright', 'test', ...process.argv.slice(2)], {
  env: { ...process.env, IR_E2E_DATA_DIR: dataDir },
  stdio: 'inherit',
})

let stopping = false
const stop = signal => {
  if (stopping) return
  stopping = true
  child.kill(signal)
}
process.on('SIGINT', () => stop('SIGINT'))
process.on('SIGTERM', () => stop('SIGTERM'))

const exitCode = await new Promise((resolve, reject) => {
  child.once('error', reject)
  child.once('exit', (code, signal) => resolve(code ?? (signal ? 1 : 0)))
})
try { rmSync(dataDir, { recursive: true, force: true, maxRetries: 5, retryDelay: 50 }) } catch { /* E2E data cleanup is best effort */ }
process.exitCode = exitCode
