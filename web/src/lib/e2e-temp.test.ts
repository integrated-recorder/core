import { afterEach, describe, expect, it } from 'vitest'
import { chmodSync, lstatSync, mkdirSync, mkdtempSync, readdirSync, rmSync, symlinkSync, writeFileSync } from 'node:fs'
import { spawn, spawnSync } from 'node:child_process'
import { tmpdir } from 'node:os'
import { dirname, join, resolve } from 'node:path'
import process from 'node:process'
import { cleanupBuildDir, cleanupStaleBuildDirs, createBuildDir } from '../../e2e/temp-build.mjs'

const markerName = '.ir-e2e-build-owner.json'
const prefix = 'integrated-recorder-e2e-build-'
const roots: string[] = []

function testRoot() {
  const root = mkdtempSync(join(tmpdir(), 'ir-e2e-temp-test-'))
  roots.push(root)
  return root
}

function treeBytes(path: string): number {
  try {
    const info = lstatSync(path)
    if (!info.isDirectory()) return info.size
    return readdirSync(path).reduce((total, name) => total + treeBytes(join(path, name)), 0)
  } catch (error: unknown) {
    // Cleanup can remove an entry between the parent listing and this stat.
    if (error && typeof error === 'object' && 'code' in error && error.code === 'ENOENT') return 0
    throw error
  }
}

afterEach(() => {
  for (const root of roots.splice(0)) rmSync(root, { recursive: true, force: true })
})

describe('E2E temporary build storage', () => {
  it('reclaims its owned directory after success, failure, cancellation, and shutdown', () => {
    const root = testRoot()
    for (let cycle = 0; cycle < 4; cycle++) {
      const path = createBuildDir(root, 101, 10_000_000)
      expect(cleanupBuildDir(path, root, 101)).toBe(true)
      expect(cleanupBuildDir(path, root, 101)).toBe(false)
    }
    expect(lstatSync(root).isDirectory()).toBe(true)
    expect((awaitImportReaddir(root))).toEqual([])
  })

  it('keeps the owner marker when removing scratch contents fails', () => {
    if (process.platform === 'win32') return
    const root = testRoot()
    const path = createBuildDir(root, 111, 10_000_000)
    writeFileSync(join(path, 'fixture.bin'), Buffer.alloc(16))
    expect(cleanupBuildDir(path, root, 111, { removeEntry: () => { throw new Error('injected removal failure') } })).toBe(false)
    expect(lstatSync(join(path, markerName)).isFile()).toBe(true)
    expect(cleanupBuildDir(path, root, 111)).toBe(true)
    expect(readdirSync(root)).toEqual([])
  })

  it('removes copied Go module caches with read-only nested directories', () => {
    if (process.platform === 'win32') return
    const root = testRoot()
    const path = createBuildDir(root, 121, 10_000_000)
    const cache = join(path, 'go-mod-cache', 'example.com', 'module@v1.0.0')
    mkdirSync(cache, { recursive: true, mode: 0o700 })
    writeFileSync(join(cache, 'go.mod'), 'module example.com/module\n', { mode: 0o444 })
    chmodSync(cache, 0o555)
    chmodSync(dirname(cache), 0o555)
    chmodSync(join(path, 'go-mod-cache'), 0o555)

    expect(cleanupBuildDir(path, root, 121)).toBe(true)
    expect(readdirSync(root)).toEqual([])
  })

  it('removes only old, marked, dead-owner directories', () => {
    const root = testRoot()
    // Keep fixture creation from running startup recovery with the host's real
    // process table. PID 202 may be occupied locally, making the old fixture
    // survive on one runner and disappear on another.
    const preserveFixtures = { isAlive: () => true }
    const old = createBuildDir(root, 202, 1_000, preserveFixtures)
    const active = createBuildDir(root, 303, 1_000, preserveFixtures)
    const recent = createBuildDir(root, 404, 9_000_000, preserveFixtures)
    const unmarked = join(root, `${prefix}unmarked`)
    mkdirSync(unmarked, { mode: 0o700 })
    const symlink = join(root, `${prefix}symlink`)
    mkdirSync(join(root, 'outside'))
    writeFileSync(join(root, 'outside', markerName), JSON.stringify({ version: 1, pid: 505, createdAtMs: 1_000 }))
    // A symlink must not be traversed or treated as an owned temporary root.
    awaitSymlink(symlink, join(root, 'outside'))
    chmodSync(root, 0o700)

    expect(cleanupStaleBuildDirs(root, { now: 9_000_000, isAlive: pid => pid === 303 })).toBe(1)
    expect(() => lstatSync(old)).toThrow()
    expect(lstatSync(active).isDirectory()).toBe(true)
    expect(lstatSync(recent).isDirectory()).toBe(true)
    expect(lstatSync(unmarked).isDirectory()).toBe(true)
    expect(lstatSync(symlink).isSymbolicLink()).toBe(true)
  })

  it('reclaims a hard-killed runner orphan on the next build invocation', () => {
    const root = testRoot()
    const deadOwner = createBuildDir(root, 707, 1_000)
    const nextInvocation = createBuildDir(root, 808, 10_000_000, { isAlive: pid => pid === 808 })
    expect(() => lstatSync(deadOwner)).toThrow()
    expect(lstatSync(nextInvocation).isDirectory()).toBe(true)
    expect(readdirSync(root)).toEqual([nextInvocation.slice(root.length + 1)])
    expect(cleanupBuildDir(nextInvocation, root, 808)).toBe(true)
  })

  it('cleans the runner scratch after build success, build failure, and SIGTERM', async () => {
    const root = testRoot()
    const fakeBin = join(root, 'bin')
    mkdirSync(fakeBin, { mode: 0o700 })
    const fakeGo = join(fakeBin, 'go')
    writeFileSync(fakeGo, `#!/usr/bin/env node
const fs = require('node:fs')
const args = process.argv.slice(2)
if (args[0] === 'env') { process.stdout.write(''); process.exit(0) }
if (process.env.IR_FAKE_BUILD_FAIL === '1') process.exit(7)
const output = args[args.indexOf('-o') + 1]
if (process.env.IR_FAKE_BUILD_BYTES) fs.writeFileSync(output + '.scratch', Buffer.alloc(Number(process.env.IR_FAKE_BUILD_BYTES)))
fs.writeFileSync(output, '#!/usr/bin/env node\\nconst fs = require("node:fs"); if (process.env.IR_FAKE_READY) fs.writeFileSync(process.env.IR_FAKE_READY, "ready"); if (process.env.IR_FAKE_WAIT === "1") setInterval(() => {}, 1000)\\n', { mode: 0o700 })
fs.chmodSync(output, 0o700)
if (process.env.IR_FAKE_BUILD_DELAY_MS) setTimeout(() => process.exit(0), Number(process.env.IR_FAKE_BUILD_DELAY_MS))
`)
    chmodSync(fakeGo, 0o700)
    const runner = resolve(dirname(new URL(import.meta.url).pathname), '../../e2e/run-backend.mjs')
    const env = { ...process.env, TMPDIR: root, PATH: `${fakeBin}:${process.env.PATH}`, IR_E2E_DATA_DIR: root }
    const stale = createBuildDir(root, 2_000_000_000, Date.now() - 10 * 60 * 1000, { isAlive: () => false })
    const success = spawnSync(process.execPath, [runner], { env, encoding: 'utf8' })
    expect(success.status).toBe(0)
    expect(() => lstatSync(stale)).toThrow()
    expect(readdirSync(root).filter(name => name.startsWith(prefix))).toEqual([])

    const baselineBytes = treeBytes(root)
    let peakBytes = baselineBytes
    for (let cycle = 0; cycle < 10; cycle++) {
      const runnerProcess = spawn(process.execPath, [runner], { env: { ...env, IR_FAKE_BUILD_BYTES: String(512 * 1024), IR_FAKE_BUILD_DELAY_MS: '100' }, stdio: 'ignore' })
      let exited = false
      const result = new Promise<number | null>(resolveExit => runnerProcess.once('exit', code => { exited = true; resolveExit(code) }))
      while (!exited) {
        peakBytes = Math.max(peakBytes, treeBytes(root))
        await new Promise(resolveWait => setTimeout(resolveWait, 5))
      }
      expect(await result).toBe(0)
      peakBytes = Math.max(peakBytes, treeBytes(root))
      expect(readdirSync(root).filter(name => name.startsWith(prefix))).toEqual([])
    }
    const finalBytes = treeBytes(root)
    console.info(`isolated E2E runner temp bytes: baseline=${baselineBytes} peak=${peakBytes} after=${finalBytes} cycles=10`)
    expect(peakBytes).toBeGreaterThan(baselineBytes)
    expect(finalBytes).toBe(baselineBytes)

    const failedBuild = spawnSync(process.execPath, [runner], { env: { ...env, IR_FAKE_BUILD_FAIL: '1' }, encoding: 'utf8' })
    expect(failedBuild.status).toBe(7)
    expect(readdirSync(root).filter(name => name.startsWith(prefix))).toEqual([])

    const ready = join(root, 'server-ready')
    const runnerProcess = spawn(process.execPath, [runner], { env: { ...env, IR_FAKE_READY: ready, IR_FAKE_WAIT: '1' }, stdio: 'ignore' })
    await expect.poll(() => {
      try { return lstatSync(ready).isFile() } catch { return false }
    }, { timeout: 10_000 }).toBe(true)
    runnerProcess.kill('SIGTERM')
    const signalExit = await new Promise<number | null>(resolveExit => runnerProcess.once('exit', code => resolveExit(code)))
    expect(signalExit).toBe(1)
    expect(readdirSync(root).filter(name => name.startsWith(prefix))).toEqual([])
  }, 30_000)

  it('keeps repeated operation baselines flat over 100 cycles', () => {
    const root = testRoot()
    const count = () => (awaitImportReaddir(root)).length
    for (let cycle = 0; cycle < 100; cycle++) {
      const path = createBuildDir(root, 606, 20_000_000 + cycle)
      writeFileSync(join(path, 'fixture.bin'), Buffer.alloc(4096))
      cleanupBuildDir(path, root, 606)
      expect(count()).toBe(0)
    }
  })
})

function awaitImportReaddir(path: string): string[] {
  // Keep the tests synchronous so each lifecycle checks the exact post-cleanup baseline.
  return readdirSync(path)
}

function awaitSymlink(path: string, target: string) {
  symlinkSync(target, path)
}
