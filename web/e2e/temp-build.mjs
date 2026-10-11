import { chmodSync, lstatSync, mkdtempSync, readFileSync, readdirSync, rmSync, rmdirSync, writeFileSync } from 'node:fs'
import { basename, dirname, join, resolve } from 'node:path'
import { tmpdir } from 'node:os'
import process from 'node:process'

const prefix = 'integrated-recorder-e2e-build-'
const markerName = '.ir-e2e-build-owner.json'
// Leave a short grace period for a detached `go build` child after the Node
// runner is killed; dead owners are otherwise reclaimed on the next E2E run.
const staleAfterMs = 5 * 60 * 1000

function ownedDirectory(path, parent) {
  const resolvedParent = resolve(parent)
  const resolvedPath = resolve(path)
  if (dirname(resolvedPath) !== resolvedParent || !basename(resolvedPath).startsWith(prefix)) return false
  try {
    const info = lstatSync(resolvedPath)
    return info.isDirectory() && !info.isSymbolicLink() && info.uid === process.getuid?.()
  } catch {
    return false
  }
}

function readMarker(path) {
  try {
    const markerPath = join(path, markerName)
    const info = lstatSync(markerPath)
    if (!info.isFile() || info.isSymbolicLink() || info.uid !== process.getuid?.()) return null
    const marker = JSON.parse(readFileSync(markerPath, 'utf8'))
    if (marker.version !== 1 || !Number.isSafeInteger(marker.pid) || marker.pid < 1 ||
        !Number.isSafeInteger(marker.createdAtMs) || marker.createdAtMs < 1) return null
    return marker
  } catch {
    return null
  }
}

function processIsAlive(pid) {
  try {
    process.kill(pid, 0)
    return true
  } catch (error) {
    return error?.code !== 'ESRCH'
  }
}

function makeOwnedDirectoriesWritable(path) {
  let info
  try {
    info = lstatSync(path)
  } catch {
    return false
  }
  if (info.isSymbolicLink() || !info.isDirectory()) return true
  try {
    // Go's module cache is copied with read-only directories. Make only
    // directories below the marker-owned scratch root removable; never follow
    // a symlink into another application's files.
    chmodSync(path, 0o700)
    for (const name of readdirSync(path)) {
      if (!makeOwnedDirectoriesWritable(join(path, name))) return false
    }
    return true
  } catch {
    return false
  }
}

function removeOwnedDirectory(path, removeEntry = (target) => rmSync(target, { recursive: true, force: true, maxRetries: 5, retryDelay: 50 })) {
  const markerPath = join(path, markerName)
  let markerContents
  let entries
  try {
    markerContents = readFileSync(markerPath)
    if (!makeOwnedDirectoriesWritable(path)) return false
    entries = readdirSync(path)
  } catch {
    return false
  }
  for (const name of entries) {
    if (name === markerName) continue
    try {
      removeEntry(join(path, name))
    } catch {
      // Keep the ownership marker so a later invocation can retry safely.
      return false
    }
  }
  try {
    if (readdirSync(path).some(name => name !== markerName)) return false
    rmSync(markerPath, { force: true, maxRetries: 5, retryDelay: 50 })
    rmdirSync(path)
    return true
  } catch {
    // If removing the empty root fails, restore its marker when possible.
    try { writeFileSync(markerPath, markerContents, { flag: 'wx', mode: 0o600 }) } catch { /* best effort */ }
    return false
  }
}

export function createBuildDir(parent = tmpdir(), pid = process.pid, now = Date.now(), options = {}) {
  const root = resolve(parent)
  cleanupStaleBuildDirs(root, { now, isAlive: options.isAlive ?? processIsAlive })
  const path = mkdtempSync(join(root, prefix))
  try {
    writeFileSync(join(path, markerName), JSON.stringify({ version: 1, pid, createdAtMs: now }), { flag: 'wx', mode: 0o600 })
  } catch (error) {
    rmSync(path, { recursive: true, force: true, maxRetries: 5, retryDelay: 50 })
    throw error
  }
  return path
}

export function cleanupBuildDir(path, parent = tmpdir(), pid = process.pid, options = {}) {
  if (!ownedDirectory(path, parent)) return false
  const marker = readMarker(path)
  if (!marker || marker.pid !== pid) return false
  return removeOwnedDirectory(path, options.removeEntry)
}

export function cleanupStaleBuildDirs(parent = tmpdir(), { now = Date.now(), isAlive = processIsAlive } = {}) {
  const root = resolve(parent)
  let entries
  try {
    entries = readdirSync(root, { withFileTypes: true })
  } catch {
    return 0
  }
  let removed = 0
  for (const entry of entries) {
    if (!entry.name.startsWith(prefix)) continue
    const path = join(root, entry.name)
    if (!ownedDirectory(path, root)) continue
    const marker = readMarker(path)
    if (!marker || now - marker.createdAtMs < staleAfterMs || isAlive(marker.pid)) continue
    if (removeOwnedDirectory(path)) removed++
    // Best effort: a later E2E run can retry the same owned orphan.
  }
  return removed
}
