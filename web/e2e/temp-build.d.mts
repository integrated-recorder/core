export function createBuildDir(parent?: string, pid?: number, now?: number, options?: { isAlive?: (pid: number) => boolean }): string
export function cleanupBuildDir(
  path: string,
  parent?: string,
  pid?: number,
  options?: { removeEntry?: (path: string) => void },
): boolean
export function cleanupStaleBuildDirs(
  parent?: string,
  options?: { now?: number; isAlive?: (pid: number) => boolean },
): number
