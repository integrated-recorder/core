import { defineConfig, devices } from '@playwright/test'
const e2eDataDir = process.env.IR_E2E_DATA_DIR
if (!e2eDataDir) throw new Error('Run Playwright through `npm run test:e2e` so the isolated E2E data directory is shared with its server.')

export default defineConfig({
  testDir: './e2e',
  // First-run setup is a distinct production Host/Control/Engine process test
  // with its own private data directory and credential handoff.
  testIgnore: ['**/setup-first-run.spec.ts'],
  fullyParallel: false,
  workers: 1,
  reporter: 'list',
  timeout: 90_000,
  use: { baseURL: 'http://127.0.0.1:4173', trace: 'retain-on-failure' },
  projects: [{ name: 'chrome', use: { ...devices['Desktop Chrome'], channel: 'chrome' } }],
  webServer: {
    // Keep the signal-handling Node runner as Playwright's direct child.
    // A shell command can be terminated while leaving its Node/Go descendants
    // behind, which skips the runner's owned-scratch cleanup.
    // Playwright launches webServer commands through a shell. `exec` replaces
    // that shell so gracefulShutdown reaches the signal-aware Node runner.
    command: 'exec node e2e/run-backend.mjs',
    url: 'http://127.0.0.1:4173/healthz',
    timeout: 180_000,
    gracefulShutdown: { signal: 'SIGTERM', timeout: 30_000 },
    reuseExistingServer: false,
    env: { IR_E2E_DATA_DIR: e2eDataDir },
  },
})
