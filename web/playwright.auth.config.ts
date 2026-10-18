import { defineConfig, devices } from '@playwright/test'

const e2eDataDir = process.env.IR_E2E_DATA_DIR
if (!e2eDataDir) throw new Error('Run the focused credential suite through `npm run test:e2e:auth` so its isolated data directory is shared with the server.')

export default defineConfig({
  testDir: './e2e',
  testMatch: ['auth-credentials.spec.ts', 'https-server.spec.ts'],
  fullyParallel: false,
  workers: 1,
  reporter: 'list',
  timeout: 90_000,
  use: { ...devices['Desktop Safari'], locale: 'ko-KR', baseURL: 'http://127.0.0.1:4173', trace: 'retain-on-failure', ignoreHTTPSErrors: true },
  projects: [{ name: 'webkit-auth', use: { ...devices['Desktop Safari'], locale: 'ko-KR', ignoreHTTPSErrors: true } }],
  webServer: {
    command: 'exec node e2e/run-backend.mjs',
    url: 'http://127.0.0.1:4173/healthz',
    timeout: 180_000,
    gracefulShutdown: { signal: 'SIGTERM', timeout: 30_000 },
    reuseExistingServer: false,
    env: { IR_E2E_DATA_DIR: e2eDataDir },
  },
})
