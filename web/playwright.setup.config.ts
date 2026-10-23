import { defineConfig, devices } from '@playwright/test'

const dataDir = process.env.IR_SETUP_E2E_DATA_DIR
const restartFile = process.env.IR_SETUP_E2E_RESTART_FILE
const webkit = process.env.IR_SETUP_E2E_BROWSER === 'webkit'
if (!dataDir || !restartFile) {
  throw new Error('Run the real-product first-run E2E through `npm run test:e2e:setup`.')
}

export default defineConfig({
  testDir: './e2e',
  testMatch: 'setup-first-run.spec.ts',
  outputDir: './test-results/setup-first-run',
  fullyParallel: false,
  workers: 1,
  reporter: 'list',
  timeout: 180_000,
  expect: { timeout: 20_000 },
  use: {
    baseURL: 'http://127.0.0.1:4173',
    // Credential form traces stay disabled to avoid retaining password values.
    trace: 'off',
    ...(webkit ? { browserName: 'webkit' as const, viewport: { width: 1280, height: 720 } } : { ...devices['Desktop Chrome'], channel: 'chrome' }),
  },
})
