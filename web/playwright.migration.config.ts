import { defineConfig, devices } from '@playwright/test'

const dataDir = process.env.IR_SETUP_E2E_DATA_DIR
if (!dataDir) throw new Error('Run the auth migration test through `npm run test:e2e:setup`.')

export default defineConfig({
  testDir: './e2e',
  testMatch: 'auth-migration.spec.ts',
  outputDir: './test-results/auth-migration',
  fullyParallel: false,
  workers: 1,
  reporter: 'list',
  timeout: 90_000,
  use: { baseURL: 'http://127.0.0.1:4173', trace: 'off', ...devices['Desktop Chrome'], channel: 'chrome' },
})
