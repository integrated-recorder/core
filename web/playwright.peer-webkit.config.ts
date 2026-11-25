import { defineConfig, devices } from '@playwright/test'

export default defineConfig({
  testDir: './e2e',
  testMatch: ['peer-playback-webkit.spec.ts', 'node-enrollment-qr.spec.ts'],
  fullyParallel: false,
  workers: 1,
  reporter: 'list',
  timeout: 30_000,
  use: { ...devices['Desktop Safari'], baseURL: 'http://127.0.0.1:4174' },
  projects: [{ name: 'webkit-peer-crypto-playback', use: { ...devices['Desktop Safari'], browserName: 'webkit' } }],
  webServer: {
    command: 'npm run dev -- --host 127.0.0.1 --port 4174 --strictPort',
    url: 'http://127.0.0.1:4174/e2e/peer-playback-webkit.html',
    timeout: 60_000,
    reuseExistingServer: false,
  },
})
