import { defineConfig, devices } from '@playwright/test'

export default defineConfig({
  testDir: './e2e',
  testMatch: ['peer-playback-webkit.spec.ts'],
  grep: /WebKit plays and seeks encrypted peer HLS media through MSE/,
  fullyParallel: false,
  workers: 1,
  reporter: 'list',
  timeout: 30_000,
  use: {
    ...devices['iPhone 13'],
    browserName: 'webkit',
    baseURL: 'http://127.0.0.1:4174',
  },
  webServer: {
    command: 'npm run dev -- --host 127.0.0.1 --port 4174 --strictPort',
    url: 'http://127.0.0.1:4174/e2e/peer-playback-webkit.html',
    timeout: 60_000,
    reuseExistingServer: false,
  },
})
