import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: "./browser-tests",
  timeout: 30000,
  workers: 1,
  reporter: "list",
  use: {
    baseURL: "http://127.0.0.1:8091",
    ignoreHTTPSErrors: false,
    launchOptions: process.env.BROWSER_PATH
      ? { executablePath: process.env.BROWSER_PATH }
      : {},
  },
  projects: [{ name: "chromium", use: { browserName: "chromium" } }],
  webServer: {
    command: "node browser-tests/server.mjs",
    url: "http://127.0.0.1:8091/browser-tests/index.html",
    timeout: 10000,
    reuseExistingServer: false,
  },
});
