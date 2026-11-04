import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: "./browser-tests",
  timeout: 30000,
  workers: 1,
  reporter: "list",
  use: {
    baseURL: "https://sql-tests.localhost:1355",
    ignoreHTTPSErrors: true,
    launchOptions: process.env.BROWSER_PATH
      ? { executablePath: process.env.BROWSER_PATH }
      : {},
  },
  projects: [{ name: "chromium", use: { browserName: "chromium" } }],
  webServer: {
    command: "npx portless sql-tests node browser-tests/server.mjs",
    url: "https://sql-tests.localhost:1355/browser-tests/index.html",
    timeout: 10000,
    reuseExistingServer: false,
  },
});
