import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: "./browser-tests",
  timeout: 30000,
  workers: 1,
  reporter: "list",
  use: {
    baseURL: "http://127.0.0.1:8091",
    ignoreHTTPSErrors: false,
    launchOptions: {
      executablePath:
        "/home/nathan/.cache/ms-playwright/chromium-1254/chrome-linux64/chrome",
    },
  },
  projects: [{ name: "chromium", use: { browserName: "chromium" } }],
});
