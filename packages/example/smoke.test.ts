import { test, expect } from '@playwright/test';

const DISTINCTIVE = '<img src=x onerror="window.unsafeMarkup=true"> ofdb; \' persistent ✓';

test.describe('ofdb browser example', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('http://127.0.0.1:8080');
    await page.waitForSelector('#controls:not([disabled])', { timeout: 15000 });
    await page.waitForSelector('#sql-controls:not([disabled])', { timeout: 15000 });
  });

  test('KV save, commit, reload, retrieve', async ({ page }) => {
    // Save
    await page.fill('#key', 'theme');
    await page.fill('#value', DISTINCTIVE);
    await page.click('#save');
    await expect(page.locator('#status')).toHaveText('Saved and committed.', { timeout: 15000 });

    // Reload
    await page.reload();
    await page.waitForSelector('#controls:not([disabled])', { timeout: 15000 });
    await page.click('#retrieve');
    await expect(page.locator('#status')).toHaveText('Retrieved committed value.', { timeout: 15000 });
    await expect(page.locator('#result')).toHaveText(DISTINCTIVE);
    await expect(page.locator('#result img')).toHaveCount(0);
  });

  test('SQL add, commit, duplicate, reload', async ({ page }) => {
    // Initial state
    await expect(page.locator('#sql-result')).toHaveText('No records yet.');

    // Add first record
    await page.fill('#record', DISTINCTIVE);
    await page.click('#add');
    await expect(page.locator('#sql-status')).toHaveText('Added and committed.', { timeout: 15000 });

    // Add second record (duplicate test)
    await page.fill('#record', DISTINCTIVE + ' second');
    await page.click('#add');
    await expect(page.locator('#sql-status')).toHaveText('Added and committed.', { timeout: 15000 });

    // Reload and list
    await page.reload();
    await page.waitForSelector('#sql-controls:not([disabled])', { timeout: 15000 });
    await page.click('#list');
    await expect(page.locator('#sql-status')).toHaveText('Listed committed records.', { timeout: 15000 });

    const rows = page.locator('#sql-result li');
    await expect(rows).toHaveCount(2);
    await expect(rows.nth(0)).toHaveText(DISTINCTIVE);
    await expect(rows.nth(1)).toHaveText(DISTINCTIVE + ' second');
    await expect(page.locator('#sql-result img')).toHaveCount(0);

    // KV should still work after SQL reload
    await expect(page.locator('#result')).toHaveText(DISTINCTIVE);
  });

  test('storage denied shows error with recovery guidance', async ({ page }) => {
    // Block IndexedDB
    await page.addInitScript(() => {
      const original = indexedDB.open.bind(indexedDB);
      indexedDB.open = () => {
        throw new DOMException('Storage denied by smoke check', 'SecurityError');
      };
    });

    await page.reload();
    await expect(page.locator('#status')).toHaveText(/Error:.*storage.*reload/i, { timeout: 15000 });
    await expect(page.locator('#controls')).toBeDisabled();
    await expect(page.locator('#sql-status')).toHaveText(/Error:.*storage.*reload/i, { timeout: 15000 });
    await expect(page.locator('#sql-controls')).toBeDisabled();
  });
});
