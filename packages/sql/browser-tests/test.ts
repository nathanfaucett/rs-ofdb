import { expect, test as base } from "@playwright/test";

export { expect };
export const test = base.extend({
  page: async ({ page }, use) => {
    await use(page);
    await page.evaluate(async () => {
      for (const { name } of await indexedDB.databases()) {
        if (!name?.startsWith("ofdb:sql:")) continue;
        await new Promise<void>((resolve, reject) => {
          const request = indexedDB.deleteDatabase(name);
          request.onsuccess = () => resolve();
          request.onerror = () => reject(request.error);
          request.onblocked = () =>
            reject(
              new Error("owned SQL handles leaked an IndexedDB connection"),
            );
        });
      }
    });
  },
});
