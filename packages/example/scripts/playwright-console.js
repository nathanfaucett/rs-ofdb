#!/usr/bin/env node
// Playwright test helper with enhanced console capture
// Usage: node scripts/playwright-console.js

import { connectViaExtension } from 'playwriter';

async function main() {
  await using connection = await connectViaExtension({ tabGroup: 'ofdb' });
  await using page = await connection.browser.contexts()[0].newPage();

  const logs = [];
  const errors = [];

  // Capture all console messages from the start
  page.on('console', msg => {
    logs.push({ type: msg.type(), text: msg.text(), timestamp: Date.now() });
  });

  page.on('pageerror', err => {
    errors.push({ message: err.message, stack: err.stack, timestamp: Date.now() });
  });

  // Capture network responses for WASM loading
  page.on('response', resp => {
    if (resp.url().includes('.wasm') || resp.url().includes('ofdb_wasm')) {
      console.log(`[WASM] ${resp.status()} ${resp.url()}`);
    }
  });

  await page.goto('http://localhost:4722');

  // Wait for app ready with detailed logging
  try {
    await page.waitForFunction(() => {
      const el = document.querySelector('p[role=status]');
      return el && el.textContent.includes('Ready');
    }, { timeout: 180000 });

    console.log('\n=== APP READY ===');
    console.log('Status:', await page.locator('p[role=status]').textContent());

  } catch (e) {
    console.log('\n=== TIMEOUT - DUMPING LOGS ===');
    console.log('Console logs:', JSON.stringify(logs, null, 2));
    console.log('Page errors:', JSON.stringify(errors, null, 2));
    process.exit(1);
  }

  // Dump all captured logs
  console.log('\n=== ALL CONSOLE LOGS ===');
  logs.forEach(l => console.log(`[${l.type}] ${l.text}`));

  console.log('\n=== ALL PAGE ERRORS ===');
  errors.forEach(e => console.log(`[ERROR] ${e.message}`));
}

main().catch(e => {
  console.error('Fatal:', e);
  process.exit(1);
});
