#!/usr/bin/env bash
# Test setup helper for playwriter E2E tests
# Usage: source scripts/test-setup.sh && wait_for_ready

set -euo pipefail

SESSION_ID=""

playwriter_new_session() {
  SESSION_ID=$(npx playwriter@latest session new --tab-group ofdb | grep -oE 'Session [0-9]+' | cut -d' ' -f2)
  echo "Created session: $SESSION_ID"
}

playwriter_navigate() {
  local url="${1:-http://localhost:4722}"
  npx playwriter@latest -s "$SESSION_ID" -e "
    state.page = await context.newPage();
    await state.page.goto('$url');
  "
}

wait_for_ready() {
  local timeout="${1:-180000}"
  echo "Waiting for app to be ready (timeout: ${timeout}ms)..."
  npx playwriter@latest -s "$SESSION_ID" --timeout "$timeout" -e '
    await state.page.waitForFunction(() => {
      const el = document.querySelector("p[role=status]");
      return el && el.textContent.includes("Ready");
    });
    console.log("App ready:", await state.page.locator("p[role=status]").textContent());
  '
  echo "✓ App is ready"
}

playwriter_cleanup() {
  if [ -n "$SESSION_ID" ]; then
    npx playwriter@latest session delete "$SESSION_ID" 2>/dev/null || true
  fi
}

# If sourced, export functions; if executed, run full setup
if [[ "${BASH_SOURCE[0]}" != "${0}" ]]; then
  export -f playwriter_new_session playwriter_navigate wait_for_ready playwriter_cleanup
else
  playwriter_new_session
  playwriter_navigate "$@"
  wait_for_ready
fi
