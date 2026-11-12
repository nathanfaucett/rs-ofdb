# Coding Standards

## Svelte 5 Patterns

### `$effect` for Async Initialization

**Acceptable** when no reactive dependency exists for one-time async initialization.

```svelte
<script>
  // side-effect: initialization
  $effect(() => {
    initNetwork();
  });
</script>
```

**Rationale**: Svelte 5 does not allow top-level `await` in `<script>`. `$effect` with no dependencies runs once on mount, making it the idiomatic replacement for `onMount`.

**Do not** use `$derived` for async initialization — it expects a synchronous expression.

---

### WASM Bundle Requirements

**Critical**: Static imports of `@ofsdk/kv` and `@ofsdk/sql` network classes are required for Vite to bundle WASM files.

```svelte
// Required for Vite to bundle WASM - do not change to type-only import
import { KvPeerNetwork } from "./sync/kv-network.js";
```

Changing to `import type { KvPeerNetwork }` will cause WASM files to be excluded from the build output.

---

### Context API over Prop Drilling

Prefer Svelte context (`setContext`/`getContext`) for sharing reactive state across component boundaries. Eliminates 10+ props and makes components more composable.

---

### Reactive State Organization

Group related reactive state into a single `$state` object rather than scattered individual `$state` declarations:

```svelte
// Preferred
let state = $state({
  network: null,
  peers: [],
  busy: true,
  status: "Starting…",
  editorState: {}
});

// Avoid
let network = $state(null);
let peers = $state([]);
let busy = $state(true);
// ...
```

---

### Accessibility

- Dialogs: Use `role="dialog"`, `aria-modal="true"`, `aria-labelledby`
- Status messages: Use `role="status"`, `aria-live="polite"`
- Buttons: Ensure accessible names via text content or `aria-label`

---

### WASM Async Code Pattern

**Rule**: WASM-targeted async code MUST NOT use `tokio`. Use `web-sys` + `futures` instead. See `crates/sync/src/wasm_sync.rs` for the required rewrite pattern.

### WASM Export Contract

**Rule**: When adding new WASM types, export in `crates/wasm/src/lib.rs` AND re-export in TypeScript package (`packages/kv|sql/src/database.ts` → `index.ts`).
