<script lang="ts">
// biome-ignore lint/correctness/noUnusedImports: Used in template
import KvTab from "./KvTab.svelte";
// biome-ignore lint/correctness/noUnusedImports: Used in template
import SqlTab from "./SqlTab.svelte";
import { setKvContext, setSqlContext } from "./stores/context.js";
import {
  createNetworkState,
  ensureState,
  getDefaultKvState,
  getDefaultSqlState,
} from "./stores/peer-network.js";
// Required for Vite to bundle WASM - do not change to type-only import
// biome-ignore lint/correctness/noUnusedImports: Used in generic type parameters and at runtime
import { type KvPeer, KvPeerNetwork } from "./sync/kv-network.js";
// Required for Vite to bundle WASM - do not change to type-only import
// biome-ignore lint/correctness/noUnusedImports: Used in generic type parameters and at runtime
import { type SqlPeer, SqlPeerNetwork } from "./sync/sql-network.js";

let activeTab = $state<"kv" | "sql">("kv");

let kvState = $state(
  createNetworkState<KvPeerNetwork, { value: string }>("Starting…"),
);
let sqlState = $state(
  createNetworkState<SqlPeerNetwork, { text: string; records: string[] }>(
    "Starting…",
  ),
);

let kvKey = $state("theme");

function msg(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function kvReport(text: string, error = false): void {
  kvState.status = text;
  kvState.statusError = error;
}

function sqlReport(text: string, error = false): void {
  sqlState.status = text;
  sqlState.statusError = error;
}

async function refreshKvValues(): Promise<void> {
  if (!kvState.network) return;
  for (const peer of kvState.network.getPeers()) {
    const editorState = kvState.editorState[peer.id];
    if (!editorState) continue;
    try {
      const val = await peer.client.get(kvKey);
      editorState.value =
        val === undefined
          ? "\u2014"
          : typeof val === "string"
            ? val
            : JSON.stringify(val);
    } catch {
      editorState.value = "error";
    }
  }
}

async function refreshSqlRecords(): Promise<void> {
  if (!sqlState.network) return;
  for (const peer of sqlState.network.getPeers()) {
    const state = sqlState.editorState[peer.id];
    if (!state) continue;
    try {
      state.records = await sqlState.network.listRecords(peer);
    } catch {
      state.records = [];
    }
  }
}

async function initKvNetwork(): Promise<void> {
  try {
    const network = new KvPeerNetwork("ofdb-example-kv");
    network.setListener(() => {
      kvState.peers = network.getPeers();
      for (const peer of kvState.peers) {
        ensureState(kvState.editorState, peer.id, getDefaultKvState());
      }
      void refreshKvValues();
    });
    kvState.network = network;
    await network.addPeer();
    kvState.peers = network.getPeers();
    for (const peer of kvState.peers) {
      ensureState(kvState.editorState, peer.id, getDefaultKvState());
    }
    kvState.busy = false;
    await refreshKvValues();
    kvReport("Ready. Add peers and toggle them offline to test sync.");
  } catch (error) {
    kvReport(`Error: ${msg(error)}`, true);
  }
}

async function initSqlNetwork(): Promise<void> {
  try {
    const network = new SqlPeerNetwork("ofdb-example-sql");
    network.setListener(() => {
      sqlState.peers = network.getPeers();
      for (const peer of sqlState.peers) {
        ensureState(sqlState.editorState, peer.id, getDefaultSqlState());
      }
      void refreshSqlRecords();
    });
    sqlState.network = network;
    await network.addPeer();
    sqlState.peers = network.getPeers();
    for (const peer of sqlState.peers) {
      ensureState(sqlState.editorState, peer.id, getDefaultSqlState());
    }
    sqlState.busy = false;
    await refreshSqlRecords();
    sqlReport("Ready. Add peers and toggle them offline to test sync.");
  } catch (error) {
    sqlReport(`Error: ${msg(error)}`, true);
  }
}

$effect(() => {
  initKvNetwork();
});

$effect(() => {
  initSqlNetwork();
});

$effect(() => {
  kvKey;
  if (kvState.network && !kvState.busy) {
    void refreshKvValues();
  }
});

const kvActions = {
  async addPeer(): Promise<void> {
    if (!kvState.network) return;
    kvState.busy = true;
    try {
      await kvState.network.addPeer();
      kvState.peers = kvState.network.getPeers();
      await refreshKvValues();
      kvReport(`Added peer. Total: ${kvState.peers.length}`);
    } catch (error) {
      kvReport(`Error: ${msg(error)}`, true);
    } finally {
      kvState.busy = false;
    }
  },
  async removePeer(id: string): Promise<void> {
    if (!kvState.network) return;
    await kvState.network.removePeer(id);
    kvState.peers = kvState.network.getPeers();
    delete kvState.editorState[id];
    await refreshKvValues();
  },
  async togglePeer(id: string, online: boolean): Promise<void> {
    if (!kvState.network) return;
    await kvState.network.setOnline(id, online);
    await refreshKvValues();
  },
  async save(peerId: string): Promise<void> {
    if (!kvState.network || kvState.busy) return;
    if (!kvKey.trim()) return;
    kvState.busy = true;
    kvReport("Saving…");
    try {
      const state = kvState.editorState[peerId];
      if (!state) return;
      await kvState.network.write(peerId, kvKey, state.value);
      await refreshKvValues();
      kvReport("Saved (sync runs automatically).");
    } catch (error) {
      kvReport(`Error: ${msg(error)}`, true);
    } finally {
      kvState.busy = false;
    }
  },
  async retrieve(peerId: string): Promise<void> {
    if (kvState.busy) return;
    if (!kvKey.trim()) return;
    kvState.busy = true;
    kvReport("Reading…");
    try {
      const peer = kvState.network?.getPeer(peerId);
      if (peer) {
        const val = await peer.client.get(kvKey);
        const editorState = kvState.editorState[peerId];
        if (editorState) {
          editorState.value =
            val === undefined
              ? "\u2014"
              : typeof val === "string"
                ? val
                : JSON.stringify(val);
        }
        kvReport("Read from peer.");
      }
    } catch (error) {
      kvReport(`Error: ${msg(error)}`, true);
    } finally {
      kvState.busy = false;
    }
  },
  async refreshValues(): Promise<void> {
    await refreshKvValues();
  },
};

const sqlActions = {
  async addPeer(): Promise<void> {
    if (!sqlState.network) return;
    sqlState.busy = true;
    try {
      await sqlState.network.addPeer();
      sqlState.peers = sqlState.network.getPeers();
      await refreshSqlRecords();
      sqlReport(`Added peer. Total: ${sqlState.peers.length}`);
    } catch (error) {
      sqlReport(`Error: ${msg(error)}`, true);
    } finally {
      sqlState.busy = false;
    }
  },
  async removePeer(id: string): Promise<void> {
    if (!sqlState.network) return;
    await sqlState.network.removePeer(id);
    sqlState.peers = sqlState.network.getPeers();
    delete sqlState.editorState[id];
    await refreshSqlRecords();
  },
  async togglePeer(id: string, online: boolean): Promise<void> {
    if (!sqlState.network) return;
    await sqlState.network.setOnline(id, online);
    await refreshSqlRecords();
  },
  async addRecord(peerId: string): Promise<void> {
    if (!sqlState.network || sqlState.busy) return;
    const state = sqlState.editorState[peerId];
    if (!state?.text.trim()) return;
    sqlState.busy = true;
    sqlReport("Adding…");
    try {
      await sqlState.network.addRecord(peerId, state.text);
      state.text = "";
      await refreshSqlRecords();
      sqlReport("Added (sync runs automatically).");
    } catch (error) {
      sqlReport(`Error: ${msg(error)}`, true);
    } finally {
      sqlState.busy = false;
    }
  },
  async refreshRecords(): Promise<void> {
    await refreshSqlRecords();
  },
};

setKvContext({
  get network() {
    return kvState.network;
  },
  get peers() {
    return kvState.peers;
  },
  get busy() {
    return kvState.busy;
  },
  get status() {
    return kvState.status;
  },
  get statusError() {
    return kvState.statusError;
  },
  get key() {
    return kvKey;
  },
  setKey(key: string) {
    kvKey = key;
  },
  get editorState() {
    return kvState.editorState;
  },
  addPeer: kvActions.addPeer,
  removePeer: kvActions.removePeer,
  togglePeer: kvActions.togglePeer,
  save: kvActions.save,
  retrieve: kvActions.retrieve,
  refreshValues: kvActions.refreshValues,
});

setSqlContext({
  get network() {
    return sqlState.network;
  },
  get peers() {
    return sqlState.peers;
  },
  get busy() {
    return sqlState.busy;
  },
  get status() {
    return sqlState.status;
  },
  get statusError() {
    return sqlState.statusError;
  },
  get editorState() {
    return sqlState.editorState;
  },
  addPeer: sqlActions.addPeer,
  removePeer: sqlActions.removePeer,
  togglePeer: sqlActions.togglePeer,
  addRecord: sqlActions.addRecord,
  refreshRecords: sqlActions.refreshRecords,
});
</script>

<main class="mx-auto max-w-4xl px-4 py-8 font-sans text-base leading-relaxed">
    <h1 class="mb-2 text-2xl font-bold">Browser KV & SQL with Sync</h1>
    <p class="mb-6 text-gray-600">
        Each tab runs multiple peers with their own IndexedDB database. Peers
        sync automatically when online. Toggle a peer offline to simulate losing
        internet, write on another peer, then bring it back online to catch up.
    </p>

    <!-- Tab Navigation -->
    <div class="mb-6 flex border-b border-gray-300">
        <button
            class="rounded-t-lg border-b-2 px-4 py-2 font-medium transition-colors {activeTab ===
            'kv'
                ? 'border-blue-600 bg-white text-blue-600'
                : 'border-transparent text-gray-500 hover:bg-gray-50 hover:text-gray-700'}"
            onclick={() => (activeTab = "kv")}
        >
            KV Store
        </button>
        <button
            class="rounded-t-lg border-b-2 px-4 py-2 font-medium transition-colors {activeTab ===
            'sql'
                ? 'border-blue-600 bg-white text-blue-600'
                : 'border-transparent text-gray-500 hover:bg-gray-50 hover:text-gray-700'}"
            onclick={() => (activeTab = "sql")}
        >
            SQL Store
        </button>
    </div>

    <!-- KV Tab -->
    {#if activeTab === "kv"}
        <div class="pt-6">
            <KvTab />
        </div>
    {/if}

    <!-- SQL Tab -->
    {#if activeTab === "sql"}
        <div class="pt-6">
            <SqlTab />
        </div>
    {/if}

    <p class="mt-8 text-sm text-gray-500">
        Storage belongs to this browser origin. Peers sync via an in-memory
        transport (MessageChannel-like); replace with WebRTC/WebSocket in
        production.
    </p>
    <noscript>Enable JavaScript and reload to run this Wasm example.</noscript>
</main>
