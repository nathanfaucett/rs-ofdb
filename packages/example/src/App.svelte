<script lang="ts">
    import { KvPeerNetwork, type KvPeer } from "./sync/kv-network.js";
    import { SqlPeerNetwork, type SqlPeer } from "./sync/sql-network.js";
    import KvTab from "./KvTab.svelte";
    import SqlTab from "./SqlTab.svelte";

    let activeTab = $state<"kv" | "sql">("kv");

    // KV state
    let kvNetwork = $state<KvPeerNetwork | null>(null);
    let kvPeers = $state<KvPeer[]>([]);
    let kvKey = $state("theme");
    let kvStatus = $state("Starting…");
    let kvStatusError = $state(false);
    let kvBusy = $state(true);

    // SQL state
    let sqlNetwork = $state<SqlPeerNetwork | null>(null);
    let sqlPeers = $state<SqlPeer[]>([]);
    let sqlStatus = $state("Starting…");
    let sqlStatusError = $state(false);
    let sqlBusy = $state(true);

    // Per-peer editor state
    const kvEditorState = $state<
        Record<string, { key: string; value: string }>
    >({});
    const sqlEditorState = $state<
        Record<string, { text: string; records: string[] }>
    >({});

    function msg(error: unknown): string {
        return error instanceof Error ? error.message : String(error);
    }

    function kvReport(text: string, error = false): void {
        kvStatus = text;
        kvStatusError = error;
    }

    function getKvState(peerId: string) {
        return kvEditorState[peerId];
    }

    function getSqlState(peerId: string) {
        return sqlEditorState[peerId];
    }

    function ensureKvState(peerId: string): void {
        if (!(peerId in kvEditorState)) {
            kvEditorState[peerId] = { key: kvKey, value: "" };
        }
    }

    function ensureSqlState(peerId: string): void {
        if (!(peerId in sqlEditorState)) {
            sqlEditorState[peerId] = { text: "", records: [] };
        }
    }

    async function refreshKvValues(): Promise<void> {
        if (!kvNetwork) return;
        for (const peer of kvNetwork.getPeers()) {
            const state = getKvState(peer.id);
            try {
                const val = await peer.client.get(state.key);
                if (val === undefined) {
                    state.value = "—";
                } else {
                    state.value =
                        typeof val === "string" ? val : JSON.stringify(val);
                }
            } catch {
                state.value = "error";
            }
        }
    }

    // Initialize KV network once at component creation
    void (async () => {
        try {
            const network = new KvPeerNetwork("ofdb-example-kv");
            network.setListener(() => {
                kvPeers = network.getPeers();
                for (const peer of kvPeers) {
                    ensureKvState(peer.id);
                }
                void refreshKvValues();
            });
            kvNetwork = network;
            await network.addPeer();
            kvPeers = network.getPeers();
            for (const peer of kvPeers) {
                ensureKvState(peer.id);
            }
            kvBusy = false;
            await refreshKvValues();
            kvReport("Ready. Add peers and toggle them offline to test sync.");
        } catch (error) {
            kvReport(`Error: ${msg(error)}`, true);
        }
    })();

    // Initialize SQL network once at component creation
    void (async () => {
        try {
            const network = new SqlPeerNetwork("ofdb-example-sql");
            network.setListener(() => {
                sqlPeers = network.getPeers();
                for (const peer of sqlPeers) {
                    ensureSqlState(peer.id);
                }
                void refreshSqlRecords();
            });
            sqlNetwork = network;
            await network.addPeer();
            sqlPeers = network.getPeers();
            for (const peer of sqlPeers) {
                ensureSqlState(peer.id);
            }
            sqlBusy = false;
            await refreshSqlRecords();
            sqlStatus =
                "Ready. Add peers and toggle them offline to test sync.";
        } catch (error) {
            sqlStatusError = true;
            sqlStatus = `Error: ${msg(error)}`;
        }
    })();

    async function kvSave(peerId: string): Promise<void> {
        if (!kvNetwork || kvBusy) return;
        const state = getKvState(peerId);
        if (!state.key.trim()) return;
        kvBusy = true;
        kvReport("Saving…");
        try {
            await kvNetwork.write(peerId, state.key, state.value);
            await refreshKvValues();
            kvReport("Saved (sync runs automatically).");
        } catch (error) {
            kvReport(`Error: ${msg(error)}`, true);
        } finally {
            kvBusy = false;
        }
    }

    async function kvRetrieve(peerId: string): Promise<void> {
        if (kvBusy) return;
        const state = getKvState(peerId);
        if (!state.key.trim()) return;
        kvBusy = true;
        kvReport("Reading…");
        try {
            const peer = kvNetwork!.getPeer(peerId);
            if (peer) {
                const val = await peer.client.get(state.key);
                state.value =
                    val === undefined
                        ? "—"
                        : typeof val === "string"
                          ? val
                          : JSON.stringify(val);
                kvReport("Read from peer.");
            }
        } catch (error) {
            kvReport(`Error: ${msg(error)}`, true);
        } finally {
            kvBusy = false;
        }
    }

    async function addKvPeer(): Promise<void> {
        if (!kvNetwork) return;
        kvBusy = true;
        try {
            await kvNetwork.addPeer();
            kvPeers = kvNetwork.getPeers();
            await refreshKvValues();
            kvReport(`Added peer. Total: ${kvPeers.length}`);
        } catch (error) {
            kvReport(`Error: ${msg(error)}`, true);
        } finally {
            kvBusy = false;
        }
    }

    async function removeKvPeer(id: string): Promise<void> {
        if (!kvNetwork) return;
        await kvNetwork.removePeer(id);
        kvPeers = kvNetwork.getPeers();
        delete kvEditorState[id];
        await refreshKvValues();
    }

    async function toggleKvPeer(id: string, online: boolean): Promise<void> {
        if (!kvNetwork) return;
        await kvNetwork.setOnline(id, online);
        // Peer object is same reference; UI reads online/syncing/error directly
        await refreshKvValues();
    }

    async function sqlAdd(peerId: string): Promise<void> {
        if (!sqlNetwork || sqlBusy) return;
        const state = getSqlState(peerId);
        if (!state.text.trim()) return;
        sqlBusy = true;
        sqlStatusError = false;
        sqlStatus = "Adding…";
        try {
            await sqlNetwork.addRecord(peerId, state.text);
            state.text = "";
            await refreshSqlRecords();
            sqlStatus = "Added (sync runs automatically).";
        } catch (error) {
            sqlStatusError = true;
            sqlStatus = `Error: ${msg(error)}`;
        } finally {
            sqlBusy = false;
        }
    }

    async function refreshSqlRecords(): Promise<void> {
        if (!sqlNetwork) return;
        for (const peer of sqlNetwork.getPeers()) {
            const state = getSqlState(peer.id);
            try {
                state.records = await sqlNetwork.listRecords(peer);
            } catch {
                state.records = [];
            }
        }
    }

    async function addSqlPeer(): Promise<void> {
        if (!sqlNetwork) return;
        sqlBusy = true;
        try {
            await sqlNetwork.addPeer();
            sqlPeers = sqlNetwork.getPeers();
            await refreshSqlRecords();
            sqlStatus = `Added peer. Total: ${sqlPeers.length}`;
        } catch (error) {
            sqlStatusError = true;
            sqlStatus = `Error: ${msg(error)}`;
        } finally {
            sqlBusy = false;
        }
    }

    async function removeSqlPeer(id: string): Promise<void> {
        if (!sqlNetwork) return;
        await sqlNetwork.removePeer(id);
        sqlPeers = sqlNetwork.getPeers();
        delete sqlEditorState[id];
        await refreshSqlRecords();
    }

    async function toggleSqlPeer(id: string, online: boolean): Promise<void> {
        if (!sqlNetwork) return;
        await sqlNetwork.setOnline(id, online);
        // Peer object is same reference; UI reads online/syncing/error directly
        await refreshSqlRecords();
    }
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
            <KvTab
                peers={kvPeers}
                busy={kvBusy}
                status={kvStatus}
                statusError={kvStatusError}
                onAdd={addKvPeer}
                onSave={kvSave}
                onRetrieve={kvRetrieve}
                onToggle={toggleKvPeer}
                onRemove={removeKvPeer}
                getState={getKvState}
            />
        </div>
    {/if}

    <!-- SQL Tab -->
    {#if activeTab === "sql"}
        <div class="pt-6">
            <SqlTab
                peers={sqlPeers}
                busy={sqlBusy}
                status={sqlStatus}
                statusError={sqlStatusError}
                onAdd={addSqlPeer}
                onAddRecord={sqlAdd}
                onToggle={toggleSqlPeer}
                onRemove={removeSqlPeer}
                getState={getSqlState}
            />
        </div>
    {/if}

    <p class="mt-8 text-sm text-gray-500">
        Storage belongs to this browser origin. Peers sync via an in-memory
        transport (MessageChannel-like); replace with WebRTC/WebSocket in
        production.
    </p>
    <noscript>Enable JavaScript and reload to run this Wasm example.</noscript>
</main>
