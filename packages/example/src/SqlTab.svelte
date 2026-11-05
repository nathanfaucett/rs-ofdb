<script lang="ts">
    import { type SqlPeer } from "./sync/sql-network.js";
    import PeerPanel from "./PeerPanel.svelte";

    interface Props {
        peers: SqlPeer[];
        busy: boolean;
        status: string;
        statusError: boolean;
        onAdd: () => void;
        onAddRecord: (peerId: string) => void;
        onToggle: (id: string, online: boolean) => void;
        onRemove: (id: string) => void;
        getState: (peerId: string) => { text: string; records: string[] };
    }

    let {
        peers,
        busy,
        status,
        statusError,
        onAdd,
        onAddRecord,
        onToggle,
        onRemove,
        getState,
    }: Props = $props();
</script>

<div>
    <p
        role="status"
        aria-live="polite"
        data-error={String(statusError)}
        class={statusError ? "mb-4 font-bold" : "mb-4"}
    >
        {status}
    </p>

    <div class="mb-4 flex items-center justify-between">
        <h2 class="text-lg font-semibold">Peers ({peers.length})</h2>
        <button
            onclick={onAdd}
            disabled={busy}
            class="rounded bg-green-600 px-3 py-1.5 text-sm text-white hover:bg-green-700 disabled:opacity-50"
            >+ Add Peer</button
        >
    </div>

    <div class="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
        {#each peers as peer (peer.id)}
            {@const sqlState = getState(peer.id)}
            <PeerPanel
                {peer}
                canRemove={peers.length > 1}
                onToggleOnline={onToggle}
                {onRemove}
            >
                <div class="mb-2 border-t border-gray-200 pt-2 space-y-2">
                    <label
                        for={`sql-text-${peer.id}`}
                        class="block text-sm font-medium">Record text</label
                    >
                    <input
                        id={`sql-text-${peer.id}`}
                        bind:value={sqlState.text}
                        class="w-full rounded border border-gray-300 px-2 py-1.5 text-sm"
                        placeholder="Enter record text..."
                        disabled={busy || !peer.online}
                    />
                    <button
                        class="rounded bg-blue-600 px-3 py-1.5 text-sm text-white hover:bg-blue-700 disabled:opacity-50"
                        onclick={() => onAddRecord(peer.id)}
                        disabled={busy || !peer.online || !sqlState.text.trim()}
                    >
                        Add & Sync
                    </button>
                </div>

                <div class="flex flex-col gap-1">
                    <span class="text-xs font-medium text-gray-500">
                        {peer.recordCount} record{peer.recordCount === 1
                            ? ""
                            : "s"}
                    </span>
                    {#if (sqlState.records ?? []).length > 0}
                        <ol class="list-decimal pl-5 text-[13px]">
                            {#each sqlState.records as row, i (i)}
                                <li>{row}</li>
                            {/each}
                        </ol>
                    {/if}
                </div>

                {#if peer.error}
                    <p class="mb-1 text-xs text-red-700">{peer.error}</p>
                {:else if peer.lastSync}
                    <p class="mb-1 text-xs text-gray-500">{peer.lastSync}</p>
                {/if}

                {#if peer.syncing}
                    <span
                        class="rounded-full bg-violet-100 px-2 py-0.5 text-xs text-violet-800"
                        >Syncing…</span
                    >
                {/if}
            </PeerPanel>
        {/each}
    </div>
</div>
