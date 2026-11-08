<script lang="ts">
// biome-ignore lint/correctness/noUnusedImports: Used in template
import PeerPanel from "./PeerPanel.svelte";
import { getSqlContext } from "./stores/context.js";

const ctx = getSqlContext();
</script>

<div>
    <p
        role="status"
        aria-live="polite"
        data-error={String(ctx.statusError)}
        class={ctx.statusError ? "mb-4 font-bold text-red-700" : "mb-4"}
    >
        {ctx.status}
    </p>

    <div class="mb-4 flex items-center justify-between">
        <h2 class="text-lg font-semibold">Peers ({ctx.peers.length})</h2>
        <button
            onclick={ctx.addPeer}
            disabled={ctx.busy}
            class="rounded bg-green-600 px-3 py-1.5 text-sm text-white hover:bg-green-700 disabled:opacity-50"
        >
            + Add Peer
        </button>
    </div>

    <div class="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
        {#each ctx.peers as peer (peer.id)}
            {@const sqlState = ctx.editorState[peer.id] ?? {
                text: "",
                records: [],
            }}
            <PeerPanel
                {peer}
                canRemove={ctx.peers.length > 1}
                onToggleOnline={ctx.togglePeer}
                onRemove={ctx.removePeer}
            >
                <div class="mb-2 border-t border-gray-200 pt-2 space-y-2">
                    <label
                        for={`sql-text-${peer.id}`}
                        class="block text-sm font-medium"
                    >
                        Record text
                    </label>
                    <input
                        id={`sql-text-${peer.id}`}
                        bind:value={sqlState.text}
                        class="w-full rounded border border-gray-300 px-2 py-1.5 text-sm"
                        placeholder="Enter record text..."
                        disabled={ctx.busy}
                    />
                    <button
                        class="rounded bg-blue-600 px-3 py-1.5 text-sm text-white hover:bg-blue-700 disabled:opacity-50"
                        onclick={() => ctx.addRecord(peer.id)}
                        disabled={ctx.busy || !sqlState.text.trim()}
                    >
                        Add
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
            </PeerPanel>
        {/each}
    </div>
</div>
