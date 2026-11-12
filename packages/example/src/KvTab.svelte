<script lang="ts">
// biome-ignore lint/correctness/noUnusedImports: Used in template
import PeerPanel from "./PeerPanel.svelte";
import { getKvContext } from "./stores/context.js";

const ctx = getKvContext();
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

    <div class="mb-4 flex items-center gap-2">
        <label for="global-key" class="text-sm font-medium">Global Key</label>
        <input
            id="global-key"
            bind:value={() => ctx.key, (v) => ctx.setKey(v)}
            class="flex-1 rounded border border-gray-300 px-2 py-1.5 text-sm font-mono"
            placeholder="Enter key (e.g., theme)"
            disabled={ctx.busy}
        />
    </div>

    <div class="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
        {#each ctx.peers as peer (peer.id)}
            {@const editorState = ctx.editorState[peer.id] ?? { value: "" }}
            <PeerPanel
                {peer}
                canRemove={ctx.peers.length > 1}
                onToggleOnline={ctx.togglePeer}
                onRemove={ctx.removePeer}
            >
                <div class="mb-2 border-t border-gray-200 pt-2 space-y-2">
                    <label
                        for={`kv-value-${peer.id}`}
                        class="block text-sm font-medium"
                    >
                        Value
                    </label>
                    <textarea
                        id={`kv-value-${peer.id}`}
                        bind:value={editorState.value}
                        rows="2"
                        class="w-full rounded border border-gray-300 px-2 py-1.5 text-sm font-mono"
                        placeholder="Enter value..."
                        disabled={ctx.busy}></textarea>
                    <button
                        class="rounded bg-blue-600 px-3 py-1.5 text-sm text-white hover:bg-blue-700 disabled:opacity-50"
                        onclick={() => ctx.save(peer.id)}
                        disabled={ctx.busy}
                    >
                        Save
                    </button>
                </div>

                <div class="flex items-center gap-1 text-xs">
                    <span class="font-mono text-gray-500">Current value:</span>
                    <span class="font-mono text-sm font-semibold"
                        >{editorState.value ?? "\u2014"}</span
                    >
                </div>
            </PeerPanel>
        {/each}
    </div>
</div>
