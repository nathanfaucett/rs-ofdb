<script lang="ts">
    import { type KvPeer } from "./sync/kv-network.js";
    import PeerPanel from "./PeerPanel.svelte";

    interface Props {
        peers: KvPeer[];
        busy: boolean;
        status: string;
        statusError: boolean;
        onAdd: () => void;
        onSave: (peerId: string) => void;
        onRetrieve: (peerId: string) => void;
        onToggle: (id: string, online: boolean) => void;
        onRemove: (id: string) => void;
        getState: (peerId: string) => { key: string; value: string };
        globalKey: string;
        onKeyChange: (key: string) => void;
        kvEditorState: Record<string, { value: string }>;
    }

    let {
        peers,
        busy,
        status,
        statusError,
        onAdd,
        onSave,
        onRetrieve,
        onToggle,
        onRemove,
        getState,
        globalKey,
        onKeyChange,
        kvEditorState,
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

    <div class="mb-4 flex items-center gap-2">
        <label for="global-key" class="text-sm font-medium">Global Key</label>
        <input
            id="global-key"
            bind:value={globalKey}
            oninput={() => onKeyChange(globalKey)}
            class="flex-1 rounded border border-gray-300 px-2 py-1.5 text-sm font-mono"
            placeholder="Enter key (e.g., theme)"
            disabled={busy}
        />
    </div>

    <div class="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
        {#each peers as peer (peer.id)}
            {@const editorValue = kvEditorState[peer.id]?.value ?? ""}
            <PeerPanel
                {peer}
                canRemove={peers.length > 1}
                onToggleOnline={onToggle}
                {onRemove}
            >
                <div class="mb-2 border-t border-gray-200 pt-2 space-y-2">
                    <label
                        for={`kv-value-${peer.id}`}
                        class="block text-sm font-medium">Value</label
                    >
                    <textarea
                        id={`kv-value-${peer.id}`}
                        bind:value={kvEditorState[peer.id].value}
                        rows="2"
                        class="w-full rounded border border-gray-300 px-2 py-1.5 text-sm font-mono"
                        placeholder="Enter value..."
                        disabled={busy}></textarea>
                    <button
                        class="rounded bg-blue-600 px-3 py-1.5 text-sm text-white hover:bg-blue-700 disabled:opacity-50"
                        onclick={() => onSave(peer.id)}
                        disabled={busy}
                    >
                        Save
                    </button>
                </div>

                <div class="flex items-center gap-1 text-xs">
                    <span class="font-mono text-gray-500">Current value:</span>
                    <span class="font-mono text-sm font-semibold"
                        >{kvEditorState[peer.id]?.value ?? "\u2014"}</span
                    >
                </div>

                {#if peer.error}
                    <p class="mb-1 text-xs text-red-700">{peer.error}</p>
                {:else if peer.lastSync}
                    <p class="mb-1 text-xs text-gray-500">{peer.lastSync}</p>
                {/if}
            </PeerPanel>
        {/each}
    </div>
</div>
