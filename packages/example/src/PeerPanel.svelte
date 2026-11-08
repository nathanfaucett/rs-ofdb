<script lang="ts">
    interface PeerLike {
        id: string;
        name: string;
        online: boolean;
        syncing: boolean;
        lastSync: string;
        error: string;
    }

    let {
        peer,
        canRemove,
        onToggleOnline,
        onRemove,
        children,
    }: {
        peer: PeerLike;
        canRemove: boolean;
        onToggleOnline: (id: string, online: boolean) => void;
        onRemove: (id: string) => void;
        children?: import("svelte").Snippet;
    } = $props();
</script>

<div class="rounded-lg border border-gray-300 bg-gray-50 p-3">
    <div class="mb-2 flex items-center gap-2">
        <span class="font-semibold">{peer.name}</span>
        <span
            class="rounded-full px-2 py-0.5 text-xs font-medium {peer.online
                ? 'bg-green-100 text-green-800'
                : 'bg-red-100 text-red-800'}"
        >
            {peer.online ? "Online" : "Offline"}
        </span>
    </div>

    {#if peer.error}
        <p class="mb-1 text-xs text-red-700">{peer.error}</p>
    {:else if peer.lastSync}
        <p class="mb-1 text-xs text-gray-500">{peer.lastSync}</p>
    {/if}

    {#if children}
        <div class="my-2 text-sm">
            {@render children()}
        </div>
    {/if}

    <div class="mt-2 flex gap-2">
        <button
            class="flex-1 rounded-md px-3 py-1.5 text-sm font-medium text-white transition-colors {peer.online
                ? 'bg-orange-600 hover:bg-orange-700'
                : 'bg-green-600 hover:bg-green-700'}"
            onclick={() => onToggleOnline(peer.id, !peer.online)}
        >
            {peer.online ? "Go Offline" : "Go Online"}
        </button>
        {#if canRemove}
            <button
                class="rounded-md border border-gray-300 bg-white px-3 py-1.5 text-sm text-red-600 hover:bg-red-50"
                onclick={() => onRemove(peer.id)}
            >
                Remove
            </button>
        {/if}
    </div>
</div>
