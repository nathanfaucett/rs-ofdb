export interface PeerState {
  value: string;
}

export interface SqlPeerState {
  text: string;
  records: string[];
}

export interface NetworkState<TPeer, TState> {
  network: TPeer | null;
  peers: TPeer[];
  busy: boolean;
  status: string;
  statusError: boolean;
  editorState: Record<string, TState>;
}

export function createNetworkState<TPeer, TState>(
  initialStatus: string
): NetworkState<TPeer, TState> {
  return {
    network: null,
    peers: [],
    busy: true,
    status: initialStatus,
    statusError: false,
    editorState: {},
  };
}

export function ensureState<TState>(
  editorState: Record<string, TState>,
  peerId: string,
  defaultState: TState
): void {
  if (!(peerId in editorState)) {
    editorState[peerId] = defaultState;
  }
}

export function getDefaultKvState(): PeerState {
  return { value: "" };
}

export function getDefaultSqlState(): SqlPeerState {
  return { text: "", records: [] };
}
