import { getContext, setContext } from "svelte";
import type { KvPeer, KvPeerNetwork } from "../sync/kv-network.js";
import type { SqlPeer, SqlPeerNetwork } from "../sync/sql-network.js";

export interface KvContextValue {
  network: KvPeerNetwork | null;
  peers: KvPeer[];
  busy: boolean;
  status: string;
  statusError: boolean;
  key: string;
  setKey: (key: string) => void;
  editorState: Record<string, { value: string }>;
  addPeer: () => Promise<void>;
  removePeer: (id: string) => Promise<void>;
  togglePeer: (id: string, online: boolean) => Promise<void>;
  save: (peerId: string) => Promise<void>;
  retrieve: (peerId: string) => Promise<void>;
  refreshValues: () => Promise<void>;
}

export interface SqlContextValue {
  network: SqlPeerNetwork | null;
  peers: SqlPeer[];
  busy: boolean;
  status: string;
  statusError: boolean;
  editorState: Record<string, { text: string; records: string[] }>;
  addPeer: () => Promise<void>;
  removePeer: (id: string) => Promise<void>;
  togglePeer: (id: string, online: boolean) => Promise<void>;
  addRecord: (peerId: string) => Promise<void>;
  refreshRecords: () => Promise<void>;
}

const KV_CTX_KEY = Symbol("kv-context");
const SQL_CTX_KEY = Symbol("sql-context");

export function setKvContext(value: KvContextValue): void {
  setContext(KV_CTX_KEY, value);
}

export function getKvContext(): KvContextValue {
  return getContext<KvContextValue>(KV_CTX_KEY);
}

export function setSqlContext(value: SqlContextValue): void {
  setContext(SQL_CTX_KEY, value);
}

export function getSqlContext(): SqlContextValue {
  return getContext<SqlContextValue>(SQL_CTX_KEY);
}
