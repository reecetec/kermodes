import { type SortKey, type ViewportColumn } from './ViewportGridModel';

/** What the engine side keeps in sync with the viewer */
export type ViewerState = {
  columns: ViewportColumn[];
  row_count: number;
  status: string;
  // Sort keys in priority order
  sort: SortKey[];
  // Quick filter text by column name
  filters: Record<string, string>;
  // Why a column's quick filter was not applied, by column name
  filter_errors: Record<string, string>;
};

/** Messages from the viewer to the engine side */
export type ViewerRequest =
  // First and last row on screen (inclusive)
  | { type: 'viewport'; top: number; bottom: number }
  // Sort keys in priority order; empty for unsorted
  | { type: 'sort'; by: SortKey[] }
  // Every column's filter text; empty text for no filter on that column
  | { type: 'filter'; filters: Record<string, string> }
  // Rows start..start+count, eg. for copying beyond the window; answered by "fetched"
  | { type: 'fetch'; id: number; start: number; count: number };

/**
 * Messages from the engine side. Both carry two buffers: an Arrow IPC stream of
 * the rows (all columns, in `columns` order) and their keys as little-endian uint64.
 */
export type ViewerReply =
  | { type: 'rows'; start: number; total: number; cycle: number }
  | { type: 'fetched'; id: number; start: number; total: number };

export type ViewerBuffer = ArrayBuffer | ArrayBufferView;

/**
 * The viewer's link to the engine, whatever carries it: a notebook widget's
 * comm, or a WebSocket in the web UI.
 */
export interface ViewerChannel {
  send(message: ViewerRequest): void;
  /** Call `handler` for each message; returns the function that stops it */
  onMessage(
    handler: (header: ViewerReply, buffers: readonly ViewerBuffer[]) => void
  ): () => void;
  /** The current synced state */
  state(): ViewerState;
  /** Call `handler` after the synced state changes; returns the function that stops it */
  onState(handler: () => void): () => void;
}

export function toBytes(buffer: ViewerBuffer): Uint8Array {
  return buffer instanceof ArrayBuffer
    ? new Uint8Array(buffer)
    : new Uint8Array(buffer.buffer, buffer.byteOffset, buffer.byteLength);
}

/** Row keys from their buffer, copied when the buffer is not 8-byte aligned */
export function toKeys(buffer: ViewerBuffer): BigUint64Array {
  const bytes = toBytes(buffer);
  return bytes.byteOffset % 8 === 0
    ? new BigUint64Array(bytes.buffer, bytes.byteOffset, bytes.byteLength / 8)
    : new BigUint64Array(bytes.slice().buffer);
}
