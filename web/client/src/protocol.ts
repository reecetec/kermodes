/** A column of the viewed table, as the engine names its type */
export type ViewportColumn = {
  name: string;
  type: string;
};

/** One key of the table's sort, as the engine is asked for it */
export type SortKey = {
  column: string;
  descending: boolean;
};

/** What the engine side keeps in sync with the viewer */
export type ViewerState = {
  columns: ViewportColumn[];
  row_count: number;
  // "loading…" while a live table catches up with its store, "static", or "ticking · …"
  status: string;
  // Sort keys in priority order
  sort: SortKey[];
  // Quick filter text by column name
  filters: Record<string, string>;
  // Why a column's quick filter was not applied, by column name
  filter_errors: Record<string, string>;
  // The highest sort or filter `request` number the rows reflect
  applied: number;
  // Why the rows shown are not current (a failed table, a lost connection), until the server
  // answers for the view again; hosts without failures to report leave it out
  error?: string | null;
  // Whether the view comes back by itself: a reconnect, or the store restarting
  retrying?: boolean;
};

/** Messages from the viewer to the engine side */
export type ViewerRequest =
  // First and last row on screen (inclusive)
  | { type: 'viewport'; top: number; bottom: number }
  // Sort keys in priority order; empty for unsorted. `request` numbers a change to show
  // pending until `applied` reaches it; without one the change is never shown pending.
  | { type: 'sort'; by: SortKey[]; request?: number }
  // Every column's filter text; empty text for no filter on that column
  | { type: 'filter'; filters: Record<string, string>; request?: number }
  // Rows start..start+count, eg. for copying beyond the window; answered by "fetched"
  | { type: 'fetch'; id: number; start: number; count: number };

/**
 * Messages from the engine side. Each carries two buffers: an Arrow IPC stream of the rows
 * (all columns, in `columns` order; a series' x then its ys) and their keys as little-endian
 * uint64 (none for a series).
 */
export type ViewerReply =
  | { type: 'rows'; start: number; total: number; cycle: number }
  | { type: 'fetched'; id: number; start: number; total: number }
  // A chart's points: `total` is the rows they stand for
  | { type: 'series'; start: number; total: number; cycle: number };

/**
 * A chart's points of a table: each `y` against `x`, of the rows `where` keeps (an engine
 * expression), sent whole while they fit `points` and downsampled to it when they do not.
 */
export type SeriesRequest = {
  x: string;
  y: readonly string[];
  where?: string;
  points?: number;
};

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
