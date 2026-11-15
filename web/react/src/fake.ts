import { tableFromJSON, tableToIPC, Table } from 'apache-arrow';
import type {
  TableInfo,
  ViewerBuffer,
  ViewerChannel,
  ViewerReply,
  ViewerRequest,
  ViewerState,
  ViewportColumn,
} from '@kermodes/client';
import type { Connection } from './connection';
import type { TableRow } from './rows';

/** A connection serving fixed rows, for testing components that use `useTable` */
export type FakeKermodes = Connection & {
  /** Replaces a table's rows, as a tick would: every view of it gets them */
  set(table: string, rows: readonly TableRow[]): void;
};

type View = {
  table: string;
  viewport: { top: number; bottom: number } | null;
  state: ViewerState;
  messages: Set<(header: ViewerReply, buffers: readonly ViewerBuffer[]) => void>;
  states: Set<() => void>;
};

/**
 * A stand-in for a Kermodes server, serving `tables` (name to rows) to `useTable` through the
 * same Arrow decoding as the real one. Rows come in the order given: sorting and filtering are
 * the server's, so tests give the rows a component should see. A table it does not have fails
 * as a missing table does.
 *
 *     const kermodes = fakeKermodes({ 'md.ticks': [{ sym: 'AAPL', price: 189.5 }] });
 *     render(<KermodesProvider connection={kermodes}><Tape /></KermodesProvider>);
 *     kermodes.set('md.ticks', [...]);
 */
export function fakeKermodes(tables: Readonly<Record<string, readonly TableRow[]>>): FakeKermodes {
  const data = new Map(Object.entries(tables));
  const views = new Set<View>();
  const changes = new Set<() => void>();
  let cycle = 0;

  const listing = (): TableInfo[] =>
    [...data].map(([name, rows]) => ({ name, columns: columnsOf(rows), refreshing: true }));

  const push = (view: View) => {
    const rows = data.get(view.table);
    if (rows == null || view.viewport == null) {
      return;
    }
    const { top, bottom } = view.viewport;
    const window = rows.slice(top, bottom + 1);
    const ipc = tableToIPC(window.length > 0 ? tableFromJSON(window as Record<string, unknown>[]) : new Table(), 'stream');
    const keys = BigUint64Array.from(window, (_, i) => BigInt(top + i));
    const header: ViewerReply = { type: 'rows', start: top, total: rows.length, cycle };
    view.messages.forEach(handler => handler(header, [ipc, keys]));
  };

  const update = (view: View) => {
    const rows = data.get(view.table);
    view.state =
      rows == null
        ? { ...view.state, error: `no table ${view.table}`, retrying: false }
        : { ...view.state, columns: columnsOf(rows), row_count: rows.length, status: 'ticking' };
    view.states.forEach(handler => handler());
  };

  return {
    get tables() {
      return listing();
    },
    onChange(listener) {
      changes.add(listener);
      return () => changes.delete(listener);
    },
    open(table): ViewerChannel & { close(): void } {
      const view: View = {
        table,
        viewport: null,
        state: {
          columns: [],
          row_count: 0,
          status: 'loading',
          sort: [],
          filters: {},
          filter_errors: {},
          applied: 0,
        },
        messages: new Set(),
        states: new Set(),
      };
      views.add(view);
      update(view);
      return {
        send(message: ViewerRequest) {
          if (message.type === 'viewport') {
            view.viewport = { top: message.top, bottom: message.bottom };
            push(view);
          }
        },
        onMessage(handler) {
          view.messages.add(handler);
          return () => view.messages.delete(handler);
        },
        state: () => view.state,
        onState(handler) {
          view.states.add(handler);
          return () => view.states.delete(handler);
        },
        close() {
          views.delete(view);
        },
      };
    },
    close() {
      views.clear();
    },
    set(table, rows) {
      const isNew = !data.has(table);
      data.set(table, rows);
      cycle += 1;
      for (const view of views) {
        if (view.table === table) {
          update(view);
          push(view);
        }
      }
      if (isNew) {
        changes.forEach(listener => listener());
      }
    },
  };
}

/** Columns as the engine names their types, from the first row's values */
function columnsOf(rows: readonly TableRow[]): ViewportColumn[] {
  const first = rows[0] ?? {};
  return Object.entries(first).map(([name, value]) => ({
    name,
    type:
      typeof value === 'number'
        ? 'float64'
        : typeof value === 'bigint'
          ? 'int64'
          : typeof value === 'boolean'
            ? 'bool'
            : value instanceof Date
              ? 'timestamp'
              : 'utf8',
  }));
}
