import {
  createContext,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from 'react';
import { tableFromIPC, type Table } from 'apache-arrow';
import {
  KermodesConnection,
  toBytes,
  type ViewerChannel,
  type TableInfo,
  type ViewportColumn,
} from '@kermodes/client';
import type { Connection } from './connection';
import {
  rowRange,
  sortKeys,
  tableStatus,
  windowRows,
  type TableRow,
  type TableStatus,
} from './rows';

export type { Connection } from './connection';
export { fakeKermodes, type FakeKermodes } from './fake';
export {
  rowRange,
  sortKeys,
  tableStatus,
  windowRows,
  type TableRow,
  type TableStatus,
} from './rows';

const ConnectionContext = createContext<Connection | null>(null);

/** The page's own server: `ws(s)://<host>/ws`, as an app served by Kermodes reaches it */
function sameOrigin(): string {
  return `${location.protocol === 'https:' ? 'wss' : 'ws'}://${location.host}/ws`;
}

/**
 * One connection to a Kermodes server for everything inside it, reconnecting by itself. `url`
 * is the server's WebSocket (`wss://kermodes.example.com/ws`); by default the page's own. Tests
 * pass `connection` instead: `fakeKermodes`.
 */
export function KermodesProvider({
  url,
  connection: given,
  children,
}: {
  url?: string;
  connection?: Connection;
  children: ReactNode;
}) {
  const connection = useMemo(() => given ?? new KermodesConnection(url ?? sameOrigin()), [given, url]);
  // Closed here only if made here
  useEffect(() => (given == null ? () => connection.close() : undefined), [connection, given]);
  return <ConnectionContext.Provider value={connection}>{children}</ConnectionContext.Provider>;
}

/** The connection of the nearest `KermodesProvider` */
export function useKermodes(): Connection {
  const connection = useContext(ConnectionContext);
  if (connection == null) {
    throw new Error('useKermodes needs a KermodesProvider around it');
  }
  return connection;
}

/** The tables the server lists, kept current */
export function useTables(): readonly TableInfo[] {
  const connection = useKermodes();
  const [tables, setTables] = useState(connection.tables);
  useEffect(() => {
    setTables(connection.tables);
    return connection.onChange(() => setTables(connection.tables));
  }, [connection]);
  return tables;
}

export type TableOptions = {
  /** Sort keys, highest priority first: `'ts'` ascending, `'-ts'` descending */
  sort?: readonly string[];
  /** Quick filters by column in the grid's syntax: `{ sym: '=AAPL', px: '> 100' }` */
  filters?: Readonly<Record<string, string>>;
  /** The first row to give, after sorting and filtering; 0 by default */
  offset?: number;
  /** How many rows to give; 100 by default */
  limit?: number;
  /**
   * Instead of `offset` and `limit`: the last rows, following the table as it grows. For the
   * newest rows of an append-only table, this needs no sort, so the server reads them by
   * position instead of holding the table to sort it.
   */
  last?: number;
};

export type TableState<Row extends TableRow = TableRow> = {
  rows: Row[];
  /** Rows in the table after filtering, of which `rows` are the asked range */
  total: number;
  columns: ViewportColumn[];
  status: TableStatus;
  error: string | null;
};

/**
 * Rows of a Kermodes table, live: sorted and filtered by the server, the asked range of them,
 * updated as the table ticks. Only those rows travel, so a table of billions costs what its
 * window does. Render them with any component.
 *
 *     const { rows } = useTable('md.quotes', { sort: ['sym'] });
 *     const tape = useTable('md.ticks', { last: 15 });
 */
export function useTable<Row extends TableRow = TableRow>(
  name: string,
  { sort = [], filters = {}, offset = 0, limit = 100, last }: TableOptions = {}
): TableState<Row> {
  const connection = useKermodes();
  const [state, setState] = useState<TableState<Row>>({
    rows: [],
    total: 0,
    columns: [],
    status: 'loading',
    error: null,
  });
  const view = useRef<(ViewerChannel & { close(): void }) | null>(null);
  // The range asked for, read when rows or a new total arrive
  const range = useRef({ offset, limit, last });
  range.current = { offset, limit, last };
  const sentViewport = useRef('');
  /** Asks the server for the rows of the current range, unless it was the last asked */
  const sendViewport = useRef((total: number) => {
    const { from, count } = rowRange(total, range.current);
    const asked = `${from}:${count}`;
    if (view.current != null && asked !== sentViewport.current) {
      sentViewport.current = asked;
      view.current.send({ type: 'viewport', top: from, bottom: from + Math.max(count, 1) - 1 });
    }
  }).current;

  // One view per table, open while the component is
  useEffect(() => {
    const opened = connection.open(name);
    view.current = opened;
    sentViewport.current = '';
    let window: { start: number; table: Table } | null = null;
    const publish = () => {
      const current = opened.state();
      const { from, count } = rowRange(current.row_count, range.current);
      setState({
        rows: (window == null ? [] : windowRows(window.table, window.start, from, count)) as Row[],
        total: current.row_count,
        columns: current.columns,
        status: tableStatus(current, window != null),
        error: current.error ?? null,
      });
      // Following the end: as the table grows, so does where its last rows start
      sendViewport(current.row_count);
    };
    const stopRows = opened.onMessage((header, buffers) => {
      if (header.type === 'rows') {
        window = { start: header.start, table: tableFromIPC(toBytes(buffers[0])) };
        publish();
      }
    });
    const stopState = opened.onState(publish);
    sendViewport(opened.state().row_count);
    return () => {
      stopRows();
      stopState();
      opened.close();
      view.current = null;
    };
  }, [connection, name, sendViewport]);

  // Arrays and objects passed inline are new on every render; their contents are what count
  const sortKey = JSON.stringify(sort);
  const filterKey = JSON.stringify(filters);
  const sent = useRef({ name: '', sort: '[]', filters: '{}' });
  useEffect(() => {
    const current = view.current;
    if (current == null) {
      return;
    }
    // A new view starts unsorted and unfiltered: only a change from that is sent
    if (sent.current.name !== name) {
      sent.current = { name, sort: '[]', filters: '{}' };
    }
    if (sortKey !== sent.current.sort) {
      current.send({ type: 'sort', by: sortKeys(JSON.parse(sortKey) as string[]) });
      sent.current.sort = sortKey;
    }
    if (filterKey !== sent.current.filters) {
      current.send({ type: 'filter', filters: JSON.parse(filterKey) as Record<string, string> });
      sent.current.filters = filterKey;
    }
  }, [name, sortKey, filterKey]);

  useEffect(() => {
    sendViewport(state.total);
  }, [offset, limit, last, state.total, sendViewport]);

  return state;
}
