// Dev page: the grid over a billion-row table whose rows are generated for the
// requested window, round-tripped through Arrow IPC like the notebook widget's.
import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { createRoot } from 'react-dom/client';
import {
  Bool,
  DateDay,
  Table,
  TimestampNanosecond,
  Utf8,
  tableFromIPC,
  tableToIPC,
  vectorFromArray,
} from 'apache-arrow';
import {
  Grid,
  ViewportGridModel,
  gridThemes,
  type GridMetrics,
  type GridThemeName,
  type ViewportColumn,
} from '../src';

const ROW_COUNT = 1_000_000_000;

// Rows fetched beyond each edge of the visible range
const WINDOW_MARGIN = 100;

// Simulated round trip to the engine, so loading cells show while scrolling
const LATENCY_MS = 40;

const COLUMNS: ViewportColumn[] = [
  { name: 'Row', type: 'int64' },
  { name: 'Sym', type: 'utf8' },
  { name: 'Price', type: 'float64' },
  { name: 'Qty', type: 'int32' },
  { name: 'Buy', type: 'bool' },
  { name: 'Day', type: 'date32' },
  { name: 'Timestamp', type: 'timestamp' },
  { name: 'Note', type: 'utf8' },
];

const SYMBOLS = ['AAPL', 'MSFT', 'NVDA', 'AMZN', 'GOOG', 'META', 'TSLA', 'JPM'];
const NOTES = ['fill', 'partial fill', 'cancel/replace', 'routed to dark pool'];
const EPOCH_MS = Date.UTC(2026, 0, 1);

/** A well-mixed 32-bit hash of a row position, so rows look random but are stable */
function hash(n: number, salt: number): number {
  let x = (n + salt * 0x9e3779b9) | 0;
  x = Math.imul(x ^ (x >>> 16), 0x85ebca6b);
  x = Math.imul(x ^ (x >>> 13), 0xc2b2ae35);
  return (x ^ (x >>> 16)) >>> 0;
}

/** Rows start..start+count as an Arrow IPC stream, as the engine would send them */
function makeRows(start: number, count: number, cycle: number): Uint8Array {
  const rows = Array.from({ length: count }, (_, i) => start + i);
  const table = new Table({
    Row: vectorFromArray(BigInt64Array.from(rows, row => BigInt(row))),
    Sym: vectorFromArray(
      rows.map(row => SYMBOLS[hash(row, 1) % SYMBOLS.length]),
      new Utf8()
    ),
    Price: vectorFromArray(
      Float64Array.from(
        rows,
        row => 50 + (hash(row, 2) % 100_000) / 100 + (cycle % 7) * 0.01
      )
    ),
    Qty: vectorFromArray(
      Int32Array.from(rows, row => (hash(row, 3) % 20_000) - 2_000)
    ),
    Buy: vectorFromArray(
      rows.map(row => hash(row, 4) % 2 === 0),
      new Bool()
    ),
    Day: vectorFromArray(
      rows.map(row => EPOCH_MS + Math.floor(row / 100_000) * 86_400_000),
      new DateDay()
    ),
    Timestamp: vectorFromArray(
      rows.map(row => EPOCH_MS + row * 37 + (hash(row, 5) % 37)),
      new TimestampNanosecond()
    ),
    Note: vectorFromArray(
      rows.map(row => (row % 7 === 3 ? null : NOTES[hash(row, 6) % NOTES.length])),
      new Utf8()
    ),
  });
  return tableToIPC(table, 'stream');
}

function Demo(): React.ReactElement {
  const model = useMemo(() => new ViewportGridModel(COLUMNS, ROW_COUNT), []);
  const gridRef = useRef<Grid>(null);
  const viewRef = useRef({ top: 0, bottom: 0 });
  const cycleRef = useRef(0);
  const [themeName, setThemeName] = useState<GridThemeName>('dark');
  const [isTicking, setIsTicking] = useState(false);
  const [held, setHeld] = useState({ start: 0, end: -1 });
  const theme = gridThemes[themeName];

  const fetchWindow = useCallback(() => {
    const { top, bottom } = viewRef.current;
    const start = Math.max(0, top - WINDOW_MARGIN);
    const end = Math.min(ROW_COUNT - 1, bottom + WINDOW_MARGIN);
    const ipc = makeRows(start, end - start + 1, cycleRef.current);
    setTimeout(() => {
      model.setWindow(start, tableFromIPC(ipc));
      setHeld({ start, end });
      gridRef.current?.forceUpdate();
    }, LATENCY_MS);
  }, [model]);

  // Fetch when the visible rows leave the window we hold
  const handleViewChanged = useCallback(
    ({ top, bottom }: GridMetrics) => {
      const view = viewRef.current;
      if (view.top === top && view.bottom === bottom) {
        return;
      }
      viewRef.current = { top, bottom };
      if (!model.isRowLoaded(top) || !model.isRowLoaded(bottom)) {
        fetchWindow();
      }
    },
    [model, fetchWindow]
  );

  useEffect(() => {
    if (!isTicking) {
      return undefined;
    }
    const timer = setInterval(() => {
      cycleRef.current += 1;
      fetchWindow();
    }, 500);
    return () => clearInterval(timer);
  }, [isTicking, fetchWindow]);

  const chrome: React.CSSProperties = {
    display: 'flex',
    alignItems: 'center',
    gap: 16,
    padding: '6px 12px',
    font: `12px ${theme.font.replace(/^\S+\s/, '')}`,
    color: theme.headerColor,
    background: theme.headerBackgroundColor,
    fontVariantNumeric: 'tabular-nums',
  };

  return (
    <div
      style={{
        display: 'flex',
        flexDirection: 'column',
        height: '100%',
        background: theme.backgroundColor,
      }}
    >
      <div style={{ ...chrome, borderBottom: `1px solid ${theme.headerSeparatorColor}` }}>
        <strong>@ursid/grid</strong>
        <label>
          Theme{' '}
          <select
            value={themeName}
            onChange={event => setThemeName(event.target.value as GridThemeName)}
          >
            <option value="dark">dark</option>
            <option value="light">light</option>
          </select>
        </label>
        <label>
          <input
            type="checkbox"
            checked={isTicking}
            onChange={event => setIsTicking(event.target.checked)}
          />{' '}
          Ticking
        </label>
      </div>
      <div style={{ display: 'flex', flex: 1, minHeight: 0 }}>
        <Grid
          ref={gridRef}
          model={model}
          theme={theme}
          onViewChanged={handleViewChanged}
        />
      </div>
      <div style={{ ...chrome, borderTop: `1px solid ${theme.headerSeparatorColor}` }}>
        <span>{ROW_COUNT.toLocaleString('en-US')} rows</span>
        <span>
          window {held.start.toLocaleString('en-US')}–
          {held.end.toLocaleString('en-US')}
        </span>
        <span style={{ marginLeft: 'auto' }}>
          {isTicking ? `ticking · cycle ${cycleRef.current}` : 'static'}
        </span>
      </div>
    </div>
  );
}

const root = document.getElementById('root');
if (root == null) throw new Error('#root missing');
createRoot(root).render(<Demo />);
