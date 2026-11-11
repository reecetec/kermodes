// Dev page: the notebook widget over a fake anywidget model that plays the
// Python side (python/kermodes/_widget.py): a ticking in-memory table with row
// keys, viewport windows, sort, quick filters with their errors, and fetch.
// ?theme=light picks the light theme, ?rows=N the table size.
import {
  Bool,
  DateDay,
  Float64,
  Int32,
  Int64,
  Table,
  TimestampMillisecond,
  Utf8,
  tableToIPC,
  vectorFromArray,
} from 'apache-arrow';
import type { SortKey, ViewerRequest, ViewerState } from '@kermodes/grid';
import widget, { type WidgetModel } from '../src/index';

const params = new URLSearchParams(location.search);
const ROWS = Number(params.get('rows') ?? 100_000);
const MARGIN = 100;
const TICK_MS = 1000;
if (params.get('theme') === 'light') {
  document.body.dataset.jpThemeLight = 'true';
}

const SYMBOLS = ['AAPL', 'MSFT', 'NVDA', 'AMZN', 'GOOG', 'META', 'TSLA', 'JPM'];
const NOTES = ['fill', 'partial fill', 'cancel/replace', 'routed to dark pool'];
const DAY_MS = 86_400_000;
const EPOCH_MS = Date.UTC(2026, 0, 1);

type Column = { name: string; type: string; values: unknown[] };

let seed = 7;
function random(): number {
  seed = (Math.imul(seed, 1103515245) + 12345) | 0;
  return ((seed >>> 8) & 0xffffff) / 0x1000000;
}

const ids = Array.from({ length: ROWS }, (_, i) => i);
const data: Column[] = [
  { name: 'id', type: 'int64', values: ids.map(i => BigInt(i)) },
  {
    name: 'sym',
    type: 'utf8',
    values: ids.map(i => (i % 11 === 5 ? null : SYMBOLS[Math.floor(random() * 8)])),
  },
  { name: 'price', type: 'float64', values: ids.map(() => 50 + Math.round(random() * 100_000) / 100) },
  { name: 'qty', type: 'int32', values: ids.map(() => Math.floor(random() * 20_000) - 2_000) },
  { name: 'buy', type: 'bool', values: ids.map(() => random() < 0.5) },
  { name: 'day', type: 'date32', values: ids.map(i => EPOCH_MS + Math.floor(i / 1_000) * DAY_MS) },
  { name: 'ts', type: 'timestamp', values: ids.map(i => EPOCH_MS + i * 37 + Math.floor(random() * 37)) },
  {
    name: 'note',
    type: 'utf8',
    values: ids.map(i => (i % 7 === 3 ? null : NOTES[Math.floor(random() * 4)])),
  },
];
const byName = new Map(data.map(column => [column.name, column]));

const NUMBER = /^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$/;
type Predicate = (value: unknown) => boolean;

/** The quick filter rules of python/kermodes/_widget.py's quick_filter, as a predicate */
function quickFilter(type: string, entry: string): Predicate {
  const text = entry.trim();
  if (text.toLowerCase() === 'null') return v => v == null;
  if (text.toLowerCase() === '!null') return v => v != null;
  if (type === 'utf8') {
    const has = (v: unknown, s: string) =>
      v != null && String(v).toLowerCase().includes(s.toLowerCase());
    if (text.startsWith('!=')) return v => v != null && v !== text.slice(2);
    if (text.startsWith('=')) return v => v === text.slice(1);
    if (text.startsWith('!')) return v => v != null && !has(v, text.slice(1));
    return v => has(v, text);
  }
  const match = /^(>=|<=|!=|=|>|<)?\s*([\s\S]*)$/.exec(text) as RegExpExecArray;
  const op = match[1] ?? '=';
  const raw = match[2].trim();
  if (raw === '') throw new Error('needs a value');
  let target: number;
  if (type === 'int32' || type === 'int64' || type === 'float64') {
    if (!NUMBER.test(raw)) throw new Error(`\`${raw}\` is not a number`);
    target = Number(raw);
  } else if (type === 'bool') {
    if (raw.toLowerCase() !== 'true' && raw.toLowerCase() !== 'false') {
      throw new Error('takes true, false, null, or !null');
    }
    target = raw.toLowerCase() === 'true' ? 1 : 0;
  } else {
    target = Date.parse(type === 'date32' ? `${raw}T00:00:00Z` : raw);
    if (Number.isNaN(target)) throw new Error(`\`${raw}\` is not an ISO date or time`);
  }
  const compare: Record<string, (a: number) => boolean> = {
    '=': a => a === target,
    '!=': a => a !== target,
    '>': a => a > target,
    '>=': a => a >= target,
    '<': a => a < target,
    '<=': a => a <= target,
  };
  return v => v != null && compare[op](Number(v));
}

function compareValues(a: unknown, b: unknown): number {
  if (a == null || b == null) return a == null ? (b == null ? 0 : 1) : -1;
  if (typeof a === 'string') return a.localeCompare(b as string);
  return Number(a) - Number(b);
}

// --- the fake widget model ---------------------------------------------------

const traits: ViewerState = {
  columns: data.map(({ name, type }) => ({ name, type })),
  row_count: ROWS,
  status: 'ticking · updated in cycle 1',
  sort: [],
  filters: {},
  filter_errors: {},
};
const listeners = new Map<string, Set<(...args: never[]) => void>>();
let cycle = 1;
let window = { start: 0, count: 2 * MARGIN };
let shown: number[] = ids;
let predicates: Array<[Column, Predicate]> = [];

function emit(event: string, ...args: unknown[]): void {
  listeners.get(event)?.forEach(listener => (listener as (...a: unknown[]) => void)(...args));
}

function setTrait<K extends keyof ViewerState>(name: K, value: ViewerState[K]): void {
  traits[name] = value;
  emit(`change:${name}`);
}

function rowsAt(start: number, count: number): [DataView, DataView] {
  const rows = shown.slice(start, start + count);
  const vectors = data.map(({ type, values }) => {
    const picked = rows.map(row => values[row]);
    switch (type) {
      case 'int64':
        return vectorFromArray(picked, new Int64());
      case 'int32':
        return vectorFromArray(picked, new Int32());
      case 'float64':
        return vectorFromArray(picked, new Float64());
      case 'bool':
        return vectorFromArray(picked, new Bool());
      case 'date32':
        return vectorFromArray(picked, new DateDay());
      case 'timestamp':
        return vectorFromArray(picked, new TimestampMillisecond());
      default:
        return vectorFromArray(picked, new Utf8());
    }
  });
  const table = new Table(Object.fromEntries(data.map((c, i) => [c.name, vectors[i]])));
  const ipc = tableToIPC(table, 'stream');
  const keys = BigUint64Array.from(rows, row => BigInt(row));
  return [
    new DataView(ipc.buffer, ipc.byteOffset, ipc.byteLength),
    new DataView(keys.buffer),
  ];
}

function derive(): void {
  shown = ids.filter(row => predicates.every(([column, keep]) => keep(column.values[row])));
  const keys = traits.sort.map(key => [byName.get(key.column) as Column, key.descending] as const);
  if (keys.length > 0) {
    shown.sort((a, b) => {
      for (const [column, descending] of keys) {
        const order = compareValues(column.values[a], column.values[b]);
        if (order !== 0) return descending ? -order : order;
      }
      return a - b;
    });
  }
}

function push(): void {
  setTrait('row_count', shown.length);
  setTrait('status', `ticking · updated in cycle ${cycle}`);
  emit('msg:custom', { type: 'rows', start: window.start, total: shown.length, cycle }, rowsAt(window.start, window.count));
}

function receive(message: ViewerRequest): void {
  if (message.type === 'viewport') {
    window = {
      start: Math.max(0, message.top - MARGIN),
      count: Math.max(0, message.bottom - message.top) + 1 + 2 * MARGIN,
    };
  } else if (message.type === 'sort') {
    setTrait('sort', message.by.map(({ column, descending }): SortKey => ({ column, descending })));
    derive();
  } else if (message.type === 'filter') {
    const filters = Object.fromEntries(
      Object.entries(message.filters).filter(([, text]) => text.trim() !== '')
    );
    const errors: Record<string, string> = {};
    predicates = [];
    for (const [name, text] of Object.entries(filters)) {
      const column = byName.get(name);
      if (column == null) {
        errors[name] = 'no such column';
        continue;
      }
      try {
        predicates.push([column, quickFilter(column.type, text)]);
      } catch (error) {
        errors[name] = (error as Error).message;
      }
    }
    setTrait('filters', filters);
    setTrait('filter_errors', errors);
    derive();
  } else if (message.type === 'fetch') {
    emit(
      'msg:custom',
      { type: 'fetched', id: message.id, start: message.start, total: shown.length },
      rowsAt(message.start, message.count)
    );
    return;
  }
  push();
}

const model: WidgetModel = {
  get: name => traits[name],
  on(event, callback) {
    if (!listeners.has(event)) listeners.set(event, new Set());
    listeners.get(event)?.add(callback);
  },
  off(event, callback) {
    listeners.get(event)?.delete(callback);
  },
  // The kernel answers asynchronously
  send: message => {
    setTimeout(() => receive(message), 15);
  },
};

// A cycle changes the price and quantity of a few rows
setInterval(() => {
  const price = byName.get('price') as Column;
  const qty = byName.get('qty') as Column;
  for (let i = 0; i < ROWS / 40; i += 1) {
    const row = Math.floor(random() * ROWS);
    price.values[row] = Math.round((price.values[row] as number) * (0.99 + random() * 0.02) * 100) / 100;
    qty.values[row] = (qty.values[row] as number) + Math.floor(random() * 200) - 100;
  }
  cycle += 1;
  if (traits.sort.length > 0 || predicates.length > 0) derive();
  push();
}, TICK_MS);

Object.assign(globalThis, { fakeModel: { traits, receive, push } });
(document.getElementById('caption') as HTMLElement).textContent =
  `${ROWS.toLocaleString('en-US')} rows, a cycle every ${TICK_MS} ms`;
widget.render({ model, el: document.getElementById('widget') as HTMLElement });
push();
