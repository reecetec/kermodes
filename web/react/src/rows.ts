import { DataType, type Table } from 'apache-arrow';
import type { SortKey, ViewerState } from '@kermodes/grid';

/** A row as `useTable` gives it: column name to a plain JavaScript value */
export type TableRow = Record<string, unknown>;

/**
 * Rows `from` to `from + count` of a window of the table that starts at row `start`, as plain
 * objects: integers as numbers (as bigints past 2^53), dates and timestamps as `Date`s, nulls
 * as null. Rows outside the window are left out.
 */
export function windowRows(table: Table, start: number, from: number, count: number): TableRow[] {
  const first = Math.max(from - start, 0);
  const last = Math.min(from + count - start, table.numRows);
  const columns = table.schema.fields.map((field, index) => ({
    name: field.name,
    vector: table.getChildAt(index),
    isTime: DataType.isTimestamp(field.type) || DataType.isDate(field.type),
  }));
  const rows: TableRow[] = [];
  for (let i = first; i < last; i += 1) {
    const row: TableRow = {};
    for (const { name, vector, isTime } of columns) {
      row[name] = plain(vector?.get(i) ?? null, isTime);
    }
    rows.push(row);
  }
  return rows;
}

function plain(value: unknown, isTime: boolean): unknown {
  if (value == null) {
    return null;
  }
  if (typeof value === 'bigint') {
    const number = Number(value);
    return Number.isSafeInteger(number) ? (isTime ? new Date(number) : number) : value;
  }
  if (isTime && typeof value === 'number') {
    return new Date(value);
  }
  return value;
}


/**
 * Which rows to give of a table of `total` rows: `limit` from `offset`, or with `last` the last
 * that many (all of them when there are fewer).
 */
export function rowRange(
  total: number,
  { offset = 0, limit = 100, last }: { offset?: number; limit?: number; last?: number }
): { from: number; count: number } {
  if (last != null) {
    return { from: Math.max(total - last, 0), count: last };
  }
  return { from: offset, count: limit };
}
/** Sort keys from names, highest priority first: `ts` ascending, `-ts` descending */
export function sortKeys(names: readonly string[]): SortKey[] {
  return names.map(name =>
    name.startsWith('-')
      ? { column: name.slice(1), descending: true }
      : { column: name, descending: false }
  );
}

/**
 * How a table's rows stand: `loading` until the first arrive (or while a live table catches up
 * with its store), `live` while they tick, `static` for a table that never changes,
 * `reconnecting` while the view comes back by itself, `failed` with an error otherwise.
 */
export type TableStatus = 'loading' | 'live' | 'static' | 'reconnecting' | 'failed';

export function tableStatus(
  state: Pick<ViewerState, 'status' | 'error' | 'retrying'>,
  hasRows: boolean
): TableStatus {
  if (state.error != null) {
    return state.retrying === true ? 'reconnecting' : 'failed';
  }
  if (!hasRows || state.status.startsWith('loading')) {
    return 'loading';
  }
  return state.status === 'static' ? 'static' : 'live';
}
