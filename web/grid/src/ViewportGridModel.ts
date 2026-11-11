import { type Table, type Vector } from 'apache-arrow';
import GridModel from './GridModel';
import { type ModelIndex } from './GridMetrics';
import { type GridTheme, type NullableGridColor } from './GridTheme';

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

/** How long a changed or copied cell stays highlighted */
export const FLASH_DURATION_MS = 600;

const IS_NUMERIC: Readonly<Record<string, true>> = {
  int32: true,
  int64: true,
  float64: true,
};

export function isNumericType(type: string): boolean {
  return IS_NUMERIC[type] === true;
}

const integerFormat = new Intl.NumberFormat('en-US', {
  maximumFractionDigits: 0,
});

// Deephaven's default decimal format, ###,##0.0000
const decimalFormat = new Intl.NumberFormat('en-US', {
  minimumFractionDigits: 4,
  maximumFractionDigits: 4,
});

/** Format one non-null cell value of a column of `type` */
export function formatValue(type: string, value: unknown): string {
  switch (type) {
    case 'int32':
    case 'int64':
      return integerFormat.format(value as number | bigint);
    case 'float64':
      return Number.isFinite(value)
        ? decimalFormat.format(value as number)
        : String(value);
    case 'timestamp':
      // apache-arrow hands timestamps over as epoch milliseconds whatever the unit
      return new Date(Number(value)).toISOString().replace('T', ' ');
    case 'date32':
      return new Date(Number(value)).toISOString().slice(0, 10);
    default:
      return String(value);
  }
}

/** A pushed window of rows: their keys and a reader of their values */
export type RowWindow = {
  keys: ArrayLike<bigint>;
  value: (column: number, index: number) => unknown;
};

/**
 * The cells of `next` whose value differs from the same row (by key) in `previous`.
 * Rows that are new to `next` are not changes, however they got there (scrolled
 * into view, filtered in), and neither are rows that only moved.
 * @returns Pairs of row index in `next` and column
 */
export function changedCells(
  previous: RowWindow,
  next: RowWindow,
  columnCount: number
): Array<[index: number, column: number]> {
  const previousIndex = new Map<bigint, number>();
  for (let i = 0; i < previous.keys.length; i += 1) {
    previousIndex.set(previous.keys[i], i);
  }
  const changes: Array<[number, number]> = [];
  for (let i = 0; i < next.keys.length; i += 1) {
    const before = previousIndex.get(next.keys[i]);
    if (before !== undefined) {
      for (let column = 0; column < columnCount; column += 1) {
        if (!Object.is(previous.value(column, before), next.value(column, i))) {
          changes.push([i, column]);
        }
      }
    }
  }
  return changes;
}

/**
 * A grid model over a window of a larger table: the column list, the table's
 * total row count, and the rows at positions `start..start + window.numRows`
 * decoded from Arrow, with the key of each row. Rows outside the window read
 * as empty (still loading). Also holds what the header shows (the sort) and
 * which cells are flashing.
 */
export class ViewportGridModel extends GridModel {
  private viewColumns: readonly ViewportColumn[];

  private totalRowCount: number;

  private windowStart = 0;

  private windowRowCount = 0;

  private vectors: readonly (Vector | null)[] = [];

  private keys: BigUint64Array = new BigUint64Array(0);

  // Row index in the window by key, for drawing flashes of rows wherever they moved
  private keyIndex = new Map<bigint, number>();

  // Formatted text of the cells in the window, keyed by row * columnCount + column.
  // Cleared with the window, so every frame after the first only does lookups.
  private textCache = new Map<number, string>();

  private sort: readonly SortKey[] = [];

  // When each flashing cell started, by `${row key}:${column}`
  private flashes = new Map<string, number>();

  private listeners = new Set<() => void>();

  constructor(columns: readonly ViewportColumn[] = [], rowCount = 0) {
    super();
    this.viewColumns = columns;
    this.totalRowCount = rowCount;
  }

  get rowCount(): number {
    return this.totalRowCount;
  }

  get columnCount(): number {
    return this.viewColumns.length;
  }

  get columns(): readonly ViewportColumn[] {
    return this.viewColumns;
  }

  /** First row position held in the window */
  get start(): number {
    return this.windowStart;
  }

  /** Call `listener` after every change; returns the function that stops it */
  subscribe(listener: () => void): () => void {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  }

  private notify(): void {
    this.listeners.forEach(listener => listener());
  }

  /** Replace the column list. Drops the window, whose columns no longer match. */
  setColumns(columns: readonly ViewportColumn[]): void {
    this.viewColumns = columns;
    this.clearWindow();
    this.notify();
  }

  /** Set the total row count of the table */
  setRowCount(rowCount: number): void {
    if (rowCount !== this.totalRowCount) {
      this.totalRowCount = rowCount;
      this.notify();
    }
  }

  /**
   * Replace the window with the rows of `table`, which start at row position
   * `start`, hold the columns in `columns` order, and have the given keys.
   * Cells whose value changed since the last window, for the same key, flash.
   */
  setWindow(
    start: number,
    table: Table,
    keys: BigUint64Array,
    now = performance.now()
  ): void {
    const vectors = this.viewColumns.map((_, index) => table.getChildAt(index));
    const next: RowWindow = {
      keys,
      value: (column, index) => vectors[column]?.get(index) ?? null,
    };
    const changes = changedCells(this.rowWindow(), next, vectors.length);
    for (let i = 0; i < changes.length; i += 1) {
      const [index, column] = changes[i];
      this.flashes.set(`${keys[index]}:${column}`, now);
    }

    this.windowStart = start;
    this.windowRowCount = table.numRows;
    this.vectors = vectors;
    this.keys = keys;
    this.keyIndex = new Map();
    for (let i = 0; i < keys.length; i += 1) {
      this.keyIndex.set(keys[i], i);
    }
    this.textCache.clear();
    this.notify();
  }

  private rowWindow(): RowWindow {
    return {
      keys: this.keys,
      value: (column, index) => this.vectors[column]?.get(index) ?? null,
    };
  }

  private clearWindow(): void {
    this.windowStart = 0;
    this.windowRowCount = 0;
    this.vectors = [];
    this.keys = new BigUint64Array(0);
    this.keyIndex = new Map();
    this.textCache.clear();
    this.flashes.clear();
  }

  /** Set the sort the header shows */
  setSort(sort: readonly SortKey[]): void {
    this.sort = sort;
    this.notify();
  }

  get sortKeys(): readonly SortKey[] {
    return this.sort;
  }

  /** The sort key on `column` and its 1-based priority, or null when unsorted */
  sortForColumn(
    column: ModelIndex
  ): { descending: boolean; priority: number } | null {
    const name = this.viewColumns[column]?.name;
    const index = this.sort.findIndex(key => key.column === name);
    return index < 0
      ? null
      : { descending: this.sort[index].descending, priority: index + 1 };
  }

  /** Whether the row at `row` has arrived */
  isRowLoaded(row: ModelIndex): boolean {
    return (
      row >= this.windowStart && row < this.windowStart + this.windowRowCount
    );
  }

  /** The raw value at a cell: undefined when the row has not arrived, null for a null */
  valueAt(column: ModelIndex, row: ModelIndex): unknown {
    if (!this.isRowLoaded(row)) {
      return undefined;
    }
    return this.vectors[column]?.get(row - this.windowStart) ?? null;
  }

  /** Start a flash on one cell, eg. to show it was copied */
  flashCell(column: ModelIndex, row: ModelIndex, now = performance.now()): void {
    if (this.isRowLoaded(row)) {
      this.flashes.set(`${this.keys[row - this.windowStart]}:${column}`, now);
      this.notify();
    }
  }

  /** Whether any cell is still flashing at `now` */
  hasFlashes(now = performance.now()): boolean {
    this.pruneFlashes(now);
    return this.flashes.size > 0;
  }

  /**
   * Call `draw` for each flashing cell in the window with how far its flash has
   * faded, from 0 (just started) to 1 (gone).
   */
  forEachFlash(
    now: number,
    draw: (column: ModelIndex, row: ModelIndex, progress: number) => void
  ): void {
    this.pruneFlashes(now);
    this.flashes.forEach((started, cell) => {
      const separator = cell.indexOf(':');
      const index = this.keyIndex.get(BigInt(cell.slice(0, separator)));
      if (index !== undefined) {
        draw(
          Number(cell.slice(separator + 1)),
          this.windowStart + index,
          (now - started) / FLASH_DURATION_MS
        );
      }
    });
  }

  private pruneFlashes(now: number): void {
    this.flashes.forEach((started, cell) => {
      if (now - started >= FLASH_DURATION_MS) {
        this.flashes.delete(cell);
      }
    });
  }

  textForColumnHeader(column: ModelIndex): string {
    return this.viewColumns[column]?.name ?? '';
  }

  textForCell(column: ModelIndex, row: ModelIndex): string {
    if (!this.isRowLoaded(row)) {
      return '';
    }
    const key = (row - this.windowStart) * this.viewColumns.length + column;
    let text = this.textCache.get(key);
    if (text === undefined) {
      const value = this.valueAt(column, row);
      text =
        value == null ? '' : formatValue(this.viewColumns[column].type, value);
      this.textCache.set(key, text);
    }
    return text;
  }

  textAlignForCell(column: ModelIndex): CanvasTextAlign {
    return isNumericType(this.viewColumns[column]?.type ?? '')
      ? 'right'
      : 'left';
  }

  backgroundColorForCell(
    column: ModelIndex,
    row: ModelIndex,
    theme: GridTheme
  ): NullableGridColor {
    return this.isRowLoaded(row) && this.valueAt(column, row) === null
      ? theme.nullBackgroundColor
      : null;
  }
}

export default ViewportGridModel;
