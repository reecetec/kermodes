import { type Table, type Vector } from 'apache-arrow';
import GridModel from './GridModel';
import { type ModelIndex } from './GridMetrics';
import { type GridTheme, type NullableGridColor } from './GridTheme';

/** A column of the viewed table, as the engine names its type */
export type ViewportColumn = {
  name: string;
  type: string;
};

const IS_NUMERIC: Readonly<Record<string, true>> = {
  int32: true,
  int64: true,
  float64: true,
};

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

/**
 * A grid model over a window of a larger table: the column list, the table's
 * total row count, and the rows at positions `start..start + window.numRows`
 * decoded from Arrow. Rows outside the window read as empty (still loading).
 */
export class ViewportGridModel extends GridModel {
  private columns: readonly ViewportColumn[];

  private totalRowCount: number;

  private windowStart = 0;

  private windowRowCount = 0;

  private vectors: readonly (Vector | null)[] = [];

  // Formatted text of the cells in the window, keyed by row * columnCount + column.
  // Cleared with the window, so every frame after the first only does lookups.
  private textCache = new Map<number, string>();

  constructor(columns: readonly ViewportColumn[] = [], rowCount = 0) {
    super();
    this.columns = columns;
    this.totalRowCount = rowCount;
  }

  get rowCount(): number {
    return this.totalRowCount;
  }

  get columnCount(): number {
    return this.columns.length;
  }

  /** First row position held in the window */
  get start(): number {
    return this.windowStart;
  }

  /** Replace the column list. Drops the window, whose columns no longer match. */
  setColumns(columns: readonly ViewportColumn[]): void {
    this.columns = columns;
    this.clearWindow();
  }

  /** Set the total row count of the table */
  setRowCount(rowCount: number): void {
    this.totalRowCount = rowCount;
  }

  /**
   * Replace the window with the rows of `table`, which start at row position
   * `start` and hold the columns in `columns` order.
   */
  setWindow(start: number, table: Table): void {
    this.windowStart = start;
    this.windowRowCount = table.numRows;
    this.vectors = this.columns.map((_, index) => table.getChildAt(index));
    this.textCache.clear();
  }

  clearWindow(): void {
    this.windowStart = 0;
    this.windowRowCount = 0;
    this.vectors = [];
    this.textCache.clear();
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

  textForColumnHeader(column: ModelIndex): string {
    return this.columns[column]?.name ?? '';
  }

  textForCell(column: ModelIndex, row: ModelIndex): string {
    if (!this.isRowLoaded(row)) {
      return '';
    }
    const key = (row - this.windowStart) * this.columns.length + column;
    let text = this.textCache.get(key);
    if (text === undefined) {
      const value = this.valueAt(column, row);
      text = value == null ? '' : formatValue(this.columns[column].type, value);
      this.textCache.set(key, text);
    }
    return text;
  }

  textAlignForCell(column: ModelIndex): CanvasTextAlign {
    const type = this.columns[column]?.type ?? '';
    return IS_NUMERIC[type] === true ? 'right' : 'left';
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
