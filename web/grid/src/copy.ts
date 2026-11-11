import GridRange from './GridRange';
import { type VisibleIndex } from './GridMetrics';
import {
  formatValue,
  isNumericType,
  type ViewportColumn,
} from './ViewportGridModel';

/** The most rows one copy takes; bigger selections copy their first rows */
export const COPY_ROW_LIMIT = 100_000;

/** A cell's value as copied: displayed text, numbers raw (no thousands separators), nulls empty */
export function copyValue(type: string, value: unknown): string {
  if (value == null) {
    return '';
  }
  return isNumericType(type) ? String(value) : formatValue(type, value);
}

/** One field of tab-separated text, quoted when it holds a tab, line break, or quote */
export function tsvField(text: string): string {
  return /[\t\n\r"]/.test(text) ? `"${text.replaceAll('"', '""')}"` : text;
}

export function toTsv(rows: readonly (readonly string[])[]): string {
  return rows.map(row => row.map(tsvField).join('\t')).join('\n');
}

/** Which cells a copy of the selected ranges takes */
export type CopyPlan = {
  // Row positions, in grid order
  rows: number[];
  // Visible column indexes, in display order
  columns: VisibleIndex[];
  // Whether the selection has more rows than were taken
  isCapped: boolean;
  // How many rows the selection holds, taken or not
  selectedRowCount: number;
  // Whether the selection holds the cell; cells in `rows` x `columns` outside it copy empty
  contains: (column: VisibleIndex, row: number) => boolean;
};

export function planCopy(
  ranges: readonly GridRange[],
  columnCount: number,
  rowCount: number,
  limit = COPY_ROW_LIMIT
): CopyPlan {
  const bounded = ranges.map(range =>
    GridRange.boundedRange(range, columnCount, rowCount)
  );

  const spans = bounded
    .map(range => [range.startRow, range.endRow] as [number, number])
    .sort((a, b) => a[0] - b[0]);
  const rows: number[] = [];
  let total = 0;
  let nextFree = 0;
  for (const [start, end] of spans) {
    const from = Math.max(start, nextFree);
    if (from <= end) {
      total += end - from + 1;
      for (let row = from; row <= end && rows.length < limit; row += 1) {
        rows.push(row);
      }
      nextFree = end + 1;
    }
  }

  const columnSet = new Set<VisibleIndex>();
  for (const range of bounded) {
    for (let column = range.startColumn; column <= range.endColumn; column += 1) {
      columnSet.add(column);
    }
  }

  return {
    rows,
    columns: [...columnSet].sort((a, b) => a - b),
    isCapped: total > rows.length,
    selectedRowCount: total,
    contains: (column, row) =>
      bounded.some(range => range.containsCell(column, row)),
  };
}

/**
 * The tab-separated text of a copy: one line per row of the plan, optionally
 * after a line of column names.
 */
export function copyText(
  plan: CopyPlan,
  columns: readonly ViewportColumn[],
  modelColumnOf: (column: VisibleIndex) => number,
  valueAt: (modelColumn: number, row: number) => unknown,
  includeHeader: boolean
): string {
  const modelColumns = plan.columns.map(modelColumnOf);
  const lines: string[][] = [];
  if (includeHeader) {
    lines.push(modelColumns.map(column => columns[column].name));
  }
  for (const row of plan.rows) {
    lines.push(
      plan.columns.map((column, i) =>
        plan.contains(column, row)
          ? copyValue(
              columns[modelColumns[i]].type,
              valueAt(modelColumns[i], row)
            )
          : ''
      )
    );
  }
  return toTsv(lines);
}

/**
 * Put `text` on the clipboard. Notebook front ends can run outputs in frames
 * that deny the clipboard API, so fall back to copying from a hidden textarea.
 */
export async function writeClipboard(text: string): Promise<void> {
  try {
    await navigator.clipboard.writeText(text);
    return;
  } catch {
    // Fall through to the textarea
  }
  const textarea = document.createElement('textarea');
  textarea.value = text;
  textarea.setAttribute('readonly', '');
  textarea.style.position = 'fixed';
  textarea.style.opacity = '0';
  const active = document.activeElement;
  document.body.appendChild(textarea);
  textarea.select();
  const isCopied = document.execCommand('copy');
  textarea.remove();
  if (active instanceof HTMLElement) {
    active.focus();
  }
  if (!isCopied) {
    throw new Error('The browser did not allow copying');
  }
}
