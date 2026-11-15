import { useCallback, useState } from 'react';

/** A value a list column sorts by; null sorts last either way */
export type SortValue = string | number | null;

export type ListSort = { column: number; descending: boolean } | null;

/** Clicking a header: ascending, then descending, then the list's own order */
export function nextSort(current: ListSort, column: number): ListSort {
  if (current == null || current.column !== column) {
    return { column, descending: false };
  }
  return current.descending ? null : { column, descending: true };
}

const text = new Intl.Collator(undefined, { numeric: true, sensitivity: 'base' });

/**
 * `rows` in `sort`'s order by the column's values (`valueOf`); unsorted, as they are. Blanks
 * go last in both directions, numbers compare as numbers and text naturally ("pod-2" before
 * "pod-10"), and ties keep the list's own order.
 */
export function sortRows<T>(
  rows: readonly T[],
  sort: ListSort,
  valueOf: (row: T, column: number) => SortValue
): T[] {
  if (sort == null) {
    return [...rows];
  }
  const sign = sort.descending ? -1 : 1;
  return rows
    .map((row, index) => ({ row, index, value: valueOf(row, sort.column) }))
    .sort((a, b) => {
      if (a.value == null || b.value == null) {
        return a.value == null && b.value == null ? a.index - b.index : a.value == null ? 1 : -1;
      }
      const order =
        typeof a.value === 'number' && typeof b.value === 'number'
          ? a.value - b.value
          : text.compare(String(a.value), String(b.value));
      return order === 0 ? a.index - b.index : sign * order;
    })
    .map(({ row }) => row);
}

/** The narrowest a column can be dragged to */
export const MIN_COLUMN_WIDTH = 48;

/**
 * A list's column widths in px, as the person last dragged them, kept per browser under
 * `name`. Widths saved for another set of columns are ignored.
 */
export function useColumnWidths(
  name: string,
  defaults: readonly number[]
): [number[], (column: number, width: number | null) => void] {
  const key = `kermodes.${name}.columns`;
  const [widths, setWidths] = useState<number[]>(() => {
    try {
      const saved: unknown = JSON.parse(localStorage.getItem(key) ?? 'null');
      if (Array.isArray(saved) && saved.length === defaults.length) {
        return saved.map((w, i) => (typeof w === 'number' ? w : defaults[i]));
      }
    } catch {
      // An unreadable value is the same as none
    }
    return [...defaults];
  });
  const setWidth = useCallback(
    (column: number, width: number | null) =>
      setWidths(current => {
        const next = [...current];
        next[column] = width == null ? defaults[column] : Math.max(MIN_COLUMN_WIDTH, width);
        localStorage.setItem(key, JSON.stringify(next));
        return next;
      }),
    [key, defaults]
  );
  return [widths, setWidth];
}
