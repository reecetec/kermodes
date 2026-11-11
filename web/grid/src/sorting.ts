import { type SortKey } from './ViewportGridModel';

/**
 * The sort after clicking the header of `column`. Each click cycles the column
 * ascending → descending → unsorted. A plain click makes it the only key; an
 * additive (shift) click cycles it in place, appending it as the last key if it
 * was unsorted, and keeps the other keys.
 */
export function nextSort(
  sort: readonly SortKey[],
  column: string,
  additive: boolean
): SortKey[] {
  const current = sort.find(key => key.column === column);
  const next: SortKey | null =
    current == null
      ? { column, descending: false }
      : current.descending
        ? null
        : { column, descending: true };

  if (!additive) {
    return next == null ? [] : [next];
  }
  if (current == null) {
    return [...sort, next as SortKey];
  }
  return next == null
    ? sort.filter(key => key.column !== column)
    : sort.map(key => (key.column === column ? next : key));
}
