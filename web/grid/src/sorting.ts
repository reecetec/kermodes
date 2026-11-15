import { type SortKey } from '@kermodes/client';

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

/**
 * What the viewer shows on an engine report. `sent` is the number of the last
 * sort or filter sent, `applied` the highest the engine reports done: until it
 * catches up the rows predate a change, which can take seconds on a big table,
 * so they are `pending` and the header keeps the sort `asked` for (`null` if
 * none was) rather than flip back to the old one the reports still carry.
 */
export function shownView(
  sent: number,
  applied: number,
  asked: readonly SortKey[] | null,
  reported: readonly SortKey[]
): { pending: boolean; sort: readonly SortKey[] } {
  const pending = applied < sent;
  return { pending, sort: pending && asked != null ? asked : reported };
}
