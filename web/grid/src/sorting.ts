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

/**
 * The sort a header shows, given the sort `asked` for and not yet seen applied
 * (or `null`), and the one the engine `reported`; and what is still asked. A
 * sort of a big table takes a while, and the engine's reports until it lands
 * carry the old sort: showing those would flip the header back. The engine
 * always ends up reporting the sort asked for, even one it could not apply.
 */
export function shownSort(
  asked: readonly SortKey[] | null,
  reported: readonly SortKey[]
): { shown: readonly SortKey[]; asked: readonly SortKey[] | null } {
  if (asked == null || sameSort(asked, reported)) {
    return { shown: reported, asked: null };
  }
  return { shown: asked, asked };
}

function sameSort(a: readonly SortKey[], b: readonly SortKey[]): boolean {
  return (
    a.length === b.length &&
    a.every(
      (key, i) => key.column === b[i].column && key.descending === b[i].descending
    )
  );
}
