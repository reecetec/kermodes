import { describe, expect, it } from 'bun:test';
import { nextSort, sortRows } from './list';

describe('nextSort', () => {
  it('goes ascending, descending, then back to the list order; another column starts over', () => {
    const asc = nextSort(null, 2);
    const desc = nextSort(asc, 2);
    expect([asc, desc, nextSort(desc, 2)]).toEqual([
      { column: 2, descending: false },
      { column: 2, descending: true },
      null,
    ]);
    expect(nextSort(desc, 0)).toEqual({ column: 0, descending: false });
  });
});

describe('sortRows', () => {
  const rows = [
    { name: 'pod-10', cpu: 3 },
    { name: 'pod-2', cpu: null },
    { name: 'Pod-1', cpu: 3 },
    { name: 'pod-3', cpu: 0.5 },
  ];
  const value = (row: (typeof rows)[number], column: number) => (column === 0 ? row.name : row.cpu);
  const names = (column: number, descending: boolean) =>
    sortRows(rows, { column, descending }, value).map(r => r.name);

  it('keeps the list order unsorted', () => {
    expect(sortRows(rows, null, value)).toEqual(rows);
  });

  it('orders text naturally, ignoring case', () => {
    expect(names(0, false)).toEqual(['Pod-1', 'pod-2', 'pod-3', 'pod-10']);
    expect(names(0, true)).toEqual(['pod-10', 'pod-3', 'pod-2', 'Pod-1']);
  });

  it('orders numbers as numbers, keeps ties in list order, and puts blanks last both ways', () => {
    expect(names(1, false)).toEqual(['pod-3', 'pod-10', 'Pod-1', 'pod-2']);
    expect(names(1, true)).toEqual(['pod-10', 'Pod-1', 'pod-3', 'pod-2']);
  });
});
