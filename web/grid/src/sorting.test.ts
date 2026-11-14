import { describe, expect, it } from 'bun:test';
import { nextSort, shownView } from './sorting';

const asc = (column: string) => ({ column, descending: false });
const desc = (column: string) => ({ column, descending: true });

describe('nextSort', () => {
  it('cycles a plain click ascending, descending, off', () => {
    expect(nextSort([], 'px', false)).toEqual([asc('px')]);
    expect(nextSort([asc('px')], 'px', false)).toEqual([desc('px')]);
    expect(nextSort([desc('px')], 'px', false)).toEqual([]);
  });

  it('makes a plainly clicked column the only key', () => {
    expect(nextSort([asc('sym'), desc('qty')], 'px', false)).toEqual([
      asc('px'),
    ]);
    // A secondary key clicked plainly cycles from where it was and drops the others
    expect(nextSort([asc('sym'), asc('qty')], 'qty', false)).toEqual([
      desc('qty'),
    ]);
  });

  it('appends a shift-clicked unsorted column as the last key', () => {
    expect(nextSort([asc('sym')], 'px', true)).toEqual([asc('sym'), asc('px')]);
    expect(nextSort([], 'px', true)).toEqual([asc('px')]);
  });

  it('cycles a shift-clicked key in place and keeps the others', () => {
    const sort = [asc('sym'), asc('px'), desc('qty')];
    const flipped = nextSort(sort, 'px', true);
    expect(flipped).toEqual([asc('sym'), desc('px'), desc('qty')]);
    expect(nextSort(flipped, 'px', true)).toEqual([asc('sym'), desc('qty')]);
  });
});

describe('shownView', () => {
  it('is pending, showing the sort asked for, until the engine applies it', () => {
    // Reports of the old sort while the engine sorts
    expect(shownView(4, 3, [desc('px')], [asc('px')])).toEqual({
      pending: true,
      sort: [desc('px')],
    });
    expect(shownView(4, 4, [desc('px')], [desc('px')])).toEqual({
      pending: false,
      sort: [desc('px')],
    });
  });

  it('is pending on a filter, keeping the reported sort', () => {
    expect(shownView(5, 4, null, [asc('sym')])).toEqual({
      pending: true,
      sort: [asc('sym')],
    });
  });

  it('shows what the engine reports once caught up, even unlike what was asked', () => {
    expect(shownView(4, 4, [desc('px')], [asc('sym')])).toEqual({
      pending: false,
      sort: [asc('sym')],
    });
    // A grid that never sent anything, or reopened on a viewer ahead of it
    expect(shownView(0, 0, null, [])).toEqual({ pending: false, sort: [] });
    expect(shownView(0, 6, null, [asc('px')])).toEqual({
      pending: false,
      sort: [asc('px')],
    });
  });
});
