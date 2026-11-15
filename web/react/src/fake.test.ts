import { describe, expect, it } from 'bun:test';
import { tableFromIPC } from 'apache-arrow';
import { toBytes } from '@kermodes/client';
import { fakeKermodes } from './fake';
import { tableStatus, windowRows } from './rows';

const at = new Date(Date.UTC(2026, 10, 15, 9, 30));

/** What `useTable` would show of `table`'s rows `from`..`from + count`, as it decodes them */
function watch(kermodes: ReturnType<typeof fakeKermodes>, table: string, from: number, count: number) {
  const view = kermodes.open(table);
  let shown: Record<string, unknown>[] = [];
  view.onMessage((header, buffers) => {
    if (header.type === 'rows') {
      shown = windowRows(tableFromIPC(toBytes(buffers[0])), header.start, from, count);
    }
  });
  view.send({ type: 'viewport', top: from, bottom: from + count - 1 });
  return { view, rows: () => shown };
}

describe('fakeKermodes', () => {
  it('serves its rows as the server does, and ticks when set', () => {
    const kermodes = fakeKermodes({
      'md.ticks': [
        { sym: 'AAPL', price: 189.5, ts: at },
        { sym: 'MSFT', price: 410.25, ts: at },
        { sym: 'NVDA', price: 120, ts: at },
      ],
    });
    expect(kermodes.tables.map(table => table.name)).toEqual(['md.ticks']);
    const { view, rows } = watch(kermodes, 'md.ticks', 1, 2);
    expect(rows()).toEqual([
      { sym: 'MSFT', price: 410.25, ts: at },
      { sym: 'NVDA', price: 120, ts: at },
    ]);
    expect(view.state().columns).toEqual([
      { name: 'sym', type: 'utf8' },
      { name: 'price', type: 'float64' },
      { name: 'ts', type: 'timestamp' },
    ]);
    expect(tableStatus(view.state(), true)).toBe('live');

    let states = 0;
    view.onState(() => {
      states += 1;
    });
    kermodes.set('md.ticks', [
      { sym: 'AAPL', price: 190, ts: at },
      { sym: 'MSFT', price: 411, ts: at },
    ]);
    expect(states).toBe(1);
    expect(view.state().row_count).toBe(2);
    expect(rows()).toEqual([{ sym: 'MSFT', price: 411, ts: at }]);
  });

  it('fails a table it does not have, as a missing table does', () => {
    const { view } = watch(fakeKermodes({}), 'md.nothing', 0, 10);
    expect(tableStatus(view.state(), false)).toBe('failed');
    expect(view.state().error).toBe('no table md.nothing');
  });
});
