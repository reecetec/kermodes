import { describe, expect, it } from 'bun:test';
import { type CatalogTable, namespaces, tableDates, totals, utcDate } from './model';

function table(name: string, weir: [string, number][], history: [string, number][]): CatalogTable {
  return {
    table: name,
    kind: 'append',
    key: [],
    sort: [],
    columns: [],
    weir: weir.map(([date, rows]) => ({ date, rows, bytes: rows * 10 })),
    history: history.map(([date, rows]) => ({ date, rows, bytes: rows * 2, files: 1 })),
  };
}

const today = '2026-11-14';
// 2026-11-13 is merged but not yet dropped from weir: in both.
const ticks = table(
  'md.ticks',
  [
    ['2026-11-13', 50],
    [today, 7],
  ],
  [
    ['2026-11-12', 40],
    ['2026-11-13', 50],
  ]
);
const quotes = table('md.quotes', [[today, 3]], []);
const checks = table('verify.rich', [], [['2026-11-01', 5]]);

describe('totals', () => {
  it("counts today's rows from today's weir date only, and each date once", () => {
    expect(totals([ticks], today)).toEqual({
      tables: 1,
      today: 7,
      weirBytes: 570,
      historyRows: 90,
      historyBytes: 180,
      dates: 3,
      first: '2026-11-12',
      last: today,
    });
  });

  it('adds tables together, dates as a union', () => {
    const sum = totals([ticks, quotes, checks], today);
    expect([sum.tables, sum.today, sum.dates, sum.first, sum.last]).toEqual([
      3,
      10,
      4,
      '2026-11-01',
      today,
    ]);
  });

  it('has no dates for tables with no rows', () => {
    const sum = totals([table('md.empty', [], [])], today);
    expect([sum.dates, sum.first, sum.last]).toEqual([0, null, null]);
  });
});

describe('namespaces', () => {
  it('groups tables by namespace, both in name order, with their totals', () => {
    const grouped = namespaces([checks, ticks, quotes], today);
    expect(grouped.map(n => [n.namespace, n.tables.map(t => t.table), n.totals.today])).toEqual([
      ['md', ['md.quotes', 'md.ticks'], 10],
      ['verify', ['verify.rich'], 0],
    ]);
  });
});

describe('tableDates', () => {
  it('lists each date newest first with what weir and history hold of it', () => {
    expect(
      tableDates(ticks).map(d => [d.date, d.weir?.rows ?? null, d.history?.rows ?? null])
    ).toEqual([
      [today, 7, null],
      ['2026-11-13', 50, 50],
      ['2026-11-12', null, 40],
    ]);
  });
});

describe('utcDate', () => {
  it('is the UTC date, whatever the local zone', () => {
    expect(utcDate(new Date('2026-11-14T23:30:00-05:00'))).toBe('2026-11-15');
  });
});
