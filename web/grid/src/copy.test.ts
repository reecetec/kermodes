import { describe, expect, it } from 'bun:test';
import GridRange from './GridRange';
import { GridUtils } from './GridUtils';
import { copyText, copyValue, planCopy, toTsv } from './copy';
import { type ViewportColumn } from './ViewportGridModel';

const COLUMNS: ViewportColumn[] = [
  { name: 'sym', type: 'utf8' },
  { name: 'qty', type: 'int64' },
  { name: 'px', type: 'float64' },
  { name: 'ts', type: 'timestamp' },
];

const TS = Date.UTC(2026, 10, 11, 9, 38, 37, 123);

// Rows by position, values by model column
const ROWS: unknown[][] = [
  ['AAPL', BigInt(1200300), 1234.5, TS],
  [null, BigInt(-5), null, null],
  ['say "hi"\tthere', BigInt(0), 0.25, TS + 1000],
];

const valueAt = (column: number, row: number): unknown => ROWS[row][column];
const identity = (column: number): number => column;

describe('copyValue', () => {
  it('copies numbers raw, timestamps as displayed, nulls empty', () => {
    expect(copyValue('int64', BigInt(1200300))).toBe('1200300');
    expect(copyValue('float64', 1234.5)).toBe('1234.5');
    expect(copyValue('timestamp', TS)).toBe('2026-11-11 09:38:37.123Z');
    expect(copyValue('date32', Date.UTC(2026, 0, 2))).toBe('2026-01-02');
    expect(copyValue('bool', false)).toBe('false');
    expect(copyValue('utf8', null)).toBe('');
  });
});

describe('copyText', () => {
  it('copies a range as rows of tab-separated values, nulls empty', () => {
    const plan = planCopy([new GridRange(0, 0, 3, 1)], 4, 3);
    expect(copyText(plan, COLUMNS, identity, valueAt, false)).toBe(
      [
        'AAPL\t1200300\t1234.5\t2026-11-11 09:38:37.123Z',
        '\t-5\t\t',
      ].join('\n')
    );
  });

  it('adds a header line and quotes fields with tabs or quotes', () => {
    const plan = planCopy([new GridRange(0, 2, 1, 2)], 4, 3);
    expect(copyText(plan, COLUMNS, identity, valueAt, true)).toBe(
      'sym\tqty\n"say ""hi""\tthere"\t0'
    );
  });

  it('copies columns in display order', () => {
    // ts moved to the front
    const moved = GridUtils.moveItem(3, 0, []);
    const plan = planCopy([new GridRange(0, 0, 1, 0)], 4, 3);
    expect(
      copyText(
        plan,
        COLUMNS,
        column => GridUtils.getModelIndex(column, moved),
        valueAt,
        true
      )
    ).toBe('ts\tsym\n2026-11-11 09:38:37.123Z\tAAPL');
  });

  it('puts several ranges in grid order, leaving unselected cells empty', () => {
    const plan = planCopy(
      [new GridRange(1, 2, 1, 2), new GridRange(0, 0, 0, 0)],
      4,
      3
    );
    expect(plan.rows).toEqual([0, 2]);
    expect(copyText(plan, COLUMNS, identity, valueAt, false)).toBe(
      'AAPL\t\n\t0'
    );
  });
});

describe('planCopy', () => {
  it('takes whole columns and caps the rows', () => {
    const plan = planCopy([new GridRange(1, null, 1, null)], 4, 1_000, 100);
    expect(plan.columns).toEqual([1]);
    expect(plan.rows).toHaveLength(100);
    expect(plan.rows[99]).toBe(99);
    expect(plan.isCapped).toBe(true);
  });

  it('counts overlapping ranges once', () => {
    const plan = planCopy(
      [new GridRange(0, 0, 0, 5), new GridRange(1, 3, 1, 8)],
      4,
      100,
      9
    );
    expect(plan.rows).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8]);
    expect(plan.isCapped).toBe(false);
  });
});

describe('toTsv', () => {
  it('quotes line breaks', () => {
    expect(toTsv([['a\nb', 'c']])).toBe('"a\nb"\tc');
  });
});
