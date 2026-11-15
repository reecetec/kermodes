import { describe, expect, it } from 'bun:test';
import { Float64, Int64, Table, TimestampMillisecond, Utf8, vectorFromArray } from 'apache-arrow';
import { rowRange, sortKeys, tableStatus, windowRows } from './rows';

// A window of four rows that starts at row 100 of its table
const window = new Table({
  sym: vectorFromArray(['AAPL', 'MSFT', null, 'NVDA'], new Utf8()),
  px: vectorFromArray([189.5, 412.25, 1, null], new Float64()),
  qty: vectorFromArray([BigInt(10), BigInt(2) ** BigInt(60), BigInt(-3), null], new Int64()),
  ts: vectorFromArray([1_763_164_800_000, 0, 1, 2], new TimestampMillisecond()),
});

describe('windowRows', () => {
  it('gives the asked rows of the window as plain values', () => {
    const rows = windowRows(window, 100, 100, 2);
    expect(rows).toHaveLength(2);
    expect(rows[0]).toEqual({ sym: 'AAPL', px: 189.5, qty: 10, ts: new Date(1_763_164_800_000) });
    // Past 2^53 an integer stays a bigint rather than losing digits
    expect(rows[1].qty).toBe(BigInt(2) ** BigInt(60));
  });

  it('keeps nulls, and leaves out rows the window does not hold', () => {
    expect(windowRows(window, 100, 102, 10)).toEqual([
      { sym: null, px: 1, qty: -3, ts: new Date(1) },
      { sym: 'NVDA', px: null, qty: null, ts: new Date(2) },
    ]);
    expect(windowRows(window, 100, 90, 12).map(r => r.sym)).toEqual(['AAPL', 'MSFT']);
    expect(windowRows(window, 100, 200, 5)).toEqual([]);
  });
});

describe('sortKeys', () => {
  it('reads a leading minus as descending', () => {
    expect(sortKeys(['-ts', 'sym'])).toEqual([
      { column: 'ts', descending: true },
      { column: 'sym', descending: false },
    ]);
  });
});

describe('rowRange', () => {
  it('gives limit rows from offset, 100 from the start by default', () => {
    expect(rowRange(5_000, {})).toEqual({ from: 0, count: 100 });
    expect(rowRange(5_000, { offset: 40, limit: 10 })).toEqual({ from: 40, count: 10 });
  });

  it('follows the end with last, or gives every row of a shorter table', () => {
    expect(rowRange(5_000, { last: 15, offset: 40 })).toEqual({ from: 4_985, count: 15 });
    expect(rowRange(6, { last: 15 })).toEqual({ from: 0, count: 15 });
  });
});

describe('tableStatus', () => {
  it('says loading, live, or static once rows arrive', () => {
    expect(tableStatus({ status: 'ticking' }, false)).toBe('loading');
    expect(tableStatus({ status: 'loading…' }, true)).toBe('loading');
    expect(tableStatus({ status: 'ticking · updated in cycle 9' }, true)).toBe('live');
    expect(tableStatus({ status: 'static' }, true)).toBe('static');
  });

  it('says whether an error passes by itself', () => {
    expect(tableStatus({ status: 'ticking', error: 'Reconnecting', retrying: true }, true)).toBe(
      'reconnecting'
    );
    expect(tableStatus({ status: 'ticking', error: 'no table', retrying: false }, true)).toBe(
      'failed'
    );
  });
});
