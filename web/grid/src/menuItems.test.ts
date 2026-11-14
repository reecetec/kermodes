import { describe, expect, it } from 'bun:test';
import { contextMenu, type MenuItem, type MenuTarget, type MenuView } from './menuItems';

const VIEW: MenuView = { sort: [], filters: {}, isSelectionWide: false };

const labels = (sections: MenuItem[][]) => sections.map(section => section.map(item => item.label));

/** The quick-filter entries a cell's menu offers, by label */
function filters(target: MenuTarget): Record<string, string> {
  return Object.fromEntries(
    contextMenu(target, VIEW)
      .flat()
      .flatMap(item => (item.action.type === 'filter' ? [[item.label, item.action.text]] : []))
  );
}

const cell = (type: string, value: unknown, text: string): MenuTarget => ({
  kind: 'cell',
  column: 'c',
  type,
  value,
  text,
});

describe('contextMenu', () => {
  it('offers a header sorting, filtering, and its name; clearing only what is set', () => {
    const header: MenuTarget = { kind: 'header', column: 'px', type: 'float64' };
    expect(labels(contextMenu(header, VIEW))).toEqual([
      ['Sort ascending', 'Sort descending'],
      ['Filter px…'],
      ['Copy column name'],
    ]);
    const sortedAndFiltered = contextMenu(header, {
      ...VIEW,
      sort: [{ column: 'sym', descending: false }],
      filters: { px: '> 5', sym: ' ' },
    });
    expect(labels(sortedAndFiltered)).toEqual([
      ['Sort ascending', 'Sort descending', 'Clear sort'],
      ['Filter px…', 'Clear filter on px', 'Clear all filters'],
      ['Copy column name'],
    ]);
    expect(sortedAndFiltered[0][2].action).toEqual({ type: 'sort', by: [] });
    expect(sortedAndFiltered[1][1].action).toEqual({ type: 'filter', column: 'px', text: '' });
  });

  it('filters a number by its raw value, not the rounded text it shows', () => {
    expect(filters(cell('float64', 54.64651234, '54.6465'))).toEqual({
      'c = 54.6465': '=54.64651234',
      'c ≠ 54.6465': '!=54.64651234',
      'c ≥ 54.6465': '>=54.64651234',
      'c ≤ 54.6465': '<=54.64651234',
    });
    expect(filters(cell('int64', BigInt('9007199254740993'), '9,007,199,254,740,993'))).toMatchObject({
      'c = 9,007,199,254,740,993': '=9007199254740993',
    });
  });

  it('offers no value filter for a number with no value to compare', () => {
    expect(filters(cell('float64', Number.NaN, 'NaN'))).toEqual({});
  });

  it('filters strings exactly, nulls by null, and dates by their day', () => {
    expect(filters(cell('utf8', 'AAPL', 'AAPL'))).toEqual({
      'c = "AAPL"': '=AAPL',
      'c ≠ "AAPL"': '!=AAPL',
    });
    expect(filters(cell('utf8', null, ''))).toEqual({
      'c is null': 'null',
      'c is not null': '!null',
    });
    expect(filters(cell('date32', Date.UTC(2026, 10, 11), '2026-11-11'))).toMatchObject({
      'c = 2026-11-11': '=2026-11-11',
    });
  });

  it('bounds a timestamp by the shown millisecond, since finer ones never reach the browser', () => {
    const at = Date.UTC(2026, 10, 14, 0, 25, 57, 855);
    expect(filters(cell('timestamp', at, '2026-11-14 00:25:57.855Z'))).toEqual({
      'c ≥ 2026-11-14 00:25:57.855Z': '>=2026-11-14T00:25:57.855Z',
      'c < 2026-11-14 00:25:57.855Z': '<2026-11-14T00:25:57.855Z',
    });
  });

  it('copies the selection only when it is more than the clicked cell', () => {
    const target = cell('utf8', 'AAPL', 'AAPL');
    expect(labels(contextMenu(target, VIEW))[0]).toEqual(['Copy cell']);
    expect(labels(contextMenu(target, { ...VIEW, isSelectionWide: true }))[0]).toEqual([
      'Copy cell',
      'Copy selection',
      'Copy selection with headers',
    ]);
  });
});
