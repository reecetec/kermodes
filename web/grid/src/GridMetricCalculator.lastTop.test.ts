import { describe, expect, it } from 'bun:test';
import GridMetricCalculator, { type GridMetricState } from './GridMetricCalculator';
import theme from './GridTheme';

/** Enough of a grid's state for scroll limits: a model of `rowCount` rows and the theme */
function state(rowCount: number): GridMetricState {
  return {
    model: { rowCount, floatingBottomRowCount: 0, floatingTopRowCount: 0 },
    theme,
    movedRows: [],
    movedColumns: [],
  } as unknown as GridMetricState;
}

describe('getLastTop', () => {
  it('is the first row of an empty table, however little room there is', () => {
    // A panel laid out while hidden has no height, and a table opened before its rows arrive
    // has none: a top of -1 drew an empty row above the first until the grid was scrolled
    const calculator = new GridMetricCalculator();
    expect(calculator.getLastTop(state(0), null, 0)).toBe(0);
    expect(calculator.getLastTop(state(0), null, 500)).toBe(0);
  });
});
