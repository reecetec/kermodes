import { describe, expect, it } from 'bun:test';
import { type FilterCell, filterCells } from './FilterBar';

const COLUMNS = [
  { name: 'sym', type: 'utf8' },
  { name: 'px', type: 'float64' },
  { name: 'qty', type: 'int64' },
];

// Three 100-wide columns in model order, 10 pixels in from the canvas edge
const METRICS = {
  columnsForRender: [0, 1, 2],
  allColumnXs: new Map([
    [0, 0],
    [1, 100],
    [2, 200],
  ]),
  allColumnWidths: new Map([
    [0, 100],
    [1, 100],
    [2, 100],
  ]),
  modelColumns: new Map([
    [0, 0],
    [1, 1],
    [2, 2],
  ]),
  gridX: 10,
};

const lefts = (cells: FilterCell[]) =>
  Object.fromEntries(cells.map(cell => [cell.column.name, cell.left]));

describe('filterCells', () => {
  it('puts each input over its column', () => {
    expect(lefts(filterCells(METRICS, COLUMNS))).toEqual({ sym: 10, px: 110, qty: 210 });
  });

  it('moves a dragged column’s input with the drag, before it reaches a new slot', () => {
    const dragging = { range: [0, 0] as [number, number], depth: 0, left: 145, width: 100 };
    expect(lefts(filterCells(METRICS, COLUMNS, dragging))).toEqual({
      sym: 155,
      px: 110,
      qty: 210,
    });
  });

  it('moves every input of a dragged range together', () => {
    const dragging = { range: [1, 2] as [number, number], depth: 0, left: 30, width: 200 };
    expect(lefts(filterCells(METRICS, COLUMNS, dragging))).toEqual({
      sym: 10,
      px: 40,
      qty: 140,
    });
  });
});
