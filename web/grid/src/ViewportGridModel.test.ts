import { describe, expect, it } from 'bun:test';
import {
  Bool,
  DateDay,
  Field,
  Float64,
  Int32,
  Int64,
  Schema,
  Table,
  TimestampNanosecond,
  Utf8,
  makeData,
  RecordBatch,
  Struct,
  tableFromIPC,
  tableToIPC,
  vectorFromArray,
} from 'apache-arrow';
import { darkTheme } from './GridTheme';
import ViewportGridModel, { type ViewportColumn } from './ViewportGridModel';

const COLUMNS: ViewportColumn[] = [
  { name: 'flag', type: 'bool' },
  { name: 'qty', type: 'int32' },
  { name: 'id', type: 'int64' },
  { name: 'px', type: 'float64' },
  { name: 'sym', type: 'utf8' },
  { name: 'day', type: 'date32' },
  { name: 'ts', type: 'timestamp' },
];

/** Two rows as an Arrow IPC stream, decoded the way the widget does it */
function twoRows(): Table {
  const vectors = [
    vectorFromArray([true, null], new Bool()),
    vectorFromArray([1234567, -5], new Int32()),
    vectorFromArray([BigInt('9007199254740993'), null], new Int64()),
    vectorFromArray([1234.5, Number.NaN], new Float64()),
    vectorFromArray(['AAPL', null], new Utf8()),
    vectorFromArray([Date.UTC(2026, 10, 11), null], new DateDay()),
    // Builders take epoch milliseconds and scale them to the type's unit
    vectorFromArray(
      [Date.UTC(2026, 10, 11, 9, 38, 37, 123), null],
      new TimestampNanosecond()
    ),
  ];
  const schema = new Schema(
    COLUMNS.map((column, i) => new Field(column.name, vectors[i].type, true))
  );
  const data = makeData({
    type: new Struct(schema.fields),
    length: 2,
    nullCount: 0,
    children: vectors.map(vector => vector.data[0]),
  });
  const table = new Table(new RecordBatch(schema, data));
  return tableFromIPC(tableToIPC(table, 'stream'));
}

describe('ViewportGridModel', () => {
  it('renders an empty body before any rows arrive', () => {
    const model = new ViewportGridModel(COLUMNS, 1_000_000_000);
    expect(model.rowCount).toBe(1_000_000_000);
    expect(model.columnCount).toBe(COLUMNS.length);
    expect(model.textForColumnHeader(4)).toBe('sym');
    expect(model.textForCell(1, 0)).toBe('');
    expect(model.backgroundColorForCell(1, 0, darkTheme)).toBeNull();
  });

  it('formats the window rows by column type', () => {
    const model = new ViewportGridModel(COLUMNS, 1_000_000_000);
    model.setWindow(500, twoRows());

    expect(model.textForCell(0, 500)).toBe('true');
    expect(model.textForCell(1, 500)).toBe('1,234,567');
    expect(model.textForCell(1, 501)).toBe('-5');
    expect(model.textForCell(2, 500)).toBe('9,007,199,254,740,993');
    expect(model.textForCell(3, 500)).toBe('1,234.5000');
    expect(model.textForCell(3, 501)).toBe('NaN');
    expect(model.textForCell(4, 500)).toBe('AAPL');
    expect(model.textForCell(5, 500)).toBe('2026-11-11');
    expect(model.textForCell(6, 500)).toBe('2026-11-11 09:38:37.123Z');
  });

  it('right-aligns numbers only', () => {
    const model = new ViewportGridModel(COLUMNS, 2);
    expect(COLUMNS.map((_, i) => model.textAlignForCell(i))).toEqual([
      'left',
      'right',
      'right',
      'right',
      'left',
      'left',
      'left',
    ]);
  });

  it('shows nulls as empty muted cells and rows outside the window as blank', () => {
    const model = new ViewportGridModel(COLUMNS, 1_000);
    model.setWindow(10, twoRows());

    expect(model.textForCell(4, 11)).toBe('');
    expect(model.backgroundColorForCell(4, 11, darkTheme)).toBe(
      darkTheme.nullBackgroundColor
    );
    expect(model.backgroundColorForCell(4, 10, darkTheme)).toBeNull();
    expect(model.textForCell(4, 9)).toBe('');
    expect(model.textForCell(4, 12)).toBe('');
    expect(model.backgroundColorForCell(4, 12, darkTheme)).toBeNull();
  });

  it('drops the window when the columns change', () => {
    const model = new ViewportGridModel(COLUMNS, 2);
    model.setWindow(0, twoRows());
    model.setColumns([{ name: 'sym', type: 'utf8' }]);

    expect(model.columnCount).toBe(1);
    expect(model.isRowLoaded(0)).toBe(false);
    expect(model.textForCell(0, 0)).toBe('');
  });
});
