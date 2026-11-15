import React from 'react';
import type GridMetrics from './GridMetrics';
import { type DraggingColumn } from './mouse-handlers/GridColumnMoveMouseHandler';
import { type ViewportColumn } from '@kermodes/client';
import { isNumericType } from './ViewportGridModel';

/** Height of the quick filter bar under the column headers */
export const FILTER_BAR_HEIGHT = 26;

export const FILTER_SYNTAX = [
  'Quick filters (Ctrl+F / ⌘F)',
  'Text: abc contains · =abc exactly · !abc does not contain · !=abc not equal',
  'Numbers, dates, times: 5 · =5 · !=5 · >5 · >=5 · <5 · <=5',
  'Bools: true · false',
  'Any column: null · !null',
].join('\n');

export function filterPlaceholder(type: string): string {
  if (type === 'utf8') {
    return 'contains… (=exact, !not)';
  }
  if (isNumericType(type)) {
    return '> 100';
  }
  if (type === 'date32' || type === 'timestamp') {
    return '>= 2026-01-01';
  }
  if (type === 'bool') {
    return 'true / false';
  }
  return 'null / !null';
}

/** Where one column's filter input sits, from the grid's metrics */
export type FilterCell = {
  column: ViewportColumn;
  left: number;
  width: number;
};

/**
 * The filter inputs' positions for the columns on screen, in display order. Columns being
 * dragged sit where the drag draws them, under the pointer, not at the slot they hold.
 */
export function filterCells(
  metrics: Pick<
    GridMetrics,
    'columnsForRender' | 'allColumnXs' | 'allColumnWidths' | 'modelColumns' | 'gridX'
  >,
  columns: readonly ViewportColumn[],
  dragging: DraggingColumn | null = null
): FilterCell[] {
  const { columnsForRender, allColumnXs, allColumnWidths, modelColumns, gridX } =
    metrics;
  const dragStart = dragging == null ? undefined : allColumnXs.get(dragging.range[0]);
  const cells: FilterCell[] = [];
  for (const visible of columnsForRender) {
    const x = allColumnXs.get(visible);
    const width = allColumnWidths.get(visible);
    const modelColumn = modelColumns.get(visible);
    if (x === undefined || width === undefined || modelColumn === undefined) {
      continue;
    }
    const isDragged =
      dragging != null &&
      dragStart !== undefined &&
      visible >= dragging.range[0] &&
      visible <= dragging.range[1];
    const left = isDragged ? dragging.left + (x - dragStart) : x;
    cells.push({ column: columns[modelColumn], left: gridX + left, width });
  }
  return cells;
}

type FilterBarProps = {
  cells: readonly FilterCell[];
  top: number;
  filters: Readonly<Record<string, string>>;
  errors: Readonly<Record<string, string>>;
  onChange: (column: string, text: string) => void;
};

/**
 * One text input per column under the headers, like Deephaven's quick filter bar. Each fills
 * its column's cell of the band the renderer draws (`drawFilterBar`), short of the divider.
 */
export function FilterBar({
  cells,
  top,
  filters,
  errors,
  onChange,
}: FilterBarProps): React.ReactElement {
  return (
    <>
      {cells.map(({ column, left, width }) => {
        const error = errors[column.name];
        return (
          <input
            key={column.name}
            className="kermodes-filter-input"
            type="text"
            spellCheck={false}
            aria-label={`Filter ${column.name}`}
            aria-invalid={error != null}
            placeholder={filterPlaceholder(column.type)}
            title={error ?? FILTER_SYNTAX}
            value={filters[column.name] ?? ''}
            onChange={event => onChange(column.name, event.target.value)}
            style={{
              position: 'absolute',
              top,
              left,
              width: Math.max(0, width - 1),
              height: FILTER_BAR_HEIGHT - 1,
            }}
          />
        );
      })}
    </>
  );
}
