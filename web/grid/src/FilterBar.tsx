import React from 'react';
import type GridMetrics from './GridMetrics';
import { type GridTheme } from './GridTheme';
import { isNumericType, type ViewportColumn } from './ViewportGridModel';

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

/** The filter inputs' positions for the columns on screen, in display order */
export function filterCells(
  metrics: GridMetrics,
  columns: readonly ViewportColumn[]
): FilterCell[] {
  const { columnsForRender, allColumnXs, allColumnWidths, modelColumns, gridX } =
    metrics;
  const cells: FilterCell[] = [];
  for (const visible of columnsForRender) {
    const x = allColumnXs.get(visible);
    const width = allColumnWidths.get(visible);
    const modelColumn = modelColumns.get(visible);
    if (x !== undefined && width !== undefined && modelColumn !== undefined) {
      cells.push({ column: columns[modelColumn], left: gridX + x, width });
    }
  }
  return cells;
}

type FilterBarProps = {
  cells: readonly FilterCell[];
  top: number;
  theme: GridTheme;
  filters: Readonly<Record<string, string>>;
  errors: Readonly<Record<string, string>>;
  onChange: (column: string, text: string) => void;
};

/** One text input per column under the headers, like Deephaven's quick filter bar */
export function FilterBar({
  cells,
  top,
  theme,
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
            placeholder={filterPlaceholder(column.type)}
            title={error ?? FILTER_SYNTAX}
            value={filters[column.name] ?? ''}
            onChange={event => onChange(column.name, event.target.value)}
            style={
              {
                position: 'absolute',
                top: top + 3,
                left: left + 2,
                width: Math.max(0, width - 4),
                height: FILTER_BAR_HEIGHT - 6,
                font: theme.font.replace(/^\S+/, '11px'),
                color: theme.textColor,
                background: theme.backgroundColor,
                border: `1px solid ${error == null ? theme.headerSeparatorColor : theme.errorColor}`,
                boxShadow:
                  error == null ? undefined : `0 0 0 1px ${theme.errorColor}`,
                '--kermodes-placeholder': theme.headerSeparatorHoverColor,
                '--kermodes-accent': theme.selectionOutlineColor,
              } as React.CSSProperties
            }
          />
        );
      })}
    </>
  );
}
