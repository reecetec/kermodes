import { useRef } from 'react';
import { ArrowDown, ArrowUp } from 'lucide-react';
import { cn } from '@/lib/utils';
import type { ListSort } from './list';

export type ListColumn = { label: string; numeric?: boolean };

/** The columns' widths, for a `table-fixed` list sized to their sum */
export function ListColGroup({ widths }: { widths: readonly number[] }) {
  return (
    <colgroup>
      {widths.map((width, i) => (
        // Columns never reorder
        // eslint-disable-next-line react/no-array-index-key
        <col key={i} style={{ width }} />
      ))}
    </colgroup>
  );
}

/**
 * A list's header row: each column can be dragged wider or narrower at its right edge (a
 * double-click restores it), and, given `onSort`, clicked to sort by it.
 */
export function ListHeader({
  columns,
  widths,
  onResize,
  sort = null,
  onSort,
  cellClassName,
}: {
  columns: readonly ListColumn[];
  widths: readonly number[];
  onResize: (column: number, width: number | null) => void;
  sort?: ListSort;
  onSort?: (column: number) => void;
  cellClassName?: string;
}) {
  const drag = useRef<{ column: number; startX: number; startWidth: number } | null>(null);
  return (
    <thead className="sticky top-0 z-10 bg-background">
      <tr className="text-left text-xs text-muted-foreground shadow-[inset_0_-1px_0_var(--border)]">
        {columns.map((c, i) => {
          const sorted = sort?.column === i ? sort : null;
          return (
            <th
              key={c.label}
              aria-sort={
                sorted == null ? undefined : sorted.descending ? 'descending' : 'ascending'
              }
              className={cn('relative font-medium select-none', cellClassName, c.numeric && 'text-right')}
            >
              {onSort == null ? (
                c.label
              ) : (
                <button
                  type="button"
                  onClick={() => onSort(i)}
                  className={cn(
                    'inline-flex items-center gap-1 hover:text-foreground',
                    sorted != null && 'text-foreground',
                    c.numeric && 'flex-row-reverse'
                  )}
                >
                  {c.label}
                  {sorted != null &&
                    (sorted.descending ? (
                      <ArrowDown className="size-3" />
                    ) : (
                      <ArrowUp className="size-3" />
                    ))}
                </button>
              )}
              <span
                role="separator"
                aria-orientation="vertical"
                aria-label={`Resize ${c.label}`}
                title="Drag to resize, double-click to reset"
                onPointerDown={event => {
                  event.currentTarget.setPointerCapture(event.pointerId);
                  drag.current = { column: i, startX: event.clientX, startWidth: widths[i] };
                }}
                onPointerMove={event => {
                  const d = drag.current;
                  if (d != null) {
                    onResize(d.column, d.startWidth + event.clientX - d.startX);
                  }
                }}
                onPointerUp={() => {
                  drag.current = null;
                }}
                onDoubleClick={() => onResize(i, null)}
                className="absolute top-1 right-0 bottom-1 w-1.5 cursor-col-resize border-r border-transparent hover:border-ring"
              />
            </th>
          );
        })}
      </tr>
    </thead>
  );
}
