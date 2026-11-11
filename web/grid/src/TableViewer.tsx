import React, {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from 'react';
import { tableFromIPC, type Table } from 'apache-arrow';
import Grid from './Grid';
import type GridMetrics from './GridMetrics';
import { type VisibleIndex } from './GridMetrics';
import GridMouseHandler, { type GridMouseEvent } from './GridMouseHandler';
import { type GridTheme } from './GridTheme';
import GridUtils, { type GridPoint } from './GridUtils';
import { type EventHandlerResult } from './EventHandlerResult';
import { selectionToRanges } from './RangedSelection';
import ViewportGridModel from './ViewportGridModel';
import {
  TableViewerMetricCalculator,
  TableViewerRenderer,
} from './TableViewerGrid';
import {
  FILTER_BAR_HEIGHT,
  FILTER_SYNTAX,
  FilterBar,
  filterCells,
  type FilterCell,
} from './FilterBar';
import { nextSort } from './sorting';
import {
  COPY_ROW_LIMIT,
  copyText,
  copyValue,
  planCopy,
  writeClipboard,
} from './copy';
import {
  toBytes,
  toKeys,
  type ViewerChannel,
  type ViewerState,
} from './ViewerChannel';

const FILTER_DEBOUNCE_MS = 250;
const TOAST_MS = 1600;
const FETCH_TIMEOUT_MS = 30_000;
// Width of the gutter left of the rows; its top cell holds the filter button
const GUTTER_WIDTH = 22;

/** Clicking a header cycles its sort; shift-click adds or cycles it as another key */
class SortMouseHandler extends GridMouseHandler {
  private down: GridPoint | null = null;

  constructor(private readonly onHeaderClick: (column: VisibleIndex, additive: boolean) => void) {
    super(450);
  }

  onDown(gridPoint: GridPoint): EventHandlerResult {
    const isHeader =
      gridPoint.row == null &&
      gridPoint.column != null &&
      gridPoint.columnHeaderDepth === 0;
    this.down = isHeader ? gridPoint : null;
    return false;
  }

  onClick(
    gridPoint: GridPoint,
    _grid: Grid,
    event: GridMouseEvent
  ): EventHandlerResult {
    const { down } = this;
    this.down = null;
    // A drag that moved the column ends in a click too; only a click in place sorts
    if (
      down == null ||
      gridPoint.column == null ||
      gridPoint.column !== down.column ||
      Math.abs(gridPoint.x - down.x) > 4 ||
      Math.abs(gridPoint.y - down.y) > 4
    ) {
      return false;
    }
    this.onHeaderClick(gridPoint.column, event.shiftKey);
    return true;
  }
}

/** Cmd/Ctrl-click on a cell copies that cell, instead of adding it to the selection */
class CopyCellMouseHandler extends GridMouseHandler {
  constructor(private readonly onCopyCell: (column: VisibleIndex, row: number) => void) {
    super(800);
  }

  onDown(
    gridPoint: GridPoint,
    _grid: Grid,
    event: GridMouseEvent
  ): EventHandlerResult {
    const { column, row } = gridPoint;
    if (!GridUtils.isModifierKeyDown(event) || column == null || row == null) {
      return false;
    }
    this.onCopyCell(column, row);
    return true;
  }
}

/**
 * Sends the visible row range when it changes, at most once per animation
 * frame, however often the grid redraws in between.
 */
function useViewportSender(channel: ViewerChannel) {
  return useMemo(() => {
    let sent: { top: number; bottom: number } | null = null;
    let pending: { top: number; bottom: number } | null = null;
    let frame: number | null = null;

    const flush = (): void => {
      frame = null;
      if (
        pending != null &&
        (sent == null ||
          pending.top !== sent.top ||
          pending.bottom !== sent.bottom)
      ) {
        sent = pending;
        channel.send({ type: 'viewport', ...sent });
      }
    };

    return {
      onViewChanged({ top, bottom }: GridMetrics): void {
        pending = { top, bottom };
        if (frame == null) {
          frame = requestAnimationFrame(flush);
        }
      },
      /** Forget what was sent, so the next view change is sent even if unchanged */
      reset(): void {
        sent = null;
      },
      dispose(): void {
        if (frame != null) {
          cancelAnimationFrame(frame);
        }
      },
    };
  }, [channel]);
}

/** Answers to "fetch" messages, by id */
function useFetcher(channel: ViewerChannel) {
  return useMemo(() => {
    let nextId = 1;
    const pending = new Map<
      number,
      { resolve: (table: Table) => void; reject: (error: Error) => void }
    >();
    return {
      fetch(start: number, count: number): Promise<Table> {
        const id = nextId;
        nextId += 1;
        const { promise, resolve, reject } = Promise.withResolvers<Table>();
        const timer = window.setTimeout(() => {
          pending.delete(id);
          reject(new Error('no answer from the engine'));
        }, FETCH_TIMEOUT_MS);
        pending.set(id, {
          resolve: table => {
            clearTimeout(timer);
            resolve(table);
          },
          reject,
        });
        channel.send({ type: 'fetch', id, start, count });
        return promise;
      },
      receive(id: number, table: Table): void {
        pending.get(id)?.resolve(table);
        pending.delete(id);
      },
    };
  }, [channel]);
}

function FilterIcon({ color }: { color: string }): React.ReactElement {
  return (
    <svg width="11" height="11" viewBox="0 0 16 16" aria-hidden="true">
      <path d="M1 2h14l-5.5 6.5V14l-3-1.5V8.5z" fill={color} />
    </svg>
  );
}

export type TableViewerProps = {
  channel: ViewerChannel;
  theme: GridTheme;
  // When set, the grid is as tall as its rows, up to this height; otherwise it fills its parent
  maxGridHeight?: number;
};

/**
 * The table viewer (architecture.md §15.3): the canvas grid over a viewport of
 * an engine table, with header sort, quick filters, copy, change highlighting,
 * and a status line. Talks to the engine only through `channel`.
 */
export function TableViewer({
  channel,
  theme: baseTheme,
  maxGridHeight,
}: TableViewerProps): React.ReactElement {
  const theme = useMemo(
    () => ({ ...baseTheme, rowHeaderWidth: GUTTER_WIDTH }),
    [baseTheme]
  );
  const [state, setState] = useState<ViewerState>(() => channel.state());
  const model = useMemo(() => {
    const initial = channel.state();
    const viewModel = new ViewportGridModel(initial.columns, initial.row_count);
    viewModel.setSort(initial.sort);
    return viewModel;
  }, [channel]);
  const gridRef = useRef<Grid>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const [rowCount, setRowCount] = useState(model.rowCount);
  const viewport = useViewportSender(channel);
  const fetcher = useFetcher(channel);

  const [isFilterBarShown, setIsFilterBarShown] = useState(
    () => Object.keys(channel.state().filters).length > 0
  );
  const [draft, setDraft] = useState<Record<string, string>>(
    () => channel.state().filters
  );
  const draftRef = useRef(draft);
  const filterTimer = useRef<number | null>(null);
  const [cells, setCells] = useState<readonly FilterCell[]>([]);
  const cellsKey = useRef('');

  const [toast, setToast] = useState<{ text: string; id: number } | null>(null);
  const toastTimer = useRef<number | undefined>(undefined);
  const showToast = useCallback((text: string) => {
    clearTimeout(toastTimer.current);
    setToast({ text, id: performance.now() });
    toastTimer.current = window.setTimeout(() => setToast(null), TOAST_MS);
  }, []);
  const metricCalculator = useMemo(() => new TableViewerMetricCalculator(), []);
  const renderer = useMemo(() => new TableViewerRenderer(), []);

  // Synced state from the engine side
  useEffect(() => {
    let columnsKey = JSON.stringify(channel.state().columns);
    return channel.onState(() => {
      const next = channel.state();
      setState(next);
      const nextColumnsKey = JSON.stringify(next.columns);
      if (nextColumnsKey !== columnsKey) {
        columnsKey = nextColumnsKey;
        model.setColumns(next.columns);
        viewport.reset();
      }
      model.setRowCount(next.row_count);
      model.setSort(next.sort);
      // Typing wins over the engine's echo of an older entry
      if (filterTimer.current == null) {
        draftRef.current = next.filters;
        setDraft(next.filters);
      }
    });
  }, [channel, model, viewport]);

  // Rows from the engine side
  useEffect(
    () =>
      channel.onMessage((header, buffers) => {
        const table = tableFromIPC(toBytes(buffers[0]));
        if (header.type === 'rows') {
          model.setRowCount(header.total);
          model.setWindow(header.start, table, toKeys(buffers[1]));
        } else if (header.type === 'fetched') {
          fetcher.receive(header.id, table);
        }
      }),
    [channel, model, fetcher]
  );

  // Redraw on every model change, and keep redrawing while cells flash
  useEffect(() => {
    let frame: number | null = null;
    const animate = (): void => {
      frame = null;
      gridRef.current?.updateCanvas();
      if (model.hasFlashes()) {
        frame = requestAnimationFrame(animate);
      }
    };
    const unsubscribe = model.subscribe(() => {
      gridRef.current?.forceUpdate();
      setRowCount(model.rowCount);
      if (frame == null && model.hasFlashes()) {
        frame = requestAnimationFrame(animate);
      }
    });
    return () => {
      unsubscribe();
      if (frame != null) {
        cancelAnimationFrame(frame);
      }
      viewport.dispose();
    };
  }, [model, viewport]);

  const handleViewChanged = useCallback(
    (metrics: GridMetrics) => {
      viewport.onViewChanged(metrics);
      const next = filterCells(metrics, model.columns);
      const key = next
        .map(cell => `${cell.column.name}:${cell.left}:${cell.width}`)
        .join('|');
      if (key !== cellsKey.current) {
        cellsKey.current = key;
        setCells(next);
      }
    },
    [viewport, model]
  );

  const handleFilterChange = useCallback(
    (column: string, text: string) => {
      draftRef.current = { ...draftRef.current, [column]: text };
      setDraft(draftRef.current);
      clearTimeout(filterTimer.current ?? undefined);
      filterTimer.current = window.setTimeout(() => {
        filterTimer.current = null;
        channel.send({ type: 'filter', filters: draftRef.current });
      }, FILTER_DEBOUNCE_MS);
    },
    [channel]
  );

  const copySelection = useCallback(
    async (includeHeader: boolean) => {
      const grid = gridRef.current;
      if (grid == null) {
        return;
      }
      const plan = planCopy(
        selectionToRanges(grid.state.selection),
        model.columnCount,
        model.rowCount
      );
      if (plan.rows.length === 0 || plan.columns.length === 0) {
        return;
      }
      const { movedColumns } = grid.state;
      const first = plan.rows[0];
      let valueAt = (column: number, row: number): unknown =>
        model.valueAt(column, row);
      if (
        !model.isRowLoaded(first) ||
        !model.isRowLoaded(plan.rows[plan.rows.length - 1])
      ) {
        // Rows of disjoint ranges far apart can span more than the limit
        if (plan.rows[plan.rows.length - 1] - first >= COPY_ROW_LIMIT) {
          plan.rows = plan.rows.filter(row => row - first < COPY_ROW_LIMIT);
          plan.isCapped = true;
        }
        const count = plan.rows[plan.rows.length - 1] - first + 1;
        const table = await fetcher.fetch(first, count);
        const vectors = model.columns.map((_, i) => table.getChildAt(i));
        valueAt = (column, row) => vectors[column]?.get(row - first) ?? null;
      }
      const text = copyText(
        plan,
        model.columns,
        column => GridUtils.getModelIndex(column, movedColumns),
        valueAt,
        includeHeader
      );
      await writeClipboard(text);
      const rows = plan.rows.length.toLocaleString('en-US');
      let message = `Copied ${rows} ${plan.rows.length === 1 ? 'row' : 'rows'}`;
      if (plan.rows.length === 1 && plan.columns.length === 1) {
        message = 'Copied 1 cell';
      } else if (plan.isCapped) {
        message = `Copied the first ${rows} of ${plan.selectedRowCount.toLocaleString('en-US')} rows`;
      }
      showToast(message);
    },
    [model, fetcher, showToast]
  );

  const copyCell = useCallback(
    (column: VisibleIndex, row: number) => {
      const grid = gridRef.current;
      if (grid == null) {
        return;
      }
      const modelColumn = GridUtils.getModelIndex(column, grid.state.movedColumns);
      const value = model.valueAt(modelColumn, row);
      if (value === undefined) {
        return;
      }
      model.flashCell(modelColumn, row);
      writeClipboard(copyValue(model.columns[modelColumn].type, value)).then(
        () => showToast('Copied 1 cell'),
        (error: Error) => showToast(`Copy failed: ${error.message}`)
      );
    },
    [model, showToast]
  );

  const mouseHandlers = useMemo(
    () => [
      new SortMouseHandler((column, additive) => {
        const grid = gridRef.current;
        if (grid == null) {
          return;
        }
        const modelColumn = GridUtils.getModelIndex(column, grid.state.movedColumns);
        const by = nextSort(model.sortKeys, model.columns[modelColumn].name, additive);
        model.setSort(by);
        channel.send({ type: 'sort', by });
      }),
      new CopyCellMouseHandler(copyCell),
    ],
    [model, channel, copyCell]
  );

  const handleKeyDown = useCallback(
    (event: React.KeyboardEvent) => {
      const isModifier = event.metaKey || event.ctrlKey;
      const key = event.key.toLowerCase();
      if (isModifier && key === 'f') {
        setIsFilterBarShown(shown => !shown);
        event.preventDefault();
      } else if (
        isModifier &&
        key === 'c' &&
        event.target instanceof HTMLCanvasElement
      ) {
        event.preventDefault();
        copySelection(event.shiftKey).catch((error: Error) =>
          showToast(`Copy failed: ${error.message}`)
        );
      }
      // Keep the notebook's command-mode shortcuts (c, v, a, f, arrows…) from acting on
      // keys meant for the grid; let modified Enter through, which runs the cell
      const runsCell =
        event.key === 'Enter' &&
        (event.shiftKey || event.ctrlKey || event.metaKey || event.altKey);
      if (!runsCell) {
        event.stopPropagation();
      }
    },
    [copySelection, showToast]
  );

  const filterBarHeight = isFilterBarShown ? FILTER_BAR_HEIGHT : 0;
  const hasFilters = Object.values(state.filters).some(text => text.trim() !== '');
  const gridHeight =
    maxGridHeight == null
      ? undefined
      : Math.min(
          maxGridHeight,
          theme.columnHeaderHeight +
            filterBarHeight +
            Math.max(rowCount, 1) * theme.rowHeight +
            theme.scrollBarSize +
            2
        );
  const chromeFont = theme.font.replace(/^\S+/, '11px');

  return (
    // eslint-disable-next-line jsx-a11y/no-static-element-interactions
    <div
      onKeyDown={handleKeyDown}
      style={{
        display: 'flex',
        flexDirection: 'column',
        width: '100%',
        height: gridHeight == null ? '100%' : undefined,
        border: `1px solid ${theme.headerSeparatorColor}`,
        borderRadius: 4,
        overflow: 'hidden',
        background: theme.backgroundColor,
      }}
    >
      <div
        ref={containerRef}
        style={{
          position: 'relative',
          display: 'flex',
          flex: gridHeight == null ? '1 1 0' : undefined,
          height: gridHeight,
          minHeight: 0,
          overflow: 'hidden',
        }}
      >
        <Grid
          ref={gridRef}
          model={model}
          theme={theme}
          filterBarHeight={filterBarHeight}
          metricCalculator={metricCalculator}
          renderer={renderer}
          mouseHandlers={mouseHandlers}
          onViewChanged={handleViewChanged}
        />
        <button
          type="button"
          title={FILTER_SYNTAX}
          aria-pressed={isFilterBarShown}
          onClick={() => setIsFilterBarShown(shown => !shown)}
          style={{
            position: 'absolute',
            top: 0,
            left: 0,
            width: GUTTER_WIDTH,
            height: theme.columnHeaderHeight,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            padding: 0,
            border: 'none',
            borderRight: `1px solid ${theme.headerSeparatorColor}`,
            borderBottom: `1px solid ${theme.headerSeparatorColor}`,
            background: isFilterBarShown
              ? theme.backgroundColor
              : theme.headerBackgroundColor,
            cursor: 'pointer',
          }}
        >
          <FilterIcon
            color={
              isFilterBarShown || hasFilters
                ? theme.selectionOutlineColor
                : theme.headerColor
            }
          />
        </button>
        {isFilterBarShown && (
          <FilterBar
            cells={cells}
            top={theme.columnHeaderHeight}
            theme={theme}
            filters={draft}
            errors={state.filter_errors}
            onChange={handleFilterChange}
          />
        )}
        {toast != null && (
          <div
            key={toast.id}
            role="status"
            style={{
              position: 'absolute',
              right: 12 + theme.scrollBarSize,
              bottom: 10 + theme.scrollBarSize,
              padding: '4px 10px',
              borderRadius: 4,
              font: chromeFont,
              color: theme.textColor,
              background: theme.headerBackgroundColor,
              border: `1px solid ${theme.selectionOutlineColor}`,
              boxShadow: `0 2px 8px ${theme.shadowColor}`,
              pointerEvents: 'none',
            }}
          >
            {toast.text}
          </div>
        )}
      </div>
      <div
        style={{
          display: 'flex',
          gap: 12,
          padding: '3px 8px',
          font: chromeFont,
          color: theme.headerColor,
          background: theme.headerBackgroundColor,
          borderTop: `1px solid ${theme.headerSeparatorColor}`,
          fontVariantNumeric: 'tabular-nums',
        }}
      >
        <span>
          {rowCount.toLocaleString('en-US')} {rowCount === 1 ? 'row' : 'rows'}
        </span>
        <span style={{ marginLeft: 'auto' }}>{state.status}</span>
      </div>
    </div>
  );
}

export default TableViewer;
