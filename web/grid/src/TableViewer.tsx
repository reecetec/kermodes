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
import { toBytes, toKeys, type SortKey, type ViewerChannel, type ViewerState } from '@kermodes/client';
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
import { nextSort, shownView } from './sorting';
import ContextMenu from './ContextMenu';
import { contextMenu, type MenuAction, type MenuItem, type MenuTarget } from './menuItems';
import {
  COPY_ROW_LIMIT,
  copyText,
  copyValue,
  planCopy,
  writeClipboard,
} from './copy';
import { viewerCover, type Cover } from './cover';
import { shownValue, type ShownValue } from './cellValue';
import ValuePopover, { type CellRect } from './ValuePopover';

const FILTER_DEBOUNCE_MS = 250;
const TOAST_MS = 1600;
const FETCH_TIMEOUT_MS = 30_000;

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

/**
 * Cmd/Ctrl-click on a cell copies that cell, instead of adding it to the selection. While the
 * key is down over a cell, the cursor says so.
 */
class CopyCellMouseHandler extends GridMouseHandler {
  // Whether the pointer was last over a cell, for the cursor when only the key changes
  isOverCell = false;

  constructor(private readonly onCopyCell: (column: VisibleIndex, row: number) => void) {
    super(800);
  }

  cursorFor(isModifierDown: boolean): string | null {
    return this.isOverCell && isModifierDown ? 'copy' : null;
  }

  onMove(
    gridPoint: GridPoint,
    _grid: Grid,
    event: GridMouseEvent
  ): EventHandlerResult {
    this.isOverCell = gridPoint.column != null && gridPoint.row != null;
    this.cursor = this.cursorFor(GridUtils.isModifierKeyDown(event));
    return this.cursor == null ? false : { stopPropagation: false, preventDefault: false };
  }

  onLeave(): EventHandlerResult {
    this.isOverCell = false;
    return false;
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

/** Tells the viewer which cell the pointer is over, for the expand button; never handles events */
class HoverCellMouseHandler extends GridMouseHandler {
  constructor(private readonly onHover: (cell: { column: VisibleIndex; row: number } | null) => void) {
    super(850);
  }

  onMove(gridPoint: GridPoint): EventHandlerResult {
    const { column, row } = gridPoint;
    this.onHover(column == null || row == null ? null : { column, row });
    return false;
  }

  onLeave(): EventHandlerResult {
    this.onHover(null);
    return false;
  }
}

function ExpandIcon(): React.ReactElement {
  return (
    <svg width="10" height="10" viewBox="0 0 16 16" aria-hidden="true">
      <path
        d="M9 2h5v5M14 2 9 7M7 14H2V9M2 14l5-5"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.75"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}

/**
 * Right-clicking a header or cell opens the viewer's menu. It runs after the selection
 * handler, which moves the selection to a clicked cell outside it.
 */
class ContextMenuMouseHandler extends GridMouseHandler {
  constructor(
    private readonly onOpen: (gridPoint: GridPoint, event: GridMouseEvent) => void
  ) {
    super(950);
  }

  onContextMenu(
    gridPoint: GridPoint,
    _grid: Grid,
    event: GridMouseEvent
  ): EventHandlerResult {
    const isHeader = gridPoint.row == null && gridPoint.columnHeaderDepth === 0;
    if (gridPoint.column == null || (gridPoint.row == null && !isHeader)) {
      return false;
    }
    this.onOpen(gridPoint, event);
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
      onViewChanged({ top, bottom }: { top: number; bottom: number }): void {
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

function Spinner(): React.ReactElement {
  return (
    <svg className="kermodes-spinner" width="14" height="14" viewBox="0 0 16 16" aria-hidden="true">
      <circle cx="8" cy="8" r="6.5" fill="none" stroke="currentColor" strokeOpacity="0.25" strokeWidth="1.75" />
      <path d="M8 1.5a6.5 6.5 0 0 1 6.5 6.5" fill="none" stroke="currentColor" strokeWidth="1.75" strokeLinecap="round" />
    </svg>
  );
}

const SKELETON_ROWS = 16;

/** What covers the rows when they are not current (`viewerCover`), under the headers */
function ViewerCover({
  cover,
  top,
  rowHeight,
  cells,
}: {
  cover: NonNullable<Cover>;
  top: number;
  rowHeight: number;
  cells: readonly FilterCell[];
}): React.ReactElement {
  if (cover.kind === 'skeleton') {
    return (
      <div className="kermodes-cover" style={{ top }} aria-busy="true" aria-label="Loading rows">
        {Array.from({ length: SKELETON_ROWS }, (_, row) =>
          cells.map((cell, column) => (
            <div
              // Fixed placeholders that never reorder
              // eslint-disable-next-line react/no-array-index-key
              key={`${row}:${column}`}
              className="kermodes-skeleton-bar"
              style={{
                top: row * rowHeight + (rowHeight - 8) / 2,
                left: cell.left + 8,
                // Varied, but the same on every render, so nothing moves
                width: Math.max(12, (cell.width - 16) * (0.45 + ((row * 7 + column * 3) % 5) * 0.1)),
              }}
            />
          ))
        )}
      </div>
    );
  }
  if (cover.kind === 'failed') {
    return (
      <div className="kermodes-cover kermodes-cover-failed" style={{ top }} role="alert">
        <div>
          <p className="kermodes-cover-title">This table cannot be shown</p>
          <p className="kermodes-cover-detail">{cover.message}</p>
        </div>
      </div>
    );
  }
  return (
    <div className="kermodes-cover kermodes-cover-dim" style={{ top }} aria-busy="true">
      <span className="kermodes-busy" role="status">
        <Spinner />
        {cover.label}
      </span>
    </div>
  );
}

export type TableViewerProps = {
  channel: ViewerChannel;
  theme: GridTheme;
  // When set, the grid is as tall as its rows, up to this height; otherwise it fills its parent
  maxGridHeight?: number;
  // Starts at the table's end and stays there as rows arrive, until scrolled up (and again
  // once scrolled back down): an append table's newest rows, by position, with no sort
  follow?: boolean;
  // The table's last rows first: an append table's newest on top, read by position, with no
  // sort for the server to hold; a sort from the header shows its own order instead
  reverse?: boolean;
};

/**
 * The table viewer (architecture.md §15.3): the canvas grid over a viewport of
 * an engine table, with header sort, quick filters, copy, change highlighting,
 * and a status line. Talks to the engine only through `channel`.
 */
export function TableViewer({
  channel,
  theme,
  maxGridHeight,
  follow = false,
  reverse = false,
}: TableViewerProps): React.ReactElement {
  const [state, setState] = useState<ViewerState>(() => channel.state());
  const model = useMemo(() => {
    const initial = channel.state();
    const viewModel = new ViewportGridModel(initial.columns, initial.row_count);
    viewModel.setSort(initial.sort);
    return viewModel;
  }, [channel]);
  useEffect(() => model.setReversed(reverse), [model, reverse]);
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
  // The number of the last sort or filter sent; until the engine reports it applied
  // (`shownView`), the rows shown predate it, so the table says it is updating and dims them.
  const sent = useRef(0);
  // The sort last asked for, shown in the header while it is pending
  const askedSort = useRef<readonly SortKey[] | null>(null);
  const [isUpdating, setIsUpdating] = useState(false);
  // Whether any rows have arrived: until then the body shows skeleton rows
  const [hasRows, setHasRows] = useState(false);
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
      const shown = shownView(sent.current, next.applied, askedSort.current, next.sort);
      if (!shown.pending) {
        askedSort.current = null;
      }
      setIsUpdating(shown.pending);
      model.setSort(shown.sort);
      // Typing, and entries sent and not yet applied, win over the engine's echo of older ones
      if (filterTimer.current == null && !shown.pending) {
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
          setHasRows(true);
        } else if (header.type === 'fetched') {
          fetcher.receive(header.id, table);
        }
      }),
    [channel, model, fetcher]
  );

  // Redraw on every model change, and again when a flash's fade next steps: not every frame,
  // which on a table always changing somewhere kept the canvas repainting at 60 frames a second
  useEffect(() => {
    let frame: number | null = null;
    let timer: number | undefined;
    const schedule = (): void => {
      clearTimeout(timer);
      const wait = model.msToNextFlashStep();
      if (wait != null) {
        timer = window.setTimeout(() => {
          frame ??= requestAnimationFrame(redraw);
        }, wait);
      }
    };
    const redraw = (): void => {
      frame = null;
      gridRef.current?.updateCanvas();
      schedule();
    };
    const unsubscribe = model.subscribe(() => {
      gridRef.current?.forceUpdate();
      setRowCount(model.rowCount);
      schedule();
    });
    return () => {
      unsubscribe();
      clearTimeout(timer);
      if (frame != null) {
        cancelAnimationFrame(frame);
      }
      viewport.dispose();
    };
  }, [model, viewport]);

  // Where the view was scrolled to when it last drew: it is called on every draw, not only on
  // scrolls, so a scroll is when this changes
  const scrolledTo = useRef('');
  const handleViewChanged = useCallback(
    (metrics: GridMetrics) => {
      // The server reads by position: the rows on screen, reversed or not, are positions
      viewport.onViewChanged(model.positionsOf(metrics.top, metrics.bottom));
      const scroll = `${metrics.top}:${metrics.topOffset}:${metrics.left}:${metrics.leftOffset}`;
      if (scroll !== scrolledTo.current) {
        scrolledTo.current = scroll;
        // The cells under a still pointer moved: the expand button goes until it moves again
        setHovered(null);
      }
      const next = filterCells(
        metrics,
        model.columns,
        gridRef.current?.state.draggingColumn ?? null
      );
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

  /** Sends every column's filter entry now, as the entries the viewer shows */
  const sendFilters = useCallback(
    (filters: Record<string, string>) => {
      clearTimeout(filterTimer.current ?? undefined);
      filterTimer.current = null;
      draftRef.current = filters;
      setDraft(filters);
      sent.current += 1;
      setIsUpdating(true);
      // A selection is by position: after a filter it would point at other rows, or none
      gridRef.current?.clearSelectedRanges();
      channel.send({ type: 'filter', filters, request: sent.current });
    },
    [channel]
  );

  const handleFilterChange = useCallback(
    (column: string, text: string) => {
      draftRef.current = { ...draftRef.current, [column]: text };
      setDraft(draftRef.current);
      clearTimeout(filterTimer.current ?? undefined);
      filterTimer.current = window.setTimeout(
        () => sendFilters(draftRef.current),
        FILTER_DEBOUNCE_MS
      );
    },
    [sendFilters]
  );

  const sortBy = useCallback(
    (by: SortKey[]) => {
      sent.current += 1;
      askedSort.current = by;
      setIsUpdating(true);
      // A selection is by position: after a sort it would point at other rows
      gridRef.current?.clearSelectedRanges();
      model.setSort(by);
      channel.send({ type: 'sort', by, request: sent.current });
    },
    [model, channel]
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
        // The selected rows' positions: the same span, from its lowest
        const { top } = model.positionsOf(first, plan.rows[plan.rows.length - 1]);
        const table = await fetcher.fetch(top, count);
        const vectors = model.columns.map((_, i) => table.getChildAt(i));
        valueAt = (column, row) => vectors[column]?.get(model.positionOf(row) - top) ?? null;
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

  const copyCellHandler = useMemo(() => new CopyCellMouseHandler(copyCell), [copyCell]);

  // Whether Cmd/Ctrl is held: the cell under the pointer is then what a click copies
  const [isCopyKeyDown, setIsCopyKeyDown] = useState(false);
  // The copy cursor and target follow the modifier key while the pointer stays still. Capture,
  // since the viewer's own key handler stops propagation (for notebook shortcuts) before a
  // bubbling window listener would hear the key while the grid has focus.
  useEffect(() => {
    const update = (event: KeyboardEvent): void => {
      if (event.key !== 'Meta' && event.key !== 'Control') {
        return;
      }
      const isDown = GridUtils.isModifierKeyDown(event);
      setIsCopyKeyDown(isDown);
      if (copyCellHandler.isOverCell) {
        gridRef.current?.setCursor(copyCellHandler.cursorFor(isDown));
      }
    };
    // A key released in another window never reaches this one
    const reset = (): void => setIsCopyKeyDown(false);
    window.addEventListener('keydown', update, true);
    window.addEventListener('keyup', update, true);
    window.addEventListener('blur', reset);
    return () => {
      window.removeEventListener('keydown', update, true);
      window.removeEventListener('keyup', update, true);
      window.removeEventListener('blur', reset);
    };
  }, [copyCellHandler]);

  // The cell under the pointer, and the cell whose whole value is open
  const [hovered, setHovered] = useState<{ column: VisibleIndex; row: number } | null>(null);
  const [opened, setOpened] = useState<{
    rect: CellRect;
    column: string;
    type: string;
    value: ShownValue;
  } | null>(null);
  // The pointer moving from a cell onto its expand button leaves the canvas: keep the button
  const isOverExpand = useRef(false);
  const hoverHandler = useMemo(
    () =>
      new HoverCellMouseHandler(cell => {
        if (cell != null) {
          setHovered(current =>
            current?.column === cell.column && current.row === cell.row ? current : cell
          );
        } else {
          window.setTimeout(() => !isOverExpand.current && setHovered(null), 80);
        }
      }),
    []
  );
  const measure = useMemo(() => document.createElement('canvas').getContext('2d'), []);

  /** Where a visible cell is drawn, relative to the viewer's body */
  const cellRect = useCallback((column: VisibleIndex, row: number): CellRect | null => {
    const metrics = gridRef.current?.metrics;
    const x = metrics?.visibleColumnXs.get(column);
    const y = metrics?.visibleRowYs.get(row);
    const width = metrics?.visibleColumnWidths.get(column);
    const height = metrics?.visibleRowHeights.get(row);
    if (metrics == null || x == null || y == null || width == null || height == null) {
      return null;
    }
    return { x: metrics.gridX + x, y: metrics.gridY + y, width, height };
  }, []);

  /** Whether a cell is worth expanding: its text is cut off, or it holds JSON */
  const isExpandable = useCallback(
    (column: VisibleIndex, row: number, width: number): boolean => {
      const grid = gridRef.current;
      if (grid == null || measure == null) {
        return false;
      }
      const modelColumn = GridUtils.getModelIndex(column, grid.state.movedColumns);
      const value = model.valueAt(modelColumn, row);
      if (value == null) {
        return false;
      }
      if (shownValue(model.columns[modelColumn].type, value).isJson) {
        return true;
      }
      measure.font = theme.font;
      const text = model.textForCell(modelColumn, row);
      return measure.measureText(text).width > width - 2 * theme.cellHorizontalPadding;
    },
    [model, measure, theme]
  );

  const showValue = useCallback(
    (column: VisibleIndex, row: number) => {
      const grid = gridRef.current;
      const rect = cellRect(column, row);
      if (grid == null || rect == null) {
        return;
      }
      const modelColumn = GridUtils.getModelIndex(column, grid.state.movedColumns);
      const value = model.valueAt(modelColumn, row);
      if (value === undefined) {
        return;
      }
      const { name, type } = model.columns[modelColumn];
      setOpened({ rect, column: name, type, value: shownValue(type, value) });
    },
    [model, cellRect]
  );
  const closeValue = useCallback(() => setOpened(null), []);
  const expandAt = hovered == null ? null : cellRect(hovered.column, hovered.row);
  const expand =
    hovered != null && expandAt != null && isExpandable(hovered.column, hovered.row, expandAt.width)
      ? { ...hovered, rect: expandAt }
      : null;

  const [menu, setMenu] = useState<{
    x: number;
    y: number;
    sections: MenuItem[][];
    column: VisibleIndex;
    row: number | null;
  } | null>(null);
  const closeMenu = useCallback(() => setMenu(null), []);

  const openMenu = useCallback(
    (gridPoint: GridPoint, event: GridMouseEvent) => {
      const grid = gridRef.current;
      const container = containerRef.current;
      const { column, row } = gridPoint;
      if (grid == null || container == null || column == null) {
        return;
      }
      const modelColumn = GridUtils.getModelIndex(column, grid.state.movedColumns);
      const { name, type } = model.columns[modelColumn];
      const target: MenuTarget =
        row == null
          ? { kind: 'header', column: name, type }
          : {
              kind: 'cell',
              column: name,
              type,
              value: model.valueAt(modelColumn, row),
              text: model.textForCell(modelColumn, row),
            };
      // A cell outside the selection becomes the selection
      const { selection } = grid.state;
      const plan = planCopy(selectionToRanges(selection), model.columnCount, model.rowCount);
      const isSelectionWide =
        row != null &&
        selection.isCellSelected(column, row) &&
        plan.selectedRowCount * plan.columns.length > 1;
      const bounds = container.getBoundingClientRect();
      setMenu({
        x: event.clientX - bounds.left,
        y: event.clientY - bounds.top,
        sections: contextMenu(target, {
          sort: model.sortKeys,
          filters: draftRef.current,
          isSelectionWide,
        }),
        column,
        row,
      });
    },
    [model]
  );

  const runMenuAction = useCallback(
    (action: MenuAction, column: VisibleIndex, row: number | null) => {
      switch (action.type) {
        case 'sort':
          sortBy(action.by);
          break;
        case 'filter':
          setIsFilterBarShown(true);
          sendFilters({ ...draftRef.current, [action.column]: action.text });
          break;
        case 'clear-filters':
          sendFilters({});
          break;
        case 'edit-filter': {
          setIsFilterBarShown(true);
          const label = CSS.escape(`Filter ${action.column}`);
          requestAnimationFrame(() =>
            containerRef.current
              ?.querySelector<HTMLInputElement>(`input[aria-label="${label}"]`)
              ?.focus()
          );
          break;
        }
        case 'copy-cell':
          if (row != null) {
            copyCell(column, row);
          }
          break;
        case 'show-value':
          if (row != null) {
            showValue(column, row);
          }
          break;
        case 'copy-selection':
          copySelection(action.header).catch((error: Error) =>
            showToast(`Copy failed: ${error.message}`)
          );
          break;
        case 'copy-text':
          writeClipboard(action.text).then(
            () => showToast(`Copied ${action.text}`),
            (error: Error) => showToast(`Copy failed: ${error.message}`)
          );
          break;
      }
    },
    [sortBy, sendFilters, copyCell, showValue, copySelection, showToast]
  );

  const mouseHandlers = useMemo(
    () => [
      new SortMouseHandler((column, additive) => {
        const grid = gridRef.current;
        if (grid == null) {
          return;
        }
        const modelColumn = GridUtils.getModelIndex(column, grid.state.movedColumns);
        sortBy(nextSort(model.sortKeys, model.columns[modelColumn].name, additive));
      }),
      copyCellHandler,
      hoverHandler,
      new ContextMenuMouseHandler(openMenu),
    ],
    [model, sortBy, copyCellHandler, hoverHandler, openMenu]
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
  const cover = viewerCover(state, hasRows, isUpdating);
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

  return (
    // eslint-disable-next-line jsx-a11y/no-static-element-interactions
    <div
      className="kermodes-viewer"
      onKeyDown={handleKeyDown}
      style={{ height: gridHeight == null ? '100%' : undefined }}
    >
      <div
        ref={containerRef}
        className="kermodes-viewer-body"
        style={{ flex: gridHeight == null ? '1 1 0' : undefined, height: gridHeight }}
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
          isStickyBottom={follow}
          isStuckToBottom={follow}
        />
        {isFilterBarShown && (
          <FilterBar
            cells={cells}
            top={theme.columnHeaderHeight}
            filters={draft}
            errors={state.filter_errors}
            onChange={handleFilterChange}
          />
        )}
        {isCopyKeyDown && expandAt != null && (
          <div
            className="kermodes-copy-target"
            style={{
              left: expandAt.x,
              top: expandAt.y,
              width: expandAt.width,
              height: expandAt.height,
            }}
          />
        )}
        {expand != null && opened == null && !isCopyKeyDown && (
          <button
            type="button"
            className="kermodes-cell-expand"
            aria-label="Show value"
            title="Show value"
            style={{
              left: expand.rect.x + expand.rect.width - 20,
              top: expand.rect.y + (expand.rect.height - 16) / 2,
            }}
            onMouseEnter={() => {
              isOverExpand.current = true;
            }}
            onMouseLeave={() => {
              isOverExpand.current = false;
              setHovered(null);
            }}
            onClick={() => showValue(expand.column, expand.row)}
          >
            <ExpandIcon />
          </button>
        )}
        {opened != null && (
          <ValuePopover
            cell={opened.rect}
            column={opened.column}
            type={opened.type}
            value={opened.value}
            onCopy={text =>
              writeClipboard(text).then(
                () => showToast('Copied the value'),
                (error: Error) => showToast(`Copy failed: ${error.message}`)
              )
            }
            onClose={closeValue}
          />
        )}
        {toast != null && (
          <div key={toast.id} role="status" className="kermodes-toast">
            {toast.text}
          </div>
        )}
        {menu != null && (
          <ContextMenu
            x={menu.x}
            y={menu.y}
            sections={menu.sections}
            onPick={action => runMenuAction(action, menu.column, menu.row)}
            onClose={closeMenu}
          />
        )}
        {cover != null && (
          <ViewerCover
            cover={cover}
            top={theme.columnHeaderHeight + filterBarHeight}
            rowHeight={theme.rowHeight}
            cells={cells}
          />
        )}
      </div>
      <div className="kermodes-viewer-footer">
        <button
          type="button"
          className="kermodes-footer-button"
          title={`Quick filters (⌘F). ${FILTER_SYNTAX}`}
          aria-pressed={isFilterBarShown}
          data-active={hasFilters}
          onClick={() => setIsFilterBarShown(shown => !shown)}
        >
          <FilterIcon color="currentColor" />
          {hasFilters ? 'Filtered' : 'Filter'}
        </button>
        {/* A live table still loading has no count worth reading yet */}
        {hasRows && !state.status.startsWith('loading') && (
          <span>
            {rowCount.toLocaleString('en-US')} {rowCount === 1 ? 'row' : 'rows'}
          </span>
        )}
      </div>
    </div>
  );
}

export default TableViewer;
