// Adapted from @deephaven/grid (Apache-2.0), web-client-ui 0bab596
// Trimmed to base columns: header groups and unmovable columns are not supported here.
import { assertNotNull, clamp } from '../utils';
import type Grid from '../Grid';
import GridUtils, { type GridPoint } from '../GridUtils';
import GridMouseHandler, { type GridMouseEvent } from '../GridMouseHandler';
import { type EventHandlerResult } from '../EventHandlerResult';
import type {
  VisibleIndex,
  GridMetrics,
  MoveOperation,
  Coordinate,
} from '../GridMetrics';
import type { BoundedAxisRange } from '../GridAxisRange';

const SLOPPY_CLICK_DISTANCE = 5;
const SCROLL_INTERVAL = 1000 / 60;
const SCROLL_DELTA = 10;

export interface DraggingColumn {
  range: BoundedAxisRange;
  depth: number;
  left: Coordinate;
  width: number;
}

interface ColumnInfo {
  visibleIndex: VisibleIndex;
  left: number;
  right: number;
  width: number;
}

/**
 * Gets info about a visible column
 * @param visibleIndex The visible index to get info for
 * @param metrics Grid metrics
 * @returns The column info, or null if the column is not visible.
 */
function getColumnInfo(
  visibleIndex: VisibleIndex | null,
  metrics: GridMetrics
): ColumnInfo | null {
  const {
    modelColumns,
    movedColumns,
    allColumnXs,
    columnCount,
    allColumnWidths,
    userColumnWidths,
    calculatedColumnWidths,
  } = metrics;

  if (visibleIndex == null || visibleIndex >= columnCount || visibleIndex < 0) {
    return null;
  }

  const left = allColumnXs.get(visibleIndex);
  if (left == null) {
    return null;
  }

  const modelIndex =
    modelColumns.get(visibleIndex) ??
    GridUtils.getModelIndex(visibleIndex, movedColumns);
  const width =
    allColumnWidths.get(visibleIndex) ??
    userColumnWidths.get(modelIndex) ??
    calculatedColumnWidths.get(modelIndex) ??
    0;

  return { visibleIndex, left, right: left + width, width };
}

class GridColumnMoveMouseHandler extends GridMouseHandler {
  cursor: string | null = null;

  private draggingOffset?: number;

  private initialOffset?: number;

  private initialGridPoint?: GridPoint;

  private scrollingInterval?: number;

  private scrollingDirection?: 'left' | 'right';

  private draggingColumn: DraggingColumn | null = null;

  private setScrollInterval(grid: Grid, direction: 'left' | 'right'): void {
    if (
      this.scrollingInterval != null &&
      direction === this.scrollingDirection
    ) {
      return;
    }

    this.scrollingDirection = direction;
    this.scrollingInterval = window.setInterval(() => {
      const { metrics } = grid;
      if (!metrics) {
        return;
      }

      const {
        left,
        lastLeft,
        leftOffset,
        userColumnWidths,
        calculatedColumnWidths,
        movedColumns,
        allColumnWidths,
      } = metrics;

      let nextLeft = left;
      let nextOffset = leftOffset;
      if (direction === 'left') {
        nextOffset -= SCROLL_DELTA;
        while (nextOffset < 0) {
          nextLeft -= 1;
          const modelIndex = GridUtils.getModelIndex(left - 1, movedColumns);
          const prevColumnWidth =
            userColumnWidths.get(modelIndex) ??
            calculatedColumnWidths.get(modelIndex);
          if (prevColumnWidth === undefined) {
            nextOffset = 0;
          } else {
            nextOffset += prevColumnWidth;
          }
          if (nextLeft < 0) {
            nextOffset = 0;
            nextLeft = 0;
          }
        }
      } else {
        nextOffset += SCROLL_DELTA;
        let leftColumnWidth = allColumnWidths.get(left);
        while (leftColumnWidth !== undefined && nextOffset > leftColumnWidth) {
          nextLeft += 1;
          nextOffset -= leftColumnWidth;
          const modelIndex = GridUtils.getModelIndex(left + 1, movedColumns);
          leftColumnWidth =
            userColumnWidths.get(modelIndex) ??
            calculatedColumnWidths.get(modelIndex);

          if (nextLeft > lastLeft) {
            nextOffset = 0;
            nextLeft = lastLeft;
          }
        }
      }

      const { mouseX, mouseY } = grid.state;
      if (mouseX == null || mouseY == null) {
        return;
      }

      this.moveDraggingColumn(
        mouseX,
        grid,
        direction === 'left' ? -SCROLL_DELTA : SCROLL_DELTA
      );
      grid.setState({ left: nextLeft, leftOffset: nextOffset });

      if (
        (direction === 'left' && nextLeft === 0 && leftOffset === 0) ||
        (direction === 'right' && nextLeft === lastLeft)
      ) {
        this.clearScrollInterval();
      }
    }, SCROLL_INTERVAL);
  }

  private clearScrollInterval(): void {
    this.scrollingDirection = undefined;
    window.clearInterval(this.scrollingInterval);
    this.scrollingInterval = undefined;
  }

  onLeave(): EventHandlerResult {
    this.clearScrollInterval();
    return false;
  }

  onDown(gridPoint: GridPoint, grid: Grid): EventHandlerResult {
    const { x, column, columnHeaderDepth } = gridPoint;
    const { metrics } = grid;
    if (!metrics) throw new Error('Metrics not set');

    const columnInfo = getColumnInfo(column, metrics);
    if (columnInfo == null || columnHeaderDepth !== 0) {
      return false;
    }

    this.initialGridPoint = gridPoint;
    this.draggingColumn = null;
    this.cursor = null;
    this.draggingOffset = x - columnInfo.left - metrics.rowHeaderWidth;
    this.initialOffset = this.draggingOffset;
    return false;
  }

  onDrag(
    gridPoint: GridPoint,
    grid: Grid,
    event: GridMouseEvent
  ): EventHandlerResult {
    if (
      this.draggingOffset === undefined ||
      this.initialGridPoint === undefined ||
      this.initialOffset === undefined
    ) {
      return false;
    }

    const { x: mouseX, y: mouseY } = gridPoint;
    const { metrics } = grid;
    if (!metrics) throw new Error('Metrics not set');

    // before considering it a drag, the mouse must have moved a minimum distance
    // this prevents click actions from triggering a drag state
    if (
      !this.draggingColumn &&
      Math.abs(this.initialGridPoint.x - mouseX) < SLOPPY_CLICK_DISTANCE &&
      Math.abs(this.initialGridPoint.y - mouseY) < SLOPPY_CLICK_DISTANCE
    ) {
      return false;
    }

    if (this.draggingColumn == null) {
      const initialColumnInfo = getColumnInfo(
        this.initialGridPoint.column,
        metrics
      );
      if (!initialColumnInfo) {
        return false;
      }

      this.draggingColumn = {
        range: [initialColumnInfo.visibleIndex, initialColumnInfo.visibleIndex],
        depth: 0,
        left: initialColumnInfo.left,
        width: initialColumnInfo.width,
      };
      grid.setState({ draggingColumn: this.draggingColumn, isDragging: true });
    }

    // At this point, we have determined we are actually dragging a column
    this.cursor = 'move';

    this.moveDraggingColumn(
      gridPoint.x,
      grid,
      event.movementX / metrics.scaleX
    );

    return true;
  }

  /**
   * Moves a dragging column, if possible, the specified distance
   * @param mouseX The point the move was initiated from
   * @param grid The Grid component
   * @param deltaX The distance of the move
   */
  moveDraggingColumn(mouseX: number, grid: Grid, deltaX: number): void {
    if (
      this.draggingOffset === undefined ||
      this.initialGridPoint === undefined ||
      this.initialOffset === undefined ||
      this.draggingColumn == null ||
      deltaX === 0
    ) {
      return;
    }
    const { metrics } = grid;
    assertNotNull(metrics, 'Metrics not set');
    const { gridX } = metrics;

    // Cursor has moved past the column drag bounds, don't move the column until we hit the initial offset point again
    if (this.initialOffset !== this.draggingOffset) {
      // Pre move < Initial < Post move or vice-versa
      // User crossed back past the iniital offset point, so we can start moving again
      if (
        (this.draggingOffset < this.initialOffset &&
          this.initialOffset < this.draggingOffset + deltaX) ||
        (this.draggingOffset > this.initialOffset &&
          this.initialOffset > this.draggingOffset + deltaX)
      ) {
        this.draggingOffset = this.initialOffset;
      } else {
        // Column can't move since we aren't back at the initial offset yet
        this.draggingOffset += deltaX;
      }

      this.draggingColumn = {
        ...this.draggingColumn,
        left: mouseX - this.draggingOffset - gridX,
      };
      grid.setState({ draggingColumn: this.draggingColumn });
      return;
    }

    const { movedColumns } = grid.state;
    const { floatingLeftWidth, width } = metrics;
    const isDraggingLeft = deltaX < 0;

    const draggingColumn = getColumnInfo(this.draggingColumn.range[0], metrics);
    if (!draggingColumn) {
      return;
    }

    // The returned left/right are the original position, not dragged position
    // This is where the dragging column's floating position accounting for dragged distance
    const floatingDraggingLeft = mouseX - this.draggingOffset - gridX;
    const floatingDraggingRight = floatingDraggingLeft + draggingColumn.width;

    this.draggingColumn = {
      ...this.draggingColumn,
      left: floatingDraggingLeft,
    };
    grid.setState({ draggingColumn: this.draggingColumn });

    const swapColumn = getColumnInfo(
      GridUtils.getColumnAtX(
        clamp(
          isDraggingLeft ? floatingDraggingLeft : floatingDraggingRight,
          floatingLeftWidth,
          width
        ) + gridX,
        metrics,
        true
      ),
      metrics
    );
    if (!swapColumn) {
      return;
    }

    if (floatingDraggingLeft <= floatingLeftWidth) {
      this.setScrollInterval(grid, 'left');
    } else if (floatingDraggingRight > width) {
      this.setScrollInterval(grid, 'right');
    } else {
      this.clearScrollInterval();
    }

    // Can't swap a column with itself
    if (swapColumn.visibleIndex === draggingColumn.visibleIndex) {
      return;
    }

    // Can't swap a column to the left when dragging right or vice versa
    if (
      (isDraggingLeft &&
        draggingColumn.visibleIndex < swapColumn.visibleIndex) ||
      (!isDraggingLeft && draggingColumn.visibleIndex > swapColumn.visibleIndex)
    ) {
      return;
    }

    const switchPoint = swapColumn.left + swapColumn.width * 0.5;
    if (
      (isDraggingLeft && floatingDraggingLeft < switchPoint) ||
      (!isDraggingLeft && floatingDraggingRight > switchPoint)
    ) {
      grid.setState({
        movedColumns: this.moveColumn(
          draggingColumn,
          swapColumn.visibleIndex,
          movedColumns
        ),
        draggingColumn: this.draggingColumn,
      });
    }
  }

  /**
   * Applies the column move and updates draggingColumn in the mouse handler
   * Does not check if the move is valid
   * @param draggingColumn The dragging column info
   * @param to The index to move the column to
   * @param movedColumns The array of column moves
   * @returns A new array of column moves
   */
  moveColumn(
    draggingColumn: ColumnInfo,
    to: number,
    movedColumns: readonly MoveOperation[]
  ): readonly MoveOperation[] {
    const newMovedColumns = GridUtils.moveItem(
      draggingColumn.visibleIndex,
      to,
      movedColumns
    );
    if (this.draggingColumn) {
      this.draggingColumn = { ...this.draggingColumn, range: [to, to] };
    }
    return newMovedColumns;
  }

  onUp(_gridPoint: GridPoint, grid: Grid): EventHandlerResult {
    this.cursor = null;
    this.clearScrollInterval();

    if (this.draggingOffset != null) {
      const wasDragging = this.draggingColumn != null;
      this.draggingOffset = undefined;
      this.draggingColumn = null;
      grid.setState({ draggingColumn: null, isDragging: false });
      return wasDragging;
    }

    return false;
  }
}

export default GridColumnMoveMouseHandler;
