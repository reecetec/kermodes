import GridMetricCalculator, {
  type GridMetricState,
} from './GridMetricCalculator';
import { type ModelIndex, type VisibleIndex } from './GridMetrics';
import GridRenderer from './GridRenderer';
import { type GridRenderState } from './GridRendererTypes';
import GridUtils from './GridUtils';
import ViewportGridModel from './ViewportGridModel';
import { getOrThrow } from './utils';

// Room at the right of every header for the sort arrow and its priority
const SORT_INDICATOR_WIDTH = 18;
const SORT_INDICATOR_FONT = 'bold 9px sans-serif';

/** Column widths leave room in the header for the sort indicator, so sorting never resizes */
export class TableViewerMetricCalculator extends GridMetricCalculator {
  calculateColumnHeaderWidth(
    modelColumn: ModelIndex,
    state: GridMetricState,
    maxColumnWidth: number
  ): number {
    return (
      super.calculateColumnHeaderWidth(modelColumn, state, maxColumnWidth) +
      SORT_INDICATOR_WIDTH
    );
  }
}

/** Draws the sort in the headers and flashes over changed or copied cells */
export class TableViewerRenderer extends GridRenderer {
  drawColumnHeaderAtIndex(
    context: CanvasRenderingContext2D,
    state: GridRenderState,
    index: VisibleIndex,
    bounds: { minX: number; maxX: number }
  ): void {
    const { metrics, model, theme } = state;
    const { modelColumns, allColumnWidths, allColumnXs, gridX } = metrics;
    const width = getOrThrow(allColumnWidths, index);
    const x = getOrThrow(allColumnXs, index) + gridX;
    const modelColumn = getOrThrow(modelColumns, index);
    const sort =
      model instanceof ViewportGridModel
        ? model.sortForColumn(modelColumn)
        : null;

    this.drawColumnHeader(
      context,
      state,
      model.textForColumnHeader(modelColumn) ?? '',
      x,
      width,
      {
        backgroundColor: model.colorForColumnHeader(modelColumn) ?? undefined,
        textInsetRight: sort == null ? 0 : SORT_INDICATOR_WIDTH,
      },
      bounds
    );
    if (sort == null || width <= SORT_INDICATOR_WIDTH) {
      return;
    }

    const { columnHeaderHeight, headerHorizontalPadding, selectionOutlineColor } =
      theme;
    const isMultiKey =
      model instanceof ViewportGridModel && model.sortKeys.length > 1;
    const right = x + width - headerHorizontalPadding;
    const y = columnHeaderHeight * 0.5;

    context.save();
    context.beginPath();
    context.rect(
      Math.max(x, bounds.minX),
      0,
      Math.min(x + width, bounds.maxX) - Math.max(x, bounds.minX),
      columnHeaderHeight
    );
    context.clip();
    context.font = SORT_INDICATOR_FONT;
    context.fillStyle = selectionOutlineColor;
    context.textAlign = 'right';
    let arrowRight = right;
    if (isMultiKey) {
      const priority = `${sort.priority}`;
      context.fillText(priority, right, y + 3);
      arrowRight -= context.measureText(priority).width + 1;
    }
    context.fillText(sort.descending ? '▼' : '▲', arrowRight, y);
    context.restore();
  }

  drawCellBackgrounds(
    context: CanvasRenderingContext2D,
    state: GridRenderState
  ): void {
    super.drawCellBackgrounds(context, state);

    const { metrics, model, theme } = state;
    if (!(model instanceof ViewportGridModel)) {
      return;
    }
    const { allColumnXs, allColumnWidths, allRowYs, allRowHeights, movedColumns } =
      metrics;
    context.save();
    context.fillStyle = theme.flashColor;
    model.forEachFlash(performance.now(), (modelColumn, row, progress) => {
      const column = GridUtils.getVisibleIndex(modelColumn, movedColumns);
      const x = allColumnXs.get(column);
      const y = allRowYs.get(row);
      if (x === undefined || y === undefined) {
        return;
      }
      // Hold the flash bright at first, then fade it out
      context.globalAlpha = 1 - progress * progress;
      context.fillRect(
        x + 1,
        y + 1,
        getOrThrow(allColumnWidths, column) - 1,
        getOrThrow(allRowHeights, row) - 1
      );
    });
    context.restore();
  }
}
