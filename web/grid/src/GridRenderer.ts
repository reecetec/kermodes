// Adapted from @deephaven/grid (Apache-2.0), web-client-ui 0bab596
import memoizeOne from 'memoize-one';
import fastMemoize from './fastMemoize';
import { clamp, getOrThrow } from './utils';
import GridUtils from './GridUtils';
import { type GridColorWay, type NullableGridColor } from './GridTheme';
import { type Coordinate, type VisibleIndex } from './GridMetrics';
import GridColumnSeparatorMouseHandler from './mouse-handlers/GridColumnSeparatorMouseHandler';
import { type BoundedAxisRange } from './GridAxisRange';
import { DEFAULT_FONT_WIDTH, type GridRenderState } from './GridRendererTypes';
import TextCellRenderer from './TextCellRenderer';
import { isTickRangeSelection } from './Selection';

/* eslint react/destructuring-assignment: "off" */
/* eslint class-methods-use-this: "off" */
/* eslint no-param-reassign: "off" */
/**
 * A GridRenderer handles rendering the different parts of the grid
 * This default rendering just renders a basic grid. Extend this class and implement
 * your own methods to customize drawing of the grid (eg. Draw icons or special features)
 */
export class GridRenderer {
  // Default radius in pixels for corners for some elements (like the active cell)
  static DEFAULT_EDGE_RADIUS = 2;

  protected textCellRenderer = new TextCellRenderer();

  /**
   * Truncate a string to the specified length and add ellipses if necessary
   * @param str The string to truncate
   * @param len The length to truncate the string to. If longer than the actual string, just returns the string
   * @returns The truncated string
   */
  static truncate(str: string, len: number): string {
    if (len < str.length) {
      // eslint-disable-next-line prefer-template
      return str.substr(0, len) + '…';
    }
    return str;
  }

  /**
   * Uses binary search to truncate a string to fit in the provided width
   * @param context The drawing context to measure the text in
   * @param str The string to get the maximum length it can draw
   * @param width The width to truncate it to
   * @param start The low boundary to start the search
   * @param end The high boundary to start the search
   * @param truncationChar This char will be repeated as the display string if the string is truncated instead of just adding an ellipsis
   * @returns The truncated string
   */
  static binaryTruncateToWidth(
    context: CanvasRenderingContext2D,
    str: string,
    width: number,
    start = 0,
    end = str.length,
    truncationChar?: string
  ): string {
    if (end >= str.length && context.measureText(str).width <= width) {
      // IDS-6069 If the whole string can fit, don't bother checking for truncation
      // The ellipses are actually slightly wider than other chars, and it's possible
      // that the "truncation" ends up being slightly longer, which messes up the search
      // algorithm below.
      // Besides, if we already fit, it's just faster to not bother checking other truncations.
      return str;
    }

    if (truncationChar !== undefined) {
      const charWidth = context.measureText(truncationChar).width;
      return truncationChar.repeat(Math.max(1, Math.floor(width / charWidth)));
    }

    let lo = start;
    let hi = Math.min(str.length - 1, end);
    let result = str;
    while (hi >= lo) {
      const mid = Math.ceil((hi + lo) / 2);
      const truncatedStr = GridRenderer.truncate(str, mid);
      if (context.measureText(truncatedStr).width <= width) {
        result = truncatedStr;
        if (lo === mid) {
          break;
        }
        lo = mid;
      } else if (mid === 0) {
        // We already truncated to zero chars and it still doesn't fit, no need to keep looking
        result = truncatedStr;
        break;
      } else {
        hi = mid - 1;
      }
    }

    return result;
  }

  /**
   * The most characters of a string that can be drawn within a width, based on
   * the estimated width of the narrowest character. `truncateToWidth` never
   * reads past this many characters.
   * @param width The width to truncate within
   * @param fontWidthLower The estimated width of the narrowest character
   * @returns The maximum number of characters that can fit
   */
  static getMaxTruncatedLength(
    width: number,
    fontWidthLower = DEFAULT_FONT_WIDTH / 2
  ): number {
    return Math.ceil(width / fontWidthLower);
  }

  /**
   * Truncate a string (if necessary) to fit in the specified width.
   * First uses the estimated font width to calculate a lower/upper bound
   * Then uses binary search within those bounds to find the exact max length
   * @param context The drawing context
   * @param str The string to calculate max length for
   * @param width The width to truncate within
   * @param fontWidth The estimated width of each character
   * @param truncationChar This char will be repeated as the display string if the string is truncated instead of just adding an ellipsis
   * @returns The truncated string that fits within the width provided
   */
  static truncateToWidth(
    context: CanvasRenderingContext2D,
    str: string,
    width: number,
    // Estimate the width of each character by taking half the default font width as the low and double it as the high
    fontWidthLower = DEFAULT_FONT_WIDTH / 2,
    fontWidthUpper = DEFAULT_FONT_WIDTH * 2,
    truncationChar?: string
  ): string {
    if (width <= 0 || str.length <= 0) {
      return '';
    }

    // Estimate the possible low and high boundaries for truncating the text
    // Use the width of the space divided by the upper and lower bounds of the width of a character
    // (minus 5 on the lower bound just to be extra safe)
    const lo = Math.min(
      Math.max(0, Math.floor(width / fontWidthUpper) - 5),
      str.length
    );
    const hi = Math.min(
      GridRenderer.getMaxTruncatedLength(width, fontWidthLower),
      str.length
    );

    return GridRenderer.binaryTruncateToWidth(
      context,
      str,
      width,
      lo,
      hi,
      truncationChar
    );
  }

  /** Split a space separated list of row background colors, cached for the last value */
  static getCachedBackgroundColors = memoizeOne(
    (backgroundColors: GridColorWay): string[] => backgroundColors.split(' ')
  );

  /**
   * Draw the grid canvas with the state provided
   * @param state The state of the grid
   */
  drawCanvas(state: GridRenderState): void {
    const { context } = state;

    context.save();

    this.configureContext(context, state);

    this.drawBackground(context, state);

    this.drawGrid(context, state);

    this.drawHeaders(context, state);

    this.drawFooters(context, state);

    this.drawScrollBars(context, state);

    context.restore();
  }

  configureContext(
    context: CanvasRenderingContext2D,
    state: GridRenderState
  ): void {
    const { theme } = state;
    context.font = theme.font;
    context.textBaseline = 'middle';
    context.lineCap = 'butt';
  }

  drawBackground(
    context: CanvasRenderingContext2D,
    state: GridRenderState
  ): void {
    const { theme, metrics } = state;
    const { width, height } = metrics;
    context.fillStyle = theme.backgroundColor;
    context.fillRect(0, 0, width, height);
  }

  drawGrid(context: CanvasRenderingContext2D, state: GridRenderState): void {
    const { metrics } = state;
    const { gridX, gridY } = metrics;

    context.translate(gridX, gridY);

    this.drawGridBackground(context, state, true);

    this.drawCellContents(context, state);

    this.drawFloatingRows(context, state);

    this.drawFloatingColumns(context, state);

    context.translate(-gridX, -gridY);
  }

  drawFloatingRows(
    context: CanvasRenderingContext2D,
    state: GridRenderState
  ): void {
    const { metrics, theme } = state;
    const {
      floatingTopRowCount,
      floatingBottomRowCount,
      floatingRows,
      rowCount,
      columnsForRender,
      allRowYs,
      allRowHeights,
    } = metrics;

    if (floatingRows.length === 0) {
      return;
    }

    if (theme.floatingRowBackgroundColors) {
      this.drawRowStripesForRows(
        context,
        state,
        floatingRows,
        theme.floatingRowBackgroundColors
      );
    }

    this.drawFloatingMouseRowHover(context, state);

    this.drawGridLinesForItems(
      context,
      state,
      columnsForRender,
      floatingRows,
      theme.floatingGridColumnColor,
      theme.floatingGridRowColor
    );

    this.drawCellBackgroundsForItems(
      context,
      state,
      columnsForRender,
      floatingRows
    );

    this.drawFloatingBorders(context, state);

    // Draw the floating row selection...
    if (floatingTopRowCount > 0) {
      this.drawSelectedRanges(context, state, {
        top: 0,
        bottom: floatingTopRowCount - 1,
        maxY:
          getOrThrow(allRowYs, floatingTopRowCount - 1) +
          getOrThrow(allRowHeights, floatingTopRowCount - 1) -
          0.5,
      });
    }
    if (floatingBottomRowCount > 0) {
      this.drawSelectedRanges(context, state, {
        top: rowCount - floatingBottomRowCount - 1,
        bottom: rowCount - 1,
        minY: getOrThrow(allRowYs, rowCount - floatingBottomRowCount) + 0.5,
        maxY:
          getOrThrow(allRowYs, rowCount - 1) +
          getOrThrow(allRowHeights, rowCount - 1) -
          0.5,
      });
    }

    // Draw the cell content...
    for (let c = 0; c < columnsForRender.length; c += 1) {
      const column = columnsForRender[c];
      for (let r = 0; r < floatingRows.length; r += 1) {
        const row = floatingRows[r];
        this.drawCellContent(context, state, column, row);
      }
    }
  }

  drawFloatingColumns(
    context: CanvasRenderingContext2D,
    state: GridRenderState
  ): void {
    const { metrics, theme } = state;
    const {
      floatingLeftColumnCount,
      floatingRightColumnCount,
      floatingLeftWidth,
      floatingRightWidth,
      floatingColumns,
      gridX,
      maxX,
      columnCount,
      visibleRows,
      allColumnXs,
      allColumnWidths,
      width,
      height,
    } = metrics;

    if (floatingColumns.length === 0) {
      return;
    }

    if (theme.floatingRowBackgroundColors) {
      this.drawRowStripesForRows(
        context,
        state,
        visibleRows,
        theme.floatingRowBackgroundColors,
        0,
        floatingLeftWidth
      );
      this.drawRowStripesForRows(
        context,
        state,
        visibleRows,
        theme.floatingRowBackgroundColors,
        width - gridX - floatingRightWidth,
        maxX
      );
    }

    this.drawFloatingMouseRowHover(context, state);

    // Clip floated column grid lines.
    context.save();
    context.beginPath();
    context.rect(0, 0, floatingLeftWidth, height);
    context.clip();

    this.drawGridLinesForItems(
      context,
      state,
      floatingColumns,
      visibleRows,
      theme.floatingGridColumnColor,
      theme.floatingGridRowColor
    );

    context.restore();

    this.drawCellBackgroundsForItems(
      context,
      state,
      floatingColumns,
      visibleRows
    );

    this.drawFloatingBorders(context, state);

    // Draw the floating column selection...
    if (floatingLeftColumnCount > 0) {
      this.drawSelectedRanges(context, state, {
        left: 0,
        right: floatingLeftColumnCount - 1,
        maxX:
          getOrThrow(allColumnXs, floatingLeftColumnCount - 1) +
          getOrThrow(allColumnWidths, floatingLeftColumnCount - 1),
      });
    }
    if (floatingRightColumnCount > 0) {
      this.drawSelectedRanges(context, state, {
        left: columnCount - floatingRightColumnCount,
        right: columnCount - 1,
        minX:
          getOrThrow(allColumnXs, columnCount - floatingRightColumnCount) + 0.5,
        maxX:
          getOrThrow(allColumnXs, columnCount - 1) +
          getOrThrow(allColumnWidths, columnCount - 1),
      });
    }

    // Draw the cell content...
    for (let c = 0; c < floatingColumns.length; c += 1) {
      const column = floatingColumns[c];
      for (let r = 0; r < visibleRows.length; r += 1) {
        const row = visibleRows[r];
        this.drawCellContent(context, state, column, row);
      }
    }
  }

  drawFloatingBorders(
    context: CanvasRenderingContext2D,
    state: GridRenderState
  ): void {
    const { metrics, theme } = state;
    const {
      floatingTopRowCount,
      floatingBottomRowCount,
      floatingLeftColumnCount,
      floatingRightColumnCount,
      rowCount,
      columnCount,
      allRowYs,
      allColumnXs,
      allRowHeights,
      allColumnWidths,
      maxX,
      maxY,
    } = metrics;
    const { floatingDividerOuterColor, floatingDividerInnerColor } = theme;

    context.lineWidth = 3;
    context.beginPath();
    context.strokeStyle = floatingDividerOuterColor;

    if (floatingTopRowCount > 0) {
      const y =
        getOrThrow(allRowYs, floatingTopRowCount - 1) +
        getOrThrow(allRowHeights, floatingTopRowCount - 1) +
        0.5;
      context.moveTo(0, y);
      context.lineTo(maxX, y);
    }

    if (floatingBottomRowCount > 0) {
      const y = getOrThrow(allRowYs, rowCount - floatingBottomRowCount) - 0.5;
      context.moveTo(0, y);
      context.lineTo(maxX, y);
    }

    if (floatingLeftColumnCount > 0) {
      const x =
        getOrThrow(allColumnXs, floatingLeftColumnCount - 1) +
        getOrThrow(allColumnWidths, floatingLeftColumnCount - 1) +
        0.5;
      context.moveTo(x, 0);
      context.lineTo(x, maxY);
    }

    if (floatingRightColumnCount > 0) {
      const x =
        getOrThrow(allColumnXs, columnCount - floatingRightColumnCount) - 0.5;
      context.moveTo(x, 0);
      context.lineTo(x, maxY);
    }

    context.stroke();

    context.beginPath();
    context.lineWidth = 1;
    context.strokeStyle = floatingDividerInnerColor;

    if (floatingTopRowCount > 0) {
      const y =
        getOrThrow(allRowYs, floatingTopRowCount - 1) +
        getOrThrow(allRowHeights, floatingTopRowCount - 1) +
        0.5;
      context.moveTo(0, y);
      context.lineTo(maxX, y);
    }

    if (floatingBottomRowCount > 0) {
      const y = getOrThrow(allRowYs, rowCount - floatingBottomRowCount) - 0.5;
      context.moveTo(0, y);
      context.lineTo(maxX, y);
    }

    if (floatingLeftColumnCount > 0) {
      const x =
        getOrThrow(allColumnXs, floatingLeftColumnCount - 1) +
        getOrThrow(allColumnWidths, floatingLeftColumnCount - 1) +
        0.5;
      context.moveTo(x, 0);
      context.lineTo(x, maxY);
    }

    if (floatingRightColumnCount > 0) {
      const x =
        getOrThrow(allColumnXs, columnCount - floatingRightColumnCount) - 0.5;
      context.moveTo(x, 0);
      context.lineTo(x, maxY);
    }

    context.stroke();
  }

  drawGridBackground(
    context: CanvasRenderingContext2D,
    state: GridRenderState,
    drawHover = false
  ): void {
    this.drawRowStripes(context, state);

    if (drawHover) {
      this.drawMouseColumnHover(context, state);

      this.drawMouseRowHover(context, state);
    }

    this.drawGridLines(context, state);

    this.drawCellBackgrounds(context, state);

    const { metrics } = state;
    const {
      bottom,
      right,
      floatingBottomRowCount,
      floatingLeftColumnCount,
      floatingRightColumnCount,
      floatingTopRowCount,
      columnCount,
      rowCount,
      allRowHeights,
      allRowYs,
      allColumnXs,
      allColumnWidths,
      width,
      height,
    } = metrics;
    this.drawSelectedRanges(context, state, {
      bottom: Math.min(bottom, rowCount - floatingBottomRowCount - 1),
      right: Math.min(right, columnCount - floatingRightColumnCount - 1),
      minX:
        floatingLeftColumnCount > 0 &&
        allColumnXs.has(floatingLeftColumnCount + 1)
          ? getOrThrow(allColumnXs, floatingLeftColumnCount + 1)
          : -10,
      minY:
        floatingTopRowCount > 0 && allRowYs.has(floatingTopRowCount + 1)
          ? getOrThrow(allRowYs, floatingTopRowCount + 1)
          : -10,
      maxX:
        floatingRightColumnCount > 0 &&
        allColumnXs.has(columnCount - floatingRightColumnCount - 1)
          ? getOrThrow(
              allColumnXs,
              columnCount - floatingRightColumnCount - 1
            ) +
            getOrThrow(
              allColumnWidths,
              columnCount - floatingRightColumnCount - 1
            ) -
            0.5
          : width + 10,
      maxY:
        floatingBottomRowCount > 0 &&
        allRowYs.has(rowCount - floatingBottomRowCount - 1)
          ? getOrThrow(allRowYs, rowCount - floatingBottomRowCount - 1) +
            getOrThrow(allRowHeights, rowCount - floatingBottomRowCount - 1) -
            0.5
          : height + 10,
    });
    this.drawGestureDragOutline(context, state);
  }

  drawRowStripes(
    context: CanvasRenderingContext2D,
    state: GridRenderState
  ): void {
    const { metrics, theme } = state;
    const { visibleRows } = metrics;
    const { rowBackgroundColors } = theme;
    if (!rowBackgroundColors) {
      return;
    }
    this.drawRowStripesForRows(
      context,
      state,
      visibleRows,
      rowBackgroundColors
    );
  }

  drawRowStripesForRows(
    context: CanvasRenderingContext2D,
    state: GridRenderState,
    rows: readonly VisibleIndex[],
    rowBackgroundColors: GridColorWay,
    minX = 0,
    maxX = state.metrics.maxX
  ): void {
    const { metrics } = state;
    const colors = GridRenderer.getCachedBackgroundColors(rowBackgroundColors);
    const { allRowYs, allRowHeights } = metrics;

    // Optimize by grouping together all rows that end up with the same color
    const colorRowMap = new Map<string, VisibleIndex[]>();
    for (let i = 0; i < rows.length; i += 1) {
      const row = rows[i];
      const color = colors[row % colors.length];
      const colorRows = colorRowMap.get(color);
      if (colorRows == null) {
        colorRowMap.set(color, [row]);
      } else {
        colorRows.push(row);
      }
    }

    colorRowMap.forEach((colorRows, color) => {
      context.fillStyle = color;

      context.beginPath();

      for (let i = 0; i < colorRows.length; i += 1) {
        const row = colorRows[i];
        const y = getOrThrow(allRowYs, row);
        const rowHeight = getOrThrow(allRowHeights, row);
        context.rect(minX, y, maxX, rowHeight);
      }

      context.fill();
    });
  }

  drawMouseColumnHover(
    context: CanvasRenderingContext2D,
    state: GridRenderState
  ): void {
    const { mouseX, mouseY, theme, metrics } = state;
    if (mouseX == null || mouseY == null) return;

    const mouseColumn = GridUtils.getColumnAtX(mouseX, metrics);
    if (mouseColumn == null || theme.columnHoverBackgroundColor == null) {
      return;
    }

    const { allColumnWidths, allColumnXs, maxY } = metrics;
    if (mouseY > maxY) {
      return;
    }

    const x = getOrThrow(allColumnXs, mouseColumn);
    const columnWidth = getOrThrow(allColumnWidths, mouseColumn);

    context.fillStyle = theme.columnHoverBackgroundColor;
    context.fillRect(x, 0, columnWidth, maxY);
  }

  drawMouseRowHover(
    context: CanvasRenderingContext2D,
    state: GridRenderState
  ): void {
    const { mouseX, mouseY, theme, metrics } = state;
    if (mouseX == null || mouseY == null) return;

    const { maxX } = metrics;
    if (mouseX > maxX || theme.rowHoverBackgroundColor == null) {
      return;
    }

    const mouseRow = GridUtils.getRowAtY(mouseY, metrics);
    if (mouseRow == null) {
      return;
    }

    this.drawMouseRowHoverForRow(context, state, mouseRow);
  }

  drawFloatingMouseRowHover(
    context: CanvasRenderingContext2D,
    state: GridRenderState
  ): void {
    const { mouseX, mouseY, theme, metrics } = state;
    const {
      maxX,
      floatingTopRowCount,
      floatingBottomRowCount,
      rowCount,
      rowFooterWidth,
    } = metrics;
    if (
      mouseX == null ||
      mouseY == null ||
      mouseX > maxX + rowFooterWidth ||
      theme.rowHoverBackgroundColor == null
    ) {
      return;
    }

    const mouseRow = GridUtils.getRowAtY(mouseY, metrics);
    if (
      mouseRow != null &&
      (mouseRow < floatingTopRowCount ||
        rowCount - floatingBottomRowCount <= mouseRow)
    ) {
      this.drawMouseRowHoverForRow(context, state, mouseRow);
    }
  }

  drawMouseRowHoverForRow(
    context: CanvasRenderingContext2D,
    state: GridRenderState,
    row: VisibleIndex
  ): void {
    const { metrics, selection, theme } = state;
    const { allRowHeights, allRowYs, columnsForRender, maxX } = metrics;

    const y = getOrThrow(allRowYs, row);
    const rowHeight = getOrThrow(allRowHeights, row);

    if (theme.rowHoverBackgroundColor != null) {
      context.fillStyle = theme.rowHoverBackgroundColor;
    }
    // A selected cell promotes the whole row to the selected hover background color.
    if (theme.selectedRowHoverBackgroundColor != null) {
      for (let i = 0; i < columnsForRender.length; i += 1) {
        if (selection.isCellSelected(columnsForRender[i], row)) {
          context.fillStyle = theme.selectedRowHoverBackgroundColor;
          break;
        }
      }
    }
    context.fillRect(0, y, maxX, rowHeight);
  }

  drawGridLines(
    context: CanvasRenderingContext2D,
    state: GridRenderState
  ): void {
    const { metrics, theme } = state;
    const { columnsForRender, visibleRows } = metrics;

    this.drawGridLinesForItems(
      context,
      state,
      columnsForRender,
      visibleRows,
      theme.gridColumnColor,
      theme.gridRowColor
    );
  }

  drawGridLinesForItems(
    context: CanvasRenderingContext2D,
    state: GridRenderState,
    columns: readonly VisibleIndex[],
    rows: readonly VisibleIndex[],
    columnColor: NullableGridColor,
    rowColor: NullableGridColor
  ): void {
    if (columnColor == null && rowColor == null) {
      return;
    }

    context.lineWidth = 1;
    context.beginPath();

    if (columnColor != null) {
      context.strokeStyle = columnColor;
      this.drawGridLinesForColumns(context, state, columns);
    }
    if (rowColor != null) {
      context.strokeStyle = rowColor;
      this.drawGridLinesForRows(context, state, rows);
    }

    context.stroke();
  }

  drawGridLinesForColumns(
    context: CanvasRenderingContext2D,
    state: GridRenderState,
    columns: readonly VisibleIndex[]
  ): void {
    const { metrics } = state;
    const { allColumnXs, maxY } = metrics;
    for (let i = 0; i < columns.length; i += 1) {
      const column = columns[i];
      const x = getOrThrow(allColumnXs, column) + 0.5;
      context.moveTo(x, 0);
      context.lineTo(x, maxY);
    }
  }

  drawGridLinesForRows(
    context: CanvasRenderingContext2D,
    state: GridRenderState,
    rows: readonly VisibleIndex[]
  ): void {
    const { metrics } = state;
    const { allRowYs, maxX: metricsMaxX } = metrics;
    const maxX = metricsMaxX;

    // Draw row lines
    for (let i = 0; i < rows.length; i += 1) {
      const row = rows[i];
      const y = getOrThrow(allRowYs, row) + 0.5;
      context.moveTo(0.5, y);
      context.lineTo(maxX - 0.5, y);
    }
  }

  drawCellBackgrounds(
    context: CanvasRenderingContext2D,
    state: GridRenderState
  ): void {
    const { metrics } = state;
    const { columnsForRender, visibleRows } = metrics;
    this.drawCellBackgroundsForItems(
      context,
      state,
      columnsForRender,
      visibleRows
    );
  }

  drawCellBackgroundsForItems(
    context: CanvasRenderingContext2D,
    state: GridRenderState,
    columns: readonly VisibleIndex[],
    rows: readonly VisibleIndex[]
  ): void {
    context.save();

    for (let i = 0; i < columns.length; i += 1) {
      const column = columns[i];

      for (let j = 0; j < rows.length; j += 1) {
        const row = rows[j];
        this.drawCellBackground(context, state, column, row);
      }
    }

    context.restore();
  }

  drawCellBackground(
    context: CanvasRenderingContext2D,
    state: GridRenderState,
    column: VisibleIndex,
    row: VisibleIndex
  ): void {
    const { metrics, model, theme } = state;
    const {
      modelColumns,
      modelRows,
      allColumnXs,
      allColumnWidths,
      allRowYs,
      allRowHeights,
    } = metrics;
    const modelRow = getOrThrow(modelRows, row);
    const modelColumn = getOrThrow(modelColumns, column);
    const backgroundColor = model.backgroundColorForCell(
      modelColumn,
      modelRow,
      theme
    );

    if (backgroundColor != null) {
      const x = getOrThrow(allColumnXs, column) + 1;
      const y = getOrThrow(allRowYs, row) + 1;
      const columnWidth = getOrThrow(allColumnWidths, column) - 1;
      const rowHeight = getOrThrow(allRowHeights, row) - 1;
      context.fillStyle = backgroundColor;
      context.fillRect(x, y, columnWidth, rowHeight);
    }
  }

  drawCellContents(
    context: CanvasRenderingContext2D,
    state: GridRenderState
  ): void {
    const { metrics } = state;
    const { columnsForRender } = metrics;

    for (let i = 0; i < columnsForRender.length; i += 1) {
      const column = columnsForRender[i];
      this.drawColumnCellContents(context, state, column);
    }
  }

  drawColumnCellContents(
    context: CanvasRenderingContext2D,
    state: GridRenderState,
    column: VisibleIndex
  ): void {
    const { metrics } = state;
    const { allColumnXs, allColumnWidths, visibleRows, height } = metrics;
    const x = getOrThrow(allColumnXs, column);
    const columnWidth = getOrThrow(allColumnWidths, column);

    context.save();

    context.beginPath();
    context.rect(x, 0, columnWidth, height);
    context.clip();

    for (let i = 0; i < visibleRows.length; i += 1) {
      const row = visibleRows[i];
      this.drawCellContent(context, state, column, row);
    }
    context.restore();
  }

  drawCellContent(
    context: CanvasRenderingContext2D,
    state: GridRenderState,
    column: VisibleIndex,
    row: VisibleIndex
  ): void {
    this.textCellRenderer.drawCellContent(context, state, column, row);
  }

  drawHeaders(context: CanvasRenderingContext2D, state: GridRenderState): void {
    const { theme } = state;

    context.font = theme.headerFont;

    this.drawColumnHeaders(context, state);

    this.drawRowHeaders(context, state);
  }

  drawFooters(context: CanvasRenderingContext2D, state: GridRenderState): void {
    const { theme } = state;

    context.font = theme.headerFont;

    this.drawRowFooters(context, state);
  }

  drawColumnHeaders(
    context: CanvasRenderingContext2D,
    state: GridRenderState
  ): void {
    const {
      mouseX,
      mouseY,
      theme,
      metrics,
      draggingColumnSeparator,
      isDragging,
      model,
    } = state;
    const {
      columnHeaderHeight,
      floatingColumns,
      gridX,
      width,
      visibleColumns,
      allColumnWidths,
      allColumnXs,
      floatingLeftColumnCount,
      floatingLeftWidth,
      floatingRightWidth,
      movedColumns,
      columnCount,
      columnHeaderMaxDepth,
    } = metrics;
    if (columnHeaderHeight <= 0) {
      return;
    }

    const {
      headerHiddenSeparatorSize,
      headerHiddenSeparatorHoverColor,
      headerSeparatorColor,
      headerSeparatorHoverColor,
    } = theme;
    const hiddenSeparatorHeight = columnHeaderHeight * 0.5;
    const hiddenY =
      columnHeaderHeight * (columnHeaderMaxDepth - 1) +
      columnHeaderHeight * 0.5 -
      hiddenSeparatorHeight * 0.5;
    const containsFrozenColumns = floatingLeftColumnCount > 0;

    context.save();

    this.drawColumnHeadersForRange(
      context,
      state,
      [visibleColumns[0], visibleColumns[visibleColumns.length - 1]],
      {
        minX: gridX + floatingLeftWidth,
        maxX: width - floatingRightWidth,
      }
    );

    if (containsFrozenColumns) {
      this.drawColumnHeadersForRange(
        context,
        state,
        [floatingColumns[0], floatingColumns[floatingColumns.length - 1]],
        {
          minX: gridX,
          maxX: gridX + floatingLeftWidth,
        }
      );
    }

    if (headerSeparatorColor) {
      context.strokeStyle = headerSeparatorColor;

      const hiddenColumns = [...allColumnWidths.entries()]
        .filter(([_, w]) => w === 0)
        .map(([index]) => index);

      // Now draw the hidden column separator boxes
      context.beginPath();
      context.fillStyle = headerSeparatorColor;
      for (let i = 0; i < hiddenColumns.length; i += 1) {
        const column = hiddenColumns[i];
        const columnX = getOrThrow(allColumnXs, column);
        const columnWidth = getOrThrow(allColumnWidths, column);
        const minX =
          gridX + columnX + columnWidth + 0.5 - headerHiddenSeparatorSize * 0.5;
        context.rect(
          minX,
          hiddenY,
          headerHiddenSeparatorSize,
          hiddenSeparatorHeight
        );
      }
      context.fill();
    }

    if (headerSeparatorHoverColor) {
      let { index: highlightedSeparator, depth } =
        draggingColumnSeparator ?? {};

      if (highlightedSeparator == null && mouseX != null && mouseY != null) {
        const separator = GridColumnSeparatorMouseHandler.getColumnSeparator(
          GridUtils.getGridPointFromXY(mouseX, mouseY, metrics),
          metrics,
          model,
          theme
        );
        highlightedSeparator = separator?.index;
        depth = separator?.depth;
      }

      let shouldDrawSeparator: boolean;

      if (highlightedSeparator == null) {
        shouldDrawSeparator = false;
      } else {
        // Expanding columns hidden under the separator can push the dragged
        // separator past the viewport, leaving it without metrics to draw with
        shouldDrawSeparator =
          allColumnXs.has(highlightedSeparator) &&
          allColumnWidths.has(highlightedSeparator) &&
          GridUtils.hasColumnSeparatorAtDepth(
            model,
            depth,
            highlightedSeparator,
            columnCount,
            movedColumns
          );
      }

      if (
        shouldDrawSeparator &&
        highlightedSeparator != null &&
        depth != null &&
        (!isDragging || draggingColumnSeparator != null)
      ) {
        context.strokeStyle = headerSeparatorHoverColor;

        const columnX = getOrThrow(allColumnXs, highlightedSeparator);
        const columnWidth = getOrThrow(allColumnWidths, highlightedSeparator);
        const x = gridX + columnX + columnWidth + 0.5;
        const visibleColumnIndex = visibleColumns.indexOf(highlightedSeparator);
        const nextColumn =
          visibleColumnIndex < visibleColumns.length - 1
            ? visibleColumns[visibleColumnIndex + 1]
            : null;
        const nextColumnWidth =
          nextColumn != null ? allColumnWidths.get(nextColumn) : null;
        const isColumnHidden = columnWidth === 0;
        const isNextColumnHidden =
          nextColumnWidth != null && nextColumnWidth === 0;
        if (isColumnHidden) {
          context.strokeStyle = headerHiddenSeparatorHoverColor;
          context.fillStyle = headerHiddenSeparatorHoverColor;
          context.fillRect(
            x,
            hiddenY,
            headerHiddenSeparatorSize * 0.5,
            hiddenSeparatorHeight
          );
        } else if (isNextColumnHidden) {
          context.fillStyle = headerSeparatorHoverColor;
          context.fillRect(
            x - headerHiddenSeparatorSize * 0.5,
            hiddenY,
            headerHiddenSeparatorSize * 0.5,
            hiddenSeparatorHeight
          );
        }

        // column seperator hover line
        context.beginPath();
        context.moveTo(
          x,
          (columnHeaderMaxDepth - depth - 1) * columnHeaderHeight
        );
        context.lineTo(
          x,
          (columnHeaderMaxDepth - depth) * columnHeaderHeight - 1
        );
        context.stroke();
      }
    }

    context.restore();
  }

  drawColumnHeadersForRange(
    context: CanvasRenderingContext2D,
    state: GridRenderState,
    range: BoundedAxisRange,
    bounds: { minX: number; maxX: number }
  ): void {
    const { model } = state;
    const { columnHeaderMaxDepth } = model;

    if (columnHeaderMaxDepth === 0) {
      return;
    }

    for (let d = 0; d <= columnHeaderMaxDepth; d += 1) {
      this.drawColumnHeadersAtDepth(context, state, range, bounds, d);
    }
  }

  drawColumnHeadersAtDepth(
    context: CanvasRenderingContext2D,
    state: GridRenderState,
    range: BoundedAxisRange,
    bounds: { minX: number; maxX: number },
    depth: number
  ): void {
    const { metrics, model, theme } = state;
    const {
      modelColumns,
      allColumnXs,
      gridX,
      userColumnWidths,
      allColumnWidths,
      movedColumns,
    } = metrics;
    const { columnHeaderHeight, columnWidth } = theme;
    const { columnHeaderMaxDepth } = model;
    const { minX, maxX } = bounds;
    const visibleWidth = maxX - minX;

    if (columnHeaderMaxDepth === 0) {
      return;
    }

    const startIndex = range[0];
    const endIndex = range[1];

    context.save();
    context.translate(
      0,
      (columnHeaderMaxDepth - depth - 1) * columnHeaderHeight
    );

    if (depth === 0) {
      // Make sure base column header background always goes to the right edge
      this.drawColumnHeader(context, state, '', minX, maxX);

      // Draw base column headers
      for (let i = startIndex; i <= endIndex; i += 1) {
        this.drawColumnHeaderAtIndex(context, state, i, bounds);
      }
    }

    // Draw column header group
    if (depth > 0) {
      let columnIndex = startIndex;

      while (columnIndex <= endIndex) {
        const { columnCount, calculatedColumnWidths } = metrics;
        const modelColumn = getOrThrow(modelColumns, columnIndex);
        const columnGroupName = model.textForColumnHeader(modelColumn, depth);
        const columnGroupColor = model.colorForColumnHeader(modelColumn, depth);
        let columnGroupLeft = getOrThrow(allColumnXs, columnIndex) + gridX;
        let columnGroupRight =
          columnGroupLeft + getOrThrow(allColumnWidths, columnIndex);

        if (columnGroupName != null) {
          // Need to determine if the column group is at least the width of the bounds
          // And if the left/right of the group extend past the bounds
          // The group will be drawn as if it were a column with a max width of the bounds width
          let prevColumnIndex = columnIndex - 1;
          while (
            prevColumnIndex >= 0 &&
            (columnGroupRight - columnGroupLeft < visibleWidth ||
              columnGroupLeft > minX)
          ) {
            const prevModelIndex =
              modelColumns.get(prevColumnIndex) ??
              GridUtils.getModelIndex(prevColumnIndex, movedColumns);
            if (
              prevModelIndex == null ||
              !GridUtils.isSameColumnGroupAtDepth(
                model,
                depth,
                modelColumn,
                prevModelIndex
              )
            ) {
              // Previous column not in the same group
              break;
            }

            const prevColumnWidth =
              userColumnWidths.get(prevModelIndex) ??
              allColumnWidths.get(prevColumnIndex) ??
              calculatedColumnWidths.get(prevModelIndex) ??
              columnWidth;

            columnGroupLeft -= prevColumnWidth;
            prevColumnIndex -= 1;
          }

          let nextColumnIndex = columnIndex + 1;
          while (
            nextColumnIndex < columnCount &&
            (columnGroupRight - columnGroupLeft < visibleWidth ||
              columnGroupRight < maxX)
          ) {
            const nextModelIndex =
              modelColumns.get(nextColumnIndex) ??
              GridUtils.getModelIndex(nextColumnIndex, movedColumns);
            if (
              !GridUtils.isSameColumnGroupAtDepth(
                model,
                depth,
                modelColumn,
                nextModelIndex
              )
            ) {
              // Next column not in the same group
              break;
            }

            const nextColumnWidth =
              userColumnWidths.get(nextModelIndex) ??
              allColumnWidths.get(nextColumnIndex) ??
              columnWidth;

            columnGroupRight += nextColumnWidth;
            nextColumnIndex += 1;
          }

          // Set column index to end of the current group
          columnIndex = nextColumnIndex - 1;

          const isFullWidth =
            columnGroupRight - columnGroupLeft >= visibleWidth;
          let x = columnGroupLeft;
          if (isFullWidth) {
            if (columnGroupRight < maxX) {
              x = columnGroupRight - visibleWidth;
            } else if (columnGroupLeft < minX) {
              x = minX;
            }
          }

          this.drawColumnHeader(
            context,
            state,
            columnGroupName,
            x,
            Math.min(columnGroupRight - columnGroupLeft, visibleWidth),
            {
              backgroundColor: columnGroupColor ?? undefined,
            },
            bounds
          );
        }
        columnIndex += 1;
      }
    }
    context.restore();
  }

  /**
   * Draws the column header for the given visible index
   * @param context Canvas context
   * @param state Grid render state
   * @param index Visible index of the column header to draw
   * @param bounds The horizontal bounds the header can be drawn in
   */
  drawColumnHeaderAtIndex(
    context: CanvasRenderingContext2D,
    state: GridRenderState,
    index: VisibleIndex,
    bounds: { minX: number; maxX: number }
  ): void {
    const { metrics, model } = state;
    const { modelColumns, allColumnWidths, allColumnXs, gridX } = metrics;
    const width = getOrThrow(allColumnWidths, index);
    const x = getOrThrow(allColumnXs, index) + gridX;
    const modelColumn = getOrThrow(modelColumns, index);
    const text = model.textForColumnHeader(modelColumn);

    if (text == null) {
      return;
    }

    this.drawColumnHeader(
      context,
      state,
      text,
      x,
      width,
      {
        backgroundColor: model.colorForColumnHeader(modelColumn) ?? undefined,
      },
      bounds
    );
  }

  drawColumnHeader(
    context: CanvasRenderingContext2D,
    state: GridRenderState,
    columnText: string,
    columnX: Coordinate,
    columnWidth: number,
    style?: {
      backgroundColor?: string;
      textColor?: string;
      separatorColor?: string;
    },
    bounds?: { minX?: number; maxX?: number }
  ): void {
    if (columnWidth <= 0) {
      return;
    }
    const { metrics, theme } = state;

    const {
      headerHorizontalPadding,
      columnHeaderHeight,
      headerBackgroundColor,
      headerColor,
      headerSeparatorColor,
    } = theme;
    const { fontWidthsLower, fontWidthsUpper, width } = metrics;

    const maxWidth = columnWidth - headerHorizontalPadding * 2;

    const {
      backgroundColor = headerBackgroundColor,
      separatorColor = headerSeparatorColor,
      textColor = headerColor,
    } = style ?? {};

    let { minX = 0, maxX = width } = bounds ?? {};

    context.save();
    context.rect(minX, 0, maxX - minX, columnHeaderHeight);
    context.clip();

    // Fill background color if specified
    if (backgroundColor != null) {
      context.fillStyle = backgroundColor;
      context.fillRect(columnX, 0, columnWidth, columnHeaderHeight);
    }

    if (separatorColor != null) {
      context.strokeStyle = separatorColor;
      context.beginPath();

      // Don't draw left separator if column touches the left edge
      if (columnX > 0) {
        context.moveTo(columnX + 0.5, 0);
        context.lineTo(columnX + 0.5, columnHeaderHeight);
      }
      context.moveTo(columnX + columnWidth + 0.5, 0);
      context.lineTo(columnX + columnWidth + 0.5, columnHeaderHeight);

      // Bottom Border, should be interior to the header height
      context.moveTo(columnX, columnHeaderHeight - 0.5);
      context.lineTo(columnX + columnWidth, columnHeaderHeight - 0.5);
      context.stroke();
    }

    context.beginPath();
    context.rect(columnX, 0, columnWidth, columnHeaderHeight);
    context.clip();
    context.fillStyle = textColor;

    const fontWidthLower = fontWidthsLower.get(context.font);
    const fontWidthUpper = fontWidthsUpper.get(context.font);
    const renderText = this.textCellRenderer.getCachedTruncatedString(
      context,
      columnText,
      maxWidth,
      fontWidthLower,
      fontWidthUpper
    );

    let x = columnX + columnWidth * 0.5;
    const y = columnHeaderHeight * 0.5;
    minX += headerHorizontalPadding;
    maxX -= headerHorizontalPadding;

    const columnLeft = columnX + headerHorizontalPadding;
    const visibleLeft = clamp(columnLeft, minX, maxX);
    const columnRight = columnX + columnWidth - headerHorizontalPadding;
    const visibleRight = clamp(columnRight, minX, maxX);
    const visibleWidth = visibleRight - visibleLeft;

    const textWidth = this.getCachedHeaderWidth(context, renderText);
    const isBeyondLeft = x - textWidth * 0.5 < minX;
    const isBeyondRight = x + textWidth * 0.5 > maxX;

    if (isBeyondLeft) {
      // Column name would be off the left side of the canvas
      if (textWidth < visibleWidth) {
        // Can render the entire text in the visible space. Stick to left
        x = minX + textWidth * 0.5;
      } else {
        x = columnRight - textWidth * 0.5;
      }
    } else if (isBeyondRight) {
      if (textWidth < visibleWidth) {
        // Can render the entire text in the visible space. Stick to right
        x = maxX - textWidth * 0.5;
      } else {
        x = columnLeft + textWidth * 0.5;
      }
    }
    context.textAlign = 'center';
    context.fillText(renderText, x, y);

    context.restore();
  }

  drawRowHeaders(
    context: CanvasRenderingContext2D,
    state: GridRenderState
  ): void {
    const { metrics, theme } = state;
    const {
      gridY,
      rowHeaderWidth,
      height,
      visibleRows,
      allRowHeights,
      allRowYs,
    } = metrics;
    if (rowHeaderWidth <= 0) {
      return;
    }

    const {
      headerBackgroundColor,
      headerColor,
      headerHiddenSeparatorSize,
      headerSeparatorColor,
    } = theme;
    const hiddenSeparatorWidth = rowHeaderWidth * 0.5;
    const hiddenX = rowHeaderWidth * 0.5 - hiddenSeparatorWidth * 0.5;

    context.save();

    context.beginPath();

    // Fill in the background
    context.fillStyle = headerBackgroundColor;
    context.fillRect(0, 0, rowHeaderWidth, height);

    // Draw the separators
    if (headerSeparatorColor) {
      context.strokeStyle = headerSeparatorColor;
      context.beginPath();
      context.moveTo(0, gridY + 0.5);
      context.lineTo(rowHeaderWidth, gridY + 0.5);

      const hiddenRows = [];
      let isPreviousRowHidden = false;
      for (let i = 0; i < visibleRows.length; i += 1) {
        const row = visibleRows[i];
        const rowY = getOrThrow(allRowYs, row);
        const rowHeight = getOrThrow(allRowHeights, row);
        if (rowHeight > 0) {
          const y = gridY + rowY + rowHeight + 0.5;

          context.moveTo(0, y);
          context.lineTo(rowHeaderWidth, y);

          isPreviousRowHidden = false;
        } else if (!isPreviousRowHidden) {
          isPreviousRowHidden = true;
          hiddenRows.push(row);
        }
      }

      // border right, interior to the headerWidth
      context.moveTo(rowHeaderWidth - 0.5, 0);
      context.lineTo(rowHeaderWidth - 0.5, height);

      context.stroke();

      // Draw the hidden column separators
      context.beginPath();
      context.fillStyle = headerSeparatorColor;
      for (let i = 0; i < hiddenRows.length; i += 1) {
        const row = hiddenRows[i];
        const rowY = getOrThrow(allRowYs, row);
        const rowHeight = getOrThrow(allRowHeights, row);
        const minY =
          gridY + rowY + rowHeight + 0.5 - headerHiddenSeparatorSize * 0.5;
        context.rect(
          hiddenX,
          minY,
          hiddenSeparatorWidth,
          headerHiddenSeparatorSize
        );
      }

      context.fill();
    }

    // Fill in the text
    context.beginPath();
    context.rect(0, gridY, rowHeaderWidth, height);
    context.clip();

    context.fillStyle = headerColor;
    context.textAlign = 'right';

    for (let i = 0; i < visibleRows.length; i += 1) {
      const row = visibleRows[i];
      const rowHeight = getOrThrow(allRowHeights, row);
      const y = getOrThrow(allRowYs, row) + gridY;
      this.drawRowHeader(context, state, row, y, rowHeight);
    }

    context.restore();
  }

  drawRowHeader(
    context: CanvasRenderingContext2D,
    state: GridRenderState,
    row: VisibleIndex,
    rowY: Coordinate,
    rowHeight: number
  ): void {
    if (rowHeight <= 0) {
      return;
    }
    const { metrics, model, theme } = state;
    const { modelRows, rowHeaderWidth } = metrics;
    const modelRow = getOrThrow(modelRows, row);
    const x = rowHeaderWidth - theme.cellHorizontalPadding;
    const y = rowY + rowHeight * 0.5;
    context.fillText(model.textForRowHeader(modelRow), x, y);
  }

  drawRowFooters(
    context: CanvasRenderingContext2D,
    state: GridRenderState
  ): void {
    const { metrics, model, theme } = state;
    const {
      gridY,
      gridX,
      maxX,
      modelRows,
      rowFooterWidth,
      height,
      verticalBarWidth,
      visibleRows,
      allRowHeights,
      allRowYs,
      width,
    } = metrics;
    if (rowFooterWidth <= 0) {
      return;
    }

    const {
      cellHorizontalPadding,
      headerBackgroundColor,
      headerColor,
      headerHiddenSeparatorSize,
      headerSeparatorColor,
    } = theme;
    const hiddenSeparatorWidth = rowFooterWidth * 0.5;
    const hiddenX = rowFooterWidth * 0.5 - hiddenSeparatorWidth * 0.5;
    const x = Math.min(gridX + maxX, width - rowFooterWidth - verticalBarWidth);

    context.save();

    context.beginPath();

    // Fill in the background
    context.fillStyle = headerBackgroundColor;
    context.fillRect(x, gridY, rowFooterWidth, height);

    // Draw the separators
    if (headerSeparatorColor) {
      context.strokeStyle = headerSeparatorColor;
      context.beginPath();
      context.moveTo(x, gridY + 0.5);
      context.lineTo(rowFooterWidth, gridY + 0.5);

      const hiddenRows = [];
      let isPreviousRowHidden = false;
      for (let i = 0; i < visibleRows.length; i += 1) {
        const row = visibleRows[i];
        const rowY = getOrThrow(allRowYs, row);
        const rowHeight = getOrThrow(allRowHeights, row);
        if (rowHeight > 0) {
          const y = gridY + rowY + rowHeight + 0.5;

          context.moveTo(x + 0.5, y);
          context.lineTo(x + rowFooterWidth - 0.5, y);

          isPreviousRowHidden = false;
        } else if (!isPreviousRowHidden) {
          isPreviousRowHidden = true;
          hiddenRows.push(row);
        }
      }

      // border left, interior to the headerWidth
      context.moveTo(x + 0.5, gridY);
      context.lineTo(x + 0.5, height);

      context.stroke();

      // Draw the hidden column separators
      context.beginPath();
      context.fillStyle = headerSeparatorColor;
      for (let i = 0; i < hiddenRows.length; i += 1) {
        const row = hiddenRows[i];
        const rowY = getOrThrow(allRowYs, row);
        const rowHeight = getOrThrow(allRowHeights, row);
        const minY =
          gridY + rowY + rowHeight + 0.5 - headerHiddenSeparatorSize * 0.5;
        context.rect(
          x + hiddenX,
          minY,
          hiddenSeparatorWidth,
          headerHiddenSeparatorSize
        );
      }

      context.fill();
    }

    // Fill in the text
    context.beginPath();
    context.rect(x, gridY, rowFooterWidth, height);
    context.clip();

    context.fillStyle = headerColor;
    context.textAlign = 'left';

    const textX = x + cellHorizontalPadding;
    for (let i = 0; i < visibleRows.length; i += 1) {
      const row = visibleRows[i];
      const rowHeight = getOrThrow(allRowHeights, row);
      if (rowHeight > 0) {
        const rowY = getOrThrow(allRowYs, row) + gridY;
        const modelRow = getOrThrow(modelRows, row);
        const textY = rowY + rowHeight * 0.5;
        context.fillText(model.textForRowFooter(modelRow), textX, textY);
      }
    }

    context.restore();
  }

  drawSelectedRanges(
    context: CanvasRenderingContext2D,
    state: GridRenderState,
    viewport: {
      left?: VisibleIndex;
      top?: VisibleIndex;
      right?: VisibleIndex;
      bottom?: VisibleIndex;
      minX?: Coordinate;
      minY?: Coordinate;
      maxX?: Coordinate;
      maxY?: Coordinate;
    } = {}
  ): void {
    const { metrics, selection, theme } = state;
    const { cursorColumn: column, cursorRow: row } = selection;
    const {
      allColumnWidths,
      allColumnXs,
      allRowHeights,
      allRowYs,
      width,
      height,
    } = metrics;
    const {
      left = metrics.left,
      top = metrics.top,
      right = metrics.right,
      bottom = metrics.bottom,
      minY = -10,
      maxY = height + 10,
      minX = -10,
      maxX = width + 10,
    } = viewport;

    if (selection.isEmpty()) {
      return;
    }

    const isCursorVisible =
      column != null &&
      row != null &&
      allColumnXs.has(column) &&
      allRowYs.has(row);
    if (isCursorVisible) {
      // Punch a hole out where the active cell is, it gets styled differently.
      const x = getOrThrow(allColumnXs, column);
      const y = getOrThrow(allRowYs, row);
      const w = getOrThrow(allColumnWidths, column);
      const h = getOrThrow(allRowHeights, row);

      context.save();

      context.beginPath();

      context.rect(0, 0, width, height);
      context.rect(x, y, w, h);

      context.clip('evenodd');
    }

    // Column bounds are constant across all rows for full-row selection.
    // Guard against missing keys during resize/initial-load when allColumnXs may be empty.
    const rowSelectionX = allColumnXs.has(left)
      ? Math.max(Math.round(getOrThrow(allColumnXs, left)) + 0.5, minX)
      : maxX;
    const rowSelectionEndX =
      allColumnXs.has(right) && allColumnWidths.has(right)
        ? Math.min(
            Math.round(
              getOrThrow(allColumnXs, right) +
                getOrThrow(allColumnWidths, right)
            ) - 0.5,
            maxX
          )
        : minX;

    context.beginPath();
    // A full-row run is a special case of a partial run whose column list is a
    // single full-width entry, so both cases share one coalescer below. Reused
    // by reference so the equality check short-circuits for consecutive full rows.
    const FULL_ROW_COLS: readonly { x: number; endX: number }[] =
      rowSelectionEndX > rowSelectionX
        ? [{ x: rowSelectionX, endX: rowSelectionEndX }]
        : [];

    // Coalesce vertically-adjacent rows sharing an identical column signature
    // into one rect per column run rather than rendering strokes between each row.
    let runStartY: number | null = null;
    let runEndY = 0;
    let runCols: readonly { x: number; endX: number }[] = [];
    const colsEqual = (
      a: readonly { x: number; endX: number }[],
      b: readonly { x: number; endX: number }[]
    ): boolean => {
      if (a === b) return true;
      if (a.length !== b.length) return false;
      for (let i = 0; i < a.length; i += 1) {
        if (a[i].x !== b[i].x || a[i].endX !== b[i].endX) return false;
      }
      return true;
    };
    const flushRun = (): void => {
      if (runStartY != null) {
        for (let i = 0; i < runCols.length; i += 1) {
          const { x, endX } = runCols[i];
          context.rect(x, runStartY, endX - x, runEndY - runStartY);
        }
      }
      runStartY = null;
      runCols = [];
    };

    for (let r = top; r <= bottom; r += 1) {
      const rowY = allRowYs.get(r);
      const rowH = allRowHeights.get(r);
      if (rowY == null || rowH == null) {
        flushRun();
        // eslint-disable-next-line no-continue
        continue;
      }
      const y = Math.max(Math.round(rowY) + 0.5, 0.5);
      const endY = Math.round(rowY + rowH) - 0.5;
      if (endY < minY || y > maxY) {
        flushRun();
        // eslint-disable-next-line no-continue
        continue;
      }

      let rowCols: readonly { x: number; endX: number }[];
      if (selection.isRowSelected(r)) {
        rowCols = FULL_ROW_COLS;
      } else {
        const built: { x: number; endX: number }[] = [];
        const pushRun = (
          runStartCol: VisibleIndex,
          runEndCol: VisibleIndex
        ): void => {
          const x = Math.max(
            Math.round(getOrThrow(allColumnXs, runStartCol)) + 0.5,
            minX
          );
          const endX = Math.min(
            Math.round(
              getOrThrow(allColumnXs, runEndCol) +
                getOrThrow(allColumnWidths, runEndCol)
            ) - 0.5,
            maxX
          );
          if (endX > x) built.push({ x, endX });
        };
        let runStart: VisibleIndex | null = null;
        for (let c = left; c <= right; c += 1) {
          if (selection.isCellSelected(c, r)) {
            if (runStart === null) runStart = c;
          } else if (runStart !== null) {
            pushRun(runStart, c - 1);
            runStart = null;
          }
        }
        if (runStart !== null) {
          pushRun(runStart, right);
        }
        rowCols = built;
      }

      if (rowCols.length === 0) {
        flushRun();
      } else if (runStartY != null && colsEqual(rowCols, runCols)) {
        runEndY = endY;
      } else {
        flushRun();
        runCols = rowCols;
        runStartY = y;
        runEndY = endY;
      }
    }
    flushRun();

    /**
     * Create the path, then draw it once. Fill and
     * stroke must be outside the beginPath loop otherwise
     * the fill/stroke will be drawn multiple times.
     */

    // draw the inner transparent fill
    context.fillStyle = theme.selectionColor;
    context.fill();

    /**
     * draw an "inner stroke" that's clipped to just inside of the rects
     * to act as a casing to the outer stroke. 3px width because 1px is outside
     * the rect (but clipped), 1px is "on" the rect (technically this pixel is
     * a half pixel clip as well due to rects offset, but we are immediately painting
     * over it), and then the 1px inside (which is the desired pixel).
     */
    context.save();
    context.clip();
    context.strokeStyle = theme.selectionOutlineCasingColor;
    context.lineWidth = 3;
    context.stroke();
    context.restore();

    // draw the outerstroke border on top of the inner stroke
    context.strokeStyle = theme.selectionOutlineColor;
    context.lineWidth = 1;
    context.stroke();

    if (isCursorVisible && column != null && row != null) {
      context.restore();

      this.drawActiveCell(context, state, column, row);
    }
  }

  drawActiveCell(
    context: CanvasRenderingContext2D,
    state: GridRenderState,
    column: VisibleIndex,
    row: VisibleIndex
  ): void {
    const { metrics, theme } = state;
    const {
      scrollX,
      scrollY,
      allColumnWidths,
      allColumnXs,
      allRowHeights,
      allRowYs,
    } = metrics;
    const { activeCellSelectionBorderWidth: borderWidth } = theme;
    const cellX = getOrThrow(allColumnXs, column);
    const cellY = getOrThrow(allRowYs, row);
    const cellW = getOrThrow(allColumnWidths, column);
    const cellH = getOrThrow(allRowHeights, row);

    // Now get the outline for the active cell
    let x = cellX - borderWidth * 0.5;
    let y = cellY - borderWidth * 0.5;
    let w = cellW + borderWidth;
    let h = cellH + borderWidth;

    // Make sure the outline is interior on the edge
    if (x <= 0 && scrollX <= 0) {
      w -= borderWidth - x;
      x = borderWidth * 0.5;
    }
    if (y <= 0 && scrollY <= 0) {
      h -= borderWidth - y;
      y = borderWidth * 0.5;
    }

    const { lineWidth } = context;
    context.beginPath();
    context.lineWidth = borderWidth;
    context.strokeStyle = theme.selectionOutlineColor;
    this.drawRoundedRect(context, x, y, w, h);
    context.stroke();
    context.lineWidth = lineWidth;
  }

  /**
   * Draws an outline around the in-flight ctrl+click / ctrl+shift+click drag
   * rectangle. Uses `selection.cursor` (mouse-down anchor) and
   * `selection.selectionEnd` (current drag position) for the bounds. For row
   * selection themes only the top and bottom horizontal strokes are drawn
   * (row-boundary preview); for column selection only left/right; for cell
   * selection all four sides.
   */
  drawGestureDragOutline(
    context: CanvasRenderingContext2D,
    state: GridRenderState
  ): void {
    const { gestureMode, selection, metrics, theme } = state;
    if (gestureMode !== 'add' && gestureMode !== 'maximize') return;
    const { cursorRow, cursorColumn, selectionEndRow, selectionEndColumn } =
      selection;
    if (
      cursorRow == null ||
      cursorColumn == null ||
      selectionEndRow == null ||
      selectionEndColumn == null
    ) {
      return;
    }
    const {
      allColumnXs,
      allColumnWidths,
      allRowYs,
      allRowHeights,
      maxX,
      maxY,
    } = metrics;
    const { autoSelectRow = false, autoSelectColumn = false } = theme;

    const topRow = Math.min(cursorRow, selectionEndRow);
    const bottomRow = Math.max(cursorRow, selectionEndRow);
    const leftCol = Math.min(cursorColumn, selectionEndColumn);
    const rightCol = Math.max(cursorColumn, selectionEndColumn);

    const topY = allRowYs.get(topRow);
    const bottomRowY = allRowYs.get(bottomRow);
    const bottomRowH = allRowHeights.get(bottomRow);
    const leftX = allColumnXs.get(leftCol);
    const rightColX = allColumnXs.get(rightCol);
    const rightColW = allColumnWidths.get(rightCol);
    if (
      topY == null ||
      bottomRowY == null ||
      bottomRowH == null ||
      leftX == null ||
      rightColX == null ||
      rightColW == null
    ) {
      return;
    }

    const y0 = Math.round(topY) + 0.5;
    const y1 = Math.round(bottomRowY + bottomRowH) - 0.5;
    const x0 = autoSelectRow ? 0 : Math.round(leftX) + 0.5;
    const x1 = autoSelectRow ? maxX : Math.round(rightColX + rightColW) - 0.5;

    context.save();
    context.beginPath();
    context.strokeStyle = theme.selectionOutlineColor;
    context.lineWidth = 1;
    // Top / bottom horizontal strokes — always drawn (row + cell modes).
    if (!autoSelectColumn) {
      context.moveTo(x0, y0);
      context.lineTo(x1, y0);
      context.moveTo(x0, y1);
      context.lineTo(x1, y1);
    }
    // Left / right vertical strokes — cell and column modes only.
    if (!autoSelectRow) {
      const yTop = autoSelectColumn ? 0 : y0;
      const yBottom = autoSelectColumn ? maxY : y1;
      context.moveTo(x0, yTop);
      context.lineTo(x0, yBottom);
      context.moveTo(x1, yTop);
      context.lineTo(x1, yBottom);
    }
    context.stroke();
    context.restore();
  }

  /**
   * Draws a rounded rectangle using the current state of the canvas.
   *
   * @param context The canvas context
   * @param x coordinate of the left side
   * @param y coordinate of the top side
   * @param w width of the rectangle
   * @param h height of the rectangle
   * @param r corner radius of the rectangle
   */
  drawRoundedRect(
    context: CanvasRenderingContext2D,
    x: Coordinate,
    y: Coordinate,
    w: number,
    h: number,
    r = GridRenderer.DEFAULT_EDGE_RADIUS
  ): void {
    context.beginPath();
    context.moveTo(x + r, y);
    context.lineTo(x + w - r, y);
    context.quadraticCurveTo(x + w, y, x + w, y + r);
    context.lineTo(x + w, y + h - r);
    context.quadraticCurveTo(x + w, y + h, x + w - r, y + h);
    context.lineTo(x + r, y + h);
    context.quadraticCurveTo(x, y + h, x, y + h - r);
    context.lineTo(x, y + r);
    context.quadraticCurveTo(x, y, x + r, y);
    context.closePath();
  }

  drawScrollBars(
    context: CanvasRenderingContext2D,
    state: GridRenderState
  ): void {
    const {
      isDraggingHorizontalScrollBar,
      isDraggingVerticalScrollBar,
      isDragging,
      metrics,
      mouseX,
      mouseY,
      theme,
    } = state;
    if (theme.scrollBarSize <= 0) {
      return;
    }

    const {
      width,
      height,
      handleHeight,
      handleWidth,
      scrollX,
      scrollY,
      hasHorizontalBar,
      hasVerticalBar,
      barWidth,
      barHeight,
      barLeft,
      barTop,
    } = metrics;
    const {
      scrollBarBackgroundColor,
      scrollBarHoverBackgroundColor,
      scrollBarCasingColor,
      scrollBarCornerColor,
      scrollBarColor,
      scrollBarHoverColor,
      scrollBarActiveColor,
      scrollBarSize,
      scrollBarHoverSize,
      scrollBarCasingWidth,
      scrollBarSelectionTick,
      scrollBarSelectionTickColor,
      scrollBarActiveSelectionTickColor,
      autoSelectRow,
      autoSelectColumn,
    } = theme;

    const isInbounds =
      mouseX != null && mouseY != null && mouseX <= width && mouseY <= height;

    const isVerticalBarHover =
      isDraggingVerticalScrollBar ||
      (hasVerticalBar &&
        !isDraggingHorizontalScrollBar &&
        !isDragging &&
        mouseX != null &&
        mouseY != null &&
        mouseX >= width - scrollBarHoverSize &&
        mouseY >= barTop &&
        isInbounds);

    const isHorizontalBarHover =
      isDraggingHorizontalScrollBar ||
      (hasHorizontalBar &&
        !isDraggingVerticalScrollBar &&
        !isDragging &&
        !isVerticalBarHover && // vert bar gets priorty in overlapped corner hover area
        mouseX != null &&
        mouseY != null &&
        mouseY >= height - scrollBarHoverSize &&
        mouseX >= barLeft &&
        isInbounds);

    const hScrollBarSize = isHorizontalBarHover
      ? scrollBarHoverSize
      : scrollBarSize;
    const vScrollBarSize = isVerticalBarHover
      ? scrollBarHoverSize
      : scrollBarSize;

    context.translate(barLeft, barTop);

    if (hasHorizontalBar && hasVerticalBar) {
      // That little corner in the bottom right
      context.fillStyle = scrollBarCasingColor;
      context.fillRect(
        width - barLeft - scrollBarSize,
        height - barTop - scrollBarSize,
        scrollBarSize,
        scrollBarSize
      );
      context.fillStyle = scrollBarCornerColor;
      context.fillRect(
        width - barLeft - scrollBarSize + scrollBarCasingWidth,
        height - barTop - scrollBarSize + scrollBarCasingWidth,
        scrollBarSize - scrollBarCasingWidth,
        scrollBarSize - scrollBarCasingWidth
      );
    }

    if (hasHorizontalBar) {
      const x = scrollX;
      const y = height - barTop - hScrollBarSize;

      // scrollbar casing
      context.fillStyle = scrollBarCasingColor;
      context.fillRect(0, y, barWidth, hScrollBarSize - scrollBarCasingWidth);

      // scrollbar track
      context.fillStyle = isHorizontalBarHover
        ? scrollBarHoverBackgroundColor
        : scrollBarBackgroundColor;
      context.fillRect(
        0,
        y + scrollBarCasingWidth,
        barWidth,
        hScrollBarSize - scrollBarCasingWidth
      );

      // scrollbar thumb
      if (isDraggingHorizontalScrollBar) {
        context.fillStyle = scrollBarActiveColor;
      } else if (isHorizontalBarHover) {
        context.fillStyle = scrollBarHoverColor;
      } else {
        context.fillStyle = scrollBarColor;
      }

      context.fillRect(
        x,
        y + scrollBarCasingWidth,
        handleWidth,
        hScrollBarSize - scrollBarCasingWidth
      );

      if (
        !autoSelectRow &&
        scrollBarSelectionTick &&
        scrollBarSelectionTickColor != null &&
        scrollBarActiveSelectionTickColor != null
      ) {
        context.fillStyle = scrollBarSelectionTickColor;
        // Scrollbar Selection Tick
        const { cursorColumn } = state.selection;
        const { lastLeft, columnCount } = metrics;

        const mergedRanges = isTickRangeSelection(state.selection)
          ? state.selection.getColumnTickRanges()
          : [];

        const getTickX = (index: number): number => {
          if (index <= lastLeft) {
            return (index / lastLeft) * (barWidth - handleWidth);
          }
          return (
            barWidth -
            handleWidth +
            ((index - lastLeft) / (columnCount - lastLeft)) * handleWidth
          );
        };
        for (let i = 0; i < mergedRanges.length; i += 1) {
          const range = mergedRanges[i];
          const startColumn = range[0];
          const endColumn = range[1];

          if (
            startColumn != null &&
            endColumn != null &&
            (startColumn !== cursorColumn || endColumn !== cursorColumn)
          ) {
            const tickX = getTickX(startColumn);
            const tickWidth = Math.max(
              1,
              Math.round(getTickX(endColumn + 1) - tickX)
            );
            const trackHeight = hScrollBarSize - scrollBarCasingWidth;
            context.fillRect(
              tickX,
              y + scrollBarCasingWidth + Math.round(trackHeight / 3),
              tickWidth,
              Math.round(trackHeight / 3)
            );
          }
        }

        // Current Active Tick
        if (cursorColumn != null) {
          const tickX = getTickX(cursorColumn);
          const tickWidth = 2;
          const trackHeight = hScrollBarSize - scrollBarCasingWidth;
          context.fillStyle = scrollBarActiveSelectionTickColor;
          context.fillRect(
            tickX,
            y + scrollBarCasingWidth,
            tickWidth,
            trackHeight
          );
        }
      }
    }

    if (hasVerticalBar) {
      const x = width - barLeft - vScrollBarSize;
      const y = scrollY;

      // scrollbar casing
      context.fillStyle = scrollBarCasingColor;
      context.fillRect(x, 0, vScrollBarSize - scrollBarCasingWidth, barHeight);

      // scrollbar track
      context.fillStyle = isVerticalBarHover
        ? scrollBarHoverBackgroundColor
        : scrollBarBackgroundColor;
      context.fillRect(
        x + scrollBarCasingWidth,
        0,
        vScrollBarSize - scrollBarCasingWidth,
        barHeight
      );

      // scrollbar thumb
      if (isDraggingVerticalScrollBar) {
        context.fillStyle = scrollBarActiveColor;
      } else if (isVerticalBarHover) {
        context.fillStyle = scrollBarHoverColor;
      } else {
        context.fillStyle = scrollBarColor;
      }

      context.fillRect(
        x + scrollBarCasingWidth,
        y,
        vScrollBarSize - scrollBarCasingWidth,
        handleHeight
      );

      if (
        !autoSelectColumn &&
        scrollBarSelectionTick &&
        scrollBarSelectionTickColor != null &&
        scrollBarActiveSelectionTickColor != null
      ) {
        // Scrollbar Selection Tick
        const { cursorRow } = state.selection;
        const { lastTop, rowCount } = metrics;

        const getTickY = (index: number): number => {
          if (index <= lastTop) {
            return (index / lastTop) * (barHeight - handleHeight);
          }
          return (
            barHeight -
            handleHeight +
            ((index - lastTop) / (rowCount - lastTop)) * handleHeight
          );
        };

        context.fillStyle = scrollBarSelectionTickColor;

        const mergedRanges = isTickRangeSelection(state.selection)
          ? state.selection.getRowTickRanges()
          : [];

        for (let i = 0; i < mergedRanges.length; i += 1) {
          const range = mergedRanges[i];
          const startRow = range[0];
          const endRow = range[1];
          if (
            startRow != null &&
            endRow != null &&
            (startRow !== cursorRow || endRow !== cursorRow)
          ) {
            const tickY = getTickY(startRow);
            const trackWidth = vScrollBarSize - scrollBarCasingWidth;
            const tickHeight = Math.max(
              1,
              Math.round(getTickY(endRow + 1) - tickY)
            );
            context.fillRect(
              x + scrollBarCasingWidth + Math.round(trackWidth / 3),
              tickY,
              Math.round(trackWidth / 3),
              tickHeight
            );
          }
        }

        // Current Active Tick
        if (cursorRow != null) {
          const tickY = Math.round(getTickY(cursorRow));

          const trackWidth = vScrollBarSize - scrollBarCasingWidth;
          const tickHeight = 2;

          context.fillStyle = scrollBarActiveSelectionTickColor;
          context.fillRect(
            x + scrollBarCasingWidth,
            tickY,
            trackWidth,
            tickHeight
          );
        }
      }
    }

    context.translate(-barLeft, -barTop);
  }

  getCachedHeaderWidth = fastMemoize(
    (context: CanvasRenderingContext2D, text: string): number =>
      context.measureText(text).width,
    1000
  );
}

export default GridRenderer;
