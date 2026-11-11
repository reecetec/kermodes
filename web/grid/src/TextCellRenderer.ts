// Adapted from @deephaven/grid (Apache-2.0), web-client-ui 0bab596
// CellRenderer and TextCellRenderer merged: text is the only cell type here.
import { getOrThrow } from './utils';
import { type VisibleIndex } from './GridMetrics';
import GridRenderer from './GridRenderer';
import { type GridRenderState } from './GridRendererTypes';
import GridUtils from './GridUtils';
import fastMemoize, {
  CELL_CACHE_SIZE,
  MAX_HASHED_STRING_LENGTH,
} from './fastMemoize';

class TextCellRenderer {
  drawCellContent(
    context: CanvasRenderingContext2D,
    state: GridRenderState,
    column: VisibleIndex,
    row: VisibleIndex
  ): void {
    const { metrics, model, theme } = state;
    const {
      fontWidthsLower,
      fontWidthsUpper,
      modelColumns,
      modelRows,
      allRowHeights,
    } = metrics;
    const rowHeight = getOrThrow(allRowHeights, row);
    const modelRow = getOrThrow(modelRows, row);
    const modelColumn = getOrThrow(modelColumns, column);
    const text = model.textForCell(modelColumn, modelRow);
    if (!text || rowHeight <= 0) {
      return;
    }

    const truncationChar = model.truncationCharForCell(modelColumn, modelRow);
    const {
      width: textWidth,
      x: textX,
      y: textY,
    } = GridUtils.getTextRenderMetrics(state, column, row);
    const truncatedText = this.getCachedTruncatedString(
      context,
      text,
      textWidth,
      fontWidthsLower.get(context.font),
      fontWidthsUpper.get(context.font),
      truncationChar
    );
    if (!truncatedText) {
      return;
    }

    context.textAlign = model.textAlignForCell(modelColumn, modelRow);
    context.fillStyle =
      model.colorForCell(modelColumn, modelRow, theme) || theme.textColor;
    context.fillText(truncatedText, textX, textY);
  }

  private cachedTruncateToWidth = fastMemoize(
    (
      context: CanvasRenderingContext2D,
      text: string,
      width: number,
      fontWidthLower?: number,
      fontWidthUpper?: number,
      truncationChar?: string
    ): string =>
      GridRenderer.truncateToWidth(
        context,
        text,
        width,
        fontWidthLower,
        fontWidthUpper,
        truncationChar
      ),
    CELL_CACHE_SIZE
  );

  getCachedTruncatedString = (
    context: CanvasRenderingContext2D,
    text: string,
    width: number,
    fontWidthLower?: number,
    fontWidthUpper?: number,
    truncationChar?: string
  ): string => {
    // Truncation never reads past the most characters that fit, so a very long
    // text is cut down to that before it is used as a cache key. The extra
    // character keeps it longer than what fits, so it is still truncated.
    const key =
      text.length > MAX_HASHED_STRING_LENGTH
        ? text.slice(
            0,
            GridRenderer.getMaxTruncatedLength(width, fontWidthLower) + 1
          )
        : text;
    return this.cachedTruncateToWidth(
      context,
      key,
      width,
      fontWidthLower,
      fontWidthUpper,
      truncationChar
    );
  };
}

export default TextCellRenderer;
