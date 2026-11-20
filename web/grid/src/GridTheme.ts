// Adapted from @deephaven/grid (Apache-2.0), web-client-ui 0bab596
/** A color parsed as CSS color value, eg. '#FF0000' */
export type GridColor = string;

/** A nullable color, eg. pass `null` to skip drawing this item */
export type NullableGridColor = GridColor | null;

/** One or more colors parsed as CSS color value separated by space, eg. '#FF0000 #00FF00' */
export type GridColorWay = string;

/** A font parsed as CSS font value */
export type GridFont = string;

export type GridTheme = {
  // Allow column/row resizing
  allowColumnResize: boolean;
  allowRowResize: boolean;

  // Select the full row/column upon selection
  autoSelectRow: boolean;
  autoSelectColumn: boolean;

  // Automatically size rows/columns to fit contents
  autoSizeColumns: boolean;
  autoSizeRows: boolean;

  // The background color for the whole grid
  backgroundColor: GridColor;

  // Color to draw text
  textColor: GridColor;

  // Text of numbers above, below, and at zero, and of dates and times, after Deephaven's grid
  positiveNumberColor: GridColor;
  negativeNumberColor: GridColor;
  zeroNumberColor: GridColor;
  dateColor: GridColor;

  // Muted tint over cells that hold no value (nulls)
  nullBackgroundColor: NullableGridColor;

  // Amount of padding within a cell and header
  cellHorizontalPadding: number;
  headerHorizontalPadding: number;

  // The font to use in the grid
  font: GridFont;

  // Colors to draw grid lines between columns and rows
  gridColumnColor: NullableGridColor;
  gridRowColor: NullableGridColor;

  // Colors for drawing the column and row headers
  headerBackgroundColor: GridColor;
  headerSeparatorColor: GridColor;
  headerSeparatorHoverColor: GridColor;
  headerSeparatorHandleSize: number;
  headerHiddenSeparatorSize: number;
  headerHiddenSeparatorHoverColor: GridColor;
  headerColor: GridColor;
  headerFont: GridFont;

  // Background color to highlight entire column/row if set
  columnHoverBackgroundColor: NullableGridColor;
  rowHoverBackgroundColor: NullableGridColor;
  selectedRowHoverBackgroundColor: NullableGridColor;
  // Background of the whole row holding the cursor cell (the cell last clicked or moved to)
  cursorRowBackgroundColor: NullableGridColor;

  // Background colors to draw for each row, cycling through each value (eg. alternating stripes)
  rowBackgroundColors: GridColorWay;

  // Scroll bar look and sizing
  minScrollHandleSize: number;
  scrollBarBackgroundColor: GridColor;
  scrollBarHoverBackgroundColor: GridColor;
  scrollBarCasingColor: GridColor;
  scrollBarCornerColor: GridColor;
  scrollBarColor: GridColor;
  scrollBarHoverColor: GridColor;
  scrollBarActiveColor: GridColor;
  scrollBarSize: number;
  scrollBarHoverSize: number;
  scrollBarCasingWidth: number;
  scrollSnapToColumn: boolean;
  scrollSnapToRow: boolean;

  scrollBarSelectionTick: boolean;
  scrollBarSelectionTickColor: NullableGridColor;
  scrollBarActiveSelectionTickColor: NullableGridColor;

  // Look of the current selection
  activeCellSelectionBorderWidth: number;
  selectionColor: GridColor;
  selectionOutlineColor: GridColor;
  selectionOutlineCasingColor: GridColor;

  // A column being dragged: how far it lifts and the shadow under it
  reorderOffset: number;
  shadowBlur: number;
  shadowColor: GridColor;

  // Background a changed or copied cell flashes before it fades out
  flashColor: GridColor;

  // Outline and text of a quick filter entry that could not be applied
  errorColor: GridColor;

  // Default row height/column width
  rowHeight: number;
  columnWidth: number;
  minColumnWidth: number;
  maxColumnWidth: number;

  // Default row/column header/footers width/height
  columnHeaderHeight: number;
  rowHeaderWidth: number;
  rowFooterWidth: number;

  // When resizing the header, will snap to the auto size of the header within this threshold
  headerResizeSnapThreshold: number;
  headerResizeHiddenSnapThreshold: number;

  // Colors for the grid in floating sections
  floatingGridColumnColor: NullableGridColor;
  floatingGridRowColor: NullableGridColor;

  // Background row colors for grid in the floating sections
  floatingRowBackgroundColors: GridColorWay;

  // Divider colors between the floating parts and the grid
  floatingDividerOuterColor: GridColor;
  floatingDividerInnerColor: GridColor;
};

/** A design token's value by name, without the leading `--` (web/theme/tokens.css) */
export type TokenReader = (name: string) => string;

/**
 * The grid's look from the design tokens (docs/theme.md), so the canvas draws with the same
 * colours and fonts as the page around it: cells in the mono for aligned figures, headers in
 * the sans.
 */
export function gridTheme(token: TokenReader): GridTheme {
  const sans = token('font-sans');
  const mono = token('font-mono');
  const background = token('background');
  const muted = token('muted');
  const border = token('border');
  const primary = token('primary');
  return {
    allowColumnResize: true,
    allowRowResize: false,
    // A click selects the whole row, as Deephaven's tables do: Cmd/Ctrl-C copies rows, and
    // Option/Alt-click copies a single cell (TableViewer).
    autoSelectRow: true,
    autoSelectColumn: false,
    autoSizeColumns: true,
    autoSizeRows: true,
    backgroundColor: background,
    textColor: token('foreground'),
    nullBackgroundColor: token('null'),
    cellHorizontalPadding: 8,
    headerHorizontalPadding: 8,
    font: `12px ${mono}`,
    gridColumnColor: border,
    gridRowColor: null,
    headerBackgroundColor: muted,
    headerSeparatorColor: border,
    headerSeparatorHoverColor: token('muted-foreground'),
    headerSeparatorHandleSize: 5,
    headerHiddenSeparatorSize: 5,
    headerHiddenSeparatorHoverColor: primary,
    headerColor: token('muted-foreground'),
    headerFont: `500 12px ${sans}`,
    columnHoverBackgroundColor: null,
    rowHoverBackgroundColor: token('accent'),
    selectedRowHoverBackgroundColor: token('selection'),
    cursorRowBackgroundColor: token('accent'),
    rowBackgroundColors: `${background} ${token('stripe')}`,
    minScrollHandleSize: 50,
    scrollBarBackgroundColor: background,
    scrollBarHoverBackgroundColor: muted,
    scrollBarCasingColor: background,
    scrollBarCornerColor: background,
    scrollBarColor: token('scrollbar'),
    scrollBarHoverColor: token('scrollbar-hover'),
    scrollBarActiveColor: token('muted-foreground'),
    scrollBarSize: 10,
    scrollBarHoverSize: 12,
    scrollBarCasingWidth: 0,
    scrollSnapToColumn: false,
    scrollSnapToRow: false,
    activeCellSelectionBorderWidth: 2,
    selectionColor: token('selection'),
    selectionOutlineColor: primary,
    selectionOutlineCasingColor: background,
    reorderOffset: 2,
    shadowBlur: 8,
    shadowColor: token('shadow'),
    flashColor: token('flash'),
    errorColor: token('destructive'),
    positiveNumberColor: token('positive'),
    negativeNumberColor: token('negative'),
    zeroNumberColor: token('muted-foreground'),
    dateColor: token('date'),
    scrollBarSelectionTick: true,
    scrollBarSelectionTickColor: token('selection'),
    scrollBarActiveSelectionTickColor: primary,

    rowHeight: 22,
    columnWidth: 100,
    minColumnWidth: 55,
    maxColumnWidth: 600,
    columnHeaderHeight: 30,
    rowHeaderWidth: 0,
    rowFooterWidth: 0,

    headerResizeSnapThreshold: 10,
    headerResizeHiddenSnapThreshold: 8,

    floatingGridColumnColor: border,
    floatingGridRowColor: null,
    floatingRowBackgroundColors: `${muted} ${muted}`,
    floatingDividerOuterColor: border,
    floatingDividerInnerColor: border,
  };
}

/** The grid's theme from the tokens in effect at `element`, which must be in the document */
export function gridThemeOf(element: Element): GridTheme {
  const style = getComputedStyle(element);
  return gridTheme(name => style.getPropertyValue(`--${name}`).trim());
}

/**
 * Geometry and behaviour for a grid given no theme; its colours are the token names, so a host
 * that forgets to pass `gridThemeOf(...)` draws nothing rather than a second palette.
 */
const baseTheme: GridTheme = Object.freeze(gridTheme(name => name));

export default baseTheme;
