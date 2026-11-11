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

const FONT_FAMILY =
  '"Fira Sans", -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif';

/**
 * Dark theme, after the default dark theme of the Deephaven web UI: near-black
 * background, subtle zebra rows, a muted header and a blue accent selection.
 */
export const darkTheme: GridTheme = Object.freeze({
  allowColumnResize: true,
  allowRowResize: false,
  autoSelectRow: false,
  autoSelectColumn: false,
  autoSizeColumns: true,
  autoSizeRows: true,
  backgroundColor: '#1a171a',
  textColor: '#f0f0ee',
  nullBackgroundColor: '#ffffff0a',
  cellHorizontalPadding: 6,
  headerHorizontalPadding: 6,
  font: `12px ${FONT_FAMILY}`,
  gridColumnColor: '#2d2a2e',
  gridRowColor: null,
  headerBackgroundColor: '#211f22',
  headerSeparatorColor: '#373438',
  headerSeparatorHoverColor: '#929192',
  headerSeparatorHandleSize: 5,
  headerHiddenSeparatorSize: 5,
  headerHiddenSeparatorHoverColor: '#4c7dee',
  headerColor: '#c0bfbf',
  headerFont: `600 12px ${FONT_FAMILY}`,
  columnHoverBackgroundColor: null,
  rowHoverBackgroundColor: '#2d2a2e',
  selectedRowHoverBackgroundColor: '#34303a',
  rowBackgroundColors: '#1a171a #211f22',
  minScrollHandleSize: 50,
  scrollBarBackgroundColor: '#1a171a',
  scrollBarHoverBackgroundColor: '#211f22',
  scrollBarCasingColor: '#1a171a',
  scrollBarCornerColor: '#1a171a',
  scrollBarColor: '#403e41',
  scrollBarHoverColor: '#5b5a5c',
  scrollBarActiveColor: '#929192',
  scrollBarSize: 10,
  scrollBarHoverSize: 14,
  scrollBarCasingWidth: 1,
  scrollSnapToColumn: false,
  scrollSnapToRow: false,
  activeCellSelectionBorderWidth: 2,
  selectionColor: '#4c7dee33',
  selectionOutlineColor: '#4c7dee',
  selectionOutlineCasingColor: '#1a171a',
  scrollBarSelectionTick: true,
  scrollBarSelectionTickColor: '#4c7dee66',
  scrollBarActiveSelectionTickColor: '#4c7deecc',

  rowHeight: 20,
  columnWidth: 100,
  minColumnWidth: 55,
  maxColumnWidth: 600,
  columnHeaderHeight: 28,
  rowHeaderWidth: 0,
  rowFooterWidth: 0,

  headerResizeSnapThreshold: 10,
  headerResizeHiddenSnapThreshold: 8,

  floatingGridColumnColor: '#2d2a2e',
  floatingGridRowColor: null,
  floatingRowBackgroundColors: '#211f22 #2d2a2e',
  floatingDividerOuterColor: '#000000',
  floatingDividerInnerColor: '#5b5a5c',
});

/** Light theme, after the default light theme of the Deephaven web UI. */
export const lightTheme: GridTheme = Object.freeze({
  ...darkTheme,
  backgroundColor: '#ffffff',
  textColor: '#1a171a',
  nullBackgroundColor: '#0000000a',
  gridColumnColor: '#e6e5e6',
  headerBackgroundColor: '#f4f4f4',
  headerSeparatorColor: '#dcdbdc',
  headerSeparatorHoverColor: '#787679',
  headerHiddenSeparatorHoverColor: '#2f5bc4',
  headerColor: '#4a474b',
  rowHoverBackgroundColor: '#eceff8',
  selectedRowHoverBackgroundColor: '#dfe6f7',
  rowBackgroundColors: '#ffffff #f7f7f7',
  scrollBarBackgroundColor: '#ffffff',
  scrollBarHoverBackgroundColor: '#f4f4f4',
  scrollBarCasingColor: '#ffffff',
  scrollBarCornerColor: '#ffffff',
  scrollBarColor: '#cfcecf',
  scrollBarHoverColor: '#a8a7a9',
  scrollBarActiveColor: '#787679',
  selectionColor: '#2f5bc426',
  selectionOutlineColor: '#2f5bc4',
  selectionOutlineCasingColor: '#ffffff',
  scrollBarSelectionTickColor: '#2f5bc466',
  scrollBarActiveSelectionTickColor: '#2f5bc4cc',
  floatingGridColumnColor: '#e6e5e6',
  floatingRowBackgroundColors: '#f7f7f7 #eeeeee',
  floatingDividerOuterColor: '#dcdbdc',
  floatingDividerInnerColor: '#a8a7a9',
});

export type GridThemeName = 'dark' | 'light';

export const gridThemes: Readonly<Record<GridThemeName, GridTheme>> = {
  dark: darkTheme,
  light: lightTheme,
};

export default darkTheme;
