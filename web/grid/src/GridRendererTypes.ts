// Adapted from @deephaven/grid (Apache-2.0), web-client-ui 0bab596
import { type Coordinate } from './GridMetrics';
import type GridMetrics from './GridMetrics';
import type GridModel from './GridModel';
import { type GridTheme } from './GridTheme';
import { type GridSeparator } from './mouse-handlers/GridSeparatorMouseHandler';
import type { GestureMode, Selection } from './Selection';

// Default font width in pixels if it cannot be retrieved from the context
export const DEFAULT_FONT_WIDTH = 10;

export type GridRenderState = {
  // Width and height of the total canvas area
  width: number;
  height: number;

  // The canvas context
  context: CanvasRenderingContext2D;

  // The grid theme
  theme: GridTheme;

  // The model used by the grid
  model: GridModel;

  // The grid metrics
  metrics: GridMetrics;

  // Location of the mouse on the grid
  mouseX: Coordinate | null;
  mouseY: Coordinate | null;

  // Current selection. Owns the cursor and shift/drag endpoint.
  selection: Selection;

  // Column separator being dragged to resize
  draggingColumnSeparator: GridSeparator | null;

  isDraggingHorizontalScrollBar: boolean;
  isDraggingVerticalScrollBar: boolean;
  isDragging: boolean;

  // Modifier-derived mode of the in-flight mouse gesture; `null` when no
  // gesture is active. Drives the drag-overlay stroke on additive gestures.
  gestureMode: GestureMode | null;
};
