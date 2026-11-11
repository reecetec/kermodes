// Adapted from @deephaven/grid (Apache-2.0), web-client-ui 0bab596
import { type MoveOperation } from './GridMetrics';
import { type BoundedAxisRange } from './GridAxisRange';

export interface IColumnHeaderGroup {
  name: string;
  depth: number;
  color?: string;
  getVisibleRange: (movedColumns: readonly MoveOperation[]) => BoundedAxisRange;
}
