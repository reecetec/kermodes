// Adapted from @deephaven/grid (Apache-2.0), web-client-ui 0bab596
import { describe, expect, it } from 'bun:test';
import GridRange, { SELECTION_DIRECTION } from './GridRange';
import { RangedSelection } from './RangedSelection';
import type { GetModel, GestureExtendOptions } from './Selection';
import {
  computeGestureExtend,
  cursorLandingCellForRanges,
  gestureModeFromModifiers,
  nextCursorInRanges,
  withCommittedCursor,
} from './GridSelectionUtils';

const COLUMN_COUNT = 10;
const ROW_COUNT = 100;
const BOUNDS = { columnCount: COLUMN_COUNT, rowCount: ROW_COUNT };

function makeGetModel(
  columnCount = COLUMN_COUNT,
  rowCount = ROW_COUNT
): GetModel {
  return () => ({ columnCount, rowCount }) as never;
}

const getModel = makeGetModel();

/** Default `computeGestureExtend` options; individual tests override `mode`. */
function extendOpts(
  overrides: Partial<GestureExtendOptions> = {}
): GestureExtendOptions {
  return {
    mode: 'replace',
    autoSelectRow: false,
    autoSelectColumn: false,
    ...overrides,
  };
}

// ─── gestureModeFromModifiers ────────────────────────────────────────────────

describe('gestureModeFromModifiers', () => {
  it('returns `maximize` for shift+ctrl', () => {
    expect(
      gestureModeFromModifiers({ isShiftKey: true, isModifierKey: true })
    ).toBe('maximize');
  });

  it('returns `extend` for shift alone', () => {
    expect(
      gestureModeFromModifiers({ isShiftKey: true, isModifierKey: false })
    ).toBe('extend');
  });

  it('returns `add` for ctrl alone', () => {
    expect(
      gestureModeFromModifiers({ isShiftKey: false, isModifierKey: true })
    ).toBe('add');
  });

  it('returns `replace` when no modifiers are pressed', () => {
    expect(
      gestureModeFromModifiers({ isShiftKey: false, isModifierKey: false })
    ).toBe('replace');
  });
});

// ─── computeGestureExtend ────────────────────────────────────────────────────

describe('computeGestureExtend', () => {
  describe('replace mode', () => {
    it('returns a single-cell overlay and flags a replacing gesture', () => {
      const result = computeGestureExtend(
        [new GridRange(1, 1, 2, 2)],
        null,
        { row: 5, column: 3 },
        extendOpts({ mode: 'replace' })
      );
      expect(result.newRanges).toEqual([GridRange.makeCell(3, 5)]);
      expect(result.isReplacing).toBe(true);
      expect(result.trimBefore).toBe(false);
      expect(result.resetAnchor).toBe(true);
    });

    it('produces a full-row cell when autoSelectRow is on', () => {
      const result = computeGestureExtend(
        [],
        null,
        { row: 5, column: 3 },
        extendOpts({ mode: 'replace', autoSelectRow: true })
      );
      expect(result.newRanges).toEqual([new GridRange(null, 5, null, 5)]);
    });

    it('produces a full-column cell when autoSelectColumn is on', () => {
      const result = computeGestureExtend(
        [],
        null,
        { row: 5, column: 3 },
        extendOpts({ mode: 'replace', autoSelectColumn: true })
      );
      expect(result.newRanges).toEqual([new GridRange(3, null, 3, null)]);
    });
  });

  describe('add mode', () => {
    it('appends a new single-cell range to activeRanges', () => {
      const prior = [new GridRange(1, 1, 2, 2)];
      const result = computeGestureExtend(
        prior,
        null,
        { row: 5, column: 3 },
        extendOpts({ mode: 'add' })
      );
      expect(result.newRanges).toEqual([...prior, GridRange.makeCell(3, 5)]);
      expect(result.isReplacing).toBe(false);
      expect(result.trimBefore).toBe(false);
      expect(result.resetAnchor).toBe(true);
    });
  });

  describe('extend mode', () => {
    it('falls through to replace semantics when no active ranges and no anchor', () => {
      const result = computeGestureExtend(
        [],
        null,
        { row: 5, column: 3 },
        extendOpts({ mode: 'extend' })
      );
      expect(result.newRanges).toEqual([GridRange.makeCell(3, 5)]);
      expect(result.isReplacing).toBe(true);
      expect(result.trimBefore).toBe(false);
      expect(result.resetAnchor).toBe(true);
    });

    it('extends the last active range from anchor to cursor', () => {
      const result = computeGestureExtend(
        [new GridRange(2, 2, 2, 2)],
        { row: 2, column: 2 },
        { row: 6, column: 4 },
        extendOpts({ mode: 'extend' })
      );
      expect(result.newRanges).toEqual([new GridRange(2, 2, 4, 6)]);
      expect(result.isReplacing).toBe(true);
      expect(result.trimBefore).toBe(true);
      expect(result.resetAnchor).toBe(false);
    });

    it('returns rangesBeforeExtend and resetAnchor=false when the extended range equals the last range', () => {
      const active = [new GridRange(2, 2, 4, 6)];
      const result = computeGestureExtend(
        active,
        { row: 2, column: 2 },
        { row: 6, column: 4 },
        extendOpts({ mode: 'extend' })
      );
      expect(result.newRanges).toEqual(active);
      expect(result.resetAnchor).toBe(false);
    });
  });

  describe('maximize mode', () => {
    it('grows the last range to encompass both endpoints', () => {
      const result = computeGestureExtend(
        [new GridRange(3, 3, 5, 5)],
        { row: 3, column: 3 },
        { row: 1, column: 7 },
        extendOpts({ mode: 'maximize' })
      );
      // min(1, 3)=1, min(7, 3)=3, max(1, 5)=5, max(7, 5)=7
      expect(result.newRanges).toEqual([new GridRange(3, 1, 7, 5)]);
      expect(result.isReplacing).toBe(false);
      expect(result.trimBefore).toBe(false);
    });

    it('keeps prior activeRanges (except last) and grows only the last one', () => {
      const prior = new GridRange(0, 0, 0, 0);
      const last = new GridRange(3, 3, 5, 5);
      const result = computeGestureExtend(
        [prior, last],
        { row: 3, column: 3 },
        { row: 6, column: 6 },
        extendOpts({ mode: 'maximize' })
      );
      expect(result.newRanges).toEqual([prior, new GridRange(3, 3, 6, 6)]);
    });
  });
});

// ─── nextCursorInRanges ──────────────────────────────────────────────────────

describe('nextCursorInRanges', () => {
  it('walks the full grid when ranges are empty', () => {
    const result = nextCursorInRanges(
      [],
      { row: 0, column: 0 },
      SELECTION_DIRECTION.RIGHT,
      BOUNDS
    );
    expect(result).not.toBeNull();
    expect(result?.column).toBe(1);
    expect(result?.row).toBe(0);
  });

  it('walks the full grid when the ranges collapse to a single cell', () => {
    const result = nextCursorInRanges(
      [GridRange.makeCell(0, 0)],
      { row: 0, column: 0 },
      SELECTION_DIRECTION.RIGHT,
      BOUNDS
    );
    expect(result?.column).toBe(1);
    expect(result?.row).toBe(0);
  });

  it('walks inside a multi-cell range', () => {
    const result = nextCursorInRanges(
      [new GridRange(2, 2, 4, 4)],
      { row: 2, column: 2 },
      SELECTION_DIRECTION.RIGHT,
      BOUNDS
    );
    expect(result?.column).toBe(3);
    expect(result?.row).toBe(2);
  });
});

// ─── cursorLandingCellForRanges ──────────────────────────────────────────────

describe('cursorLandingCellForRanges', () => {
  it('returns null when ranges are empty', () => {
    expect(cursorLandingCellForRanges([], BOUNDS)).toBeNull();
  });

  it('returns the first cell of a bounded range', () => {
    const result = cursorLandingCellForRanges(
      [new GridRange(3, 4, 5, 6)],
      BOUNDS
    );
    expect(result).toEqual({ column: 3, row: 4 });
  });

  it('bounds a full-row range to (0, row)', () => {
    const result = cursorLandingCellForRanges(
      [new GridRange(null, 4, null, 4)],
      BOUNDS
    );
    expect(result).toEqual({ column: 0, row: 4 });
  });
});

// ─── withCommittedCursor ─────────────────────────────────────────────────────

describe('withCommittedCursor', () => {
  function selectionWith(
    ranges: readonly GridRange[],
    cursor: { row: number | null; column: number | null } = {
      row: null,
      column: null,
    }
  ): RangedSelection {
    return new RangedSelection(
      ranges,
      getModel,
      null,
      null,
      cursor.row,
      cursor.column
    );
  }

  it('returns settled unchanged when opts.cursor is undefined', () => {
    const settled = selectionWith([new GridRange(1, 1, 2, 2)]);
    const result = withCommittedCursor(
      settled,
      { row: 0, column: 0 },
      {
        autoSelectRow: false,
      }
    );
    expect(result).toBe(settled);
  });

  it('places the cursor at the target when settled is empty', () => {
    const settled = selectionWith([]);
    const result = withCommittedCursor(
      settled,
      { row: 9, column: 9 },
      {
        autoSelectRow: false,
        cursor: { row: 5, column: 3 },
      }
    );
    expect(result.cursorRow).toBe(5);
    expect(result.cursorColumn).toBe(3);
  });

  it('places the cursor at the target when target is inside the settled selection', () => {
    const settled = selectionWith([new GridRange(1, 1, 4, 4)]);
    const result = withCommittedCursor(
      settled,
      { row: 0, column: 0 },
      {
        autoSelectRow: false,
        cursor: { row: 3, column: 3 },
      }
    );
    expect(result.cursorRow).toBe(3);
    expect(result.cursorColumn).toBe(3);
  });

  it('falls back to landing when target is outside a non-empty settled selection', () => {
    const settled = selectionWith([new GridRange(1, 1, 4, 4)]);
    const result = withCommittedCursor(
      settled,
      { row: 1, column: 1 },
      {
        autoSelectRow: false,
        cursor: { row: 8, column: 8 },
      }
    );
    expect(result.cursorRow).toBe(1);
    expect(result.cursorColumn).toBe(1);
  });

  it('returns settled unchanged when the desired cursor already matches', () => {
    const settled = selectionWith([new GridRange(1, 1, 4, 4)], {
      row: 3,
      column: 3,
    });
    const result = withCommittedCursor(
      settled,
      { row: 0, column: 0 },
      {
        autoSelectRow: false,
        cursor: { row: 3, column: 3 },
      }
    );
    expect(result).toBe(settled);
  });
});
