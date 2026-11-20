import { describe, expect, it } from 'bun:test';
import { type ViewerState } from '@kermodes/client';
import { viewerCover as coverOf, viewerShowing } from './cover';

type Viewed = Pick<ViewerState, 'status' | 'error' | 'retrying'>;

const viewerCover = (state: Viewed, hasRows: boolean, isUpdating: boolean) =>
  coverOf(state, viewerShowing(state.status, hasRows, isUpdating));

const ticking: Viewed = { status: 'ticking' };

describe('viewerCover', () => {
  it('covers current rows with nothing', () => {
    expect(viewerCover(ticking, true, false)).toBeNull();
    expect(viewerCover({ status: 'static', error: null, retrying: false }, true, false)).toBeNull();
  });

  it('shows skeleton rows until the first rows arrive', () => {
    expect(viewerCover(ticking, false, false)).toEqual({ kind: 'skeleton' });
    expect(viewerCover({ status: 'loading…' }, false, true)).toEqual({ kind: 'skeleton' });
  });

  it('dims the rows while a sort or filter, or the table, catches up', () => {
    expect(viewerCover(ticking, true, true)).toEqual({ kind: 'busy', label: 'Updating' });
    expect(viewerCover({ status: 'loading…' }, true, false)).toEqual({
      kind: 'busy',
      label: 'Loading',
    });
  });

  it('dims the rows while the view comes back by itself, whatever else is pending', () => {
    const away = { ...ticking, error: 'Reconnecting', retrying: true };
    expect(viewerCover(away, true, true)).toEqual({ kind: 'busy', label: 'Reconnecting' });
    expect(viewerCover(away, false, false)).toEqual({ kind: 'busy', label: 'Reconnecting' });
  });

  it('puts a failure in place of the rows', () => {
    const gone = { ...ticking, error: 'the table is gone', retrying: false };
    expect(viewerCover(gone, true, true)).toEqual({
      kind: 'failed',
      message: 'the table is gone',
    });
  });
});
