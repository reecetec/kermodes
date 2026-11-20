import { type ViewerState } from '@kermodes/client';

/**
 * What a viewer's body is showing: no rows before the first arrive, then rows that may be
 * behind the table while it loads, or behind a sort or filter sent and not yet applied. The
 * two can lag at once, and each is read on its own: the cover by the request, the row count by
 * the table.
 */
export type Showing =
  | { kind: 'noRows' }
  | { kind: 'rows'; tableLoading: boolean; requestPending: boolean };

/** Decides what the body shows */
export function viewerShowing(
  status: ViewerState['status'],
  hasRows: boolean,
  requestPending: boolean
): Showing {
  if (!hasRows) {
    return { kind: 'noRows' };
  }
  return { kind: 'rows', tableLoading: status === 'loading…', requestPending };
}

/**
 * What covers a viewer's rows when they are not current (docs/theme.md, "Live data states"):
 * a failure in their place, skeleton rows before the first arrive, or the rows dimmed under a
 * spinner while something brings them up to date. Current rows are covered by nothing.
 */
export type Cover =
  | { kind: 'failed'; message: string }
  | { kind: 'skeleton' }
  | { kind: 'busy'; label: string }
  | null;

export function viewerCover(
  state: Pick<ViewerState, 'error' | 'retrying'>,
  showing: Showing
): Cover {
  if (state.error != null) {
    return state.retrying === true
      ? { kind: 'busy', label: state.error }
      : { kind: 'failed', message: state.error };
  }
  if (showing.kind === 'noRows') {
    return { kind: 'skeleton' };
  }
  if (showing.requestPending) {
    return { kind: 'busy', label: 'Updating' };
  }
  if (showing.tableLoading) {
    return { kind: 'busy', label: 'Loading' };
  }
  return null;
}
