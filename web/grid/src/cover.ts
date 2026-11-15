import { type ViewerState } from './ViewerChannel';

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
  state: Pick<ViewerState, 'status' | 'error' | 'retrying'>,
  hasRows: boolean,
  isUpdating: boolean
): Cover {
  if (state.error != null) {
    return state.retrying === true
      ? { kind: 'busy', label: state.error }
      : { kind: 'failed', message: state.error };
  }
  if (!hasRows) {
    return { kind: 'skeleton' };
  }
  if (isUpdating) {
    return { kind: 'busy', label: 'Updating' };
  }
  if (state.status.startsWith('loading')) {
    return { kind: 'busy', label: 'Loading' };
  }
  return null;
}
