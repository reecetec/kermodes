import { type SortKey } from '@kermodes/client';
import { isNumericType } from './ViewportGridModel';

/** What a menu item does; the viewer carries it out */
export type MenuAction =
  | { type: 'sort'; by: SortKey[] }
  // Set `column`'s quick-filter entry to `text` (FilterBar syntax)
  | { type: 'filter'; column: string; text: string }
  // Open the filter bar on `column`'s entry
  | { type: 'edit-filter'; column: string }
  | { type: 'clear-filters' }
  | { type: 'copy-cell' }
  // Open the cell's whole value in a popover
  | { type: 'show-value' }
  | { type: 'copy-selection'; header: boolean }
  | { type: 'copy-text'; text: string };

export type MenuItem = { label: string; action: MenuAction; hint?: string };

/**
 * What was right-clicked: a column header, or a cell with its raw value (null
 * for a null) and the text the grid shows for it.
 */
export type MenuTarget =
  | { kind: 'header'; column: string; type: string }
  | { kind: 'cell'; column: string; type: string; value: unknown; text: string };

/** The view the menu acts on */
export type MenuView = {
  sort: readonly SortKey[];
  filters: Readonly<Record<string, string>>;
  // Whether the selection holds more than the clicked cell (right-clicking a cell outside the
  // selection moves it there)
  isSelectionWide: boolean;
};

const SHOWN_VALUE_LENGTH = 24;

/** The menu for `target`, in sections separated by rules */
export function contextMenu(target: MenuTarget, view: MenuView): MenuItem[][] {
  const { column } = target;
  const sorts: MenuItem[] = [
    { label: 'Sort ascending', action: { type: 'sort', by: [{ column, descending: false }] } },
    { label: 'Sort descending', action: { type: 'sort', by: [{ column, descending: true }] } },
  ];
  if (view.sort.length > 0) {
    sorts.push({ label: 'Clear sort', action: { type: 'sort', by: [] } });
  }
  const clearFilters: MenuItem[] = [];
  if (view.filters[column]?.trim()) {
    clearFilters.push({
      label: `Clear filter on ${column}`,
      action: { type: 'filter', column, text: '' },
    });
  }
  if (Object.values(view.filters).some(text => text.trim() !== '')) {
    clearFilters.push({ label: 'Clear all filters', action: { type: 'clear-filters' } });
  }

  if (target.kind === 'header') {
    return [
      sorts,
      [
        { label: `Filter ${column}…`, action: { type: 'edit-filter', column }, hint: '⌘F' },
        ...clearFilters,
      ],
      [{ label: 'Copy column name', action: { type: 'copy-text', text: column } }],
    ];
  }

  const copies: MenuItem[] = [
    { label: 'Show value', action: { type: 'show-value' } },
    { label: 'Copy cell', action: { type: 'copy-cell' }, hint: '⌘-click' },
  ];
  if (view.isSelectionWide) {
    copies.push(
      { label: 'Copy selection', action: { type: 'copy-selection', header: false }, hint: '⌘C' },
      {
        label: 'Copy selection with headers',
        action: { type: 'copy-selection', header: true },
        hint: '⇧⌘C',
      }
    );
  }
  return [
    copies,
    [...valueFilters(target), ...clearFilters],
    sorts,
  ].filter(section => section.length > 0);
}

/**
 * Filters by the clicked value, as quick-filter entries the engine parses
 * (`kermodes_core::viewer::quick_filter`). Numbers use the raw value, not the
 * rounded text. Timestamps reach the browser as milliseconds, so equality
 * would miss finer ones: they get "at or after" and "before" the shown time.
 */
function valueFilters(
  target: Extract<MenuTarget, { kind: 'cell' }>
): MenuItem[] {
  const { column, type, value, text } = target;
  const item = (op: string, entry: string, shown = shorten(text)): MenuItem => ({
    label: `${column} ${op} ${shown}`,
    action: { type: 'filter', column, text: entry },
  });
  if (value === null) {
    return [
      { label: `${column} is null`, action: { type: 'filter', column, text: 'null' } },
      { label: `${column} is not null`, action: { type: 'filter', column, text: '!null' } },
    ];
  }
  if (value === undefined) {
    return [];
  }
  if (type === 'utf8') {
    const shown = `"${shorten(String(value))}"`;
    return [item('=', `=${String(value)}`, shown), item('≠', `!=${String(value)}`, shown)];
  }
  if (type === 'bool') {
    return [item('=', `=${String(value)}`), item('≠', `!=${String(value)}`)];
  }
  if (type === 'timestamp') {
    const time = new Date(Number(value)).toISOString();
    return [item('≥', `>=${time}`), item('<', `<${time}`)];
  }
  const raw =
    type === 'date32'
      ? text
      : isNumericType(type) && (typeof value === 'bigint' || Number.isFinite(value))
        ? String(value)
        : null;
  if (raw == null) {
    return [];
  }
  return [
    item('=', `=${raw}`),
    item('≠', `!=${raw}`),
    item('≥', `>=${raw}`),
    item('≤', `<=${raw}`),
  ];
}

function shorten(text: string): string {
  return text.length > SHOWN_VALUE_LENGTH ? `${text.slice(0, SHOWN_VALUE_LENGTH - 1)}…` : text;
}
