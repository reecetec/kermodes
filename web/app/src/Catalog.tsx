import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { ChevronDown, ChevronRight, Search, Table2, X } from 'lucide-react';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { cn } from '@/lib/utils';
import {
  type Catalog,
  type CatalogTable,
  type Totals,
  namespaceOf,
  namespaces,
  tableDates,
  tableFields,
  totals,
  utcDate,
} from '@/catalog/model';
import { usePolled } from '@/deployments/data';
import { bytes, matchesSearch } from '@/deployments/model';
import { Field } from '@/deployments/parts';

const parseCatalog = (r: Response) => r.json() as Promise<Catalog>;

const PANEL_STORAGE = 'kermodes.catalog.panel-width';
const PANEL_DEFAULT = 440;
const PANEL_MIN = 300;
const PAGE_MIN = 480;

const count = (n: number) => n.toLocaleString();
const size = (n: number) => (n === 0 ? '—' : bytes(n));

/** Widths fixed so that values changing, or the panel opening, never move a column */
const COLUMNS: { label: string; width: string; numeric?: boolean }[] = [
  { label: 'Table', width: 'w-[16rem]' },
  { label: 'Kind', width: 'w-[8rem]' },
  { label: 'Rows today', width: 'w-[8rem]', numeric: true },
  { label: 'In weir', width: 'w-[7rem]', numeric: true },
  { label: 'History rows', width: 'w-[9rem]', numeric: true },
  { label: 'History size', width: 'w-[7.5rem]', numeric: true },
  { label: 'Dates', width: 'w-[13rem]' },
  { label: 'Columns', width: 'w-[6rem]', numeric: true },
];

function dateRange(t: Totals) {
  if (t.first == null) return '—';
  return t.first === t.last ? t.first : `${t.first} → ${t.last}`;
}

/** The totals cells shared by a namespace's row and a table's */
function TotalsCells({ t }: { t: Totals }) {
  return (
    <>
      <td className="py-1 pr-4 text-right tabular-nums">{count(t.today)}</td>
      <td className="py-1 pr-4 text-right tabular-nums">{size(t.weirBytes)}</td>
      <td className="py-1 pr-4 text-right tabular-nums">{count(t.historyRows)}</td>
      <td className="py-1 pr-4 text-right tabular-nums">{size(t.historyBytes)}</td>
      <td className="truncate py-1 pr-4 text-xs text-muted-foreground tabular-nums">
        {t.dates > 0 && <span className="mr-2 text-foreground">{t.dates}</span>}
        {dateRange(t)}
      </td>
    </>
  );
}

/** A handle between the table and the panel to its right: drag, arrow keys, or double-click */
function Splitter({ width, onResize }: { width: number; onResize: (width: number) => void }) {
  const [dragging, setDragging] = useState(false);
  const start = useRef({ x: 0, width: 0 });
  return (
    <div
      role="separator"
      aria-orientation="vertical"
      aria-label="Resize details"
      aria-valuenow={width}
      tabIndex={0}
      data-dragging={dragging}
      title="Drag to resize · double-click to reset"
      onPointerDown={event => {
        event.preventDefault();
        event.currentTarget.setPointerCapture(event.pointerId);
        start.current = { x: event.clientX, width };
        setDragging(true);
      }}
      onPointerMove={event => {
        if (dragging) onResize(start.current.width + start.current.x - event.clientX);
      }}
      onPointerUp={() => setDragging(false)}
      onPointerCancel={() => setDragging(false)}
      onDoubleClick={() => onResize(PANEL_DEFAULT)}
      onKeyDown={event => {
        if (event.key === 'ArrowLeft' || event.key === 'ArrowRight') {
          event.preventDefault();
          onResize(width + (event.key === 'ArrowLeft' ? 32 : -32));
        }
      }}
      className={cn(
        'w-1.5 shrink-0 cursor-col-resize border-l bg-muted/40 outline-none',
        'hover:bg-primary/30 focus-visible:bg-primary/40 data-[dragging=true]:bg-primary/50'
      )}
    />
  );
}

function TableDetail({
  table,
  onOpen,
  onClose,
}: {
  table: CatalogTable;
  onOpen: (table: string) => void;
  onClose: () => void;
}) {
  const dates = tableDates(table);
  return (
    <div className="flex h-full min-h-0 flex-col">
      <div className="flex h-11 shrink-0 items-center gap-2 border-b px-3">
        <span className="truncate font-mono text-[13px] font-medium" title={table.table}>
          {table.table}
        </span>
        <Badge variant="outline">{table.kind}</Badge>
        <Button size="sm" variant="secondary" className="ml-auto" onClick={() => onOpen(table.table)}>
          <Table2 />
          Open live
        </Button>
        <Button size="icon-sm" variant="ghost" aria-label="Close details" onClick={onClose}>
          <X />
        </Button>
      </div>
      <div className="min-h-0 flex-1 overflow-auto px-3 pb-3">
        {table.key.length > 0 && <Field label="Key">{table.key.join(', ')}</Field>}
        {table.sort.length > 0 && <Field label="Sorted by">{table.sort.join(', ')}</Field>}
        <h3 className="mt-4 mb-1 text-xs font-medium text-muted-foreground">
          Columns · {table.columns.length}
        </h3>
        <table className="w-full text-sm">
          <tbody>
            {table.columns.map(c => (
              <tr key={c.name} className="border-b last:border-b-0">
                <td className="py-1 pr-4 font-mono text-[13px]">{c.name}</td>
                <td className="py-1 text-xs text-muted-foreground">{c.type}</td>
              </tr>
            ))}
          </tbody>
        </table>
        <h3 className="mt-4 mb-1 text-xs font-medium text-muted-foreground">
          Dates · {dates.length}
        </h3>
        {dates.length === 0 ? (
          <p className="text-sm text-muted-foreground">No rows yet.</p>
        ) : (
          <table className="w-full text-sm tabular-nums">
            <thead>
              <tr className="text-left text-xs text-muted-foreground">
                <th className="py-1 pr-3 font-medium">Date</th>
                <th className="py-1 pr-3 text-right font-medium">weir rows</th>
                <th className="py-1 pr-3 text-right font-medium">weir</th>
                <th className="py-1 pr-3 text-right font-medium">history rows</th>
                <th className="py-1 text-right font-medium">history</th>
              </tr>
            </thead>
            <tbody>
              {dates.map(d => (
                <tr key={d.date} className="border-t">
                  <td className="py-1 pr-3">{d.date}</td>
                  <td className="py-1 pr-3 text-right">{d.weir ? count(d.weir.rows) : '—'}</td>
                  <td className="py-1 pr-3 text-right">{d.weir ? size(d.weir.bytes) : '—'}</td>
                  <td className="py-1 pr-3 text-right">
                    {d.history ? count(d.history.rows) : '—'}
                  </td>
                  <td
                    className="py-1 text-right"
                    title={d.history ? `${d.history.files} file(s)` : undefined}
                  >
                    {d.history ? size(d.history.bytes) : '—'}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}

/** Every stored table by namespace, with what each takes in weir and in history (§15.2) */
export function CatalogPage({ onOpen }: { onOpen: (table: string) => void }) {
  const { value: catalog, error } = usePolled('/api/catalog', 10_000, parseCatalog);
  const [query, setQuery] = useState('');
  const [selected, setSelected] = useState<string | null>(null);
  const [collapsed, setCollapsed] = useState<ReadonlySet<string>>(new Set());
  const [width, setWidth] = useState(
    () => Number(localStorage.getItem(PANEL_STORAGE)) || PANEL_DEFAULT
  );
  const page = useRef<HTMLDivElement>(null);
  const search = useRef<HTMLInputElement>(null);
  const close = useCallback(() => setSelected(null), []);

  const resize = useCallback((next: number) => {
    const available = page.current?.clientWidth ?? window.innerWidth;
    const clamped = Math.round(Math.max(PANEL_MIN, Math.min(next, available - PAGE_MIN)));
    setWidth(clamped);
    localStorage.setItem(PANEL_STORAGE, String(clamped));
  }, []);

  const today = utcDate(new Date());
  const tables = catalog?.tables ?? [];
  const shown = useMemo(
    () => tables.filter(t => matchesSearch(tableFields(t), query)),
    [tables, query]
  );
  const groups = useMemo(() => namespaces(shown, today), [shown, today]);
  const all = useMemo(() => totals(tables, today), [tables, today]);
  const detail = tables.find(t => t.table === selected) ?? null;

  // `/` searches; Escape leaves the search, else closes the details.
  useEffect(() => {
    const onKey = (event: KeyboardEvent) => {
      const typing =
        event.target instanceof HTMLElement &&
        event.target.closest('input, textarea, select, [contenteditable="true"]') != null;
      if (event.key === '/' && !typing && !event.metaKey && !event.ctrlKey) {
        event.preventDefault();
        search.current?.focus();
      } else if (event.key === 'Escape' && !typing && detail != null) {
        close();
      }
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [detail, close]);

  const toggle = (namespace: string) =>
    setCollapsed(current => {
      const next = new Set(current);
      if (!next.delete(namespace)) next.add(namespace);
      return next;
    });

  const volume = catalog?.volume;
  const used = volume ? volume.bytes - volume.free : 0;
  const spaces = new Set(tables.map(t => namespaceOf(t.table))).size;
  const plural = (n: number, word: string) => `${n} ${word}${n === 1 ? '' : 's'}`;
  return (
    <div ref={page} className="flex min-h-0 flex-1">
      <div className="flex min-w-0 flex-1 flex-col gap-3 p-4">
        <div className="flex flex-wrap items-center gap-x-5 gap-y-2">
          <div className="relative w-full max-w-sm">
            <Search className="pointer-events-none absolute top-1/2 left-2.5 size-4 -translate-y-1/2 text-muted-foreground" />
            <Input
              ref={search}
              value={query}
              onChange={event => setQuery(event.target.value)}
              onKeyDown={event => {
                if (event.key === 'Escape') {
                  setQuery('');
                  event.currentTarget.blur();
                }
              }}
              placeholder="Search tables and columns"
              aria-label="Search tables"
              className="h-8 pr-8 pl-8"
            />
            {query === '' ? (
              <kbd className="pointer-events-none absolute top-1/2 right-2 -translate-y-1/2 rounded border px-1.5 font-mono text-[11px] text-muted-foreground">
                /
              </kbd>
            ) : (
              <button
                type="button"
                aria-label="Clear search"
                onClick={() => setQuery('')}
                className="absolute top-1/2 right-2 -translate-y-1/2 text-muted-foreground hover:text-foreground"
              >
                <X className="size-4" />
              </button>
            )}
          </div>
          <span className="text-xs text-muted-foreground tabular-nums">
            {shown.length === tables.length
              ? `${plural(tables.length, 'table')} in ${plural(spaces, 'namespace')}`
              : `${shown.length} of ${tables.length} tables`}
            {' · '}
            {count(all.today)} rows today · history {size(all.historyBytes)}
          </span>
          {volume && (
            <span
              className="flex items-center gap-2 text-xs text-muted-foreground tabular-nums"
              title={`${bytes(volume.free)} free`}
            >
              weir volume
              <span className="h-1.5 w-24 overflow-hidden rounded-full bg-muted">
                <span
                  className={cn(
                    'block h-full',
                    used / volume.bytes > 0.85 ? 'bg-destructive' : 'bg-primary'
                  )}
                  style={{ width: `${(100 * used) / volume.bytes}%` }}
                />
              </span>
              {bytes(used)} of {bytes(volume.bytes)}
            </span>
          )}
        </div>
        {error != null && (
          <p className="rounded-md border border-destructive/30 bg-destructive/10 px-3 py-2 text-sm">
            Cannot read the catalog: {error}
          </p>
        )}
        <div className="min-h-0 flex-1 overflow-auto rounded-md border">
          <table className="w-full min-w-[75rem] table-fixed text-sm">
            <colgroup>
              {COLUMNS.map(c => (
                <col key={c.label} className={c.width} />
              ))}
            </colgroup>
            <thead className="sticky top-0 z-10 bg-background">
              <tr className="text-left text-xs text-muted-foreground shadow-[inset_0_-1px_0_var(--border)]">
                {COLUMNS.map(c => (
                  <th
                    key={c.label}
                    className={cn('py-2 pr-4 font-medium first:pl-3', c.numeric && 'text-right')}
                  >
                    {c.label}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {groups.map(group => {
                const open = query !== '' || !collapsed.has(group.namespace);
                return [
                  <tr
                    key={group.namespace}
                    onClick={() => toggle(group.namespace)}
                    aria-expanded={open}
                    className="cursor-pointer border-b bg-muted/40 font-medium hover:bg-accent/50"
                  >
                    <td className="truncate py-1 pr-4 pl-2">
                      <span className="flex items-center gap-1">
                        {open ? (
                          <ChevronDown className="size-4 text-muted-foreground" />
                        ) : (
                          <ChevronRight className="size-4 text-muted-foreground" />
                        )}
                        {group.namespace}
                        <span className="ml-1 text-xs font-normal text-muted-foreground">
                          {group.totals.tables}
                        </span>
                      </span>
                    </td>
                    <td />
                    <TotalsCells t={group.totals} />
                    <td />
                  </tr>,
                  ...(open
                    ? group.tables.map(t => {
                        const isSelected = t.table === selected;
                        return (
                          <tr
                            key={t.table}
                            aria-selected={isSelected}
                            onClick={() => setSelected(t.table)}
                            onDoubleClick={() => onOpen(t.table)}
                            className={cn(
                              'cursor-pointer border-b last:border-b-0 hover:bg-accent/50',
                              isSelected && 'bg-accent'
                            )}
                          >
                            <td className="truncate py-1 pr-4 pl-8 font-mono text-[13px]" title={t.table}>
                              {t.table.slice(group.namespace.length + 1)}
                            </td>
                            <td className="truncate py-1 pr-4 text-xs text-muted-foreground">
                              {t.kind}
                              {t.key.length > 0 && ` · ${t.key.join(', ')}`}
                            </td>
                            <TotalsCells t={totals([t], today)} />
                            <td className="py-1 pr-4 text-right tabular-nums">{t.columns.length}</td>
                          </tr>
                        );
                      })
                    : []),
                ];
              })}
            </tbody>
          </table>
          {catalog != null && shown.length === 0 && (
            <p className="px-3 py-4 text-sm text-muted-foreground">
              {tables.length > 0 ? `No table matches “${query}”.` : 'No tables yet.'}
            </p>
          )}
        </div>
      </div>
      {detail != null && (
        <>
          <Splitter width={width} onResize={resize} />
          <aside style={{ width }} className="shrink-0 bg-sidebar">
            <TableDetail key={detail.table} table={detail} onOpen={onOpen} onClose={close} />
          </aside>
        </>
      )}
    </div>
  );
}
