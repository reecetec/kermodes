import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { AlertTriangle, Play, RotateCcw, Rocket, Search, Square, X } from 'lucide-react';
import type { KermodesConnection } from '@kermodes/client';
import { Clipped } from '@/components/Clipped';
import { ListColGroup, ListHeader } from '@/components/ListHeader';
import { type ListSort, type SortValue, nextSort, sortRows, useColumnWidths } from '@/components/list';
import { SidePanel } from '@/components/SidePanel';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Switch } from '@/components/ui/switch';
import { cn } from '@/lib/utils';
import type { Catalog } from '@/catalog/model';
import {
  DEPLOYMENTS_VIEW,
  usePolled,
  useComponents,
  useDeployments,
  useProjects,
} from '@/deployments/data';
import { RowDetail } from '@/deployments/Details';
import { type Disk, ago, bytes, cores } from '@/deployments/model';
import { Commit, DiskUse, TONE_FILL, Usage } from '@/deployments/parts';
import {
  type Filters,
  type Row,
  type Tone,
  NO_FILTERS,
  SYSTEM,
  choices,
  counts,
  matches,
  tableRows,
  toneOf,
} from '@/deployments/rows';

export { DEPLOYMENTS_VIEW };

const parseCatalog = (r: Response) => r.json() as Promise<Catalog>;

/**
 * The columns, their widths in px (fixed, so values changing or the panel resizing never move
 * a column; a person may drag them), and what each sorts by.
 */
const COLUMNS: { label: string; width: number; numeric?: boolean; value: (r: Row) => SortValue }[] = [
  { label: 'Name', width: 240, value: r => r.name },
  { label: 'Project', width: 128, value: r => r.project },
  { label: 'Kind', width: 112, value: r => r.kind },
  { label: 'Status', width: 136, value: r => r.status },
  { label: 'Revision', width: 136, value: r => r.revision },
  { label: 'CPU', width: 136, value: r => r.cpu },
  { label: 'Memory', width: 136, value: r => r.memory },
  {
    label: 'Disk',
    width: 192,
    value: r => {
      const disk = fullest(r.disks);
      return disk == null ? null : disk.size == null ? disk.used : disk.used / disk.size;
    },
  },
  { label: 'Restarts', width: 72, value: r => r.restarts },
  // Longest up first when descending: the earliest start
  { label: 'Up', width: 88, value: r => (r.started == null ? null : -r.started) },
  { label: 'Node', width: 128, value: r => r.node },
  { label: 'Last warning', width: 288, value: r => r.warning },
];
const DEFAULT_WIDTHS = COLUMNS.map(c => c.width);

const TONE_LABEL: Record<Tone, string> = {
  ok: 'running',
  busy: 'changing',
  bad: 'failing',
  idle: 'stopped',
};

async function post(url: string): Promise<string | null> {
  const response = await fetch(url, { method: 'POST' }).catch(e => e as Error);
  if (response instanceof Error) return String(response);
  if (response.ok) return null;
  const text = await response.text();
  try {
    return JSON.parse(text).error ?? text;
  } catch {
    return text || `${response.status} ${response.statusText}`;
  }
}

/** The disk nearest full, which the table shows; every disk is in its title */
function fullest(disks: readonly Disk[]): Disk | null {
  const share = (d: Disk) => (d.size ? d.used / d.size : 0);
  return disks.reduce<Disk | null>((a, d) => (a == null || share(d) > share(a) ? d : a), null);
}

/** Roll out, Restart, Stop or Start for the selected deployment; the platform's own rows are the Helm release's */
function Actions({ row, onDone }: { row: Row | null; onDone: (error: string | null) => void }) {
  const [busy, setBusy] = useState<string | null>(null);
  const d = row?.source.type === 'deployment' ? row.source.deployment : null;
  const managed = row != null && row.project === SYSTEM;
  const stopped = d != null && (d.status === 'Stopped' || d.status === 'Stopping');
  const act = async (action: 'rollout' | 'restart' | 'stop' | 'start') => {
    if (d == null) return;
    setBusy(action);
    const path = `/api/deployments/${encodeURIComponent(d.project)}/${encodeURIComponent(d.name)}`;
    onDone(await post(`${path}/${action}`));
    setBusy(null);
  };
  const off = d == null || busy != null;
  const why = managed
    ? 'Managed by the Helm release'
    : d == null
      ? 'Select a deployment'
      : undefined;
  return (
    <div className="flex items-center gap-1.5" title={why}>
      {d?.available != null && d.update === 'manual' && (
        <Button size="sm" variant="outline" disabled={off} onClick={() => void act('rollout')}>
          <Rocket />
          Roll out {d.available.slice(0, 7)}
        </Button>
      )}
      <Button size="sm" variant="outline" disabled={off || stopped} onClick={() => void act('restart')}>
        <RotateCcw />
        Restart
      </Button>
      <Button
        size="sm"
        variant="outline"
        disabled={off}
        onClick={() => void act(stopped ? 'start' : 'stop')}
      >
        {stopped ? <Play /> : <Square />}
        {stopped ? 'Start' : 'Stop'}
      </Button>
    </div>
  );
}

/** Radix reserves the empty value, so "any" travels as this and is '' outside the menu */
const ANY = '__any';

function Choice({
  label,
  value,
  options,
  onChange,
}: {
  label: string;
  value: string;
  options: string[];
  onChange: (value: string) => void;
}) {
  return (
    <Select
      value={value === '' ? ANY : value}
      onValueChange={next => onChange(next === ANY ? '' : next)}
    >
      <SelectTrigger
        size="sm"
        aria-label={label}
        className={cn(value === '' ? 'text-muted-foreground' : 'text-foreground')}
      >
        <SelectValue />
      </SelectTrigger>
      <SelectContent>
        <SelectItem value={ANY}>{label}: any</SelectItem>
        {options.map(o => (
          <SelectItem key={o} value={o}>
            {label}: {o}
          </SelectItem>
        ))}
      </SelectContent>
    </Select>
  );
}

export function DeploymentsPage({ connection }: { connection: KermodesConnection }) {
  const published = connection.pages.includes(DEPLOYMENTS_VIEW);
  const { rows: deployments, history } = useDeployments(connection, published);
  const projects = useProjects();
  const system = useComponents();
  const catalog = usePolled('/api/catalog', 30_000, parseCatalog).value;
  const [selected, setSelected] = useState<string | null>(null);
  const [filters, setFilters] = useState<Filters>(NO_FILTERS);
  const [error, setError] = useState<string | null>(null);
  const search = useRef<HTMLInputElement>(null);
  const close = useCallback(() => setSelected(null), []);
  const set = (change: Partial<Filters>) => setFilters(current => ({ ...current, ...change }));

  const sources = useMemo(() => {
    const links = new Map<string, string | null>();
    for (const project of projects) {
      for (const resource of project.resources ?? []) {
        links.set(`${project.name}/${resource.name}`, resource.source);
      }
    }
    return links;
  }, [projects]);
  const failed = projects.filter(p => p.failed != null);
  const rows = useMemo(
    () => tableRows(deployments, system.components, catalog),
    [deployments, system.components, catalog]
  );
  const [sort, setSort] = useState<ListSort>(null);
  const [widths, setWidth] = useColumnWidths('deployments', DEFAULT_WIDTHS);
  const shown = useMemo(
    () =>
      sortRows(
        rows.filter(r => matches(r, filters)),
        sort,
        (r, column) => COLUMNS[column].value(r)
      ),
    [rows, filters, sort]
  );
  const tally = counts(rows);
  const row = rows.find(r => r.key === selected) ?? null;
  const filtered = JSON.stringify(filters) !== JSON.stringify(NO_FILTERS);

  // `/` searches; Escape leaves the search, else closes the details.
  useEffect(() => {
    const onKey = (event: KeyboardEvent) => {
      const typing =
        event.target instanceof HTMLElement &&
        event.target.closest('input, textarea, select, [contenteditable="true"]') != null;
      if (event.key === '/' && !typing && !event.metaKey && !event.ctrlKey) {
        event.preventDefault();
        search.current?.focus();
      } else if (event.key === 'Escape' && !typing && row != null) {
        close();
      }
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [row, close]);

  const samplesOf = (r: Row) =>
    (r.source.type === 'component'
      ? system.history.get(r.source.component.name)
      : history.get(r.key)) ?? [];

  return (
    <div className="flex min-h-0 flex-1">
      <div className="flex min-w-0 flex-1 flex-col gap-2 p-3">
        <div className="flex flex-wrap items-center gap-2">
          {(['ok', 'busy', 'bad', 'idle'] as const).map(tone => (
            <span
              key={tone}
              className={cn(
                'rounded px-2 py-0.5 text-xs font-medium tabular-nums',
                tally[tone] > 0 ? TONE_FILL[tone] : 'bg-muted/50 text-muted-foreground'
              )}
            >
              {tally[tone]} {TONE_LABEL[tone]}
            </span>
          ))}
          {system.error != null && (
            <span className="text-xs text-destructive">Platform: {system.error}</span>
          )}
          <div className="ml-auto">
            <Actions row={row} onDone={setError} />
          </div>
        </div>
        {error != null && (
          <p className="flex items-center gap-2 rounded-md border border-destructive/30 bg-destructive/10 px-3 py-1.5 text-sm">
            {error}
            <button type="button" className="ml-auto" aria-label="Dismiss" onClick={() => setError(null)}>
              <X className="size-4" />
            </button>
          </p>
        )}
        <div className="flex flex-wrap items-center gap-2">
          <div className="relative w-72">
            <Search className="pointer-events-none absolute top-1/2 left-2.5 size-4 -translate-y-1/2 text-muted-foreground" />
            <Input
              ref={search}
              value={filters.query}
              onChange={event => set({ query: event.target.value })}
              onKeyDown={event => {
                if (event.key === 'Escape') {
                  set({ query: '' });
                  event.currentTarget.blur();
                }
              }}
              placeholder="Search"
              aria-label="Search deployments"
              className="h-8 pr-8 pl-8"
            />
            {filters.query === '' ? (
              <kbd className="pointer-events-none absolute top-1/2 right-2 -translate-y-1/2 rounded border px-1.5 font-mono text-[11px] text-muted-foreground">
                /
              </kbd>
            ) : (
              <button
                type="button"
                aria-label="Clear search"
                onClick={() => set({ query: '' })}
                className="absolute top-1/2 right-2 -translate-y-1/2 text-muted-foreground hover:text-foreground"
              >
                <X className="size-4" />
              </button>
            )}
          </div>
          <Choice label="Project" value={filters.project} options={choices(rows, 'project')} onChange={project => set({ project })} />
          <Choice label="Status" value={filters.status} options={choices(rows, 'status')} onChange={status => set({ status })} />
          <Choice label="Kind" value={filters.kind} options={choices(rows, 'kind')} onChange={kind => set({ kind })} />
          <Choice label="Node" value={filters.node} options={choices(rows, 'node')} onChange={node => set({ node })} />
          <label className="flex items-center gap-2 text-xs text-muted-foreground">
            <Switch
              checked={filters.hideStopped}
              onCheckedChange={hideStopped => set({ hideStopped })}
            />
            Hide stopped
          </label>
          {filtered && (
            <Button size="sm" variant="ghost" onClick={() => setFilters(NO_FILTERS)}>
              Clear
            </Button>
          )}
          <span className="ml-auto text-xs text-muted-foreground tabular-nums">
            {shown.length === rows.length ? `${rows.length} rows` : `${shown.length} of ${rows.length}`}
          </span>
        </div>
        {failed.map(p => (
          <div
            key={p.name}
            className="flex gap-2 rounded-md border border-destructive/30 bg-destructive/10 px-3 py-2 text-sm"
          >
            <AlertTriangle className="mt-0.5 size-4 shrink-0 text-destructive" />
            <div className="min-w-0">
              <span className="font-medium">{p.name}</span>: commit <Commit commit={p.failed!.commit} />{' '}
              failed discovery, so it is not rolled out.
              <pre className="mt-1 overflow-x-auto font-mono text-xs whitespace-pre-wrap text-muted-foreground">
                {p.failed!.error}
              </pre>
            </div>
          </div>
        ))}
        <div className="min-h-0 flex-1 overflow-auto rounded-md border">
          <table
            className="table-fixed text-[13px]"
            style={{ width: widths.reduce((sum, w) => sum + w, 0) }}
          >
            <ListColGroup widths={widths} />
            <ListHeader
              columns={COLUMNS}
              widths={widths}
              onResize={setWidth}
              sort={sort}
              onSort={column => setSort(current => nextSort(current, column))}
              cellClassName="h-7 pr-3 first:pl-2"
            />
            <tbody>
              {shown.map(r => {
                const isSelected = r.key === selected;
                const samples = samplesOf(r);
                const disk = fullest(r.disks);
                return (
                  <tr
                    key={r.key}
                    aria-selected={isSelected}
                    onClick={() => setSelected(r.key)}
                    className={cn(
                      'h-6 cursor-pointer border-b hover:bg-accent/50',
                      isSelected && 'bg-accent',
                      r.project === SYSTEM && 'bg-muted/25'
                    )}
                  >
                    <td className="pr-3 pl-2">
                      <Clipped value={r.name} label="Name" className="font-mono font-medium" />
                    </td>
                    <td className="truncate pr-3 text-muted-foreground">{r.project}</td>
                    <td className="truncate pr-3 text-muted-foreground">{r.kind}</td>
                    <td className={cn('truncate px-2 font-medium', TONE_FILL[toneOf(r.status)])} title={r.status}>
                      {r.status}
                    </td>
                    <td className="truncate pr-3 pl-2">
                      {r.source.type === 'deployment' ? (
                        <>
                          <Commit commit={r.revision} href={sources.get(r.key)} />
                          {r.source.deployment.available != null && (
                            <span className="ml-1 text-xs text-warning" title={`update ${r.source.deployment.available}`}>
                              ↑
                            </span>
                          )}
                        </>
                      ) : (
                        <Clipped value={r.revision ?? '—'} label="Revision" className="font-mono text-xs" />
                      )}
                    </td>
                    <td className="pr-3">
                      {r.cpu == null ? (
                        <span className="text-muted-foreground">—</span>
                      ) : (
                        <Usage value={r.cpu} limit={r.cpuLimit} samples={samples.map(s => s.cpu)} format={cores} width={52} />
                      )}
                    </td>
                    <td className="pr-3">
                      {r.memory == null ? (
                        <span className="text-muted-foreground">—</span>
                      ) : (
                        <Usage value={r.memory} limit={r.memoryLimit} samples={samples.map(s => s.memory)} format={bytes} width={52} />
                      )}
                    </td>
                    <td
                      className="truncate pr-3"
                      title={r.disks.map(d => `${d.name} ${bytes(d.used)}${d.size ? ` of ${bytes(d.size)}` : ''}`).join(' · ')}
                    >
                      {disk == null ? (
                        <span className="text-muted-foreground">—</span>
                      ) : (
                        <DiskUse name={disk.name} used={disk.used} size={disk.size} compact />
                      )}
                    </td>
                    <td className={cn('pr-3 tabular-nums', r.restarts > 0 && 'text-warning')}>{r.restarts}</td>
                    <td className="pr-3 tabular-nums">{ago(r.started)}</td>
                    <td className="pr-3">
                      <Clipped value={r.node ?? '—'} label="Node" className="text-xs text-muted-foreground" />
                    </td>
                    <td className="pr-2">
                      {r.warning == null ? (
                        <span className="text-xs text-muted-foreground">—</span>
                      ) : (
                        <Clipped value={r.warning} label="Warning" className="text-xs text-destructive" />
                      )}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
          {shown.length === 0 && (
            <p className="px-3 py-4 text-sm text-muted-foreground">
              {rows.length > 0
                ? 'Nothing matches these filters.'
                : published
                  ? 'No deployments yet.'
                  : 'Waiting for sys.deployments…'}
            </p>
          )}
        </div>
      </div>
      <SidePanel name="deployments">
        {fold =>
          row == null ? (
            <div className="flex h-full flex-col">
              <div className="flex h-11 shrink-0 items-center justify-end border-b px-3">{fold}</div>
              <p className="p-4 text-sm text-muted-foreground">
                Select a row to see its summary, logs, events, and settings.
              </p>
            </div>
          ) : (
            <RowDetail row={row} source={sources.get(row.key) ?? null} fold={fold} onClose={close} />
          )
        }
      </SidePanel>
    </div>
  );
}
