import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { AlertTriangle, Search, X } from 'lucide-react';
import type { KermodesConnection } from '@kermodes/client';
import { Input } from '@/components/ui/input';
import { cn } from '@/lib/utils';
import { DEPLOYMENTS_VIEW, useComponents, useDeployments, useProjects } from '@/deployments/data';
import { ComponentDetail, DeploymentDetail } from '@/deployments/Details';
import {
  ago,
  bytes,
  clampPanel,
  cores,
  deploymentFields,
  key,
  matchesSearch,
} from '@/deployments/model';
import { Commit, Status, Usage } from '@/deployments/parts';
import { SystemStrip } from '@/deployments/SystemStrip';

export { DEPLOYMENTS_VIEW };

type Selected = { kind: 'deployment'; key: string } | { kind: 'component'; name: string };

const PANEL_STORAGE = 'kermodes.deployments.panel-height';
const PANEL_DEFAULT = 380;

/** The details panel's height as the user last left it */
function useStoredHeight() {
  const [height, setHeight] = useState(
    () => Number(localStorage.getItem(PANEL_STORAGE)) || PANEL_DEFAULT
  );
  const store = useCallback((next: number) => {
    setHeight(next);
    localStorage.setItem(PANEL_STORAGE, String(next));
  }, []);
  return [height, store] as const;
}

/** An element's height, kept current as it resizes */
function useHeight(ref: React.RefObject<HTMLElement | null>) {
  const [height, setHeight] = useState(0);
  useEffect(() => {
    const element = ref.current;
    if (element == null) return;
    const observer = new ResizeObserver(([entry]) => setHeight(entry.contentRect.height));
    observer.observe(element);
    return () => observer.disconnect();
  }, [ref]);
  return height;
}

/** A handle between the page and the panel below it: drag, arrow keys, or double-click to reset */
function Splitter({
  height,
  onResize,
}: {
  height: number;
  onResize: (height: number) => void;
}) {
  const [dragging, setDragging] = useState(false);
  const start = useRef({ y: 0, height: 0 });
  return (
    <div
      role="separator"
      aria-orientation="horizontal"
      aria-label="Resize details"
      aria-valuenow={height}
      tabIndex={0}
      data-dragging={dragging}
      title="Drag to resize · double-click to reset"
      onPointerDown={event => {
        event.preventDefault();
        event.currentTarget.setPointerCapture(event.pointerId);
        start.current = { y: event.clientY, height };
        setDragging(true);
      }}
      onPointerMove={event => {
        if (dragging) {
          onResize(start.current.height + start.current.y - event.clientY);
        }
      }}
      onPointerUp={() => setDragging(false)}
      onPointerCancel={() => setDragging(false)}
      onDoubleClick={() => onResize(PANEL_DEFAULT)}
      onKeyDown={event => {
        if (event.key === 'ArrowUp' || event.key === 'ArrowDown') {
          event.preventDefault();
          onResize(height + (event.key === 'ArrowUp' ? 32 : -32));
        }
      }}
      className={cn(
        'group relative h-1.5 shrink-0 cursor-row-resize border-t bg-muted/40 outline-none',
        'hover:bg-primary/30 focus-visible:bg-primary/40 data-[dragging=true]:bg-primary/50'
      )}
    >
      <span className="absolute top-1/2 left-1/2 h-0.5 w-10 -translate-x-1/2 -translate-y-1/2 rounded-full bg-muted-foreground/40 group-hover:bg-primary" />
    </div>
  );
}

/** Widths fixed so that values changing, or the panel opening, never move a column */
const COLUMNS: { label: string; width: string }[] = [
  { label: 'Deployment', width: 'w-[18rem]' },
  { label: 'Project', width: 'w-[9rem]' },
  { label: 'Status', width: 'w-[8.5rem]' },
  { label: 'Revision', width: 'w-[10rem]' },
  { label: 'CPU', width: 'w-[10.5rem]' },
  { label: 'Memory', width: 'w-[10.5rem]' },
  { label: 'Restarts', width: 'w-[5.5rem]' },
  { label: 'Up', width: 'w-[6.5rem]' },
  { label: 'Node', width: 'w-[9rem]' },
  { label: 'Last warning', width: '' },
];

export function DeploymentsPage({ connection }: { connection: KermodesConnection }) {
  const published = connection.pages.includes(DEPLOYMENTS_VIEW);
  const { rows, history } = useDeployments(connection, published);
  const projects = useProjects();
  const system = useComponents();
  const [selected, setSelected] = useState<Selected | null>(null);
  const [query, setQuery] = useState('');
  const search = useRef<HTMLInputElement>(null);
  const page = useRef<HTMLDivElement>(null);
  const table = useRef<HTMLDivElement>(null);
  const available = useHeight(page);
  const [height, setHeight] = useStoredHeight();
  const close = useCallback(() => setSelected(null), []);

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
  const shown = useMemo(
    () => rows.filter(d => matchesSearch(deploymentFields(d), query)),
    [rows, query]
  );

  const deployment =
    selected?.kind === 'deployment' ? (rows.find(d => key(d) === selected.key) ?? null) : null;
  const component =
    selected?.kind === 'component'
      ? (system.components?.find(c => c.name === selected.name) ?? null)
      : null;
  const open = deployment != null || component != null;

  // `/` searches; Escape leaves the search, else closes the details.
  useEffect(() => {
    const onKey = (event: KeyboardEvent) => {
      const typing =
        event.target instanceof HTMLElement &&
        event.target.closest('input, textarea, select, [contenteditable="true"]') != null;
      if (event.key === '/' && !typing && !event.metaKey && !event.ctrlKey) {
        event.preventDefault();
        search.current?.focus();
      } else if (event.key === 'Escape' && !typing && open) {
        close();
      }
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [open, close]);

  // The chosen row stays in sight as the panel opens beneath it.
  const selectedKey = selected?.kind === 'deployment' ? selected.key : null;
  useEffect(() => {
    if (selectedKey == null) return;
    const frame = requestAnimationFrame(() =>
      table.current
        ?.querySelector('[aria-selected="true"]')
        ?.scrollIntoView({ block: 'nearest' })
    );
    return () => cancelAnimationFrame(frame);
  }, [selectedKey]);

  const panelHeight = clampPanel(height, available);
  return (
    <div ref={page} className="flex min-h-0 flex-1 flex-col">
      <div className="flex min-h-0 flex-1 flex-col gap-3 px-4 pt-4">
        <SystemStrip
          components={system.components}
          error={system.error}
          history={system.history}
          selected={selected?.kind === 'component' ? selected.name : null}
          onSelect={name => setSelected({ kind: 'component', name })}
        />
        {failed.map(p => (
          <div
            key={p.name}
            className="flex gap-2 rounded-md border border-destructive/30 bg-destructive/10 px-4 py-3 text-sm"
          >
            <AlertTriangle className="mt-0.5 size-4 shrink-0 text-destructive" />
            <div className="min-w-0">
              <div>
                <span className="font-medium">{p.name}</span>: commit{' '}
                <Commit commit={p.failed!.commit} /> failed discovery, so it is not rolled out.
              </div>
              <pre className="mt-1 overflow-x-auto font-mono text-xs whitespace-pre-wrap text-muted-foreground">
                {p.failed!.error}
              </pre>
            </div>
          </div>
        ))}
        <div className="flex items-center gap-3">
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
              placeholder="Search deployments"
              aria-label="Search deployments"
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
            {shown.length === rows.length
              ? `${rows.length} ${rows.length === 1 ? 'deployment' : 'deployments'}`
              : `${shown.length} of ${rows.length}`}
          </span>
        </div>
        <div ref={table} className="min-h-0 flex-1 overflow-auto rounded-md border">
          <table className="w-full min-w-[96rem] table-fixed text-sm">
            <colgroup>
              {COLUMNS.map(c => (
                <col key={c.label} className={c.width} />
              ))}
            </colgroup>
            <thead className="sticky top-0 z-10 bg-background">
              <tr className="text-left text-xs text-muted-foreground shadow-[inset_0_-1px_0_var(--border)]">
                {COLUMNS.map(c => (
                  <th key={c.label} className="py-2 pr-4 font-medium first:pl-3">
                    {c.label}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {shown.map(d => {
                const samples = history.get(key(d)) ?? [];
                const isSelected = selected?.kind === 'deployment' && selected.key === key(d);
                return (
                  <tr
                    key={key(d)}
                    aria-selected={isSelected}
                    onClick={() => setSelected({ kind: 'deployment', key: key(d) })}
                    className={cn(
                      'cursor-pointer border-b last:border-b-0 hover:bg-accent/50',
                      isSelected && 'bg-accent'
                    )}
                  >
                    <td className="truncate py-2 pr-4 pl-3">
                      <div className="truncate font-mono text-[13px] font-medium" title={d.name}>
                        {d.name}
                      </div>
                      <div className="text-xs text-muted-foreground">{d.kind}</div>
                    </td>
                    <td className="truncate py-2 pr-4" title={d.project}>
                      {d.project}
                    </td>
                    <td className="py-2 pr-4">
                      <Status status={d.status} />
                    </td>
                    <td className="py-2 pr-4">
                      <Commit commit={d.commit} href={sources.get(key(d))} />
                      {d.available != null && (
                        <div className="truncate text-xs text-warning">
                          {d.update === 'manual' ? 'update available' : 'rolling to'}{' '}
                          {d.available.slice(0, 7)}
                        </div>
                      )}
                    </td>
                    <td className="py-2 pr-4">
                      <Usage
                        value={d.cpu}
                        limit={d.cpu_limit}
                        samples={samples.map(s => s.cpu)}
                        format={cores}
                      />
                    </td>
                    <td className="py-2 pr-4">
                      <Usage
                        value={d.memory}
                        limit={d.memory_limit}
                        samples={samples.map(s => s.memory)}
                        format={bytes}
                      />
                    </td>
                    <td className={cn('py-2 pr-4 tabular-nums', d.restarts > 0 && 'text-warning')}>
                      {d.restarts}
                    </td>
                    <td className="py-2 pr-4 tabular-nums">{ago(d.started)}</td>
                    <td className="truncate py-2 pr-4 text-xs text-muted-foreground" title={d.node ?? undefined}>
                      {d.node ?? '—'}
                    </td>
                    <td className="truncate py-2 pr-3 text-xs" title={d.event ?? undefined}>
                      {d.event == null ? (
                        <span className="text-muted-foreground">—</span>
                      ) : (
                        <span className="text-destructive">{d.event}</span>
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
                ? `No deployment matches “${query}”.`
                : published
                  ? 'No deployments yet.'
                  : 'Waiting for sys.deployments…'}
            </p>
          )}
        </div>
      </div>
      {open ? (
        <>
          <Splitter height={panelHeight} onResize={next => setHeight(clampPanel(next, available))} />
          <div style={{ height: panelHeight }} className="shrink-0">
            {deployment != null && (
              <DeploymentDetail
                key={key(deployment)}
                deployment={deployment}
                source={sources.get(key(deployment)) ?? null}
                onClose={close}
              />
            )}
            {component != null && (
              <ComponentDetail key={component.name} component={component} onClose={close} />
            )}
          </div>
        </>
      ) : (
        <div className="h-4 shrink-0" />
      )}
    </div>
  );
}
