import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  AlertTriangle,
  GitCommitHorizontal,
  Play,
  RotateCcw,
  Rocket,
  ServerCog,
  Square,
  X,
} from 'lucide-react';
import { tableFromIPC } from 'apache-arrow';
import type { KermodesConnection } from '@kermodes/client';
import { toBytes } from '@kermodes/grid';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { ScrollArea } from '@/components/ui/scroll-area';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { cn } from '@/lib/utils';

/** The view of `sys.deployments` the UI's server publishes for this page: each deployment's newest row */
export const DEPLOYMENTS_VIEW = 'deployments';

/** A row of `sys.deployments`, as `kermodes control` reports it every few seconds */
export type Deployment = {
  ts: number;
  project: string;
  name: string;
  kind: string;
  status: string;
  update: string;
  commit: string | null;
  available: string | null;
  node: string | null;
  started: number | null;
  restarts: number;
  cpu: number;
  cpu_limit: number;
  cpu_pct: number;
  memory: number;
  memory_limit: number;
  memory_pct: number;
  event: string | null;
  event_at: number | null;
};

type Project = {
  name: string;
  failed: { commit: string; error: string } | null;
};

const key = (d: { project: string; name: string }) => `${d.project}/${d.name}`;

// Arrow gives int64 as bigint and timestamps as milliseconds
const num = (value: unknown) => (value == null ? null : Number(value));

/** Samples of CPU and memory kept per deployment for the sparklines: about 5 minutes */
const HISTORY = 60;

/** The deployments' newest rows, live, and a short history of their usage */
function useDeployments(connection: KermodesConnection, published: boolean) {
  const [rows, setRows] = useState<Deployment[]>([]);
  const history = useRef(new Map<string, { ts: number; cpu: number; memory: number }[]>());
  useEffect(() => {
    if (!published) {
      return;
    }
    const view = connection.open(DEPLOYMENTS_VIEW);
    view.send({
      type: 'sort',
      by: [
        { column: 'project', descending: false },
        { column: 'name', descending: false },
      ],
    });
    view.send({ type: 'viewport', top: 0, bottom: 9_999 });
    const stop = view.onMessage((header, buffers) => {
      if (header.type !== 'rows') {
        return;
      }
      const table = tableFromIPC(toBytes(buffers[0]));
      const next = table.toArray().map(row => {
        const r = row.toJSON() as Record<string, unknown>;
        return {
          ...(r as Omit<Deployment, 'ts' | 'started' | 'event_at' | 'restarts' | 'memory'>),
          ts: num(r.ts) ?? 0,
          started: num(r.started),
          event_at: num(r.event_at),
          restarts: num(r.restarts) ?? 0,
          memory: num(r.memory) ?? 0,
          memory_limit: num(r.memory_limit) ?? 0,
        } as Deployment;
      });
      for (const d of next) {
        const samples = history.current.get(key(d)) ?? [];
        if (samples.at(-1)?.ts !== d.ts) {
          samples.push({ ts: d.ts, cpu: d.cpu_pct, memory: d.memory_pct });
          history.current.set(key(d), samples.slice(-HISTORY));
        }
      }
      setRows(next);
    });
    return () => {
      stop();
      view.close();
    };
  }, [connection, published]);
  return { rows, history: history.current };
}

/** Projects with a commit that failed discovery, polled from the controller */
function useFailedProjects(): Project[] {
  const [projects, setProjects] = useState<Project[]>([]);
  useEffect(() => {
    const load = async () => {
      const response = await fetch('/api/projects').catch(() => null);
      if (response?.ok) {
        setProjects((await response.json()).projects);
      }
    };
    void load();
    const timer = setInterval(() => void load(), 10_000);
    return () => clearInterval(timer);
  }, []);
  return projects.filter(p => p.failed != null);
}

const STATUS_VARIANT: Record<string, 'success' | 'warning' | 'destructive' | 'outline'> = {
  Running: 'success',
  'Rolling out': 'warning',
  Starting: 'warning',
  'Not running': 'outline',
  Stopped: 'outline',
  Stopping: 'warning',
};

function Status({ status }: { status: string }) {
  return <Badge variant={STATUS_VARIANT[status] ?? 'destructive'}>{status}</Badge>;
}

function Commit({ commit }: { commit: string | null }) {
  return (
    <span className="inline-flex items-center gap-1 font-mono text-[13px]">
      {commit != null && <GitCommitHorizontal className="size-3.5 text-muted-foreground" />}
      {commit == null ? '—' : commit.slice(0, 7)}
    </span>
  );
}

function bytes(n: number): string {
  if (n >= 1 << 30) return `${(n / (1 << 30)).toFixed(1)} GiB`;
  if (n >= 1 << 20) return `${Math.round(n / (1 << 20))} MiB`;
  return `${Math.round(n / 1024)} KiB`;
}

function cores(n: number): string {
  return n >= 1 ? `${n.toFixed(2)} cores` : `${Math.round(n * 1000)}m`;
}

function ago(ms: number | null): string {
  if (ms == null) return '—';
  const s = Math.max(0, Math.round((Date.now() - ms) / 1000));
  if (s < 60) return `${s}s`;
  if (s < 3600) return `${Math.floor(s / 60)}m`;
  if (s < 86_400) return `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m`;
  return `${Math.floor(s / 86_400)}d ${Math.floor((s % 86_400) / 3600)}h`;
}

/** A share of a deployment's size, as a bar with its history behind it */
function Usage({
  pct,
  samples,
  label,
}: {
  pct: number;
  samples: number[];
  label: string;
}) {
  const width = 96;
  const height = 22;
  const points = samples
    .map((v, i) => {
      const x = samples.length < 2 ? width : (i / (samples.length - 1)) * width;
      const y = height - (Math.min(v, 100) / 100) * height;
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    })
    .join(' ');
  const tone = pct >= 90 ? 'text-destructive' : pct >= 70 ? 'text-warning' : 'text-success';
  return (
    <div className="flex items-center gap-2" title={label}>
      <svg width={width} height={height} className={cn('shrink-0', tone)} aria-hidden>
        <rect x={0} y={0} width={width} height={height} className="fill-muted/40" rx={3} />
        <rect
          x={0}
          y={height - 3}
          width={(Math.min(pct, 100) / 100) * width}
          height={3}
          className="fill-current opacity-60"
          rx={1.5}
        />
        {samples.length > 1 && (
          <polyline points={points} fill="none" stroke="currentColor" strokeWidth={1.25} />
        )}
      </svg>
      <span className="w-11 text-right font-mono text-xs tabular-nums">{pct.toFixed(0)}%</span>
    </div>
  );
}

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

type Event = {
  type: string;
  reason: string | null;
  message: string | null;
  count: number | null;
  at_ns: number;
  object: string;
};

function usePolled<T>(url: string, every: number, parse: (r: Response) => Promise<T>) {
  const [value, setValue] = useState<T | null>(null);
  const [error, setError] = useState<string | null>(null);
  useEffect(() => {
    let live = true;
    const load = async () => {
      const response = await fetch(url).catch(e => e as Error);
      if (!live) return;
      if (response instanceof Error) {
        setError(String(response));
      } else if (!response.ok) {
        setError((await response.text()) || `${response.status}`);
      } else {
        setError(null);
        setValue(await parse(response));
      }
    };
    void load();
    const timer = setInterval(() => void load(), every);
    return () => {
      live = false;
      clearInterval(timer);
    };
  }, [url, every, parse]);
  return { value, error };
}

const parseJson = (r: Response) => r.json() as Promise<Event[]>;
const parseText = (r: Response) => r.text();

function Events({ path }: { path: string }) {
  const { value, error } = usePolled(`${path}/events`, 5000, parseJson);
  if (error != null) return <p className="p-4 text-sm text-destructive">{error}</p>;
  if (value == null) return <p className="p-4 text-sm text-muted-foreground">Loading…</p>;
  if (value.length === 0) return <p className="p-4 text-sm text-muted-foreground">No events.</p>;
  return (
    <table className="w-full text-sm">
      <tbody>
        {value.map((e, i) => (
          <tr key={i} className="border-b align-top">
            <td className="py-2 pr-3 pl-4 whitespace-nowrap text-muted-foreground">
              {ago(e.at_ns / 1e6)} ago
            </td>
            <td className="py-2 pr-3">
              <span className={cn('font-medium', e.type === 'Warning' && 'text-destructive')}>
                {e.reason}
              </span>
              {(e.count ?? 1) > 1 && (
                <span className="text-muted-foreground"> ×{e.count}</span>
              )}
              <div className="font-mono text-xs text-muted-foreground">{e.object}</div>
            </td>
            <td className="py-2 pr-4">{e.message}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function Logs({ path }: { path: string }) {
  const { value, error } = usePolled(`${path}/logs?tail=500`, 3000, parseText);
  const end = useRef<HTMLDivElement>(null);
  useEffect(() => end.current?.scrollIntoView({ block: 'end' }), [value]);
  if (error != null) return <p className="p-4 text-sm text-destructive">{error}</p>;
  return (
    <pre className="p-4 font-mono text-xs leading-relaxed whitespace-pre-wrap">
      {value ?? 'Loading…'}
      <div ref={end} />
    </pre>
  );
}

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="grid grid-cols-[9rem_1fr] gap-2 border-b py-2 text-sm">
      <span className="text-muted-foreground">{label}</span>
      <span className="min-w-0 break-words">{children}</span>
    </div>
  );
}

function Detail({ deployment, onClose }: { deployment: Deployment; onClose: () => void }) {
  const d = deployment;
  const path = `/api/deployments/${encodeURIComponent(d.project)}/${encodeURIComponent(d.name)}`;
  const [busy, setBusy] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const stopped = d.status === 'Stopped' || d.status === 'Stopping';
  const act = async (action: 'rollout' | 'restart' | 'stop' | 'start') => {
    setBusy(action);
    setError(await post(`${path}/${action}`));
    setBusy(null);
  };
  return (
    <aside className="flex w-[min(44rem,55vw)] shrink-0 flex-col border-l bg-background">
      <header className="flex items-center gap-3 border-b px-4 py-3">
        <ServerCog className="size-4 text-muted-foreground" />
        <div className="min-w-0">
          <div className="font-mono text-[15px] font-semibold">{d.name}</div>
          <div className="text-xs text-muted-foreground">
            {d.project} · {d.kind}
          </div>
        </div>
        <Status status={d.status} />
        <div className="ml-auto flex items-center gap-2">
          {d.available != null && d.update === 'manual' && (
            <Button size="sm" variant="outline" disabled={busy != null} onClick={() => void act('rollout')}>
              <Rocket />
              Roll out {d.available.slice(0, 7)}
            </Button>
          )}
          {!stopped && (
            <Button size="sm" variant="outline" disabled={busy != null} onClick={() => void act('restart')}>
              <RotateCcw />
              Restart
            </Button>
          )}
          <Button
            size="sm"
            variant="outline"
            disabled={busy != null}
            onClick={() => void act(stopped ? 'start' : 'stop')}
          >
            {stopped ? <Play /> : <Square />}
            {stopped ? 'Start' : 'Stop'}
          </Button>
          <Button variant="ghost" size="icon-sm" onClick={onClose} aria-label="Close">
            <X />
          </Button>
        </div>
      </header>
      {error != null && <p className="border-b px-4 py-2 text-sm text-destructive">{error}</p>}
      <Tabs defaultValue="overview" className="min-h-0 flex-1 gap-0">
        <TabsList className="h-10 w-full justify-start rounded-none border-b bg-transparent px-2">
          <TabsTrigger value="overview">Overview</TabsTrigger>
          <TabsTrigger value="events">Events</TabsTrigger>
          <TabsTrigger value="logs">Logs</TabsTrigger>
        </TabsList>
        <TabsContent value="overview" className="min-h-0">
          <ScrollArea className="h-full">
            <div className="px-4 py-2">
              <Field label="Status">{d.status}</Field>
              <Field label="Commit">
                <span className="font-mono text-[13px]">{d.commit ?? '—'}</span>
              </Field>
              <Field label="Update available">
                {d.available == null ? 'Up to date' : <span className="font-mono text-[13px]">{d.available}</span>}
              </Field>
              <Field label="Update policy">{d.update}</Field>
              <Field label="Node">{d.node ?? '—'}</Field>
              <Field label="Up for">{ago(d.started)}</Field>
              <Field label="Restarts">{d.restarts}</Field>
              <Field label="CPU">
                {cores(d.cpu)} of {cores(d.cpu_limit)} ({d.cpu_pct.toFixed(0)}%)
              </Field>
              <Field label="Memory">
                {bytes(d.memory)} of {bytes(d.memory_limit)} ({d.memory_pct.toFixed(0)}%)
              </Field>
              <Field label="Last warning">
                {d.event == null ? '—' : `${d.event} (${ago(d.event_at)} ago)`}
              </Field>
              <Field label="Reported">{ago(d.ts)} ago</Field>
            </div>
          </ScrollArea>
        </TabsContent>
        <TabsContent value="events" className="min-h-0">
          <ScrollArea className="h-full">
            <Events path={path} />
          </ScrollArea>
        </TabsContent>
        <TabsContent value="logs" className="min-h-0">
          <ScrollArea className="h-full">
            <Logs path={path} />
          </ScrollArea>
        </TabsContent>
      </Tabs>
    </aside>
  );
}

export function DeploymentsPage({ connection }: { connection: KermodesConnection }) {
  const published = connection.pages.includes(DEPLOYMENTS_VIEW);
  const { rows, history } = useDeployments(connection, published);
  const failed = useFailedProjects();
  const [selected, setSelected] = useState<string | null>(null);
  const open = useMemo(() => rows.find(d => key(d) === selected) ?? null, [rows, selected]);
  const close = useCallback(() => setSelected(null), []);
  return (
    <div className="flex min-h-0 flex-1">
      <ScrollArea className="min-h-0 min-w-0 flex-1">
        <div className="flex flex-col gap-3 p-4">
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
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b text-left text-xs text-muted-foreground">
                <th className="py-2 pr-4 pl-3 font-medium">Deployment</th>
                <th className="py-2 pr-4 font-medium">Project</th>
                <th className="py-2 pr-4 font-medium">Status</th>
                <th className="py-2 pr-4 font-medium">Revision</th>
                <th className="py-2 pr-4 font-medium">CPU</th>
                <th className="py-2 pr-4 font-medium">Memory</th>
                <th className="py-2 pr-4 font-medium">Restarts</th>
                <th className="py-2 pr-4 font-medium">Up</th>
                <th className="py-2 pr-4 font-medium">Node</th>
                <th className="py-2 pr-3 font-medium">Last warning</th>
              </tr>
            </thead>
            <tbody>
              {rows.map(d => {
                const samples = history.get(key(d)) ?? [];
                return (
                  <tr
                    key={key(d)}
                    onClick={() => setSelected(key(d))}
                    className={cn(
                      'cursor-pointer border-b hover:bg-accent/50',
                      selected === key(d) && 'bg-accent'
                    )}
                  >
                    <td className="py-2 pr-4 pl-3">
                      <div className="font-mono text-[13px] font-medium">{d.name}</div>
                      <div className="text-xs text-muted-foreground">{d.kind}</div>
                    </td>
                    <td className="py-2 pr-4">{d.project}</td>
                    <td className="py-2 pr-4">
                      <Status status={d.status} />
                    </td>
                    <td className="py-2 pr-4">
                      <Commit commit={d.commit} />
                      {d.available != null && (
                        <div className="text-xs text-warning">
                          {d.update === 'manual' ? 'update available' : 'rolling to'}{' '}
                          {d.available.slice(0, 7)}
                        </div>
                      )}
                    </td>
                    <td className="py-2 pr-4">
                      <Usage
                        pct={d.cpu_pct}
                        samples={samples.map(s => s.cpu)}
                        label={`${cores(d.cpu)} of ${cores(d.cpu_limit)}`}
                      />
                    </td>
                    <td className="py-2 pr-4">
                      <Usage
                        pct={d.memory_pct}
                        samples={samples.map(s => s.memory)}
                        label={`${bytes(d.memory)} of ${bytes(d.memory_limit)}`}
                      />
                    </td>
                    <td className={cn('py-2 pr-4 tabular-nums', d.restarts > 0 && 'text-warning')}>
                      {d.restarts}
                    </td>
                    <td className="py-2 pr-4 tabular-nums">{ago(d.started)}</td>
                    <td className="py-2 pr-4 text-xs text-muted-foreground">{d.node ?? '—'}</td>
                    <td className="max-w-80 truncate py-2 pr-3 text-xs" title={d.event ?? undefined}>
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
          {rows.length === 0 && (
            <p className="px-3 text-sm text-muted-foreground">
              {published ? 'No deployments yet.' : 'Waiting for sys.deployments…'}
            </p>
          )}
        </div>
      </ScrollArea>
      {open != null && <Detail deployment={open} onClose={close} />}
    </div>
  );
}
