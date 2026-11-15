import { useEffect, useRef, useState } from 'react';
import { tableFromIPC } from 'apache-arrow';
import { toBytes, type KermodesConnection } from '@kermodes/client';
import { type Artifacts, type Component, type Deployment, type Project, componentUsage, key } from './model';

/** The view of `sys.deployments` the UI's server publishes for this page: each deployment's newest row */
export const DEPLOYMENTS_VIEW = 'deployments';

/** Samples of CPU and memory kept for the sparklines: about 5 minutes */
const HISTORY = 60;

/** Cores and bytes in use at `ts` */
export type Sample = { ts: number; cpu: number; memory: number };

function remember(history: Map<string, Sample[]>, id: string, sample: Sample): void {
  const samples = history.get(id) ?? [];
  if (samples.at(-1)?.ts !== sample.ts) {
    samples.push(sample);
    history.set(id, samples.slice(-HISTORY));
  }
}

// Arrow gives int64 as bigint and timestamps as milliseconds
const num = (value: unknown) => (value == null ? null : Number(value));

/** The deployments' newest rows, live, and a short history of their usage */
export function useDeployments(connection: KermodesConnection, published: boolean) {
  const [rows, setRows] = useState<Deployment[]>([]);
  const history = useRef(new Map<string, Sample[]>());
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
        remember(history.current, key(d), { ts: d.ts, cpu: d.cpu, memory: d.memory });
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

/** `url`'s answer every `every` ms, parsed, and the last error */
export function usePolled<T>(url: string, every: number, parse: (r: Response) => Promise<T>) {
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
        const parsed = await parse(response);
        if (!live) return;
        setError(null);
        setValue(parsed);
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

const parseProjects = async (r: Response) => (await r.json()).projects as Project[];
const parseComponents = (r: Response) => r.json() as Promise<Component[]>;

/** The projects the controller syncs: their resources' code links, and commits that failed */
export function useProjects(): Project[] {
  return usePolled('/api/projects', 10_000, parseProjects).value ?? [];
}

const parseArtifacts = (r: Response) => r.json() as Promise<Artifacts>;

/** The artifacts store's objects and what each is for; null where there is none */
export function useArtifacts(): Artifacts | null {
  return usePolled('/api/artifacts', 30_000, parseArtifacts).value;
}

/** The platform's components, polled from the controller, and a short history of their usage */
export function useComponents() {
  const { value, error } = usePolled('/api/components', 5_000, parseComponents);
  const history = useRef(new Map<string, Sample[]>());
  useEffect(() => {
    const now = Date.now();
    for (const component of value ?? []) {
      const usage = componentUsage(component.instances);
      remember(history.current, component.name, { ts: now, cpu: usage.cpu, memory: usage.memory });
    }
  }, [value]);
  return { components: value, error, history: history.current };
}
