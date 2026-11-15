import type { Catalog } from '@/catalog/model';
import {
  type Artifacts,
  type Component,
  type Deployment,
  type Disk,
  componentUsage,
  key,
  matchesSearch,
} from './model';

/** How a status reads at a glance: fine, changing, wrong, or not running by choice */
export type Tone = 'ok' | 'busy' | 'bad' | 'idle';

const TONES: Record<string, Tone> = {
  Running: 'ok',
  Merging: 'busy',
  Building: 'busy',
  Unchecked: 'idle',
  'Rolling out': 'busy',
  Starting: 'busy',
  Degraded: 'busy',
  Stopping: 'busy',
  'Not running': 'idle',
  Stopped: 'idle',
};

/** Any status not known to be fine, changing, or stopped is wrong: CrashLoopBackOff, Failed, … */
export const toneOf = (status: string): Tone => TONES[status] ?? 'bad';

/** The project the platform's own parts are listed under: the Helm release runs them */
export const SYSTEM = 'system';

/** One row of the Deployments table: a deployment, or one of the platform's own parts */
export type Row = {
  key: string;
  name: string;
  project: string;
  kind: string;
  status: string;
  /** A deployment's commit, or the version a part's pods run */
  revision: string | null;
  cpu: number | null;
  cpuLimit: number | null;
  memory: number | null;
  memoryLimit: number | null;
  disks: Disk[];
  restarts: number;
  /** When it last started, in ms */
  started: number | null;
  node: string | null;
  warning: string | null;
  source:
    | { type: 'deployment'; deployment: Deployment }
    | { type: 'component'; component: Component }
    | { type: 'lake'; catalog: Catalog | null; component: Component | null }
    | { type: 'artifacts'; artifacts: Artifacts | null; component: Component | null };
};

const PARTS: Record<string, { name: string; kind: string }> = {
  weir: { name: 'weir', kind: 'store' },
  database: { name: 'postgres', kind: 'database' },
  control: { name: 'kermodes-control', kind: 'controller' },
  ui: { name: 'kermodes-ui', kind: 'web UI' },
};

/** The version a part's pods run: one, or each of them while a rollout replaces them */
export function versionOf(component: Component): string {
  return [...new Set(component.instances.map(i => i.version ?? '?'))].join(' → ');
}

export function deploymentRow(d: Deployment): Row {
  // An app has no pods: the UI's server serves what its build produced.
  const runs = d.kind !== 'app';
  return {
    key: key(d),
    name: d.name,
    project: d.project,
    kind: d.kind,
    status: d.status,
    revision: d.commit,
    cpu: runs ? d.cpu : null,
    cpuLimit: runs ? d.cpu_limit : null,
    memory: runs ? d.memory : null,
    memoryLimit: runs ? d.memory_limit : null,
    disks: [],
    restarts: d.restarts,
    started: d.started,
    node: d.node,
    warning: d.event,
    source: { type: 'deployment', deployment: d },
  };
}

/** Where an app built on Kermodes is served */
export const appUrl = (row: Pick<Row, 'project' | 'name'>) =>
  `/apps/${encodeURIComponent(row.project)}/${encodeURIComponent(row.name)}/`;

/** A part of the platform as one row: its pods' use summed, and its primary's disks (the first pod's if none is) */
export function componentRow(c: Component): Row {
  const part = PARTS[c.name] ?? { name: c.name, kind: c.name };
  const usage = componentUsage(c.instances);
  const primary = c.instances.find(i => i.role === 'primary') ?? c.instances[0];
  const nodes = [...new Set(c.instances.flatMap(i => (i.node == null ? [] : [i.node])))];
  const starts = c.instances.flatMap(i => (i.started_ns == null ? [] : [i.started_ns / 1e6]));
  return {
    key: `${SYSTEM}/${part.name}`,
    name: part.name,
    project: SYSTEM,
    kind: part.kind,
    status: c.status,
    revision: versionOf(c),
    cpu: usage.cpu,
    cpuLimit: usage.cpuLimit,
    memory: usage.memory,
    memoryLimit: usage.memoryLimit,
    disks: primary?.disks ?? [],
    restarts: c.instances.reduce((sum, i) => sum + i.restarts, 0),
    // The newest start: a pod that restarted shows how long the part has been whole.
    started: starts.length > 0 ? Math.max(...starts) : null,
    node: nodes.length === 1 ? nodes[0] : nodes.length > 1 ? `${nodes.length} nodes` : null,
    warning: c.warning,
    source: { type: 'component', component: c },
  };
}

/**
 * The lake as a row, with no pods of its own: how the controller finds it (its store probed,
 * its merges), what every table's merged dates take, and the newest date merged.
 */
export function lakeRow(catalog: Catalog | null, component: Component | null): Row {
  const tables = catalog?.tables ?? [];
  const used = tables.reduce((sum, t) => sum + t.history.reduce((s, d) => s + d.bytes, 0), 0);
  const newest = tables
    .flatMap(t => t.history.map(d => d.date))
    .reduce<string | null>((max, date) => (max == null || date > max ? date : max), null);
  return {
    key: `${SYSTEM}/lake`,
    name: 'lake',
    project: SYSTEM,
    kind: 'object store',
    // Without history configured the controller does not check it
    status: component?.status ?? 'Unchecked',
    revision: newest == null ? null : `through ${newest}`,
    cpu: null,
    cpuLimit: null,
    memory: null,
    memoryLimit: null,
    disks: catalog == null ? [] : [{ name: 'lake', used, size: null }],
    restarts: 0,
    started: null,
    node: null,
    warning: component?.warning ?? null,
    source: { type: 'lake', catalog, component },
  };
}

/**
 * The artifacts store as a row, with no pods of its own: whether the controller can list it,
 * its size, and how many app builds and environments it holds.
 */
export function artifactsRow(artifacts: Artifacts | null, component: Component | null): Row {
  const count = (kind: string) => artifacts?.objects.filter(o => o.kind === kind).length ?? 0;
  const [apps, environments] = [count('app'), count('environment')];
  const plural = (n: number, one: string) => `${n} ${one}${n === 1 ? '' : 's'}`;
  return {
    key: `${SYSTEM}/artifacts`,
    name: 'artifacts',
    project: SYSTEM,
    kind: 'object store',
    status: component?.status ?? 'Unchecked',
    revision:
      artifacts == null ? null : `${plural(apps, 'app build')} · ${plural(environments, 'environment')}`,
    cpu: null,
    cpuLimit: null,
    memory: null,
    memoryLimit: null,
    disks: artifacts == null ? [] : [{ name: 'artifacts', used: artifacts.size, size: null }],
    restarts: 0,
    started: null,
    node: null,
    warning: component?.warning ?? null,
    source: { type: 'artifacts', artifacts, component },
  };
}

/** Every row: the platform's parts first, in their order, then deployments by project and name */
export function tableRows(
  deployments: readonly Deployment[],
  components: readonly Component[] | null,
  catalog: Catalog | null,
  artifacts: Artifacts | null = null
): Row[] {
  const lake = components?.find(c => c.name === 'lake') ?? null;
  const store = components?.find(c => c.name === 'artifacts') ?? null;
  const system = [
    ...(components ?? []).filter(c => c !== lake && c !== store).map(componentRow),
    ...(catalog == null && lake == null ? [] : [lakeRow(catalog, lake)]),
    ...(artifacts == null && store == null ? [] : [artifactsRow(artifacts, store)]),
  ];
  const others = deployments
    .map(deploymentRow)
    .sort((a, b) => a.project.localeCompare(b.project) || a.name.localeCompare(b.name));
  return [...system, ...others];
}

export type Filters = {
  query: string;
  /** Empty for any */
  project: string;
  status: string;
  kind: string;
  node: string;
  /** Leave out what is not running by choice (stopped, never started) */
  hideStopped: boolean;
};

export const NO_FILTERS: Filters = {
  query: '',
  project: '',
  status: '',
  kind: '',
  node: '',
  hideStopped: false,
};

/** Whether `row` passes every filter */
export function matches(row: Row, f: Filters): boolean {
  return (
    (f.project === '' || row.project === f.project) &&
    (f.status === '' || row.status === f.status) &&
    (f.kind === '' || row.kind === f.kind) &&
    (f.node === '' || row.node === f.node) &&
    !(f.hideStopped && toneOf(row.status) === 'idle') &&
    matchesSearch(
      [row.name, row.project, row.kind, row.status, row.revision, row.node, row.warning],
      f.query
    )
  );
}

/** How many rows read each way */
export function counts(rows: readonly Row[]): Record<Tone, number> {
  const out: Record<Tone, number> = { ok: 0, busy: 0, bad: 0, idle: 0 };
  for (const row of rows) out[toneOf(row.status)] += 1;
  return out;
}

/** The distinct values of `field` among `rows`, sorted, for a filter's choices */
export function choices(
  rows: readonly Row[],
  field: 'project' | 'status' | 'kind' | 'node'
): string[] {
  return [...new Set(rows.flatMap(r => (r[field] == null ? [] : [r[field] as string])))].sort();
}
