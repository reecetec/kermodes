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

/** A project the controller syncs (`/api/projects`) */
export type Project = {
  name: string;
  failed: { commit: string; error: string } | null;
  resources: { name: string; source: string | null }[];
};

/** One pod of a platform component (`/api/components`) */
export type Instance = {
  pod: string;
  role: string | null;
  version: string | null;
  ready: boolean;
  node: string | null;
  started_ns: number | null;
  restarts: number;
  waiting: string | null;
  cpu: number;
  cpu_limit: number | null;
  memory: number;
  memory_limit: number | null;
  /** Its volumes: weir's store; Postgres's data (its databases) and WAL */
  disks: Disk[];
};

/** A volume of a pod: bytes in use, and its size when known */
export type Disk = { name: string; used: number; size: number | null };

/** A platform component: weir, Postgres, the controller, or the web UI */
export type Component = {
  name: string;
  status: string;
  instances: Instance[];
  warning: string | null;
  warning_ns: number | null;
};

export const key = (d: { project: string; name: string }) => `${d.project}/${d.name}`;

/**
 * Whether a row whose searchable text is `fields` matches `query`: every word of the query
 * appears in some field, ignoring case. An empty query matches everything.
 */
export function matchesSearch(fields: readonly (string | null | undefined)[], query: string): boolean {
  const words = query.toLowerCase().split(/\s+/).filter(Boolean);
  if (words.length === 0) {
    return true;
  }
  const text = fields
    .filter((field): field is string => field != null)
    .map(field => field.toLowerCase());
  return words.every(word => text.some(field => field.includes(word)));
}

/**
 * A component's use and size across its pods: cores and bytes, and their limits (`null`
 * unless every pod declares one, since a share of part of the size would mislead).
 */
export function componentUsage(instances: readonly Instance[]): {
  cpu: number;
  cpuLimit: number | null;
  memory: number;
  memoryLimit: number | null;
} {
  const sum = (values: (number | null)[]) =>
    values.some(v => v == null) ? null : values.reduce<number>((a, v) => a + (v ?? 0), 0);
  return {
    cpu: instances.reduce((a, i) => a + i.cpu, 0),
    cpuLimit: instances.length === 0 ? null : sum(instances.map(i => i.cpu_limit)),
    memory: instances.reduce((a, i) => a + i.memory, 0),
    memoryLimit: instances.length === 0 ? null : sum(instances.map(i => i.memory_limit)),
  };
}

export function bytes(n: number): string {
  if (n >= 1 << 30) return `${(n / (1 << 30)).toFixed(1)} GiB`;
  if (n >= 1 << 20) return `${Math.round(n / (1 << 20))} MiB`;
  return `${Math.round(n / 1024)} KiB`;
}

export function cores(n: number): string {
  return n >= 1 ? `${n.toFixed(2)} cores` : `${Math.round(n * 1000)}m`;
}

export function ago(ms: number | null): string {
  if (ms == null) return '—';
  const s = Math.max(0, Math.round((Date.now() - ms) / 1000));
  if (s < 60) return `${s}s`;
  if (s < 3600) return `${Math.floor(s / 60)}m`;
  if (s < 86_400) return `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m`;
  return `${Math.floor(s / 86_400)}d ${Math.floor((s % 86_400) / 3600)}h`;
}
