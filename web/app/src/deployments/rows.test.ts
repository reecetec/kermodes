import { describe, expect, it } from 'bun:test';
import type { Catalog } from '@/catalog/model';
import type { Component, Deployment, Instance } from './model';
import {
  NO_FILTERS,
  choices,
  artifactsRow,
  componentRow,
  counts,
  lakeRow,
  matches,
  tableRows,
  toneOf,
} from './rows';

function deployment(project: string, name: string, status = 'Running'): Deployment {
  return {
    ts: 0,
    project,
    name,
    kind: 'deployment',
    status,
    update: 'auto',
    commit: 'abc1234',
    available: null,
    node: 'node-a',
    started: 1,
    restarts: 0,
    cpu: 0.1,
    cpu_limit: 1,
    cpu_pct: 10,
    memory: 1,
    memory_limit: 2,
    memory_pct: 50,
    event: null,
    event_at: null,
  };
}

function pod(pod: string, role: string | null, node: string, started_s: number): Instance {
  return {
    pod,
    role,
    version: '17.2',
    ready: true,
    node,
    started_ns: started_s * 1e9,
    restarts: 1,
    waiting: null,
    cpu: 0.5,
    cpu_limit: 1,
    memory: 100,
    memory_limit: 200,
    disks: [{ name: 'data', used: role === 'primary' ? 7 : 3, size: 10 }],
  };
}

const postgres: Component = {
  name: 'database',
  status: 'Running',
  instances: [pod('pg-1', 'replica', 'node-a', 10), pod('pg-2', 'primary', 'node-b', 30)],
  warning: null,
  warning_ns: null,
};

const catalog: Catalog = {
  volume: { bytes: 100, free: 40 },
  tables: [
    {
      table: 'md.ticks',
      kind: 'append',
      key: [],
      sort: [],
      columns: [],
      weir: [],
      history: [
        { date: '2026-11-12', rows: 5, bytes: 50, files: 1 },
        { date: '2026-11-13', rows: 5, bytes: 20, files: 1 },
      ],
    },
    {
      table: 'md.quotes',
      kind: 'keyed',
      key: ['sym'],
      sort: [],
      columns: [],
      weir: [],
      history: [{ date: '2026-11-13', rows: 1, bytes: 5, files: 1 }],
    },
  ],
};

describe('tableRows', () => {
  it('lists the platform first, then deployments by project and name', () => {
    const rows = tableRows(
      [deployment('zeta', 'b'), deployment('alpha', 'z'), deployment('alpha', 'a')],
      [postgres],
      catalog
    );
    expect(rows.map(r => r.key)).toEqual([
      'system/postgres',
      'system/lake',
      'alpha/a',
      'alpha/z',
      'zeta/b',
    ]);
  });
});

describe('componentRow', () => {
  it("sums its pods' use and shows the primary's disks, its newest start, and its nodes", () => {
    const row = componentRow(postgres);
    expect([row.project, row.kind, row.cpu, row.cpuLimit, row.memory, row.restarts]).toEqual([
      'system',
      'database',
      1,
      2,
      200,
      2,
    ]);
    expect(row.disks).toEqual([{ name: 'data', used: 7, size: 10 }]);
    expect(row.started).toBe(30_000);
    expect(row.node).toBe('2 nodes');
  });
});

describe('artifactsRow', () => {
  it('is the store as the controller lists it: its size, and what it holds', () => {
    const store = { name: 'artifacts', status: 'Running', instances: [], warning: null, warning_ns: null };
    const listed = {
      size: 13_900_000,
      objects: [
        { object: 'environments/e1.tar', size: 13_480_000, kind: 'environment' as const, uses: ['ticker at c70e728'], building: false },
        { object: 'apps/ticker/market/c7.tar', size: 420_000, kind: 'app' as const, uses: ['ticker/market, serving'], building: false },
      ],
    };
    const row = artifactsRow(listed, store);
    expect(row).toMatchObject({ project: 'system', kind: 'object store', status: 'Running' });
    expect(row.revision).toBe('1 app build · 1 environment');
    expect(row.disks).toEqual([{ name: 'artifacts', used: 13_900_000, size: null }]);
    // One row for the store, not one among the parts with pods as well
    const rows = tableRows([], [store], null, listed);
    expect(rows.map(r => r.key)).toEqual(['system/artifacts']);
  });
});

describe('lakeRow', () => {
  it("is what every table's merged dates take, through the newest, as the controller finds it", () => {
    const check = {
      name: 'lake',
      status: 'Merge failed',
      instances: [],
      warning: 'merging md.ticks 2026-11-14 failed; it is tried again tomorrow',
      warning_ns: null,
    };
    const row = lakeRow(catalog, check);
    expect(row.disks).toEqual([{ name: 'lake', used: 75, size: null }]);
    expect([row.status, row.warning, toneOf(row.status)]).toEqual([
      'Merge failed',
      check.warning,
      'bad',
    ]);
    expect(row.revision).toMatch(/^through \d{4}-\d{2}-\d{2}$/);
    // Without history configured the controller does not check the lake
    expect(toneOf(lakeRow(catalog, null).status)).toBe('idle');
  });

  it('takes the lake out of the components it arrives with', () => {
    const check = { name: 'lake', status: 'Running', instances: [], warning: null, warning_ns: null };
    const keys = tableRows([], [postgres, check], catalog).map(r => r.key);
    expect(keys).toEqual(['system/postgres', 'system/lake']);
  });
});

describe('toneOf and counts', () => {
  it('reads any status it does not know as wrong', () => {
    expect(['Running', 'Degraded', 'Stopped', 'CrashLoopBackOff'].map(toneOf)).toEqual([
      'ok',
      'busy',
      'idle',
      'bad',
    ]);
    const rows = tableRows(
      [deployment('p', 'a', 'Stopped'), deployment('p', 'b', 'ImagePullBackOff')],
      [postgres],
      null
    );
    expect(counts(rows)).toEqual({ ok: 1, busy: 0, bad: 1, idle: 1 });
  });
});

describe('matches', () => {
  const rows = tableRows(
    [deployment('ticker', 'ticks'), deployment('ticker', 'old', 'Stopped')],
    [postgres],
    catalog
  );
  const keys = (f: Partial<typeof NO_FILTERS>) =>
    rows.filter(r => matches(r, { ...NO_FILTERS, ...f })).map(r => r.key);

  it('keeps every row with no filters', () => {
    expect(keys({})).toHaveLength(4);
  });

  it('filters by each field, hides stopped rows, and searches every column', () => {
    expect(keys({ project: 'system' })).toEqual(['system/postgres', 'system/lake']);
    expect(keys({ hideStopped: true })).not.toContain('ticker/old');
    expect(keys({ kind: 'object store' })).toEqual(['system/lake']);
    expect(keys({ query: 'ticker stopped' })).toEqual(['ticker/old']);
    expect(keys({ node: '2 nodes', project: 'ticker' })).toEqual([]);
  });

  it('offers the values present as choices', () => {
    expect(choices(rows, 'project')).toEqual(['system', 'ticker']);
  });
});
