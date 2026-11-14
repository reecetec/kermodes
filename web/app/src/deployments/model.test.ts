import { describe, expect, it } from 'bun:test';
import { type Instance, componentUsage, matchesSearch } from './model';

describe('matchesSearch', () => {
  const fields = ['ticker-ticks', 'ticker', 'Running', 'talos-4oh-emk', null];

  it('matches every word somewhere, ignoring case and spacing', () => {
    expect(matchesSearch(fields, '')).toBe(true);
    expect(matchesSearch(fields, 'TICKS')).toBe(true);
    expect(matchesSearch(fields, '  ticker   running ')).toBe(true);
    expect(matchesSearch(fields, '4oh')).toBe(true);
  });

  it('fails when any word is nowhere', () => {
    expect(matchesSearch(fields, 'ticker stopped')).toBe(false);
    expect(matchesSearch(fields, 'null')).toBe(false);
  });
});

describe('componentUsage', () => {
  const pod = (cpu: number, limit: number | null): Instance => ({
    pod: 'p',
    role: null,
    version: null,
    ready: true,
    node: null,
    started_ns: null,
    restarts: 0,
    waiting: null,
    cpu,
    cpu_limit: limit,
    memory: 100,
    memory_limit: limit == null ? null : 1000,
  });

  it('adds use and size across pods', () => {
    expect(componentUsage([pod(0.1, 0.5), pod(0.2, 0.5)])).toEqual({
      cpu: 0.30000000000000004,
      cpuLimit: 1,
      memory: 200,
      memoryLimit: 2000,
    });
  });

  it('has no size when a pod declares none, rather than a share of part of it', () => {
    expect(componentUsage([pod(0.1, 0.5), pod(0.2, null)])).toMatchObject({
      cpuLimit: null,
      memoryLimit: null,
    });
    expect(componentUsage([]).cpuLimit).toBeNull();
  });
});
