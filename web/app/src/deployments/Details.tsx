import { useState } from 'react';
import { X } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { ScrollArea } from '@/components/ui/scroll-area';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { cn } from '@/lib/utils';
import { totals, utcDate } from '@/catalog/model';
import { type Component, type Deployment, type Instance, ago, bytes, cores } from './model';
import { Commit, DiskUse, Events, Field, Logs, StatusFill } from './parts';
import type { Row } from './rows';

/** A row's details, docked to the right of the table: a header, then tabs filling the rest */
export function RowDetail({
  row,
  source,
  fold,
  onClose,
}: {
  row: Row;
  /** Where a deployment is declared at the commit it runs, on its git host */
  source: string | null;
  fold: React.ReactNode;
  onClose: () => void;
}) {
  const tabs = tabsOf(row, source);
  return (
    <section className="flex h-full min-h-0 flex-col">
      <header className="flex shrink-0 items-start gap-2 border-b px-3 py-2">
        <div className="min-w-0 flex-1">
          <div className="truncate font-mono text-[14px] font-semibold" title={row.name}>
            {row.name}
          </div>
          <div className="truncate text-xs text-muted-foreground">
            {row.project} · {row.kind}
          </div>
        </div>
        <StatusFill status={row.status} />
        {fold}
        <Button
          variant="ghost"
          size="icon-sm"
          onClick={onClose}
          aria-label="Close details"
          title="Close (Esc)"
        >
          <X />
        </Button>
      </header>
      <Tabs key={row.key} defaultValue={tabs[0].name} className="min-h-0 flex-1 gap-0">
        <TabsList className="h-9 w-full shrink-0 justify-start gap-1 rounded-none border-b bg-transparent px-2">
          {tabs.map(({ name }) => (
            <TabsTrigger
              key={name}
              value={name}
              className="h-full border-b-2 border-transparent px-3 capitalize hover:text-foreground data-[state=active]:border-primary data-[state=active]:text-foreground"
            >
              {name}
            </TabsTrigger>
          ))}
        </TabsList>
        {tabs.map(({ name, content }) => (
          <TabsContent key={name} value={name} className="min-h-0">
            <ScrollArea className="h-full">{content}</ScrollArea>
          </TabsContent>
        ))}
      </Tabs>
    </section>
  );
}

function tabsOf(row: Row, source: string | null): { name: string; content: React.ReactNode }[] {
  switch (row.source.type) {
    case 'deployment': {
      const d = row.source.deployment;
      const path = `/api/deployments/${encodeURIComponent(d.project)}/${encodeURIComponent(d.name)}`;
      return [
        { name: 'summary', content: <DeploymentSummary d={d} source={source} /> },
        { name: 'logs', content: <Logs url={`${path}/logs?tail=500`} /> },
        { name: 'events', content: <Events url={`${path}/events`} /> },
        { name: 'settings', content: <DeploymentSettings d={d} source={source} /> },
      ];
    }
    case 'component': {
      const c = row.source.component;
      const path = `/api/components/${encodeURIComponent(c.name)}`;
      return [
        { name: 'summary', content: <ComponentSummary c={c} /> },
        { name: 'logs', content: <ComponentLogs c={c} path={path} /> },
        { name: 'events', content: <Events url={`${path}/events`} /> },
        { name: 'settings', content: <ComponentSettings c={c} /> },
      ];
    }
    case 'history':
      return [{ name: 'summary', content: <HistorySummary row={row} /> }];
  }
}

function DeploymentSummary({ d, source }: { d: Deployment; source: string | null }) {
  return (
    <div className="px-3 py-1">
      <Field label="Commit">
        <Commit commit={d.commit} href={source} />
      </Field>
      <Field label="Update available">
        {d.available == null ? (
          'Up to date'
        ) : (
          <span className="font-mono text-[13px] text-warning">{d.available.slice(0, 12)}</span>
        )}
      </Field>
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
        {d.event == null ? '—' : <span className="text-destructive">{d.event}</span>}
        {d.event_at != null && (
          <span className="text-muted-foreground"> ({ago(d.event_at)} ago)</span>
        )}
      </Field>
      <Field label="Reported">{ago(d.ts)} ago</Field>
    </div>
  );
}

function DeploymentSettings({ d, source }: { d: Deployment; source: string | null }) {
  return (
    <div className="px-3 py-1">
      <Field label="Kind">{d.kind}</Field>
      <Field label="Update policy">
        {d.update}
        <div className="text-xs text-muted-foreground">
          {d.update === 'manual'
            ? 'A new commit waits for Roll out.'
            : 'Each new commit rolls out by itself.'}
        </div>
      </Field>
      <Field label="CPU">{cores(d.cpu_limit)}</Field>
      <Field label="Memory">{bytes(d.memory_limit)}</Field>
      <Field label="Declared in">
        {source == null ? (
          '—'
        ) : (
          <a href={source} target="_blank" rel="noreferrer" className="break-all underline">
            {source}
          </a>
        )}
      </Field>
      <p className="py-2 text-xs text-muted-foreground">
        Settings come from the project's code; change them there and the next commit rolls out.
      </p>
    </div>
  );
}

function InstanceBlock({ i }: { i: Instance }) {
  return (
    <div className="border-b px-3 py-2 text-sm">
      <div className="flex items-center gap-2">
        <span className="truncate font-mono text-[13px] font-medium">{i.pod}</span>
        {i.role != null && <span className="text-xs text-muted-foreground">{i.role}</span>}
        <span
          className={cn('ml-auto text-xs', i.ready ? 'text-success' : 'text-destructive')}
        >
          {i.ready ? 'ready' : (i.waiting ?? 'not ready')}
        </span>
      </div>
      <div className="mt-1 grid grid-cols-2 gap-x-3 gap-y-0.5 text-xs text-muted-foreground tabular-nums">
        <span>
          version <span className="font-mono text-foreground">{i.version ?? '—'}</span>
        </span>
        <span className="truncate">node {i.node ?? '—'}</span>
        <span>up {ago(i.started_ns == null ? null : i.started_ns / 1e6)}</span>
        <span className={cn(i.restarts > 0 && 'text-warning')}>{i.restarts} restarts</span>
        <span>
          cpu <span className="text-foreground">{cores(i.cpu)}</span>
          {i.cpu_limit != null && ` of ${cores(i.cpu_limit)}`}
        </span>
        <span>
          mem <span className="text-foreground">{bytes(i.memory)}</span>
          {i.memory_limit != null && ` of ${bytes(i.memory_limit)}`}
        </span>
      </div>
      {i.disks.length > 0 && (
        <div className="mt-1 flex flex-col gap-0.5">
          {i.disks.map(d => (
            <DiskUse key={d.name} name={d.name} used={d.used} size={d.size} />
          ))}
        </div>
      )}
    </div>
  );
}

function ComponentSummary({ c }: { c: Component }) {
  return (
    <div>
      {c.warning != null && (
        <p className="border-b px-3 py-2 text-sm text-destructive">{c.warning}</p>
      )}
      {c.instances.map(i => (
        <InstanceBlock key={i.pod} i={i} />
      ))}
    </div>
  );
}

function ComponentLogs({ c, path }: { c: Component; path: string }) {
  const [pod, setPod] = useState('');
  return (
    <div>
      {c.instances.length > 1 && (
        <div className="flex items-center gap-2 border-b px-3 py-2 text-xs text-muted-foreground">
          Pod
          <select
            value={pod}
            onChange={event => setPod(event.target.value)}
            className="rounded-md border bg-background px-2 py-1 font-mono text-xs text-foreground"
          >
            <option value="">a ready one</option>
            {c.instances.map(i => (
              <option key={i.pod} value={i.pod}>
                {i.pod}
                {i.role != null ? ` (${i.role})` : ''}
              </option>
            ))}
          </select>
        </div>
      )}
      <Logs
        key={pod}
        url={`${path}/logs?tail=500${pod ? `&pod=${encodeURIComponent(pod)}` : ''}`}
      />
    </div>
  );
}

function ComponentSettings({ c }: { c: Component }) {
  return (
    <div className="px-3 py-1">
      <Field label="Pods">{c.instances.length}</Field>
      <Field label="Images">
        {[...new Set(c.instances.map(i => i.version ?? '?'))].join(', ')}
      </Field>
      <p className="py-2 text-xs text-muted-foreground">
        The Helm release runs this part of the platform: change it in the chart's values, and
        roll it out with the release, not from here.
      </p>
    </div>
  );
}

function HistorySummary({ row }: { row: Row }) {
  if (row.source.type !== 'history') return null;
  const tables = row.source.catalog.tables;
  const all = totals(tables, utcDate(new Date()));
  const largest = tables
    .map(t => ({ table: t.table, bytes: t.history.reduce((s, d) => s + d.bytes, 0) }))
    .filter(t => t.bytes > 0)
    .sort((a, b) => b.bytes - a.bytes)
    .slice(0, 8);
  return (
    <div className="px-3 py-1">
      <Field label="Size">{bytes(all.historyBytes)}</Field>
      <Field label="Rows">{all.historyRows.toLocaleString()}</Field>
      <Field label="Tables">{tables.filter(t => t.history.length > 0).length}</Field>
      <h3 className="mt-3 mb-1 text-xs font-medium text-muted-foreground">Largest</h3>
      {largest.map(t => (
        <div key={t.table} className="flex justify-between border-b py-1 text-sm">
          <span className="font-mono text-[13px]">{t.table}</span>
          <span className="tabular-nums">{bytes(t.bytes)}</span>
        </div>
      ))}
      <p className="py-2 text-xs text-muted-foreground">
        Merged from weir each night by the controller; the Catalog tab has every date.
      </p>
    </div>
  );
}
