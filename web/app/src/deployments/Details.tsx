import { useState } from 'react';
import { Play, RotateCcw, Rocket, ServerCog, Square, X } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { ScrollArea } from '@/components/ui/scroll-area';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { cn } from '@/lib/utils';
import { type Component, type Deployment, ago, bytes, cores } from './model';
import { Commit, Events, Field, Logs, Status } from './parts';
import { componentMeta, versionOf } from './SystemStrip';

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

/** A details panel: its header, then tabs filling the rest */
function Panel({
  header,
  tabs,
}: {
  header: React.ReactNode;
  tabs: { name: string; content: React.ReactNode }[];
}) {
  return (
    <section className="flex h-full min-h-0 flex-col bg-background">
      {header}
      <Tabs defaultValue={tabs[0].name} className="min-h-0 flex-1 gap-0">
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

function Close({ onClose }: { onClose: () => void }) {
  return (
    <Button variant="ghost" size="icon-sm" onClick={onClose} aria-label="Close details" title="Close (Esc)">
      <X />
    </Button>
  );
}

export function DeploymentDetail({
  deployment,
  source,
  onClose,
}: {
  deployment: Deployment;
  source: string | null;
  onClose: () => void;
}) {
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
  const header = (
    <>
      <header className="flex shrink-0 items-center gap-3 border-b px-4 py-2.5">
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
          <Button size="sm" variant="outline" disabled={busy != null} onClick={() => void act(stopped ? 'start' : 'stop')}>
            {stopped ? <Play /> : <Square />}
            {stopped ? 'Start' : 'Stop'}
          </Button>
          <Close onClose={onClose} />
        </div>
      </header>
      {error != null && <p className="shrink-0 border-b px-4 py-2 text-sm text-destructive">{error}</p>}
    </>
  );
  const overview = (
    <div className="grid gap-x-8 px-4 py-2 md:grid-cols-2 xl:grid-cols-3">
      <Field label="Status">{d.status}</Field>
      <Field label="Commit">
        <Commit commit={d.commit} href={source} />
      </Field>
      <Field label="Update available">
        {d.available == null ? 'Up to date' : <span className="font-mono text-[13px]">{d.available.slice(0, 12)}</span>}
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
      <Field label="Last warning">{d.event == null ? '—' : `${d.event} (${ago(d.event_at)} ago)`}</Field>
      <Field label="Reported">{ago(d.ts)} ago</Field>
    </div>
  );
  return (
    <Panel
      header={header}
      tabs={[
        { name: 'overview', content: overview },
        { name: 'events', content: <Events url={`${path}/events`} /> },
        { name: 'logs', content: <Logs url={`${path}/logs?tail=500`} /> },
      ]}
    />
  );
}

export function ComponentDetail({ component, onClose }: { component: Component; onClose: () => void }) {
  const meta = componentMeta(component.name);
  const Icon = meta.icon;
  const path = `/api/components/${encodeURIComponent(component.name)}`;
  const [pod, setPod] = useState<string>('');
  const header = (
    <header className="flex shrink-0 items-center gap-3 border-b px-4 py-2.5">
      <Icon className="size-4 text-muted-foreground" />
      <div className="min-w-0">
        <div className="text-[15px] font-semibold">{meta.label}</div>
        <div className="text-xs text-muted-foreground">
          {meta.about} · <span className="font-mono">{versionOf(component)}</span>
        </div>
      </div>
      <Status status={component.status} />
      <div className="ml-auto">
        <Close onClose={onClose} />
      </div>
    </header>
  );
  const instances = (
    <table className="w-full text-sm">
      <thead>
        <tr className="border-b text-left text-xs text-muted-foreground">
          {['Pod', 'Role', 'Ready', 'Version', 'Node', 'Up', 'Restarts', 'CPU', 'Memory'].map(h => (
            <th key={h} className="py-2 pr-4 font-medium first:pl-4">
              {h}
            </th>
          ))}
        </tr>
      </thead>
      <tbody>
        {component.instances.map(i => (
          <tr key={i.pod} className="border-b">
            <td className="py-2 pr-4 pl-4 font-mono text-[13px]">{i.pod}</td>
            <td className="py-2 pr-4">{i.role ?? '—'}</td>
            <td className={cn('py-2 pr-4', !i.ready && 'text-destructive')}>
              {i.ready ? 'yes' : (i.waiting ?? 'no')}
            </td>
            <td className="py-2 pr-4 font-mono text-xs">{i.version ?? '—'}</td>
            <td className="py-2 pr-4 text-xs text-muted-foreground">{i.node ?? '—'}</td>
            <td className="py-2 pr-4 tabular-nums">{ago(i.started_ns == null ? null : i.started_ns / 1e6)}</td>
            <td className={cn('py-2 pr-4 tabular-nums', i.restarts > 0 && 'text-warning')}>{i.restarts}</td>
            <td className="py-2 pr-4 tabular-nums">
              {cores(i.cpu)}
              {i.cpu_limit != null && <span className="text-muted-foreground"> of {cores(i.cpu_limit)}</span>}
            </td>
            <td className="py-2 pr-4 tabular-nums">
              {bytes(i.memory)}
              {i.memory_limit != null && <span className="text-muted-foreground"> of {bytes(i.memory_limit)}</span>}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
  const logs = (
    <div>
      {component.instances.length > 1 && (
        <div className="flex items-center gap-2 border-b px-4 py-2 text-xs text-muted-foreground">
          Pod
          <select
            value={pod}
            onChange={event => setPod(event.target.value)}
            className="rounded-md border bg-background px-2 py-1 font-mono text-xs text-foreground"
          >
            <option value="">a ready one</option>
            {component.instances.map(i => (
              <option key={i.pod} value={i.pod}>
                {i.pod}
                {i.role != null ? ` (${i.role})` : ''}
              </option>
            ))}
          </select>
        </div>
      )}
      <Logs key={pod} url={`${path}/logs?tail=500${pod ? `&pod=${encodeURIComponent(pod)}` : ''}`} />
    </div>
  );
  return (
    <Panel
      header={header}
      tabs={[
        { name: 'overview', content: instances },
        { name: 'events', content: <Events url={`${path}/events`} /> },
        { name: 'logs', content: logs },
      ]}
    />
  );
}
