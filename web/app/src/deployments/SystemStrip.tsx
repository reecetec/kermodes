import { AppWindow, Database, HardDrive, ServerCog, TriangleAlert, type LucideIcon } from 'lucide-react';
import { cn } from '@/lib/utils';
import type { Sample } from './data';
import { type Component, bytes, componentUsage, cores } from './model';
import { Status, StatusDot, Usage } from './parts';

/** How each component is named and drawn */
export const COMPONENTS: Record<string, { label: string; icon: LucideIcon; about: string }> = {
  weir: { label: 'weir', icon: HardDrive, about: 'Ingest and today’s tables' },
  database: { label: 'Postgres', icon: Database, about: 'Catalog and controller state' },
  control: { label: 'Controller', icon: ServerCog, about: 'Syncs projects, runs deployments' },
  ui: { label: 'Web UI', icon: AppWindow, about: 'This page and its tables' },
};

export function componentMeta(name: string) {
  return COMPONENTS[name] ?? { label: name, icon: ServerCog, about: '' };
}

/** The version its pods run: one, or each of them while a rollout replaces them */
export function versionOf(component: Component): string {
  const versions = [...new Set(component.instances.map(i => i.version ?? '?'))];
  return versions.join(' → ');
}

/** The platform's components, one card each; a card opens its details */
export function SystemStrip({
  components,
  error,
  history,
  selected,
  onSelect,
}: {
  components: Component[] | null;
  error: string | null;
  history: Map<string, Sample[]>;
  selected: string | null;
  onSelect: (name: string) => void;
}) {
  if (components == null) {
    return error == null ? null : (
      <p className="flex items-center gap-2 text-xs text-muted-foreground">
        <TriangleAlert className="size-3.5" />
        System status is unavailable: {error}
      </p>
    );
  }
  return (
    <div className="grid grid-cols-[repeat(auto-fill,minmax(17rem,1fr))] gap-2">
      {components.map(component => (
        <Card
          key={component.name}
          component={component}
          samples={history.get(component.name) ?? []}
          selected={selected === component.name}
          onSelect={() => onSelect(component.name)}
        />
      ))}
      {error != null && (
        <p className="col-span-full text-xs text-destructive">Last refresh failed: {error}</p>
      )}
    </div>
  );
}

function Card({
  component,
  samples,
  selected,
  onSelect,
}: {
  component: Component;
  samples: Sample[];
  selected: boolean;
  onSelect: () => void;
}) {
  const meta = componentMeta(component.name);
  const Icon = meta.icon;
  const usage = componentUsage(component.instances);
  const ready = component.instances.filter(i => i.ready).length;
  const restarts = component.instances.reduce((sum, i) => sum + i.restarts, 0);
  const primary = component.instances.find(i => i.role === 'primary');
  return (
    <button
      type="button"
      onClick={onSelect}
      aria-pressed={selected}
      className={cn(
        'flex min-w-0 flex-col gap-2 rounded-lg border bg-card px-3 py-2.5 text-left transition-colors',
        'hover:border-primary/40 hover:bg-accent/30 focus-visible:ring-2 focus-visible:ring-ring focus-visible:outline-none',
        selected && 'border-primary/60 bg-accent/40'
      )}
    >
      <div className="flex items-center gap-2">
        <StatusDot status={component.status} />
        <Icon className="size-4 text-muted-foreground" />
        <span className="font-medium">{meta.label}</span>
        {component.status !== 'Running' && <Status status={component.status} />}
        <span className="ml-auto truncate font-mono text-xs text-muted-foreground">
          {versionOf(component)}
        </span>
      </div>
      <div className="flex items-center gap-2 text-xs text-muted-foreground">
        <span className={cn(ready < component.instances.length && 'text-warning')}>
          {ready}/{component.instances.length} ready
        </span>
        {primary != null && <span>· primary on {primary.node ?? '?'}</span>}
        {component.instances.length === 1 && primary == null && (
          <span className="truncate">· {component.instances[0].node ?? '—'}</span>
        )}
        {restarts > 0 && <span className="text-warning">· {restarts} restarts</span>}
      </div>
      <div className="grid grid-cols-2 gap-3">
        <div>
          <div className="mb-0.5 text-[11px] text-muted-foreground">CPU</div>
          <Usage
            value={usage.cpu}
            limit={usage.cpuLimit}
            samples={samples.map(s => s.cpu)}
            format={cores}
            width={64}
          />
        </div>
        <div>
          <div className="mb-0.5 text-[11px] text-muted-foreground">Memory</div>
          <Usage
            value={usage.memory}
            limit={usage.memoryLimit}
            samples={samples.map(s => s.memory)}
            format={bytes}
            width={64}
          />
        </div>
      </div>
      {component.warning != null && (
        <div className="truncate text-xs text-destructive" title={component.warning}>
          {component.warning}
        </div>
      )}
    </button>
  );
}
