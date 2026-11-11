import { useCallback, useEffect, useState } from 'react';
import { AlertTriangle, GitBranch, GitCommitHorizontal, Rocket } from 'lucide-react';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { ScrollArea } from '@/components/ui/scroll-area';

/** A resource as `kermodes control` reports it (`GET /api/status`, through the UI's server) */
export type Resource = {
  name: string;
  update: 'auto' | 'manual' | 'onstart';
  /** The commit it runs, if it ever ran */
  commit: string | null;
  /** A newer deployable commit it does not run yet */
  available: string | null;
  rollout: 'ready' | 'rolling' | 'failed' | 'absent';
};

export type Project = {
  name: string;
  url: string;
  branch: string;
  path: string;
  /** The newest commit whose discovery succeeded: what the project runs */
  commit: string | null;
  /** The mainline's newest commit, when its discovery failed */
  failed: { commit: string; error: string } | null;
  resources: Resource[];
};

export type Programs =
  | { state: 'loading' }
  | { state: 'none' }
  | { state: 'error'; error: string }
  | { state: 'ok'; projects: Project[] };

const POLL_MS = 3000;

async function errorOf(response: Response): Promise<string> {
  const body = await response.json().catch(() => null);
  return body?.error ?? `${response.status} ${response.statusText}`;
}

/** The controller's programs, polled; `none` when this server has no controller */
export function usePrograms(): [Programs, () => Promise<void>] {
  const [programs, setPrograms] = useState<Programs>({ state: 'loading' });
  const refresh = useCallback(async () => {
    try {
      const response = await fetch('/api/programs');
      if (response.status === 404) {
        setPrograms({ state: 'none' });
      } else if (!response.ok) {
        setPrograms({ state: 'error', error: await errorOf(response) });
      } else {
        setPrograms({ state: 'ok', projects: (await response.json()).projects });
      }
    } catch (e) {
      setPrograms({ state: 'error', error: String(e) });
    }
  }, []);
  useEffect(() => {
    void refresh();
    const timer = setInterval(() => void refresh(), POLL_MS);
    return () => clearInterval(timer);
  }, [refresh]);
  return [programs, refresh];
}

function Commit({ commit }: { commit: string | null }) {
  return (
    <span className="inline-flex items-center gap-1 font-mono text-[13px]">
      {commit != null && <GitCommitHorizontal className="size-3.5 text-muted-foreground" />}
      {commit == null ? '—' : commit.slice(0, 7)}
    </span>
  );
}

const ROLLOUT: Record<
  Resource['rollout'],
  { label: string; variant: 'success' | 'warning' | 'destructive' | 'outline' }
> = {
  ready: { label: 'Ready', variant: 'success' },
  rolling: { label: 'Rolling out', variant: 'warning' },
  failed: { label: 'Rollout failed', variant: 'destructive' },
  absent: { label: 'Not running', variant: 'outline' },
};

const UPDATE: Record<Resource['update'], string> = {
  auto: 'auto',
  manual: 'manual',
  onstart: 'on start',
};

function ResourceRow({
  project,
  resource,
  onChanged,
}: {
  project: string;
  resource: Resource;
  onChanged: () => Promise<void>;
}) {
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const rollout = ROLLOUT[resource.rollout];
  const rollOut = async () => {
    setBusy(true);
    setError(null);
    const response = await fetch(
      `/api/programs/${encodeURIComponent(project)}/${encodeURIComponent(resource.name)}/rollout`,
      { method: 'POST' }
    ).catch(e => e as Error);
    if (response instanceof Error) {
      setError(String(response));
    } else if (!response.ok) {
      setError(await errorOf(response));
    }
    await onChanged();
    setBusy(false);
  };
  return (
    <tr className="border-t">
      <td className="py-2.5 pr-4 font-mono text-[13px] font-medium">{resource.name}</td>
      <td className="py-2.5 pr-4 text-muted-foreground">{UPDATE[resource.update]}</td>
      <td className="py-2.5 pr-4">
        <Commit commit={resource.commit} />
      </td>
      <td className="py-2.5 pr-4">
        <Badge variant={rollout.variant}>{rollout.label}</Badge>
      </td>
      <td className="py-2.5">
        {resource.available == null ? (
          <span className="text-muted-foreground">Up to date</span>
        ) : resource.update === 'manual' ? (
          <div className="flex items-center gap-2">
            <Button size="sm" variant="outline" disabled={busy} onClick={() => void rollOut()}>
              <Rocket />
              Roll out {resource.available.slice(0, 7)}
            </Button>
            {error != null && <span className="text-xs text-destructive">{error}</span>}
          </div>
        ) : (
          <span className="text-muted-foreground">
            Rolling to <Commit commit={resource.available} />
          </span>
        )}
      </td>
    </tr>
  );
}

function ProjectCard({
  project,
  onChanged,
}: {
  project: Project;
  onChanged: () => Promise<void>;
}) {
  return (
    <section className="rounded-lg border bg-card">
      <header className="flex flex-wrap items-baseline gap-x-3 gap-y-1 border-b px-4 py-3">
        <h2 className="text-[15px] font-semibold">{project.name}</h2>
        <span className="inline-flex items-center gap-1 text-sm text-muted-foreground">
          <GitBranch className="size-3.5" />
          {project.branch}
          {project.path !== '' && <span className="font-mono">/{project.path}</span>}
        </span>
        <span className="truncate text-xs text-muted-foreground">{project.url}</span>
        <span className="ml-auto text-sm">
          Runs <Commit commit={project.commit} />
        </span>
      </header>
      {project.failed != null && (
        <div className="flex gap-2 border-b bg-destructive/10 px-4 py-3 text-sm">
          <AlertTriangle className="mt-0.5 size-4 shrink-0 text-destructive" />
          <div className="min-w-0">
            <div>
              Commit <Commit commit={project.failed.commit} /> failed discovery, so it is not
              rolled out.
            </div>
            <pre className="mt-1 overflow-x-auto font-mono text-xs whitespace-pre-wrap text-muted-foreground">
              {project.failed.error}
            </pre>
          </div>
        </div>
      )}
      {project.resources.length === 0 ? (
        <p className="px-4 py-3 text-sm text-muted-foreground">
          {project.commit == null ? 'Not synced yet.' : 'This commit declares no resources.'}
        </p>
      ) : (
        <table className="w-full text-sm">
          <thead>
            <tr className="text-left text-xs text-muted-foreground">
              <th className="py-2 pr-4 pl-4 font-medium">Resource</th>
              <th className="py-2 pr-4 font-medium">Update</th>
              <th className="py-2 pr-4 font-medium">Running</th>
              <th className="py-2 pr-4 font-medium">Rollout</th>
              <th className="py-2 font-medium">Update available</th>
            </tr>
          </thead>
          <tbody className="[&_td:first-child]:pl-4">
            {project.resources.map(resource => (
              <ResourceRow
                key={resource.name}
                project={project.name}
                resource={resource}
                onChanged={onChanged}
              />
            ))}
          </tbody>
        </table>
      )}
    </section>
  );
}

export function ProgramsPage({
  programs,
  onChanged,
}: {
  programs: Programs;
  onChanged: () => Promise<void>;
}) {
  return (
    <ScrollArea className="min-h-0 flex-1">
      <div className="mx-auto flex max-w-5xl flex-col gap-4 p-6">
        {programs.state === 'error' && (
          <p className="text-sm text-destructive">{programs.error}</p>
        )}
        {programs.state === 'ok' &&
          programs.projects.map(project => (
            <ProjectCard key={project.name} project={project} onChanged={onChanged} />
          ))}
        {programs.state === 'ok' && programs.projects.length === 0 && (
          <p className="text-sm text-muted-foreground">
            No projects: list them in the chart's <code>control.projects</code>.
          </p>
        )}
      </div>
    </ScrollArea>
  );
}
