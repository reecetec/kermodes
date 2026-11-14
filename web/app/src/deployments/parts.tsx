import { useEffect, useRef } from 'react';
import { ExternalLink, GitCommitHorizontal } from 'lucide-react';
import { Badge } from '@/components/ui/badge';
import { cn } from '@/lib/utils';
import { usePolled } from './data';
import { ago, bytes } from './model';

const STATUS_VARIANT: Record<string, 'success' | 'warning' | 'destructive' | 'outline'> = {
  Running: 'success',
  'Rolling out': 'warning',
  Starting: 'warning',
  Degraded: 'warning',
  'Not running': 'outline',
  Stopped: 'outline',
  Stopping: 'warning',
};

export function Status({ status }: { status: string }) {
  return <Badge variant={STATUS_VARIANT[status] ?? 'destructive'}>{status}</Badge>;
}

/** A status as a coloured dot, for dense places */
export function StatusDot({ status }: { status: string }) {
  const variant = STATUS_VARIANT[status] ?? 'destructive';
  return (
    <span
      title={status}
      className={cn(
        'inline-block size-2 shrink-0 rounded-full',
        variant === 'success' && 'bg-success',
        variant === 'warning' && 'bg-warning',
        variant === 'destructive' && 'bg-destructive',
        variant === 'outline' && 'bg-muted-foreground/50'
      )}
    />
  );
}

/**
 * A short commit, inline with the text around it. With `href` (where the code is declared at
 * that commit, on the git host) it opens there in a new tab.
 */
export function Commit({ commit, href }: { commit: string | null; href?: string | null }) {
  if (commit == null) return <span className="text-muted-foreground">—</span>;
  const content = (
    <>
      <GitCommitHorizontal className="mr-1 inline size-[1.1em] align-[-0.2em] text-muted-foreground" />
      {commit.slice(0, 7)}
    </>
  );
  if (href == null) {
    return <span className="font-mono whitespace-nowrap">{content}</span>;
  }
  return (
    <a
      href={href}
      target="_blank"
      rel="noopener noreferrer"
      title={`Open the code at ${commit.slice(0, 12)}`}
      onClick={event => event.stopPropagation()}
      className="group/commit font-mono whitespace-nowrap underline-offset-2 hover:text-primary hover:underline"
    >
      {content}
      <ExternalLink className="ml-1 inline size-3 align-[-0.1em] opacity-0 transition-opacity group-hover/commit:opacity-70" />
    </a>
  );
}

/**
 * Use as a line of its recent history over a bar of the latest: as a share of `limit` when
 * there is one, else (nothing declared) in `format`'s units against its own recent peak.
 */
export function Usage({
  value,
  limit,
  samples,
  format,
  width = 96,
}: {
  value: number;
  limit: number | null;
  samples: number[];
  format: (n: number) => string;
  width?: number;
}) {
  const height = 22;
  const sized = limit != null && limit > 0;
  const scale = sized ? limit : Math.max(value, ...samples) * 1.25 || 1;
  const pct = sized ? (value / limit) * 100 : null;
  const points = samples
    .map((v, i) => {
      const x = samples.length < 2 ? width : (i / (samples.length - 1)) * width;
      const y = height - Math.min(v / scale, 1) * height;
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    })
    .join(' ');
  const tone =
    pct == null
      ? 'text-muted-foreground'
      : pct >= 90
        ? 'text-destructive'
        : pct >= 70
          ? 'text-warning'
          : 'text-success';
  const title = sized
    ? `${format(value)} of ${format(limit)}`
    : `${format(value)}; no size declared`;
  return (
    <div className="flex items-center gap-2" title={title}>
      <svg width={width} height={height} className={cn('shrink-0', tone)} aria-hidden>
        <rect x={0} y={0} width={width} height={height} className="fill-muted/40" rx={3} />
        <rect
          x={0}
          y={height - 3}
          width={Math.min(value / scale, 1) * width}
          height={3}
          className="fill-current opacity-60"
          rx={1.5}
        />
        {samples.length > 1 && (
          <polyline points={points} fill="none" stroke="currentColor" strokeWidth={1.25} />
        )}
      </svg>
      <span className="min-w-11 text-right font-mono text-xs tabular-nums">
        {pct == null ? format(value) : `${pct.toFixed(0)}%`}
      </span>
    </div>
  );
}

/** A disk's use: its name, a bar of its size when known (red past 85%), and used of size */
export function DiskUse({ name, used, size }: { name: string; used: number; size: number | null }) {
  const share = size != null && size > 0 ? used / size : null;
  return (
    <span className="flex items-center gap-2 text-xs text-muted-foreground tabular-nums">
      {name}
      {share != null && (
        <span className="h-1.5 w-16 shrink-0 overflow-hidden rounded-full bg-muted">
          <span
            className={cn('block h-full', share > 0.85 ? 'bg-destructive' : 'bg-primary')}
            style={{ width: `${Math.min(100, 100 * share)}%` }}
          />
        </span>
      )}
      <span className="text-foreground">{bytes(used)}</span>
      {size != null && <span>of {bytes(size)}</span>}
    </span>
  );
}

export function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="grid grid-cols-[8rem_1fr] gap-2 border-b py-2 text-sm">
      <span className="text-muted-foreground">{label}</span>
      <span className="min-w-0 break-words">{children}</span>
    </div>
  );
}

type Event = {
  type: string;
  reason: string | null;
  message: string | null;
  count: number | null;
  at_ns: number;
  object: string;
};

const parseEvents = (r: Response) => r.json() as Promise<Event[]>;
const parseText = (r: Response) => r.text();

/** Kubernetes events from `url`, newest first, polled */
export function Events({ url }: { url: string }) {
  const { value, error } = usePolled(url, 5000, parseEvents);
  if (error != null) return <p className="p-4 text-sm text-destructive">{error}</p>;
  if (value == null) return <p className="p-4 text-sm text-muted-foreground">Loading…</p>;
  if (value.length === 0) return <p className="p-4 text-sm text-muted-foreground">No events.</p>;
  return (
    <table className="w-full text-sm">
      <tbody>
        {value.map((e, i) => (
          <tr key={i} className="border-b align-top">
            <td className="w-24 py-2 pr-3 pl-4 whitespace-nowrap text-muted-foreground">
              {ago(e.at_ns / 1e6)} ago
            </td>
            <td className="w-72 py-2 pr-3">
              <span className={cn('font-medium', e.type === 'Warning' && 'text-destructive')}>
                {e.reason}
              </span>
              {(e.count ?? 1) > 1 && <span className="text-muted-foreground"> ×{e.count}</span>}
              <div className="font-mono text-xs text-muted-foreground">{e.object}</div>
            </td>
            <td className="py-2 pr-4">{e.message}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

/** A log's tail from `url`, polled, kept scrolled to its end */
export function Logs({ url }: { url: string }) {
  const { value, error } = usePolled(url, 3000, parseText);
  const end = useRef<HTMLDivElement>(null);
  // A block body: scrollIntoView returns a Promise in newer browsers, and an effect may only
  // return its cleanup.
  useEffect(() => {
    end.current?.scrollIntoView({ block: 'end' });
  }, [value]);
  if (error != null) return <p className="p-4 text-sm text-destructive">{error}</p>;
  return (
    <pre className="p-4 font-mono text-xs leading-relaxed whitespace-pre-wrap">
      {value ?? 'Loading…'}
      <div ref={end} />
    </pre>
  );
}
