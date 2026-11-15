import { Children, Fragment, type ReactNode } from 'react';
import {
  Group,
  type LayoutStorage,
  Panel as Pane,
  Separator,
  useDefaultLayout,
} from 'react-resizable-panels';
import type { TableStatus } from './rows';

/*
 * The layout of a dashboard (docs/theme.md): panels that meet edge to edge, each a thin header
 * over content that fills it, split and resized by hand, the figures in a strip. Styled by
 * @kermodes/theme's layout.css, so they look right whatever else a page uses.
 *
 *     <Dashboard>
 *       <Stats><Stat label="Ticks" value="8,256,132" /></Stats>
 *       <Split id="market" sizes={[60, 40]}>
 *         <Panel title="Board"><Table name="board" /></Panel>
 *         <Panel title="Tape"><Table name="md.ticks" follow /></Panel>
 *       </Split>
 *     </Dashboard>
 */

/** The whole page: its children stacked, a strip of figures or a split filling the rest */
export function Dashboard({ children }: { children: ReactNode }) {
  return <div className="km-dashboard">{children}</div>;
}

/** A titled pane: a thin header (its title, and `actions` at the right) over `children`, which fill it */
export function Panel({ title, actions, children }: { title: ReactNode; actions?: ReactNode; children: ReactNode }) {
  return (
    <section className="km-panel">
      <header className="km-panel-header">
        <span className="km-panel-title">{title}</span>
        {actions != null && <div className="km-panel-actions">{actions}</div>}
      </header>
      <div className="km-panel-body">{children}</div>
    </section>
  );
}

// Where a split without an id keeps its sizes: nowhere, so each load starts from `sizes`.
const forgotten: LayoutStorage = { getItem: () => null, setItem: () => undefined };

/**
 * Its children side by side (`horizontal`) or stacked (`vertical`), a hairline between each that
 * a person drags to resize. `sizes` are percentages; with an `id`, the sizes a person chooses
 * are kept in the browser and come back on the next load.
 */
export function Split({
  id,
  direction = 'horizontal',
  sizes,
  children,
}: {
  id?: string;
  direction?: 'horizontal' | 'vertical';
  sizes?: number[];
  children: ReactNode;
}) {
  const parts = Children.toArray(children);
  const kept = useDefaultLayout({
    id: `kermodes-split-${id ?? 'unnamed'}`,
    storage: id == null ? forgotten : localStorage,
  });
  return (
    <Group
      orientation={direction}
      className={`km-split km-split-${direction}`}
      defaultLayout={kept.defaultLayout}
      onLayoutChanged={kept.onLayoutChanged}
    >
      {parts.map((part, i) => (
        <Fragment key={i}>
          {i > 0 && <Separator className="km-separator" />}
          <Pane id={String(i)} defaultSize={sizes?.[i] == null ? undefined : `${sizes[i]}%`} minSize="5%">
            {part}
          </Pane>
        </Fragment>
      ))}
    </Group>
  );
}

/** A strip of figures across the page or a panel */
export function Stats({ children }: { children: ReactNode }) {
  return <div className="km-stats">{children}</div>;
}

/**
 * A figure: its label over its value, in the mono. With `sign`, the value is coloured by it, as
 * the grid colours numbers (a change, a P&L).
 */
export function Stat({ label, value, sign }: { label: ReactNode; value: ReactNode; sign?: number }) {
  const tone = sign == null || sign === 0 || Number.isNaN(sign) ? undefined : sign > 0 ? 'positive' : 'negative';
  return (
    <div className="km-stat">
      <span className="km-stat-label">{label}</span>
      <span className="km-stat-value" data-sign={tone}>
        {value}
      </span>
    </div>
  );
}

/**
 * Nothing while a table is live (docs/theme.md: normal looks normal); says so when it is not,
 * as a panel's action or beside a figure.
 */
export function Status({ status }: { status: TableStatus }) {
  if (status === 'live' || status === 'static') {
    return null;
  }
  return (
    <span className="km-status" data-status={status}>
      {status === 'failed' ? 'Unavailable' : status === 'reconnecting' ? 'Reconnecting' : 'Connecting'}
    </span>
  );
}
