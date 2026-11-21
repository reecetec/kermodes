import {
  Children,
  createContext,
  isValidElement,
  useCallback,
  useContext,
  type ReactElement,
  type ReactNode,
} from 'react';
import {
  DockviewDefaultTab,
  DockviewReact,
  type DockviewReadyEvent,
  type DockviewTheme,
  type IDockviewHeaderActionsProps,
  type IDockviewPanelHeaderProps,
  type IDockviewPanelProps,
  type SerializedDockview,
} from 'dockview-react';
// dockview's own geometry; its colours are the tokens (@kermodes/theme's layout.css)
import 'dockview-react/dist/styles/dockview.css';
import { dockLayout, type LayoutNode } from './dock';
import type { TableStatus } from './rows';

/*
 * The layout of a dashboard (docs/theme.md): panels that meet edge to edge, each a tab over
 * content that fills it, the figures in a strip. The page's Splits and Panels are where its
 * panels start; a person then resizes them, drags one beside or into another's tabs, and the
 * browser keeps what they made of it. Styled by @kermodes/theme's layout.css.
 *
 *     <Dashboard>
 *       <Stats><Stat label="Ticks" value="8,256,132" /></Stats>
 *       <Split sizes={[60, 40]}>
 *         <Panel title="Board"><Table name="board" /></Panel>
 *         <Panel title="Tape"><Table name="md.ticks" follow /></Panel>
 *       </Split>
 *     </Dashboard>
 */

type PanelProps = { title: string; actions?: ReactNode; children: ReactNode };
type SplitProps = {
  direction?: 'horizontal' | 'vertical';
  /** Percentages, one per child; equal shares without */
  sizes?: number[];
  children: ReactNode;
};

/**
 * A titled pane in a Dashboard: its tab says `title` (unique on the page), `actions` show at the
 * right of its group's tabs while it is the one shown, and `children` fill it.
 */
export function Panel(_: PanelProps): ReactNode {
  throw new Error('A Panel is laid out by its Dashboard: put it inside one');
}

/** Its children side by side (`horizontal`) or stacked (`vertical`) in a Dashboard, at `sizes` */
export function Split(_: SplitProps): ReactNode {
  throw new Error('A Split is laid out by its Dashboard: put it inside one');
}

const isLayout = (node: ReactNode): node is ReactElement<PanelProps | SplitProps> =>
  isValidElement(node) && (node.type === Panel || node.type === Split);

/** The tree a page's Splits and Panels describe, each panel's content kept by its id */
function describe(element: ReactElement<PanelProps | SplitProps>, bodies: Map<string, PanelProps>): LayoutNode {
  if (element.type === Panel) {
    const props = element.props as PanelProps;
    if (bodies.has(props.title)) {
      throw new Error(`Two panels are titled "${props.title}": a dashboard's panel titles are unique`);
    }
    bodies.set(props.title, props);
    return { kind: 'panel', id: props.title, title: props.title };
  }
  const props = element.props as SplitProps;
  const children = Children.toArray(props.children);
  if (!children.every(isLayout)) {
    throw new Error('A Split holds only Panels and Splits');
  }
  return {
    kind: 'split',
    direction: props.direction ?? 'horizontal',
    sizes: props.sizes,
    children: children.map(child => describe(child, bodies)),
  };
}

const Bodies = createContext<Map<string, PanelProps>>(new Map());

function PanelBody({ api }: IDockviewPanelProps) {
  return <div className="km-panel-body">{useContext(Bodies).get(api.id)?.children}</div>;
}

function PanelActions({ activePanel }: IDockviewHeaderActionsProps) {
  const actions = useContext(Bodies).get(activePanel?.id ?? '')?.actions;
  return actions == null ? null : <div className="km-panel-actions">{actions}</div>;
}

// An app's panels are its own: they move, but do not close.
function PanelTab(props: IDockviewPanelHeaderProps) {
  return <DockviewDefaultTab {...props} hideClose />;
}

const COMPONENTS = { panel: PanelBody };
const THEME: DockviewTheme = { name: 'kermodes', className: 'dockview-theme-light dockview-theme-kermodes' };

/** The layout this browser keeps for the page, if it has the same panels the page describes */
function kept(key: string, layout: SerializedDockview): SerializedDockview | null {
  try {
    const saved = JSON.parse(localStorage.getItem(key) ?? 'null') as SerializedDockview | null;
    const titles = (panels: SerializedDockview['panels']) =>
      JSON.stringify(Object.values(panels).map(p => [p.id, p.title]).sort());
    return saved != null && titles(saved.panels) === titles(layout.panels) ? saved : null;
  } catch {
    return null;
  }
}

function Dock({ root }: { root: ReactElement<PanelProps | SplitProps> }) {
  const bodies = new Map<string, PanelProps>();
  const tree = describe(root, bodies);
  // A change to the page's panels (a new one, a new title) lays it out afresh from the page.
  const shape = JSON.stringify(tree);
  const onReady = useCallback(
    ({ api }: DockviewReadyEvent) => {
      const key = `kermodes-dashboard:${location.pathname}`;
      const layout = dockLayout(JSON.parse(shape) as LayoutNode);
      const saved = kept(key, layout);
      try {
        api.fromJSON(saved ?? layout);
      } catch {
        // A layout from an older dockview, or edited by hand: the page's own instead
        api.fromJSON(layout);
      }
      api.onDidLayoutChange(() => localStorage.setItem(key, JSON.stringify(api.toJSON())));
    },
    [shape]
  );
  return (
    <Bodies.Provider value={bodies}>
      <DockviewReact
        key={shape}
        className="km-dock"
        theme={THEME}
        components={COMPONENTS}
        defaultTabComponent={PanelTab}
        rightHeaderActionsComponent={PanelActions}
        disableFloatingGroups
        onReady={onReady}
      />
    </Bodies.Provider>
  );
}

/**
 * The whole page: its children stacked, the Split or Panel among them filling the rest as
 * panels a person can rearrange.
 */
export function Dashboard({ children }: { children: ReactNode }) {
  return (
    <div className="km-dashboard">
      {Children.map(children, child => (isLayout(child) ? <Dock root={child} /> : child))}
    </div>
  );
}

/** A strip of figures across the page or a panel */
export function Stats({ children }: { children: ReactNode }) {
  return <div className="km-stats">{children}</div>;
}

/**
 * A figure: its label over its value, in the mono. With `sign`, the value is coloured by it, as
 * the grid colours numbers (a change, a P&L). With `status`, the status of the table the value
 * comes from (`useTable`): until it has loaded, a placeholder where the value goes, so a figure
 * not yet known never reads as a 0 or a dash.
 */
export function Stat({
  label,
  value,
  sign,
  status,
}: {
  label: ReactNode;
  value: ReactNode;
  sign?: number;
  status?: TableStatus;
}) {
  const tone = sign == null || sign === 0 || Number.isNaN(sign) ? undefined : sign > 0 ? 'positive' : 'negative';
  return (
    <div className="km-stat">
      <span className="km-stat-label">{label}</span>
      {status === 'loading' ? (
        <span className="km-stat-value" aria-busy="true" aria-label="Loading">
          <span className="km-stat-placeholder" />
        </span>
      ) : (
        <span className="km-stat-value" data-sign={tone}>
          {value}
        </span>
      )}
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
