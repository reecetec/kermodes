import {
  createContext,
  forwardRef,
  useCallback,
  useContext,
  useEffect,
  useImperativeHandle,
  useRef,
  useState,
} from 'react';
import {
  DockviewReact,
  positionToDirection,
  type DockviewApi,
  type DockviewDidDropEvent,
  type DockviewReadyEvent,
  type DockviewTheme,
  type IDockviewPanelProps,
} from 'dockview-react';
import { TableProperties } from 'lucide-react';
import type { KermodesConnection, SocketView } from '@kermodes/client';
import { TableViewer, type GridTheme } from '@kermodes/grid';

/** The drag payload of a table dragged from the sidebar onto the workspace */
export const TABLE_MIME = 'application/x-kermodes-table';

const LAYOUT_KEY = 'kermodes-tables-layout';

/** dockview's look from the design tokens (`.dockview-theme-kermodes` in index.css) */
const THEME: DockviewTheme = {
  name: 'kermodes',
  className: 'dockview-theme-light dockview-theme-kermodes',
};

type Shared = {
  connection: KermodesConnection;
  gridTheme: GridTheme;
  /** Whether the Tables page is on screen: views on other pages are paused */
  isShown: boolean;
};

const SharedContext = createContext<Shared | null>(null);

const panelId = (table: string) => `table:${table}`;

/**
 * One table in a panel. The panel owns the table's view on the server: opened when the panel
 * is, closed with it, and paused whenever it is not on screen (another tab of its group, or
 * another page), keeping its sort and filters.
 */
function TablePanel({ api, params }: IDockviewPanelProps<{ table: string }>) {
  const shared = useContext(SharedContext);
  const [view, setView] = useState<SocketView | null>(null);
  const connection = shared?.connection;
  useEffect(() => {
    if (connection == null) {
      return undefined;
    }
    const opened = connection.open(params.table);
    setView(opened);
    return () => opened.close();
  }, [connection, params.table]);
  const isShown = shared?.isShown ?? false;
  useEffect(() => {
    if (view == null) {
      return undefined;
    }
    const apply = () => (isShown && api.isVisible ? view.resume() : view.pause());
    apply();
    const subscription = api.onDidVisibilityChange(apply);
    return () => subscription.dispose();
  }, [api, view, isShown]);
  if (view == null || shared == null) {
    return null;
  }
  return <TableViewer channel={view} theme={shared.gridTheme} />;
}

function Watermark() {
  return (
    <div className="flex h-full flex-col items-center justify-center gap-3 text-center">
      <TableProperties className="size-6 text-muted-foreground" />
      <div>
        <p className="text-sm font-medium">No table open</p>
        <p className="text-sm text-muted-foreground">
          Pick a table on the left, or drag one here
        </p>
      </div>
    </div>
  );
}

const COMPONENTS = { table: TablePanel };

export type WorkspaceHandle = {
  /** Shows `table`'s panel, opening it in the active group if it has none */
  open: (table: string) => void;
};

/** Restores the layout this browser last had, or starts empty */
function restore(api: DockviewApi): void {
  const saved = localStorage.getItem(LAYOUT_KEY);
  if (saved == null) {
    return;
  }
  try {
    api.fromJSON(JSON.parse(saved));
  } catch {
    // A layout from an older app, or edited by hand: start afresh rather than fail the page
    api.clear();
    localStorage.removeItem(LAYOUT_KEY);
  }
}

/**
 * The Tables page's workspace (docs/theme.md, "Layout"): every open table is a panel that can
 * be dragged into another's tabs, or beside, above, or below it, and the layout is kept per
 * browser. Tables dragged from the sidebar land where they are dropped.
 */
export const TablesWorkspace = forwardRef<
  WorkspaceHandle,
  Shared & { onActive: (table: string | null) => void }
>(function TablesWorkspace({ connection, gridTheme, isShown, onActive }, ref) {
  const apiRef = useRef<DockviewApi | null>(null);

  useImperativeHandle(ref, () => ({
    open(table) {
      const api = apiRef.current;
      if (api == null) {
        return;
      }
      const existing = api.getPanel(panelId(table));
      if (existing != null) {
        existing.api.setActive();
        return;
      }
      api.addPanel({ id: panelId(table), component: 'table', title: table, params: { table } });
    },
  }));

  const onReady = useCallback(
    ({ api }: DockviewReadyEvent) => {
      apiRef.current = api;
      restore(api);
      api.onDidLayoutChange(() => localStorage.setItem(LAYOUT_KEY, JSON.stringify(api.toJSON())));
      api.onDidActivePanelChange(({ panel }) => onActive(panel?.params?.table ?? null));
      onActive(api.activePanel?.params?.table ?? null);
      // Accept tables dragged from the sidebar
      api.onUnhandledDragOver(event => {
        if (
          event.nativeEvent instanceof DragEvent &&
          event.nativeEvent.dataTransfer?.types.includes(TABLE_MIME)
        ) {
          event.accept();
        }
      });
    },
    [onActive]
  );

  const onDidDrop = useCallback((event: DockviewDidDropEvent) => {
    const { nativeEvent } = event;
    const table =
      nativeEvent instanceof DragEvent ? nativeEvent.dataTransfer?.getData(TABLE_MIME) : '';
    if (table == null || table === '') {
      return;
    }
    const existing = event.api.getPanel(panelId(table));
    if (existing != null) {
      existing.api.setActive();
      return;
    }
    event.api.addPanel({
      id: panelId(table),
      component: 'table',
      title: table,
      params: { table },
      position:
        event.group == null
          ? { direction: positionToDirection(event.position) }
          : { referenceGroup: event.group, direction: positionToDirection(event.position) },
    });
  }, []);

  return (
    <SharedContext.Provider value={{ connection, gridTheme, isShown }}>
      <DockviewReact
        className="min-h-0 flex-1"
        theme={THEME}
        components={COMPONENTS}
        watermarkComponent={Watermark}
        onReady={onReady}
        onDidDrop={onDidDrop}
      />
    </SharedContext.Provider>
  );
});
