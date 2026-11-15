import { useCallback, useEffect, useMemo, useState } from 'react';
import { LoaderCircle, Moon, Search, Sun, Table2, TableProperties, X } from 'lucide-react';
import {
  type ConnectionStatus,
  type SocketView,
  type TableInfo,
  type KermodesConnection,
} from '@kermodes/client';
import { TableViewer, gridThemeOf } from '@kermodes/grid';
import { CatalogPage } from '@/Catalog';
import { DEPLOYMENTS_VIEW, DeploymentsPage } from '@/Deployments';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { ScrollArea } from '@/components/ui/scroll-area';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from '@/components/ui/tooltip';
import { Logo } from '@/components/Logo';
import { cn } from '@/lib/utils';

const THEME_KEY = 'kermodes-theme';

type ThemeName = 'dark' | 'light';

type Tab = { key: string; view: SocketView };
type Page = 'deployments' | 'catalog' | 'tables';

const PAGE_LABEL: Record<Page, string> = {
  deployments: 'Deployments',
  catalog: 'Catalog',
  tables: 'Tables',
};

/** Re-render when the connection's status, tables, or last error change */
function useConnection(connection: KermodesConnection): void {
  const [, setVersion] = useState(0);
  useEffect(
    () => connection.onChange(() => setVersion(version => version + 1)),
    [connection]
  );
}

/** The saved theme, applied to <html> before the first render (docs/theme.md) */
export function savedTheme(): ThemeName {
  return localStorage.getItem(THEME_KEY) === 'light' ? 'light' : 'dark';
}

export function applyTheme(name: ThemeName): void {
  document.documentElement.dataset.theme = name;
  localStorage.setItem(THEME_KEY, name);
}

/**
 * The theme and its toggle. The attribute changes before the state does, so the grid's
 * `gridThemeOf` reads the new tokens in the same render.
 */
function useThemeName(): [ThemeName, () => void] {
  const [name, setName] = useState<ThemeName>(savedTheme);
  const toggle = useCallback(() => {
    const next = name === 'dark' ? 'light' : 'dark';
    applyTheme(next);
    setName(next);
  }, [name]);
  return [name, toggle];
}

/** How long the socket may be away before the top bar says so: a page load connects sooner */
const NOTICE_DELAY_MS = 600;

/**
 * Nothing while connected (docs/theme.md: normal looks normal); a quiet notice with a spinner
 * once the socket has been away for a moment. Open views dim on their own meanwhile.
 */
function ConnectionNotice({
  status,
  error,
}: {
  status: ConnectionStatus;
  error: string | null;
}) {
  const [isShown, setIsShown] = useState(false);
  useEffect(() => {
    if (status === 'open') {
      setIsShown(false);
      return undefined;
    }
    const timer = setTimeout(() => setIsShown(true), NOTICE_DELAY_MS);
    return () => clearTimeout(timer);
  }, [status]);
  if (!isShown) {
    return null;
  }
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <span role="status" className="inline-flex items-center gap-2 text-xs text-muted-foreground">
          <LoaderCircle className="size-3.5 animate-spin" />
          {status === 'connecting' ? 'Connecting' : 'Reconnecting'}
        </span>
      </TooltipTrigger>
      <TooltipContent side="bottom">
        {error ?? `Live updates resume when ${location.host} answers`}
      </TooltipContent>
    </Tooltip>
  );
}

/** Whether the server has a store, and so a catalog: one without answers 404 */
function useHasCatalog(): boolean {
  const [has, setHas] = useState(false);
  useEffect(() => {
    let live = true;
    fetch('/api/catalog')
      .then(response => live && setHas(response.status !== 404))
      .catch(() => {});
    return () => {
      live = false;
    };
  }, []);
  return has;
}

function TopBar({
  connection,
  themeName,
  onToggleTheme,
  page,
  onPage,
  pages,
  home,
}: {
  connection: KermodesConnection;
  themeName: ThemeName;
  onToggleTheme: () => void;
  page: Page;
  onPage: (page: Page) => void;
  pages: readonly Page[];
  /** Whether the brand opens Deployments, the home page wherever a controller runs */
  home: boolean;
}) {
  return (
    <header className="flex h-12 shrink-0 items-center gap-3 border-b bg-sidebar px-4">
      {home ? (
        <button
          type="button"
          onClick={() => onPage('deployments')}
          aria-current={page === 'deployments' ? 'page' : undefined}
          title="Deployments"
          className={cn(
            '-ml-2 flex h-8 items-center gap-2 rounded-md px-2 hover:bg-accent',
            page === 'deployments' && 'bg-secondary'
          )}
        >
          <Logo className="size-6" />
          <span className="text-[15px] font-semibold tracking-tight">Kermodes</span>
        </button>
      ) : (
        <div className="flex items-center gap-2">
          <Logo className="size-6" />
          <span className="text-[15px] font-semibold tracking-tight">Kermodes</span>
        </div>
      )}
      <nav className="flex items-center gap-1">
        {pages.map(name => (
          <Button
            key={name}
            variant={page === name ? 'secondary' : 'ghost'}
            size="sm"
            onClick={() => onPage(name)}
          >
            {PAGE_LABEL[name]}
          </Button>
        ))}
      </nav>
      <div className="ml-auto flex items-center gap-2">
        <ConnectionNotice status={connection.status} error={connection.lastError} />
        <Tooltip>
          <TooltipTrigger asChild>
            <Button
              variant="ghost"
              size="icon-sm"
              onClick={onToggleTheme}
              aria-label="Toggle theme"
            >
              {themeName === 'dark' ? <Sun /> : <Moon />}
            </Button>
          </TooltipTrigger>
          <TooltipContent side="bottom">
            {themeName === 'dark' ? 'Light theme' : 'Dark theme'}
          </TooltipContent>
        </Tooltip>
      </div>
    </header>
  );
}

function Sidebar({
  tables,
  activeTable,
  isConnected,
  onOpen,
}: {
  tables: readonly TableInfo[];
  activeTable: string | null;
  isConnected: boolean;
  onOpen: (table: string) => void;
}) {
  const [query, setQuery] = useState('');
  const shown = tables.filter(table =>
    table.name.toLowerCase().includes(query.trim().toLowerCase())
  );
  return (
    <aside className="flex w-72 shrink-0 flex-col border-r bg-sidebar">
      <div className="space-y-3 p-3">
        <div className="flex items-baseline justify-between px-1">
          <span className="text-sm font-medium">Tables</span>
          <span className="text-xs text-muted-foreground tabular-nums">{tables.length}</span>
        </div>
        <div className="relative">
          <Search className="pointer-events-none absolute top-2 left-2.5 size-4 text-muted-foreground" />
          <Input
            value={query}
            onChange={event => setQuery(event.target.value)}
            placeholder="Search"
            aria-label="Search tables"
            className="h-8 pl-8"
          />
        </div>
      </div>
      <ScrollArea className="min-h-0 flex-1">
        <ul className="space-y-0.5 px-2 pb-3">
          {shown.map(table => (
            <li key={table.name}>
              <button
                type="button"
                onClick={() => onOpen(table.name)}
                aria-current={activeTable === table.name ? 'true' : undefined}
                className={cn(
                  'group flex w-full items-center gap-2.5 rounded-md px-2.5 py-1.5 text-left transition-colors hover:bg-accent',
                  activeTable === table.name && 'bg-secondary hover:bg-secondary'
                )}
              >
                <Table2
                  className={cn(
                    'size-4 shrink-0 text-muted-foreground',
                    activeTable === table.name && 'text-primary'
                  )}
                />
                <span className="min-w-0 flex-1">
                  <span className="block truncate font-mono text-[13px]">
                    {table.name}
                  </span>
                  <span className="block text-xs text-muted-foreground">
                    {table.columns.length}{' '}
                    {table.columns.length === 1 ? 'column' : 'columns'}
                  </span>
                </span>
              </button>
            </li>
          ))}
          {shown.length === 0 && (
            <li className="px-3 py-8 text-center text-sm text-muted-foreground">
              {!isConnected
                ? 'Waiting for the server'
                : tables.length === 0
                  ? 'No tables yet'
                  : 'No table matches'}
            </li>
          )}
        </ul>
      </ScrollArea>
    </aside>
  );
}

function EmptyState() {
  return (
    <div className="flex flex-1 flex-col items-center justify-center gap-3 text-center">
      <div className="flex size-12 items-center justify-center rounded-xl border bg-card">
        <TableProperties className="size-6 text-muted-foreground" />
      </div>
      <div>
        <p className="font-medium">No table open</p>
        <p className="text-sm text-muted-foreground">
          Pick a published table on the left to view it live.
        </p>
      </div>
    </div>
  );
}

export default function App({ connection }: { connection: KermodesConnection }) {
  useConnection(connection);
  const [themeName, toggleTheme] = useThemeName();
  const [tabs, setTabs] = useState<Tab[]>([]);
  const [active, setActive] = useState<string | null>(null);
  // The canvas draws with the tokens in effect on <html>; recomputed when the theme flips
  const theme = useMemo(() => gridThemeOf(document.documentElement), [themeName]);
  // Deployments is the main tab wherever a controller reports them, until someone picks a tab.
  const hasDeployments = connection.pages.includes(DEPLOYMENTS_VIEW);
  const hasCatalog = useHasCatalog();
  const pages: Page[] = [...(hasCatalog ? (['catalog'] as const) : []), 'tables'];
  const [picked, setPicked] = useState<Page | null>(null);
  const page: Page = picked ?? (hasDeployments ? 'deployments' : 'tables');

  const openTable = useCallback(
    (table: string) => {
      const existing = tabs.find(tab => tab.view.table === table);
      if (existing != null) {
        setActive(existing.key);
        return;
      }
      const view = connection.open(table);
      const key = `${view.id}`;
      setTabs(current => [...current, { key, view }]);
      setActive(key);
    },
    [connection, tabs]
  );

  const closeTab = useCallback(
    (key: string) => {
      const index = tabs.findIndex(tab => tab.key === key);
      if (index < 0) {
        return;
      }
      tabs[index].view.close();
      const remaining = tabs.filter(tab => tab.key !== key);
      setTabs(remaining);
      if (active === key) {
        setActive(remaining[Math.min(index, remaining.length - 1)]?.key ?? null);
      }
    },
    [tabs, active]
  );

  const activeTable = useMemo(
    () => tabs.find(tab => tab.key === active)?.view.table ?? null,
    [tabs, active]
  );

  // Only the table on screen gets windows: hidden tabs, and every tab while another page is
  // open, pause their views, keeping their sort and filters.
  useEffect(() => {
    for (const tab of tabs) {
      if (page === 'tables' && tab.key === active) {
        tab.view.resume();
      } else {
        tab.view.pause();
      }
    }
  }, [tabs, active, page]);

  return (
    <TooltipProvider>
      <div className="flex h-full flex-col">
        <TopBar
          connection={connection}
          themeName={themeName}
          onToggleTheme={toggleTheme}
          page={page}
          onPage={setPicked}
          pages={pages}
          home={hasDeployments}
        />
        {page === 'deployments' && <DeploymentsPage connection={connection} />}
        {page === 'catalog' && (
          <CatalogPage
            onOpen={table => {
              openTable(table);
              setPicked('tables');
            }}
          />
        )}
        {/* Hidden rather than unmounted, so open tables keep their views. */}
        <div className={cn('flex min-h-0 flex-1', page !== 'tables' && 'hidden')}>
          <Sidebar
            tables={connection.tables}
            activeTable={activeTable}
            isConnected={connection.status === 'open'}
            onOpen={openTable}
          />
          <main className="flex min-w-0 flex-1 flex-col">
            {tabs.length === 0 || active == null ? (
              <EmptyState />
            ) : (
              <Tabs
                value={active}
                onValueChange={setActive}
                className="min-h-0 flex-1 gap-0"
              >
                <TabsList className="h-11 w-full justify-start gap-1 overflow-x-auto border-b bg-sidebar px-2 pt-2">
                  {tabs.map(tab => (
                    <div key={tab.key} className="group relative flex h-full">
                      <TabsTrigger
                        value={tab.key}
                        className="h-full rounded-t-md border border-b-0 border-transparent pr-8 pl-3 font-mono text-[13px] text-muted-foreground hover:text-foreground data-[state=active]:border-border data-[state=active]:bg-background data-[state=active]:text-foreground"
                      >
                        <Table2 className="size-3.5" />
                        {tab.view.table}
                      </TabsTrigger>
                      <button
                        type="button"
                        aria-label={`Close ${tab.view.table}`}
                        onClick={() => closeTab(tab.key)}
                        className="absolute top-1/2 right-1.5 flex size-5 -translate-y-1/2 items-center justify-center rounded text-muted-foreground opacity-60 hover:bg-accent hover:text-foreground hover:opacity-100"
                      >
                        <X className="size-3.5" />
                      </button>
                    </div>
                  ))}
                </TabsList>
                <div className="relative min-h-0 flex-1">
                  {tabs.map(tab => (
                    <TabsContent
                      key={tab.key}
                      value={tab.key}
                      forceMount
                      className="absolute inset-0 data-[state=inactive]:invisible"
                    >
                      <TableViewer channel={tab.view} theme={theme} />
                    </TabsContent>
                  ))}
                </div>
              </Tabs>
            )}
          </main>
        </div>
      </div>
    </TooltipProvider>
  );
}
