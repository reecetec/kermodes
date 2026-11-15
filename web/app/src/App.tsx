import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { LoaderCircle, Moon, Search, Sun, Table2 } from 'lucide-react';
import {
  type ConnectionStatus,
  type TableInfo,
  type KermodesConnection,
} from '@kermodes/client';
import { gridThemeOf } from '@kermodes/grid';
import { CatalogPage } from '@/Catalog';
import { DEPLOYMENTS_VIEW, DeploymentsPage } from '@/Deployments';
import { Button, buttonVariants } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { ScrollArea } from '@/components/ui/scroll-area';
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from '@/components/ui/tooltip';
import { Logo } from '@/components/Logo';
import { cn } from '@/lib/utils';
import { TABLE_MIME, TablesWorkspace, type WorkspaceHandle } from '@/tables/Workspace';

const THEME_KEY = 'kermodes-theme';

type ThemeName = 'dark' | 'light';

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

/** The apps built on Kermodes this server serves at `/apps/<name>/` (`--apps`) */
function useApps(): string[] {
  const [apps, setApps] = useState<string[]>([]);
  useEffect(() => {
    let live = true;
    fetch('/api/apps')
      .then(response => (response.ok ? (response.json() as Promise<string[]>) : []))
      .then(names => live && setApps(names))
      .catch(() => {});
    return () => {
      live = false;
    };
  }, []);
  return apps;
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
  const apps = useApps();
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
        {apps.length > 0 && <span className="mx-1 h-4 w-px bg-border" aria-hidden />}
        {apps.map(name => (
          <a key={name} href={`/apps/${name}/`} className={buttonVariants({ variant: 'ghost', size: 'sm' })}>
            {name}
          </a>
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
                // Dragged onto the workspace, a table opens where it is dropped
                draggable
                onDragStart={event => {
                  event.dataTransfer.setData(TABLE_MIME, table.name);
                  event.dataTransfer.effectAllowed = 'copy';
                }}
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

export default function App({ connection }: { connection: KermodesConnection }) {
  useConnection(connection);
  const [themeName, toggleTheme] = useThemeName();
  const [activeTable, setActiveTable] = useState<string | null>(null);
  const workspace = useRef<WorkspaceHandle>(null);
  // The canvas draws with the tokens in effect on <html>; recomputed when the theme flips
  const theme = useMemo(() => gridThemeOf(document.documentElement), [themeName]);
  // Deployments is the main tab wherever a controller reports them, until someone picks a tab.
  const hasDeployments = connection.pages.includes(DEPLOYMENTS_VIEW);
  const hasCatalog = useHasCatalog();
  const pages: Page[] = [...(hasCatalog ? (['catalog'] as const) : []), 'tables'];
  const [picked, setPicked] = useState<Page | null>(null);
  const page: Page = picked ?? (hasDeployments ? 'deployments' : 'tables');

  const openTable = useCallback((table: string) => workspace.current?.open(table), []);

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
        {/* Hidden rather than unmounted, so open tables keep their views and layout. */}
        <div className={cn('flex min-h-0 flex-1', page !== 'tables' && 'hidden')}>
          <Sidebar
            tables={connection.tables}
            activeTable={activeTable}
            isConnected={connection.status === 'open'}
            onOpen={openTable}
          />
          <main className="flex min-w-0 flex-1 flex-col">
            <TablesWorkspace
              ref={workspace}
              connection={connection}
              gridTheme={theme}
              isShown={page === 'tables'}
              onActive={setActiveTable}
            />
          </main>
        </div>
      </div>
    </TooltipProvider>
  );
}
