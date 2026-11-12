import { useCallback, useEffect, useMemo, useState } from 'react';
import {
  Moon,
  PawPrint,
  Search,
  Sun,
  Table2,
  TableProperties,
  X,
} from 'lucide-react';
import {
  type ConnectionStatus,
  type SocketView,
  type TableInfo,
  type KermodesConnection,
} from '@kermodes/client';
import { TableViewer, gridThemes, type GridThemeName } from '@kermodes/grid';
import { DEPLOYMENTS_VIEW, DeploymentsPage } from '@/Deployments';
import { Badge } from '@/components/ui/badge';
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
import { cn } from '@/lib/utils';

const THEME_KEY = 'kermodes-theme';

type Tab = { key: string; view: SocketView };
type Page = 'deployments' | 'tables';

/** Re-render when the connection's status, tables, or last error change */
function useConnection(connection: KermodesConnection): void {
  const [, setVersion] = useState(0);
  useEffect(
    () => connection.onChange(() => setVersion(version => version + 1)),
    [connection]
  );
}

function useThemeName(): [GridThemeName, () => void] {
  const [name, setName] = useState<GridThemeName>(() =>
    localStorage.getItem(THEME_KEY) === 'light' ? 'light' : 'dark'
  );
  useEffect(() => {
    document.documentElement.classList.toggle('dark', name === 'dark');
    localStorage.setItem(THEME_KEY, name);
  }, [name]);
  return [name, () => setName(current => (current === 'dark' ? 'light' : 'dark'))];
}

const STATUS_LABEL: Record<ConnectionStatus, string> = {
  open: 'Connected',
  connecting: 'Connecting…',
  closed: 'Reconnecting…',
};

function ConnectionPill({
  status,
  error,
}: {
  status: ConnectionStatus;
  error: string | null;
}) {
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <span className="inline-flex items-center gap-2 rounded-full border px-2.5 py-1 text-xs text-muted-foreground">
          <span
            className={cn(
              'size-2 rounded-full',
              status === 'open' && 'bg-success shadow-[0_0_6px] shadow-success/60',
              status === 'connecting' && 'animate-pulse bg-warning',
              status === 'closed' && 'animate-pulse bg-destructive'
            )}
          />
          {STATUS_LABEL[status]}
        </span>
      </TooltipTrigger>
      <TooltipContent side="bottom">
        {error ?? `${location.host} · live table updates over WebSocket`}
      </TooltipContent>
    </Tooltip>
  );
}

function TopBar({
  connection,
  themeName,
  onToggleTheme,
  page,
  onPage,
  hasDeployments,
}: {
  connection: KermodesConnection;
  themeName: GridThemeName;
  onToggleTheme: () => void;
  page: Page;
  onPage: (page: Page) => void;
  hasDeployments: boolean;
}) {
  return (
    <header className="flex h-12 shrink-0 items-center gap-3 border-b bg-sidebar px-4">
      <div className="flex size-7 items-center justify-center rounded-md bg-primary text-primary-foreground shadow-sm">
        <PawPrint className="size-4" />
      </div>
      <span className="text-[15px] font-semibold tracking-tight">Kermodes</span>
      {hasDeployments ? (
        <nav className="flex items-center gap-1">
          {(['deployments', 'tables'] as const).map(name => (
            <Button
              key={name}
              variant={page === name ? 'secondary' : 'ghost'}
              size="sm"
              onClick={() => onPage(name)}
            >
              {name === 'tables' ? 'Tables' : 'Deployments'}
            </Button>
          ))}
        </nav>
      ) : (
        <span className="text-sm text-muted-foreground">Tables</span>
      )}
      <div className="ml-auto flex items-center gap-2">
        <ConnectionPill status={connection.status} error={connection.lastError} />
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
        <div className="flex items-center justify-between px-1">
          <span className="text-xs font-semibold tracking-wider text-muted-foreground uppercase">
            Published tables
          </span>
          <Badge variant="secondary" className="tabular-nums">
            {tables.length}
          </Badge>
        </div>
        <div className="relative">
          <Search className="pointer-events-none absolute top-2.5 left-2.5 size-4 text-muted-foreground" />
          <Input
            value={query}
            onChange={event => setQuery(event.target.value)}
            placeholder="Filter tables…"
            className="h-9 pl-8"
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
                className={cn(
                  'group flex w-full items-center gap-2.5 rounded-md px-2.5 py-2 text-left transition-colors hover:bg-accent',
                  activeTable === table.name && 'bg-accent'
                )}
              >
                <Table2
                  className={cn(
                    'size-4 shrink-0 text-muted-foreground group-hover:text-foreground',
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
                {table.refreshing && (
                  <Badge variant="success" className="gap-1.5 px-1.5">
                    <span className="size-1.5 animate-pulse rounded-full bg-success" />
                    ticking
                  </Badge>
                )}
              </button>
            </li>
          ))}
          {shown.length === 0 && (
            <li className="px-3 py-8 text-center text-sm text-muted-foreground">
              {!isConnected
                ? 'Waiting for the session…'
                : tables.length === 0
                  ? 'No tables published yet.'
                  : 'No table matches.'}
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
  const theme = gridThemes[themeName];
  // Deployments is the main tab wherever a controller reports them, until someone picks a tab.
  const hasDeployments = connection.pages.includes(DEPLOYMENTS_VIEW);
  const [picked, setPicked] = useState<Page | null>(null);
  const page: Page = hasDeployments ? (picked ?? 'deployments') : 'tables';

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

  return (
    <TooltipProvider>
      <div className="flex h-full flex-col">
        <TopBar
          connection={connection}
          themeName={themeName}
          onToggleTheme={toggleTheme}
          page={page}
          onPage={setPicked}
          hasDeployments={hasDeployments}
        />
        {page === 'deployments' && <DeploymentsPage connection={connection} />}
        {/* Hidden rather than unmounted, so open tables keep their views. */}
        <div className={cn('flex min-h-0 flex-1', page === 'deployments' && 'hidden')}>
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
                      className="absolute inset-3 data-[state=inactive]:invisible"
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
