import { useEffect, useLayoutEffect, useRef, useState, type CSSProperties } from 'react';
import { TableViewer, gridThemeOf, type GridTheme } from '@kermodes/grid';
import { useKermodes, type Connection } from './index';

type View = ReturnType<Connection['open']>;

/**
 * A Kermodes table as the platform's own viewer shows it: live, the cells that change flashing,
 * any number of rows scrolled by the server, with sort, quick filters, and copy, in the page's
 * theme (@kermodes/theme's tokens). It fills its parent, which needs a height. An append table's
 * newest rows cost the server no sort, as they are read by position: with `reverse` they come
 * first, on top (a tape); with `follow` the table starts at its end and stays there as rows
 * arrive (a log).
 *
 *     <div className="h-96"><Table name="md.ticks" reverse /></div>
 */
export function Table({
  name,
  follow = false,
  reverse = false,
  className,
  style,
}: {
  name: string;
  follow?: boolean;
  reverse?: boolean;
  className?: string;
  style?: CSSProperties;
}) {
  const connection = useKermodes();
  const [view, setView] = useState<View | null>(null);
  useEffect(() => {
    const opened = connection.open(name);
    setView(opened);
    return () => {
      opened.close();
      setView(null);
    };
  }, [connection, name]);
  // The grid draws on a canvas: its colours are read from the page's tokens where it sits.
  const host = useRef<HTMLDivElement>(null);
  const [theme, setTheme] = useState<GridTheme | null>(null);
  useLayoutEffect(() => {
    if (host.current != null) {
      setTheme(gridThemeOf(host.current));
    }
  }, []);
  return (
    <div ref={host} className={className} style={{ position: 'relative', height: '100%', ...style }}>
      {view != null && theme != null && (
        <TableViewer channel={view} theme={theme} follow={follow} reverse={reverse} />
      )}
    </div>
  );
}
