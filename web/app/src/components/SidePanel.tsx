import { useCallback, useRef, useState } from 'react';
import { PanelRightClose, PanelRightOpen } from 'lucide-react';
import { cn } from '@/lib/utils';

const MIN = 300;
/** What the panel always leaves the page beside it */
const PAGE_MIN = 480;

/** A remembered number in localStorage under `key` */
function useStored(key: string, initial: number) {
  const [value, setValue] = useState(() => Number(localStorage.getItem(key)) || initial);
  const store = useCallback(
    (next: number) => {
      setValue(next);
      localStorage.setItem(key, String(next));
    },
    [key]
  );
  return [value, store] as const;
}

/**
 * A panel docked to the right of a page: dragged (or arrow keys) to any width, double-click to
 * reset, folded to a thin strip and back. Its width and whether it is folded are remembered
 * under `name`.
 */
export function SidePanel({
  name,
  defaultWidth = 440,
  children,
}: {
  name: string;
  defaultWidth?: number;
  /** Given the fold button, to place in the panel's own header */
  children: (fold: React.ReactNode) => React.ReactNode;
}) {
  const [width, setWidth] = useStored(`kermodes.${name}.panel-width`, defaultWidth);
  const [folded, setFolded] = useStored(`kermodes.${name}.panel-folded`, 0);
  const [dragging, setDragging] = useState(false);
  const start = useRef({ x: 0, width: 0 });
  const aside = useRef<HTMLElement>(null);

  const resize = (next: number) => {
    const page = aside.current?.parentElement?.clientWidth ?? window.innerWidth;
    setWidth(Math.round(Math.max(MIN, Math.min(next, page - PAGE_MIN))));
  };

  if (folded) {
    return (
      <aside ref={aside} className="flex w-9 shrink-0 flex-col items-center border-l bg-sidebar py-2">
        <button
          type="button"
          aria-label="Show details"
          title="Show details"
          onClick={() => setFolded(0)}
          className="rounded p-1 text-muted-foreground hover:bg-accent hover:text-foreground"
        >
          <PanelRightOpen className="size-4" />
        </button>
      </aside>
    );
  }
  return (
    <>
      <div
        role="separator"
        aria-orientation="vertical"
        aria-label="Resize details"
        aria-valuenow={width}
        tabIndex={0}
        data-dragging={dragging}
        title="Drag to resize · double-click to reset"
        onPointerDown={event => {
          event.preventDefault();
          event.currentTarget.setPointerCapture(event.pointerId);
          start.current = { x: event.clientX, width };
          setDragging(true);
        }}
        onPointerMove={event => {
          if (dragging) resize(start.current.width + start.current.x - event.clientX);
        }}
        onPointerUp={() => setDragging(false)}
        onPointerCancel={() => setDragging(false)}
        onDoubleClick={() => resize(defaultWidth)}
        onKeyDown={event => {
          if (event.key === 'ArrowLeft' || event.key === 'ArrowRight') {
            event.preventDefault();
            resize(width + (event.key === 'ArrowLeft' ? 32 : -32));
          }
        }}
        className={cn(
          'w-1.5 shrink-0 cursor-col-resize border-l bg-muted/40 outline-none',
          'hover:bg-primary/30 focus-visible:bg-primary/40 data-[dragging=true]:bg-primary/50'
        )}
      />
      <aside
        ref={aside}
        style={{ width }}
        className="flex min-h-0 shrink-0 flex-col bg-sidebar"
      >
        {children(
          <button
            type="button"
            aria-label="Hide details"
            title="Hide details"
            onClick={() => setFolded(1)}
            className="rounded p-1 text-muted-foreground hover:bg-accent hover:text-foreground"
          >
            <PanelRightClose className="size-4" />
          </button>
        )}
      </aside>
    </>
  );
}
