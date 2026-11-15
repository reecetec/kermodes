import { useRef, useState } from 'react';
import { Maximize2 } from 'lucide-react';
import { shownValue } from '@kermodes/grid';
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover';
import { cn } from '@/lib/utils';

/**
 * A list cell's text, cut to its column. When it is cut, hovering shows a button that opens
 * the whole value (JSON pretty-printed) with Copy, as the table viewer's cells do.
 */
export function Clipped({ value, label, className }: { value: string; label: string; className?: string }) {
  const text = useRef<HTMLSpanElement>(null);
  const [clipped, setClipped] = useState(false);
  const [open, setOpen] = useState(false);
  const [copied, setCopied] = useState(false);
  // Measured on hover, so a resized column or a changed value is judged as it is now
  const measure = () => {
    const element = text.current;
    setClipped(element != null && element.scrollWidth > element.clientWidth);
  };
  const shown = open ? shownValue('Utf8', value) : null;
  return (
    <span className="group/clipped relative flex min-w-0 items-center" onPointerEnter={measure}>
      <span ref={text} className={cn('min-w-0 truncate', className)}>
        {value}
      </span>
      {(clipped || open) && (
        <Popover open={open} onOpenChange={setOpen}>
          <PopoverTrigger asChild>
            <button
              type="button"
              aria-label={`Show the whole ${label}`}
              // The row selects on click; opening a value must not
              onClick={event => event.stopPropagation()}
              className="absolute right-0 hidden h-5 w-5 items-center justify-center rounded-sm border bg-popover text-muted-foreground group-hover/clipped:flex hover:text-foreground data-[state=open]:flex"
            >
              <Maximize2 className="size-3" />
            </button>
          </PopoverTrigger>
          <PopoverContent
            className="flex max-h-80 w-[28rem] max-w-[90vw] flex-col"
            onClick={event => event.stopPropagation()}
          >
            <div className="flex items-center gap-2 border-b px-3 py-1.5 text-xs">
              <span className="font-medium">{label}</span>
              {shown?.isJson === true && <span className="text-muted-foreground">JSON</span>}
              <button
                type="button"
                className="ml-auto rounded-sm px-1.5 py-0.5 text-muted-foreground hover:bg-accent hover:text-foreground"
                onClick={() => {
                  void navigator.clipboard.writeText(shown?.text ?? value).then(() => {
                    setCopied(true);
                    setTimeout(() => setCopied(false), 1200);
                  });
                }}
              >
                {copied ? 'Copied' : 'Copy'}
              </button>
            </div>
            <pre className="min-h-0 overflow-auto px-3 py-2 font-mono text-xs whitespace-pre-wrap wrap-anywhere select-text">
              {shown?.text}
            </pre>
          </PopoverContent>
        </Popover>
      )}
    </span>
  );
}
