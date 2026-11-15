import React, { useEffect, useLayoutEffect, useRef, useState } from 'react';
import { type ShownValue } from './cellValue';

/** Where a cell is, relative to the positioned parent the popover is drawn in */
export type CellRect = { x: number; y: number; width: number; height: number };

export type ValuePopoverProps = {
  cell: CellRect;
  column: string;
  type: string;
  value: ShownValue;
  onCopy: (text: string) => void;
  onClose: () => void;
};

const GAP = 4;

/**
 * A cell's whole value under it (above it where there is no room below): JSON pretty-printed,
 * selectable, with Copy. Escape or a click elsewhere closes it.
 */
export function ValuePopover({
  cell,
  column,
  type,
  value,
  onCopy,
  onClose,
}: ValuePopoverProps): React.ReactElement {
  const ref = useRef<HTMLDivElement>(null);
  const [position, setPosition] = useState({ left: cell.x, top: cell.y + cell.height + GAP });

  // Keep it inside its parent, under the cell or else over it
  useLayoutEffect(() => {
    const popover = ref.current;
    const parent = popover?.offsetParent;
    if (popover == null || !(parent instanceof HTMLElement)) {
      return;
    }
    const below = cell.y + cell.height + GAP;
    const fitsBelow = below + popover.offsetHeight <= parent.clientHeight;
    setPosition({
      left: Math.max(GAP, Math.min(cell.x, parent.clientWidth - popover.offsetWidth - GAP)),
      top: fitsBelow ? below : Math.max(GAP, cell.y - popover.offsetHeight - GAP),
    });
    popover.focus();
  }, [cell.x, cell.y, cell.height]);

  useEffect(() => {
    const onPointerDown = (event: PointerEvent): void => {
      if (!ref.current?.contains(event.target as Node)) {
        onClose();
      }
    };
    window.addEventListener('pointerdown', onPointerDown, true);
    return () => window.removeEventListener('pointerdown', onPointerDown, true);
  }, [onClose]);

  return (
    <div
      ref={ref}
      role="dialog"
      aria-label={`Value of ${column}`}
      tabIndex={-1}
      className="kermodes-value"
      style={position}
      onKeyDown={event => {
        if (event.key === 'Escape') {
          event.stopPropagation();
          onClose();
        }
      }}
    >
      <div className="kermodes-value-head">
        <span className="kermodes-value-name">{column}</span>
        <span className="kermodes-value-type">{value.isJson ? 'JSON' : type}</span>
        <button type="button" className="kermodes-footer-button" onClick={() => onCopy(value.text)}>
          Copy
        </button>
        <button type="button" className="kermodes-footer-button" aria-label="Close" onClick={onClose}>
          ✕
        </button>
      </div>
      <pre className="kermodes-value-text">{value.text}</pre>
    </div>
  );
}

export default ValuePopover;
