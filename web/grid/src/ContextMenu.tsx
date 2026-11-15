import React, { Fragment, useEffect, useLayoutEffect, useRef, useState } from 'react';
import { type MenuAction, type MenuItem } from './menuItems';

export type ContextMenuProps = {
  // Where it was opened, relative to the positioned parent it is drawn in
  x: number;
  y: number;
  sections: MenuItem[][];
  onPick: (action: MenuAction) => void;
  onClose: () => void;
};

/**
 * A right-click menu over the grid. Escape, a click elsewhere, or scrolling
 * closes it; arrow keys move between items.
 */
export function ContextMenu({
  x,
  y,
  sections,
  onPick,
  onClose,
}: ContextMenuProps): React.ReactElement {
  const ref = useRef<HTMLDivElement>(null);
  const [position, setPosition] = useState({ left: x, top: y });

  // Keep it inside its parent: open up or left of the pointer near an edge
  useLayoutEffect(() => {
    const menu = ref.current;
    const parent = menu?.offsetParent;
    if (menu == null || !(parent instanceof HTMLElement)) {
      return;
    }
    setPosition({
      left: Math.max(0, x + menu.offsetWidth > parent.clientWidth ? x - menu.offsetWidth : x),
      top: Math.max(0, y + menu.offsetHeight > parent.clientHeight ? y - menu.offsetHeight : y),
    });
    menu.focus();
  }, [x, y]);

  useEffect(() => {
    const closeOutside = (event: Event): void => {
      if (!(event.target instanceof Node) || !ref.current?.contains(event.target)) {
        onClose();
      }
    };
    window.addEventListener('mousedown', closeOutside, true);
    window.addEventListener('wheel', closeOutside, true);
    window.addEventListener('blur', onClose);
    return () => {
      window.removeEventListener('mousedown', closeOutside, true);
      window.removeEventListener('wheel', closeOutside, true);
      window.removeEventListener('blur', onClose);
    };
  }, [onClose]);

  const handleKeyDown = (event: React.KeyboardEvent): void => {
    event.stopPropagation();
    if (event.key === 'Escape') {
      onClose();
      return;
    }
    if (event.key !== 'ArrowDown' && event.key !== 'ArrowUp') {
      return;
    }
    event.preventDefault();
    const items = [...(ref.current?.querySelectorAll<HTMLButtonElement>('[role=menuitem]') ?? [])];
    const at = items.findIndex(item => item === document.activeElement);
    const step = event.key === 'ArrowDown' ? 1 : -1;
    items[(at + step + items.length) % items.length]?.focus();
  };

  return (
    <div
      ref={ref}
      role="menu"
      tabIndex={-1}
      className="kermodes-menu"
      onKeyDown={handleKeyDown}
      onContextMenu={event => event.preventDefault()}
      style={position}
    >
      {sections.map((section, i) => (
        // Sections are rebuilt with the menu and never reorder
        // eslint-disable-next-line react/no-array-index-key
        <Fragment key={i}>
          {i > 0 && <div role="separator" className="kermodes-menu-separator" />}
          {section.map(item => (
            <button
              key={item.label}
              type="button"
              role="menuitem"
              className="kermodes-menu-item"
              onClick={() => {
                onClose();
                onPick(item.action);
              }}
            >
              <span>{item.label}</span>
              {item.hint != null && <span className="kermodes-menu-hint">{item.hint}</span>}
            </button>
          ))}
        </Fragment>
      ))}
    </div>
  );
}

export default ContextMenu;
