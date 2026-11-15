// Adapted from @deephaven/grid (Apache-2.0), web-client-ui 0bab596 (Grid.scss)
// Injected by the component so every host (Vite app, anywidget bundle) gets it
// without a CSS pipeline. Colours are the design tokens (web/theme/tokens.css), which the host
// puts in scope; the canvas reads the same tokens through `gridThemeOf`.

const STYLE_ID = 'kermodes-grid-styles';

const GRID_CSS = `
.grid-wrapper {
  flex: 1 1 0;
  max-width: 100%;
  max-height: 100%;
  min-width: 0;
  min-height: 0;
  position: relative;
  font-family: var(--font-sans);
  font-feature-settings: 'tnum';
}
.grid-canvas { display: block; }
.grid-canvas:focus { outline: none; }
.grid-cursor-default { cursor: default; }
.grid-cursor-not-allowed { cursor: not-allowed; }
.grid-cursor-e-resize { cursor: e-resize; }
.grid-cursor-col-resize { cursor: col-resize; }
.grid-cursor-pointer { cursor: pointer; }
.grid-cursor-move { cursor: grabbing; }
.grid-cursor-copy { cursor: copy; }
.grid-block-events { pointer-events: none; }

.kermodes-viewer {
  display: flex; flex-direction: column; width: 100%; min-width: 0;
  background: var(--background); color: var(--foreground);
  font: 12px var(--font-sans);
}
.kermodes-viewer-body { position: relative; display: flex; min-height: 0; overflow: hidden; }
.kermodes-viewer-footer {
  display: flex; align-items: center; gap: 12px; height: 28px; padding: 0 8px; flex-shrink: 0;
  border-top: 1px solid var(--border); background: var(--muted); color: var(--muted-foreground);
  font-variant-numeric: tabular-nums;
}
.kermodes-viewer-footer > :last-child { margin-left: auto; }
.kermodes-footer-button {
  display: flex; align-items: center; gap: 4px; height: 20px; padding: 0 6px; margin-left: -6px;
  border: 0; border-radius: 4px; background: transparent; color: inherit; font: inherit;
  cursor: pointer; transition: background-color var(--duration-fast) var(--ease-out);
}
.kermodes-footer-button:hover { background: var(--accent); color: var(--foreground); }
.kermodes-footer-button[aria-pressed='true'], .kermodes-footer-button[data-active='true'] {
  color: var(--primary);
}
.kermodes-footer-button:focus-visible { outline: 2px solid var(--ring); outline-offset: -2px; }

.kermodes-filter-input {
  box-sizing: border-box; padding: 0 8px; margin: 0; border: 0; border-radius: 0; outline: none;
  background: transparent; color: var(--foreground); font: 12px var(--font-mono);
}
.kermodes-filter-input::placeholder { color: var(--faint-foreground); opacity: 1; }
.kermodes-filter-input:hover { background: var(--accent); }
.kermodes-filter-input:focus { background: var(--background); box-shadow: inset 0 -2px 0 var(--primary); }
.kermodes-filter-input[aria-invalid='true'] {
  color: var(--destructive); box-shadow: inset 0 -2px 0 var(--destructive);
}

.kermodes-menu {
  position: absolute; z-index: 10; min-width: 200px; padding: 4px; outline: none;
  font: 13px var(--font-sans); color: var(--popover-foreground); background: var(--popover);
  border: 1px solid var(--border); border-radius: 10px; box-shadow: 0 8px 24px var(--shadow);
}
.kermodes-menu-item {
  display: flex; width: 100%; gap: 24px; padding: 5px 8px; border: 0; border-radius: 6px;
  background: transparent; color: inherit; font: inherit; text-align: left; cursor: default;
  white-space: nowrap;
}
.kermodes-menu-item > :first-child { flex: 1; }
.kermodes-menu-item:hover, .kermodes-menu-item:focus { background: var(--accent); outline: none; }
.kermodes-menu-hint { color: var(--muted-foreground); }
.kermodes-menu-separator { height: 1px; margin: 4px 0; background: var(--border); }

.kermodes-toast {
  position: absolute; right: 16px; bottom: 16px; padding: 6px 10px; pointer-events: none;
  font: 12px var(--font-sans); color: var(--popover-foreground); background: var(--popover);
  border: 1px solid var(--border); border-radius: 8px; box-shadow: 0 8px 24px var(--shadow);
}

.kermodes-cover { position: absolute; left: 0; right: 0; bottom: 0; pointer-events: none; }
.kermodes-cover-dim {
  display: flex; align-items: center; justify-content: center;
  background: color-mix(in srgb, var(--background) 60%, transparent);
}
.kermodes-cover-failed {
  display: flex; align-items: center; justify-content: center; padding: 24px;
  background: var(--background); text-align: center; pointer-events: auto;
}
.kermodes-cover-failed > div { max-width: 420px; }
.kermodes-cover-title { margin: 0 0 4px; font: 500 13px var(--font-sans); color: var(--foreground); }
.kermodes-cover-detail { margin: 0; font: 12px var(--font-sans); color: var(--muted-foreground); }
.kermodes-busy {
  display: flex; align-items: center; gap: 8px; padding: 6px 12px;
  font: 12px var(--font-sans); color: var(--muted-foreground); background: var(--popover);
  border: 1px solid var(--border); border-radius: 8px; box-shadow: 0 8px 24px var(--shadow);
}
.kermodes-skeleton-bar {
  position: absolute; height: 8px; border-radius: 4px; background: var(--secondary);
}
@keyframes kermodes-spin { to { transform: rotate(360deg); } }
.kermodes-spinner { animation: kermodes-spin 0.8s linear infinite; }

.kermodes-copy-target {
  position: absolute; z-index: 4; pointer-events: none; box-sizing: border-box;
  border: 1.5px solid var(--primary); border-radius: 2px; background: var(--selection);
}
.kermodes-cell-expand {
  position: absolute; z-index: 5; display: flex; align-items: center; justify-content: center;
  width: 16px; height: 16px; padding: 0; border: 1px solid var(--border); border-radius: 4px;
  background: var(--popover); color: var(--muted-foreground); cursor: pointer;
}
.kermodes-cell-expand:hover { color: var(--foreground); border-color: var(--input); }
.kermodes-value {
  position: absolute; z-index: 11; display: flex; flex-direction: column; width: 480px;
  max-width: calc(100% - 8px); outline: none; font: 12px var(--font-sans);
  color: var(--popover-foreground); background: var(--popover); border: 1px solid var(--border);
  border-radius: 10px; box-shadow: 0 8px 24px var(--shadow);
}
.kermodes-value-head {
  display: flex; align-items: center; gap: 8px; padding: 6px 8px 6px 12px;
  border-bottom: 1px solid var(--border);
}
.kermodes-value-name { font-family: var(--font-mono); color: var(--foreground); }
.kermodes-value-type { flex: 1; color: var(--muted-foreground); }
.kermodes-value-text {
  margin: 0; padding: 10px 12px; max-height: 320px; overflow: auto; user-select: text;
  font: 12px/1.5 var(--font-mono); white-space: pre-wrap; overflow-wrap: anywhere;
}
`;

export function injectGridStyles(doc: Document = document): void {
  if (doc.getElementById(STYLE_ID) != null) {
    return;
  }
  const style = doc.createElement('style');
  style.id = STYLE_ID;
  style.textContent = GRID_CSS;
  doc.head.appendChild(style);
}

export default injectGridStyles;
