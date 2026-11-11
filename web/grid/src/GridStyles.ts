// Adapted from @deephaven/grid (Apache-2.0), web-client-ui 0bab596 (Grid.scss)
// Injected by the component so every host (Vite app, anywidget bundle) gets it
// without a CSS pipeline.

const STYLE_ID = 'ursid-grid-styles';

const GRID_CSS = `
.grid-wrapper {
  flex: 1 1 0;
  max-width: 100%;
  max-height: 100%;
  min-width: 0;
  min-height: 0;
  position: relative;
  font-family: sans-serif;
  font-feature-settings: 'tnum';
}
.grid-canvas { display: block; }
.grid-canvas:focus { outline: none; }
.grid-cursor-default { cursor: default; }
.grid-cursor-not-allowed { cursor: not-allowed; }
.grid-cursor-e-resize { cursor: e-resize; }
.grid-cursor-col-resize { cursor: col-resize; }
.grid-cursor-pointer { cursor: pointer; }
.grid-block-events { pointer-events: none; }
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
