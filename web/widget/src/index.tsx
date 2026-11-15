/// <reference types="vite/client" />
import { useLayoutEffect, useRef, useState } from 'react';
import { createRoot } from 'react-dom/client';
import { TableViewer, gridThemeOf, type GridTheme } from '@kermodes/grid';
import {
  type ViewerBuffer,
  type ViewerChannel,
  type ViewerReply,
  type ViewerRequest,
  type ViewerState,
} from '@kermodes/client';
import tokens from '@kermodes/theme/tokens.css?inline';

const SCOPE = 'kermodes-widget';
const STYLE_ID = 'kermodes-widget-tokens';

/**
 * The design tokens scoped to the widget's root instead of `:root`, so the notebook page keeps
 * its own variables; the frame stands in for the panel edges the app gives the viewer.
 */
function injectTokens(): void {
  if (document.getElementById(STYLE_ID) != null) {
    return;
  }
  const style = document.createElement('style');
  style.id = STYLE_ID;
  style.textContent = `${tokens.replaceAll(':root', `.${SCOPE}`)}
.${SCOPE} { border: 1px solid var(--border); border-radius: 8px; overflow: hidden; }`;
  document.head.appendChild(style);
}

/** The slice of anywidget's model the viewer uses */
export interface WidgetModel {
  get<K extends keyof ViewerState>(name: K): ViewerState[K];
  on(event: string, callback: (...args: never[]) => void): void;
  off(event: string, callback: (...args: never[]) => void): void;
  send(content: ViewerRequest): void;
}

const TRAITS: readonly (keyof ViewerState)[] = [
  'columns',
  'row_count',
  'status',
  'sort',
  'filters',
  'filter_errors',
  'applied',
];

const MAX_GRID_HEIGHT = 420;

/** The viewer's channel over a notebook widget: its comm messages and synced traits */
export function widgetChannel(model: WidgetModel): ViewerChannel {
  return {
    send: message => model.send(message),
    onMessage(handler) {
      const listener = (header: ViewerReply, buffers: ViewerBuffer[]): void =>
        handler(header, buffers);
      model.on('msg:custom', listener);
      return () => model.off('msg:custom', listener);
    },
    state: () => ({
      columns: model.get('columns'),
      row_count: model.get('row_count'),
      status: model.get('status'),
      sort: model.get('sort') ?? [],
      filters: model.get('filters') ?? {},
      filter_errors: model.get('filter_errors') ?? {},
      applied: model.get('applied') ?? 0,
    }),
    onState(handler) {
      const events = TRAITS.map(name => `change:${name}`);
      events.forEach(event => model.on(event, handler));
      return () => events.forEach(event => model.off(event, handler));
    },
  };
}

/** The host notebook's theme: JupyterLab and VS Code mark light themes on <body> */
function hostThemeName(): 'dark' | 'light' {
  const { body } = document;
  if (
    body.dataset.jpThemeLight === 'true' ||
    body.classList.contains('vscode-light') ||
    body.classList.contains('vscode-high-contrast-light')
  ) {
    return 'light';
  }
  return 'dark';
}

/** The viewer inside the scoped root, themed once the root is in the document */
function WidgetViewer({ model }: { model: WidgetModel }): React.ReactElement {
  const root = useRef<HTMLDivElement>(null);
  const [theme, setTheme] = useState<GridTheme | null>(null);
  useLayoutEffect(() => {
    if (root.current != null) {
      setTheme(gridThemeOf(root.current));
    }
  }, []);
  return (
    <div ref={root} className={SCOPE} data-theme={hostThemeName()}>
      {theme != null && (
        <TableViewer
          channel={widgetChannel(model)}
          theme={theme}
          maxGridHeight={MAX_GRID_HEIGHT}
        />
      )}
    </div>
  );
}

function render({
  model,
  el,
}: {
  model: WidgetModel;
  el: HTMLElement;
}): () => void {
  injectTokens();
  const root = createRoot(el);
  root.render(<WidgetViewer model={model} />);
  return () => root.unmount();
}

export default { render };
