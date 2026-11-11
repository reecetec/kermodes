import { createRoot } from 'react-dom/client';
import {
  TableViewer,
  gridThemes,
  type GridThemeName,
  type ViewerBuffer,
  type ViewerChannel,
  type ViewerReply,
  type ViewerRequest,
  type ViewerState,
} from '@kermodes/grid';

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
    }),
    onState(handler) {
      const events = TRAITS.map(name => `change:${name}`);
      events.forEach(event => model.on(event, handler));
      return () => events.forEach(event => model.off(event, handler));
    },
  };
}

/** The host notebook's theme: JupyterLab and VS Code mark light themes on <body> */
function hostThemeName(): GridThemeName {
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

function render({
  model,
  el,
}: {
  model: WidgetModel;
  el: HTMLElement;
}): () => void {
  const root = createRoot(el);
  root.render(
    <TableViewer
      channel={widgetChannel(model)}
      theme={gridThemes[hostThemeName()]}
      maxGridHeight={MAX_GRID_HEIGHT}
    />
  );
  return () => root.unmount();
}

export default { render };
