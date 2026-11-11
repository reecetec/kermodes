import React, { useEffect, useMemo, useRef, useState } from 'react';
import { createRoot } from 'react-dom/client';
import { tableFromIPC } from 'apache-arrow';
import {
  Grid,
  ViewportGridModel,
  gridThemes,
  type GridMetrics,
  type GridThemeName,
  type ViewportColumn,
} from '@ursid/grid';

/** The slice of anywidget's model this widget uses */
interface WidgetModel {
  get(name: 'columns'): ViewportColumn[];
  get(name: 'row_count'): number;
  get(name: 'status'): string;
  on(event: string, callback: (...args: never[]) => void): void;
  off(event: string, callback: (...args: never[]) => void): void;
  send(content: ViewportMessage): void;
}

type ViewportMessage = { type: 'viewport'; top: number; bottom: number };

type RowsMessage = {
  type: 'rows';
  start: number;
  total: number;
  cycle: number;
};

type Buffer = ArrayBuffer | ArrayBufferView;

const MAX_GRID_HEIGHT = 420;

function toBytes(buffer: Buffer): Uint8Array {
  return buffer instanceof ArrayBuffer
    ? new Uint8Array(buffer)
    : new Uint8Array(buffer.buffer, buffer.byteOffset, buffer.byteLength);
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

/**
 * Sends the visible row range when it changes, at most once per animation
 * frame, however often the grid redraws in between.
 */
function useViewportSender(model: WidgetModel) {
  return useMemo(() => {
    let sent: ViewportMessage | null = null;
    let pending: ViewportMessage | null = null;
    let frame: number | null = null;

    const flush = (): void => {
      frame = null;
      if (
        pending != null &&
        (sent == null ||
          pending.top !== sent.top ||
          pending.bottom !== sent.bottom)
      ) {
        sent = pending;
        model.send(sent);
      }
    };

    return {
      onViewChanged(metrics: GridMetrics): void {
        pending = { type: 'viewport', top: metrics.top, bottom: metrics.bottom };
        if (frame == null) {
          frame = requestAnimationFrame(flush);
        }
      },
      /** Forget what was sent, so the next view change is sent even if unchanged */
      reset(): void {
        sent = null;
      },
      dispose(): void {
        if (frame != null) {
          cancelAnimationFrame(frame);
        }
      },
    };
  }, [model]);
}

function TableViewer({ model }: { model: WidgetModel }): React.ReactElement {
  const gridModel = useMemo(
    () => new ViewportGridModel(model.get('columns'), model.get('row_count')),
    [model]
  );
  const gridRef = useRef<Grid>(null);
  const [rowCount, setRowCount] = useState(() => model.get('row_count'));
  const [status, setStatus] = useState(() => model.get('status'));
  const viewport = useViewportSender(model);
  const themeName = useMemo(hostThemeName, []);
  const theme = gridThemes[themeName];

  useEffect(() => {
    const redraw = (): void => {
      gridRef.current?.forceUpdate();
    };
    const onColumns = (): void => {
      gridModel.setColumns(model.get('columns'));
      viewport.reset();
      redraw();
    };
    const onRowCount = (): void => {
      const count = model.get('row_count');
      gridModel.setRowCount(count);
      setRowCount(count);
      redraw();
    };
    const onStatus = (): void => {
      setStatus(model.get('status'));
    };
    const onMessage = (message: RowsMessage, buffers: Buffer[]): void => {
      if (message.type !== 'rows') {
        return;
      }
      gridModel.setRowCount(message.total);
      gridModel.setWindow(message.start, tableFromIPC(toBytes(buffers[0])));
      redraw();
    };

    model.on('change:columns', onColumns);
    model.on('change:row_count', onRowCount);
    model.on('change:status', onStatus);
    model.on('msg:custom', onMessage);
    return () => {
      model.off('change:columns', onColumns);
      model.off('change:row_count', onRowCount);
      model.off('change:status', onStatus);
      model.off('msg:custom', onMessage);
      viewport.dispose();
    };
  }, [model, gridModel, viewport]);

  // Fit short tables, scroll long ones
  const gridHeight = Math.min(
    MAX_GRID_HEIGHT,
    theme.columnHeaderHeight +
      Math.max(rowCount, 1) * theme.rowHeight +
      theme.scrollBarSize +
      2
  );

  return (
    <div
      style={{
        display: 'flex',
        flexDirection: 'column',
        width: '100%',
        border: `1px solid ${theme.headerSeparatorColor}`,
        borderRadius: 4,
        overflow: 'hidden',
        background: theme.backgroundColor,
      }}
    >
      <div style={{ display: 'flex', height: gridHeight }}>
        <Grid
          ref={gridRef}
          model={gridModel}
          theme={theme}
          onViewChanged={viewport.onViewChanged}
        />
      </div>
      <div
        style={{
          display: 'flex',
          gap: 12,
          padding: '3px 8px',
          font: `11px ${theme.font.replace(/^\S+\s/, '')}`,
          color: theme.headerColor,
          background: theme.headerBackgroundColor,
          borderTop: `1px solid ${theme.headerSeparatorColor}`,
          fontVariantNumeric: 'tabular-nums',
        }}
      >
        <span>
          {rowCount.toLocaleString('en-US')} {rowCount === 1 ? 'row' : 'rows'}
        </span>
        <span style={{ marginLeft: 'auto' }}>{status}</span>
      </div>
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
  const root = createRoot(el);
  root.render(<TableViewer model={model} />);
  return () => root.unmount();
}

export default { render };
