import {
  type ViewerBuffer,
  type ViewerChannel,
  type ViewerReply,
  type ViewerRequest,
  type ViewerState,
  type ViewportColumn,
} from '@kermodes/grid';
import { parseFrame } from './frame';

/** A table the session publishes */
export type TableInfo = {
  name: string;
  columns: ViewportColumn[];
  refreshing: boolean;
};

export type ConnectionStatus = 'connecting' | 'open' | 'closed';

/** The parts of a WebSocket the connection uses, so tests can stand in for one */
export interface SocketLike {
  binaryType: BinaryType;
  readonly readyState: number;
  onopen: ((event: Event) => void) | null;
  onclose: ((event: CloseEvent) => void) | null;
  onerror: ((event: Event) => void) | null;
  onmessage: ((event: MessageEvent) => void) | null;
  send(data: string): void;
  close(): void;
}

export type ConnectionOptions = {
  createSocket?: (url: string) => SocketLike;
  // Delay before the first reconnect, doubled after each failure up to `maxBackoffMs`
  backoffMs?: number;
  maxBackoffMs?: number;
};

const OPEN = 1;

type StateMessage = {
  type: 'state';
  view: number;
  row_count: number;
  status: string;
  sort: ViewerState['sort'];
  filters: ViewerState['filters'];
  filter_errors: ViewerState['filter_errors'];
  applied: ViewerState['applied'];
};

type TextMessage =
  | { type: 'tables'; tables: TableInfo[]; views?: string[] }
  | StateMessage
  | { type: 'error'; view: number | null; message: string; retry?: boolean };

/** One table open in the viewer over the shared socket */
export class SocketView implements ViewerChannel {
  readonly id: number;

  readonly table: string;

  private current: ViewerState;

  // The last viewport, sort, and filter asked for, to ask again after reconnecting
  private replay = new Map<string, ViewerRequest>();

  private messageHandlers = new Set<
    (header: ViewerReply, buffers: readonly ViewerBuffer[]) => void
  >();

  private stateHandlers = new Set<() => void>();

  // Whether the app has paused this view: the server sends it nothing until resumed
  private paused = false;

  constructor(
    private readonly connection: KermodesConnection,
    id: number,
    table: string,
    columns: ViewportColumn[]
  ) {
    this.id = id;
    this.table = table;
    this.current = {
      columns,
      row_count: 0,
      status: 'opening…',
      sort: [],
      filters: {},
      filter_errors: {},
      applied: 0,
    };
  }

  send(message: ViewerRequest): void {
    if (message.type !== 'fetch') {
      this.replay.set(message.type, message);
    }
    this.connection.sendView(this.id, message);
  }

  onMessage(
    handler: (header: ViewerReply, buffers: readonly ViewerBuffer[]) => void
  ): () => void {
    this.messageHandlers.add(handler);
    return () => {
      this.messageHandlers.delete(handler);
    };
  }

  state(): ViewerState {
    return this.current;
  }

  onState(handler: () => void): () => void {
    this.stateHandlers.add(handler);
    return () => {
      this.stateHandlers.delete(handler);
    };
  }

  /** Stop viewing the table */
  close(): void {
    this.connection.closeView(this.id);
  }

  /**
   * Stop the server sending this view anything while it is not shown (a hidden tab), keeping
   * its sort and filters; `resume` starts again with the current window.
   */
  pause(): void {
    if (!this.paused) {
      this.paused = true;
      this.connection.sendControl(this.id, 'pause');
    }
  }

  resume(): void {
    if (this.paused) {
      this.paused = false;
      this.connection.sendControl(this.id, 'resume');
    }
  }

  /** @internal The messages that reopen this view on a new socket */
  reopenMessages(): object[] {
    const { replay } = this;
    return [
      { type: 'open', view: this.id, table: this.table },
      ...['filter', 'sort', 'viewport']
        .map(kind => replay.get(kind))
        .filter(message => message != null)
        .map(message => ({ ...message, view: this.id })),
      ...(this.paused ? [{ type: 'pause', view: this.id }] : []),
    ];
  }

  /** @internal */
  receive(header: ViewerReply, buffers: readonly ViewerBuffer[]): void {
    this.messageHandlers.forEach(handler => handler(header, buffers));
  }

  /** @internal */
  update(changes: Partial<ViewerState>): void {
    this.current = { ...this.current, ...changes };
    this.stateHandlers.forEach(handler => handler());
  }
}

/**
 * One WebSocket to the gateway, shared by every open view. Lists the published
 * tables, routes each view's messages by its id, and reconnects with backoff,
 * reopening the views as they were (viewport, sort, filters).
 */
export class KermodesConnection {
  status: ConnectionStatus = 'connecting';

  tables: readonly TableInfo[] = [];

  /** Views published for pages of the app's own, not listed among tables (`deployments`) */
  pages: readonly string[] = [];

  // The last error not tied to one view
  lastError: string | null = null;

  private socket: SocketLike | null = null;

  private views = new Map<number, SocketView>();

  private nextViewId = 1;

  private listeners = new Set<() => void>();

  private backoff: number;

  private reconnectTimer: ReturnType<typeof setTimeout> | undefined;

  private isClosed = false;

  // Views the server ended to retry (their table's store was away): when each opens again,
  // and the delay before the next try, doubled up to the maximum until rows arrive
  private retries = new Map<
    number,
    { timer: ReturnType<typeof setTimeout>; delay: number }
  >();

  private readonly createSocket: (url: string) => SocketLike;

  private readonly initialBackoff: number;

  private readonly maxBackoff: number;

  constructor(
    private readonly url: string,
    {
      createSocket = socketUrl => new WebSocket(socketUrl),
      backoffMs = 500,
      maxBackoffMs = 10_000,
    }: ConnectionOptions = {}
  ) {
    this.createSocket = createSocket;
    this.initialBackoff = backoffMs;
    this.backoff = backoffMs;
    this.maxBackoff = maxBackoffMs;
    this.connect();
  }

  /** Call `listener` when the status, the tables, or the last error change */
  onChange(listener: () => void): () => void {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  }

  /** Open `table` in a new view */
  open(table: string): SocketView {
    const id = this.nextViewId;
    this.nextViewId += 1;
    const columns =
      this.tables.find(info => info.name === table)?.columns ?? [];
    const view = new SocketView(this, id, table, columns);
    this.views.set(id, view);
    if (this.socket?.readyState === OPEN) {
      view.reopenMessages().forEach(message => this.sendText(message));
    }
    return view;
  }

  /** Close the socket for good */
  close(): void {
    this.isClosed = true;
    clearTimeout(this.reconnectTimer);
    this.socket?.close();
  }

  /** Ask for the tables list again */
  refreshTables(): void {
    this.sendText({ type: 'tables' });
  }

  /** @internal */
  sendView(view: number, message: ViewerRequest): void {
    this.sendText({ ...message, view });
  }

  /** @internal */
  sendControl(view: number, type: 'pause' | 'resume'): void {
    this.sendText({ type, view });
  }

  /** @internal */
  closeView(view: number): void {
    clearTimeout(this.retries.get(view)?.timer);
    this.retries.delete(view);
    if (this.views.delete(view)) {
      this.sendText({ type: 'close', view });
    }
  }

  /** Opens `view` again after a delay that doubles with each try until its rows arrive */
  private retry(view: SocketView): void {
    const delay = this.retries.get(view.id)?.delay ?? this.initialBackoff;
    const timer = setTimeout(() => {
      if (this.views.has(view.id)) {
        view.reopenMessages().forEach(message => this.sendText(message));
      }
    }, delay);
    this.retries.set(view.id, { timer, delay: Math.min(delay * 2, this.maxBackoff) });
  }

  private sendText(message: object): void {
    if (this.socket?.readyState === OPEN) {
      this.socket.send(JSON.stringify(message));
    }
  }

  private notify(): void {
    this.listeners.forEach(listener => listener());
  }

  private connect(): void {
    this.status = 'connecting';
    this.notify();
    const socket = this.createSocket(this.url);
    socket.binaryType = 'arraybuffer';
    this.socket = socket;

    socket.onopen = () => {
      this.backoff = this.initialBackoff;
      this.status = 'open';
      this.lastError = null;
      this.notify();
      // Every view opens again on the new socket, including any waiting to retry.
      this.retries.forEach(({ timer }) => clearTimeout(timer));
      this.retries.clear();
      this.views.forEach(view =>
        view.reopenMessages().forEach(message => this.sendText(message))
      );
    };
    socket.onmessage = event => this.receive(event.data);
    socket.onclose = () => {
      if (this.socket !== socket) {
        return;
      }
      this.socket = null;
      this.status = 'closed';
      this.views.forEach(view =>
        view.update({ error: 'Reconnecting', retrying: true })
      );
      this.notify();
      if (!this.isClosed) {
        this.reconnectTimer = setTimeout(() => this.connect(), this.backoff);
        this.backoff = Math.min(this.backoff * 2, this.maxBackoff);
      }
    };
  }

  private receive(data: unknown): void {
    if (typeof data === 'string') {
      this.receiveText(JSON.parse(data) as TextMessage);
      return;
    }
    const { header, rows, keys } = parseFrame(data as ArrayBuffer);
    if (header.type === 'rows' && this.retries.has(header.view)) {
      clearTimeout(this.retries.get(header.view)?.timer);
      this.retries.delete(header.view);
    }
    this.views.get(header.view)?.receive(header, [rows, keys]);
  }

  private receiveText(message: TextMessage): void {
    switch (message.type) {
      case 'tables': {
        this.tables = message.tables;
        this.pages = message.views ?? [];
        this.views.forEach(view => {
          const info = message.tables.find(table => table.name === view.table);
          if (
            info != null &&
            JSON.stringify(info.columns) !== JSON.stringify(view.state().columns)
          ) {
            view.update({ columns: info.columns });
          }
        });
        this.notify();
        break;
      }
      case 'state': {
        const { row_count, status, sort, filters, filter_errors, applied } = message;
        // The server answering for the view means it is current again
        this.views.get(message.view)?.update({
          row_count,
          status,
          sort,
          filters,
          filter_errors,
          applied,
          error: null,
          retrying: false,
        });
        break;
      }
      case 'error': {
        const view = message.view == null ? undefined : this.views.get(message.view);
        if (view == null) {
          this.lastError = message.message;
          this.notify();
        } else {
          view.update({ error: message.message, retrying: message.retry === true });
          if (message.retry) {
            this.retry(view);
          }
        }
        break;
      }
      default:
        break;
    }
  }
}
