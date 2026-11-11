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
};

type TextMessage =
  | { type: 'tables'; tables: TableInfo[] }
  | StateMessage
  | { type: 'error'; view: number | null; message: string };

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

  /** @internal The messages that reopen this view on a new socket */
  reopenMessages(): object[] {
    const { replay } = this;
    return [
      { type: 'open', view: this.id, table: this.table },
      ...['filter', 'sort', 'viewport']
        .map(kind => replay.get(kind))
        .filter(message => message != null)
        .map(message => ({ ...message, view: this.id })),
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

  // The last error not tied to one view
  lastError: string | null = null;

  private socket: SocketLike | null = null;

  private views = new Map<number, SocketView>();

  private nextViewId = 1;

  private listeners = new Set<() => void>();

  private backoff: number;

  private reconnectTimer: ReturnType<typeof setTimeout> | undefined;

  private isClosed = false;

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
  closeView(view: number): void {
    if (this.views.delete(view)) {
      this.sendText({ type: 'close', view });
    }
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
        view.update({ status: 'disconnected · reconnecting…' })
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
    this.views.get(header.view)?.receive(header, [rows, keys]);
  }

  private receiveText(message: TextMessage): void {
    switch (message.type) {
      case 'tables': {
        this.tables = message.tables;
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
        const { row_count, status, sort, filters, filter_errors } = message;
        this.views
          .get(message.view)
          ?.update({ row_count, status, sort, filters, filter_errors });
        break;
      }
      case 'error': {
        const view = message.view == null ? undefined : this.views.get(message.view);
        if (view == null) {
          this.lastError = message.message;
          this.notify();
        } else {
          view.update({ status: message.message });
        }
        break;
      }
      default:
        break;
    }
  }
}
