import { afterEach, beforeEach, describe, expect, it, jest } from 'bun:test';
import { type ViewerBuffer, type ViewerReply } from '@ursid/grid';
import { parseFrame } from './frame';
import { UrsidConnection, type SocketLike } from './connection';

/** A binary frame as the gateway builds it */
function frame(header: object, rows: Uint8Array, keys: bigint[]): ArrayBuffer {
  const head = new TextEncoder().encode(JSON.stringify(header));
  const bytes = new Uint8Array(4 + head.length + 4 + rows.length + keys.length * 8);
  const view = new DataView(bytes.buffer);
  view.setUint32(0, head.length, true);
  bytes.set(head, 4);
  view.setUint32(4 + head.length, rows.length, true);
  bytes.set(rows, 8 + head.length);
  keys.forEach((key, i) =>
    view.setBigUint64(8 + head.length + rows.length + i * 8, key, true)
  );
  return bytes.buffer;
}

class FakeSocket implements SocketLike {
  binaryType: BinaryType = 'blob';

  readyState = 0;

  onopen: ((event: Event) => void) | null = null;

  onclose: ((event: CloseEvent) => void) | null = null;

  onerror: ((event: Event) => void) | null = null;

  onmessage: ((event: MessageEvent) => void) | null = null;

  sent: unknown[] = [];

  send(data: string): void {
    this.sent.push(JSON.parse(data));
  }

  close(): void {
    this.drop();
  }

  open(): void {
    this.readyState = 1;
    this.onopen?.(new Event('open'));
  }

  drop(): void {
    this.readyState = 3;
    this.onclose?.(new Event('close') as CloseEvent);
  }

  receive(data: string | ArrayBuffer): void {
    this.onmessage?.({ data } as MessageEvent);
  }
}

function connect(): { connection: UrsidConnection; sockets: FakeSocket[] } {
  const sockets: FakeSocket[] = [];
  const connection = new UrsidConnection('ws://test/ws', {
    createSocket: () => {
      const socket = new FakeSocket();
      sockets.push(socket);
      return socket;
    },
    backoffMs: 1,
  });
  return { connection, sockets };
}

const TABLES = {
  type: 'tables',
  tables: [
    { name: 'trades', columns: [{ name: 'px', type: 'float64' }], refreshing: true },
    { name: 'symbols', columns: [{ name: 'sym', type: 'utf8' }], refreshing: false },
  ],
};

describe('parseFrame', () => {
  it('splits header, rows, and keys', () => {
    const rows = Uint8Array.of(1, 2, 3, 4, 5);
    const parsed = parseFrame(
      frame(
        { view: 3, type: 'rows', start: 10, total: 99, cycle: 7 },
        rows,
        [BigInt(5), BigInt(2) ** BigInt(63)]
      )
    );
    expect(parsed.header).toEqual({
      view: 3,
      type: 'rows',
      start: 10,
      total: 99,
      cycle: 7,
    });
    expect([...parsed.rows]).toEqual([1, 2, 3, 4, 5]);
    // Keys start at an odd offset here; read them the way the viewer does
    const keys = new DataView(
      parsed.keys.buffer,
      parsed.keys.byteOffset,
      parsed.keys.byteLength
    );
    expect(keys.getBigUint64(0, true)).toBe(BigInt(5));
    expect(keys.getBigUint64(8, true)).toBe(BigInt(2) ** BigInt(63));
  });

  it('reads fetch answers and frames without keys', () => {
    const parsed = parseFrame(
      frame({ view: 1, type: 'fetched', id: 4, start: 0, total: 0, cycle: 1 }, new Uint8Array(0), [])
    );
    expect(parsed.header).toMatchObject({ type: 'fetched', id: 4 });
    expect(parsed.rows.length).toBe(0);
    expect(parsed.keys.length).toBe(0);
  });

  it('rejects a frame cut short', () => {
    const whole = frame({ view: 1, type: 'rows', start: 0, total: 1, cycle: 1 }, Uint8Array.of(1, 2), [BigInt(1)]);
    expect(() => parseFrame(whole.slice(0, whole.byteLength - 3))).toThrow();
  });
});

describe('UrsidConnection', () => {
  it('lists tables and opens views with their columns', () => {
    const { connection, sockets } = connect();
    sockets[0].open();
    sockets[0].receive(JSON.stringify(TABLES));
    expect(connection.tables.map(table => table.name)).toEqual(['trades', 'symbols']);

    const view = connection.open('trades');
    expect(view.state().columns).toEqual([{ name: 'px', type: 'float64' }]);
    expect(sockets[0].sent).toEqual([{ type: 'open', view: view.id, table: 'trades' }]);
  });

  it('routes messages, state, and errors to their view', () => {
    const { connection, sockets } = connect();
    sockets[0].open();
    sockets[0].receive(JSON.stringify(TABLES));
    const trades = connection.open('trades');
    const symbols = connection.open('symbols');

    const received: Array<[string, ViewerReply, readonly ViewerBuffer[]]> = [];
    trades.onMessage((header, buffers) => received.push(['trades', header, buffers]));
    symbols.onMessage((header, buffers) => received.push(['symbols', header, buffers]));
    let tradesStates = 0;
    trades.onState(() => {
      tradesStates += 1;
    });

    trades.send({ type: 'sort', by: [{ column: 'px', descending: true }] });
    expect(sockets[0].sent.at(-1)).toEqual({
      type: 'sort',
      by: [{ column: 'px', descending: true }],
      view: trades.id,
    });

    sockets[0].receive(
      JSON.stringify({
        type: 'state',
        view: trades.id,
        row_count: 42,
        status: 'ticking · cycle 3',
        sort: [{ column: 'px', descending: true }],
        filters: {},
        filter_errors: {},
      })
    );
    expect(tradesStates).toBe(1);
    expect(trades.state()).toMatchObject({ row_count: 42, status: 'ticking · cycle 3' });
    expect(symbols.state().row_count).toBe(0);

    sockets[0].receive(
      frame({ view: symbols.id, type: 'rows', start: 0, total: 1, cycle: 3 }, Uint8Array.of(9), [BigInt(1)])
    );
    expect(received.map(([name, header]) => [name, header.type])).toEqual([['symbols', 'rows']]);
    expect([...(received[0][2][0] as Uint8Array)]).toEqual([9]);

    sockets[0].receive(JSON.stringify({ type: 'error', view: trades.id, message: 'the table is gone' }));
    expect(trades.state().status).toBe('the table is gone');
    sockets[0].receive(JSON.stringify({ type: 'error', view: null, message: 'not JSON' }));
    expect(connection.lastError).toBe('not JSON');
  });

  it('closes a view and forgets its messages', () => {
    const { connection, sockets } = connect();
    sockets[0].open();
    const view = connection.open('trades');
    let messages = 0;
    view.onMessage(() => {
      messages += 1;
    });
    view.close();
    expect(sockets[0].sent.at(-1)).toEqual({ type: 'close', view: view.id });
    sockets[0].receive(frame({ view: view.id, type: 'rows', start: 0, total: 0, cycle: 1 }, new Uint8Array(0), []));
    expect(messages).toBe(0);
  });

  beforeEach(() => {
    jest.useFakeTimers();
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  it('reconnects and reopens views as they were', () => {
    const { connection, sockets } = connect();
    // Opened before the socket is up: sent once it opens
    const view = connection.open('trades');
    sockets[0].open();
    view.send({ type: 'filter', filters: { px: '> 5' } });
    view.send({ type: 'viewport', top: 100, bottom: 120 });
    view.send({ type: 'fetch', id: 1, start: 0, count: 10 });
    view.send({ type: 'viewport', top: 200, bottom: 220 });

    sockets[0].drop();
    expect(connection.status).toBe('closed');
    expect(view.state().status).toContain('reconnecting');

    jest.advanceTimersByTime(1);
    expect(sockets).toHaveLength(2);
    sockets[1].open();
    expect(connection.status).toBe('open');
    expect(sockets[1].sent).toEqual([
      { type: 'open', view: view.id, table: 'trades' },
      { type: 'filter', filters: { px: '> 5' }, view: view.id },
      { type: 'viewport', top: 200, bottom: 220, view: view.id },
    ]);
  });

  it('stays closed when closed on purpose', () => {
    const { connection, sockets } = connect();
    sockets[0].open();
    connection.close();
    jest.advanceTimersByTime(60_000);
    expect(sockets).toHaveLength(1);
  });
});
