import type { TableInfo, ViewerChannel } from '@kermodes/client';

/** What the hooks use of a connection: a `KermodesConnection`, or `fakeKermodes` in tests */
export type Connection = {
  readonly tables: readonly TableInfo[];
  onChange(listener: () => void): () => void;
  open(table: string): ViewerChannel & { close(): void };
  close(): void;
};
