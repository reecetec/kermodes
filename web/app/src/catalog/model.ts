/** Rows and bytes of one date of a table, in weir or in history */
export type DateSize = { date: string; rows: number; bytes: number };

/** One stored table as `/api/catalog` reports it (`Store::stats_json`) */
export type CatalogTable = {
  table: string;
  kind: 'append' | 'keyed';
  key: string[];
  sort: string[];
  columns: { name: string; type: string }[];
  /** Each date weir holds: today's, and past dates not yet merged and dropped */
  weir: DateSize[];
  /** Each date of its history, with the files it spans */
  history: (DateSize & { files: number })[];
};

export type Catalog = {
  tables: CatalogTable[];
  /** weir's volume: its size and the bytes free on it */
  volume: { bytes: number; free: number };
};

/** What a table, or a namespace's tables together, take */
export type Totals = {
  tables: number;
  /** Rows of `today` in weir */
  today: number;
  /** Bytes in weir, over every date it holds */
  weirBytes: number;
  historyRows: number;
  historyBytes: number;
  /** Distinct dates in weir or history (a date is in both between a merge and weir dropping it) */
  dates: number;
  first: string | null;
  last: string | null;
};

export type Namespace = { namespace: string; totals: Totals; tables: CatalogTable[] };

/** The UTC date of `now`: weir's day partitions are UTC dates */
export function utcDate(now: Date): string {
  return now.toISOString().slice(0, 10);
}

export const namespaceOf = (table: string) => table.slice(0, table.indexOf('.'));

/** What `tables` take together, `today` being the date whose weir rows count as today's */
export function totals(tables: readonly CatalogTable[], today: string): Totals {
  const dates = new Set<string>();
  const sum: Totals = {
    tables: tables.length,
    today: 0,
    weirBytes: 0,
    historyRows: 0,
    historyBytes: 0,
    dates: 0,
    first: null,
    last: null,
  };
  for (const table of tables) {
    for (const held of table.weir) {
      dates.add(held.date);
      sum.weirBytes += held.bytes;
      if (held.date === today) sum.today += held.rows;
    }
    for (const merged of table.history) {
      dates.add(merged.date);
      sum.historyRows += merged.rows;
      sum.historyBytes += merged.bytes;
    }
  }
  const sorted = [...dates].sort();
  return { ...sum, dates: sorted.length, first: sorted[0] ?? null, last: sorted.at(-1) ?? null };
}

/** `tables` grouped by namespace, both in name order, each namespace with its totals */
export function namespaces(tables: readonly CatalogTable[], today: string): Namespace[] {
  const groups = new Map<string, CatalogTable[]>();
  for (const table of [...tables].sort((a, b) => a.table.localeCompare(b.table))) {
    const namespace = namespaceOf(table.table);
    groups.set(namespace, [...(groups.get(namespace) ?? []), table]);
  }
  return [...groups]
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([namespace, members]) => ({ namespace, totals: totals(members, today), tables: members }));
}

/** What a table is searched by: its name, kind, key and sort columns, and column names */
export function tableFields(table: CatalogTable): string[] {
  return [table.table, table.kind, ...table.key, ...table.sort, ...table.columns.map(c => c.name)];
}

/** Each date of a table, newest first, with its rows and bytes in weir and in history */
export function tableDates(
  table: CatalogTable
): { date: string; weir: DateSize | null; history: (DateSize & { files: number }) | null }[] {
  const dates = new Set([...table.weir, ...table.history].map(d => d.date));
  return [...dates]
    .sort()
    .reverse()
    .map(date => ({
      date,
      weir: table.weir.find(d => d.date === date) ?? null,
      history: table.history.find(d => d.date === date) ?? null,
    }));
}
