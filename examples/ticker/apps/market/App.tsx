import { useEffect, useRef } from 'react';
import { useTable, type TableStatus } from '@kermodes/react';
import { Table } from '@kermodes/react/table';

/** A row of `board`, which the app's own Python computes (app.py, beside this) */
type Quote = { sym: string; price: number; trades: number; volume: number; low: number; high: number };

const price = new Intl.NumberFormat('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
const count = new Intl.NumberFormat('en-US');
// A day's volume runs to tens of millions: compact, so a card's line does not wrap.
const volume = new Intl.NumberFormat('en-US', { notation: 'compact', maximumFractionDigits: 1 });

/**
 * A market board: the app's own `board` (each symbol's last price and day) as cards in ordinary
 * markup from `useTable`, which keeps their rows current, and the tape as the platform's own
 * table viewer, following md.ticks' end. Both come from the app's own server.
 */
export default function App() {
  const board = useTable<Quote>('board', { sort: ['sym'], limit: 24 });
  // Only its size: the last row is the least there is to ask for.
  const ticks = useTable('md.ticks', { last: 1 });
  return (
    <div className="mx-auto flex min-h-full max-w-6xl flex-col gap-6 p-8">
      <header className="flex items-baseline justify-between">
        <h1 className="text-xl font-medium">Market</h1>
        <Status status={ticks.status} />
      </header>

      <section className="grid grid-cols-2 gap-3">
        <Figure label="Ticks today" value={count.format(ticks.total)} />
        <Figure label="Symbols quoted" value={count.format(board.total)} />
      </section>

      <div className="grid grid-cols-[1fr_28rem] gap-6">
        <section>
          <h2 className="mb-3 text-sm text-muted-foreground">Quotes</h2>
          <div className="grid grid-cols-4 gap-2">
            {board.rows.map(q => (
              <QuoteCard key={q.sym} quote={q} />
            ))}
          </div>
        </section>

        <section>
          <h2 className="mb-3 text-sm text-muted-foreground">Latest ticks</h2>
          <div className="h-[30rem] overflow-hidden rounded-lg border">
            <Table name="md.ticks" follow />
          </div>
        </section>
      </div>
    </div>
  );
}

/** Nothing extra while live; says so when it is not (docs/theme.md: normal looks normal) */
function Status({ status }: { status: TableStatus }) {
  if (status === 'live' || status === 'static') {
    return null;
  }
  return <span className="text-xs text-muted-foreground">{status === 'failed' ? 'Unavailable' : 'Connecting'}</span>;
}

function Figure({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-lg border bg-card px-4 py-3">
      <div className="text-xs text-muted-foreground">{label}</div>
      <div className="mt-1 text-2xl font-medium tabular-nums">{value}</div>
    </div>
  );
}

/** A quote's card, tinted for a moment when its price changes */
function QuoteCard({ quote }: { quote: Quote }) {
  const ref = useRef<HTMLDivElement>(null);
  const last = useRef(quote.price);
  useEffect(() => {
    const card = ref.current;
    if (card != null && quote.price !== last.current) {
      card.classList.remove('changed');
      // Restart the animation on the same element
      void card.offsetWidth;
      card.classList.add('changed');
    }
    last.current = quote.price;
  }, [quote.price]);
  return (
    <div ref={ref} className="rounded-lg border bg-card px-3 py-2">
      <div className="font-mono text-xs text-muted-foreground">{quote.sym}</div>
      <div className="mt-0.5 text-lg font-medium tabular-nums">{price.format(quote.price)}</div>
      <div className="mt-0.5 text-[11px] text-muted-foreground tabular-nums">
        {price.format(quote.low)}–{price.format(quote.high)} · {volume.format(quote.volume)}
      </div>
    </div>
  );
}
