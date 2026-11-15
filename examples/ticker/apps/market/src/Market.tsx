import { useEffect, useRef } from 'react';
import { useTable, type TableStatus } from '@kermodes/react';

type Quote = { sym: string; px: number };
type Tick = { sym: string; price: number; size: number; ts: Date };

const price = new Intl.NumberFormat('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
const count = new Intl.NumberFormat('en-US');
const time = new Intl.DateTimeFormat('en-GB', {
  hour: '2-digit',
  minute: '2-digit',
  second: '2-digit',
  fractionalSecondDigits: 3,
  timeZone: 'UTC',
});

/**
 * A market board from three live tables, in ordinary markup: each `useTable` keeps its rows
 * current, and the page draws them however it likes.
 */
export function Market() {
  const quotes = useTable<Quote>('verify.quotes', { sort: ['sym'], limit: 24 });
  const tape = useTable<Tick>('md.ticks', { last: 14 });
  return (
    <div className="mx-auto flex min-h-full max-w-6xl flex-col gap-6 p-8">
      <header className="flex items-baseline justify-between">
        <h1 className="text-xl font-medium">Market</h1>
        <Status status={tape.status} />
      </header>

      <section className="grid grid-cols-2 gap-3">
        <Figure label="Ticks today" value={count.format(tape.total)} />
        <Figure label="Symbols quoted" value={count.format(quotes.total)} />
      </section>

      <div className="grid grid-cols-[1fr_22rem] gap-6">
        <section>
          <h2 className="mb-3 text-sm text-muted-foreground">Quotes</h2>
          <div className="grid grid-cols-4 gap-2">
            {quotes.rows.map(q => (
              <QuoteCard key={q.sym} quote={q} />
            ))}
          </div>
        </section>

        <section>
          <h2 className="mb-3 text-sm text-muted-foreground">Latest ticks</h2>
          <ol className="overflow-hidden rounded-lg border bg-card">
            {/* By position: at 100 ticks a second every update replaces all the rows, so rows
                keyed by tick would all be torn down and rebuilt four times a second (flashing);
                these keep their elements and change their text, as a tape runs. */}
            {[...tape.rows].reverse().map((t, position) => (
              <li
                key={position}
                className="grid grid-cols-[4rem_1fr_3rem_6.5rem] items-baseline gap-2 border-b px-3 py-1.5 font-mono text-[12px] tabular-nums last:border-b-0"
              >
                <span>{t.sym}</span>
                <span className="text-right">{price.format(t.price)}</span>
                <span className="text-right text-muted-foreground">{t.size}</span>
                <span className="text-right text-muted-foreground">{time.format(t.ts)}</span>
              </li>
            ))}
          </ol>
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
  const last = useRef(quote.px);
  useEffect(() => {
    const card = ref.current;
    if (card != null && quote.px !== last.current) {
      card.classList.remove('changed');
      // Restart the animation on the same element
      void card.offsetWidth;
      card.classList.add('changed');
    }
    last.current = quote.px;
  }, [quote.px]);
  return (
    <div ref={ref} className="rounded-lg border bg-card px-3 py-2">
      <div className="font-mono text-xs text-muted-foreground">{quote.sym}</div>
      <div className="mt-0.5 text-lg font-medium tabular-nums">{price.format(quote.px)}</div>
    </div>
  );
}
