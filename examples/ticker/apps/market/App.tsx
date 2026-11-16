import { Dashboard, Panel, Split, Stat, Stats, Status, useTable } from '@kermodes/react';
import { Chart } from '@kermodes/react/chart';
import { Table } from '@kermodes/react/table';

/** The symbol whose day the price chart draws */
const WATCHED = 'S0001';

/** A row of `movers`, which the app's own Python computes (app.py, beside this) */
type Mover = { sym: string; last: number; change_pct: number };

const count = new Intl.NumberFormat('en-US');
const percent = new Intl.NumberFormat('en-US', { signDisplay: 'exceptZero', maximumFractionDigits: 2 });

/**
 * A market dashboard: the day's figures across the top; the board of every symbol (the app's own
 * table) over a chart of each symbol's change, from the app's movers; one symbol's price over the
 * day, a line through every tick (downsampled by the server to what the chart draws), above the
 * tape (md.ticks, newest on top) and the movers. The panels resize, dock, and tab, and the
 * browser keeps how they are arranged.
 */
export default function App() {
  // Only its size: the last row is the least there is to ask for.
  const ticks = useTable('md.ticks', { last: 1 });
  const board = useTable('board', { limit: 1 });
  const movers = useTable<Mover>('movers', { limit: 1 });
  const leader = movers.rows[0];
  return (
    <Dashboard>
      <Stats>
        <Stat label="Ticks today" value={count.format(ticks.total)} />
        <Stat label="Symbols" value={count.format(board.total)} />
        <Stat
          label="Leader"
          value={leader == null ? '—' : `${leader.sym} ${percent.format(leader.change_pct)}%`}
          sign={leader?.change_pct}
        />
      </Stats>
      <Split sizes={[60, 40]}>
        <Split direction="vertical" sizes={[60, 40]}>
          <Panel title="Board" actions={<Status status={ticks.status} />}>
            <Table name="board" />
          </Panel>
          <Panel title="Change by symbol">
            <Chart table="movers" x="sym" y="change_pct" type="bar" />
          </Panel>
        </Split>
        <Split direction="vertical" sizes={[40, 35, 25]}>
          <Panel title={`${WATCHED} price`}>
            <Chart table="md.ticks" x="ts" y="price" where={`sym = '${WATCHED}'`} />
          </Panel>
          <Panel title="Tape">
            <Table name="md.ticks" reverse />
          </Panel>
          <Panel title="Movers">
            <Table name="movers" />
          </Panel>
        </Split>
      </Split>
    </Dashboard>
  );
}
