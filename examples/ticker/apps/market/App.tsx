import { Dashboard, Panel, Split, Stat, Stats, Status, useTable } from '@kermodes/react';
import { Table } from '@kermodes/react/table';

/** A row of `movers`, which the app's own Python computes (app.py, beside this) */
type Mover = { sym: string; last: number; change_pct: number };

const count = new Intl.NumberFormat('en-US');
const percent = new Intl.NumberFormat('en-US', { signDisplay: 'exceptZero', maximumFractionDigits: 2 });

/**
 * A market dashboard: the day's figures across the top, the board of every symbol (the app's own
 * table), and the tape following md.ticks' end beside the biggest movers. Each table is the
 * platform's own viewer, filling its panel; the panels resize, and keep their sizes.
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
      <Split id="market" sizes={[60, 40]}>
        <Panel title="Board" actions={<Status status={ticks.status} />}>
          <Table name="board" />
        </Panel>
        <Split id="market-side" direction="vertical" sizes={[62, 38]}>
          <Panel title="Tape">
            <Table name="md.ticks" follow />
          </Panel>
          <Panel title="Movers">
            <Table name="movers" />
          </Panel>
        </Split>
      </Split>
    </Dashboard>
  );
}
