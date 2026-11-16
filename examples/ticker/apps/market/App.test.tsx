import { afterEach, expect, it } from 'bun:test';
import { act, cleanup, render, screen, waitFor } from '@testing-library/react';
import { KermodesProvider, fakeKermodes } from '@kermodes/react';
import App from './App';

afterEach(cleanup);

const at = (second: number) => new Date(Date.UTC(2026, 10, 16, 9, 30, second));

it("shows the day's figures: ticks as they arrive, the symbols, and the leader by sign", async () => {
  const ticks = [
    { sym: 'AAPL', price: 189.5, size: 100, ts: at(0) },
    { sym: 'MSFT', price: 410.25, size: 50, ts: at(1) },
  ];
  const kermodes = fakeKermodes({
    board: [
      { sym: 'AAPL', last: 189.5, change_pct: -0.4 },
      { sym: 'MSFT', last: 410.25, change_pct: 1.25 },
    ],
    // Biggest gain first, as the app's Python sorts it
    movers: [
      { sym: 'MSFT', last: 410.25, change_pct: 1.25 },
      { sym: 'AAPL', last: 189.5, change_pct: -0.4 },
    ],
    'md.ticks': ticks,
  });
  render(
    <KermodesProvider connection={kermodes}>
      <App />
    </KermodesProvider>
  );

  const figure = (label: string) => screen.getByText(label).nextElementSibling;
  expect((await screen.findByText('MSFT +1.25%')).dataset.sign).toBe('positive');
  expect(figure('Symbols')?.textContent).toBe('2');
  expect(figure('Ticks today')?.textContent).toBe('2');

  act(() => kermodes.set('md.ticks', [...ticks, { sym: 'NVDA', price: 120.75, size: 10, ts: at(2) }]));
  expect(figure('Ticks today')?.textContent).toBe('3');
});

it("charts each symbol's change from the app's movers, in their order", async () => {
  const kermodes = fakeKermodes({
    board: [],
    movers: [
      { sym: 'MSFT', last: 410.25, change_pct: 1.25 },
      { sym: 'AAPL', last: 189.5, change_pct: -0.4 },
    ],
    'md.ticks': [],
  });
  render(
    <KermodesProvider connection={kermodes}>
      <App />
    </KermodesProvider>
  );
  const bars = async () => {
    const plot = await waitFor(() => {
      const found = screen.getByText('Change by symbol').closest('.dv-groupview')?.querySelector('.js-plotly-plot');
      if (found == null) throw new Error('no chart yet');
      return found as HTMLElement & { data: { x: string[]; y: number[] }[] };
    });
    return { x: plot.data[0].x, y: plot.data[0].y };
  };
  expect(await bars()).toEqual({ x: ['MSFT', 'AAPL'], y: [1.25, -0.4] });
});
