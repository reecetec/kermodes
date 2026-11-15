import { afterEach, expect, it } from 'bun:test';
import { act, cleanup, render, screen } from '@testing-library/react';
import { KermodesProvider, fakeKermodes } from '@kermodes/react';
import App from './App';

afterEach(cleanup);

const at = (second: number) => new Date(Date.UTC(2026, 10, 16, 9, 30, second));

it("shows each symbol's quote and day, and counts the ticks as they arrive", async () => {
  const ticks = [
    { sym: 'AAPL', price: 189.5, size: 100, ts: at(0) },
    { sym: 'MSFT', price: 410.25, size: 50, ts: at(1) },
  ];
  const kermodes = fakeKermodes({
    board: [
      { sym: 'AAPL', price: 189.5, trades: 1, volume: 100, low: 189.5, high: 189.5 },
      { sym: 'MSFT', price: 410.25, trades: 1, volume: 41_041_590, low: 401, high: 412.75 },
    ],
    'md.ticks': ticks,
  });
  render(
    <KermodesProvider connection={kermodes}>
      <App />
    </KermodesProvider>
  );

  // Each quote's card: its last price, and its day's range and volume.
  expect(await screen.findByText('410.25')).toBeDefined();
  expect(screen.getByText('189.50')).toBeDefined();
  expect(screen.getByText('401.00–412.75 · 41M')).toBeDefined();
  const figure = (label: string) => screen.getByText(label).nextElementSibling?.textContent;
  expect(figure('Ticks today')).toBe('2');
  expect(figure('Symbols quoted')).toBe('2');

  act(() => kermodes.set('md.ticks', [...ticks, { sym: 'NVDA', price: 120.75, size: 10, ts: at(2) }]));
  expect(figure('Ticks today')).toBe('3');
});
