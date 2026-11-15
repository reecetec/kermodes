import { afterEach, expect, it } from 'bun:test';
import { act, cleanup, render, screen } from '@testing-library/react';
import { KermodesProvider, fakeKermodes } from '@kermodes/react';
import { Market } from './Market';

afterEach(cleanup);

const at = (second: number) => new Date(Date.UTC(2026, 10, 16, 9, 30, second));

it('shows each quote, and counts the ticks as they arrive', async () => {
  const ticks = [
    { sym: 'AAPL', price: 189.5, size: 100, ts: at(0) },
    { sym: 'MSFT', price: 410.25, size: 50, ts: at(1) },
  ];
  const kermodes = fakeKermodes({
    'verify.quotes': [
      { sym: 'AAPL', px: 189.5 },
      { sym: 'MSFT', px: 410.25 },
    ],
    'md.ticks': ticks,
  });
  render(
    <KermodesProvider connection={kermodes}>
      <Market />
    </KermodesProvider>
  );

  // Each quote's card, and how many ticks there are.
  expect(await screen.findByText('410.25')).toBeDefined();
  expect(screen.getByText('189.50')).toBeDefined();
  const figure = (label: string) => screen.getByText(label).nextElementSibling?.textContent;
  expect(figure('Ticks today')).toBe('2');
  expect(figure('Symbols quoted')).toBe('2');

  act(() => kermodes.set('md.ticks', [...ticks, { sym: 'NVDA', price: 120.75, size: 10, ts: at(2) }]));
  expect(figure('Ticks today')).toBe('3');
});
