import { afterEach, expect, it } from 'bun:test';
import { act, cleanup, render, screen } from '@testing-library/react';
import { KermodesProvider, fakeKermodes } from '@kermodes/react';
import { Market } from './Market';

afterEach(cleanup);

const at = (second: number) => new Date(Date.UTC(2026, 10, 16, 9, 30, second));

it('shows each quote, the newest tick first, and follows the tape as it ticks', async () => {
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

  expect(await screen.findByText('Symbols quoted')).toBeDefined();
  const tape = () => [...document.querySelectorAll('ol li')].map(row => row.textContent);
  expect(tape()[0]).toContain('MSFT');
  expect(tape()).toHaveLength(2);

  act(() => kermodes.set('md.ticks', [...ticks, { sym: 'NVDA', price: 120.75, size: 10, ts: at(2) }]));
  expect(tape()[0]).toContain('NVDA');
  expect(tape()[0]).toContain('120.75');
  expect(screen.getByText('3')).toBeDefined();
});
