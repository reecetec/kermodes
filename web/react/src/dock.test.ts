import { expect, it } from 'bun:test';
import { dockLayout, type LayoutNode } from './dock';

const panel = (id: string): LayoutNode => ({ kind: 'panel', id, title: id });

/** The grid as nested lists of [group's panel, size] and branches, sizes rounded */
function shape(node: ReturnType<typeof dockLayout>['grid']['root']): unknown {
  if (node.type === 'leaf') {
    return [(node.data as { views: string[] }).views[0], Math.round(node.size ?? 0)];
  }
  return (node.data as (typeof node)[]).map(shape);
}

it('lays a split out at its sizes, a nested split across it', () => {
  const layout = dockLayout({
    kind: 'split',
    direction: 'horizontal',
    sizes: [60, 40],
    children: [
      panel('board'),
      { kind: 'split', direction: 'vertical', sizes: [75, 25], children: [panel('tape'), panel('movers')] },
    ],
  });
  expect(layout.grid.orientation).toBe('HORIZONTAL');
  expect(shape(layout.grid.root)).toEqual([['board', 600], [['tape', 750], ['movers', 250]]]);
  expect(Object.keys(layout.panels)).toEqual(['board', 'tape', 'movers']);
});

it('splices a split into its parent when both go the same way', () => {
  const layout = dockLayout({
    kind: 'split',
    direction: 'vertical',
    sizes: [50, 50],
    children: [
      panel('a'),
      { kind: 'split', direction: 'vertical', sizes: [1, 3], children: [panel('b'), panel('c')] },
    ],
  });
  expect(layout.grid.orientation).toBe('VERTICAL');
  expect(shape(layout.grid.root)).toEqual([['a', 500], ['b', 125], ['c', 375]]);
});

it('shares equally when the sizes do not fit the children', () => {
  for (const sizes of [undefined, [100], [50, 0]]) {
    const layout = dockLayout({ kind: 'split', direction: 'horizontal', sizes, children: [panel('a'), panel('b')] });
    expect(shape(layout.grid.root)).toEqual([['a', 500], ['b', 500]]);
  }
});

it('fills the page with a lone panel', () => {
  const layout = dockLayout(panel('only'));
  expect(shape(layout.grid.root)).toEqual([['only', 1000]]);
});
