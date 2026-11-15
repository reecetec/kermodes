import { Orientation, type SerializedDockview } from 'dockview-react';

/** A dashboard's layout as its page writes it: panels, and splits of them (layout.tsx) */
export type LayoutNode =
  | { kind: 'panel'; id: string; title: string }
  | {
      kind: 'split';
      direction: 'horizontal' | 'vertical';
      /** Percentages, one per child; equal shares when missing or not one per child */
      sizes?: number[];
      children: LayoutNode[];
    };

type Grid = SerializedDockview['grid']['root'];

// The serialized extent of the whole grid: dockview scales the sizes to the page when it lays out.
const EXTENT = 1000;

/** Each child's share of its split, summing to 1 */
function shares(sizes: number[] | undefined, count: number): number[] {
  if (sizes == null || sizes.length !== count || sizes.some(size => !(size > 0))) {
    return Array.from({ length: count }, () => 1 / count);
  }
  const total = sizes.reduce((sum, size) => sum + size, 0);
  return sizes.map(size => size / total);
}

/**
 * A split's children with their shares, a child split in the same direction spliced into it:
 * dockview alternates direction at each level, so a level can only hold the other direction.
 */
function flatten(
  split: Extract<LayoutNode, { kind: 'split' }>
): { node: LayoutNode; share: number }[] {
  const children = split.children;
  return shares(split.sizes, children.length).flatMap((share, i) => {
    const child = children[i];
    if (child.kind === 'split' && child.direction === split.direction) {
      return flatten(child).map(inner => ({ node: inner.node, share: share * inner.share }));
    }
    return [{ node: child, share }];
  });
}

/**
 * The dockview layout a page's tree describes: each panel its own group, each split a branch
 * of its children's groups at their shares. Given to `api.fromJSON` when the browser has no
 * layout of its own for the page.
 */
export function dockLayout(root: LayoutNode): SerializedDockview {
  const panels: SerializedDockview['panels'] = {};
  let groups = 0;
  // `width` and `height` are the node's extent; `size` its extent along its parent's direction.
  const grid = (node: LayoutNode, width: number, height: number, size: number): Grid => {
    if (node.kind === 'panel') {
      panels[node.id] = { id: node.id, contentComponent: 'panel', title: node.title };
      groups += 1;
      return {
        type: 'leaf',
        data: { views: [node.id], activeView: node.id, id: String(groups) },
        size,
      };
    }
    const horizontal = node.direction === 'horizontal';
    return {
      type: 'branch',
      data: flatten(node).map(({ node: child, share }) =>
        horizontal
          ? grid(child, width * share, height, width * share)
          : grid(child, width, height * share, height * share)
      ),
      size,
    };
  };
  const split: LayoutNode =
    root.kind === 'panel' ? { kind: 'split', direction: 'horizontal', children: [root] } : root;
  const horizontal = split.direction === 'horizontal';
  return {
    grid: {
      // The root's size is across its direction: the height of a row, the width of a column.
      root: grid(split, EXTENT, EXTENT, EXTENT),
      width: EXTENT,
      height: EXTENT,
      orientation: horizontal ? Orientation.HORIZONTAL : Orientation.VERTICAL,
    },
    panels,
  };
}
