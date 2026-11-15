# Theme and visual design

Living document: it describes the design as it is and why. Change it in the same commit that
changes the design; where the code teaches us otherwise, fix this file.

The tokens are [`web/theme/tokens.css`](../web/theme/tokens.css), the one source for the web
UI, the notebook grid, and any app built on Kermodes. `web/theme/tailwind.css` maps them into
Tailwind under shadcn/ui's names. A rebrand is that file.

## Direction

- **Product.** Live and historical tables for people who trade and the engineers beside them:
  a ticking grid, the deployments that compute it, the lake that keeps it.
- **Character.** Exact, calm, quick. Deephaven's density and Apple's restraint: the feel of
  well-made professional hardware, where every part is there for a reason and nothing is
  missing. Simple, but no simpler than the work needs.
- **World.** A desk of instruments at night: graphite surfaces, black glass, figures that line
  up. The neutrals are graphite with no tint; colour belongs to the data.
- **Moments.** The data itself: numbers coloured by sign, dates by type, a cell that just
  changed. Chrome never competes with it.
- **Signature.** A cell that changes flashes once, faintly, and fades. It is the only thing that
  moves on its own, and it is how a person sees a table is live without a badge saying so.

## Principles

1. **Quiet chrome, rich data.** Surfaces, borders, and text stay neutral. Colour appears for the
   current place, the primary action, focus, state, and data, nowhere else. No gradients, glow,
   glass, or decorative badges.
2. **Normal looks normal.** A healthy, connected, ticking state shows nothing extra. Only what
   needs attention speaks: a reconnect, a failure, a stale view. A word carries the meaning;
   colour confirms it and never stands alone.
3. **One unit, one edge.** A thing and its parts read as one object. The grid fills its panel; its
   header, filter row, and cells share one set of hairlines. Panels meet edge to edge.
4. **Every number lines up.** Tabular figures, right-aligned, one format per column. Cells are in
   the mono so digits align on canvas, where tabular figures cannot be switched on.
5. **Own every control.** Menus, selects, scrollbars, tooltips, and dialogs are Kermodes'
   (shadcn/ui on Radix, themed by the tokens), never the operating system's defaults.
6. **Show what is happening.** Loading, updating, reconnecting, empty, and failed each have a
   visible state at the place they affect, not only a word in a corner.

## Theme model

Dark is canonical: `:root` holds it. Light redefines every colour under `[data-theme='light']`,
on `<html>` or any subtree. The app sets `data-theme` on `<html>` before its first render and
again when the theme flips, then rebuilds the grid's theme, so both change in one frame.

- Components use semantic tokens (`bg-muted`, `var(--border)`), never raw colours.
- Every colour token is defined in both themes, as hex: the grid reads tokens into
  space-separated colour lists, which `rgb(… / …)` would split (`web/theme/tokens.test.ts`).
- The canvas grid takes its colours from `gridThemeOf(element)`, which reads the tokens in effect
  at its host element. The grid's DOM parts (filter cells, menu, toast, footer) use the CSS
  variables directly.
- The notebook widget injects the tokens scoped to its own root (`.kermodes-widget`), so the
  notebook page keeps its variables, and follows the notebook's light or dark theme.
- An app built on Kermodes imports `@import '@kermodes/theme/tailwind.css'` after Tailwind and
  gets the same look by default, with any component it likes.

## Tokens

Ratios are against the canvas (`--background`) and the header and chrome surface (`--muted`).
Targets: 4.5:1 for text, 3:1 for control edges.

| Token | Dark | Light | Ratio dark | Ratio light | Use |
|---|---|---|---|---|---|
| `--background` | `#121214` | `#ffffff` | | | content, the grid's cells |
| `--sidebar` | `#19191c` | `#f5f5f7` | | | the top bar and side panels |
| `--muted` | `#1d1d20` | `#f3f3f5` | | | grid header and filter row, footers |
| `--card` / `--popover` | `#1a1a1d` / `#262629` | `#ffffff` | | | raised groups / menus, dialogs |
| `--secondary` | `#2a2a2e` | `#ebebee` | | | the current item, secondary buttons |
| `--accent` / `--stripe` | `#ffffff0f` / `05` | `#0000000b` / `05` | | | hover fill / alternate grid rows |
| `--foreground` | `#f2f2f4` | `#1d1d1f` | 16.74 / 15.04 | 16.83 / 15.19 | text |
| `--muted-foreground` | `#a1a1a7` | `#6b6b70` | 7.28 / 6.54 | 5.30 / 4.78 | labels, headers, meta |
| `--faint-foreground` | `#6c6c72` | `#a1a1a6` | 3.59 / 3.22 | 2.57 / 2.32 | disabled and placeholders only |
| `--border` | `#ffffff17` | `#0000001a` | | | hairlines between things |
| `--input` | `#737379` | `#86868b` | 3.97 / 3.57 | 3.62 / 3.27 | a control's edge |
| `--primary` / `--ring` | `#7aa7ff` | `#2a5fd0` | 7.84 / 7.05 | 5.76 / 5.20 | current place, primary action, focus |
| `--selection` / `--flash` | primary at 22% / 35% | primary at 15% / 25% | | | selected cells / a changed cell |
| `--success` | `#62cc94` | `#1b7a4a` | 9.43 / 8.48 | 5.34 / 4.82 | a good state |
| `--warning` | `#e6b85c` | `#8a5a00` | 10.14 / 9.11 | 5.93 / 5.35 | needs attention |
| `--destructive` | `#ff8f86` | `#c03a2b` | 8.49 / 7.63 | 5.41 / 4.88 | failed, errors |
| `--positive` / `--negative` | `#8dd6a0` / `#ff9b94` | `#1b7a4a` / `#c03a2b` | ≥ 9.2 | ≥ 4.8 | numbers by sign |
| `--date` | `#e5c07b` | `#8a5a00` | 10.83 / 9.73 | 5.93 / 5.35 | dates and times |

`--primary` is the only accent. It never fills more than one control per view. On dark, text on
it is `--primary-foreground` (`#0b1220`), since white on the light blue would not hold 4.5:1.

## Type, space, shape, motion

- **Type.** The system face (`--font-sans`: SF Pro on Apple devices, Segoe UI on Windows), so
  the UI feels native and loads nothing. `--font-mono` for figures in the grid, identifiers, and
  code. Weights 400 and 500, 600 at most. Sentence case everywhere; no uppercase tracked labels.
- **Space.** A 4px grid. Grid rows are 22px and its header 30px; the filter row 26px.
- **Radius.** `--radius` is 8px: controls 6px, groups and menus 8–10px, items inside a menu
  6px, so curves nest. Panels and the grid have square edges: they meet other panels.
- **Elevation.** Surfaces step in lightness; only overlays (menus, popovers, toasts) cast a
  shadow (`--shadow`).
- **Motion.** `--duration-fast` (150ms) for hover and colour, `--duration-base` (200ms) for things
  that open, both on `--ease-out`. Nothing loops except a spinner while something is truly in
  progress. All of it stops under `prefers-reduced-motion`.

## Live data states

| State | What shows |
|---|---|
| Connected, ticking | Nothing extra. Changed cells flash. |
| Reconnecting | A quiet "Reconnecting" with a spinner in the top bar; open views dim until fresh data arrives. |
| Loading a view | Skeleton rows until the first rows arrive. |
| Updating a sort or filter | The current rows dim under a spinner until the engine applies it. |
| Failed | The view says what failed and why, in place of the rows. |

The status line under a grid says rows and whether it is filtered, in the reader's terms: never
engine terms such as cycles.

## Layout

The Tables page is panels (dockview): any table can be dragged beside, above, or below another,
or into another's tabs, and the layout is kept per browser. The grid fills its panel.

## Checks

- `web/theme/tokens.test.ts`: every colour token is defined in both themes, as hex.
- `web/app/src/controls.test.ts`: no native select, checkbox, or radio in the app.
- Verify a design change in a browser, in both themes, and look at it.
