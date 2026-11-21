import { useEffect, useLayoutEffect, useRef, useState, type CSSProperties } from 'react';
import Plotly from 'plotly.js-finance-dist';
import { DataType, type Table, type Timestamp, type Vector } from 'apache-arrow';
import { injectGridStyles, Spinner } from '@kermodes/grid';
import { useSeries, type SeriesOptions } from './index';

export type ChartProps = Omit<SeriesOptions, 'range'> & {
  /** The table charted: any the server lists, stored or computed */
  table: string;
  /** How each y is drawn: `line` (by default), `area`, `bar`, or `scatter` */
  type?: 'line' | 'area' | 'bar' | 'scatter';
  className?: string;
  style?: CSSProperties;
};

/** The page's tokens a chart draws with, read where it sits (Plotly takes colours, not variables) */
type Look = {
  sans: string;
  mono: string;
  text: string;
  muted: string;
  border: string;
  popover: string;
  colors: string[];
};

function lookOf(element: HTMLElement): Look {
  const style = getComputedStyle(element);
  const token = (name: string) => style.getPropertyValue(`--${name}`).trim();
  return {
    sans: token('font-sans'),
    mono: token('font-mono'),
    text: token('foreground'),
    muted: token('muted-foreground'),
    border: token('border'),
    popover: token('popover'),
    colors: [1, 2, 3, 4, 5, 6].map(i => token(`chart-${i}`)),
  };
}

function isTime(vector: Vector): boolean {
  return DataType.isTimestamp(vector.type) || DataType.isDate(vector.type);
}

/**
 * A column's values as Plotly draws them, NULL as a gap. Times go as UTC date strings, which
 * Plotly draws as written: given epoch milliseconds, it would draw the browser's local time,
 * where the grid shows UTC.
 */
function values(vector: Vector): (number | string | null)[] {
  const time = isTime(vector);
  return Array.from(vector, (value: unknown) => {
    if (value == null) {
      return null;
    }
    if (time) {
      return new Date(value as number).toISOString().slice(0, 23).replace('T', ' ');
    }
    return typeof value === 'bigint' ? Number(value) : (value as number | string);
  });
}

// Milliseconds to a timestamp's unit (Arrow's TimeUnit: second, milli, micro, nano)
const PER_MILLISECOND = [1e-3, 1, 1e3, 1e6];

// The placeholder bars' heights (percent of the chart) while it loads: varied, but fixed, so
// nothing moves until the points arrive.
const SKELETON_BARS = [38, 52, 46, 61, 57, 70, 64, 49, 58, 66, 54, 72];

/**
 * A value of Plotly's x axis in x's own units, as the server ranges by: a time's unit since the
 * epoch, a date's days, a number as is. Plotly gives a date axis's ends as UTC strings as drawn
 * ("2026-11-16 00:10:00.1234", or a date alone).
 */
function toUnits(value: string | number, x: Vector): number {
  if (!isTime(x)) {
    return Number(value);
  }
  let ms = Number(value);
  if (typeof value === 'string') {
    const iso = value.trim().replace(' ', 'T').replace(/(\.\d{3})\d+/, '$1');
    ms = Date.parse(iso.includes('T') ? `${iso}Z` : `${iso}T00:00Z`);
  }
  if (DataType.isDate(x.type)) {
    return ms / 86_400_000;
  }
  return ms * PER_MILLISECOND[(x.type as Timestamp).unit];
}

function axisType(vector: Vector): 'date' | 'category' | 'linear' {
  if (isTime(vector)) {
    return 'date';
  }
  return DataType.isUtf8(vector.type) ? 'category' : 'linear';
}

function traces(points: Table, type: NonNullable<ChartProps['type']>, colors: string[]): Plotly.Data[] {
  const [x, ...ys] = points.schema.fields.map(field => field.name);
  const xs = values(points.getChild(x) as Vector);
  return ys.map((name, i) => {
    const color = colors[i % colors.length];
    const up = values(points.getChild(name) as Vector);
    if (type === 'bar') {
      return { type: 'bar', name, x: xs, y: up, marker: { color } } as Plotly.Data;
    }
    return {
      type: 'scatter',
      name,
      x: xs,
      y: up,
      mode: type === 'scatter' ? 'markers' : 'lines',
      fill: type === 'area' ? 'tozeroy' : 'none',
      line: { color, width: 1.5 },
      marker: { color, size: 4 },
    } as Plotly.Data;
  });
}

function layout(points: Table, look: Look): Partial<Plotly.Layout> {
  const axis = {
    gridcolor: look.border,
    linecolor: look.border,
    zeroline: false,
    automargin: true,
    tickfont: { family: look.mono, size: 10, color: look.muted },
  };
  return {
    paper_bgcolor: 'transparent',
    plot_bgcolor: 'transparent',
    font: { family: look.sans, size: 11, color: look.muted },
    margin: { l: 8, r: 8, t: 8, b: 8 },
    xaxis: { ...axis, type: axisType(points.getChildAt(0) as Vector) },
    yaxis: axis,
    showlegend: points.numCols > 2,
    legend: { orientation: 'h', x: 0, y: 1, yanchor: 'bottom', font: { color: look.text } },
    hovermode: 'x unified',
    hoverlabel: {
      bgcolor: look.popover,
      bordercolor: look.border,
      font: { family: look.mono, size: 11, color: look.text },
    },
    // Zoom and pan stay where a person put them as new points arrive.
    uirevision: 'kept',
  };
}

/**
 * A live chart of any Kermodes table (docs/theme.md, Charts): each `y` against `x`, of the rows
 * `where` keeps, drawn in the page's tokens and redrawn as the table changes. The server sends at
 * most `points` (useSeries), so a chart of a day of ticks costs what its pixels do; zooming in
 * asks for that range alone, in full detail, and a double-click goes back to all of it. It
 * fills its parent, which needs a height.
 *
 *     <Chart table="md.ticks" x="ts" y="price" where="sym = 'AAPL'" />
 *     <Chart table="board" x="sym" y="change_pct" type="bar" />
 */
export function Chart({ table, x, y, where, points: budget, type = 'line', className, style }: ChartProps) {
  const [range, setRange] = useState<[number, number] | null>(null);
  const series = useSeries(table, { x, y, where, points: budget, range });
  const host = useRef<HTMLDivElement>(null);
  const [look, setLook] = useState<Look | null>(null);
  useLayoutEffect(() => {
    if (host.current != null) {
      setLook(lookOf(host.current));
    }
  }, []);

  const { points } = series;
  // The x the axis is read against when zoomed: the points' own column
  const xColumn = useRef<Vector | null>(null);
  xColumn.current = (points?.getChildAt(0) as Vector | null) ?? null;
  const listening = useRef(false);
  // Nothing to draw: before the first points, or a table with no rows (said in place below). A
  // zoom onto no rows still draws its axes, so it can be zoomed out of.
  const drawable = points != null && (points.numRows > 0 || range != null);
  useEffect(() => {
    const element = host.current;
    if (element == null || look == null) {
      return;
    }
    if (points == null || !drawable) {
      Plotly.purge(element);
      listening.current = false;
      return;
    }
    void Plotly.react(element, traces(points, type, look.colors), layout(points, look), {
      displaylogo: false,
      displayModeBar: 'hover',
      responsive: false,
    }).then(plot => {
      if (listening.current) {
        return;
      }
      listening.current = true;
      plot.on('plotly_relayout', event => {
        const column = xColumn.current;
        if (column == null || axisType(column) === 'category') {
          return;
        }
        if (event['xaxis.autorange'] === true) {
          setRange(null);
          return;
        }
        const [low, high] = [event['xaxis.range[0]'], event['xaxis.range[1]']];
        if (low != null && high != null) {
          setRange([toUnits(low, column), toUnits(high, column)]);
        }
      });
    });
  }, [points, drawable, type, look]);

  // Plotly sizes itself once; a panel resized or docked elsewhere tells it again.
  useEffect(() => {
    const element = host.current;
    if (element == null) {
      return undefined;
    }
    const observer = new ResizeObserver(() => {
      if (element.classList.contains('js-plotly-plot')) {
        void Plotly.Plots.resize(element);
      }
    });
    observer.observe(element);
    return () => {
      observer.disconnect();
      Plotly.purge(element);
    };
  }, []);

  // Loading and reconnecting look as a table's do: the same spinner in a pill, over faint
  // placeholder bars before any points, or over the points so far dimmed. A table still catching
  // up with its store is loading even once points arrive, so its first, partial frames (none,
  // for a table opened as weir replays its day) never read as empty or final.
  useEffect(() => injectGridStyles(host.current?.ownerDocument), []);
  const failed = series.status === 'failed';
  const busy =
    series.status === 'reconnecting' ? 'Reconnecting' : series.status === 'loading' ? 'Loading' : null;
  const empty = failed ? 'This chart cannot be shown' : busy != null || drawable ? null : 'No rows';
  return (
    <div className={className} style={{ position: 'relative', width: '100%', height: '100%', ...style }}>
      <div ref={host} style={{ position: 'absolute', inset: 0 }} />
      {busy != null && (
        <div
          className={drawable ? 'km-chart-loading km-chart-dim' : 'km-chart-loading'}
          aria-busy="true"
          aria-label={busy}
        >
          {!drawable && (
            <div className="km-chart-skeleton" aria-hidden="true">
              {SKELETON_BARS.map((height, at) => (
                // Fixed placeholders that never reorder
                // eslint-disable-next-line react/no-array-index-key
                <span key={at} className="kermodes-skeleton-bar" style={{ height: `${height}%` }} />
              ))}
            </div>
          )}
          <span className="kermodes-busy" role="status">
            <Spinner />
            {busy}
          </span>
        </div>
      )}
      {empty != null && (
        <div className="km-chart-empty">
          <span>{empty}</span>
          {failed && series.error != null && <span className="km-chart-reason">{series.error}</span>}
        </div>
      )}
    </div>
  );
}
