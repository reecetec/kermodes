// Plotly's finance bundle (lines, bars, OHLC, candlesticks) is plotly.js with fewer traces: the
// same API, typed by @types/plotly.js. Unminified, since Vite's dev server cannot read the
// pre-bundled minified one (an app's build minifies it anyway).
declare module 'plotly.js-finance-dist' {
  import * as Plotly from 'plotly.js';
  export default Plotly;
}
