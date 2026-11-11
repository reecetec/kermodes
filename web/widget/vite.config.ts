import { defineConfig, transformWithEsbuild, type Plugin } from 'vite';

// Library mode leaves ES output unminified so consumers can tree-shake it. This
// bundle is consumed as-is by anywidget (sent over the comm for every widget),
// so minify the finished chunk.
const minifyBundle: Plugin = {
  name: 'minify-bundle',
  async generateBundle(_options, bundle) {
    for (const chunk of Object.values(bundle)) {
      if (chunk.type === 'chunk') {
        const result = await transformWithEsbuild(chunk.code, chunk.fileName, {
          minify: true,
          format: 'esm',
          target: 'es2022',
        });
        chunk.code = result.code;
      }
    }
  },
};

// One self-contained ES module for anywidget's `_esm`: React, the grid and
// apache-arrow are bundled; the grid injects its own styles at runtime.
export default defineConfig({
  define: {
    'process.env.NODE_ENV': JSON.stringify('production'),
  },
  plugins: [minifyBundle],
  build: {
    target: 'es2022',
    minify: false,
    lib: {
      entry: 'src/index.tsx',
      formats: ['es'],
      fileName: () => 'widget.js',
    },
  },
});
