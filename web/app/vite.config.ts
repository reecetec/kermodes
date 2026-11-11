import path from 'node:path';
import tailwindcss from '@tailwindcss/vite';
import react from '@vitejs/plugin-react';
import { defineConfig } from 'vite';

// The gateway (`kermodes` serve) serves the built app at `/` from the wheel's static
// directory and the protocol on `/ws`; in development Vite proxies `/ws` to it.
const gatewayPort = process.env.KERMODES_PORT ?? '8765';

export default defineConfig({
  base: './',
  plugins: [react(), tailwindcss()],
  resolve: {
    alias: { '@': path.resolve(import.meta.dirname, 'src') },
  },
  server: {
    proxy: {
      '/ws': { target: `http://127.0.0.1:${gatewayPort}`, ws: true },
    },
  },
  build: {
    outDir: path.resolve(import.meta.dirname, '../../python/kermodes/static/app'),
    emptyOutDir: true,
    // One page: React, the grid and apache-arrow load together, so no split helps
    chunkSizeWarningLimit: 800,
  },
});
