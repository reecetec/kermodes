import path from 'node:path';
import tailwindcss from '@tailwindcss/vite';
import react from '@vitejs/plugin-react';
import { defineConfig } from 'vite';

// Served by Kermodes at /apps/market/ (`kermodes ui --apps web/dist/apps`), so assets are
// relative; in development Vite proxies the WebSocket to a running `kermodes ui` or `dev`.
const gatewayPort = process.env.KERMODES_PORT ?? '8765';

export default defineConfig({
  base: './',
  plugins: [react(), tailwindcss()],
  server: {
    proxy: { '/ws': { target: `http://127.0.0.1:${gatewayPort}`, ws: true } },
  },
  build: {
    outDir: path.resolve(import.meta.dirname, '../../dist/apps/market'),
    emptyOutDir: true,
  },
});
