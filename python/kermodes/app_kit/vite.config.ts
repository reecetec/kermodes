// The platform's build of an app's page (architecture.md §15.2). `kermodes dev` and the cluster's
// build Job write this beside the app, into the project's .kermodes/apps/<name>/, so its imports
// resolve from the project's node_modules. An app is its App.tsx, which main.tsx mounts in
// Kermodes' provider and theme.
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import tailwindcss from '@tailwindcss/vite';
import react from '@vitejs/plugin-react';
import { type Alias, defineConfig } from 'vite';

function required(name: string): string {
  const value = process.env[name];
  if (value == null || value === '') throw new Error(`${name} is not set: run the app with kermodes`);
  return value;
}

const app = required('KERMODES_APP_DIR');
const project = required('KERMODES_PROJECT');
// A Kermodes checkout's web/ (`kermodes dev` from one): the packages from source, so an edit to
// the theme, layout, or table reloads the app at once.
const web = process.env.KERMODES_WEB;
const fromSource: Alias[] =
  web == null
    ? []
    : [
        { find: /^@kermodes\/react\/table$/, replacement: `${web}/react/src/table.tsx` },
        { find: /^@kermodes\/react$/, replacement: `${web}/react/src/index.tsx` },
        { find: /^@kermodes\/client$/, replacement: `${web}/client/src/index.ts` },
        { find: /^@kermodes\/grid$/, replacement: `${web}/grid/src/index.ts` },
        { find: /^@kermodes\/theme\/(.*)$/, replacement: `${web}/theme/$1` },
      ];

export default defineConfig({
  root: path.dirname(fileURLToPath(import.meta.url)),
  plugins: [react(), tailwindcss()],
  resolve: {
    alias: [{ find: /^@app\//, replacement: `${app}/` }, ...fromSource],
    // One React, the project's, though the checkout's packages import their own.
    dedupe: ['react', 'react-dom'],
  },
  server: { fs: { allow: [project, ...(web == null ? [] : [web])] } },
});
