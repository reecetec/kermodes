import tailwindcss from '@tailwindcss/vite';
import react from '@vitejs/plugin-react';
import { defineConfig } from 'vite';

// Kermodes serves the app at /apps/<project>/<name>/ and gives Vite that as --base, both under
// `kermodes dev` and when it builds the app for the cluster; the page reaches /ws on its origin.
export default defineConfig({
  plugins: [react(), tailwindcss()],
});
