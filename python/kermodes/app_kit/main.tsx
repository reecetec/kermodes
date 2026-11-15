// An app's page: its App.tsx (its default export), connected to its own server and themed.
import { createRoot } from 'react-dom/client';
import { KermodesProvider } from '@kermodes/react';
import App from '@app/App';
import './app.css';

createRoot(document.getElementById('root') as HTMLElement).render(
  <KermodesProvider>
    <App />
  </KermodesProvider>
);
