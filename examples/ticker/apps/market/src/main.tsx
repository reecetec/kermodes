import { createRoot } from 'react-dom/client';
import { KermodesProvider } from '@kermodes/react';
import { Market } from './Market';
import './index.css';

createRoot(document.getElementById('root') as HTMLElement).render(
  <KermodesProvider>
    <Market />
  </KermodesProvider>
);
