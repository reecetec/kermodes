import { createRoot } from 'react-dom/client';
import { KermodesConnection } from '@kermodes/client';
import App, { applyTheme, savedTheme } from './App';
import './index.css';

// Before the first render, so the grid's first theme reads the saved tokens
applyTheme(savedTheme());

const socketUrl = `${location.protocol === 'https:' ? 'wss' : 'ws'}://${location.host}/ws`;

// One socket for the page's lifetime, shared by every open table
const connection = new KermodesConnection(socketUrl);

createRoot(document.getElementById('root') as HTMLElement).render(
  <App connection={connection} />
);
