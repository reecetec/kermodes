import { createRoot } from 'react-dom/client';
import { KermodesConnection } from '@kermodes/client';
import App from './App';
import './index.css';

const socketUrl = `${location.protocol === 'https:' ? 'wss' : 'ws'}://${location.host}/ws`;

// One socket for the page's lifetime, shared by every open table
const connection = new KermodesConnection(socketUrl);

createRoot(document.getElementById('root') as HTMLElement).render(
  <App connection={connection} />
);
