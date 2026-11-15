import { describe, expect, it } from 'bun:test';
import { pageServer } from './index';

describe('pageServer', () => {
  const at = (pathname: string, protocol = 'http:') => pageServer({ protocol, host: 'k.example.com', pathname });

  it("is an app's own server, under its path, from any page of it", () => {
    expect(at('/apps/ticker/market/')).toBe('ws://k.example.com/apps/ticker/market/ws');
    expect(at('/apps/ticker/market/quotes/AAPL')).toBe('ws://k.example.com/apps/ticker/market/ws');
    expect(at('/apps/ticker/market/', 'https:')).toBe('wss://k.example.com/apps/ticker/market/ws');
  });

  it("is the origin's for any other page", () => {
    expect(at('/')).toBe('ws://k.example.com/ws');
    expect(at('/catalog')).toBe('ws://k.example.com/ws');
    // Not an app's path: an app's needs its project, its name, and the slash after
    expect(at('/apps/ticker/market')).toBe('ws://k.example.com/ws');
  });
});
