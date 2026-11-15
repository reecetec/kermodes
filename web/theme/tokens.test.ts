import { describe, expect, it } from 'bun:test';
import { readFileSync } from 'node:fs';

const css = readFileSync(new URL('./tokens.css', import.meta.url), 'utf8');

/** The custom properties of the rule block for `selector` (its last selector), with values */
function block(selector: string): Map<string, string> {
  const start = css.indexOf(`${selector} {`);
  expect(start).toBeGreaterThan(-1);
  const body = css.slice(start, css.indexOf('}', start));
  return new Map(
    [...body.matchAll(/(--[\w-]+):\s*([^;]+);/g)].map(([, name, value]) => [name, value.trim()])
  );
}

const dark = block("[data-theme='dark']");
const light = block("[data-theme='light']");
const isColour = (value: string) => value.startsWith('#');

describe('design tokens', () => {
  it('defines every colour in both themes', () => {
    const colours = (theme: Map<string, string>) =>
      [...theme].filter(([, value]) => isColour(value)).map(([name]) => name).sort();
    expect(colours(light)).toEqual(colours(dark));
  });

  it('writes colours as hex, which the grid can split into colour lists', () => {
    for (const theme of [dark, light]) {
      for (const [name, value] of theme) {
        if (!value.startsWith('var(') && name !== 'color-scheme') {
          expect({ name, value }).toEqual({ name, value: expect.stringMatching(/^#[0-9a-f]{6}([0-9a-f]{2})?$/) });
        }
      }
    }
  });
});
