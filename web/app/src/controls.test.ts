import { describe, expect, it } from 'bun:test';
import { readFileSync, readdirSync } from 'node:fs';
import { join, relative } from 'node:path';

// docs/theme.md, "Own every control": the operating system's selects, checkboxes, and radios
// ignore the theme, so the app uses its own (components/ui) instead.
const NATIVE = /<select\b|type=["'](checkbox|radio)["']/;
const SRC = import.meta.dir;

function sources(dir: string): string[] {
  return readdirSync(dir, { withFileTypes: true }).flatMap(entry => {
    const path = join(dir, entry.name);
    if (entry.isDirectory()) {
      return sources(path);
    }
    return entry.name.endsWith('.tsx') ? [path] : [];
  });
}

describe('controls', () => {
  it('uses no native select, checkbox, or radio', () => {
    const native = sources(SRC)
      .filter(path => NATIVE.test(readFileSync(path, 'utf8')))
      .map(path => relative(SRC, path));
    expect(native).toEqual([]);
  });
});
