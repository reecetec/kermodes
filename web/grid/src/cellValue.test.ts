import { describe, expect, it } from 'bun:test';
import { shownValue } from './cellValue';

describe('shownValue', () => {
  it('pretty-prints text holding a JSON object or array', () => {
    expect(shownValue('utf8', ' {"side":"buy","fills":[1,2]} ')).toEqual({
      text: '{\n  "side": "buy",\n  "fills": [\n    1,\n    2\n  ]\n}',
      isJson: true,
    });
    expect(shownValue('utf8', '[]')).toEqual({ text: '[]', isJson: true });
  });

  it('shows other text, and brackets that are not JSON, as they are', () => {
    expect(shownValue('utf8', 'routed to dark pool')).toEqual({
      text: 'routed to dark pool',
      isJson: false,
    });
    expect(shownValue('utf8', '[draft] {todo')).toEqual({ text: '[draft] {todo', isJson: false });
    // A bare JSON number or string is not worth reformatting
    expect(shownValue('utf8', '"quoted"').isJson).toBe(false);
  });

  it('shows numbers unrounded and a null as null', () => {
    expect(shownValue('float64', 0.1 + 0.2).text).toBe('0.30000000000000004');
    expect(shownValue('utf8', null)).toEqual({ text: 'null', isJson: false });
  });
});
