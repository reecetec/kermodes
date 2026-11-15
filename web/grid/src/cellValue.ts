import { copyValue } from './copy';

/** A cell's whole value as the value popover shows it */
export type ShownValue = { text: string; isJson: boolean };

/**
 * `value` in full: text holding a JSON object or array pretty-printed with two-space indents,
 * anything else as copying it would give (numbers unrounded, dates in full). A null is the
 * word null, so an empty popover never means "no value".
 */
export function shownValue(type: string, value: unknown): ShownValue {
  if (value == null) {
    return { text: 'null', isJson: false };
  }
  const text = copyValue(type, value);
  const trimmed = text.trim();
  if (trimmed.startsWith('{') || trimmed.startsWith('[')) {
    try {
      return { text: JSON.stringify(JSON.parse(trimmed), null, 2), isJson: true };
    } catch {
      // Brackets but not JSON: shown as it is
    }
  }
  return { text, isJson: false };
}
