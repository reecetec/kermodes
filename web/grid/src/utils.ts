// Adapted from @deephaven/utils (Apache-2.0), web-client-ui 0bab596
// The few helpers @deephaven/grid used from @deephaven/utils and lodash.clamp.

export const EMPTY_ARRAY: readonly never[] = Object.freeze([]);

export function clamp(value: number, lower: number, upper: number): number {
  return Math.min(Math.max(value, lower), upper);
}

export function assertNotNull<T>(
  value: T | null | undefined,
  message = 'Value is null or undefined'
): asserts value is T {
  if (value == null) throw new Error(message);
}

export function assertNotNaN(value: number, message = 'Value is NaN'): void {
  if (Number.isNaN(value)) throw new Error(message);
}

/**
 * Retrieve a value from a map. If the value is not found and no default value is provided, throw.
 * Use when the value _must_ be present
 * @param map The map to get the value from
 * @param key The key to fetch the value for
 * @param defaultValue A default value to set if the key is not present
 * @returns The value set for that key
 */
export function getOrThrow<K, V>(
  map: ReadonlyMap<K, V>,
  key: K,
  defaultValue: V | undefined = undefined
): V {
  const value = map.get(key) ?? defaultValue;
  if (value !== undefined) {
    return value;
  }

  throw new Error(`Missing value for key ${key}`);
}

/** Keys whose values differ (by identity) between two objects */
export function getChangedKeys(
  oldObject: object,
  newObject: object
): string[] {
  const oldRecord = oldObject as Record<string, unknown>;
  const newRecord = newObject as Record<string, unknown>;
  const keys = new Set([...Object.keys(oldRecord), ...Object.keys(newRecord)]);
  const changedKeys: string[] = [];
  keys.forEach(key => {
    if (oldRecord[key] !== newRecord[key]) {
      changedKeys.push(key);
    }
  });
  return changedKeys;
}
