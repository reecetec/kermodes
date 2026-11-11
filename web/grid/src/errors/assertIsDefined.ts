// Adapted from @deephaven/grid (Apache-2.0), web-client-ui 0bab596
import AssertionError from './AssertionError';

export function assertIsDefined<T>(val: T): asserts val is NonNullable<T> {
  if (val === undefined || val === null) {
    throw new AssertionError(
      `Expected 'val' to be defined, but received ${val}`
    );
  }
}

export default assertIsDefined;
