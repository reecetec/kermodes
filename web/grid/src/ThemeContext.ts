// Adapted from @deephaven/grid (Apache-2.0), web-client-ui 0bab596
import React from 'react';
import { type GridTheme as GridThemeType } from './GridTheme';

export type ThemeContextValue = Partial<GridThemeType>;

export const ThemeContext: React.Context<ThemeContextValue> =
  React.createContext({});

export default ThemeContext;
