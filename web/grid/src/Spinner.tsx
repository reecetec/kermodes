import React from 'react';

/** The one busy indicator: tables and charts that are loading show the same (`injectGridStyles`) */
export function Spinner(): React.ReactElement {
  return (
    <svg className="kermodes-spinner" width="14" height="14" viewBox="0 0 16 16" aria-hidden="true">
      <circle cx="8" cy="8" r="6.5" fill="none" stroke="currentColor" strokeOpacity="0.25" strokeWidth="1.75" />
      <path d="M8 1.5a6.5 6.5 0 0 1 6.5 6.5" fill="none" stroke="currentColor" strokeWidth="1.75" strokeLinecap="round" />
    </svg>
  );
}
