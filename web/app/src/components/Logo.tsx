/**
 * The Kermode, or spirit bear: a black bear with cream fur. Face-on, as it is known by its tall
 * rounded ears, broad brow narrowing to a long grey muzzle, and dark nose. On the light theme a
 * thin tan line keeps the pale head from fading into the page.
 */
export function Logo({ className }: { className?: string }) {
  const outline = 'stroke-[#a08b68] stroke-[0.8] [stroke-linejoin:round] dark:stroke-none';
  return (
    <svg viewBox="0 0 32 32" aria-hidden="true" className={className}>
      <g fill="#f0e3c6" className={outline}>
        <path d="M7.4 10.8 C5.3 8.2 5 4.8 6.9 3.7 C8.8 2.7 11.4 4.4 12.9 7.3 Z" />
        <path d="M24.6 10.8 C26.7 8.2 27 4.8 25.1 3.7 C23.2 2.7 20.6 4.4 19.1 7.3 Z" />
      </g>
      <g fill="#cdb48a">
        <path d="M8.3 9 C7.1 7.4 7 5.6 8 5 C9 4.5 10.3 5.5 11.2 7.3 Z" />
        <path d="M23.7 9 C24.9 7.4 25 5.6 24 5 C23 4.5 21.7 5.5 20.8 7.3 Z" />
      </g>
      <path
        fill="#f0e3c6"
        className={outline}
        d="M16 6.4 C19.6 6.4 22.6 7.3 24.7 9 C27.1 11 27.7 14.4 26.7 18 C25.7 21.5 22.2 24.4 19.9 26.6 C18.7 27.9 17.4 28.7 16 28.7 C14.6 28.7 13.3 27.9 12.1 26.6 C9.8 24.4 6.3 21.5 5.3 18 C4.3 14.4 4.9 11 7.3 9 C9.4 7.3 12.4 6.4 16 6.4 Z"
      />
      <path
        fill="#ddd2bf"
        d="M16 17.6 C18.7 17.6 20.6 20.2 20.4 23.2 C20.2 26 18.2 28 16 28 C13.8 28 11.8 26 11.6 23.2 C11.4 20.2 13.3 17.6 16 17.6 Z"
      />
      <g fill="#26211e">
        <path d="M13.7 21 C13.7 19.8 18.3 19.8 18.3 21 C18.3 22.5 17 23.6 16 23.6 C15 23.6 13.7 22.5 13.7 21 Z" />
        <ellipse cx="12.5" cy="15" rx="0.8" ry="0.72" />
        <ellipse cx="19.5" cy="15" rx="0.8" ry="0.72" />
      </g>
      <path d="M16 23.6 L16 25.2" stroke="#26211e" strokeWidth="0.6" strokeLinecap="round" />
    </svg>
  );
}
