/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  darkMode: "class",
  theme: {
    extend: {
      colors: {
        'page-bg': '#060B0E',
        'surface-card': '#0D171B',
        'surface-elevated': '#13232A',
        'border-custom': '#1E3640',
        'text-primary': '#D5EEEC',
        'text-secondary': '#7FA3A6',
        'accent': '#5FD0C8',
        'accent-hover': '#7FE0D8',
        'accent-pressed': '#46B8B0',
        'on-accent': '#041014',
        'status-success': '#5FD99A',
        'status-warning': '#E8B86A',
        'status-error': '#E8737F',
        'badge-video': '#6FB8E8',
        'badge-image': '#E8B86A',
        'badge-audio': '#5FD99A',
        // Compatibility with phosphor theme:
        'surface': '#0f1417',
        'surface-dim': '#0f1417',
        'surface-bright': '#343a3d',
        'surface-container-lowest': '#090f12',
        'surface-container-low': '#171c20',
        'surface-container': '#1b2024',
        'surface-container-high': '#252b2e',
        'surface-container-highest': '#303639',
        'on-surface': '#dee3e7',
        'on-surface-variant': '#bcc9c7',
        'primary': '#7dece4',
        'primary-container': '#5fd0c8',
        'on-primary': '#003734',
        'on-primary-container': '#005753',
        'primary-fixed': '#86f5ec',
        'secondary': '#b8c9d2',
        'secondary-container': '#394951',
        'error': '#ffb4ab',
        'error-container': '#93000a',
        'outline': '#869391',
        'outline-variant': '#3d4948',
      },
      fontFamily: {
        sans: ['Inter', 'system-ui', 'sans-serif'],
        mono: ['"JetBrains Mono"', 'monospace'],
        display: ['Inter', 'sans-serif'],
      },
      spacing: {
        'gutter': '1.25rem',
        'margin': '1.5rem',
        'space-xs': '0.25rem',
        'space-sm': '0.5rem',
        'space-md': '1rem',
        'space-lg': '1.5rem',
        'space-xl': '2.5rem'
      },
      borderRadius: {
        'card': '12px',
        'ui': '16px'
      }
    },
  },
  plugins: [],
}
