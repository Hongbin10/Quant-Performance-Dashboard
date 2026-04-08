import { createTheme } from '@mui/material/styles'

// ── Palette ───────────────────────────────────────────────────────────────────
// Deep charcoal background, warm white text, electric cyan accent.
// Numbers use IBM Plex Mono for instant readability at a glance.

const CHARCOAL = '#0d1117'
const SURFACE  = '#161b22'
const BORDER   = '#21262d'
const MUTED    = '#8b949e'
const TEXT     = '#e6edf3'
const ACCENT   = '#58a6ff'   // GitHub-blue: familiar to devs, crisp on dark bg
const GREEN    = '#3fb950'
const RED      = '#f85149'
const AMBER    = '#d29922'

export const theme = createTheme({
  palette: {
    mode: 'dark',
    background: { default: CHARCOAL, paper: SURFACE },
    primary:    { main: ACCENT },
    success:    { main: GREEN },
    error:      { main: RED },
    warning:    { main: AMBER },
    text:       { primary: TEXT, secondary: MUTED },
    divider:    BORDER,
  },

  typography: {
    fontFamily: '"IBM Plex Sans", sans-serif',
    // Mono for any numeric display
    h1: { fontFamily: '"IBM Plex Mono", monospace', fontSize: '2rem',   fontWeight: 600 },
    h2: { fontFamily: '"IBM Plex Mono", monospace', fontSize: '1.5rem', fontWeight: 600 },
    h3: { fontSize: '1.125rem', fontWeight: 600 },
    h4: { fontSize: '0.875rem', fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.08em', color: MUTED },
    body1: { fontSize: '0.875rem', lineHeight: 1.6 },
    body2: { fontSize: '0.8125rem', color: MUTED },
    caption: { fontSize: '0.75rem', fontFamily: '"IBM Plex Mono", monospace' },
  },

  shape: { borderRadius: 6 },

  components: {
    MuiCssBaseline: {
      styleOverrides: {
        body: { background: CHARCOAL, scrollbarWidth: 'thin', scrollbarColor: `${BORDER} transparent` },
        '*::-webkit-scrollbar':       { width: '6px', height: '6px' },
        '*::-webkit-scrollbar-track': { background: 'transparent' },
        '*::-webkit-scrollbar-thumb': { background: BORDER, borderRadius: '3px' },
      },
    },
    MuiDrawer: {
      styleOverrides: {
        paper: { background: SURFACE, borderRight: `1px solid ${BORDER}`, width: 225 },
      },
    },
    MuiAppBar: {
      styleOverrides: {
        root: { background: SURFACE, borderBottom: `1px solid ${BORDER}`, boxShadow: 'none' },
      },
    },
    MuiCard: {
      styleOverrides: {
        root: {
          background: SURFACE,
          border: `1px solid ${BORDER}`,
          boxShadow: 'none',
          borderRadius: 8,
        },
      },
    },
    MuiChip: {
      styleOverrides: {
        root: { fontFamily: '"IBM Plex Mono", monospace', fontSize: '0.75rem' },
      },
    },
    MuiTab: {
      styleOverrides: {
        root: {
          textTransform: 'none',
          fontWeight: 500,
          fontSize: '0.875rem',
          minHeight: 44,
        },
      },
    },
    MuiButton: {
      styleOverrides: {
        root: { textTransform: 'none', fontWeight: 500 },
      },
    },
    MuiTableCell: {
      styleOverrides: {
        root: { borderColor: BORDER },
        head: { color: MUTED, fontWeight: 600, fontSize: '0.75rem', textTransform: 'uppercase', letterSpacing: '0.06em' },
      },
    },
    MuiSelect: {
      styleOverrides: {
        root: { fontSize: '0.875rem' },
      },
    },
    MuiOutlinedInput: {
      styleOverrides: {
        notchedOutline: { borderColor: BORDER },
      },
    },
  },
})

// Colour helpers for PnL — use these everywhere for consistency
export const pnlColor = (value: number) =>
  value > 0 ? GREEN : value < 0 ? RED : MUTED

export const CHART_COLORS = [ACCENT, GREEN, AMBER, '#bc8cff', '#ff7b72', '#79c0ff']
