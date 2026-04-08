import { createContext, useContext, useState, type ReactNode } from 'react'
import { ThemeProvider as MuiThemeProvider } from '@mui/material'
import { createTheme } from '@mui/material/styles'

const ACCENT = '#58a6ff'
const GREEN  = '#3fb950'
const RED    = '#f85149'
const AMBER  = '#d29922'

function buildTheme(mode: 'dark' | 'light') {
  const isDark = mode === 'dark'

  const CHARCOAL = isDark ? '#0d1117' : '#f6f8fa'
  const SURFACE  = isDark ? '#161b22' : '#ffffff'
  const BORDER   = isDark ? '#21262d' : '#d0d7de'
  const MUTED    = isDark ? '#8b949e' : '#656d76'
  const TEXT     = isDark ? '#e6edf3' : '#1f2328'

  return createTheme({
    palette: {
      mode,
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
          body: {
            background: CHARCOAL,
            scrollbarWidth: 'thin',
            scrollbarColor: `${BORDER} transparent`,
          },
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
          root: { background: SURFACE, border: `1px solid ${BORDER}`, boxShadow: 'none', borderRadius: 8 },
        },
      },
      MuiChip: {
        styleOverrides: {
          root: { fontFamily: '"IBM Plex Mono", monospace', fontSize: '0.75rem' },
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
        styleOverrides: { root: { fontSize: '0.875rem' } },
      },
      MuiOutlinedInput: {
        styleOverrides: { notchedOutline: { borderColor: BORDER } },
      },
    },
  })
}

// ── Context ────────────────────────────────────────────────────────────────────
interface ThemeModeCtx {
  mode:   'dark' | 'light'
  toggle: () => void
}

const ThemeModeContext = createContext<ThemeModeCtx>({
  mode: 'dark', toggle: () => {},
})

export function useThemeMode() {
  return useContext(ThemeModeContext)
}

export function ThemeModeProvider({ children }: { children: ReactNode }) {
  const [mode, setMode] = useState<'dark' | 'light'>(() => {
    return (localStorage.getItem('colorMode') as 'dark' | 'light') ?? 'dark'
  })

  const toggle = () => {
    setMode((prev) => {
      const next = prev === 'dark' ? 'light' : 'dark'
      localStorage.setItem('colorMode', next)
      // Update AG-Grid theme class on body
      document.body.classList.toggle('ag-light', next === 'light')
      return next
    })
  }

  return (
    <ThemeModeContext.Provider value={{ mode, toggle }}>
      <MuiThemeProvider theme={buildTheme(mode)}>
        {children}
      </MuiThemeProvider>
    </ThemeModeContext.Provider>
  )
}

export const pnlColor = (value: number) =>
  value > 0 ? GREEN : value < 0 ? RED : '#8b949e'

export const CHART_COLORS = [ACCENT, GREEN, AMBER, '#bc8cff', '#ff7b72', '#79c0ff']
