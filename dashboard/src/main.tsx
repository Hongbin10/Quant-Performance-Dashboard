import '@fontsource/ibm-plex-sans/400.css'
import '@fontsource/ibm-plex-sans/600.css'
import '@fontsource/ibm-plex-mono/400.css'
import '@fontsource/ibm-plex-mono/600.css'

import 'ag-grid-community/styles/ag-grid.css'
import 'ag-grid-community/styles/ag-theme-balham.css'

import React from 'react'
import ReactDOM from 'react-dom/client'
import { CssBaseline } from '@mui/material'
import { ThemeModeProvider } from './context/ThemeContext'
import { BackendProvider } from './context/BackendContext'
import App from './App'

import { setBackend, type BackendKey } from './api/client'
const saved = localStorage.getItem('backend') as BackendKey | null
if (saved) setBackend(saved)

const agStyle = document.createElement('style')
agStyle.textContent = `
  .ag-theme-balham-dark {
    --ag-background-color:         #161b22;
    --ag-odd-row-background-color: #161b22;
    --ag-even-row-background-color: #1c2128;
    --ag-header-background-color:  #0d1117;
    --ag-border-color:                  #21262d;
    --ag-row-border-color:              #21262d;
    --ag-header-column-separator-color: #21262d;
    --ag-row-hover-color:               rgba(88,166,255,0.05);
    --ag-selected-row-background-color: rgba(88,166,255,0.08);
    --ag-font-family:                   "IBM Plex Mono", monospace;
    --ag-font-size:                     13px;
    --ag-foreground-color:              #e6edf3;
    --ag-secondary-foreground-color:    #8b949e;
    --ag-header-foreground-color:       #8b949e;
    --ag-disabled-foreground-color:     #484f58;
    --ag-alpine-active-color:           #58a6ff;
    --ag-range-selection-border-color:  #58a6ff;
    --ag-input-focus-border-color:      #58a6ff;
  }
  body.ag-light .ag-theme-balham-dark {
    --ag-background-color:              #ffffff;
    --ag-odd-row-background-color:      #f6f8fa;
    --ag-even-row-background-color:     #ffffff;
    --ag-header-background-color:       #f6f8fa;
    --ag-border-color:                  #d0d7de;
    --ag-row-border-color:              #d0d7de;
    --ag-header-column-separator-color: #d0d7de;
    --ag-row-hover-color:               rgba(88,166,255,0.06);
    --ag-selected-row-background-color: rgba(88,166,255,0.1);
    --ag-foreground-color:              #1f2328;
    --ag-secondary-foreground-color:    #656d76;
    --ag-header-foreground-color:       #656d76;
    --ag-disabled-foreground-color:     #9ba1aa;
  }
  .ag-theme-balham-dark .ag-header-cell-text {
    text-transform: uppercase;
    letter-spacing: 0.05em;
    font-size: 11px;
    font-weight: 600;
    font-family: "IBM Plex Sans", sans-serif;
  }
  .ag-theme-balham-dark .ag-paging-panel {
    border-top: 1px solid var(--ag-border-color);
    font-size: 12px;
    font-family: "IBM Plex Mono", monospace;
  }
  .ag-theme-balham-dark .ag-root-wrapper {
    border: 1px solid var(--ag-border-color);
    border-radius: 4px;
  }
`
document.head.appendChild(agStyle)

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <ThemeModeProvider>
      <CssBaseline />
      <BackendProvider>
        <App />
      </BackendProvider>
    </ThemeModeProvider>
  </React.StrictMode>
)
