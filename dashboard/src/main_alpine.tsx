import '@fontsource/ibm-plex-sans/400.css'
import '@fontsource/ibm-plex-sans/600.css'
import '@fontsource/ibm-plex-mono/400.css'
import '@fontsource/ibm-plex-mono/600.css'

import React from 'react'
import ReactDOM from 'react-dom/client'
import { ThemeProvider, CssBaseline } from '@mui/material'
import { theme } from './theme'
import { BackendProvider } from './context/BackendContext'
import App from './App'

// Restore last-used backend before first render
import { setBackend, type BackendKey } from './api/client'
const saved = localStorage.getItem('backend') as BackendKey | null
if (saved) setBackend(saved)

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <BackendProvider>
        <App />
      </BackendProvider>
    </ThemeProvider>
  </React.StrictMode>
)
