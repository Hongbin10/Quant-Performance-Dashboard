import { Box } from '@mui/material'
import type { ReactNode } from 'react'
import Sidebar from './Sidebar'
import TopBar from './TopBar'

export default function AppShell({ children }: { children: ReactNode }) {
  return (
    <Box sx={{ display: 'flex' }}>
      <Sidebar />
      <TopBar />
      <Box
        component="main"
        sx={{
          flexGrow: 1,
          ml: '225px',
          mt: '52px', 
          p: 3,
          minHeight: 'calc(100vh - 52px)',
          bgcolor: 'background.default',
        }}
      >
        {children}
      </Box>
    </Box>
  )
}
