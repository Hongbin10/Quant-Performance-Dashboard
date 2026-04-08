import {
  Box, Drawer, List, ListItemButton,
  ListItemIcon, ListItemText, Typography,
} from '@mui/material'
import DashboardIcon   from '@mui/icons-material/Dashboard'
import BarChartIcon    from '@mui/icons-material/BarChart'
import TableChartIcon  from '@mui/icons-material/TableChart'
import ShieldIcon      from '@mui/icons-material/Shield'
import PieChartIcon    from '@mui/icons-material/PieChart'
import { useLocation, useNavigate } from 'react-router-dom'

const NAV = [
  { label: 'Overview',     path: '/',            icon: <DashboardIcon  fontSize="small" /> },
  { label: 'Strategies',   path: '/strategies',  icon: <BarChartIcon   fontSize="small" /> },
  { label: 'Trade Log',    path: '/trades',      icon: <TableChartIcon fontSize="small" /> },
  { label: 'Risk',         path: '/risk',        icon: <ShieldIcon     fontSize="small" />, soon: true },
  { label: 'Attribution',  path: '/attribution', icon: <PieChartIcon   fontSize="small" />, soon: true },
]

export default function Sidebar() {
  const navigate    = useNavigate()
  const { pathname } = useLocation()

  // Detail pages are children of /strategies — keep Strategies active
  const isActive = (path: string) =>
    path === '/'
      ? pathname === '/'
      : pathname === path || pathname.startsWith(path + '/')

  return (
    <Drawer variant="permanent">
      {/* Logo */}
      <Box sx={{ px: 2, py: 2, borderBottom: '1px solid', borderColor: 'divider' }}>
        <Typography sx={{
          fontFamily: '"IBM Plex Mono", monospace',
          fontWeight: 800,
          fontSize: '0.9rem',
          color: 'primary.main',
          whiteSpace: 'nowrap',
          overflow: 'hidden',
          textOverflow: 'ellipsis',
        }}>
          Quant Performance
        </Typography>
        <Typography variant="body2" sx={{ mt: 0.45, fontSize: '0.72rem', color: 'text.secondary' }}>
          For Quant Researchers
        </Typography>
      </Box>

      {/* Nav items */}
      <List sx={{ px: 1, pt: 1.5 }} disablePadding>
        {NAV.map(({ label, path, icon, soon }) => {
          const active = isActive(path)
          return (
            <ListItemButton
              key={path}
              onClick={() => !soon && navigate(path)}
              sx={{
                borderRadius: 1.5,
                mb: 0.5,
                px: 1.5,
                py: 0.75,
                color: active ? 'primary.main' : soon ? 'text.disabled' : 'text.secondary',
                bgcolor: active ? 'rgba(88,166,255,0.08)' : 'transparent',
                cursor: soon ? 'default' : 'pointer',
                '&:hover': {
                  bgcolor: soon ? 'transparent' : 'rgba(255,255,255,0.04)',
                  color: soon ? 'text.disabled' : 'text.primary',
                },
              }}
            >
              <ListItemIcon sx={{ minWidth: 32, color: 'inherit' }}>{icon}</ListItemIcon>
              <ListItemText
                primary={label}
                primaryTypographyProps={{ fontSize: '0.875rem', fontWeight: active ? 600 : 400 }}
              />
              {soon && (
                <Typography sx={{
                  fontSize: '9px', color: 'text.disabled',
                  border: '0.5px solid', borderColor: 'divider',
                  borderRadius: 1, px: 0.6, py: 0.2, ml: 0.5,
                }}>
                  soon
                </Typography>
              )}
            </ListItemButton>
          )
        })}
      </List>
    </Drawer>
  )
}