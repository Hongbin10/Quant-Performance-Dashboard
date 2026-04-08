import {
  Box, Card, CardContent, CircularProgress,
  Grid, IconButton, Typography,
} from '@mui/material'
import ArrowBackIcon from '@mui/icons-material/ArrowBack'
import { useEffect, useState } from 'react'
import { useNavigate, useParams } from 'react-router-dom'
import MetricCard from '../components/common/MetricCard'
import SectionHeader from '../components/common/SectionHeader'
import EquityCurveChart from '../components/charts/EquityCurveChart'
import DrawdownChart from '../components/charts/DrawdownChart'
import MonthlyHeatmapChart from '../components/charts/MonthlyHeatmapChart'
import AssetBreakdownChart from '../components/charts/AssetBreakdownChart'
import TradeLogGrid from '../components/tables/TradeLogGrid'
import { useEquityCurves } from '../hooks/usePerformance'
import { useThemeMode } from '../context/ThemeContext'
import { strategiesApi } from '../api/strategies'
import { performanceApi } from '../api/performance'
import { tradesApi } from '../api/trades'
import type { StrategyMetrics, MonthlyPnl, AssetClassBreakdown, Trade } from '../types'

function Loader() {
  return (
    <Box sx={{ display: 'flex', justifyContent: 'center', py: 6 }}>
      <CircularProgress size={28} />
    </Box>
  )
}

export default function StrategyDetail() {
  const { id }     = useParams<{ id: string }>()
  const navigate   = useNavigate()
  const { mode }   = useThemeMode()
  const isDark     = mode === 'dark'
  const strategyId = Number(id)

  const [metrics,   setMetrics]   = useState<StrategyMetrics | null>(null)
  const [monthly,   setMonthly]   = useState<MonthlyPnl[]>([])
  const [breakdown, setBreakdown] = useState<AssetClassBreakdown[]>([])
  const [trades,    setTrades]    = useState<Trade[]>([])
  const [loading,   setLoading]   = useState(true)

  useEffect(() => {
    if (!strategyId) return
    strategiesApi.getMetrics(strategyId).then(setMetrics)
  }, [strategyId])

  const strategyName = metrics?.strategyName
  const { data: curves, loading: curvesLoading } = useEquityCurves(strategyName)

  useEffect(() => {
    if (!strategyName) return
    setLoading(true)
    Promise.all([
      performanceApi.getMonthly(strategyName),
      performanceApi.getAssetBreakdown(strategyName),
      tradesApi.getTrades({ strategy: strategyName, pageSize: 10000 }),
    ]).then(([mon, bkd, tr]) => {
      setMonthly(mon)
      setBreakdown(bkd)
      setTrades(tr.items)
    }).finally(() => setLoading(false))
  }, [strategyName])

  if (!metrics) {
    return (
      <Box sx={{ display: 'flex', justifyContent: 'center', py: 10 }}>
        <CircularProgress size={32} />
      </Box>
    )
  }

  const kpis = [
    { label: 'Total PnL',    value: `$${metrics.totalPnl.toLocaleString('en-US', { minimumFractionDigits: 0 })}`, isPnl: true },
    { label: 'Sharpe',       value: metrics.sharpeRatio.toFixed(4)    },
    { label: 'Sortino',      value: metrics.sortinoRatio.toFixed(4)   },
    { label: 'Max DD',       value: `${metrics.maxDrawdownPct.toFixed(2)}%` },
    { label: 'Win Rate',     value: `${metrics.winRatePct.toFixed(2)}%`     },
    { label: 'Total Trades', value: metrics.totalTrades.toLocaleString() },
  ]

  return (
    <Box>
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5, mb: 3 }}>
        <IconButton size="small" onClick={() => navigate('/strategies')} sx={{ mr: 0.5 }}>
          <ArrowBackIcon fontSize="small" />
        </IconButton>
        <Box>
          <Typography variant="h3">{metrics.strategyName}</Typography>
          <Typography variant="body2" sx={{ color: 'text.secondary', mt: 0.25 }}>
            Strategy detail · {metrics.totalTrades.toLocaleString()} trades · 2020–2024
          </Typography>
        </Box>
      </Box>

      <Grid container spacing={2} sx={{ mb: 3 }}>
        {kpis.map(({ label, value, isPnl }) => (
          <Grid item xs={6} sm={4} md={2} key={label}>
            <MetricCard label={label} value={value} isPnl={isPnl} />
          </Grid>
        ))}
      </Grid>

      <Grid container spacing={2} sx={{ mb: 2 }}>
        <Grid item xs={12} md={7}>
          <Card sx={{ height: '100%' }}>
            <CardContent>
              <SectionHeader title="Cumulative PnL" />
              {curvesLoading ? <Loader /> : <EquityCurveChart data={curves} height={280} />}
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} md={5}>
          <Card sx={{ height: '100%' }}>
            <CardContent>
              <SectionHeader title="Drawdown" />
              {curvesLoading ? <Loader /> : <DrawdownChart data={curves} height={280} />}
            </CardContent>
          </Card>
        </Grid>
      </Grid>

      <Grid container spacing={2} sx={{ mb: 2 }}>
        <Grid item xs={12} md={7}>
          <Card>
            <CardContent>
              <SectionHeader title="Monthly PnL" />
              {loading ? <Loader /> : (
                <MonthlyHeatmapChart data={monthly} height={300} isDark={isDark} />
              )}
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} md={5}>
          <Card>
            <CardContent>
              <SectionHeader title="PnL by asset class" />
              {loading ? <Loader /> : (
                <AssetBreakdownChart data={breakdown} height={240} isDark={isDark} />
              )}
            </CardContent>
          </Card>
        </Grid>
      </Grid>

      <Card>
        <CardContent>
          <SectionHeader title={`Trade log · ${metrics.strategyName}`} />
          <TradeLogGrid data={trades} loading={loading} />
        </CardContent>
      </Card>
    </Box>
  )
}
