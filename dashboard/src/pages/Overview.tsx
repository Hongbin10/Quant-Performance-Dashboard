import { Box, Grid, Typography, CircularProgress, Card, CardContent } from '@mui/material'
import MetricCard from '../components/common/MetricCard'
import SectionHeader from '../components/common/SectionHeader'
import StrategyMetricsGrid from '../components/tables/StrategyMetricsGrid'
import EquityCurveChart from '../components/charts/EquityCurveChart'
import { useStrategyMetrics } from '../hooks/useStrategies'
import { useEquityCurves } from '../hooks/usePerformance'
import LivePriceTicker from '../components/charts/LivePriceTicker'


export default function Overview() {
  const { data: metrics, loading: mLoading } = useStrategyMetrics()
  const { data: curves,  loading: cLoading } = useEquityCurves()

  // Roll up top-level summary numbers
  const totalPnl    = metrics.reduce((s, m) => s + m.totalPnl, 0)
  const avgSharpe   = metrics.length ? metrics.reduce((s, m) => s + m.sharpeRatio, 0) / metrics.length : 0
  const bestSharpe  = metrics.length ? Math.max(...metrics.map((m) => m.sharpeRatio)) : 0
  const maxDD       = metrics.length ? Math.max(...metrics.map((m) => m.maxDrawdownPct)) : 0
  const totalTrades = metrics.reduce((s, m) => s + m.totalTrades, 0)

  return (
    <Box>
      <Typography variant="h3" sx={{ mb: 0.5 }}>Overview</Typography>
      <Typography variant="body2" sx={{ mb: 3 }}>
        All strategies · live data from Finnhub API
      </Typography>

      <LivePriceTicker />

      {/* KPI row */}
      <Grid container spacing={2} sx={{ mb: 3 }}>
        {[
          { label: 'Total PnL',      value: `$${totalPnl.toLocaleString('en-US', { minimumFractionDigits: 0 })}`, isPnl: true },
          { label: 'Avg Sharpe',     value: avgSharpe.toFixed(4) },
          { label: 'Best Sharpe',    value: bestSharpe.toFixed(4) },
          { label: 'Max Drawdown',   value: `${maxDD.toFixed(2)}%` },
          { label: 'Total Trades',   value: totalTrades.toLocaleString() },
          { label: 'Strategies',     value: metrics.length },
        ].map(({ label, value, isPnl }) => (
          <Grid item xs={6} sm={4} md={2} key={label}>
            <MetricCard label={label} value={value} loading={mLoading} />
          </Grid>
        ))}
      </Grid>

      {/* Equity curves */}
      <Card sx={{ mb: 3 }}>
        <CardContent>
          <SectionHeader title="Cumulative PnL — all strategies" />
          {cLoading ? (
            <Box sx={{ display: 'flex', justifyContent: 'center', py: 6 }}>
              <CircularProgress size={28} />
            </Box>
          ) : (
            <EquityCurveChart data={curves} loading={cLoading} />
          )}
        </CardContent>
      </Card>

      {/* Strategy metrics table */}
      <Card>
        <CardContent>
          <SectionHeader title="Strategy metrics" />
          <StrategyMetricsGrid data={metrics} loading={mLoading} />
        </CardContent>
      </Card>
    </Box>
  )
}
