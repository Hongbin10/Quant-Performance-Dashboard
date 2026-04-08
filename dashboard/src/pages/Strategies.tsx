import { Box, Card, CardContent, Typography } from '@mui/material'
import SectionHeader from '../components/common/SectionHeader'
import StrategyMetricsGrid from '../components/tables/StrategyMetricsGrid'
import { useStrategyMetrics } from '../hooks/useStrategies'

export default function Strategies() {
  const { data: metrics, loading } = useStrategyMetrics()

  return (
    <Box>
      <Box sx={{ mb: 3 }}>
        <Typography variant="h3">Strategies</Typography>
        <Typography variant="body2" sx={{ mt: 0.5, color: 'text.secondary' }}>
          {metrics.length} strategies · click "View Detail" to drill down
        </Typography>
      </Box>

      <Card>
        <CardContent>
          <SectionHeader title="Strategy metrics" />
          <StrategyMetricsGrid data={metrics} loading={loading} />
        </CardContent>
      </Card>
    </Box>
  )
}
