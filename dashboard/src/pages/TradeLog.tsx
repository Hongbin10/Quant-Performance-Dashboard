import {
  Box, Card, CardContent, MenuItem,
  Select, Stack, TextField, Typography,
} from '@mui/material'
import { useEffect, useState } from 'react'
import SectionHeader from '../components/common/SectionHeader'
import TradeLogGrid from '../components/tables/TradeLogGrid'
import { useTrades } from '../hooks/useTrades'
import { useStrategyMetrics } from '../hooks/useStrategies'
import { tradesApi, type DateRange } from '../api/trades'
import type { TradeFilterParams } from '../types'

const ASSET_CLASSES = ['equity', 'futures', 'forex']
const DIRECTIONS    = ['Long', 'Short']
const PAGE_SIZE     = 50   // rows per page — matches AG-Grid paginationPageSize

export default function TradeLog() {
  const { data: metrics } = useStrategyMetrics()
  const [filters, setFilters] = useState<TradeFilterParams>({ page: 1, pageSize: 10000 })
  const [dateRange, setDateRange] = useState<DateRange | null>(null)
  const { data, loading } = useTrades(filters)
  const today = new Date().toISOString().split('T')[0]

  useEffect(() => {
    tradesApi.getDateRange().then(setDateRange).catch(() => {})
  }, [])

  // Field setter — resets to page 1 whenever a filter changes
  const set = (key: keyof TradeFilterParams) => (e: any) =>
    setFilters((f) => ({ ...f, [key]: e.target.value || undefined, page: 1 }))


  return (
    <Box>
      <Typography variant="h3" sx={{ mb: 3 }}>Trade Log</Typography>

      {/* Filter bar */}
      <Card sx={{ mb: 2 }}>
        <CardContent sx={{ py: '12px !important' }}>
          <Stack direction="row" spacing={1.5} flexWrap="wrap" gap={1}>
            <Select size="small" displayEmpty value={filters.strategy ?? ''} onChange={set('strategy')} sx={{ minWidth: 160 }}>
              <MenuItem value="">All strategies</MenuItem>
              {metrics.map((m) => <MenuItem key={m.strategyId} value={m.strategyName}>{m.strategyName}</MenuItem>)}
            </Select>

            <Select size="small" displayEmpty value={filters.assetClass ?? ''} onChange={set('assetClass')} sx={{ minWidth: 120 }}>
              <MenuItem value="">All classes</MenuItem>
              {ASSET_CLASSES.map((c) => <MenuItem key={c} value={c}>{c}</MenuItem>)}
            </Select>

            <Select size="small" displayEmpty value={filters.direction ?? ''} onChange={set('direction')} sx={{ minWidth: 100 }}>
              <MenuItem value="">All dirs</MenuItem>
              {DIRECTIONS.map((d) => <MenuItem key={d} value={d}>{d}</MenuItem>)}
            </Select>

            <TextField
              size="small" type="date" label="From"
              InputLabelProps={{ shrink: true }}
              value={filters.dateFrom ?? dateRange?.minDate ?? ''}
              onChange={set('dateFrom')}
            />
            <TextField
              size="small" type="date" label="To"
              InputLabelProps={{ shrink: true }}
              value={filters.dateTo ?? today}
              onChange={set('dateTo')}
            />
          </Stack>
        </CardContent>
      </Card>

      {/* Table */}
      <Card>
        <CardContent sx={{ p: '0 !important' }}>
          <SectionHeader title="" />
          <TradeLogGrid
            data={data?.items ?? []}
            loading={loading}
          />
        </CardContent>
      </Card>
    </Box>
  )
}
