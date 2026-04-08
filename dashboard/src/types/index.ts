// Mirrors C# DTOs exactly — one source of truth for all API shapes

export interface Strategy {
  id: number
  name: string
  description: string
}

export interface StrategyMetrics {
  strategyId:     number
  strategyName:   string
  totalPnl:       number
  sharpeRatio:    number
  sortinoRatio:   number
  maxDrawdownPct: number
  winRatePct:     number
  avgWin:         number
  avgLoss:        number
  profitFactor:   number
  totalTrades:    number
  computedAt:     string
  equityHistory:  number[]   // daily cumulative PnL array for sparkline
}

export interface Trade {
  id:         number
  strategy:   string
  symbol:     string
  assetClass: string
  tradeDate:  string
  direction:  'Long' | 'Short'
  entryPrice: number
  exitPrice:  number
  quantity:   number
  pnl:        number
  returnPct:  number
}

export interface TradeFilterParams {
  strategy?:   string
  assetClass?: string
  symbol?:     string
  dateFrom?:   string
  dateTo?:     string
  direction?:  string
  page?:       number
  pageSize?:   number
}

export interface PagedResult<T> {
  items:      T[]
  totalCount: number
  page:       number
  pageSize:   number
  totalPages: number
}

export interface DailyPerformance {
  strategy:       string
  perfDate:       string
  dailyPnl:       number
  dailyReturnPct: number
  cumulativePnl:  number
  drawdownPct:    number
  tradeCount:     number
  winCount:       number
}

export interface AssetClassBreakdown {
  tradeDate:    string
  assetClass:   string
  totalPnl:     number
  tradeCount:   number
  avgReturnPct: number
}

export interface MonthlyPnl {
  strategy:               string
  month:                  string
  monthlyPnl:             number
  totalTrades:            number
  approxMonthlyReturnPct: number
}
