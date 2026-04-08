import client from './client'
import type { AssetClassBreakdown, DailyPerformance, MonthlyPnl } from '../types'

export const performanceApi = {
  getEquityCurves: (strategy?: string) =>
    client
      .get<DailyPerformance[]>('/api/performance/equity-curve', {
        params: strategy ? { strategy } : {},
      })
      .then((r) => r.data),

  getAssetBreakdown: (strategy?: string, dateFrom?: string, dateTo?: string) =>
    client
      .get<AssetClassBreakdown[]>('/api/performance/asset-breakdown', {
        params: { strategy, dateFrom, dateTo },
      })
      .then((r) => r.data),

  getMonthly: (strategy?: string) =>
    client
      .get<MonthlyPnl[]>('/api/performance/monthly', {
        params: strategy ? { strategy } : {},
      })
      .then((r) => r.data),
}
