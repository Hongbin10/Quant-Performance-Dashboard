import client from './client'
import type { Strategy, StrategyMetrics } from '../types'

export const strategiesApi = {
  getAll: () =>
    client.get<Strategy[]>('/api/strategies').then((r) => r.data),

  getAllMetrics: () =>
    client.get<StrategyMetrics[]>('/api/strategies/metrics-with-equity').then((r) => r.data),

  getMetrics: (id: number) =>
    client.get<StrategyMetrics>(`/api/strategies/${id}/metrics`).then((r) => r.data),
}
