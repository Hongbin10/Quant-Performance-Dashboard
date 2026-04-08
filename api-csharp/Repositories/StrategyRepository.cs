using Microsoft.EntityFrameworkCore;
using TradingApi.Data;
using TradingApi.DTOs;

namespace TradingApi.Repositories;

public class StrategyRepository(TradingDbContext db) : IStrategyRepository
{
    public async Task<IEnumerable<StrategyDto>> GetAllAsync()
    {
        return await db.Strategies
            .OrderBy(s => s.Name)
            .Select(s => new StrategyDto(s.Id, s.Name, s.Description ?? ""))
            .ToListAsync();
    }

    public async Task<StrategyMetricsDto?> GetMetricsAsync(int strategyId)
    {
        return await db.StrategyMetrics
            .Where(m => m.StrategyId == strategyId)
            .Select(m => new StrategyMetricsDto(
                m.StrategyId,
                m.Strategy.Name,
                m.TotalPnl,
                Math.Round(m.SharpeRatio,  4),
                Math.Round(m.SortinoRatio, 4),
                Math.Round(m.MaxDrawdown * 100, 2),
                Math.Round(m.WinRate     * 100, 2),
                m.AvgWin,
                m.AvgLoss,
                m.ProfitFactor,
                m.TotalTrades,
                m.ComputedAt
            ))
            .FirstOrDefaultAsync();
    }

    public async Task<IEnumerable<StrategyMetricsDto>> GetAllMetricsAsync()
    {
        return await db.StrategyMetrics
            .Include(m => m.Strategy)
            .OrderByDescending(m => m.SharpeRatio)
            .Select(m => new StrategyMetricsDto(
                m.StrategyId,
                m.Strategy.Name,
                m.TotalPnl,
                Math.Round(m.SharpeRatio,  4),
                Math.Round(m.SortinoRatio, 4),
                Math.Round(m.MaxDrawdown * 100, 2),
                Math.Round(m.WinRate     * 100, 2),
                m.AvgWin,
                m.AvgLoss,
                m.ProfitFactor,
                m.TotalTrades,
                m.ComputedAt
            ))
            .ToListAsync();
    }

    public async Task<IEnumerable<StrategyMetricsWithEquityDto>> GetAllMetricsWithEquityAsync()
    {
        // 1. Load all metrics
        var metrics = await db.StrategyMetrics
            .Include(m => m.Strategy)
            .OrderByDescending(m => m.SharpeRatio)
            .ToListAsync();

        // 2. Load daily cumulative PnL per strategy
        var dailyGroups = await db.DailyPerformance
            .GroupBy(d => d.StrategyId)
            .Select(g => new {
                StrategyId    = g.Key,
                EquityHistory = g.OrderBy(d => d.PerfDate)
                                 .Select(d => Math.Round(d.CumulativePnl, 2))
                                 .ToArray()
            })
            .ToListAsync();

        var equityMap = dailyGroups.ToDictionary(x => x.StrategyId, x => x.EquityHistory);

        // 3. Combine metrics + equity history
        return metrics.Select(m => new StrategyMetricsWithEquityDto(
            m.StrategyId,
            m.Strategy.Name,
            Math.Round(m.TotalPnl,     2),
            Math.Round(m.SharpeRatio,  4),
            Math.Round(m.SortinoRatio, 4),
            Math.Round(m.MaxDrawdown * 100, 2),
            Math.Round(m.WinRate     * 100, 2),
            Math.Round(m.AvgWin,       2),
            Math.Round(m.AvgLoss,      2),
            Math.Round(m.ProfitFactor, 4),
            m.TotalTrades,
            m.ComputedAt.ToString("o"),
            equityMap.TryGetValue(m.StrategyId, out var hist) ? hist : Array.Empty<double>()
        ));
    }
}