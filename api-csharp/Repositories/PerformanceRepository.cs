using Microsoft.EntityFrameworkCore;
using TradingApi.Data;
using TradingApi.DTOs;
using NpgsqlTypes;

namespace TradingApi.Repositories;

public class PerformanceRepository(TradingDbContext db) : IPerformanceRepository
{
    public async Task<IEnumerable<DailyPerformanceDto>> GetEquityCurvesAsync(string? strategyName)
    {
        var query = db.DailyPerformance
            .Include(d => d.Strategy)
            .AsQueryable();

        if (!string.IsNullOrEmpty(strategyName))
            query = query.Where(d => d.Strategy.Name == strategyName);

        return await query
            .OrderBy(d => d.Strategy.Name)
            .ThenBy(d => d.PerfDate)
            .Select(d => new DailyPerformanceDto(
                d.Strategy.Name,
                d.PerfDate.ToString("yyyy-MM-dd"),
                Math.Round(d.DailyPnl,      2),
                Math.Round(d.DailyReturn  * 100, 4),
                Math.Round(d.CumulativePnl, 2),
                Math.Round(d.Drawdown     * 100, 4),
                d.TradeCount,
                d.WinCount
            ))
            .ToListAsync();
    }

    public async Task<IEnumerable<AssetClassBreakdownDto>> GetAssetClassBreakdownAsync(
        string? strategyName,
        string? dateFrom,
        string? dateTo)
    {
        var sql = """
            SELECT
                t.trade_date::text                            AS "TradeDate",
                a.asset_class                                  AS "AssetClass",
                ROUND(SUM(t.pnl)::numeric, 2)                 AS "TotalPnl",
                COUNT(*)::int                                  AS "TradeCount",
                ROUND((AVG(t.return_pct) * 100)::numeric, 4)  AS "AvgReturnPct"
            FROM trades t
            JOIN assets     a ON a.id = t.asset_id
            JOIN strategies s ON s.id = t.strategy_id
            WHERE (@Strategy IS NULL OR s.name = @Strategy)
              AND (@DateFrom  IS NULL OR t.trade_date >= @DateFrom::date)
              AND (@DateTo    IS NULL OR t.trade_date <= @DateTo::date)
            GROUP BY t.trade_date, a.asset_class
            ORDER BY t.trade_date, a.asset_class
            """;

        return await db.Database
            .SqlQueryRaw<AssetClassBreakdownDto>(
                sql,
                new Npgsql.NpgsqlParameter("Strategy", NpgsqlDbType.Text)
                    { Value = (object?)strategyName ?? DBNull.Value },
                new Npgsql.NpgsqlParameter("DateFrom", NpgsqlDbType.Text)
                    { Value = (object?)dateFrom ?? DBNull.Value },
                new Npgsql.NpgsqlParameter("DateTo",   NpgsqlDbType.Text)
                    { Value = (object?)dateTo   ?? DBNull.Value }
            )
            .ToListAsync();
    }

    public async Task<IEnumerable<MonthlyPnlDto>> GetMonthlyPnlAsync(string? strategyName)
    {
        var sql = """
            SELECT
                s.name                                               AS "Strategy",
                DATE_TRUNC('month', dp.perf_date)::text              AS "Month",
                ROUND(SUM(dp.daily_pnl)::numeric, 2)                 AS "MonthlyPnl",
                SUM(dp.trade_count)::int                             AS "TotalTrades",
                ROUND((AVG(dp.daily_return) * 100 * 21)::numeric, 4) AS "ApproxMonthlyReturnPct"
            FROM daily_performance dp
            JOIN strategies s ON s.id = dp.strategy_id
            WHERE (@Strategy IS NULL OR s.name = @Strategy)
            GROUP BY s.name, DATE_TRUNC('month', dp.perf_date)
            ORDER BY s.name, "Month"
            """;

        return await db.Database
            .SqlQueryRaw<MonthlyPnlDto>(
                sql,
                new Npgsql.NpgsqlParameter("Strategy", NpgsqlDbType.Text)
                    { Value = (object?)strategyName ?? DBNull.Value }
            )
            .ToListAsync();
    }
}