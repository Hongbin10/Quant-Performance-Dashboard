namespace TradingApi.DTOs;

public record StrategyDto(int Id, string Name, string Description);

public record StrategyMetricsDto(
    int      StrategyId,
    string   StrategyName,
    double   TotalPnl,
    double   SharpeRatio,
    double   SortinoRatio,
    double   MaxDrawdownPct,
    double   WinRatePct,
    double   AvgWin,
    double   AvgLoss,
    double   ProfitFactor,
    int      TotalTrades,
    DateTime ComputedAt
);

public record TradeDto(
    int    Id,
    string Strategy,
    string Symbol,
    string AssetClass,
    string TradeDate,
    string Direction,
    double EntryPrice,
    double ExitPrice,
    int    Quantity,
    double Pnl,
    double ReturnPct
);

public record TradeFilterParams(
    string? Strategy   = null,
    string? AssetClass = null,
    string? Symbol     = null,
    string? DateFrom   = null,
    string? DateTo     = null,
    string? Direction  = null,
    int     Page       = 1,
    int     PageSize   = 100
);

public record PagedResult<T>(
    IEnumerable<T> Items,
    int            TotalCount,
    int            Page,
    int            PageSize,
    int            TotalPages
);

public record DailyPerformanceDto(
    string Strategy,
    string PerfDate,
    double DailyPnl,
    double DailyReturnPct,
    double CumulativePnl,
    double DrawdownPct,
    int    TradeCount,
    int    WinCount
);

public record AssetClassBreakdownDto(
    string TradeDate,
    string AssetClass,
    double TotalPnl,
    int    TradeCount,
    double AvgReturnPct
);

public record MonthlyPnlDto(
    string Strategy,
    string Month,
    double MonthlyPnl,
    int    TotalTrades,
    double ApproxMonthlyReturnPct
);

// New
public record DateRangeDto(string MinDate, string MaxDate);

public record StrategyMetricsWithEquityDto(
    int      StrategyId,
    string   StrategyName,
    double   TotalPnl,
    double   SharpeRatio,
    double   SortinoRatio,
    double   MaxDrawdownPct,
    double   WinRatePct,
    double   AvgWin,
    double   AvgLoss,
    double   ProfitFactor,
    int      TotalTrades,
    string   ComputedAt,
    double[] EquityHistory
);
