using TradingApi.DTOs;

namespace TradingApi.Repositories;

public interface IStrategyRepository
{
    Task<IEnumerable<StrategyDto>>                  GetAllAsync();
    Task<StrategyMetricsDto?>                       GetMetricsAsync(int strategyId);
    Task<IEnumerable<StrategyMetricsDto>>           GetAllMetricsAsync();
    Task<IEnumerable<StrategyMetricsWithEquityDto>> GetAllMetricsWithEquityAsync();
}

public interface ITradeRepository
{
    Task<PagedResult<TradeDto>> GetTradesAsync(TradeFilterParams filters);
    Task<IEnumerable<string>>   GetSymbolsAsync();
    Task<DateRangeDto>          GetDateRangeAsync();
}

public interface IPerformanceRepository
{
    Task<IEnumerable<DailyPerformanceDto>>    GetEquityCurvesAsync(string? strategyName);
    Task<IEnumerable<AssetClassBreakdownDto>> GetAssetClassBreakdownAsync(string? strategyName, string? dateFrom, string? dateTo);
    Task<IEnumerable<MonthlyPnlDto>>          GetMonthlyPnlAsync(string? strategyName);
}
