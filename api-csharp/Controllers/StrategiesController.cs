using Microsoft.AspNetCore.Mvc;
using TradingApi.Repositories;

namespace TradingApi.Controllers;

[ApiController]
[Route("api/[controller]")]
public class StrategiesController(IStrategyRepository repo) : ControllerBase
{
    /// <summary>Returns all strategy names and IDs.</summary>
    [HttpGet]
    public async Task<IActionResult> GetAll() =>
        Ok(await repo.GetAllAsync());

    /// <summary>Returns summary metrics for all strategies — main AG-Grid table data.</summary>
    [HttpGet("metrics")]
    public async Task<IActionResult> GetAllMetrics() =>
        Ok(await repo.GetAllMetricsAsync());

    /// <summary>Returns metrics + equity history array for sparkline rendering.</summary>
    [HttpGet("metrics-with-equity")]
    public async Task<IActionResult> GetAllMetricsWithEquity() =>
        Ok(await repo.GetAllMetricsWithEquityAsync());

    /// <summary>Returns metrics for a single strategy by ID.</summary>
    [HttpGet("{id:int}/metrics")]
    public async Task<IActionResult> GetMetrics(int id)
    {
        var result = await repo.GetMetricsAsync(id);
        return result is null ? NotFound($"Strategy {id} not found.") : Ok(result);
    }
}
