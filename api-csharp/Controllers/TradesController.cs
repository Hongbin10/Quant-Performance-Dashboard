using Microsoft.AspNetCore.Mvc;
using TradingApi.DTOs;
using TradingApi.Repositories;

namespace TradingApi.Controllers;

[ApiController]
[Route("api/[controller]")]
public class TradesController(ITradeRepository repo) : ControllerBase
{
    /// <summary>
    /// Paginated, filterable trade log — consumed by the AG-Grid trade detail table.
    /// </summary>
    [HttpGet]
    public async Task<IActionResult> GetTrades([FromQuery] TradeFilterParams filters)
    {
        if (filters.PageSize > 50000)
            return BadRequest("pageSize cannot exceed 500.");

        var result = await repo.GetTradesAsync(filters);
        return Ok(result);
    }

    /// <summary>Returns all asset symbols — used to populate filter dropdowns.</summary>
    [HttpGet("symbols")]
    public async Task<IActionResult> GetSymbols() =>
        Ok(await repo.GetSymbolsAsync());

    /// <summary>Returns the min and max trade dates in the database.</summary>
    [HttpGet("date-range")]
    public async Task<IActionResult> GetDateRange() =>
        Ok(await repo.GetDateRangeAsync());
}
