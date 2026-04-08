using System.Net.WebSockets;
using System.Text;
using System.Text.Json;

namespace TradingApi.Services;

public class FinnhubWebSocketService : BackgroundService
{
    private readonly ILogger<FinnhubWebSocketService> _logger;
    private readonly IConfiguration _config;
    private readonly MarketDataBroadcaster _broadcaster;

    // Mirrors LivePriceTicker.tsx — two groups matching QRT's trading universe
    private static readonly string[] Symbols =
    [
    // Market Overview — 24h real-time via Finnhub free tier
        "BINANCE:BTCUSDT",  // Risk sentiment proxy (correlated with Nasdaq)
        "BINANCE:ETHUSDT",  // Risk sentiment proxy
        "OANDA:USD_JPY",    // Safe-haven proxy (JPY strengthens when VIX rises)
        "OANDA:BCO_USD",    // Brent Crude — QRT commodity focus
        "OANDA:XAU_USD",    // Gold — macro hedge
        "OANDA:EUR_USD",    // EUR/USD

    // Key Equities
        "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL",
    ];

    public FinnhubWebSocketService(
        ILogger<FinnhubWebSocketService> logger,
        IConfiguration config,
        MarketDataBroadcaster broadcaster)
    {
        _logger      = logger;
        _config      = config;
        _broadcaster = broadcaster;
    }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try   { await ConnectAndListenAsync(ct); }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)
            {
                _logger.LogWarning("Finnhub disconnected: {msg}. Retrying in 5s…", ex.Message);
                await Task.Delay(5_000, ct);
            }
        }
    }

    private async Task ConnectAndListenAsync(CancellationToken ct)
    {
        var apiKey = _config["Finnhub:ApiKey"]
            ?? throw new InvalidOperationException("Finnhub:ApiKey not set in appsettings.json");

        using var ws = new ClientWebSocket();
        var uri      = new Uri($"wss://ws.finnhub.io?token={apiKey}");

        _logger.LogInformation("Connecting to Finnhub…");
        await ws.ConnectAsync(uri, ct);
        _logger.LogInformation("Connected. Subscribing to {n} symbols.", Symbols.Length);

        foreach (var symbol in Symbols)
        {
            var msg = JsonSerializer.Serialize(new { type = "subscribe", symbol });
            await ws.SendAsync(Encoding.UTF8.GetBytes(msg), WebSocketMessageType.Text, true, ct);
        }

        var buffer = new byte[8192];
        while (ws.State == WebSocketState.Open && !ct.IsCancellationRequested)
        {
            var result = await ws.ReceiveAsync(buffer, ct);
            if (result.MessageType == WebSocketMessageType.Close) break;
            ProcessMessage(Encoding.UTF8.GetString(buffer, 0, result.Count));
        }
    }

    private void ProcessMessage(string json)
    {
        try
        {
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;
            if (!root.TryGetProperty("type", out var t) || t.GetString() != "trade") return;
            if (!root.TryGetProperty("data", out var data)) return;

            foreach (var trade in data.EnumerateArray())
            {
                var symbol = trade.GetProperty("s").GetString() ?? "";
                var price  = trade.GetProperty("p").GetDouble();
                var ts     = trade.GetProperty("t").GetInt64();
                _broadcaster.Publish(new PriceUpdate(symbol, price, ts));
            }
        }
        catch (Exception ex) { _logger.LogDebug("Parse error: {e}", ex.Message); }
    }
}

public record PriceUpdate(string Symbol, double Price, long TimestampMs);
