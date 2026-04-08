"""
Trading Data Pipeline
=====================
Generates simulated multi-asset trading data and writes to PostgreSQL.
Calculates: PnL, Sharpe Ratio, Max Drawdown, Win Rate, Volatility.

Asset classes: Equity, Futures, Forex
Strategies: 5 distinct strategies with different return profiles
Date range: configurable, default 2 years
"""

import os
import logging
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────
load_dotenv()

DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", 5432)),
    "dbname":   os.getenv("DB_NAME", "trading_db"),
    "user":     os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "password"),
}

START_DATE = datetime(2023, 1, 1)
END_DATE   = datetime(2024, 12, 31)
RANDOM_SEED = 42

# ── Universe ───────────────────────────────────────────────────────────────────
ASSETS = {
    "equity": [
        ("AAPL",   "Apple Inc.",            0.0008, 0.018),   # (symbol, name, daily_mu, daily_sigma)
        ("MSFT",   "Microsoft Corp.",        0.0009, 0.016),
        ("GOOGL",  "Alphabet Inc.",          0.0007, 0.019),
        ("NVDA",   "NVIDIA Corp.",           0.0015, 0.030),
        ("AMZN",   "Amazon.com Inc.",        0.0006, 0.021),
        ("META",   "Meta Platforms Inc.",    0.0010, 0.025),
    ],
    "futures": [
        ("ES",  "E-mini S&P 500",            0.0004, 0.012),
        ("NQ",  "E-mini Nasdaq-100",         0.0005, 0.016),
        ("CL",  "Crude Oil WTI",             0.0002, 0.025),
        ("GC",  "Gold Futures",              0.0003, 0.010),
        ("ZB",  "US 30Y T-Bond",            -0.0001, 0.008),
        ("6E",  "EUR/USD Futures",           0.0001, 0.007),
    ],
    "forex": [
        ("EURUSD", "Euro / US Dollar",       0.0001, 0.006),
        ("GBPUSD", "Pound / US Dollar",      0.0001, 0.007),
        ("USDJPY", "US Dollar / Yen",        0.0001, 0.006),
        ("AUDUSD", "Australian / US Dollar", 0.0000, 0.007),
        ("USDCAD", "US Dollar / CAD",        0.0000, 0.005),
        ("USDCHF", "US Dollar / CHF",       -0.0001, 0.005),
    ],
}

STRATEGIES = [
    # (name, description, sharpe_target, preferred_classes)
    ("MomentumAlpha",  "Cross-asset momentum with trend filters",    1.4, ["equity", "futures"]),
    ("MeanReversion",  "Statistical mean reversion on equities",     1.1, ["equity"]),
    ("StatArb",        "Pairs trading and statistical arbitrage",     1.6, ["equity", "forex"]),
    ("MacroTrend",     "Macro trend following across all classes",    0.9, ["futures", "forex"]),
    ("VolBreakout",    "Volatility breakout with ATR sizing",         1.2, ["futures", "equity"]),
]


# ══════════════════════════════════════════════════════════════════════════════
# Database helpers
# ══════════════════════════════════════════════════════════════════════════════

def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def create_schema(conn):
    """Drop and recreate all tables cleanly."""
    ddl = """
    DROP TABLE IF EXISTS strategy_metrics, daily_performance, trades, strategies, assets CASCADE;

    CREATE TABLE assets (
        id           SERIAL PRIMARY KEY,
        symbol       VARCHAR(16) NOT NULL UNIQUE,
        name         VARCHAR(128) NOT NULL,
        asset_class  VARCHAR(16) NOT NULL,   -- equity | futures | forex
        daily_mu     FLOAT NOT NULL,          -- expected daily return
        daily_sigma  FLOAT NOT NULL           -- daily volatility
    );

    CREATE TABLE strategies (
        id          SERIAL PRIMARY KEY,
        name        VARCHAR(64) NOT NULL UNIQUE,
        description TEXT
    );

    -- Individual trade records
    CREATE TABLE trades (
        id            SERIAL PRIMARY KEY,
        strategy_id   INT  NOT NULL REFERENCES strategies(id),
        asset_id      INT  NOT NULL REFERENCES assets(id),
        trade_date    DATE NOT NULL,
        direction     CHAR(1) NOT NULL CHECK (direction IN ('L','S')),  -- Long / Short
        entry_price   FLOAT NOT NULL,
        exit_price    FLOAT NOT NULL,
        quantity      INT   NOT NULL,
        pnl           FLOAT NOT NULL,          -- realised PnL in USD
        return_pct    FLOAT NOT NULL           -- trade return %
    );

    CREATE INDEX ix_trades_date     ON trades(trade_date);
    CREATE INDEX ix_trades_strategy ON trades(strategy_id, trade_date);

    -- Daily aggregated performance per strategy
    CREATE TABLE daily_performance (
        id              SERIAL PRIMARY KEY,
        strategy_id     INT  NOT NULL REFERENCES strategies(id),
        perf_date       DATE NOT NULL,
        daily_pnl       FLOAT NOT NULL,
        daily_return    FLOAT NOT NULL,        -- portfolio return %
        cumulative_pnl  FLOAT NOT NULL,
        drawdown        FLOAT NOT NULL,        -- current drawdown %
        trade_count     INT   NOT NULL,
        win_count       INT   NOT NULL,
        UNIQUE (strategy_id, perf_date)
    );

    CREATE INDEX ix_daily_perf_strategy ON daily_performance(strategy_id, perf_date);

    -- Summary metrics per strategy (recomputed each run)
    CREATE TABLE strategy_metrics (
        strategy_id   INT  PRIMARY KEY REFERENCES strategies(id),
        total_pnl     FLOAT NOT NULL,
        sharpe_ratio  FLOAT NOT NULL,
        sortino_ratio FLOAT NOT NULL,
        max_drawdown  FLOAT NOT NULL,          -- peak-to-trough %
        win_rate      FLOAT NOT NULL,          -- fraction of winning trades
        avg_win       FLOAT NOT NULL,          -- avg winning trade PnL
        avg_loss      FLOAT NOT NULL,          -- avg losing trade PnL
        profit_factor FLOAT NOT NULL,          -- gross profit / gross loss
        total_trades  INT   NOT NULL,
        computed_at   TIMESTAMPTZ DEFAULT NOW()
    );
    """
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()
    log.info("Schema created.")


# ══════════════════════════════════════════════════════════════════════════════
# Data generation
# ══════════════════════════════════════════════════════════════════════════════

def generate_price_series(mu: float, sigma: float, n_days: int, seed_offset: int = 0) -> np.ndarray:
    """Geometric Brownian Motion price series starting at 100."""
    rng = np.random.default_rng(RANDOM_SEED + seed_offset)
    daily_returns = rng.normal(mu, sigma, n_days)
    prices = 100.0 * np.exp(np.cumsum(daily_returns))
    return prices


def insert_assets(conn) -> dict:
    """Insert asset rows, return {symbol: id} map."""
    rows = []
    for asset_class, items in ASSETS.items():
        for symbol, name, mu, sigma in items:
            rows.append((symbol, name, asset_class, mu, sigma))

    with conn.cursor() as cur:
        execute_values(
            cur,
            "INSERT INTO assets (symbol, name, asset_class, daily_mu, daily_sigma) "
            "VALUES %s ON CONFLICT (symbol) DO NOTHING RETURNING id, symbol",
            rows,
        )
        # Re-fetch all IDs (handles both insert and pre-existing)
        cur.execute("SELECT id, symbol FROM assets")
        asset_map = {row[1]: row[0] for row in cur.fetchall()}
    conn.commit()
    log.info("Inserted %d assets.", len(rows))
    return asset_map


def insert_strategies(conn) -> dict:
    """Insert strategy rows, return {name: id} map."""
    rows = [(s[0], s[1]) for s in STRATEGIES]
    with conn.cursor() as cur:
        execute_values(
            cur,
            "INSERT INTO strategies (name, description) VALUES %s "
            "ON CONFLICT (name) DO NOTHING",
            rows,
        )
        cur.execute("SELECT id, name FROM strategies")
        strat_map = {row[1]: row[0] for row in cur.fetchall()}
    conn.commit()
    log.info("Inserted %d strategies.", len(rows))
    return strat_map


def simulate_trades_for_strategy(
    strategy_idx: int,
    strategy_name: str,
    asset_map: dict,
    trading_days: pd.DatetimeIndex,
) -> pd.DataFrame:
    """
    Simulate individual trades for one strategy.
    Each trading day we randomly enter 2-6 trades across the strategy's preferred assets.
    Direction and sizing are influenced by the strategy's 'edge' (sharpe_target).
    """
    _, _, sharpe_target, preferred_classes = STRATEGIES[strategy_idx]

    # Collect assets this strategy trades
    tradeable = [
        (symbol, asset_class)
        for asset_class, items in ASSETS.items()
        for symbol, *_ in items
        if asset_class in preferred_classes
    ]

    rng = np.random.default_rng(RANDOM_SEED + strategy_idx * 1000)

    # Pre-generate price series for each asset
    n_days = len(trading_days)
    price_series = {}
    for i, (asset_class, items) in enumerate(ASSETS.items()):
        for j, (symbol, _, mu, sigma) in enumerate(items):
            price_series[symbol] = generate_price_series(mu, sigma, n_days, seed_offset=i * 100 + j)

    records = []
    for day_idx, trade_date in enumerate(trading_days):
        # Number of trades this day
        n_trades = rng.integers(2, 7)
        chosen = [tradeable[i] for i in rng.choice(len(tradeable), size=n_trades, replace=False)]

        for symbol, asset_class in chosen:
            prices = price_series[symbol]
            entry = prices[day_idx]

            # Exit price: 1-day forward (or same day with slippage sim)
            hold_days = rng.integers(1, 4)  # hold 1-3 days
            exit_idx = min(day_idx + hold_days, n_days - 1)
            base_exit = prices[exit_idx]

            # Strategy edge: slightly bias returns upward based on sharpe target
            edge = (sharpe_target / 252) * entry * rng.normal(1.0, 0.5)
            exit_price = base_exit + edge

            direction = "L" if rng.random() > 0.38 else "S"  # 62% long bias
            quantity  = int(rng.integers(10, 501))             # 10-500 units
            sign      = 1 if direction == "L" else -1

            raw_pnl    = sign * (exit_price - entry) * quantity
            return_pct = sign * (exit_price - entry) / entry

            records.append({
                "strategy_name": strategy_name,
                "symbol":        symbol,
                "trade_date":    trade_date.date(),
                "direction":     direction,
                "entry_price":   round(entry, 6),
                "exit_price":    round(exit_price, 6),
                "quantity":      quantity,
                "pnl":           round(raw_pnl, 2),
                "return_pct":    round(return_pct, 6),
            })

    return pd.DataFrame(records)


def compute_daily_performance(trades_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate trades → daily PnL + running metrics per strategy."""
    daily = (
        trades_df
        .groupby(["strategy_name", "trade_date"])
        .agg(
            daily_pnl   = ("pnl", "sum"),
            trade_count = ("pnl", "count"),
            win_count   = ("pnl", lambda x: (x > 0).sum()),
        )
        .reset_index()
        .sort_values(["strategy_name", "trade_date"])
    )

    # Cumulative PnL and drawdown per strategy
    results = []
    for strategy_name, grp in daily.groupby("strategy_name"):
        grp = grp.copy().sort_values("trade_date")
        grp["cumulative_pnl"] = grp["daily_pnl"].cumsum()
        grp["daily_return"]   = grp["daily_pnl"] / 100_000  # assume 100k AUM

        # Drawdown: use portfolio value (AUM + cumPnL) so denominator never approaches 0
        AUM = 100_000
        portfolio_value = AUM + grp["cumulative_pnl"]
        running_max = portfolio_value.cummax()
        grp["drawdown"] = (running_max - portfolio_value) / running_max

        results.append(grp)

    return pd.concat(results).reset_index(drop=True)


# ══════════════════════════════════════════════════════════════════════════════
# Metrics calculations
# ══════════════════════════════════════════════════════════════════════════════

def calc_sharpe(daily_returns: np.ndarray, risk_free_rate: float = 0.05) -> float:
    """Annualised Sharpe Ratio."""
    excess = daily_returns - risk_free_rate / 252
    if excess.std() == 0:
        return 0.0
    return float(np.sqrt(252) * excess.mean() / excess.std())


def calc_sortino(daily_returns: np.ndarray, risk_free_rate: float = 0.05) -> float:
    """Annualised Sortino Ratio (downside deviation only)."""
    excess     = daily_returns - risk_free_rate / 252
    downside   = excess[excess < 0]
    down_std   = np.sqrt((downside ** 2).mean()) if len(downside) > 0 else 1e-9
    return float(np.sqrt(252) * excess.mean() / down_std)


def calc_max_drawdown(cumulative_pnl: np.ndarray) -> float:
    """Peak-to-trough drawdown as a positive fraction, using portfolio value as denominator."""
    AUM = 100_000
    portfolio_value = AUM + cumulative_pnl
    running_max = np.maximum.accumulate(portfolio_value)
    drawdowns = (running_max - portfolio_value) / running_max
    return float(drawdowns.max())


def compute_strategy_metrics(trades_df: pd.DataFrame, daily_df: pd.DataFrame) -> pd.DataFrame:
    """Compute summary metrics per strategy."""
    rows = []
    for strategy_name, t_grp in trades_df.groupby("strategy_name"):
        d_grp = daily_df[daily_df["strategy_name"] == strategy_name].sort_values("trade_date")

        daily_ret  = d_grp["daily_return"].values
        cum_pnl    = d_grp["cumulative_pnl"].values
        wins       = t_grp[t_grp["pnl"] > 0]["pnl"]
        losses     = t_grp[t_grp["pnl"] < 0]["pnl"]

        profit_factor = (wins.sum() / (-losses.sum())) if len(losses) > 0 else float("inf")

        rows.append({
            "strategy_name": strategy_name,
            "total_pnl":     round(float(t_grp["pnl"].sum()), 2),
            "sharpe_ratio":  round(calc_sharpe(daily_ret), 4),
            "sortino_ratio": round(calc_sortino(daily_ret), 4),
            "max_drawdown":  round(calc_max_drawdown(cum_pnl), 4),
            "win_rate":      round(float((t_grp["pnl"] > 0).mean()), 4),
            "avg_win":       round(float(wins.mean()) if len(wins) > 0 else 0, 2),
            "avg_loss":      round(float(losses.mean()) if len(losses) > 0 else 0, 2),
            "profit_factor": round(min(profit_factor, 99.0), 4),
            "total_trades":  int(len(t_grp)),
        })

    return pd.DataFrame(rows)


# ══════════════════════════════════════════════════════════════════════════════
# DB writes
# ══════════════════════════════════════════════════════════════════════════════

def write_trades(conn, trades_df: pd.DataFrame, asset_map: dict, strat_map: dict):
    rows = [
        (
            strat_map[r.strategy_name],
            asset_map[r.symbol],
            r.trade_date,
            r.direction,
            r.entry_price,
            r.exit_price,
            r.quantity,
            r.pnl,
            r.return_pct,
        )
        for r in trades_df.itertuples()
    ]
    with conn.cursor() as cur:
        execute_values(
            cur,
            """INSERT INTO trades
               (strategy_id, asset_id, trade_date, direction,
                entry_price, exit_price, quantity, pnl, return_pct)
               VALUES %s""",
            rows,
            page_size=2000,
        )
    conn.commit()
    log.info("Inserted %d trade records.", len(rows))


def write_daily_performance(conn, daily_df: pd.DataFrame, strat_map: dict):
    rows = [
        (
            strat_map[r.strategy_name],
            r.trade_date,
            r.daily_pnl,
            r.daily_return,
            r.cumulative_pnl,
            r.drawdown,
            r.trade_count,
            r.win_count,
        )
        for r in daily_df.itertuples()
    ]
    with conn.cursor() as cur:
        execute_values(
            cur,
            """INSERT INTO daily_performance
               (strategy_id, perf_date, daily_pnl, daily_return,
                cumulative_pnl, drawdown, trade_count, win_count)
               VALUES %s
               ON CONFLICT (strategy_id, perf_date) DO UPDATE SET
                 daily_pnl      = EXCLUDED.daily_pnl,
                 daily_return   = EXCLUDED.daily_return,
                 cumulative_pnl = EXCLUDED.cumulative_pnl,
                 drawdown       = EXCLUDED.drawdown,
                 trade_count    = EXCLUDED.trade_count,
                 win_count      = EXCLUDED.win_count""",
            rows,
            page_size=2000,
        )
    conn.commit()
    log.info("Inserted %d daily performance rows.", len(rows))


def write_strategy_metrics(conn, metrics_df: pd.DataFrame, strat_map: dict):
    rows = [
        (
            strat_map[r.strategy_name],
            r.total_pnl,
            r.sharpe_ratio,
            r.sortino_ratio,
            r.max_drawdown,
            r.win_rate,
            r.avg_win,
            r.avg_loss,
            r.profit_factor,
            r.total_trades,
        )
        for r in metrics_df.itertuples()
    ]
    with conn.cursor() as cur:
        execute_values(
            cur,
            """INSERT INTO strategy_metrics
               (strategy_id, total_pnl, sharpe_ratio, sortino_ratio,
                max_drawdown, win_rate, avg_win, avg_loss, profit_factor, total_trades)
               VALUES %s
               ON CONFLICT (strategy_id) DO UPDATE SET
                 total_pnl     = EXCLUDED.total_pnl,
                 sharpe_ratio  = EXCLUDED.sharpe_ratio,
                 sortino_ratio = EXCLUDED.sortino_ratio,
                 max_drawdown  = EXCLUDED.max_drawdown,
                 win_rate      = EXCLUDED.win_rate,
                 avg_win       = EXCLUDED.avg_win,
                 avg_loss      = EXCLUDED.avg_loss,
                 profit_factor = EXCLUDED.profit_factor,
                 total_trades  = EXCLUDED.total_trades,
                 computed_at   = NOW()""",
            rows,
        )
    conn.commit()
    log.info("Upserted %d strategy metric rows.", len(rows))


# ══════════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════════

def main():
    log.info("=== Trading Data Pipeline START ===")

    # Trading calendar: weekdays only
    trading_days = pd.bdate_range(start=START_DATE, end=END_DATE)
    log.info("Trading days: %d  (%s → %s)", len(trading_days), START_DATE.date(), END_DATE.date())

    conn = get_connection()
    try:
        # 1. Schema
        create_schema(conn)

        # 2. Reference data
        asset_map = insert_assets(conn)
        strat_map = insert_strategies(conn)

        # 3. Simulate trades for every strategy
        all_trades = []
        for idx, (name, *_) in enumerate(STRATEGIES):
            log.info("Simulating strategy: %s", name)
            df = simulate_trades_for_strategy(idx, name, asset_map, trading_days)
            all_trades.append(df)

        trades_df = pd.concat(all_trades, ignore_index=True)
        log.info("Total trades generated: %d", len(trades_df))

        # 4. Aggregate daily performance
        daily_df = compute_daily_performance(trades_df)
        log.info("Daily performance rows: %d", len(daily_df))

        # 5. Summary metrics
        metrics_df = compute_strategy_metrics(trades_df, daily_df)

        # 6. Write to DB
        write_trades(conn, trades_df, asset_map, strat_map)
        write_daily_performance(conn, daily_df, strat_map)
        write_strategy_metrics(conn, metrics_df, strat_map)

        # 7. Quick sanity print
        log.info("\n── Strategy Metrics Summary ──")
        print(
            metrics_df[["strategy_name", "total_pnl", "sharpe_ratio",
                         "max_drawdown", "win_rate", "total_trades"]]
            .to_string(index=False)
        )

    finally:
        conn.close()

    log.info("=== Pipeline DONE ===")


if __name__ == "__main__":
    main()