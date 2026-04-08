"""
Trading Data Pipeline — Real Market Data v2
============================================
Yahoo Finance 2020-2024 real prices.
Higher-frequency signals + full asset coverage → ~5000 trades.

Changes vs v1:
  - MomentumAlpha: weekly rebalance (was monthly)
  - MeanReversion: all 8 equities, 5-day hold max (was 4 equities, 10-day)
  - StatArb: add NVDA/AMZN pair alongside AAPL/MSFT
  - MacroTrend: all assets with enough history, weekly check (was daily scan)
  - VolBreakout: 10-day hold max + lower ATR multiplier to trigger more entries
"""

import os
import logging
import math
from datetime import datetime

import numpy as np
import pandas as pd
import yfinance as yf
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", 5432)),
    "dbname":   os.getenv("DB_NAME", "trading_db"),
    "user":     os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "password"),
}

START_DATE = "2020-01-01"
END_DATE   = "2024-12-31"
AUM        = 1_000_000

ASSETS = [
    ("AAPL",    "AAPL",   "Apple Inc.",             "equity"),
    ("MSFT",    "MSFT",   "Microsoft Corp.",         "equity"),
    ("GOOGL",   "GOOGL",  "Alphabet Inc.",           "equity"),
    ("NVDA",    "NVDA",   "NVIDIA Corp.",            "equity"),
    ("AMZN",    "AMZN",   "Amazon.com Inc.",         "equity"),
    ("META",    "META",   "Meta Platforms Inc.",     "equity"),
    ("TSLA",    "TSLA",   "Tesla Inc.",              "equity"),
    ("JPM",     "JPM",    "JPMorgan Chase",          "equity"),
    ("ES=F",    "ES",     "E-mini S&P 500",          "futures"),
    ("NQ=F",    "NQ",     "E-mini Nasdaq-100",       "futures"),
    ("GC=F",    "GC",     "Gold Futures",            "futures"),
    ("CL=F",    "CL",     "Crude Oil WTI",           "futures"),
    ("ZB=F",    "ZB",     "US 30Y T-Bond",           "futures"),
    ("EURUSD=X","EURUSD", "Euro / US Dollar",        "forex"),
    ("GBPUSD=X","GBPUSD", "Pound / US Dollar",       "forex"),
    ("USDJPY=X","USDJPY", "US Dollar / Yen",         "forex"),
    ("AUDUSD=X","AUDUSD", "Australian / US Dollar",  "forex"),
    ("USDCHF=X","USDCHF", "US Dollar / CHF",         "forex"),
]

STRATEGIES = [
    "MomentumAlpha", "MeanReversion", "StatArb", "MacroTrend", "VolBreakout"
]


# ── Download ──────────────────────────────────────────────────────────────────

def download_prices() -> pd.DataFrame:
    yahoo_symbols = [a[0] for a in ASSETS]
    log.info("Downloading from Yahoo Finance (%s → %s)...", START_DATE, END_DATE)
    df = yf.download(yahoo_symbols, start=START_DATE, end=END_DATE,
                     auto_adjust=True, progress=False)["Close"]
    rename = {a[0]: a[1] for a in ASSETS}
    df.rename(columns=rename, inplace=True)
    df = df.ffill().dropna(how="all")
    log.info("Downloaded %d days, %d assets.", len(df), df.shape[1])

    missing = df.isna().mean() * 100
    bad = missing[missing > 20]
    if len(bad):
        log.warning("Assets with >20%% missing (will be skipped):\n%s", bad.to_string())
    return df


def good_cols(df: pd.DataFrame, min_pct: float = 0.8) -> list:
    """Return columns with at least min_pct non-null."""
    return [c for c in df.columns if df[c].notna().mean() >= min_pct]


# ── Strategies ────────────────────────────────────────────────────────────────

def strategy_momentum(prices: pd.DataFrame) -> pd.DataFrame:
    """Weekly cross-sectional momentum — top 3 long, bottom 3 short."""
    equity_cols = [c for c in good_cols(prices) if c in
                   [a[1] for a in ASSETS if a[3] == "equity"]]
    eq = prices[equity_cols].dropna()
    records = []

    # Rebalance every 5 trading days (weekly)
    for i in range(20, len(eq) - 5, 5):
        window  = eq.iloc[i-20:i]
        returns = (window.iloc[-1] / window.iloc[0]) - 1
        ranked  = returns.sort_values(ascending=False)
        longs   = ranked.head(3).index.tolist()
        shorts  = ranked.tail(3).index.tolist()

        entry_date = eq.index[i].date()
        exit_idx   = min(i + 5, len(eq) - 1)
        exit_date  = eq.index[exit_idx].date()

        for sym, direction in [(s, "L") for s in longs] + [(s, "S") for s in shorts]:
            ep = eq.loc[eq.index[i], sym]
            xp = eq.loc[eq.index[exit_idx], sym]
            qty = max(1, int(AUM * 0.06 / ep))
            sign = 1 if direction == "L" else -1
            pnl  = sign * (xp - ep) * qty
            records.append({
                "strategy": "MomentumAlpha", "symbol": sym,
                "trade_date": entry_date, "direction": direction,
                "entry_price": round(ep, 4), "exit_price": round(xp, 4),
                "quantity": qty, "pnl": round(pnl, 2),
                "return_pct": round(sign * (xp - ep) / ep, 6),
            })
    return pd.DataFrame(records)


def strategy_mean_reversion(prices: pd.DataFrame) -> pd.DataFrame:
    """Bollinger Band mean reversion on all equities, 5-day max hold."""
    equity_cols = [c for c in good_cols(prices) if c in
                   [a[1] for a in ASSETS if a[3] == "equity"]]
    records = []

    for sym in equity_cols:
        p    = prices[sym].dropna()
        ma   = p.rolling(20).mean()
        std  = p.rolling(20).std()
        upper = ma + 2 * std
        lower = ma - 2 * std

        in_trade = False
        ep = 0.0; ed = None; direction = "L"

        for i in range(20, len(p)):
            price = p.iloc[i]
            date  = p.index[i].date()

            if not in_trade:
                if price < lower.iloc[i]:
                    in_trade = True; ep = price; ed = date; direction = "L"
                elif price > upper.iloc[i]:
                    in_trade = True; ep = price; ed = date; direction = "S"
            else:
                days = (p.index[i] - pd.Timestamp(ed)).days
                at_mean = (direction == "L" and price >= ma.iloc[i]) or \
                          (direction == "S" and price <= ma.iloc[i])
                if at_mean or days >= 5:
                    qty  = max(1, int(AUM * 0.04 / ep))
                    sign = 1 if direction == "L" else -1
                    pnl  = sign * (price - ep) * qty
                    ret  = sign * (price - ep) / ep
                    records.append({
                        "strategy": "MeanReversion", "symbol": sym,
                        "trade_date": ed, "direction": direction,
                        "entry_price": round(ep, 4), "exit_price": round(price, 4),
                        "quantity": qty, "pnl": round(pnl, 2),
                        "return_pct": round(ret, 6),
                    })
                    in_trade = False
    return pd.DataFrame(records)


def strategy_stat_arb(prices: pd.DataFrame) -> pd.DataFrame:
    """Pairs trading: AAPL/MSFT and NVDA/AMZN."""
    pairs = [("AAPL", "MSFT"), ("NVDA", "AMZN")]
    records = []

    for sym1, sym2 in pairs:
        if sym1 not in prices.columns or sym2 not in prices.columns:
            continue
        p1 = prices[sym1].dropna()
        p2 = prices[sym2].dropna()
        idx = p1.index.intersection(p2.index)
        p1, p2 = p1[idx], p2[idx]

        ratio  = p1 / p2
        z      = (ratio - ratio.rolling(40).mean()) / ratio.rolling(40).std()

        in_trade = False; ez = 0.0; ep1 = ep2 = 0.0; ed = None; leg = "L"

        for i in range(40, len(idx)):
            zv   = z.iloc[i]
            date = idx[i].date()
            if not in_trade:
                if zv > 1.5:
                    in_trade = True; ez = zv; ep1 = p1.iloc[i]
                    ep2 = p2.iloc[i]; ed = date; leg = "S_p1"
                elif zv < -1.5:
                    in_trade = True; ez = zv; ep1 = p1.iloc[i]
                    ep2 = p2.iloc[i]; ed = date; leg = "L_p1"
            else:
                days = (idx[i] - pd.Timestamp(ed)).days
                if abs(zv) < 0.3 or days > 15:
                    q1 = max(1, int(AUM * 0.04 / ep1))
                    q2 = max(1, int(AUM * 0.04 / ep2))
                    for sym, ep, xp, qty, d in [
                        (sym1, ep1, p1.iloc[i], q1, "L" if leg == "L_p1" else "S"),
                        (sym2, ep2, p2.iloc[i], q2, "S" if leg == "L_p1" else "L"),
                    ]:
                        sign = 1 if d == "L" else -1
                        records.append({
                            "strategy": "StatArb", "symbol": sym,
                            "trade_date": ed, "direction": d,
                            "entry_price": round(ep, 4), "exit_price": round(xp, 4),
                            "quantity": qty,
                            "pnl": round(sign * (xp - ep) * qty, 2),
                            "return_pct": round(sign * (xp - ep) / ep, 6),
                        })
                    in_trade = False
    return pd.DataFrame(records)


def strategy_macro_trend(prices: pd.DataFrame) -> pd.DataFrame:
    """MA50/MA200 golden cross on all asset classes, check weekly."""
    cols = good_cols(prices, min_pct=0.7)
    records = []

    for sym in cols:
        p = prices[sym].dropna()
        if len(p) < 210:
            continue
        ma50  = p.rolling(50).mean()
        ma200 = p.rolling(200).mean()

        in_trade = False; ep = 0.0; ed = None

        # Check every 5 days instead of every day — more realistic, more trades over time
        check_indices = list(range(200, len(p), 5))
        for i in check_indices:
            price  = p.iloc[i]
            date   = p.index[i].date()
            golden = ma50.iloc[i] > ma200.iloc[i]
            death  = ma50.iloc[i] < ma200.iloc[i]

            if not in_trade and golden:
                in_trade = True; ep = price; ed = date
            elif in_trade and death:
                qty = max(1, int(AUM * 0.03 / ep))
                pnl = (price - ep) * qty
                records.append({
                    "strategy": "MacroTrend", "symbol": sym,
                    "trade_date": ed, "direction": "L",
                    "entry_price": round(ep, 4), "exit_price": round(price, 4),
                    "quantity": qty, "pnl": round(pnl, 2),
                    "return_pct": round((price - ep) / ep, 6),
                })
                in_trade = False
    return pd.DataFrame(records)


def strategy_vol_breakout(prices: pd.DataFrame) -> pd.DataFrame:
    """ATR breakout on all equities + futures, lower threshold, 10-day hold."""
    cols = [c for c in good_cols(prices) if c in
            [a[1] for a in ASSETS if a[3] in ("equity", "futures")]]
    records = []

    for sym in cols:
        p = prices[sym].dropna()
        atr    = p.diff().abs().rolling(14).mean()
        high20 = p.rolling(20).max()

        in_trade = False; ep = 0.0; ed = None

        for i in range(20, len(p)):
            price = p.iloc[i]
            date  = p.index[i].date()
            # Lower multiplier (1.0 vs 1.5) = more breakout signals
            breakout = high20.iloc[i-1] + 1.0 * atr.iloc[i]

            if not in_trade and price > breakout:
                in_trade = True; ep = price; ed = date
            elif in_trade:
                days = (p.index[i] - pd.Timestamp(ed)).days
                stop = ep - 2.0 * atr.iloc[i]
                if price < stop or days >= 10:
                    qty = max(1, int(AUM * 0.03 / ep))
                    pnl = (price - ep) * qty
                    records.append({
                        "strategy": "VolBreakout", "symbol": sym,
                        "trade_date": ed, "direction": "L",
                        "entry_price": round(ep, 4), "exit_price": round(price, 4),
                        "quantity": qty, "pnl": round(pnl, 2),
                        "return_pct": round((price - ep) / ep, 6),
                    })
                    in_trade = False
    return pd.DataFrame(records)


# ── Metrics ───────────────────────────────────────────────────────────────────

def calc_sharpe(r, rf=0.02):
    excess = r - rf / 252
    return float(np.sqrt(252) * excess.mean() / excess.std()) if excess.std() > 0 else 0.0

def calc_sortino(r, rf=0.02):
    excess = r - rf / 252
    down   = excess[excess < 0]
    dstd   = np.sqrt((down**2).mean()) if len(down) > 0 else 1e-9
    return float(np.sqrt(252) * excess.mean() / dstd)

def calc_max_drawdown(cum):
    port = AUM + cum
    mx   = np.maximum.accumulate(port)
    return float(((mx - port) / mx).max())

def compute_daily(trades_df):
    results = []
    for strat, grp in trades_df.groupby("strategy"):
        d = (grp.groupby("trade_date")
               .agg(daily_pnl=("pnl","sum"), trade_count=("pnl","count"),
                    win_count=("pnl", lambda x: (x>0).sum()))
               .reset_index().sort_values("trade_date"))
        d["cumulative_pnl"] = d["daily_pnl"].cumsum()
        d["daily_return"]   = d["daily_pnl"] / AUM
        port = AUM + d["cumulative_pnl"]
        mx   = port.cummax()
        d["drawdown"] = (mx - port) / mx
        d["strategy"] = strat
        results.append(d)
    return pd.concat(results).reset_index(drop=True)

def compute_metrics(trades_df, daily_df):
    rows = []
    for strat, t in trades_df.groupby("strategy"):
        d    = daily_df[daily_df["strategy"] == strat].sort_values("trade_date")
        wins = t[t["pnl"] > 0]["pnl"]
        loss = t[t["pnl"] < 0]["pnl"]
        pf   = (wins.sum() / -loss.sum()) if len(loss) > 0 else 99.0
        rows.append({
            "strategy":     strat,
            "total_pnl":    round(float(t["pnl"].sum()), 2),
            "sharpe_ratio": round(calc_sharpe(d["daily_return"].values), 4),
            "sortino_ratio":round(calc_sortino(d["daily_return"].values), 4),
            "max_drawdown": round(calc_max_drawdown(d["cumulative_pnl"].values), 4),
            "win_rate":     round(float((t["pnl"] > 0).mean()), 4),
            "avg_win":      round(float(wins.mean()) if len(wins) else 0, 2),
            "avg_loss":     round(float(loss.mean()) if len(loss) else 0, 2),
            "profit_factor":round(min(pf, 99.0), 4),
            "total_trades": int(len(t)),
        })
    return pd.DataFrame(rows)


# ── DB ────────────────────────────────────────────────────────────────────────

def get_conn(): return psycopg2.connect(**DB_CONFIG)

def create_schema(conn):
    with conn.cursor() as cur:
        cur.execute("""
        DROP TABLE IF EXISTS strategy_metrics, daily_performance, trades, strategies, assets CASCADE;
        CREATE TABLE assets (id SERIAL PRIMARY KEY, symbol VARCHAR(16) NOT NULL UNIQUE,
            name VARCHAR(128) NOT NULL, asset_class VARCHAR(16) NOT NULL,
            daily_mu FLOAT NOT NULL DEFAULT 0, daily_sigma FLOAT NOT NULL DEFAULT 0);
        CREATE TABLE strategies (id SERIAL PRIMARY KEY, name VARCHAR(64) NOT NULL UNIQUE, description TEXT);
        CREATE TABLE trades (id SERIAL PRIMARY KEY, strategy_id INT NOT NULL REFERENCES strategies(id),
            asset_id INT NOT NULL REFERENCES assets(id), trade_date DATE NOT NULL,
            direction CHAR(1) NOT NULL, entry_price FLOAT NOT NULL, exit_price FLOAT NOT NULL,
            quantity INT NOT NULL, pnl FLOAT NOT NULL, return_pct FLOAT NOT NULL);
        CREATE INDEX ix_t_date ON trades(trade_date);
        CREATE INDEX ix_t_strat ON trades(strategy_id, trade_date);
        CREATE TABLE daily_performance (id SERIAL PRIMARY KEY,
            strategy_id INT NOT NULL REFERENCES strategies(id), perf_date DATE NOT NULL,
            daily_pnl FLOAT NOT NULL, daily_return FLOAT NOT NULL, cumulative_pnl FLOAT NOT NULL,
            drawdown FLOAT NOT NULL, trade_count INT NOT NULL, win_count INT NOT NULL,
            UNIQUE (strategy_id, perf_date));
        CREATE TABLE strategy_metrics (strategy_id INT PRIMARY KEY REFERENCES strategies(id),
            total_pnl FLOAT NOT NULL, sharpe_ratio FLOAT NOT NULL, sortino_ratio FLOAT NOT NULL,
            max_drawdown FLOAT NOT NULL, win_rate FLOAT NOT NULL, avg_win FLOAT NOT NULL,
            avg_loss FLOAT NOT NULL, profit_factor FLOAT NOT NULL, total_trades INT NOT NULL,
            computed_at TIMESTAMPTZ DEFAULT NOW());
        """)
    conn.commit()
    log.info("Schema created.")

def insert_ref(conn):
    with conn.cursor() as cur:
        execute_values(cur,
            "INSERT INTO assets (symbol,name,asset_class) VALUES %s ON CONFLICT (symbol) DO NOTHING",
            [(a[1],a[2],a[3]) for a in ASSETS])
        cur.execute("SELECT id,symbol FROM assets")
        am = {r[1]:r[0] for r in cur.fetchall()}
        execute_values(cur,
            "INSERT INTO strategies (name,description) VALUES %s ON CONFLICT (name) DO NOTHING",
            [(s, f"Real-data {s} strategy — Yahoo Finance 2020-2024") for s in STRATEGIES])
        cur.execute("SELECT id,name FROM strategies")
        sm = {r[1]:r[0] for r in cur.fetchall()}
    conn.commit()
    return am, sm

def write_trades(conn, df, am, sm):
    df = df[df["symbol"].isin(am)]
    rows = [(sm[r.strategy], am[r.symbol], r.trade_date, r.direction,
             r.entry_price, r.exit_price, r.quantity, r.pnl, r.return_pct)
            for r in df.itertuples()]
    with conn.cursor() as cur:
        execute_values(cur,
            "INSERT INTO trades (strategy_id,asset_id,trade_date,direction,"
            "entry_price,exit_price,quantity,pnl,return_pct) VALUES %s",
            rows, page_size=2000)
    conn.commit()
    log.info("Inserted %d trades.", len(rows))

def write_daily(conn, df, sm):
    rows = [(sm[r.strategy], r.trade_date, r.daily_pnl, r.daily_return,
             r.cumulative_pnl, r.drawdown, r.trade_count, r.win_count)
            for r in df.itertuples()]
    with conn.cursor() as cur:
        execute_values(cur,
            "INSERT INTO daily_performance (strategy_id,perf_date,daily_pnl,daily_return,"
            "cumulative_pnl,drawdown,trade_count,win_count) VALUES %s "
            "ON CONFLICT (strategy_id,perf_date) DO NOTHING",
            rows, page_size=2000)
    conn.commit()
    log.info("Inserted %d daily rows.", len(rows))

def write_metrics(conn, df, sm):
    rows = [(sm[r.strategy], r.total_pnl, r.sharpe_ratio, r.sortino_ratio,
             r.max_drawdown, r.win_rate, r.avg_win, r.avg_loss, r.profit_factor, r.total_trades)
            for r in df.itertuples()]
    with conn.cursor() as cur:
        execute_values(cur,
            "INSERT INTO strategy_metrics (strategy_id,total_pnl,sharpe_ratio,sortino_ratio,"
            "max_drawdown,win_rate,avg_win,avg_loss,profit_factor,total_trades) VALUES %s "
            "ON CONFLICT (strategy_id) DO UPDATE SET total_pnl=EXCLUDED.total_pnl,"
            "sharpe_ratio=EXCLUDED.sharpe_ratio,sortino_ratio=EXCLUDED.sortino_ratio,"
            "max_drawdown=EXCLUDED.max_drawdown,win_rate=EXCLUDED.win_rate,"
            "avg_win=EXCLUDED.avg_win,avg_loss=EXCLUDED.avg_loss,"
            "profit_factor=EXCLUDED.profit_factor,total_trades=EXCLUDED.total_trades,"
            "computed_at=NOW()", rows)
    conn.commit()
    log.info("Upserted %d metric rows.", len(rows))


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    log.info("=== Trading Data Pipeline (Real Data v2) START ===")

    prices = download_prices()

    log.info("Running strategies...")
    dfs = []
    for fn in [strategy_momentum, strategy_mean_reversion, strategy_stat_arb,
               strategy_macro_trend, strategy_vol_breakout]:
        df = fn(prices)
        log.info("  %-25s %d trades", fn.__name__, len(df))
        if len(df): dfs.append(df)

    trades_df = pd.concat(dfs, ignore_index=True)
    log.info("Total trades: %d", len(trades_df))

    daily_df   = compute_daily(trades_df)
    metrics_df = compute_metrics(trades_df, daily_df)

    log.info("\n── Strategy Summary ──")
    print(metrics_df[["strategy","total_pnl","sharpe_ratio",
                       "max_drawdown","win_rate","total_trades"]].to_string(index=False))

    conn = get_conn()
    try:
        create_schema(conn)
        am, sm = insert_ref(conn)
        write_trades(conn, trades_df, am, sm)
        write_daily(conn, daily_df, sm)
        write_metrics(conn, metrics_df, sm)
    finally:
        conn.close()

    log.info("=== Pipeline DONE ===")

if __name__ == "__main__":
    main()