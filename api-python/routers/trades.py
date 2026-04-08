from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Optional
import math

from db.session import get_db
from models.schemas import DateRangeSchema, PagedResult, TradeSchema

router = APIRouter(prefix="/api/trades", tags=["trades"])


@router.get("", response_model=PagedResult[TradeSchema])
async def get_trades(
    strategy:    Optional[str] = Query(None),
    asset_class: Optional[str] = Query(None, alias="assetClass"),
    symbol:      Optional[str] = Query(None),
    date_from:   Optional[str] = Query(None, alias="dateFrom"),
    date_to:     Optional[str] = Query(None, alias="dateTo"),
    direction:   Optional[str] = Query(None),
    page:        int           = Query(1,   ge=1),
    page_size:   int           = Query(100, ge=1, le=50000, alias="pageSize"),
    db: AsyncSession           = Depends(get_db),
):
    conditions = ["1=1"]
    params: dict = {}

    if strategy:
        conditions.append("s.name = :strategy")
        params["strategy"] = strategy
    if asset_class:
        conditions.append("a.asset_class = :asset_class")
        params["asset_class"] = asset_class
    if symbol:
        conditions.append("a.symbol = :symbol")
        params["symbol"] = symbol
    if date_from:
        conditions.append("t.trade_date >= :date_from::date")
        params["date_from"] = date_from
    if date_to:
        conditions.append("t.trade_date <= :date_to::date")
        params["date_to"] = date_to
    if direction:
        dir_char = "L" if direction.lower() == "long" else "S"
        conditions.append("t.direction = :direction")
        params["direction"] = dir_char

    where = " AND ".join(conditions)
    base_sql = f"""
        FROM trades t
        JOIN strategies s ON s.id = t.strategy_id
        JOIN assets     a ON a.id = t.asset_id
        WHERE {where}
    """

    count_row = await db.execute(text(f"SELECT COUNT(*) {base_sql}"), params)
    total_count: int = count_row.scalar_one()

    offset = (page - 1) * page_size
    data_sql = text(f"""
        SELECT
            t.id,
            s.name                                  AS strategy,
            a.symbol,
            a.asset_class,
            t.trade_date::text                      AS trade_date,
            CASE WHEN t.direction = 'L' THEN 'Long' ELSE 'Short' END AS direction,
            ROUND(t.entry_price::numeric, 4)        AS entry_price,
            ROUND(t.exit_price::numeric,  4)        AS exit_price,
            t.quantity,
            ROUND(t.pnl::numeric, 2)                AS pnl,
            ROUND((t.return_pct * 100)::numeric, 4) AS return_pct
        {base_sql}
        ORDER BY t.trade_date DESC, t.pnl DESC
        LIMIT :limit OFFSET :offset
    """)
    rows = await db.execute(data_sql, {**params, "limit": page_size, "offset": offset})
    items = [dict(r._mapping) for r in rows]

    return PagedResult(
        items=items,
        total_count=total_count,
        page=page,
        page_size=page_size,
        total_pages=math.ceil(total_count / page_size),
    )


@router.get("/symbols", response_model=List[str])
async def get_symbols(db: AsyncSession = Depends(get_db)):
    rows = await db.execute(
        text("SELECT symbol FROM assets ORDER BY asset_class, symbol")
    )
    return [r[0] for r in rows]


@router.get("/date-range", response_model=DateRangeSchema)
async def get_date_range(db: AsyncSession = Depends(get_db)):
    row = await db.execute(
        text("SELECT MIN(trade_date)::text AS min_date, MAX(trade_date)::text AS max_date FROM trades")
    )
    r = row.first()
    return {"min_date": r.min_date, "max_date": r.max_date}
