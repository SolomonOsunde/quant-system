"""Load instruments and stat-arb pairs from Timescale/Postgres."""
from __future__ import annotations

from functools import lru_cache
from typing import Optional

from loguru import logger
from sqlalchemy import create_engine, text

import config


def _engine():
    return create_engine(config.DB_URL, pool_pre_ping=True, pool_size=2)


@lru_cache(maxsize=1)
def load_enabled_symbols(market: Optional[str] = None) -> tuple[str, ...]:
    """Return enabled instrument symbols, optionally filtered by market."""
    sql = "SELECT symbol FROM instruments WHERE enabled = TRUE"
    params: dict = {}
    if market:
        sql += " AND market = :market"
        params["market"] = market.upper()
    sql += " ORDER BY priority ASC, symbol ASC"
    try:
        with _engine().connect() as conn:
            rows = conn.execute(text(sql), params).fetchall()
        symbols = tuple(r[0] for r in rows)
        logger.info(
            "Loaded {} enabled instrument(s) from DB{}",
            len(symbols),
            f" ({market})" if market else "",
        )
        return symbols
    except Exception as e:
        logger.error("Failed to load instruments from DB: {}", e)
        raise


@lru_cache(maxsize=1)
def load_stat_arb_pairs() -> tuple[tuple[str, str], ...]:
    sql = """
        SELECT leg1, leg2 FROM stat_arb_pairs
        WHERE enabled = TRUE
        ORDER BY priority ASC, leg1 ASC, leg2 ASC
    """
    try:
        with _engine().connect() as conn:
            rows = conn.execute(text(sql)).fetchall()
        pairs = tuple((r[0], r[1]) for r in rows)
        logger.info("Loaded {} stat-arb pair(s) from DB", len(pairs))
        return pairs
    except Exception as e:
        logger.error("Failed to load stat_arb_pairs from DB: {}", e)
        raise


def clear_instrument_cache():
    load_enabled_symbols.cache_clear()
    load_stat_arb_pairs.cache_clear()
