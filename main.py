"""Binance USDT perpetual futures: confirmed 1h short reversal WATCH Bot.

Version 3.0.0. Notifications only; no trades, CSV or persistent state.
Dependencies: aiohttp, python-dotenv (existing requirements.txt).
"""
import asyncio
import json
import logging
import math
import os
import signal
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import aiohttp
from dotenv import load_dotenv

load_dotenv()

VERSION = "3.0.0"
BASE_URL = os.getenv("BINANCE_FAPI_BASE_URL", "https://fapi.binance.com").rstrip("/")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
REQUEST_CONCURRENCY = max(1, int(os.getenv("REQUEST_CONCURRENCY", "8")))
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "20"))
REQUEST_RETRIES = max(1, int(os.getenv("REQUEST_RETRIES", "3")))
RUN_BUFFER_SECONDS = max(5, int(os.getenv("RUN_BUFFER_SECONDS", "20")))

# Fixed strategy parameters. Old DAILY_* and ABNORMAL_* env variables are ignored.
MIN_VOLUME_USDT = 1_000_000.0
MIN_REL_VOLUME = 1.5
MIN_OI_RISE_PCT = 5.0
MIN_UPPER_WICK_BODY = 1.0
HIGH_LOOKBACK = 24
VOLUME_LOOKBACK = 24
OI_LOOKBACK = 24
MA_PERIODS = (5, 10, 30, 50, 100)
KLINE_LIMIT = 130
JST = timezone(timedelta(hours=9))
UTC = timezone.utc
HOUR_MS = 3_600_000
OI_TIME_TOLERANCE_MS = 3 * 60_000

logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("binance-short-watch")


@dataclass(frozen=True)
class KlineCandidate:
    symbol: str
    start_ms: int
    end_boundary_ms: int
    open: float
    high: float
    low: float
    close: float
    high_rank: int
    wick_body_pct: float
    quote_volume: float
    volume_multiple: float
    mas: dict[int, float]


@dataclass(frozen=True)
class Alert:
    candle: KlineCandidate
    funding_rate: float
    oi_usdt: float
    oi_min_rise_pct: float
    oi_change_1h_pct: float
    oi_change_3h_pct: float


def finite_float(value: Any) -> float:
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("non-finite numeric data")
    return number


def percent_change(now: float, before: float) -> float:
    if before <= 0:
        raise ValueError("invalid denominator")
    return (now / before - 1.0) * 100.0


def human_usdt(number: float) -> str:
    if number >= 1e9:
        return f"{number / 1e9:,.2f}B USDT"
    if number >= 1e6:
        return f"{number / 1e6:,.2f}M USDT"
    if number >= 1e3:
        return f"{number / 1e3:,.2f}K USDT"
