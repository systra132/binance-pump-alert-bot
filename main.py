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
    return f"{number:,.2f} USDT"


def price_text(number: float) -> str:
    if number >= 1000:
        return f"{number:,.2f}"
    if number >= 1:
        return f"{number:.4f}"
    if number >= 0.01:
        return f"{number:.6f}"
    return f"{number:.10f}".rstrip("0").rstrip(".")


def candle_label(start_ms: int) -> str:
    dt = datetime.fromtimestamp(start_ms / 1000, JST)
    return f"{dt:%Y/%m/%d %H:%M}–{(dt + timedelta(minutes=59)):%H:%M} JST"


async def get_json(session: aiohttp.ClientSession, path: str, params: dict | None = None) -> Any:
    url = BASE_URL + path
    for attempt in range(1, REQUEST_RETRIES + 1):
        try:
            async with session.get(url, params=params) as response:
                raw = await response.text()
                if response.status == 200:
                    return json.loads(raw)
                # Respect exchange protection and do not retry malformed symbol/errors.
                if response.status in (418, 429) or response.status >= 500:
                    wait = min(60.0, float(response.headers.get("Retry-After", attempt * 2)))
                    if attempt < REQUEST_RETRIES:
                        log.warning("Retry Binance %s %s HTTP %s", path, params, response.status)
                        await asyncio.sleep(wait)
                        continue
                raise RuntimeError(f"Binance HTTP {response.status}: {path} {params} {raw[:180]}")
        except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError) as exc:
            if attempt >= REQUEST_RETRIES:
                raise RuntimeError(f"Binance request failed: {path} {params}: {exc}") from exc
            await asyncio.sleep(min(10, attempt * 2))
    raise RuntimeError(f"Binance request exhausted: {path}")


async def get_symbols(session: aiohttp.ClientSession) -> list[str]:
    data = await get_json(session, "/fapi/v1/exchangeInfo")
    return sorted(
        item["symbol"]
        for item in data["symbols"]
        if item.get("contractType") == "PERPETUAL"
        and item.get("quoteAsset") == "USDT"
        and item.get("status") == "TRADING"
    )


async def get_funding_map(session: aiohttp.ClientSession) -> dict[str, float]:
    # Bulk endpoint: 1 request for funding rates of all active contracts.
    data = await get_json(session, "/fapi/v1/premiumIndex")
    if not isinstance(data, list):
        raise RuntimeError("premiumIndex returned unexpected response")
    result = {}
    for item in data:
        try:
            result[item["symbol"]] = finite_float(item["lastFundingRate"])
        except (KeyError, TypeError, ValueError):
            continue
    return result


def evaluate_klines(symbol: str, klines: list, target_boundary_ms: int) -> KlineCandidate | None:
    # Explicit target boundary prevents accidentally using the previous hour
    # when a symbol's most recent candle has not propagated to the API yet.
    by_open = {int(k[0]): k for k in klines if len(k) > 7}
    target_open = target_boundary_ms - HOUR_MS
    history = [by_open.get(target_open - i * HOUR_MS) for i in range(KLINE_LIMIT - 1, -1, -1)]
    if any(k is None for k in history):
        # Fresh listings can have fewer than 130 bars: only 125 are required.
        needed = [by_open.get(target_open - i * HOUR_MS) for i in range(124, -1, -1)]
        if any(k is None for k in needed):
            return None
        history = needed
    target = history[-1]
    if int(target[6]) >= target_boundary_ms:
        return None
    opens = [finite_float(k[1]) for k in history]
    highs = [finite_float(k[2]) for k in history]
    lows = [finite_float(k[3]) for k in history]
    closes = [finite_float(k[4]) for k in history]
    volumes = [finite_float(k[7]) for k in history]  # quote asset volume in USDT
    o, h, l, c = opens[-1], highs[-1], lows[-1], closes[-1]
    if not (o > c > 0 and h >= o and l <= c):
        return None
    body = o - c
    wick = h - o
    if wick < MIN_UPPER_WICK_BODY * body:
        return None
    wick_body_pct = wick / body * 100.0
    # Standard competition ranking: ties share the same rank.
    high_rank = 1 + sum(value > h for value in highs[-HIGH_LOOKBACK:])
    if high_rank > 3:
        return None
    mas = {n: sum(closes[-n:]) / n for n in MA_PERIODS}
    if not all(mas[short] > mas[long] for short in (5, 10) for long in (30, 50, 100)):
        return None
    volume = volumes[-1]
    if volume < MIN_VOLUME_USDT:
        return None
    base = sum(volumes[-(VOLUME_LOOKBACK + 1):-1]) / VOLUME_LOOKBACK
    if base <= 0:
        return None
    volume_multiple = volume / base
    if volume_multiple < MIN_REL_VOLUME:
        return None
    return KlineCandidate(
        symbol, target_open, target_boundary_ms, o, h, l, c,
        high_rank, wick_body_pct, volume, volume_multiple, mas,
    )


async def check_klines(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    symbol: str,
    target_boundary_ms: int,
) -> KlineCandidate | None:
    async with sem:
        try:
            data = await get_json(session, "/fapi/v1/klines", {
                "symbol": symbol, "interval": "1h", "limit": KLINE_LIMIT,
                "endTime": target_boundary_ms - 1,
            })
            return evaluate_klines(symbol, data, target_boundary_ms)
        except Exception as exc:
            log.warning("%s kline check failed: %s", symbol, exc)
            return None


def match_hourly_oi(rows: list[dict], boundary_ms: int) -> list[dict] | None:
    """Require exact/near-boundary OI snapshots for all 24 consecutive hours.

    OI timestamps are data snapshot times, not candlestick OHLC timestamps.
    Reject missing/stale buckets rather than silently aligning to the wrong hour.
    """
    parsed = []
    for row in rows:
        try:
            ts = int(row["timestamp"])
            qty = finite_float(row["sumOpenInterest"])
            usd = finite_float(row["sumOpenInterestValue"])
            if qty > 0 and usd >= 0:
                parsed.append((ts, qty, usd))
        except (KeyError, TypeError, ValueError):
            continue
    matched = []
    for i in range(OI_LOOKBACK - 1, -1, -1):
        expected = boundary_ms - i * HOUR_MS
        options = [item for item in parsed if abs(item[0] - expected) <= OI_TIME_TOLERANCE_MS]
        if not options:
            return None
        ts, qty, usd = min(options, key=lambda item: abs(item[0] - expected))
        matched.append({"timestamp": ts, "quantity": qty, "usdt": usd})
    return matched


async def check_market(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    candle: KlineCandidate,
    funding_rate: float,
) -> Alert | None:
    if funding_rate <= 0:
        return None
    async with sem:
        try:
            rows = await get_json(session, "/futures/data/openInterestHist", {
                "symbol": candle.symbol,
                "period": "1h",
                "limit": 30,
                "endTime": candle.end_boundary_ms + OI_TIME_TOLERANCE_MS,
            })
            if not isinstance(rows, list):
                raise ValueError("invalid OI response")
            oi = match_hourly_oi(rows, candle.end_boundary_ms)
            if oi is None:
                log.warning("%s: OI snapshots missing for target hour; skipped", candle.symbol)
                return None
            qty = [row["quantity"] for row in oi]
            oi_min_rise = percent_change(qty[-1], min(qty))
            if oi_min_rise < MIN_OI_RISE_PCT:
                return None
            return Alert(
                candle=candle,
                funding_rate=funding_rate,
                oi_usdt=oi[-1]["usdt"],
                oi_min_rise_pct=oi_min_rise,
                oi_change_1h_pct=percent_change(qty[-1], qty[-2]),
                oi_change_3h_pct=percent_change(qty[-1], qty[-4]),
            )
        except Exception as exc:
            log.warning("%s OI check failed: %s", candle.symbol, exc)
            return None


def discord_payload(a: Alert) -> dict:
    c = a.candle
    return {
        "username": "Binance SHORT WATCH",
        "allowed_mentions": {"parse": []},
        "embeds": [{
            "title": f"🔴 SHORT WATCH | {c.symbol}",
            "url": f"https://www.binance.com/en/futures/{c.symbol}",
            "description": (
                f"**確定足**：{candle_label(c.start_ms)}\n"
                "**判定**：全条件一致（裁量確認用）"
            ),
            "color": 0xD94F55,
            "fields": [
                {"name": "終値", "value": f"{price_text(c.close)} USDT", "inline": True},
                {"name": "高値順位", "value": f"{c.high_rank}位 / 24本", "inline": True},
                {"name": "上ヒゲ / 実体", "value": f"{c.wick_body_pct:.1f}%", "inline": True},
                {"name": "出来高", "value": human_usdt(c.quote_volume), "inline": True},
                {"name": "出来高倍率", "value": f"{c.volume_multiple:.2f}倍", "inline": True},
                {"name": "FR（取得時点）", "value": f"{a.funding_rate * 100:+.4f}%", "inline": True},
                {"name": "OI総額", "value": human_usdt(a.oi_usdt), "inline": True},
                {"name": "24h最低OI比（数量）", "value": f"{a.oi_min_rise_pct:+.2f}%", "inline": True},
                {"name": "1h OI変化率（数量）", "value": f"{a.oi_change_1h_pct:+.2f}%", "inline": True},
                {"name": "3h OI変化率（数量）", "value": f"{a.oi_change_3h_pct:+.2f}%", "inline": True},
            ],
            "footer": {"text": f"MA5/10 > MA30/50/100 | v{VERSION} | 自動売買なし"},
        }],
    }


async def send_discord(session: aiohttp.ClientSession, payload: dict) -> bool:
    for attempt in range(1, REQUEST_RETRIES + 1):
        try:
            async with session.post(DISCORD_WEBHOOK_URL, json=payload) as r:
                body = await r.text()
                if r.status in (200, 204):
                    return True
                if r.status == 429 or r.status >= 500:
                    retry = min(60., float(r.headers.get("Retry-After", attempt * 2)))
                    if attempt < REQUEST_RETRIES:
                        await asyncio.sleep(retry)
                        continue
                log.error("Discord HTTP %s: %s", r.status, body[:200])
                return False
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            log.warning("Discord attempt %s failed: %s", attempt, exc)
            if attempt < REQUEST_RETRIES:
                await asyncio.sleep(attempt * 2)
    return False


async def scan(session: aiohttp.ClientSession, boundary_ms: int) -> None:
    start = asyncio.get_running_loop().time()
    symbols = await get_symbols(session)
    funding = await get_funding_map(session)
    sem = asyncio.Semaphore(REQUEST_CONCURRENCY)
    log.info("Checking %s symbols for candle ending %s UTC", len(symbols),
             datetime.fromtimestamp(boundary_ms / 1000, UTC).isoformat())
    k_results = await asyncio.gather(*(
        check_klines(session, sem, symbol, boundary_ms) for symbol in symbols
    ))
    candidates = [c for c in k_results if c is not None and funding.get(c.symbol, 0) > 0]
    log.info("Kline+FR candidates: %d", len(candidates))
    market_results = await asyncio.gather(*(
        check_market(session, sem, c, funding[c.symbol]) for c in candidates
    ))
    alerts = sorted((a for a in market_results if a),
                    key=lambda a: a.candle.volume_multiple, reverse=True)
    # No candidates => no Discord message. No deduplication: consecutive hourly
    # matches for the same symbol are reported again.
    sent = 0
    for alert in alerts:
        if await send_discord(session, discord_payload(alert)):
            sent += 1
        await asyncio.sleep(0.25)
    elapsed = asyncio.get_running_loop().time() - start
    log.info("Scan complete: %d matched, %d sent, %.1fs", len(alerts), sent, elapsed)


def next_run_at(now: datetime) -> datetime:
    boundary = now.replace(minute=0, second=0, microsecond=0)
    run_at = boundary + timedelta(seconds=RUN_BUFFER_SECONDS)
    if now >= run_at:
        run_at += timedelta(hours=1)
    return run_at


async def main() -> None:
    if not DISCORD_WEBHOOK_URL:
        raise RuntimeError("DISCORD_WEBHOOK_URL is not set. Keep it in the VPS .env file.")
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
    connector = aiohttp.TCPConnector(limit=REQUEST_CONCURRENCY + 5)
    stop = asyncio.Event()
    try:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, stop.set)
            except NotImplementedError:
                pass
    except RuntimeError:
        pass
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        log.info("SHORT WATCH v%s started (hourly +%ds JST)", VERSION, RUN_BUFFER_SECONDS)
        while not stop.is_set():
            run_at = next_run_at(datetime.now(UTC))
            seconds = max(0, (run_at - datetime.now(UTC)).total_seconds())
            log.info("Next scan at %s JST", run_at.astimezone(JST).isoformat())
            try:
                await asyncio.wait_for(stop.wait(), timeout=seconds)
                break
            except asyncio.TimeoutError:
                pass
            # Anchor the target candle to the intended schedule (not scan finish).
            boundary_ms = int(run_at.replace(minute=0, second=0, microsecond=0).timestamp() * 1000)
            try:
                await scan(session, boundary_ms)
            except Exception:
                log.exception("Scan failed; will retry at next scheduled hour")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
