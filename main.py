import asyncio
import logging
import os
import signal
import time
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Optional

import aiohttp
from dotenv import load_dotenv

load_dotenv()

BASE_URL = os.getenv("BINANCE_FAPI_BASE_URL", "https://fapi.binance.com")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
INTERVAL_SECONDS = int(os.getenv("INTERVAL_SECONDS", "3600"))
TOP_N = int(os.getenv("TOP_N", "10"))
REQUEST_CONCURRENCY = int(os.getenv("REQUEST_CONCURRENCY", "8"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "20"))
MIN_VOLUME_USD = float(os.getenv("MIN_VOLUME_USD", "1000000"))
VOLUME_RATIO_THRESHOLD = float(os.getenv("VOLUME_RATIO_THRESHOLD", "3"))
PRICE_CHANGE_THRESHOLD_PCT = float(os.getenv("PRICE_CHANGE_THRESHOLD_PCT", "5"))
OI_CHANGE_THRESHOLD_PCT = float(os.getenv("OI_CHANGE_THRESHOLD_PCT", "10"))
QUOTE_ASSET = os.getenv("QUOTE_ASSET", "USDT")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("binance-pump-alert")

shutdown_event = asyncio.Event()


@dataclass(frozen=True)
class Candidate:
    symbol: str
    score: float
    volume_ratio: float
    oi_change_pct: float
    price_change_pct: float
    max_volume_12h: float
    avg_volume_12h: float
    avg_volume_72h: float
    latest_close: float


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def jst_str(dt: Optional[datetime] = None) -> str:
    dt = dt or utc_now()
    return dt.astimezone(timezone(timedelta(hours=9))).strftime("%Y-%m-%d %H:%M:%S JST")


def setup_signal_handlers() -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, shutdown_event.set)
        except NotImplementedError:
            pass


async def request_json(session: aiohttp.ClientSession, path: str, params: Optional[dict[str, Any]] = None) -> Any:
    url = f"{BASE_URL}{path}"
    last_error: Optional[Exception] = None
    for attempt in range(3):
        try:
            async with session.get(url, params=params, timeout=REQUEST_TIMEOUT) as resp:
                text = await resp.text()
                if resp.status == 429:
                    await asyncio.sleep(2 + attempt * 3)
                    continue
                if resp.status >= 400:
                    raise RuntimeError(f"HTTP {resp.status}: {text[:300]}")
                return await resp.json()
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            await asyncio.sleep(1 + attempt * 2)
    raise RuntimeError(f"request failed: {path} params={params} error={last_error}")


async def fetch_symbols(session: aiohttp.ClientSession) -> list[str]:
    data = await request_json(session, "/fapi/v1/exchangeInfo")
    symbols: list[str] = []
    for item in data.get("symbols", []):
        if (
            item.get("contractType") == "PERPETUAL"
            and item.get("quoteAsset") == QUOTE_ASSET
            and item.get("status") == "TRADING"
        ):
            symbols.append(item["symbol"])
    return sorted(symbols)


def parse_closed_klines(raw_klines: list[list[Any]]) -> list[list[Any]]:
    now_ms = int(time.time() * 1000)
    closed = [k for k in raw_klines if int(k[6]) < now_ms]
    return closed


async def fetch_klines(session: aiohttp.ClientSession, symbol: str) -> list[list[Any]]:
    raw = await request_json(
        session,
        "/fapi/v1/klines",
        {"symbol": symbol, "interval": "1h", "limit": 100},
    )
    return parse_closed_klines(raw)


async def fetch_oi_hist(session: aiohttp.ClientSession, symbol: str) -> list[dict[str, Any]]:
    # pair is symbol such as BTCUSDT. contractType=PERPETUAL limits to perpetual futures.
    data = await request_json(
        session,
        "/futures/data/openInterestHist",
        {"pair": symbol, "contractType": "PERPETUAL", "period": "1h", "limit": 30},
    )
    if not isinstance(data, list):
        return []
    return data


def evaluate_symbol(symbol: str, klines: list[list[Any]], oi_hist: list[dict[str, Any]]) -> Optional[Candidate]:
    if len(klines) < 85:
        return None

    # Binance kline fields: [open_time, open, high, low, close, volume, close_time, quote_volume, ...]
    last_84 = klines[-84:]
    recent_12 = last_84[-12:]
    previous_72 = last_84[:-12]

    quote_vol_recent_12 = [float(k[7]) for k in recent_12]
    quote_vol_prev_72 = [float(k[7]) for k in previous_72]

    max_volume_12h = max(quote_vol_recent_12)
    if max_volume_12h <= MIN_VOLUME_USD:
        return None

    avg_volume_12h = sum(quote_vol_recent_12) / len(quote_vol_recent_12)
    avg_volume_72h = sum(quote_vol_prev_72) / len(quote_vol_prev_72)
    if avg_volume_72h <= 0:
        return None

    volume_ratio = avg_volume_12h / avg_volume_72h
    if volume_ratio < VOLUME_RATIO_THRESHOLD:
        return None

    latest_close = float(klines[-1][4])
    close_24h_ago = float(klines[-25][4])
    if close_24h_ago <= 0:
        return None
    price_change_pct = (latest_close / close_24h_ago - 1) * 100
    if price_change_pct < PRICE_CHANGE_THRESHOLD_PCT:
        return None

    if len(oi_hist) < 13:
        return None
    try:
        latest_oi = float(oi_hist[-1]["sumOpenInterest"])
        oi_12h_ago = float(oi_hist[-13]["sumOpenInterest"])
    except (KeyError, ValueError, TypeError):
        return None
    if oi_12h_ago <= 0:
        return None
    oi_change_pct = (latest_oi / oi_12h_ago - 1) * 100
    if oi_change_pct < OI_CHANGE_THRESHOLD_PCT:
        return None

    score = volume_ratio * oi_change_pct
    return Candidate(
        symbol=symbol,
        score=score,
        volume_ratio=volume_ratio,
        oi_change_pct=oi_change_pct,
        price_change_pct=price_change_pct,
        max_volume_12h=max_volume_12h,
        avg_volume_12h=avg_volume_12h,
        avg_volume_72h=avg_volume_72h,
        latest_close=latest_close,
    )


async def analyze_one(session: aiohttp.ClientSession, sem: asyncio.Semaphore, symbol: str) -> Optional[Candidate]:
    async with sem:
        try:
            klines_task = asyncio.create_task(fetch_klines(session, symbol))
            oi_task = asyncio.create_task(fetch_oi_hist(session, symbol))
            klines, oi_hist = await asyncio.gather(klines_task, oi_task)
            return evaluate_symbol(symbol, klines, oi_hist)
        except Exception as exc:  # noqa: BLE001
            logger.warning("%s skipped: %s", symbol, exc)
            return None


def build_discord_message(candidates: list[Candidate], total_count: int) -> str:
    top = candidates[:TOP_N]
    lines = [
        "🚨 Binance USDT無期限先物 PUMP候補ランキング",
        f"判定時刻: {jst_str()}",
        f"条件通過銘柄数: {total_count}件",
        f"表示: 上位{len(top)}件 / 最大{TOP_N}件",
        "",
        "条件: Vol>1M(直近12本内) / Vol異常度>=3 / 24h価格+5%以上 / 12h OI+10%以上",
        "",
    ]
    if not top:
        lines.append("条件通過銘柄はありません。")
        return "\n".join(lines)

    for idx, c in enumerate(top, start=1):
        lines.extend(
            [
                f"{idx}. {c.symbol}",
                f"スコア: {c.score:.2f}",
                f"出来高異常度: {c.volume_ratio:.2f}倍",
                f"OI増加率: +{c.oi_change_pct:.2f}%",
                f"価格上昇率(24h): +{c.price_change_pct:.2f}%",
                f"直近12h最大出来高: ${c.max_volume_12h:,.0f}",
                "",
            ]
        )
    return "\n".join(lines).strip()


async def post_discord(session: aiohttp.ClientSession, content: str) -> None:
    if not DISCORD_WEBHOOK_URL:
        logger.info("DISCORD_WEBHOOK_URL is empty. Message:\n%s", content)
        return

    # Discord content limit is 2000 chars. Split conservatively.
    chunks: list[str] = []
    current = ""
    for block in content.split("\n\n"):
        candidate = block if not current else current + "\n\n" + block
        if len(candidate) > 1800:
            chunks.append(current)
            current = block
        else:
            current = candidate
    if current:
        chunks.append(current)

    for chunk in chunks:
        async with session.post(DISCORD_WEBHOOK_URL, json={"content": chunk}, timeout=REQUEST_TIMEOUT) as resp:
            text = await resp.text()
            if resp.status >= 300:
                raise RuntimeError(f"Discord webhook failed HTTP {resp.status}: {text[:300]}")


async def run_once(session: aiohttp.ClientSession) -> list[Candidate]:
    symbols = await fetch_symbols(session)
    logger.info("symbols=%s", len(symbols))
    sem = asyncio.Semaphore(REQUEST_CONCURRENCY)
    tasks = [analyze_one(session, sem, symbol) for symbol in symbols]
    results = await asyncio.gather(*tasks)
    candidates = sorted([c for c in results if c is not None], key=lambda x: x.score, reverse=True)
    message = build_discord_message(candidates, len(candidates))
    await post_discord(session, message)
    logger.info("candidates=%s top=%s", len(candidates), [c.symbol for c in candidates[:TOP_N]])
    return candidates


def seconds_until_next_hour(offset_seconds: int = 15) -> float:
    now = utc_now()
    next_hour = (now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1))
    target = next_hour + timedelta(seconds=offset_seconds)
    return max(1.0, (target - now).total_seconds())


async def main() -> None:
    setup_signal_handlers()
    connector = aiohttp.TCPConnector(limit=REQUEST_CONCURRENCY * 2, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector) as session:
        logger.info("bot started at %s", jst_str())
        while not shutdown_event.is_set():
            try:
                await run_once(session)
            except Exception as exc:  # noqa: BLE001
                logger.exception("run failed: %s", exc)
                if DISCORD_WEBHOOK_URL:
                    try:
                        await post_discord(session, f"⚠️ Binance PUMP通知Botでエラー発生: {exc}")
                    except Exception:
                        logger.exception("failed to post error to discord")

            sleep_sec = seconds_until_next_hour() if INTERVAL_SECONDS == 3600 else INTERVAL_SECONDS
            logger.info("next run in %.0f seconds", sleep_sec)
            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=sleep_sec)
            except asyncio.TimeoutError:
                pass
        logger.info("bot stopped")


if __name__ == "__main__":
    asyncio.run(main())
