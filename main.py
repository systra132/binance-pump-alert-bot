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


@dataclass
class PassCounts:
    total_symbols: int = 0
    enough_kline: int = 0
    enough_oi: int = 0
    condition1_volume_1m: int = 0
    condition2_volume_ratio: int = 0
    condition3_price_change: int = 0
    condition4_oi_change: int = 0
    final_pass: int = 0
    kline_failed: int = 0
    oi_failed: int = 0


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def jst_now() -> datetime:
    return utc_now().astimezone(timezone(timedelta(hours=9)))


def format_usd(value: float) -> str:
    if value >= 1_000_000_000:
        return f"${value / 1_000_000_000:.2f}B"
    if value >= 1_000_000:
        return f"${value / 1_000_000:.2f}M"
    if value >= 1_000:
        return f"${value / 1_000:.2f}K"
    return f"${value:.0f}"


async def request_json(
    session: aiohttp.ClientSession,
    path: str,
    params: Optional[dict[str, Any]] = None,
) -> Any:
    url = BASE_URL + path
    async with session.get(url, params=params, timeout=REQUEST_TIMEOUT) as response:
        text = await response.text()
        if response.status != 200:
            raise RuntimeError(
                f"{path} params={params} error=HTTP {response.status}: {text[:300]}"
            )
        return await response.json()


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
    return [k for k in raw_klines if int(k[6]) < now_ms]


async def fetch_klines(session: aiohttp.ClientSession, symbol: str) -> list[list[Any]]:
    raw = await request_json(
        session,
        "/fapi/v1/klines",
        {"symbol": symbol, "interval": "1h", "limit": 100},
    )
    return parse_closed_klines(raw)


async def fetch_oi_hist(session: aiohttp.ClientSession, symbol: str) -> list[dict[str, Any]]:
    # Binance current spec requires symbol, period, limit.
    # Do not use old pair / contractType parameters.
    data = await request_json(
        session,
        "/futures/data/openInterestHist",
        {"symbol": symbol, "period": "1h", "limit": 30},
    )
    if not isinstance(data, list):
        return []
    return data


def evaluate_symbol(
    symbol: str,
    klines: list[list[Any]],
    oi_hist: list[dict[str, Any]],
    counts: PassCounts,
) -> Optional[Candidate]:
    if len(klines) < 85:
        return None
    counts.enough_kline += 1

    if len(oi_hist) < 13:
        return None
    counts.enough_oi += 1

    last_84 = klines[-84:]
    recent_12 = last_84[-12:]
    previous_72 = last_84[:-12]

    quote_vol_recent_12 = [float(k[7]) for k in recent_12]
    quote_vol_prev_72 = [float(k[7]) for k in previous_72]

    max_volume_12h = max(quote_vol_recent_12)
    avg_volume_12h = sum(quote_vol_recent_12) / len(quote_vol_recent_12)
    avg_volume_72h = sum(quote_vol_prev_72) / len(quote_vol_prev_72)

    if max_volume_12h <= MIN_VOLUME_USD:
        return None
    counts.condition1_volume_1m += 1

    if avg_volume_72h <= 0:
        return None
    volume_ratio = avg_volume_12h / avg_volume_72h
    if volume_ratio < VOLUME_RATIO_THRESHOLD:
        return None
    counts.condition2_volume_ratio += 1

    latest_close = float(klines[-1][4])
    close_24h_ago = float(klines[-25][4])
    if close_24h_ago <= 0:
        return None

    price_change_pct = (latest_close / close_24h_ago - 1.0) * 100
    if price_change_pct < PRICE_CHANGE_THRESHOLD_PCT:
        return None
    counts.condition3_price_change += 1

    try:
        latest_oi = float(oi_hist[-1]["sumOpenInterest"])
        oi_12h_ago = float(oi_hist[-13]["sumOpenInterest"])
    except (KeyError, TypeError, ValueError):
        return None

    if oi_12h_ago <= 0:
        return None

    oi_change_pct = (latest_oi / oi_12h_ago - 1.0) * 100
    if oi_change_pct < OI_CHANGE_THRESHOLD_PCT:
        return None
    counts.condition4_oi_change += 1

    score = volume_ratio * oi_change_pct
    counts.final_pass += 1

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


async def evaluate_one_symbol(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    symbol: str,
    counts: PassCounts,
) -> Optional[Candidate]:
    async with sem:
        try:
            klines = await fetch_klines(session, symbol)
        except Exception as exc:
            counts.kline_failed += 1
            logger.warning("%s skipped: kline request failed: %s", symbol, exc)
            return None

        try:
            oi_hist = await fetch_oi_hist(session, symbol)
        except Exception as exc:
            counts.oi_failed += 1
            logger.warning("%s skipped: OI request failed: %s", symbol, exc)
            return None

        return evaluate_symbol(symbol, klines, oi_hist, counts)


def build_discord_message(candidates: list[Candidate], counts: PassCounts) -> str:
    now = jst_now().strftime("%Y-%m-%d %H:%M:%S JST")
    top = candidates[:TOP_N]

    lines: list[str] = [
        "🚨 Binance USDT無期限先物 PUMP候補ランキング",
        f"判定時刻: {now}",
        f"対象銘柄数: {counts.total_symbols}件",
        "",
        "条件通過状況:",
        f"条件1 Vol>1M: {counts.condition1_volume_1m}件",
        f"条件2 Vol異常度>=3: {counts.condition2_volume_ratio}件",
        f"条件3 24h価格+5%以上: {counts.condition3_price_change}件",
        f"条件4 12h OI+10%以上: {counts.condition4_oi_change}件",
        f"最終通過: {counts.final_pass}件",
        f"表示: 上位{len(top)}件 / 最大{TOP_N}件",
        "",
        f"条件: Vol>1M(直近12本内) / Vol異常度>={VOLUME_RATIO_THRESHOLD:g} / "
        f"24h価格+{PRICE_CHANGE_THRESHOLD_PCT:g}%以上 / 12h OI+{OI_CHANGE_THRESHOLD_PCT:g}%以上",
    ]

    if counts.kline_failed or counts.oi_failed:
        lines.extend(
            [
                "",
                f"取得失敗: kline={counts.kline_failed}件 / OI={counts.oi_failed}件",
            ]
        )

    if not top:
        lines.extend(["", "条件通過銘柄はありません。"])
        return "\n".join(lines)

    lines.append("")
    for i, c in enumerate(top, start=1):
        lines.extend(
            [
                f"{i}. {c.symbol}",
                f"スコア: {c.score:.2f}",
                f"出来高異常度: {c.volume_ratio:.2f}倍",
                f"OI増加率: +{c.oi_change_pct:.2f}%",
                f"価格上昇率: +{c.price_change_pct:.2f}%",
                f"直近12h最大出来高: {format_usd(c.max_volume_12h)}",
                "",
            ]
        )

    return "\n".join(lines).strip()


async def send_discord(session: aiohttp.ClientSession, message: str) -> None:
    if not DISCORD_WEBHOOK_URL:
        logger.warning("DISCORD_WEBHOOK_URL is empty. Discord notification skipped.")
        return

    chunks: list[str] = []
    current = ""
    for line in message.splitlines():
        if len(current) + len(line) + 1 > 1900:
            chunks.append(current)
            current = line
        else:
            current = current + "\n" + line if current else line
    if current:
        chunks.append(current)

    for chunk in chunks:
        async with session.post(DISCORD_WEBHOOK_URL, json={"content": chunk}, timeout=REQUEST_TIMEOUT) as response:
            text = await response.text()
            if response.status >= 300:
                raise RuntimeError(f"Discord error=HTTP {response.status}: {text[:300]}")


async def scan_once() -> tuple[list[Candidate], PassCounts]:
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT + 10)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        symbols = await fetch_symbols(session)
        counts = PassCounts(total_symbols=len(symbols))
        logger.info("symbols=%d", len(symbols))

        sem = asyncio.Semaphore(REQUEST_CONCURRENCY)
        tasks = [evaluate_one_symbol(session, sem, sym, counts) for sym in symbols]
        results = await asyncio.gather(*tasks)

        candidates = [r for r in results if r is not None]
        candidates.sort(key=lambda x: x.score, reverse=True)

        logger.info(
            "pass counts: total=%d enough_kline=%d enough_oi=%d c1=%d c2=%d c3=%d c4=%d final=%d "
            "kline_failed=%d oi_failed=%d",
            counts.total_symbols,
            counts.enough_kline,
            counts.enough_oi,
            counts.condition1_volume_1m,
            counts.condition2_volume_ratio,
            counts.condition3_price_change,
            counts.condition4_oi_change,
            counts.final_pass,
            counts.kline_failed,
            counts.oi_failed,
        )
        logger.info("top=%s", [c.symbol for c in candidates[:TOP_N]])

        message = build_discord_message(candidates, counts)
        await send_discord(session, message)

        return candidates, counts


def seconds_until_next_hour(buffer_seconds: int = 20) -> int:
    now = jst_now()
    next_hour = (now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1))
    seconds = int((next_hour - now).total_seconds()) + buffer_seconds
    return max(60, seconds)


async def main_loop() -> None:
    logger.info("bot started at %s", jst_now().strftime("%Y-%m-%d %H:%M:%S JST"))

    while not shutdown_event.is_set():
        started = time.time()
        try:
            await scan_once()
        except Exception:
            logger.exception("scan failed")

        elapsed = time.time() - started
        sleep_seconds = seconds_until_next_hour()
        logger.info("scan elapsed %.1f sec. next run in %d seconds", elapsed, sleep_seconds)

        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=sleep_seconds)
        except asyncio.TimeoutError:
            pass

    logger.info("bot stopped")


def handle_signal() -> None:
    shutdown_event.set()


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, handle_signal)
        except NotImplementedError:
            pass

    try:
        loop.run_until_complete(main_loop())
    finally:
        loop.close()
