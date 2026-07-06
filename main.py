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

QUOTE_ASSET = os.getenv("QUOTE_ASSET", "USDT")
REQUEST_CONCURRENCY = int(os.getenv("REQUEST_CONCURRENCY", "8"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "20"))
TOP_N = int(os.getenv("TOP_N", "20"))

# 毎時00分 + 20秒頃に判定
RUN_BUFFER_SECONDS = int(os.getenv("RUN_BUFFER_SECONDS", "20"))

# 条件
MIN_VOLUME_USD = float(os.getenv("MIN_VOLUME_USD", "1000000"))
MIN_BULLISH_CANDLE_PCT = float(os.getenv("MIN_BULLISH_CANDLE_PCT", "3"))

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
)

logger = logging.getLogger("binance-ma-break-alert")
shutdown_event = asyncio.Event()

MA_PERIODS = [5, 10, 30, 50, 100]


@dataclass(frozen=True)
class Candidate:
    symbol: str
    latest_close: float
    latest_open: float
    bullish_pct: float
    quote_volume: float
    ma5: float
    ma10: float
    ma30: float
    ma50: float
    ma100: float


@dataclass
class PassCounts:
    total_symbols: int = 0
    enough_kline: int = 0
    past_11_below_any_ma: int = 0
    latest_above_all_ma: int = 0
    latest_volume_1m: int = 0
    latest_bullish_4pct: int = 0
    final_pass: int = 0
    kline_failed: int = 0


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
        {"symbol": symbol, "interval": "1h", "limit": 150},
    )
    return parse_closed_klines(raw)


def sma(closes: list[float], end_index: int, period: int) -> Optional[float]:
    start = end_index - period + 1
    if start < 0:
        return None
    values = closes[start : end_index + 1]
    if len(values) != period:
        return None
    return sum(values) / period


def get_mas(closes: list[float], index: int) -> Optional[dict[int, float]]:
    result: dict[int, float] = {}
    for period in MA_PERIODS:
        value = sma(closes, index, period)
        if value is None:
            return None
        result[period] = value
    return result


def evaluate_symbol(
    symbol: str,
    klines: list[list[Any]],
    counts: PassCounts,
) -> Optional[Candidate]:
    # 最新確定足を除いた過去11本のうち、最も古い足でも100MAが必要。
    # よって最低111本必要。
    if len(klines) < 111:
        return None

    counts.enough_kline += 1

    closes = [float(k[4]) for k in klines]

    latest_index = len(klines) - 1
    latest = klines[latest_index]

    latest_open = float(latest[1])
    latest_close = float(latest[4])
    latest_quote_volume = float(latest[7])

    latest_mas = get_mas(closes, latest_index)
    if latest_mas is None:
        return None

    # 条件1:
    # 最新確定足を除く過去11本すべてで、
    # 終値が 5MA / 10MA / 30MA / 50MA / 100MA のいずれかより下。
    past_11_indices = range(latest_index - 11, latest_index)

    for i in past_11_indices:
        mas = get_mas(closes, i)
        if mas is None:
            return None

        close = closes[i]

        # 「いずれかよりも下」= close < 少なくとも1本のMA
        below_any_ma = any(close < ma for ma in mas.values())

        if not below_any_ma:
            return None

    counts.past_11_below_any_ma += 1

    # 条件2:
    # 直近の1時間足の終値が全MAより上。
    latest_above_all_ma = all(latest_close > ma for ma in latest_mas.values())
    if not latest_above_all_ma:
        return None

    counts.latest_above_all_ma += 1

    # 条件3:
    # 直近の1時間足の出来高が1M USDT以上。
    if latest_quote_volume < MIN_VOLUME_USD:
        return None

    counts.latest_volume_1m += 1

    # 条件4:
    # 直近の1時間足が4%以上の陽線。
    if latest_open <= 0:
        return None

    bullish_pct = (latest_close / latest_open - 1.0) * 100

    if bullish_pct < MIN_BULLISH_CANDLE_PCT:
        return None

    counts.latest_bullish_4pct += 1
    counts.final_pass += 1

    return Candidate(
        symbol=symbol,
        latest_close=latest_close,
        latest_open=latest_open,
        bullish_pct=bullish_pct,
        quote_volume=latest_quote_volume,
        ma5=latest_mas[5],
        ma10=latest_mas[10],
        ma30=latest_mas[30],
        ma50=latest_mas[50],
        ma100=latest_mas[100],
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

        return evaluate_symbol(symbol, klines, counts)


def build_discord_message(candidates: list[Candidate], counts: PassCounts) -> str:
    now = jst_now().strftime("%Y-%m-%d %H:%M:%S JST")
    top = candidates[:TOP_N]

    lines: list[str] = [
        "Binance USDT無期限先物 MA上抜けPUMP候補",
        f"判定時刻: {now}",
        f"最終通過: {counts.final_pass}件 / 対象: {counts.total_symbols}件",
        "",
        "条件:",
        "・判断足: 1時間足の確定足",
        "・最新確定足を除く過去11本すべてで、終値が5/10/30/50/100MAのいずれかより下",
        f"・最新確定足の終値が全MAより上",
        f"・最新確定足の出来高が{format_usd(MIN_VOLUME_USD)}以上",
        f"・最新確定足が+{MIN_BULLISH_CANDLE_PCT:g}%以上の陽線",
        "",
        "条件通過状況:",
        f"過去11本 below any MA: {counts.past_11_below_any_ma}件",
        f"最新足 close > all MA: {counts.latest_above_all_ma}件",
        f"最新足 volume >= 1M: {counts.latest_volume_1m}件",
        f"最新足 bullish >= 4%: {counts.latest_bullish_4pct}件",
    ]

    if counts.kline_failed:
        lines.append(f"取得失敗: kline={counts.kline_failed}件")

    lines.append("")

    for i, c in enumerate(top, start=1):
        lines.extend(
            [
                f"{i}. {c.symbol}",
                f"陽線率: +{c.bullish_pct:.2f}%",
                f"出来高: {format_usd(c.quote_volume)}",
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
        async with session.post(
            DISCORD_WEBHOOK_URL,
            json={"content": chunk},
            timeout=REQUEST_TIMEOUT,
        ) as response:
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

        # 強い陽線かつ出来高が大きいものを上位表示
        candidates.sort(
            key=lambda x: (x.bullish_pct, x.quote_volume),
            reverse=True,
        )

        logger.info(
            "pass counts: total=%d enough_kline=%d past11=%d above_all=%d vol=%d bullish=%d final=%d failed=%d",
            counts.total_symbols,
            counts.enough_kline,
            counts.past_11_below_any_ma,
            counts.latest_above_all_ma,
            counts.latest_volume_1m,
            counts.latest_bullish_4pct,
            counts.final_pass,
            counts.kline_failed,
        )

        logger.info("matched=%s", [c.symbol for c in candidates[:TOP_N]])

        # 条件に一致しなければ通知しない
        if candidates:
            message = build_discord_message(candidates, counts)
            await send_discord(session, message)
        else:
            logger.info("no matched symbols. Discord notification skipped.")

        return candidates, counts


def seconds_until_next_hour(buffer_seconds: int = RUN_BUFFER_SECONDS) -> int:
    now = jst_now()
    next_hour = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
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
