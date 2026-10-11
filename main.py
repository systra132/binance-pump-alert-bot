import asyncio
import json
import logging
import os
import signal
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import aiohttp
from dotenv import load_dotenv


# =========================================================
# Binance Daily Pump Filter Bot
# Version: 3.1.0
#
# 07:30 JST
#   プレ通知
#   形成中の日足を、その時点の仮終値・累積出来高として判定
#   条件一致がなければ通知しない
#
# 09:00 JST
#   本通知
#   直前に確定した日足で判定
#   条件一致がなくても「条件一致なし」と通知
#
# 判定条件
#   1. 日足出来高 >= 20M USDT
#   2. 日足出来高 >= 前日日足出来高 × 2
#   3. 日足終値 > 直前30本すべての日足終値
#   4. 日足終値 > SMA5 / 10 / 30 / 50 / 100
#   5. SMA100を計算できない銘柄は除外
#
# 通知内容
#   ・銘柄名
#   ・日足出来高
#   ・出来高変化率（前日比）
#
# 出来高
#   Binance kline[7] の Quote Asset Volume（USDT建て）
#
# 出来高変化率
#   (対象日足出来高 / 前日日足出来高 - 1) × 100
#
# MA
#   判定対象の日足自身を含めて計算
# =========================================================


load_dotenv()

VERSION = "3.1.0"

BASE_URL = os.getenv(
    "BINANCE_FAPI_BASE_URL",
    "https://fapi.binance.com",
)

DISCORD_WEBHOOK_URL = os.getenv(
    "DISCORD_WEBHOOK_URL",
    "",
).strip()

QUOTE_ASSET = os.getenv(
    "QUOTE_ASSET",
    "USDT",
)

REQUEST_CONCURRENCY = int(
    os.getenv("REQUEST_CONCURRENCY", "8")
)

REQUEST_TIMEOUT = int(
    os.getenv("REQUEST_TIMEOUT", "20")
)

REQUEST_RETRIES = int(
    os.getenv("REQUEST_RETRIES", "3")
)

# 7:30 / 9:00 ちょうどではなく、
# デフォルトでは20秒後に実行する。
#
# 例:
#   07:30:20 JST
#   09:00:20 JST
#
# 9時の確定直後にBinance側のデータ反映を
# 安全に取得するための余裕。
DAILY_RUN_BUFFER_SECONDS = int(
    os.getenv("DAILY_RUN_BUFFER_SECONDS", "20")
)


# =========================================================
# 判定条件
# =========================================================

MIN_DAILY_VOLUME_USDT = float(
    os.getenv(
        "DAILY_MIN_VOLUME_USDT",
        "20000000",
    )
)

VOLUME_MULTIPLIER = float(
    os.getenv(
        "DAILY_VOLUME_MULTIPLIER",
        "2",
    )
)

MA_PERIODS = (5, 10, 30, 50, 100)

RECENT_HIGH_PERIOD = 30

KLINE_LIMIT = 105


# =========================================================
# Logging
# =========================================================

LOG_LEVEL = os.getenv(
    "LOG_LEVEL",
    "INFO",
).upper()

logging.basicConfig(
    level=getattr(
        logging,
        LOG_LEVEL,
        logging.INFO,
    ),
    format="%(asctime)s [%(levelname)s] %(message)s",
)

logger = logging.getLogger(
    "binance-daily-pump-filter"
)


# =========================================================
# Timezone
# =========================================================

JST = timezone(
    timedelta(hours=9)
)

shutdown_event = asyncio.Event()


# =========================================================
# Data classes
# =========================================================

@dataclass(frozen=True)
class Candidate:
    symbol: str
    quote_volume: float
    close: float
    previous_quote_volume: float
    volume_multiple: float
    volume_change_pct: float
    open_time_ms: int
    close_time_ms: int


@dataclass
class PassCounts:
    total_symbols: int = 0

    enough_klines: int = 0

    volume_20m: int = 0

    volume_2x: int = 0

    highest_close_30: int = 0

    above_all_ma: int = 0

    final_pass: int = 0

    kline_failed: int = 0


# =========================================================
# Time helpers
# =========================================================

def utc_now() -> datetime:
    return datetime.now(
        timezone.utc
    )


def jst_now() -> datetime:
    return utc_now().astimezone(
        JST
    )


# =========================================================
# Formatting
# =========================================================

def format_volume(
    value: float,
) -> str:

    if value >= 1_000_000_000:
        return (
            f"${value / 1_000_000_000:.2f}B"
        )

    if value >= 1_000_000:
        return (
            f"${value / 1_000_000:.2f}M"
        )

    if value >= 1_000:
        return (
            f"${value / 1_000:.2f}K"
        )

    return f"${value:.2f}"


def format_change_pct(
    value: float,
) -> str:

    return f"{value:+.1f}%"


# =========================================================
# HTTP
# =========================================================

async def request_json(
    session: aiohttp.ClientSession,
    path: str,
    params: Optional[
        dict[str, Any]
    ] = None,
) -> Any:

    url = BASE_URL + path

    last_error: Optional[
        Exception
    ] = None

    for attempt in range(
        1,
        REQUEST_RETRIES + 1,
    ):

        try:
            async with session.get(
                url,
                params=params,
                timeout=REQUEST_TIMEOUT,
            ) as response:

                text = (
                    await response.text()
                )

                if response.status == 200:
                    return json.loads(
                        text
                    )

                error = RuntimeError(
                    f"{path} "
                    f"params={params} "
                    f"HTTP {response.status}: "
                    f"{text[:300]}"
                )

                last_error = error

                if (
                    response.status == 429
                    or response.status >= 500
                ):

                    retry_after = (
                        response.headers.get(
                            "Retry-After"
                        )
                    )

                    if retry_after:
                        try:
                            wait_seconds = max(
                                float(
                                    retry_after
                                ),
                                1.0,
                            )
                        except ValueError:
                            wait_seconds = float(
                                attempt
                            )
                    else:
                        wait_seconds = float(
                            attempt
                        )

                    logger.warning(
                        "request retry "
                        "%d/%d: %s; "
                        "wait=%.1fs",
                        attempt,
                        REQUEST_RETRIES,
                        error,
                        wait_seconds,
                    )

                    await asyncio.sleep(
                        wait_seconds
                    )

                    continue

                raise error

        except (
            aiohttp.ClientError,
            asyncio.TimeoutError,
            json.JSONDecodeError,
        ) as exc:

            last_error = exc

            if (
                attempt
                >= REQUEST_RETRIES
            ):
                break

            wait_seconds = float(
                attempt
            )

            logger.warning(
                "request retry "
                "%d/%d: "
                "%s params=%s "
                "error=%s; "
                "wait=%.1fs",
                attempt,
                REQUEST_RETRIES,
                path,
                params,
                exc,
                wait_seconds,
            )

            await asyncio.sleep(
                wait_seconds
            )

    raise RuntimeError(
        "request failed after "
        f"{REQUEST_RETRIES} attempts: "
        f"{path} params={params}: "
        f"{last_error}"
    )


# =========================================================
# Binance symbols
# =========================================================

async def fetch_symbols(
    session: aiohttp.ClientSession,
) -> list[str]:

    data = await request_json(
        session,
        "/fapi/v1/exchangeInfo",
    )

    symbols: list[str] = []

    for item in data.get(
        "symbols",
        [],
    ):

        if (
            item.get("contractType")
            == "PERPETUAL"
            and item.get("quoteAsset")
            == QUOTE_ASSET
            and item.get("status")
            == "TRADING"
        ):

            symbol = item.get(
                "symbol"
            )

            if symbol:
                symbols.append(
                    symbol
                )

    return sorted(
        symbols
    )


# =========================================================
# Daily klines
# =========================================================

async def fetch_daily_klines(
    session: aiohttp.ClientSession,
    symbol: str,
) -> list[list[Any]]:

    data = await request_json(
        session,
        "/fapi/v1/klines",
        {
            "symbol": symbol,
            "interval": "1d",
            "limit": KLINE_LIMIT,
        },
    )

    if not isinstance(
        data,
        list,
    ):
        raise RuntimeError(
            f"{symbol}: "
            "invalid kline response"
        )

    return data


# =========================================================
# Select target candle
# =========================================================

def select_pre_klines(
    klines: list[list[Any]],
) -> list[list[Any]]:
    """
    07:30 JST プレ通知用。

    Binanceの日足は
    UTC 00:00 = JST 09:00
    で開始する。

    07:30時点では最後の1本が
    まだ形成中なので、それを
    判定対象に含める。

    終値:
        07:30時点の現在値相当

    出来高:
        07:30時点までの
        当日日足累積Quote Volume
    """

    if not klines:
        return []

    now_ms = int(
        datetime.now(
            timezone.utc
        ).timestamp() * 1000
    )

    latest = klines[-1]

    # kline[6] = close time
    if int(latest[6]) < now_ms:
        logger.warning(
            "pre scan: latest kline "
            "is already closed"
        )
        return []

    return klines


def select_main_klines(
    klines: list[list[Any]],
) -> list[list[Any]]:
    """
    09:00 JST 本通知用。

    現在時刻よりclose timeが
    前にある確定日足のみを残す。

    09:00を過ぎるとBinance APIには
    新しい形成中の日足が含まれるため、
    それを除外する。
    """

    now_ms = int(
        datetime.now(
            timezone.utc
        ).timestamp() * 1000
    )

    closed = [
        kline
        for kline in klines
        if int(kline[6]) < now_ms
    ]

    return closed


# =========================================================
# Moving average
# =========================================================

def sma(
    values: list[float],
    period: int,
) -> Optional[float]:

    if len(values) < period:
        return None

    return (
        sum(
            values[-period:]
        )
        / period
    )


# =========================================================
# Symbol evaluation
# =========================================================

def evaluate_symbol(
    symbol: str,
    klines: list[list[Any]],
    mode: str,
    counts: PassCounts,
) -> Optional[Candidate]:

    if mode == "pre":
        usable = select_pre_klines(
            klines
        )

    elif mode == "main":
        usable = select_main_klines(
            klines
        )

    else:
        raise ValueError(
            f"invalid mode: {mode}"
        )

    # SMA100を作るため、
    # 判定対象の日足を含めて最低100本必要。
    if len(usable) < 100:
        return None

    counts.enough_klines += 1

    target = usable[-1]
    previous = usable[-2]

    target_close = float(
        target[4]
    )

    target_quote_volume = float(
        target[7]
    )

    previous_quote_volume = float(
        previous[7]
    )

    if (
        target_close <= 0
        or previous_quote_volume <= 0
    ):
        return None


    # -----------------------------------------------------
    # 条件1
    # 対象日足のUSDT建て出来高 >= 20M USDT
    # -----------------------------------------------------

    if (
        target_quote_volume
        < MIN_DAILY_VOLUME_USDT
    ):
        return None

    counts.volume_20m += 1


    # -----------------------------------------------------
    # 出来高倍率
    # -----------------------------------------------------

    volume_multiple = (
        target_quote_volume
        / previous_quote_volume
    )


    # -----------------------------------------------------
    # 出来高変化率
    #
    # 例:
    # 前日 20M
    # 当日 50M
    #
    # 50 / 20 = 2.5倍
    # (2.5 - 1) × 100 = +150%
    # -----------------------------------------------------

    volume_change_pct = (
        volume_multiple - 1.0
    ) * 100.0


    # -----------------------------------------------------
    # 条件2
    # 対象日足出来高 >= 前日日足出来高 × 2
    # -----------------------------------------------------

    if (
        volume_multiple
        < VOLUME_MULTIPLIER
    ):
        return None

    counts.volume_2x += 1


    # -----------------------------------------------------
    # 条件3
    #
    # 対象日足の終値が、
    # 対象日足を除いた直前30本
    # すべての終値より高い。
    #
    # target_close
    # > max(previous 30 closes)
    # -----------------------------------------------------

    previous_30 = (
        usable[
            -(RECENT_HIGH_PERIOD + 1):-1
        ]
    )

    if (
        len(previous_30)
        < RECENT_HIGH_PERIOD
    ):
        return None

    previous_30_closes = [
        float(kline[4])
        for kline in previous_30
    ]

    if (
        target_close
        <= max(
            previous_30_closes
        )
    ):
        return None

    counts.highest_close_30 += 1


    # -----------------------------------------------------
    # 条件4
    #
    # 対象日足終値が
    # SMA5 / 10 / 30 / 50 / 100
    # のすべてより上。
    #
    # MA計算には対象日足自身を含む。
    # -----------------------------------------------------

    closes = [
        float(kline[4])
        for kline in usable
    ]

    moving_averages: list[
        float
    ] = []

    for period in MA_PERIODS:

        value = sma(
            closes,
            period,
        )

        if value is None:
            return None

        moving_averages.append(
            value
        )

    if not all(
        target_close > ma
        for ma in moving_averages
    ):
        return None

    counts.above_all_ma += 1

    counts.final_pass += 1

    return Candidate(
        symbol=symbol,
        quote_volume=target_quote_volume,
        close=target_close,
        previous_quote_volume=previous_quote_volume,
        volume_multiple=volume_multiple,
        volume_change_pct=volume_change_pct,
        open_time_ms=int(
            target[0]
        ),
        close_time_ms=int(
            target[6]
        ),
    )


# =========================================================
# Concurrent evaluation
# =========================================================

async def evaluate_one_symbol(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    symbol: str,
    mode: str,
    counts: PassCounts,
) -> Optional[Candidate]:

    async with semaphore:

        try:
            klines = (
                await fetch_daily_klines(
                    session,
                    symbol,
                )
            )

        except Exception as exc:

            counts.kline_failed += 1

            logger.warning(
                "%s skipped: "
                "daily kline request "
                "failed: %s",
                symbol,
                exc,
            )

            return None

    try:
        return evaluate_symbol(
            symbol,
            klines,
            mode,
            counts,
        )

    except (
        TypeError,
        ValueError,
        IndexError,
    ) as exc:

        logger.warning(
            "%s skipped: "
            "invalid kline data: %s",
            symbol,
            exc,
        )

        return None


# =========================================================
# Discord
# =========================================================

async def send_discord(
    session: aiohttp.ClientSession,
    message: str,
) -> None:

    if not DISCORD_WEBHOOK_URL:
        raise RuntimeError(
            "DISCORD_WEBHOOK_URL is empty"
        )

    last_error: Optional[
        Exception
    ] = None

    for attempt in range(
        1,
        REQUEST_RETRIES + 1,
    ):

        try:
            async with session.post(
                DISCORD_WEBHOOK_URL,
                json={
                    "content": message
                },
                timeout=REQUEST_TIMEOUT,
            ) as response:

                text = (
                    await response.text()
                )

                if response.status < 300:
                    return

                error = RuntimeError(
                    "Discord "
                    f"HTTP {response.status}: "
                    f"{text[:300]}"
                )

                last_error = error

                if (
                    response.status == 429
                    or response.status >= 500
                ):

                    wait_seconds = float(
                        attempt
                    )

                    if (
                        response.status
                        == 429
                    ):

                        try:
                            body = json.loads(
                                text
                            )

                            wait_seconds = max(
                                float(
                                    body.get(
                                        "retry_after",
                                        wait_seconds,
                                    )
                                ),
                                1.0,
                            )

                        except (
                            json.JSONDecodeError,
                            TypeError,
                            ValueError,
                        ):
                            pass

                    logger.warning(
                        "Discord retry "
                        "%d/%d; wait=%.1fs",
                        attempt,
                        REQUEST_RETRIES,
                        wait_seconds,
                    )

                    await asyncio.sleep(
                        wait_seconds
                    )

                    continue

                raise error

        except (
            aiohttp.ClientError,
            asyncio.TimeoutError,
        ) as exc:

            last_error = exc

            if (
                attempt
                >= REQUEST_RETRIES
            ):
                break

            wait_seconds = float(
                attempt
            )

            await asyncio.sleep(
                wait_seconds
            )

    raise RuntimeError(
        "Discord notification "
        "failed after "
        f"{REQUEST_RETRIES} attempts: "
        f"{last_error}"
    )


# =========================================================
# Discord message
# =========================================================

def build_message_chunks(
    candidates: list[Candidate],
    mode: str,
    scan_time: datetime,
) -> list[str]:

    if mode == "pre":
        title = (
            "🟡【プレ通知】日足PUMP候補"
        )

    else:
        title = (
            "🔴【本通知】日足PUMP候補"
        )

    header = "\n".join(
        [
            title,
            scan_time.strftime(
                "%Y/%m/%d %H:%M JST"
            ),
            "",
        ]
    )

    # 本通知のみ、
    # 一致0件でも通知する。
    if not candidates:

        if mode == "main":
            return [
                header
                + "条件一致なし"
            ]

        return []

    lines: list[str] = []

    for index, candidate in enumerate(
        candidates,
        start=1,
    ):

        lines.append(
            f"{index}. "
            f"**{candidate.symbol}**"
        )

        lines.append(
            "日足出来高: "
            f"{format_volume(candidate.quote_volume)}"
        )

        lines.append(
            "出来高変化率: "
            f"{format_change_pct(candidate.volume_change_pct)}"
        )

        lines.append("")

    # Discord content上限対策
    chunks: list[str] = []

    current = header

    for line in lines:

        addition = (
            line + "\n"
        )

        if (
            len(current)
            + len(addition)
            > 1900
        ):

            chunks.append(
                current.rstrip()
            )

            current = (
                title
                + "（続き）\n\n"
                + addition
            )

        else:
            current += addition

    if current.strip():
        chunks.append(
            current.rstrip()
        )

    return chunks


# =========================================================
# Scan
# =========================================================

async def scan_once(
    mode: str,
) -> tuple[
    list[Candidate],
    PassCounts,
]:

    scan_time = jst_now()

    timeout = aiohttp.ClientTimeout(
        total=REQUEST_TIMEOUT + 10
    )

    async with aiohttp.ClientSession(
        timeout=timeout
    ) as session:

        symbols = await fetch_symbols(
            session
        )

        counts = PassCounts(
            total_symbols=len(
                symbols
            )
        )

        logger.info(
            "scan start: "
            "version=%s "
            "mode=%s "
            "time=%s "
            "symbols=%d",
            VERSION,
            mode,
            scan_time.strftime(
                "%Y-%m-%d "
                "%H:%M:%S JST"
            ),
            len(symbols),
        )

        semaphore = asyncio.Semaphore(
            REQUEST_CONCURRENCY
        )

        tasks = [
            evaluate_one_symbol(
                session,
                semaphore,
                symbol,
                mode,
                counts,
            )
            for symbol in symbols
        ]

        results = await asyncio.gather(
            *tasks
        )

        candidates = [
            result
            for result in results
            if result is not None
        ]

        # 出来高の降順
        candidates.sort(
            key=lambda item:
                item.quote_volume,
            reverse=True,
        )

        logger.info(
            "pass counts: "
            "total=%d "
            "enough100=%d "
            "vol20m=%d "
            "vol2x=%d "
            "highest30=%d "
            "aboveMA=%d "
            "final=%d "
            "failed=%d",
            counts.total_symbols,
            counts.enough_klines,
            counts.volume_20m,
            counts.volume_2x,
            counts.highest_close_30,
            counts.above_all_ma,
            counts.final_pass,
            counts.kline_failed,
        )

        if candidates:

            logger.info(
                "matched symbols: %s",
                [
                    candidate.symbol
                    for candidate
                    in candidates
                ],
            )

        else:

            logger.info(
                "matched symbols: none"
            )

        messages = (
            build_message_chunks(
                candidates,
                mode,
                scan_time,
            )
        )

        # プレ通知:
        # 0件ならmessages=[]なので送信しない。
        #
        # 本通知:
        # 0件でも「条件一致なし」を送信。
        for message in messages:

            try:
                await send_discord(
                    session,
                    message,
                )

            except Exception:
                logger.exception(
                    "Discord notification "
                    "failed"
                )

        logger.info(
            "scan finished: "
            "mode=%s "
            "matched=%d",
            mode,
            len(candidates),
        )

        return candidates, counts


# =========================================================
# Scheduler
# =========================================================

def next_scheduled_run(
    now: Optional[
        datetime
    ] = None,
) -> tuple[
    datetime,
    str,
]:

    current = (
        now
        or jst_now()
    )

    buffer = timedelta(
        seconds=(
            DAILY_RUN_BUFFER_SECONDS
        )
    )

    # 今日 07:30 + buffer
    pre_time = current.replace(
        hour=7,
        minute=30,
        second=0,
        microsecond=0,
    ) + buffer

    # 今日 09:00 + buffer
    main_time = current.replace(
        hour=9,
        minute=0,
        second=0,
        microsecond=0,
    ) + buffer

    possible: list[
        tuple[datetime, str]
    ] = []

    if pre_time > current:
        possible.append(
            (
                pre_time,
                "pre",
            )
        )

    if main_time > current:
        possible.append(
            (
                main_time,
                "main",
            )
        )

    if possible:

        possible.sort(
            key=lambda item:
                item[0]
        )

        return possible[0]

    # 今日の2回が終わっている場合は
    # 翌日の07:30へ。
    tomorrow = (
        current
        + timedelta(days=1)
    )

    next_pre = tomorrow.replace(
        hour=7,
        minute=30,
        second=0,
        microsecond=0,
    ) + buffer

    return (
        next_pre,
        "pre",
    )


async def wait_until(
    target: datetime,
) -> bool:
    """
    targetまで待機する。

    shutdown_eventが先にセットされた場合は
    Falseを返す。
    """

    while (
        not shutdown_event.is_set()
    ):

        seconds = (
            target
            - jst_now()
        ).total_seconds()

        if seconds <= 0:
            return True

        timeout = min(
            seconds,
            60.0,
        )

        try:
            await asyncio.wait_for(
                shutdown_event.wait(),
                timeout=timeout,
            )

        except asyncio.TimeoutError:
            continue

    return False


# =========================================================
# Main loop
# =========================================================

async def main_loop() -> None:

    logger.info(
        "bot started: "
        "version=%s "
        "time=%s",
        VERSION,
        jst_now().strftime(
            "%Y-%m-%d "
            "%H:%M:%S JST"
        ),
    )

    logger.info(
        "schedule: "
        "pre=07:30 JST "
        "main=09:00 JST "
        "buffer=%d sec",
        DAILY_RUN_BUFFER_SECONDS,
    )

    while (
        not shutdown_event.is_set()
    ):

        target_time, mode = (
            next_scheduled_run()
        )

        logger.info(
            "next run: "
            "mode=%s "
            "time=%s",
            mode,
            target_time.strftime(
                "%Y-%m-%d "
                "%H:%M:%S JST"
            ),
        )

        should_run = await wait_until(
            target_time
        )

        if not should_run:
            break

        if shutdown_event.is_set():
            break

        try:
            await scan_once(
                mode
            )

        except Exception:
            logger.exception(
                "scan failed: mode=%s",
                mode,
            )

        try:
            await asyncio.wait_for(
                shutdown_event.wait(),
                timeout=1.0,
            )

        except asyncio.TimeoutError:
            pass


# =========================================================
# Shutdown
# =========================================================

def handle_shutdown() -> None:

    if (
        not shutdown_event.is_set()
    ):

        logger.info(
            "shutdown signal received"
        )

        shutdown_event.set()


async def async_main() -> None:

    loop = (
        asyncio.get_running_loop()
    )

    for sig in (
        signal.SIGINT,
        signal.SIGTERM,
    ):

        try:
            loop.add_signal_handler(
                sig,
                handle_shutdown,
            )

        except NotImplementedError:
            pass

    await main_loop()


# =========================================================
# Entry point
# =========================================================

if __name__ == "__main__":

    try:
        asyncio.run(
            async_main()
        )

    except KeyboardInterrupt:
        pass
