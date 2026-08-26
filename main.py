import asyncio
import json
import logging
import os
import signal
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

import aiohttp
from dotenv import load_dotenv


# =========================================================
# Binance Abnormal Move Alert Bot
# Version: 2.0.0
# =========================================================

load_dotenv()

VERSION = "2.1.0"

BASE_URL = os.getenv("BINANCE_FAPI_BASE_URL", "https://fapi.binance.com")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
QUOTE_ASSET = os.getenv("QUOTE_ASSET", "USDT")

REQUEST_CONCURRENCY = int(os.getenv("REQUEST_CONCURRENCY", "8"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "20"))
REQUEST_RETRIES = int(os.getenv("REQUEST_RETRIES", "3"))

# 毎時00分 + 20秒頃に判定する。
RUN_BUFFER_SECONDS = int(os.getenv("RUN_BUFFER_SECONDS", "20"))

# 今回の異常値動き判定条件。
# 旧Botの .env に MIN_VOLUME_USD 等が残っていても影響しないよう、
# 新しい環境変数名を使用する。
ABNORMAL_MIN_VOLUME_USDT = float(
    os.getenv("ABNORMAL_MIN_VOLUME_USDT", "10000000")
)
ABNORMAL_VOLUME_MULTIPLIER = float(
    os.getenv("ABNORMAL_VOLUME_MULTIPLIER", "2")
)
ABNORMAL_PRICE_RISE_PCT = float(
    os.getenv("ABNORMAL_PRICE_RISE_PCT", "10")
)

# 通知済み銘柄はGit管理ディレクトリ外に保存する。
# systemd の User=ubuntu でも書き込める場所を Path.home() から作る。
DEFAULT_STATE_FILE = (
    Path.home()
    / ".local"
    / "state"
    / "binance-pump-alert-bot"
    / "notification_state.json"
)
STATE_FILE = Path(os.getenv("STATE_FILE", str(DEFAULT_STATE_FILE))).expanduser()

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("binance-abnormal-move-alert")

JST = timezone(timedelta(hours=9))
shutdown_event = asyncio.Event()


@dataclass(frozen=True)
class Candidate:
    symbol: str
    quote_volume: float
    avg_quote_volume_24: float
    volume_multiple: float
    price_rise_pct: float
    latest_open: float
    latest_close: float
    close_time_ms: int


@dataclass(frozen=True)
class MarketSnapshot:
    # 判定対象となる最新確定1時間足の終了時点に対応するOI。
    oi_notional_usdt: Optional[float]
    oi_timestamp_ms: Optional[int]
    # 資金調達率は通知時点で取得できる直近の lastFundingRate。
    funding_rate: Optional[float]


@dataclass
class PassCounts:
    total_symbols: int = 0
    enough_closed_klines: int = 0
    bullish: int = 0
    volume_10m: int = 0
    volume_2x: int = 0
    price_rise_10pct: int = 0
    highest_close_24: int = 0
    final_pass: int = 0
    already_notified: int = 0
    kline_failed: int = 0
    market_data_failed: int = 0
    discord_failed: int = 0


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def jst_now() -> datetime:
    return utc_now().astimezone(JST)


def notification_day_key(now: Optional[datetime] = None) -> str:
    """
    JST 09:00 を1日の起点とする。

    例:
      2026-08-26 08:59 JST -> 2026-08-25
      2026-08-26 09:00 JST -> 2026-08-26
    """
    current = now or jst_now()
    shifted = current - timedelta(hours=9)
    return shifted.date().isoformat()


def format_usd(value: Optional[float]) -> str:
    if value is None:
        return "取得失敗"
    if value >= 1_000_000_000:
        return f"${value / 1_000_000_000:.2f}B"
    if value >= 1_000_000:
        return f"${value / 1_000_000:.2f}M"
    if value >= 1_000:
        return f"${value / 1_000:.2f}K"
    return f"${value:.2f}"


def format_funding_rate(rate: Optional[float]) -> str:
    if rate is None:
        return "取得失敗"
    return f"{rate * 100:+.4f}%"


def load_notification_state() -> set[str]:
    current_key = notification_day_key()

    if not STATE_FILE.exists():
        return set()

    try:
        with STATE_FILE.open("r", encoding="utf-8") as f:
            data = json.load(f)

        if data.get("notification_day") != current_key:
            logger.info(
                "notification day changed: old=%s new=%s",
                data.get("notification_day"),
                current_key,
            )
            return set()

        symbols = data.get("symbols", [])
        if not isinstance(symbols, list):
            raise ValueError("state symbols is not a list")

        return {str(symbol) for symbol in symbols}

    except Exception as exc:
        logger.warning("failed to load state file %s: %s", STATE_FILE, exc)
        return set()


def save_notification_state(notified_symbols: set[str]) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)

    data = {
        "notification_day": notification_day_key(),
        "symbols": sorted(notified_symbols),
        "updated_at_jst": jst_now().isoformat(),
    }

    temp_file = STATE_FILE.with_name(STATE_FILE.name + ".tmp")

    with temp_file.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())

    os.replace(temp_file, STATE_FILE)


async def request_json(
    session: aiohttp.ClientSession,
    path: str,
    params: Optional[dict[str, Any]] = None,
) -> Any:
    url = BASE_URL + path
    last_error: Optional[Exception] = None

    for attempt in range(1, REQUEST_RETRIES + 1):
        try:
            async with session.get(
                url,
                params=params,
                timeout=REQUEST_TIMEOUT,
            ) as response:
                text = await response.text()

                if response.status == 200:
                    return json.loads(text)

                error = RuntimeError(
                    f"{path} params={params} error=HTTP {response.status}: "
                    f"{text[:300]}"
                )

                if response.status == 429 or response.status >= 500:
                    last_error = error
                    retry_after = response.headers.get("Retry-After")
                    if retry_after:
                        try:
                            wait_seconds = max(float(retry_after), 1.0)
                        except ValueError:
                            wait_seconds = float(attempt)
                    else:
                        wait_seconds = float(attempt)

                    logger.warning(
                        "request retry %d/%d: %s; wait=%.1fs",
                        attempt,
                        REQUEST_RETRIES,
                        error,
                        wait_seconds,
                    )
                    await asyncio.sleep(wait_seconds)
                    continue

                raise error

        except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError) as exc:
            last_error = exc
            if attempt >= REQUEST_RETRIES:
                break

            wait_seconds = float(attempt)
            logger.warning(
                "request retry %d/%d: %s params=%s error=%s; wait=%.1fs",
                attempt,
                REQUEST_RETRIES,
                path,
                params,
                exc,
                wait_seconds,
            )
            await asyncio.sleep(wait_seconds)

    raise RuntimeError(
        f"request failed after {REQUEST_RETRIES} attempts: {path} params={params}: "
        f"{last_error}"
    )


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
    """
    Binance kline の close time が現在時刻より前の足だけ残す。
    形成中の未確定1時間足はここで完全に除外する。
    """
    now_ms = int(time.time() * 1000)
    return [kline for kline in raw_klines if int(kline[6]) < now_ms]


async def fetch_klines(
    session: aiohttp.ClientSession,
    symbol: str,
) -> list[list[Any]]:
    # 必要なのは「判定対象の最新確定足 + その直前24本」。
    # APIレスポンスに未確定足が1本含まれても足りるよう26本取得する。
    raw = await request_json(
        session,
        "/fapi/v1/klines",
        {
            "symbol": symbol,
            "interval": "1h",
            "limit": 26,
        },
    )
    return parse_closed_klines(raw)


def evaluate_symbol(
    symbol: str,
    klines: list[list[Any]],
    counts: PassCounts,
) -> Optional[Candidate]:
    # 最新確定足1本 + その直前24本 = 最低25本必要。
    if len(klines) < 25:
        return None

    counts.enough_closed_klines += 1

    latest = klines[-1]
    previous_24 = klines[-25:-1]

    latest_open = float(latest[1])
    latest_close = float(latest[4])
    latest_quote_volume = float(latest[7])
    close_time_ms = int(latest[6])

    if latest_open <= 0:
        return None

    # ---------------------------------------------------------
    # 条件1: 陽線
    # ---------------------------------------------------------
    if latest_close <= latest_open:
        return None
    counts.bullish += 1

    # ---------------------------------------------------------
    # 条件2: 最新確定1時間足の出来高が10M USDT以上
    # kline[7] = Quote asset volume (USDT建て出来高)
    # ---------------------------------------------------------
    if latest_quote_volume < ABNORMAL_MIN_VOLUME_USDT:
        return None
    counts.volume_10m += 1

    # ---------------------------------------------------------
    # 条件3: 最新確定足の出来高が、
    #         「その直前24本の確定足平均」の2倍以上
    # 判定対象の最新確定足自身は平均に含めない。
    # 形成中の未確定足も parse_closed_klines で除外済み。
    # ---------------------------------------------------------
    previous_24_volumes = [float(kline[7]) for kline in previous_24]
    avg_quote_volume_24 = sum(previous_24_volumes) / 24.0

    if avg_quote_volume_24 <= 0:
        return None

    volume_multiple = latest_quote_volume / avg_quote_volume_24

    if volume_multiple < ABNORMAL_VOLUME_MULTIPLIER:
        return None
    counts.volume_2x += 1

    # ---------------------------------------------------------
    # 条件4: 最新確定1時間足が、始値→終値で+10%以上上昇
    # ---------------------------------------------------------
    price_rise_pct = (latest_close / latest_open - 1.0) * 100.0

    if price_rise_pct < ABNORMAL_PRICE_RISE_PCT:
        return None
    counts.price_rise_10pct += 1

    # ---------------------------------------------------------
    # 条件5: 最新確定足の終値が、直近24本の確定足終値で最高
    # 「直近24本」には判定対象の最新確定足を含める。
    # ---------------------------------------------------------
    recent_24_closes = [float(kline[4]) for kline in klines[-24:]]

    if latest_close < max(recent_24_closes):
        return None
    counts.highest_close_24 += 1

    counts.final_pass += 1

    return Candidate(
        symbol=symbol,
        quote_volume=latest_quote_volume,
        avg_quote_volume_24=avg_quote_volume_24,
        volume_multiple=volume_multiple,
        price_rise_pct=price_rise_pct,
        latest_open=latest_open,
        latest_close=latest_close,
        close_time_ms=close_time_ms,
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


async def fetch_closed_hour_oi(
    session: aiohttp.ClientSession,
    symbol: str,
    close_time_ms: int,
) -> tuple[Optional[float], Optional[int]]:
    """
    判定対象の最新確定1時間足の終了時点に対応するOIを取得する。

    Binance Kline の close_time_ms は通常「次の正時 - 1ms」なので、
    +1ms した時刻を対象1時間足の終了境界として扱う。

    /futures/data/openInterestHist の period=1h を使い、
    sumOpenInterestValue（USDT建てOI）を採用する。

    1時間古いデータを誤って採用しないよう、対象境界との時刻差が
    5分以内のレコードだけを有効とする。データ反映が少し遅れた場合に
    備えて3回まで短時間リトライする。
    """
    target_ms = close_time_ms + 1
    tolerance_ms = 5 * 60 * 1000

    params = {
        "symbol": symbol,
        "period": "1h",
        "limit": 5,
        "startTime": target_ms - 2 * 60 * 60 * 1000,
        "endTime": target_ms + tolerance_ms,
    }

    for data_attempt in range(1, 4):
        data = await request_json(
            session,
            "/futures/data/openInterestHist",
            params,
        )

        if isinstance(data, list) and data:
            valid_rows: list[tuple[int, float]] = []

            for row in data:
                try:
                    timestamp_ms = int(row["timestamp"])
                    oi_value = float(row["sumOpenInterestValue"])
                except (KeyError, TypeError, ValueError):
                    continue

                if abs(timestamp_ms - target_ms) <= tolerance_ms:
                    valid_rows.append((timestamp_ms, oi_value))

            if valid_rows:
                # 対象の1時間足終了境界に最も近いOIスナップショットを採用。
                timestamp_ms, oi_value = min(
                    valid_rows,
                    key=lambda item: abs(item[0] - target_ms),
                )
                return oi_value, timestamp_ms

        if data_attempt < 3:
            logger.info(
                "%s OI history for closed hour not available yet; retry %d/3",
                symbol,
                data_attempt + 1,
            )
            await asyncio.sleep(2)

    logger.warning(
        "%s no matching 1h OI history near target=%d",
        symbol,
        target_ms,
    )
    return None, None


async def fetch_market_snapshot(
    session: aiohttp.ClientSession,
    symbol: str,
    close_time_ms: int,
) -> MarketSnapshot:
    """
    条件通過銘柄だけ通知用データを追加取得する。

    OI:
      判定対象の最新確定1時間足の終了時点に対応する
      1時間OI履歴の sumOpenInterestValue を使用する。

    資金調達率:
      /fapi/v1/premiumIndex が返す直近の lastFundingRate を使用する。
    """
    oi_task = fetch_closed_hour_oi(
        session,
        symbol,
        close_time_ms,
    )
    premium_task = request_json(
        session,
        "/fapi/v1/premiumIndex",
        {"symbol": symbol},
    )

    oi_result, premium_result = await asyncio.gather(
        oi_task,
        premium_task,
        return_exceptions=True,
    )

    oi_notional_usdt: Optional[float] = None
    oi_timestamp_ms: Optional[int] = None
    funding_rate: Optional[float] = None

    if isinstance(oi_result, Exception):
        logger.warning(
            "%s closed-hour OI fetch failed: %s",
            symbol,
            oi_result,
        )
    else:
        oi_notional_usdt, oi_timestamp_ms = oi_result

    if isinstance(premium_result, Exception):
        logger.warning(
            "%s premiumIndex fetch failed: %s",
            symbol,
            premium_result,
        )
    else:
        try:
            funding_rate = float(premium_result["lastFundingRate"])
        except (KeyError, TypeError, ValueError) as exc:
            logger.warning(
                "%s invalid lastFundingRate response: %s",
                symbol,
                exc,
            )

    return MarketSnapshot(
        oi_notional_usdt=oi_notional_usdt,
        oi_timestamp_ms=oi_timestamp_ms,
        funding_rate=funding_rate,
    )


def build_discord_message(
    candidate: Candidate,
    snapshot: MarketSnapshot,
) -> str:
    # ユーザー指定の通知内容だけを簡潔に表示する。
    return "\n".join(
        [
            "🚨 Binance 異常値動き検出",
            f"**{candidate.symbol}**",
            f"1時間足出来高: {format_usd(candidate.quote_volume)}",
            f"OI: {format_usd(snapshot.oi_notional_usdt)}",
            f"資金調達率: {format_funding_rate(snapshot.funding_rate)}",
            f"価格上昇率: {candidate.price_rise_pct:+.2f}%",
        ]
    )


async def send_discord(
    session: aiohttp.ClientSession,
    message: str,
) -> None:
    if not DISCORD_WEBHOOK_URL:
        raise RuntimeError("DISCORD_WEBHOOK_URL is empty")

    last_error: Optional[Exception] = None

    for attempt in range(1, REQUEST_RETRIES + 1):
        try:
            async with session.post(
                DISCORD_WEBHOOK_URL,
                json={"content": message},
                timeout=REQUEST_TIMEOUT,
            ) as response:
                text = await response.text()

                if response.status < 300:
                    return

                error = RuntimeError(
                    f"Discord error=HTTP {response.status}: {text[:300]}"
                )
                last_error = error

                if response.status == 429 or response.status >= 500:
                    wait_seconds = float(attempt)

                    if response.status == 429:
                        try:
                            body = json.loads(text)
                            wait_seconds = max(
                                float(body.get("retry_after", wait_seconds)),
                                1.0,
                            )
                        except (json.JSONDecodeError, TypeError, ValueError):
                            pass

                    logger.warning(
                        "Discord retry %d/%d: %s; wait=%.1fs",
                        attempt,
                        REQUEST_RETRIES,
                        error,
                        wait_seconds,
                    )
                    await asyncio.sleep(wait_seconds)
                    continue

                raise error

        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            last_error = exc
            if attempt >= REQUEST_RETRIES:
                break

            wait_seconds = float(attempt)
            logger.warning(
                "Discord retry %d/%d: %s; wait=%.1fs",
                attempt,
                REQUEST_RETRIES,
                exc,
                wait_seconds,
            )
            await asyncio.sleep(wait_seconds)

    raise RuntimeError(
        f"Discord notification failed after {REQUEST_RETRIES} attempts: {last_error}"
    )


async def scan_once() -> tuple[list[Candidate], PassCounts]:
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT + 10)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        symbols = await fetch_symbols(session)
        counts = PassCounts(total_symbols=len(symbols))
        notified_symbols = load_notification_state()
        state_day = notification_day_key()

        logger.info(
            "scan start: version=%s symbols=%d notification_day=%s already_notified=%d",
            VERSION,
            len(symbols),
            notification_day_key(),
            len(notified_symbols),
        )

        sem = asyncio.Semaphore(REQUEST_CONCURRENCY)
        tasks = [
            evaluate_one_symbol(session, sem, symbol, counts)
            for symbol in symbols
        ]
        results = await asyncio.gather(*tasks)

        candidates = [result for result in results if result is not None]

        # 値動きが大きい順、同率なら出来高倍率が大きい順。
        candidates.sort(
            key=lambda item: (item.price_rise_pct, item.volume_multiple),
            reverse=True,
        )

        logger.info(
            "pass counts: total=%d enough=%d bullish=%d vol10m=%d vol2x=%d "
            "rise10pct=%d highest24=%d final=%d failed=%d",
            counts.total_symbols,
            counts.enough_closed_klines,
            counts.bullish,
            counts.volume_10m,
            counts.volume_2x,
            counts.price_rise_10pct,
            counts.highest_close_24,
            counts.final_pass,
            counts.kline_failed,
        )

        if not candidates:
            logger.info("no matched symbols. Discord notification skipped.")
            return candidates, counts

        logger.info("matched=%s", [candidate.symbol for candidate in candidates])

        for candidate in candidates:
            # スキャン中にJST 09:00を跨いだ場合も、新しい通知日に切り替える。
            current_day = notification_day_key()
            if current_day != state_day:
                logger.info(
                    "notification day rolled over during scan: old=%s new=%s",
                    state_day,
                    current_day,
                )
                state_day = current_day
                notified_symbols.clear()

            # JST 09:00起点の同一通知日で既に通知済みなら送らない。
            if candidate.symbol in notified_symbols:
                counts.already_notified += 1
                logger.info(
                    "%s already notified in notification_day=%s; skipped",
                    candidate.symbol,
                    notification_day_key(),
                )
                continue

            try:
                snapshot = await fetch_market_snapshot(
                    session,
                    candidate.symbol,
                    candidate.close_time_ms,
                )

                if (
                    snapshot.oi_notional_usdt is None
                    or snapshot.funding_rate is None
                ):
                    counts.market_data_failed += 1

                message = build_discord_message(candidate, snapshot)
                await send_discord(session, message)

                # 送信処理中に09:00を跨いだ場合は、送信完了時点の通知日に合わせる。
                sent_day = notification_day_key()
                if sent_day != state_day:
                    logger.info(
                        "notification day rolled over during send: old=%s new=%s",
                        state_day,
                        sent_day,
                    )
                    state_day = sent_day
                    notified_symbols.clear()

                # Discord送信成功後にだけ通知済みとして保存する。
                notified_symbols.add(candidate.symbol)
                save_notification_state(notified_symbols)

                logger.info(
                    "%s Discord notified and state saved: day=%s",
                    candidate.symbol,
                    notification_day_key(),
                )

            except Exception as exc:
                counts.discord_failed += 1
                logger.exception(
                    "%s notification failed; state was NOT updated: %s",
                    candidate.symbol,
                    exc,
                )

        logger.info(
            "notification summary: matched=%d already_notified=%d "
            "market_data_partial=%d discord_failed=%d",
            len(candidates),
            counts.already_notified,
            counts.market_data_failed,
            counts.discord_failed,
        )

        return candidates, counts


def seconds_until_next_hour(
    buffer_seconds: int = RUN_BUFFER_SECONDS,
) -> int:
    """
    毎回「次の正時」を計算して待つため、
    sleep時間を1時間ずつ積み重ねる方式の時刻ずれは起こさない。
    """
    now = jst_now()
    next_hour = now.replace(
        minute=0,
        second=0,
        microsecond=0,
    ) + timedelta(hours=1)

    seconds = int((next_hour - now).total_seconds()) + buffer_seconds
    return max(1, seconds)


async def main_loop() -> None:
    logger.info(
        "bot started: version=%s time=%s state_file=%s",
        VERSION,
        jst_now().strftime("%Y-%m-%d %H:%M:%S JST"),
        STATE_FILE,
    )

    while not shutdown_event.is_set():
        started = time.time()

        try:
            await scan_once()
        except Exception:
            logger.exception("scan failed")

        elapsed = time.time() - started
        sleep_seconds = seconds_until_next_hour()

        logger.info(
            "scan elapsed %.1f sec. next run in %d seconds",
            elapsed,
            sleep_seconds,
        )

        try:
            await asyncio.wait_for(
                shutdown_event.wait(),
                timeout=sleep_seconds,
            )
        except asyncio.TimeoutError:
            pass


def handle_shutdown() -> None:
    if not shutdown_event.is_set():
        logger.info("shutdown signal received")
        shutdown_event.set()


async def async_main() -> None:
    loop = asyncio.get_running_loop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, handle_shutdown)
        except NotImplementedError:
            # Windows等ではadd_signal_handlerが未対応の場合がある。
            pass

    await main_loop()


if __name__ == "__main__":
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        pass
