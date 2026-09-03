#!/usr/bin/env python3
"""BES real-time Bithumb KRW capital-flow scanner (no orders, no sizing)."""

from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass, field
import json
import os
from pathlib import Path
import time
import uuid
from typing import Any

from aiohttp import ClientSession, WSMsgType, web


REST_API = "https://api.bithumb.com/v1"
WS_API = "wss://ws-api.bithumb.com/websocket/v1"
DATA_DIR = Path(os.environ.get("BES_DATA_DIR", "data-live"))
STATE_FILE = DATA_DIR / "scanner_state.json"
EVENT_FILE = DATA_DIR / "events.jsonl"
STATIC_DIR = Path(__file__).with_name("static")
STABLE = {"USDT", "USDC", "DAI", "TUSD", "FDUSD", "USDE", "PYUSD"}
STATE_CONFIRM_MS = {
    "수급 유입": 10_000,
    "상승 가능": 15_000,
    "관찰 유지": 12_000,
    "수급 이탈": 3_000,
    "일반 감시": 20_000,
    "과열·추격 금지": 0,
}
ACTIVE_STATE_LOCK_MS = 30_000
OTHER_STATE_LOCK_MS = 10_000
CHART_WINDOW_MS = 3 * 60_000
CHART_BUCKET_MS = 5_000
CANDIDATE_HOLD_MS = 3 * 60_000
STATE_STRENGTH = {"일반 감시": 0, "관찰 유지": 1, "수급 유입": 2, "상승 가능": 3}


@dataclass
class Tick:
    ts: int
    price: float
    volume: float
    side: str

    @property
    def value(self) -> float:
        return self.price * self.volume


@dataclass
class Coin:
    market: str
    baseline_per_second: float
    ticks: deque[Tick] = field(default_factory=lambda: deque(maxlen=30_000))
    best_bid: float = 0.0
    best_ask: float = 0.0
    bid_depth: float = 0.0
    ask_depth: float = 0.0
    state: str = "일반 감시"
    score: int = 0
    first_seen_at: int | None = None
    first_seen_price: float | None = None
    peak_price: float | None = None
    trough_price: float | None = None
    last_change_at: int = 0
    state_since: int = 0
    pending_state: str | None = None
    pending_since: int = 0
    locked_until: int = 0
    candidate_confirmed_at: int | None = None
    candidate_hold_until: int = 0
    live_visible: bool = False
    live_first_seen_at: int | None = None
    live_first_seen_price: float | None = None
    live_peak_price: float | None = None
    live_trough_price: float | None = None
    live_weak_since: int | None = None
    flow_stage: str = ""
    flow_first_at: int | None = None
    flow_first_price: float | None = None
    flow_first_strength: float = 0.0
    flow_second_at: int | None = None
    flow_second_strength: float = 0.0
    flow_peak_price: float | None = None
    flow_trough_price: float | None = None
    flow_invalidation_price: float | None = None
    flow_hold_until: int = 0
    flow_exit_reason: str = ""


def pct(new: float, old: float) -> float:
    return (new / old - 1.0) * 100.0 if old > 0 else 0.0


def clip(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def metrics(coin: Coin, now: int, seconds: int) -> dict[str, float]:
    cutoff = now - seconds * 1000
    rows = [tick for tick in coin.ticks if tick.ts >= cutoff and tick.volume > 0]
    if not rows:
        return {"count": 0.0, "value": 0.0, "buy_ratio": 0.5, "change": 0.0}
    value = sum(tick.value for tick in rows)
    buy_value = sum(tick.value for tick in rows if tick.side == "BID")
    return {
        "count": float(len(rows)),
        "value": value,
        "buy_ratio": buy_value / value if value else 0.5,
        "change": pct(rows[-1].price, rows[0].price),
    }


def features(coin: Coin, now: int) -> dict[str, float]:
    m10 = metrics(coin, now, 10)
    m30 = metrics(coin, now, 30)
    m60 = metrics(coin, now, 60)
    m180 = metrics(coin, now, 180)
    baseline = max(coin.baseline_per_second, 1.0)
    depth = coin.bid_depth + coin.ask_depth
    price = coin.ticks[-1].price if coin.ticks else 0.0
    return {
        "price": price,
        "trade_count_10s": m10["count"],
        "flow_10s": m10["value"] / (baseline * 10.0),
        "buy_10s": m10["buy_ratio"],
        "trade_count_30s": m30["count"],
        "flow_30s": m30["value"] / (baseline * 30.0),
        "flow_1m": m60["value"] / (baseline * 60.0),
        "flow_3m": m180["value"] / (baseline * 180.0),
        "buy_30s": m30["buy_ratio"],
        "buy_1m": m60["buy_ratio"],
        "change_30s": m30["change"],
        "change_1m": m60["change"],
        "change_3m": m180["change"],
        "book_buy": coin.bid_depth / depth if depth else 0.5,
        "spread": pct(coin.best_ask, coin.best_bid) if coin.best_bid and coin.best_ask else 0.0,
    }


def score_signal(row: dict[str, float], flow_percentile: float) -> int:
    if row["trade_count_30s"] < 5 or row["price"] <= 0:
        return 0
    score = 0.0
    score += clip((row["flow_30s"] - 1.2) * 13.0, 0.0, 30.0)
    score += clip((row["flow_1m"] - 1.0) * 10.0, 0.0, 18.0)
    score += clip((flow_percentile - 70.0) * 0.6, 0.0, 18.0)
    score += clip((row["buy_30s"] - 0.50) * 80.0, 0.0, 14.0)
    score += clip((row["book_buy"] - 0.50) * 50.0, 0.0, 8.0)
    if 0.05 <= row["change_30s"] <= 2.5:
        score += 6.0
    if 0.10 <= row["change_1m"] <= 4.0:
        score += 6.0
    if row["spread"] > 0.8:
        score -= 20.0
    return round(clip(score, 0.0, 100.0))


def desired_state(coin: Coin, row: dict[str, float], flow_percentile: float) -> str:
    coin.score = score_signal(row, flow_percentile)
    overheat = row["change_30s"] >= 3.5 or row["change_1m"] >= 5.0 or row["change_3m"] >= 10.0
    outflow = (
        coin.state in {"수급 유입", "상승 가능", "과열·추격 금지"}
        and row["buy_30s"] < 0.43
        and row["change_30s"] < -0.40
    )
    rising = (
        coin.score >= 80
        and row["trade_count_30s"] >= 20
        and row["buy_30s"] >= 0.60
        and row["buy_1m"] >= 0.55
        and row["book_buy"] >= 0.52
        and 0.10 <= row["change_1m"] < 5.0
        and row["spread"] <= 0.8
    )
    if outflow:
        return "수급 이탈"
    if overheat:
        return "과열·추격 금지"
    if rising:
        return "상승 가능"
    if coin.score >= 60:
        return "수급 유입"
    if coin.state != "일반 감시" and coin.score >= 40:
        return "관찰 유지"
    return "일반 감시"


def classify(coin: Coin, row: dict[str, float], flow_percentile: float, now: int) -> dict[str, Any] | None:
    previous = coin.state
    wanted = desired_state(coin, row, flow_percentile)
    safety_override = wanted in {"과열·추격 금지", "수급 이탈"}

    if wanted == previous:
        coin.pending_state = None
        coin.pending_since = 0
    elif (
        now < coin.locked_until
        and not safety_override
        and STATE_STRENGTH.get(wanted, 0) <= STATE_STRENGTH.get(previous, 0)
    ):
        coin.pending_state = None
        coin.pending_since = 0
    else:
        if coin.pending_state != wanted:
            coin.pending_state = wanted
            coin.pending_since = now
        required = STATE_CONFIRM_MS[wanted]
        if now - coin.pending_since >= required:
            coin.state = wanted
            coin.state_since = now
            coin.last_change_at = now
            coin.locked_until = now + (
                ACTIVE_STATE_LOCK_MS
                if wanted in {"수급 유입", "상승 가능"}
                else OTHER_STATE_LOCK_MS
            )
            coin.pending_state = None
            coin.pending_since = 0

    price = row["price"]
    if coin.state in {"수급 유입", "상승 가능"} and coin.first_seen_at is None:
        coin.first_seen_at = now
        coin.first_seen_price = price
        coin.peak_price = price
        coin.trough_price = price
    if coin.first_seen_price:
        coin.peak_price = max(coin.peak_price or price, price)
        coin.trough_price = min(coin.trough_price or price, price)
    if coin.state in {"수급 유입", "상승 가능"}:
        if coin.candidate_confirmed_at is None:
            coin.candidate_confirmed_at = now
        coin.candidate_hold_until = now + CANDIDATE_HOLD_MS
    if coin.state != previous:
        if coin.state == "상승 가능":
            coin.candidate_confirmed_at = coin.candidate_confirmed_at or now
        elif coin.state == "수급 이탈":
            coin.candidate_hold_until = 0
        return {"timestamp_ms": now, "market": coin.market, "state": coin.state, "score": coin.score, "price": price}
    return None


def chart_prices(coin: Coin) -> list[list[float | int]]:
    if not coin.ticks:
        return []
    cutoff = coin.ticks[-1].ts - CHART_WINDOW_MS
    buckets: dict[int, Tick] = {}
    for tick in coin.ticks:
        if tick.ts >= cutoff:
            buckets[tick.ts // CHART_BUCKET_MS] = tick
    return [[tick.ts, round(tick.price, 8)] for tick in buckets.values()]


def public_coin(coin: Coin, row: dict[str, float]) -> dict[str, Any]:
    price = row.get("price", 0.0)
    now = int(time.time() * 1000)
    if coin.state == "과열·추격 금지":
        action = "눌림 대기"
    elif coin.state == "상승 가능":
        action = "초입 검토"
    elif coin.candidate_hold_until > now and coin.state != "수급 이탈":
        action = "관찰"
    else:
        action = "매수 금지"
    return {
        "market": coin.market,
        "symbol": coin.market.split("-", 1)[1],
        "state": coin.state,
        "action": action,
        "score": coin.score,
        "current_price": price,
        "first_seen_at": coin.live_first_seen_at,
        "first_seen_price": coin.live_first_seen_price,
        "return_since_first": round(pct(price, coin.live_first_seen_price), 3) if coin.live_first_seen_price else None,
        "peak_return": round(pct(coin.live_peak_price, coin.live_first_seen_price), 3) if coin.live_first_seen_price and coin.live_peak_price else None,
        "mae": round(pct(coin.live_trough_price, coin.live_first_seen_price), 3) if coin.live_first_seen_price and coin.live_trough_price else None,
        "flow_30s": round(row.get("flow_30s", 0.0), 2),
        "flow_1m": round(row.get("flow_1m", 0.0), 2),
        "flow_3m": round(row.get("flow_3m", 0.0), 2),
        "trade_count_30s": int(row.get("trade_count_30s", 0.0)),
        "buy_ratio_30s": round(row.get("buy_30s", 0.5) * 100.0, 1),
        "buy_ratio_1m": round(row.get("buy_1m", 0.5) * 100.0, 1),
        "orderbook_buy_ratio": round(row.get("book_buy", 0.5) * 100.0, 1),
        "spread": round(row.get("spread", 0.0), 3),
        "change_30s": round(row.get("change_30s", 0.0), 3),
        "change_1m": round(row.get("change_1m", 0.0), 3),
        "change_3m": round(row.get("change_3m", 0.0), 3),
        "last_change_at": coin.last_change_at,
        "state_since": coin.state_since,
        "candidate_confirmed_at": coin.candidate_confirmed_at,
        "candidate_hold_until": coin.candidate_hold_until,
        "confirmed_for_seconds": max(0, int((time.time() * 1000 - coin.state_since) / 1000)) if coin.state_since else 0,
        "pending_state": coin.pending_state,
        "pending_for_seconds": max(0, int((time.time() * 1000 - coin.pending_since) / 1000)) if coin.pending_since else 0,
        "chart_prices": chart_prices(coin),
    }


def still_qualifies(row: dict[str, Any]) -> bool:
    """Catch early flow quickly, but exclude weak selling, wide spreads, and overheated moves."""
    return (
        int(row["score"]) >= 60
        and int(row["trade_count_30s"]) >= 8
        and float(row["buy_ratio_30s"]) >= 52.0
        and float(row["buy_ratio_1m"]) >= 50.0
        and -0.30 <= float(row["change_1m"]) < 5.0
        and float(row["change_30s"]) < 3.5
        and float(row["change_3m"]) < 10.0
        and float(row["spread"]) <= 0.8
    )


def must_remove_now(row: dict[str, Any]) -> bool:
    return (
        int(row["score"]) < 45
        or (float(row["buy_ratio_30s"]) < 43.0 and float(row["change_30s"]) < -0.40)
        or float(row["change_30s"]) >= 3.5
        or float(row["change_1m"]) >= 5.0
        or float(row["change_3m"]) >= 10.0
        or float(row["spread"]) > 0.8
    )


def reset_flow_sequence(coin: Coin) -> None:
    coin.flow_stage = ""
    coin.flow_first_at = None
    coin.flow_first_price = None
    coin.flow_first_strength = 0.0
    coin.flow_second_at = None
    coin.flow_second_strength = 0.0
    coin.flow_peak_price = None
    coin.flow_trough_price = None
    coin.flow_invalidation_price = None
    coin.flow_hold_until = 0
    coin.flow_exit_reason = ""


def flow_pulse(coin: Coin, row: dict[str, float]) -> bool:
    return (
        coin.score >= 60
        and row["trade_count_10s"] >= 3
        and row["flow_10s"] >= 1.0
        and row["buy_10s"] >= 0.52
        and row["trade_count_30s"] >= 8
        and row["buy_30s"] >= 0.52
        and row["buy_1m"] >= 0.50
        and row["spread"] <= 0.8
        and -0.30 <= row["change_1m"] < 5.0
        and row["change_30s"] < 3.5
        and row["change_3m"] < 10.0
    )


def update_flow_sequence(coin: Coin, row: dict[str, float], now: int) -> None:
    price = float(row["price"])
    if price <= 0:
        return
    pulse = flow_pulse(coin, row)
    strong_sell = row["buy_30s"] < 0.43 and row["change_30s"] < -0.40

    if coin.flow_stage == "탈락":
        if now >= coin.flow_hold_until:
            reset_flow_sequence(coin)
        return

    if coin.flow_stage in {"수급 2회 확인", "초입 검토"}:
        coin.flow_peak_price = max(coin.flow_peak_price or price, price)
        coin.flow_trough_price = min(coin.flow_trough_price or price, price)
        stop_broken = bool(coin.flow_invalidation_price and price <= coin.flow_invalidation_price)
        if stop_broken or strong_sell:
            coin.flow_stage = "탈락"
            coin.flow_exit_reason = "기준 저점·손절선 이탈" if stop_broken else "강한 매도 전환"
            coin.flow_hold_until = now + 30_000
            return
        if coin.flow_stage == "수급 2회 확인" and coin.flow_second_at and now - coin.flow_second_at >= 5_000:
            coin.flow_stage = "초입 검토"
            coin.flow_hold_until = now + 180_000
        elif coin.flow_stage == "초입 검토" and now >= coin.flow_hold_until:
            reset_flow_sequence(coin)
        return

    if coin.flow_stage == "수급 1회":
        coin.flow_peak_price = max(coin.flow_peak_price or price, price)
        coin.flow_trough_price = min(coin.flow_trough_price or price, price)
        if strong_sell or (coin.flow_first_price and price <= coin.flow_first_price * 0.97):
            coin.flow_stage = "탈락"
            coin.flow_exit_reason = "1차 포착 후 가격·수급 붕괴"
            coin.flow_hold_until = now + 30_000
            return
        age = now - int(coin.flow_first_at or now)
        if age > 90_000:
            reset_flow_sequence(coin)
            return
        defended = bool(
            coin.flow_first_price
            and price >= coin.flow_first_price * 0.985
            and (coin.flow_trough_price or price) >= coin.flow_first_price * 0.985
        )
        repeated = row["flow_10s"] >= max(1.0, coin.flow_first_strength * 0.50)
        if 20_000 <= age <= 90_000 and pulse and repeated and defended:
            coin.flow_stage = "수급 2회 확인"
            coin.flow_second_at = now
            coin.flow_second_strength = row["flow_10s"]
            short_floor = float(coin.flow_trough_price or price) * 0.997
            coin.flow_invalidation_price = max(coin.flow_first_price * 0.97, short_floor)
            coin.flow_hold_until = now + 185_000
        return

    if pulse:
        coin.flow_stage = "수급 1회"
        coin.flow_first_at = now
        coin.flow_first_price = price
        coin.flow_first_strength = row["flow_10s"]
        coin.flow_peak_price = price
        coin.flow_trough_price = price


class Scanner:
    def __init__(self) -> None:
        self.coins: dict[str, Coin] = {}
        self.latest: dict[str, dict[str, float]] = {}
        self.dirty: set[str] = set()
        self.percentiles: dict[str, float] = {}
        self.events: list[dict[str, Any]] = []
        self.connected = False
        self.updated_at = 0

    async def bootstrap(self, session: ClientSession) -> None:
        async with session.get(f"{REST_API}/market/all", params={"isDetails": "true"}) as response:
            markets = await response.json()
        codes = [
            row["market"] for row in markets
            if row["market"].startswith("KRW-")
            and row["market"].split("-", 1)[1] not in STABLE
            and str(row.get("market_warning", "NONE")).upper() == "NONE"
        ]
        tickers: list[dict[str, Any]] = []
        for offset in range(0, len(codes), 100):
            async with session.get(f"{REST_API}/ticker", params={"markets": ",".join(codes[offset:offset + 100])}) as response:
                tickers.extend(await response.json())
        ticker_map = {row["market"]: row for row in tickers}
        for code in codes:
            average = float(ticker_map.get(code, {}).get("acc_trade_price_24h", 0.0)) / 86_400.0
            self.coins[code] = Coin(code, max(average, 1.0))
        self.restore()

    def restore(self) -> None:
        try:
            saved = json.loads(STATE_FILE.read_text(encoding="utf-8")).get("coins", {})
        except (OSError, ValueError, AttributeError):
            return
        fields = (
            "state", "score", "first_seen_at", "first_seen_price", "peak_price", "trough_price",
            "last_change_at", "state_since", "pending_state", "pending_since", "locked_until",
            "candidate_confirmed_at", "candidate_hold_until",
            "flow_stage", "flow_first_at", "flow_first_price", "flow_first_strength",
            "flow_second_at", "flow_second_strength", "flow_peak_price", "flow_trough_price",
            "flow_invalidation_price", "flow_hold_until", "flow_exit_reason",
        )
        for code, values in saved.items():
            coin = self.coins.get(code)
            if coin is None or not isinstance(values, dict):
                continue
            for name in fields:
                if name in values:
                    setattr(coin, name, values[name])

    async def save_loop(self) -> None:
        while True:
            await asyncio.sleep(5)
            saved = {
                code: {
                    "state": coin.state,
                    "score": coin.score,
                    "first_seen_at": coin.first_seen_at,
                    "first_seen_price": coin.first_seen_price,
                    "peak_price": coin.peak_price,
                    "trough_price": coin.trough_price,
                    "last_change_at": coin.last_change_at,
                    "state_since": coin.state_since,
                    "pending_state": coin.pending_state,
                    "pending_since": coin.pending_since,
                    "locked_until": coin.locked_until,
                    "candidate_confirmed_at": coin.candidate_confirmed_at,
                    "candidate_hold_until": coin.candidate_hold_until,
                    "flow_stage": coin.flow_stage,
                    "flow_first_at": coin.flow_first_at,
                    "flow_first_price": coin.flow_first_price,
                    "flow_first_strength": coin.flow_first_strength,
                    "flow_second_at": coin.flow_second_at,
                    "flow_second_strength": coin.flow_second_strength,
                    "flow_peak_price": coin.flow_peak_price,
                    "flow_trough_price": coin.flow_trough_price,
                    "flow_invalidation_price": coin.flow_invalidation_price,
                    "flow_hold_until": coin.flow_hold_until,
                    "flow_exit_reason": coin.flow_exit_reason,
                }
                for code, coin in self.coins.items()
            }
            temporary = STATE_FILE.with_suffix(".tmp")
            temporary.write_text(json.dumps({"coins": saved}, ensure_ascii=False), encoding="utf-8")
            temporary.replace(STATE_FILE)

    def receive(self, payload: dict[str, Any]) -> None:
        code = str(payload.get("code", ""))
        coin = self.coins.get(code)
        if coin is None:
            return
        kind = payload.get("type")
        now = int(payload.get("trade_timestamp") or payload.get("timestamp") or time.time() * 1000)
        if kind == "trade":
            price = float(payload.get("trade_price", 0.0))
            volume = float(payload.get("trade_volume", 0.0))
            if price > 0 and volume > 0:
                coin.ticks.append(Tick(now, price, volume, str(payload.get("ask_bid", ""))))
                cutoff = now - 20 * 60_000
                while coin.ticks and coin.ticks[0].ts < cutoff:
                    coin.ticks.popleft()
                self.dirty.add(code)
        elif kind == "orderbook":
            units = payload.get("orderbook_units", [])
            if units:
                coin.best_ask = float(units[0].get("ask_price", 0.0))
                coin.best_bid = float(units[0].get("bid_price", 0.0))
                coin.ask_depth = sum(float(unit.get("ask_size", 0.0)) for unit in units[:5])
                coin.bid_depth = sum(float(unit.get("bid_size", 0.0)) for unit in units[:5])
                self.dirty.add(code)
        elif kind == "ticker":
            value24 = float(payload.get("acc_trade_price_24h", 0.0))
            if value24 > 0:
                observed = value24 / 86_400.0
                coin.baseline_per_second = coin.baseline_per_second * 0.995 + observed * 0.005

    async def evaluate(self) -> None:
        while True:
            await asyncio.sleep(0.25)
            now = int(time.time() * 1000)
            dirty = list(self.dirty)
            self.dirty.clear()
            if not dirty:
                continue
            for code in dirty:
                self.latest[code] = features(self.coins[code], now)
            ranked = sorted(self.latest, key=lambda code: self.latest[code].get("flow_30s", 0.0))
            denominator = max(1, len(ranked) - 1)
            self.percentiles = {code: index / denominator * 100.0 for index, code in enumerate(ranked)}
            for code in dirty:
                event = classify(self.coins[code], self.latest[code], self.percentiles.get(code, 0.0), now)
                update_flow_sequence(self.coins[code], self.latest[code], now)
                if event:
                    self.events.append(event)
                    self.events = self.events[-500:]
                    DATA_DIR.mkdir(parents=True, exist_ok=True)
                    with EVENT_FILE.open("a", encoding="utf-8") as handle:
                        handle.write(json.dumps(event, ensure_ascii=False) + "\n")
            self.updated_at = now

    async def stream(self, session: ClientSession) -> None:
        delay = 1
        while True:
            try:
                async with session.ws_connect(WS_API, heartbeat=30) as socket:
                    codes = list(self.coins)
                    await socket.send_json([
                        {"ticket": f"bes-{uuid.uuid4()}"},
                        {"type": "ticker", "codes": codes, "isOnlyRealtime": True},
                        {"type": "trade", "codes": codes, "isOnlyRealtime": True},
                        {"type": "orderbook", "codes": codes, "isOnlyRealtime": True},
                    ])
                    self.connected = True
                    delay = 1
                    async for message in socket:
                        if message.type == WSMsgType.TEXT:
                            self.receive(json.loads(message.data))
                        elif message.type == WSMsgType.BINARY:
                            self.receive(json.loads(message.data.decode()))
                        elif message.type in {WSMsgType.CLOSED, WSMsgType.ERROR}:
                            break
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.events.append({"timestamp_ms": int(time.time() * 1000), "state": "연결 오류", "error": str(exc)})
            finally:
                self.connected = False
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30)

    def snapshot(self) -> dict[str, Any]:
        btc_row = self.latest.get("KRW-BTC", {})
        btc_falling = float(btc_row.get("change_3m", 0.0)) <= -0.35 or float(btc_row.get("change_1m", 0.0)) <= -0.20
        rows = []
        for code, coin in self.coins.items():
            if code not in self.latest or not coin.flow_stage:
                continue
            row = public_coin(coin, self.latest[code])
            row["first_seen_at"] = coin.flow_first_at
            row["first_seen_price"] = coin.flow_first_price
            row["return_since_first"] = round(pct(float(row["current_price"]), coin.flow_first_price), 3) if coin.flow_first_price else None
            row["peak_return"] = round(pct(coin.flow_peak_price, coin.flow_first_price), 3) if coin.flow_first_price and coin.flow_peak_price else None
            row["mae"] = round(pct(coin.flow_trough_price, coin.flow_first_price), 3) if coin.flow_first_price and coin.flow_trough_price else None
            row["flow_first_strength"] = round(coin.flow_first_strength, 2)
            row["flow_second_strength"] = round(coin.flow_second_strength, 2) if coin.flow_second_at else None
            row["invalidation_price"] = round(coin.flow_invalidation_price, 8) if coin.flow_invalidation_price else round(coin.flow_first_price * 0.97, 8) if coin.flow_first_price else None
            row["exit_reason"] = coin.flow_exit_reason
            if coin.flow_stage == "초입 검토" and btc_falling:
                row["action"] = "고위험 초입"
            else:
                row["action"] = coin.flow_stage
            row["risk"] = "BTC 단기 하락" if btc_falling else "일반"
            row["stop_price_3pct"] = round(float(row["first_seen_price"]) * 0.97, 8) if row.get("first_seen_price") else None
            prices = [float(point[1]) for point in row.get("chart_prices", [])]
            row["recent_low"] = round(min(prices), 8) if prices else None
            rows.append(row)
        stage_order = {"초입 검토": 0, "고위험 초입": 0, "수급 2회 확인": 1, "수급 1회": 2, "탈락": 3}
        rows.sort(key=lambda row: (stage_order.get(row["action"], 9), -row["score"], -(row["first_seen_at"] or 0)))
        return {
            "engine": "BES Repeated Flow & Defense V2.0",
            "connected": self.connected,
            "updated_at_ms": self.updated_at,
            "market_count": len(self.coins),
            "candidate_count": len(rows),
            "buy_review_count": sum(row["action"] in {"초입 검토", "고위험 초입"} for row in rows),
            "btc_market": {"state": "단기 하락·고위험" if btc_falling else "보통", "blocking": False},
            "results": rows,
            "events": self.events[-100:],
        }


async def main() -> None:
    scanner = Scanner()
    async with ClientSession(headers={"User-Agent": "bes-realtime-scanner/1.0"}) as session:
        await scanner.bootstrap(session)
        app = web.Application()
        app.router.add_get("/api/state", lambda _: web.json_response(scanner.snapshot()))
        app.router.add_get("/health", lambda _: web.json_response({"ok": scanner.connected, "markets": len(scanner.coins)}))
        app.router.add_static("/", STATIC_DIR, show_index=True)
        runner = web.AppRunner(app)
        await runner.setup()
        await web.TCPSite(runner, "0.0.0.0", int(os.environ.get("PORT", "8080"))).start()
        await asyncio.gather(scanner.stream(session), scanner.evaluate(), scanner.save_loop())


if __name__ == "__main__":
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    asyncio.run(main())
