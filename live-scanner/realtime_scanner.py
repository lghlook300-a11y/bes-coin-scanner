#!/usr/bin/env python3
"""BES real-time Bithumb KRW capital-flow scanner (no orders, no sizing)."""

from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
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
PERFORMANCE_FILE = DATA_DIR / "flow_performance.json"
DAILY_COUNT_FILE = DATA_DIR / "daily_detection_counts_v2_5.json"
DAILY_HISTORY_FILE = DATA_DIR / "daily_history_v2_8.json"
EARLY_RADAR_FILE = DATA_DIR / "early_bottom_radar_v2_8.json"
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
A_CONTEXT_REFRESH_MS = 5 * 60_000
EARLY_RADAR_REFRESH_MS = 15 * 60_000
EARLY_RADAR_REDETECTION_MS = 12 * 60 * 60_000
REDETECTION_GAP_MS = 30 * 60_000
DISPLAY_SNAPSHOT_MS = 3 * 60_000
BTC_REGIME_REFRESH_MS = 15 * 60_000
MAX_DETAIL_MARKETS = 60
DETAIL_ROTATION_MS = 5 * 60_000
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
    flow_cooldown_until: int = 0
    last_sequence_ended_at: int = 0
    decision_action: str = ""
    decision_changed_at: int = 0
    a_checked_at: int = 0
    a_near: bool = False
    a_price: float | None = None
    a_distance_percent: float | None = None
    a_defended: bool = False
    a_reason: str = "4시간봉 확인 전"
    abc_stage: str = ""
    abc_cycle_id: int | None = None
    abc_a_price: float | None = None
    abc_b_price: float | None = None
    abc_c_price: float | None = None
    abc_updated_at: int = 0
    abc_reason: str = "PRE-A 탐색 중"
    confirm_price: float | None = None
    confirm_at: int | None = None
    confirm_a_price: float | None = None
    confirm_b_price: float | None = None
    confirm_c_price: float | None = None
    entry_cycle_state: str = ""
    entry_attempt_count: int = 0
    entry_attempt_price: float | None = None
    entry_stop_price: float | None = None
    entry_stopped_at: int | None = None
    entry_cycle_reason: str = "CONFIRM 구조 대기"
    radar_checked_at: int = 0
    radar_score: int = 0
    radar_stage: str = ""
    radar_reason: str = "바닥 구조 확인 전"
    radar_first_at: int | None = None
    radar_first_price: float | None = None
    radar_l1_price: float | None = None
    radar_l2_price: float | None = None
    radar_atr_percent: float | None = None
    radar_obv_divergence: bool = False
    radar_volume_contraction: bool = False
    radar_ema_recovered: bool = False
    radar_breakout_ready: bool = False
    swing_checked_at: int = 0
    daily_state: str = "확인 중"
    h4_state: str = "확인 중"
    swing_state: str = "일봉·4시간봉 확인 중"
    swing_buy_zone: bool = False


def pct(new: float, old: float) -> float:
    return (new / old - 1.0) * 100.0 if old > 0 else 0.0


def clip(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


KST = timezone(timedelta(hours=9))


def kst_session_date(timestamp_ms: int) -> str:
    """Return the KST trading-day key whose boundary is 09:00, not midnight."""
    local = datetime.fromtimestamp(timestamp_ms / 1000, KST)
    if local.hour < 9:
        local -= timedelta(days=1)
    return local.strftime("%Y-%m-%d")


def kst_session_bounds(timestamp_ms: int) -> tuple[int, int]:
    local = datetime.fromtimestamp(timestamp_ms / 1000, KST)
    start_date = local.date() if local.hour >= 9 else (local - timedelta(days=1)).date()
    start = datetime.combine(start_date, datetime.min.time(), KST) + timedelta(hours=9)
    end = start + timedelta(days=1)
    return int(start.timestamp() * 1000), int(end.timestamp() * 1000)


def ema(values: list[float], length: int) -> float:
    if not values:
        return 0.0
    alpha = 2.0 / (length + 1.0)
    result = values[0]
    for value in values[1:]:
        result = value * alpha + result * (1.0 - alpha)
    return result


def analyze_btc_swing_regime(daily_candles: list[dict[str, Any]], h4_candles: list[dict[str, Any]]) -> dict[str, Any]:
    """Describe the BTC swing backdrop using confirmed daily and 4H candles only."""
    daily = sorted(daily_candles, key=lambda item: str(item.get("candle_date_time_utc", "")))[:-1]
    h4 = sorted(h4_candles, key=lambda item: str(item.get("candle_date_time_utc", "")))[:-1]
    if len(daily) < 22 or len(h4) < 24:
        return {"state": "일봉·4시간봉 자료 준비 중", "daily": "확인 중", "four_hour": "확인 중",
                "buy_zone": False, "reason": "확정봉 자료 부족"}

    daily_closes = [float(item["trade_price"]) for item in daily]
    daily_lows = [float(item["low_price"]) for item in daily]
    h4_closes = [float(item["trade_price"]) for item in h4]
    daily_ma20 = sum(daily_closes[-20:]) / 20
    daily_ma20_prev = sum(daily_closes[-21:-1]) / 20
    daily_close = daily_closes[-1]
    daily_bottom = min(daily_lows[-20:])
    near_daily_bottom = pct(daily_close, daily_bottom) <= 8.0

    if near_daily_bottom and daily_close > daily_closes[-2]:
        daily_state = "바닥권 반등 시도"
    elif daily_close > daily_ma20 and daily_ma20 > daily_ma20_prev:
        daily_state = "상승"
    elif daily_close < daily_ma20 and daily_ma20 < daily_ma20_prev:
        daily_state = "하락"
    else:
        daily_state = "횡보·전환 구간"

    h4_ema20 = ema(h4_closes[-40:], 20)
    h4_ema20_prev = ema(h4_closes[-41:-1], 20)
    h4_close = h4_closes[-1]
    h4_up = h4_close > h4_ema20 and h4_ema20 > h4_ema20_prev
    h4_down = h4_close < h4_ema20 and h4_ema20 < h4_ema20_prev
    h4_state = "상승 전환·유지" if h4_up else "하락" if h4_down else "횡보·전환 확인 중"

    buy_zone = near_daily_bottom and h4_up
    if buy_zone:
        state = "바닥 확인 후 상승 전환 · 매수 검토"
        reason = "일봉이 20일 저점권이고 4시간봉이 20EMA 위에서 상승"
    elif daily_state == "상승" and h4_up:
        state = "상승 진행 · 신규 추격 주의"
        reason = "방향은 상승이지만 바닥권 진입 구간은 아님"
    elif h4_down:
        state = "하락 중 · 바닥과 상승 전환 대기"
        reason = "4시간봉 20EMA 아래 하락"
    else:
        state = "바닥·상승 전환 확인 대기"
        reason = "일봉 바닥과 4시간봉 상승이 동시에 확인되지 않음"
    return {"state": state, "daily": daily_state, "four_hour": h4_state,
            "buy_zone": buy_zone, "reason": reason}


def analyze_a_context(candles: list[dict[str, Any]], current_price: float) -> dict[str, Any]:
    """Conservative 4H A proxy: confirmed swing low, no low break, and price still near it."""
    rows = sorted(candles, key=lambda item: str(item.get("candle_date_time_utc", "")))
    if len(rows) < 12 or current_price <= 0:
        return {"near": False, "price": None, "distance": None, "defended": False, "reason": "4시간봉 자료 부족"}
    lows = [float(item["low_price"]) for item in rows]
    closes = [float(item["trade_price"]) for item in rows]
    pivots = [i for i in range(2, len(rows) - 2)
              if lows[i] < min(lows[i - 2:i]) and lows[i] <= min(lows[i + 1:i + 3])]
    recent = [i for i in pivots if i >= len(rows) - 14]
    if not recent:
        return {"near": False, "price": None, "distance": None, "defended": False, "reason": "최근 확인된 4H A 없음"}
    index = recent[-1]
    a_price = lows[index]
    subsequent_low = min(lows[index + 1:]) if index + 1 < len(lows) else current_price
    defended = subsequent_low >= a_price * 0.99 and current_price >= a_price * 0.99
    distance = pct(current_price, a_price)
    near = defended and -1.0 <= distance <= 7.0
    recovered = current_price >= ema(closes[-20:], 20) * 0.985
    if not defended:
        reason = "A 기준 저점 이탈"
    elif distance > 7.0:
        reason = "A에서 이미 멀어짐"
    elif distance < -1.0:
        reason = "A 저점 재확인 필요"
    elif not recovered:
        reason = "A 부근·4H 구조 회복 대기"
    else:
        reason = "A 부근·저점 방어 확인"
    return {"near": near, "price": a_price, "distance": distance,
            "defended": defended, "recovered": recovered, "reason": reason}


def analyze_fast_a_context(candles: list[dict[str, Any]], current_price: float) -> dict[str, Any]:
    """Early, unconfirmed 4H low candidate. This never acts as a buy signal."""
    rows = sorted(candles, key=lambda item: str(item.get("candle_date_time_utc", "")))
    if len(rows) < 10 or current_price <= 0:
        return {"candidate": False, "price": None, "reason": "PRE-A 자료 부족"}
    recent = rows[-3:]
    lows = [float(item["low_price"]) for item in recent]
    candidate_price = min(lows)
    candidate = recent[lows.index(candidate_price)]
    opening = float(candidate["opening_price"])
    close = float(candidate["trade_price"])
    high = float(candidate["high_price"])
    low = float(candidate["low_price"])
    body = max(abs(close - opening), max(high - low, 1e-12) * 0.08)
    lower_wick = max(0.0, min(opening, close) - low)
    prior_close = float(rows[-6]["trade_price"])
    decline = pct(close, prior_close)
    recovery = pct(current_price, candidate_price)
    previous_floor = min(float(item["low_price"]) for item in rows[-9:-3])
    near_new_low = candidate_price <= previous_floor * 1.012
    slowing = lower_wick >= body * 0.55 or close >= low * 1.006
    detected = near_new_low and decline <= 0.5 and slowing and 0.0 <= recovery <= 5.0
    return {"candidate": detected, "price": candidate_price,
            "reason": "4H 새 저점 부근·하락 둔화" if detected else "PRE-A 조건 대기"}


def analyze_early_bottom_radar(candles: list[dict[str, Any]], current_price: float) -> dict[str, Any]:
    """Non-trading shadow detector for a defended/higher 4H low before flow confirmation."""
    rows = sorted(candles, key=lambda item: str(item.get("candle_date_time_utc", "")))
    if len(rows) < 36 or current_price <= 0:
        return {"score": 0, "stage": "", "reason": "4시간봉 자료 부족"}
    # The newest Bithumb candle is still forming.  Use it only as the live price.
    rows = rows[:-1]
    highs = [float(row["high_price"]) for row in rows]
    lows = [float(row["low_price"]) for row in rows]
    closes = [float(row["trade_price"]) for row in rows]
    volumes = [float(row.get("candle_acc_trade_volume", 0.0)) for row in rows]
    true_ranges = []
    for i in range(1, len(rows)):
        true_ranges.append(max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]),
                               abs(lows[i] - closes[i - 1])))
    atr = sum(true_ranges[-14:]) / max(1, len(true_ranges[-14:]))
    atr_percent = pct(closes[-1] + atr, closes[-1]) if closes[-1] else 0.0

    start = max(2, len(rows) - 42)
    pivots = [i for i in range(start, len(rows) - 2)
              if lows[i] <= min(lows[i - 2:i]) and lows[i] <= min(lows[i + 1:i + 3])]
    pair = None
    for right_pos in range(len(pivots) - 1, 0, -1):
        l2_index = pivots[right_pos]
        for left_pos in range(right_pos - 1, -1, -1):
            l1_index = pivots[left_pos]
            if l2_index - l1_index < 3:
                continue
            tolerance = max(lows[l1_index] * 0.025, atr * 0.55)
            if lows[l2_index] >= lows[l1_index] - tolerance and lows[l2_index] <= lows[l1_index] * 1.10:
                pair = (l1_index, l2_index)
                break
        if pair:
            break
    if pair is None:
        return {"score": 0, "stage": "", "reason": "방어된 두 번째 저점 대기",
                "atr_percent": round(atr_percent, 3)}

    l1_index, l2_index = pair
    l1, l2 = lows[l1_index], lows[l2_index]
    higher_low = l2 >= l1 or l2 >= l1 - max(l1 * 0.012, atr * 0.35)
    first_volume = max(volumes[l1_index], 1e-12)
    volume_contraction = volumes[l2_index] <= first_volume * 0.85

    obv = [0.0]
    for i in range(1, len(rows)):
        direction = 1.0 if closes[i] > closes[i - 1] else -1.0 if closes[i] < closes[i - 1] else 0.0
        obv.append(obv[-1] + direction * volumes[i])
    obv_divergence = l2 <= l1 * 1.04 and obv[l2_index] > obv[l1_index]
    ema20 = ema(closes[-20:], 20)
    ema_recovered = current_price >= ema20 * 0.985
    between_high = max(highs[l1_index + 1:l2_index]) if l2_index > l1_index + 1 else highs[l1_index]
    breakout_ready = current_price >= between_high * 0.985
    recent_volume = sum(volumes[-3:]) / 3.0
    base_volume = sum(volumes[-20:-3]) / max(1, len(volumes[-20:-3]))
    volume_reentry = recent_volume >= base_volume * 1.20

    score = 25
    score += 10 if higher_low else 0
    score += 15 if volume_contraction else 0
    score += 20 if obv_divergence else 0
    score += 15 if ema_recovered else 0
    score += 10 if breakout_ready else 0
    score += 5 if volume_reentry else 0
    if score >= 75 and breakout_ready:
        stage = "돌파 준비"
    elif score >= 65:
        stage = "A 초기 후보"
    elif score >= 50:
        stage = "바닥 준비 관찰"
    else:
        stage = ""
    reasons = []
    if higher_low: reasons.append("저점 방어")
    if volume_contraction: reasons.append("재시험 거래량 감소")
    if obv_divergence: reasons.append("OBV 선행")
    if ema_recovered: reasons.append("20EMA 회복")
    if breakout_ready: reasons.append("직전 고점 접근")
    if volume_reentry: reasons.append("거래량 재유입")
    return {"score": score, "stage": stage, "reason": "·".join(reasons) or "조건 누적 중",
            "l1_price": l1, "l2_price": l2, "atr_percent": round(atr_percent, 3),
            "higher_low": higher_low, "volume_contraction": volume_contraction,
            "obv_divergence": obv_divergence, "ema_recovered": ema_recovered,
            "breakout_ready": breakout_ready, "volume_reentry": volume_reentry}


def analyze_pine_h4_bull(candles: list[dict[str, Any]], pivot_bars: int = 12,
                         structural_lookback: int = 360, rearm_bars: int = 72) -> dict[str, Any]:
    """Replay the uploaded Pine 4H Bull A state machine on confirmed Bithumb candles."""
    rows = sorted(candles, key=lambda item: str(item.get("candle_date_time_utc", "")))
    if len(rows) < pivot_bars * 2 + 2:
        return {"stage": "구조 대기", "a": None, "b": None, "c": None, "last_confirm": None}
    # Bithumb includes the still-forming 4H candle; Pine transitions use confirmed closes.
    rows = rows[:-1]
    lows = [float(row["low_price"]) for row in rows]
    highs = [float(row["high_price"]) for row in rows]
    closes = [float(row["trade_price"]) for row in rows]
    times = [str(row.get("candle_date_time_utc", "")) for row in rows]
    wait_a, wait_b, wait_c, wait_break = range(4)
    state = wait_a
    a = b = c = None
    a_index = b_index = c_index = None
    last_break_index = None
    last_confirm = None
    for bar in range(len(rows)):
        pivot_index = bar - pivot_bars
        pivot_low = pivot_high = None
        if pivot_index >= pivot_bars and pivot_index + pivot_bars < len(rows):
            low = lows[pivot_index]
            high = highs[pivot_index]
            if low < min(lows[pivot_index - pivot_bars:pivot_index]) and low <= min(lows[pivot_index + 1:pivot_index + pivot_bars + 1]):
                pivot_low = low
            if high > max(highs[pivot_index - pivot_bars:pivot_index]) and high >= max(highs[pivot_index + 1:pivot_index + pivot_bars + 1]):
                pivot_high = high
        structural_low = False
        if pivot_low is not None:
            start = max(0, pivot_index - structural_lookback + 1)
            structural_low = pivot_low <= min(lows[start:pivot_index + 1])
        passes_rearm = last_break_index is None or pivot_index >= last_break_index + rearm_bars
        if state in {wait_b, wait_c} and a is not None and closes[bar] < a:
            state, a, b, c = wait_a, None, None, None
            a_index = b_index = c_index = None
        elif state == wait_a:
            if structural_low and passes_rearm:
                a, a_index, b, b_index, c, c_index = pivot_low, pivot_index, None, None, None, None
                state = wait_b
        elif state == wait_b:
            if pivot_low is not None and pivot_index > int(a_index) and pivot_low < float(a):
                a, a_index = pivot_low, pivot_index
            elif pivot_high is not None and pivot_index > int(a_index):
                b, b_index, state = pivot_high, pivot_index, wait_c
        elif state == wait_c:
            if pivot_low is not None and pivot_index > int(b_index) and pivot_low <= float(a):
                a, a_index, b, b_index, c, c_index = pivot_low, pivot_index, None, None, None, None
                state = wait_b
            elif pivot_high is not None and pivot_index > int(b_index) and pivot_high > float(b):
                b, b_index = pivot_high, pivot_index
            elif pivot_low is not None and pivot_index > int(b_index) and pivot_low > float(a):
                c, c_index, state = pivot_low, pivot_index, wait_break
        elif state == wait_break:
            if closes[bar] < float(a):
                state, a, b, c = wait_a, None, None, None
                a_index = b_index = c_index = None
            elif closes[bar] > float(b):
                last_confirm = {"a": a, "b": b, "c": c, "confirm_price": b,
                                "confirm_close": closes[bar], "confirm_time": times[bar],
                                "confirm_bar": bar}
                last_break_index = bar
                state, a, b, c = wait_a, None, None, None
                a_index = b_index = c_index = None
    stage = {wait_a: "구조 대기", wait_b: "PRE-A", wait_c: "B 진행", wait_break: "C 눌림 대기"}[state]
    return {"stage": stage, "a": a, "b": b, "c": c, "last_confirm": last_confirm,
            "bars_used": len(rows)}


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


def trade_decision(coin: Coin, row: dict[str, float], btc_falling: bool = False) -> dict[str, Any]:
    """Turn a detected flow sequence into one clear, continuously refreshed action."""
    price = float(row.get("price", 0.0))
    first = float(coin.flow_first_price or 0.0)
    invalidation = float(coin.flow_invalidation_price or (first * 0.97 if first else 0.0))
    signal_return = pct(price, first) if first else 0.0
    stop_distance = abs(pct(invalidation, price)) if price and invalidation else 99.0
    latest_ts = coin.ticks[-1].ts if coin.ticks else 0
    prior_prices = [
        tick.price for tick in coin.ticks
        if (not coin.flow_first_at or tick.ts >= coin.flow_first_at) and tick.ts <= latest_ts - 10_000
    ]
    short_resistance = max(prior_prices) if prior_prices else first
    overheated = signal_return >= 6.0 or row.get("change_3m", 0.0) >= 6.0 or row.get("change_1m", 0.0) >= 4.0
    strong_sell = row.get("buy_30s", 0.5) < 0.45 or (
        row.get("buy_1m", 0.5) < 0.48 and row.get("change_30s", 0.0) < -0.25
    )
    broken = bool(invalidation and price <= invalidation)

    if coin.flow_stage == "탈락" or broken:
        action, reason = "매수 금지", coin.flow_exit_reason or "무효화 가격 이탈"
    elif overheated:
        action, reason = "매수 금지", "이미 단기 급등·추격 금지"
    elif strong_sell:
        action, reason = "매수 금지", "현재 매도 우세·수급 약화"
    elif coin.flow_stage == "수급 1회":
        action, reason = "기다림", "1차 포착만 확인·2차 수급 대기"
    else:
        repeated = coin.flow_stage in {"수급 2회 확인", "초입 검토"}
        defended = bool(first and price >= first * 0.995)
        flow_alive = row.get("buy_30s", 0.5) >= 0.52 and row.get("buy_1m", 0.5) >= 0.50 and row.get("flow_30s", 0.0) >= 1.0
        breakout = (
            repeated and defended and flow_alive and short_resistance > 0
            and price >= short_resistance * 1.001
            and row.get("buy_30s", 0.5) >= 0.58 and row.get("buy_1m", 0.5) >= 0.54
            and row.get("change_1m", 0.0) >= 0.05
        )
        small_try = repeated and defended and flow_alive and stop_distance <= 3.2
        if breakout:
            action, reason = "돌파 확인", "2차 수급·가격 방어·상승 지속"
        elif small_try:
            action, reason = "소액 시도 가능", "2차 수급·최초 포착가 방어"
        else:
            action, reason = "기다림", "가격 방어 또는 현재 수급 재확인 필요"

    if btc_falling and action in {"소액 시도 가능", "돌파 확인"}:
        reason += " · BTC 약세이므로 비중 축소"
    route = "A+수급" if coin.a_near else "기존 수급"
    if coin.a_near and action == "기다림":
        reason = "A 부근 확인 · " + reason
    elif coin.a_near and action in {"소액 시도 가능", "돌파 확인"}:
        reason = "A 저점 방어+" + reason
    entry_low = first * 0.995 if first else None
    entry_high = first * 1.01 if first else None
    return {
        "action": action,
        "decision_reason": reason,
        "entry_low": round(entry_low, 8) if entry_low else None,
        "entry_high": round(entry_high, 8) if entry_high else None,
        "decision_stop_price": round(invalidation, 8) if invalidation else None,
        "breakout_price": round(short_resistance, 8) if short_resistance else None,
        "stop_distance_percent": round(stop_distance, 2) if stop_distance < 99 else None,
        "chase": overheated,
        "detection_route": route,
        "a_near": coin.a_near,
        "a_price": round(coin.a_price, 8) if coin.a_price else None,
        "a_distance_percent": round(coin.a_distance_percent, 2) if coin.a_distance_percent is not None else None,
        "a_defended": coin.a_defended,
        "a_reason": coin.a_reason,
    }


def public_coin(coin: Coin, row: dict[str, float]) -> dict[str, Any]:
    price = row.get("price", 0.0)
    return {
        "market": coin.market,
        "symbol": coin.market.split("-", 1)[1],
        "state": coin.state,
        "action": "기다림",
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


def reset_flow_sequence(coin: Coin, ended_at: int | None = None) -> None:
    if ended_at is not None:
        coin.last_sequence_ended_at = ended_at
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
        and row["change_3m"] < 6.0
    )


def update_flow_sequence(coin: Coin, row: dict[str, float], now: int, btc_falling: bool = False) -> None:
    price = float(row["price"])
    if price <= 0:
        return
    pulse = flow_pulse(coin, row)
    strong_sell = row["buy_30s"] < 0.43 and row["change_30s"] < -0.40

    if coin.flow_stage == "탈락":
        if now >= coin.flow_hold_until:
            reset_flow_sequence(coin, now)
        return

    if coin.flow_stage in {"수급 2회 확인", "초입 검토"}:
        coin.flow_peak_price = max(coin.flow_peak_price or price, price)
        coin.flow_trough_price = min(coin.flow_trough_price or price, price)
        stop_broken = bool(coin.flow_invalidation_price and price <= coin.flow_invalidation_price)
        if stop_broken or strong_sell:
            coin.flow_stage = "탈락"
            coin.flow_exit_reason = "기준 저점·손절선 이탈" if stop_broken else "강한 매도 전환"
            coin.flow_hold_until = now + 30_000
            coin.flow_cooldown_until = now + 30 * 60_000
            return
        if coin.flow_stage == "수급 2회 확인" and coin.flow_second_at and now - coin.flow_second_at >= 5_000:
            coin.flow_stage = "초입 검토"
            coin.flow_hold_until = now + 180_000
        elif coin.flow_stage == "초입 검토" and now >= coin.flow_hold_until:
            reset_flow_sequence(coin, now)
        return

    if coin.flow_stage == "수급 1회":
        coin.flow_peak_price = max(coin.flow_peak_price or price, price)
        coin.flow_trough_price = min(coin.flow_trough_price or price, price)
        if strong_sell or (coin.flow_first_price and price <= coin.flow_first_price * 0.97):
            coin.flow_stage = "탈락"
            coin.flow_exit_reason = "1차 포착 후 가격·수급 붕괴"
            coin.flow_hold_until = now + 30_000
            coin.flow_cooldown_until = now + 30 * 60_000
            return
        age = now - int(coin.flow_first_at or now)
        if age > 90_000:
            reset_flow_sequence(coin, now)
            return
        defended = bool(coin.flow_first_price and price >= coin.flow_first_price)
        required_ratio = 1.0 if btc_falling else 0.80
        repeated = row["flow_10s"] >= max(1.0, coin.flow_first_strength * required_ratio)
        if 20_000 <= age <= 90_000 and pulse and row["buy_10s"] >= 0.56 and repeated and defended:
            coin.flow_stage = "수급 2회 확인"
            coin.flow_second_at = now
            coin.flow_second_strength = row["flow_10s"]
            short_floor = float(coin.flow_trough_price or price) * 0.997
            coin.flow_invalidation_price = max(coin.flow_first_price * 0.97, short_floor)
            coin.flow_hold_until = now + 185_000
        return

    if pulse and now >= coin.flow_cooldown_until:
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
        self.performance_records = self.load_performance()
        self.early_radar_records = self.load_early_radar()
        self.daily_counts = self.load_daily_counts()
        self.daily_history = self.load_daily_history()
        self.active_session_key = kst_session_date(int(time.time() * 1000))
        for day_key in sorted(self.daily_counts):
            if day_key != self.active_session_key and day_key not in self.daily_history:
                day_start = int(datetime.strptime(day_key, "%Y-%m-%d").replace(tzinfo=KST).timestamp() * 1000)
                self.archive_completed_session(day_key, day_start + 33 * 60 * 60 * 1000)
        self.session: ClientSession | None = None
        self.a_refreshing: set[str] = set()
        # The engine keeps calculating in real time, but the dashboard receives
        # only a completed decision snapshot.  This prevents borderline coins
        # from appearing/disappearing every 1.5 seconds while a user is reading.
        self.published_snapshot: dict[str, Any] | None = None
        self.published_at = 0
        self.btc_swing_regime: dict[str, Any] = {
            "state": "일봉·4시간봉 자료 준비 중", "daily": "확인 중", "four_hour": "확인 중",
            "buy_zone": False, "reason": "확정봉 자료 수집 전", "checked_at_ms": 0,
        }
        # Low-cost swing mode: all markets receive lightweight ticker updates,
        # while only a rotating shortlist receives trade/orderbook depth.
        self.light_tickers: dict[str, dict[str, float]] = {}
        self.previous_trade_values: dict[str, float] = {}
        self.detail_codes: set[str] = set()

    def select_detail_codes(self) -> set[str]:
        """Choose at most 60 markets for expensive trade/orderbook processing."""
        active = [code for code, coin in self.coins.items()
                  if coin.flow_stage or coin.abc_stage or coin.entry_cycle_state or coin.radar_stage]
        # Keep actionable/recheck candidates ahead of passive radar entries.
        active.sort(key=lambda code: (
            0 if self.coins[code].entry_cycle_state in {
                "첫 시도", "재진입", "CONFIRM 재확인 대기", "단기 실패"
            } else (1 if self.coins[code].flow_stage else 2),
            -int(self.coins[code].radar_score),
        ))

        liquid = sorted(
            self.light_tickers,
            key=lambda code: float(self.light_tickers[code].get("trade_value_24h", 0.0)),
            reverse=True,
        )
        momentum = sorted(
            self.light_tickers,
            key=lambda code: (
                float(self.light_tickers[code].get("trade_value_delta", 0.0)),
                float(self.light_tickers[code].get("signed_change_rate", 0.0)),
            ),
            reverse=True,
        )
        chosen: list[str] = []
        # Preserve the most relevant existing candidates first. The rest
        # combines liquid markets and newly accelerating markets.
        for group in (active[:20], liquid[:30], momentum[:30], ["KRW-BTC"]):
            for code in group:
                if code in self.coins and code not in chosen:
                    chosen.append(code)
                if len(chosen) >= MAX_DETAIL_MARKETS:
                    break
            if len(chosen) >= MAX_DETAIL_MARKETS:
                break
        self.detail_codes = set(chosen)
        for code, row in self.light_tickers.items():
            self.previous_trade_values[code] = float(row.get("trade_value_24h", 0.0))
        return self.detail_codes

    async def refresh_btc_swing_regime(self) -> None:
        """Refresh the display-only swing regime every 15 minutes."""
        while True:
            try:
                if self.session is not None:
                    async with self.session.get(f"{REST_API}/candles/days",
                                                params={"market": "KRW-BTC", "count": 40}) as response:
                        response.raise_for_status()
                        daily = await response.json()
                    async with self.session.get(f"{REST_API}/candles/minutes/240",
                                                params={"market": "KRW-BTC", "count": 60}) as response:
                        response.raise_for_status()
                        h4 = await response.json()
                    regime = analyze_btc_swing_regime(daily, h4)
                    regime["checked_at_ms"] = int(time.time() * 1000)
                    self.btc_swing_regime = regime
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.events.append({"timestamp_ms": int(time.time() * 1000),
                                    "state": "BTC 일봉·4시간봉 확인 오류", "error": str(exc)})
            await asyncio.sleep(BTC_REGIME_REFRESH_MS / 1000)

    def load_daily_counts(self) -> dict[str, dict[str, dict[str, Any]]]:
        try:
            rows = json.loads(DAILY_COUNT_FILE.read_text(encoding="utf-8"))
            return rows if isinstance(rows, dict) else {}
        except (OSError, ValueError):
            return {}

    def load_early_radar(self) -> list[dict[str, Any]]:
        try:
            rows = json.loads(EARLY_RADAR_FILE.read_text(encoding="utf-8"))
            return rows if isinstance(rows, list) else []
        except (OSError, ValueError):
            return []

    def load_daily_history(self) -> dict[str, dict[str, Any]]:
        try:
            rows = json.loads(DAILY_HISTORY_FILE.read_text(encoding="utf-8"))
            return rows if isinstance(rows, dict) else {}
        except (OSError, ValueError):
            return {}

    def archive_completed_session(self, day_key: str, finalized_at_ms: int) -> None:
        """Freeze one KST 09:00 session without changing any scanner signal rules."""
        start = int(datetime.strptime(day_key, "%Y-%m-%d").replace(tzinfo=KST).timestamp() * 1000)
        start += 9 * 60 * 60 * 1000
        end = start + 24 * 60 * 60 * 1000
        records = [dict(item) for item in self.performance_records
                   if start <= int(item.get("first_at_ms") or 0) < end]
        for record in records:
            first_at = int(record.get("first_at_ms") or 0)
            hit_10 = int(record.get("hit_10_at_ms") or 0)
            hit_minus_5 = int(record.get("hit_minus_5_at_ms") or 0)
            deadline = first_at + 12 * 60 * 60 * 1000
            if hit_10 and hit_10 <= deadline and (not hit_minus_5 or hit_10 < hit_minus_5):
                verdict = "성공"
            elif hit_minus_5 and hit_minus_5 <= min(deadline, finalized_at_ms):
                verdict = "실패"
            elif finalized_at_ms >= deadline:
                verdict = "실패"
            else:
                verdict = "진행 중"
            record["daily_verdict"] = verdict
            record["hit_10_elapsed_minutes"] = (
                round((hit_10 - first_at) / 60_000, 2) if hit_10 else None
            )
        self.daily_history[day_key] = {
            "engine": "BES Flow CONFIRM Re-entry V2.8",
            "start_at_ms": start,
            "end_at_ms": end,
            "finalized_at_ms": finalized_at_ms,
            "daily_counts": self.daily_counts.get(day_key, {}),
            "performance_records": records,
        }
        self.daily_history = {key: self.daily_history[key] for key in sorted(self.daily_history)[-90:]}

    def rollover_daily_history(self, now: int) -> None:
        current_key = kst_session_date(now)
        if current_key == self.active_session_key:
            return
        self.archive_completed_session(self.active_session_key, now)
        self.active_session_key = current_key

    def daily_row(self, coin: Coin, now: int) -> dict[str, Any]:
        day = self.daily_counts.setdefault(kst_session_date(now), {})
        return day.setdefault(coin.market, {
            "symbol": coin.market.split("-", 1)[1], "total_count": 0, "a_count": 0,
            "pre_a_count": 0, "a_defense_count": 0,
            "second_count": 0, "actionable_count": 0, "last_counted_at_ms": 0,
            "last_seen_at_ms": 0, "prices": [], "counted_signal_ids": [],
            "a_signal_ids": [], "pre_a_signal_ids": [], "a_defense_signal_ids": [],
            "second_signal_ids": [], "actionable_signal_ids": [],
        })

    def count_new_detection(self, coin: Coin, now: int, price: float) -> None:
        row = self.daily_row(coin, now)
        row["last_seen_at_ms"] = now
        last = int(row.get("last_counted_at_ms", 0))
        if last and now - last < REDETECTION_GAP_MS:
            return
        if coin.last_sequence_ended_at and now - coin.last_sequence_ended_at < REDETECTION_GAP_MS:
            return
        signal_id = f"{coin.market}-{coin.flow_first_at}"
        row["total_count"] = int(row.get("total_count", 0)) + 1
        row["last_counted_at_ms"] = now
        row.setdefault("prices", []).append(round(price, 8))
        row["prices"] = row["prices"][-48:]
        row.setdefault("counted_signal_ids", []).append(signal_id)
        row["counted_signal_ids"] = row["counted_signal_ids"][-48:]

    def count_stage_once(self, coin: Coin, now: int, kind: str) -> None:
        row = self.daily_row(coin, now)
        signal_id = f"{coin.market}-{coin.flow_first_at}"
        if signal_id not in row.get("counted_signal_ids", []):
            return
        key, ids_key = {
            "a": ("a_count", "a_signal_ids"),
            "pre_a": ("pre_a_count", "pre_a_signal_ids"),
            "a_defense": ("a_defense_count", "a_defense_signal_ids"),
            "second": ("second_count", "second_signal_ids"),
            "actionable": ("actionable_count", "actionable_signal_ids"),
        }[kind]
        ids = row.setdefault(ids_key, [])
        if signal_id in ids:
            return
        ids.append(signal_id)
        row[key] = int(row.get(key, 0)) + 1

    def public_daily_count(self, coin: Coin, now: int) -> dict[str, Any]:
        row = self.daily_row(coin, now)
        prices = [float(value) for value in row.get("prices", [])]
        if len(prices) < 2:
            trend = "비교 전"
        elif prices[-1] > prices[0] * 1.003:
            trend = "포착가 상승형"
        elif prices[-1] < prices[0] * 0.997:
            trend = "포착가 하락형"
        else:
            trend = "포착가 보합형"
        return {
            "daily_detection_count": int(row.get("total_count", 0)),
            "daily_a_count": int(row.get("a_count", 0)),
            "daily_pre_a_count": int(row.get("pre_a_count", 0)),
            "daily_a_defense_count": int(row.get("a_defense_count", 0)),
            "daily_second_count": int(row.get("second_count", 0)),
            "daily_actionable_count": int(row.get("actionable_count", 0)),
            "daily_last_seen_at_ms": int(row.get("last_seen_at_ms", 0)),
            "daily_price_trend": trend,
            "daily_detection_prices": prices,
        }

    def top_daily_counts(self, now: int) -> list[dict[str, Any]]:
        day = self.daily_counts.get(kst_session_date(now), {})
        ranked = []
        for market, row in day.items():
            total = int(row.get("total_count", 0))
            if total <= 0:
                continue
            prices = [float(value) for value in row.get("prices", [])]
            if len(prices) < 2:
                trend = "비교 전"
            elif prices[-1] > prices[0] * 1.003:
                trend = "포착가 상승형"
            elif prices[-1] < prices[0] * 0.997:
                trend = "포착가 하락형"
            else:
                trend = "포착가 보합형"
            ranked.append({
                "market": market,
                "symbol": row.get("symbol") or market.split("-", 1)[-1],
                "count": total,
                "a_count": int(row.get("a_count", 0)),
                "pre_a_count": int(row.get("pre_a_count", 0)),
                "a_defense_count": int(row.get("a_defense_count", 0)),
                "second_count": int(row.get("second_count", 0)),
                "actionable_count": int(row.get("actionable_count", 0)),
                "last_seen_at_ms": int(row.get("last_seen_at_ms", 0)),
                "price_trend": trend,
            })
        ranked.sort(key=lambda row: (-row["count"], -row["second_count"],
                                    -row["actionable_count"], -row["last_seen_at_ms"], row["symbol"]))
        return [dict(row, rank=index) for index, row in enumerate(ranked[:5], 1)]

    def set_abc_stage(self, coin: Coin, stage: str, reason: str, now: int) -> None:
        if coin.abc_stage == stage:
            coin.abc_reason = reason
            return
        coin.abc_stage = stage
        coin.abc_reason = reason
        coin.abc_updated_at = now
        if stage == "PRE-A":
            self.count_stage_once(coin, now, "pre_a")
        elif stage == "A 방어":
            self.count_stage_once(coin, now, "a_defense")

    def update_abc_context(self, coin: Coin, fast: dict[str, Any], confirmed: dict[str, Any],
                           current: float, now: int) -> None:
        if current <= 0:
            return
        if coin.abc_a_price and current < coin.abc_a_price * 0.99:
            self.set_abc_stage(coin, "A 실패", "A 기준 저점 이탈", now)
            return
        confirmed_price = confirmed.get("price") if confirmed.get("defended") else None
        if not coin.abc_stage or coin.abc_stage == "A 실패":
            candidate_price = confirmed_price or (fast.get("price") if fast.get("candidate") else None)
            if (coin.abc_stage == "A 실패" and candidate_price and coin.abc_a_price
                    and float(candidate_price) >= coin.abc_a_price * 0.985):
                return
            if candidate_price:
                coin.abc_cycle_id = now
                coin.abc_a_price = float(candidate_price)
                coin.abc_b_price = None
                coin.abc_c_price = None
                if confirmed_price:
                    self.set_abc_stage(coin, "A 확인", "4H 피벗 A와 저점 방어 확인", now)
                else:
                    self.set_abc_stage(coin, "PRE-A", str(fast.get("reason")), now)
            return
        if coin.abc_stage == "PRE-A":
            if confirmed_price and abs(pct(float(confirmed_price), float(coin.abc_a_price))) <= 4.0:
                coin.abc_a_price = float(confirmed_price)
                self.set_abc_stage(coin, "A 확인", "4H 피벗 A 확정", now)
            elif coin.flow_second_at and current >= float(coin.abc_a_price):
                self.set_abc_stage(coin, "A 방어", "A 저점 유지·반복 수급 확인", now)
        elif coin.abc_stage == "A 방어" and confirmed_price:
            coin.abc_a_price = float(confirmed_price)
            self.set_abc_stage(coin, "A 확인", "4H 피벗 A 확정", now)

    def update_abc_live(self, coin: Coin, current: float, now: int) -> None:
        if not coin.abc_stage or not coin.abc_a_price or current <= 0:
            return
        if current < coin.abc_a_price * 0.99:
            self.set_abc_stage(coin, "A 실패", "A 기준 저점 이탈", now)
            return
        if coin.abc_stage == "PRE-A" and coin.flow_second_at:
            self.set_abc_stage(coin, "A 방어", "A 저점 유지·반복 수급 확인", now)
        if coin.abc_stage in {"A 확인", "B 진행"}:
            coin.abc_b_price = max(float(coin.abc_b_price or current), current)
            if coin.abc_b_price >= coin.abc_a_price * 1.02:
                self.set_abc_stage(coin, "B 진행", "A 이후 2% 이상 반등", now)
            if (coin.abc_stage == "B 진행" and current <= coin.abc_b_price * 0.99
                    and current > coin.abc_a_price * 1.01):
                coin.abc_c_price = current
                self.set_abc_stage(coin, "C 눌림 대기", "B 이후 눌림·A 저점 유지", now)
        elif coin.abc_stage == "C 눌림 대기":
            coin.abc_c_price = min(float(coin.abc_c_price or current), current)
            if coin.abc_b_price and current >= coin.abc_b_price * 1.003:
                self.set_abc_stage(coin, "ABC 확인", "C 방어 후 B 고점 돌파", now)

    @staticmethod
    def confirm_time_ms(value: str) -> int:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return int(parsed.timestamp() * 1000)
        except (TypeError, ValueError):
            return 0

    def apply_pine_h4_structure(self, coin: Coin, structure: dict[str, Any], now: int) -> None:
        active_stage = str(structure.get("stage", "구조 대기"))
        if active_stage in {"PRE-A", "B 진행", "C 눌림 대기"}:
            self.set_abc_stage(coin, active_stage, "Pine 4H Pivot 12 구조 추적", now)
            coin.abc_a_price = structure.get("a")
            coin.abc_b_price = structure.get("b")
            coin.abc_c_price = structure.get("c")
        confirmed = structure.get("last_confirm")
        if not isinstance(confirmed, dict):
            return
        confirmed_at = self.confirm_time_ms(str(confirmed.get("confirm_time", "")))
        if confirmed_at <= int(coin.confirm_at or 0):
            return
        coin.confirm_at = confirmed_at
        coin.confirm_price = float(confirmed["confirm_price"])
        coin.confirm_a_price = float(confirmed["a"])
        coin.confirm_b_price = float(confirmed["b"])
        coin.confirm_c_price = float(confirmed["c"])
        coin.abc_stage = "ABC 확인"
        coin.abc_a_price = coin.confirm_a_price
        coin.abc_b_price = coin.confirm_b_price
        coin.abc_c_price = coin.confirm_c_price
        coin.abc_updated_at = confirmed_at
        coin.abc_reason = "4H 종가 B 돌파·파란 CONFIRM"
        coin.entry_cycle_state = "CONFIRM 재확인 대기"
        coin.entry_attempt_count = 0
        coin.entry_attempt_price = None
        coin.entry_stop_price = None
        coin.entry_stopped_at = None
        coin.entry_cycle_reason = "CONFIRM 가격 눌림과 재수급 대기"

    def update_confirm_entry(self, coin: Coin, row: dict[str, float], now: int) -> None:
        price = float(row.get("price", 0.0))
        confirm = float(coin.confirm_price or 0.0)
        base = float(coin.confirm_a_price or 0.0)
        if not price or not confirm or not base:
            return
        if price < base:
            coin.entry_cycle_state = "구조 종료"
            coin.entry_cycle_reason = "4H A 기준 저점 붕괴"
            return
        distance = pct(price, confirm)
        if coin.entry_cycle_state == "추격 금지" and -2.0 <= distance <= 5.0:
            coin.entry_cycle_state = "CONFIRM 재확인 대기"
            coin.entry_cycle_reason = "과열 해소·CONFIRM 부근 재수급 대기"
        if price > confirm * 1.15 and coin.entry_cycle_state not in {"첫 시도", "재진입"}:
            coin.entry_cycle_state = "추격 금지"
            coin.entry_cycle_reason = "CONFIRM 대비 15% 초과 상승"
            return
        if coin.entry_cycle_state in {"첫 시도", "재진입"} and coin.entry_stop_price:
            if price <= coin.entry_stop_price:
                coin.entry_cycle_state = "단기 실패"
                coin.entry_stopped_at = now
                coin.entry_cycle_reason = "단기 방어선 이탈·A 구조는 별도 확인"
                return
            if coin.entry_attempt_price and price >= coin.entry_attempt_price * 1.05:
                coin.entry_cycle_state = "단기 성공"
                coin.entry_cycle_reason = "시도 가격 대비 +5% 도달"
                return
        repeated = coin.flow_second_at is not None
        buy_flow = row.get("buy_30s", 0.0) >= 0.52 and row.get("buy_1m", 0.0) >= 0.50
        calm = row.get("change_3m", 0.0) < 3.0
        support = max(float(coin.confirm_c_price or base), confirm * 0.97)
        stop = support * 0.995
        stop_distance = abs(pct(stop, price))
        setup = -2.0 <= distance <= 5.0 and repeated and buy_flow and calm and stop_distance <= 5.0
        if not setup:
            return
        if coin.entry_cycle_state == "CONFIRM 재확인 대기":
            coin.entry_cycle_state = "첫 시도"
            coin.entry_attempt_count = 1
            coin.entry_attempt_price = price
            coin.entry_stop_price = stop
            coin.entry_cycle_reason = "CONFIRM 부근 방어·2차 수급 재유입"
        elif (coin.entry_cycle_state == "단기 실패" and coin.entry_stopped_at
              and coin.flow_first_at and coin.flow_first_at > coin.entry_stopped_at
              and price >= confirm):
            coin.entry_cycle_state = "재진입"
            coin.entry_attempt_count += 1
            coin.entry_attempt_price = price
            coin.entry_stop_price = stop
            coin.entry_cycle_reason = "단기 실패 후 CONFIRM 재회복·새 수급"

    async def refresh_a_context(self, coin: Coin) -> None:
        now = int(time.time() * 1000)
        if self.session is None or coin.market in self.a_refreshing or now - coin.a_checked_at < A_CONTEXT_REFRESH_MS:
            return
        self.a_refreshing.add(coin.market)
        try:
            async with self.session.get(f"{REST_API}/candles/minutes/240",
                                        params={"market": coin.market, "count": 200}) as response:
                response.raise_for_status()
                candles = await response.json()
            current = coin.ticks[-1].price if coin.ticks else 0.0
            context = analyze_a_context(candles, current)
            radar = analyze_early_bottom_radar(candles, current)
            pine_structure = analyze_pine_h4_bull(candles)
            # Daily candles are requested only for active candidates and no more
            # than once per 15 minutes, keeping API traffic and Railway cost low.
            active_candidate = bool(coin.flow_stage or coin.abc_stage or coin.entry_cycle_state or coin.radar_stage)
            if active_candidate and now - coin.swing_checked_at >= BTC_REGIME_REFRESH_MS:
                async with self.session.get(f"{REST_API}/candles/days",
                                            params={"market": coin.market, "count": 40}) as response:
                    response.raise_for_status()
                    daily_candles = await response.json()
                swing = analyze_btc_swing_regime(daily_candles, candles)
                coin.swing_checked_at = now
                coin.daily_state = str(swing["daily"])
                coin.h4_state = str(swing["four_hour"])
                coin.swing_state = str(swing["state"])
                coin.swing_buy_zone = bool(swing["buy_zone"])
            coin.a_checked_at = now
            coin.radar_checked_at = now
            coin.a_near = bool(context["near"])
            coin.a_price = context["price"]
            coin.a_distance_percent = context["distance"]
            coin.a_defended = bool(context["defended"])
            coin.a_reason = str(context["reason"])
            previous_radar_stage = coin.radar_stage
            coin.radar_score = int(radar.get("score", 0))
            coin.radar_stage = str(radar.get("stage", ""))
            coin.radar_reason = str(radar.get("reason", "조건 누적 중"))
            coin.radar_l1_price = radar.get("l1_price")
            coin.radar_l2_price = radar.get("l2_price")
            coin.radar_atr_percent = radar.get("atr_percent")
            coin.radar_obv_divergence = bool(radar.get("obv_divergence"))
            coin.radar_volume_contraction = bool(radar.get("volume_contraction"))
            coin.radar_ema_recovered = bool(radar.get("ema_recovered"))
            coin.radar_breakout_ready = bool(radar.get("breakout_ready"))
            if coin.radar_stage and not previous_radar_stage:
                recent = next((item for item in reversed(self.early_radar_records)
                               if item.get("market") == coin.market), None)
                if recent is None or now - int(recent.get("first_at_ms", 0)) >= EARLY_RADAR_REDETECTION_MS:
                    coin.radar_first_at = now
                    coin.radar_first_price = current
                    self.early_radar_records.append({
                        "radar_id": f"{coin.market}-{now}", "market": coin.market,
                        "symbol": coin.market.split("-", 1)[1], "engine_version": "V2.8-early-radar-1",
                        "first_at_ms": now, "first_price": current, "first_stage": coin.radar_stage,
                        "first_score": coin.radar_score, "reason": coin.radar_reason,
                        "l1_price": coin.radar_l1_price, "l2_price": coin.radar_l2_price,
                        "atr_percent": coin.radar_atr_percent, "peak_return": 0.0,
                        "mae": 0.0, "current_return": 0.0, "status": "진행 중",
                    })
            self.apply_pine_h4_structure(coin, pine_structure, now)
            signal_id = f"{coin.market}-{coin.flow_first_at}"
            record = next((item for item in reversed(self.performance_records)
                           if item.get("signal_id") == signal_id), None)
            if record is not None:
                record["challenger_route"] = coin.a_near
                record["detection_route"] = "A+수급" if coin.a_near else "기존 수급"
                record["a_price"] = coin.a_price
                record["a_distance_percent"] = coin.a_distance_percent
                record["a_reason"] = coin.a_reason
            if coin.a_near:
                self.count_stage_once(coin, now, "a")
        except Exception as exc:
            coin.a_checked_at = now
            coin.a_reason = f"4시간봉 조회 실패: {type(exc).__name__}"
        finally:
            self.a_refreshing.discard(coin.market)

    def update_early_radar_performance(self, coin: Coin, price: float, now: int) -> None:
        for record in self.early_radar_records:
            if record.get("market") != coin.market or not record.get("first_at_ms"):
                continue
            age = now - int(record["first_at_ms"])
            if age < 0 or age > 24 * 60 * 60_000 or not record.get("first_price"):
                continue
            ret = pct(price, float(record["first_price"]))
            record["current_return"] = round(ret, 4)
            record["peak_return"] = round(max(float(record.get("peak_return", 0.0)), ret), 4)
            record["mae"] = round(min(float(record.get("mae", 0.0)), ret), 4)
            if ret >= 10.0 and not record.get("hit_10_at_ms"):
                record["hit_10_at_ms"] = now
            if ret <= -5.0 and not record.get("hit_minus_5_at_ms"):
                record["hit_minus_5_at_ms"] = now
            hit_10 = int(record.get("hit_10_at_ms") or 0)
            hit_minus_5 = int(record.get("hit_minus_5_at_ms") or 0)
            if hit_10 and (not hit_minus_5 or hit_10 < hit_minus_5):
                record["status"] = "성공"
            elif hit_minus_5 and (not hit_10 or hit_minus_5 < hit_10):
                record["status"] = "실패"
            elif age >= 12 * 60 * 60_000:
                record["status"] = "실패"

    def load_performance(self) -> list[dict[str, Any]]:
        try:
            rows = json.loads(PERFORMANCE_FILE.read_text(encoding="utf-8"))
            return rows if isinstance(rows, list) else []
        except (OSError, ValueError):
            return []

    def record_flow_transition(self, coin: Coin, previous: str, previous_first_at: int | None,
                               row: dict[str, float], now: int) -> None:
        signal_id = f"{coin.market}-{coin.flow_first_at or previous_first_at}"
        record = next((row for row in reversed(self.performance_records) if row.get("signal_id") == signal_id), None)
        if coin.flow_stage == "수급 1회" and record is None:
            self.count_new_detection(coin, now, float(row["price"]))
            self.performance_records.append({
                "signal_id": signal_id, "market": coin.market, "symbol": coin.market.split("-", 1)[1],
                "first_at_ms": coin.flow_first_at, "first_price": coin.flow_first_price,
                "first_flow": round(coin.flow_first_strength, 4), "status": "1차 포착",
                "peak_return": 0.0, "mae": 0.0, "snapshots": {},
                "champion_route": True, "challenger_route": coin.a_near,
                "detection_route": "A+수급" if coin.a_near else "기존 수급",
                "a_price": coin.a_price, "a_distance_percent": coin.a_distance_percent,
            })
            return
        if record is None:
            return
        if coin.flow_stage == "수급 2회 확인":
            self.count_stage_once(coin, now, "second")
            record.update({"second_at_ms": coin.flow_second_at, "second_price": float(row["price"]),
                           "second_flow": round(coin.flow_second_strength, 4), "status": "2회 확인",
                           "invalidation_price": coin.flow_invalidation_price})
            record["challenger_route"] = bool(coin.a_near)
            record["detection_route"] = "A+수급" if coin.a_near else "기존 수급"
            record["a_price"] = coin.a_price
            record["a_distance_percent"] = coin.a_distance_percent
        elif coin.flow_stage == "초입 검토":
            record.update({"confirmed_at_ms": now, "status": "진행 중"})
        elif coin.flow_stage == "탈락":
            record.update({"exited_at_ms": now, "status": "탈락", "exit_reason": coin.flow_exit_reason})
        elif not coin.flow_stage and previous == "수급 1회":
            record.update({"ended_at_ms": now, "status": "2회 미확인"})

    def update_flow_performance(self, coin: Coin, row: dict[str, float], now: int) -> None:
        records = [item for item in self.performance_records
                   if item.get("market") == coin.market and item.get("first_at_ms")
                   and now - int(item["first_at_ms"]) <= 24 * 60 * 60_000]
        for record in records:
            if not record.get("first_price"):
                continue
            ret = pct(float(row["price"]), float(record["first_price"]))
            record["current_return"] = round(ret, 4)
            record["peak_return"] = round(max(float(record.get("peak_return", 0.0)), ret), 4)
            record["mae"] = round(min(float(record.get("mae", 0.0)), ret), 4)
            for target, key in ((5.0, "hit_5_at_ms"), (10.0, "hit_10_at_ms")):
                if ret >= target and not record.get(key):
                    record[key] = now
            if ret <= -5.0 and not record.get("hit_minus_5_at_ms"):
                record["hit_minus_5_at_ms"] = now
            # Validate from the price where the scanner first said the coin was
            # actionable, not from the earlier flow-detection price.
            actionable_price = float(record.get("first_actionable_price") or 0.0)
            if actionable_price > 0:
                actionable_ret = pct(float(row["price"]), actionable_price)
                record["actionable_current_return"] = round(actionable_ret, 4)
                record["actionable_peak_return"] = round(
                    max(float(record.get("actionable_peak_return", 0.0)), actionable_ret), 4)
                record["actionable_mae"] = round(
                    min(float(record.get("actionable_mae", 0.0)), actionable_ret), 4)
                if actionable_ret >= 5.0 and not record.get("actionable_hit_5_at_ms"):
                    record["actionable_hit_5_at_ms"] = now
                if actionable_ret <= -3.0 and not record.get("actionable_hit_minus_3_at_ms"):
                    record["actionable_hit_minus_3_at_ms"] = now
            age = now - int(record["first_at_ms"])
            snapshots = record.setdefault("snapshots", {})
            for limit, key in ((30 * 60_000, "30m"), (60 * 60_000, "1h"), (180 * 60_000, "3h")):
                if age >= limit and key not in snapshots:
                    snapshots[key] = round(ret, 4)
            if age >= 180 * 60_000 and record["status"] not in {"탈락", "2회 미확인"}:
                record["status"] = "3시간 완료"

    def record_decision(self, coin: Coin, row: dict[str, float], now: int, btc_falling: bool) -> None:
        decision = trade_decision(coin, row, btc_falling)
        action = str(decision["action"])
        if action == coin.decision_action:
            return
        coin.decision_action = action
        coin.decision_changed_at = now
        signal_id = f"{coin.market}-{coin.flow_first_at}"
        record = next((item for item in reversed(self.performance_records)
                       if item.get("signal_id") == signal_id), None)
        if record is None:
            return
        event = {"at_ms": now, "action": action, "price": float(row["price"]),
                 "reason": decision["decision_reason"]}
        record.setdefault("decision_events", []).append(event)
        record["current_action"] = action
        if action in {"소액 시도 가능", "돌파 확인"} and not record.get("first_actionable_at_ms"):
            record["first_actionable_at_ms"] = now
            record["first_actionable_price"] = float(row["price"])
            record["first_actionable_type"] = action
        if action in {"소액 시도 가능", "돌파 확인"}:
            self.count_stage_once(coin, now, "actionable")

    async def bootstrap(self, session: ClientSession) -> None:
        self.session = session
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
            ticker = ticker_map.get(code, {})
            self.light_tickers[code] = {
                "price": float(ticker.get("trade_price", 0.0)),
                "trade_value_24h": float(ticker.get("acc_trade_price_24h", 0.0)),
                "trade_value_delta": 0.0,
                "signed_change_rate": float(ticker.get("signed_change_rate", 0.0)),
            }
        self.restore()
        self.select_detail_codes()

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
            "flow_cooldown_until",
            "last_sequence_ended_at",
            "decision_action", "decision_changed_at",
            "a_checked_at", "a_near", "a_price", "a_distance_percent", "a_defended", "a_reason",
            "abc_stage", "abc_cycle_id", "abc_a_price", "abc_b_price", "abc_c_price",
            "abc_updated_at", "abc_reason",
            "confirm_price", "confirm_at", "confirm_a_price", "confirm_b_price", "confirm_c_price",
            "entry_cycle_state", "entry_attempt_count", "entry_attempt_price", "entry_stop_price",
            "entry_stopped_at", "entry_cycle_reason",
            "radar_checked_at", "radar_score", "radar_stage", "radar_reason",
            "radar_first_at", "radar_first_price", "radar_l1_price", "radar_l2_price",
            "radar_atr_percent", "radar_obv_divergence", "radar_volume_contraction",
            "radar_ema_recovered", "radar_breakout_ready",
            "swing_checked_at", "daily_state", "h4_state", "swing_state", "swing_buy_zone",
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
            now = int(time.time() * 1000)
            self.rollover_daily_history(now)
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
                    "flow_cooldown_until": coin.flow_cooldown_until,
                    "last_sequence_ended_at": coin.last_sequence_ended_at,
                    "decision_action": coin.decision_action,
                    "decision_changed_at": coin.decision_changed_at,
                    "a_checked_at": coin.a_checked_at,
                    "a_near": coin.a_near,
                    "a_price": coin.a_price,
                    "a_distance_percent": coin.a_distance_percent,
                    "a_defended": coin.a_defended,
                    "a_reason": coin.a_reason,
                    "abc_stage": coin.abc_stage,
                    "abc_cycle_id": coin.abc_cycle_id,
                    "abc_a_price": coin.abc_a_price,
                    "abc_b_price": coin.abc_b_price,
                    "abc_c_price": coin.abc_c_price,
                    "abc_updated_at": coin.abc_updated_at,
                    "abc_reason": coin.abc_reason,
                    "confirm_price": coin.confirm_price,
                    "confirm_at": coin.confirm_at,
                    "confirm_a_price": coin.confirm_a_price,
                    "confirm_b_price": coin.confirm_b_price,
                    "confirm_c_price": coin.confirm_c_price,
                    "entry_cycle_state": coin.entry_cycle_state,
                    "entry_attempt_count": coin.entry_attempt_count,
                    "entry_attempt_price": coin.entry_attempt_price,
                    "entry_stop_price": coin.entry_stop_price,
                    "entry_stopped_at": coin.entry_stopped_at,
                    "entry_cycle_reason": coin.entry_cycle_reason,
                    "radar_checked_at": coin.radar_checked_at,
                    "radar_score": coin.radar_score,
                    "radar_stage": coin.radar_stage,
                    "radar_reason": coin.radar_reason,
                    "radar_first_at": coin.radar_first_at,
                    "radar_first_price": coin.radar_first_price,
                    "radar_l1_price": coin.radar_l1_price,
                    "radar_l2_price": coin.radar_l2_price,
                    "radar_atr_percent": coin.radar_atr_percent,
                    "radar_obv_divergence": coin.radar_obv_divergence,
                    "radar_volume_contraction": coin.radar_volume_contraction,
                    "radar_ema_recovered": coin.radar_ema_recovered,
                    "radar_breakout_ready": coin.radar_breakout_ready,
                    "swing_checked_at": coin.swing_checked_at,
                    "daily_state": coin.daily_state,
                    "h4_state": coin.h4_state,
                    "swing_state": coin.swing_state,
                    "swing_buy_zone": coin.swing_buy_zone,
                }
                for code, coin in self.coins.items()
            }
            temporary = STATE_FILE.with_suffix(".tmp")
            temporary.write_text(json.dumps({"coins": saved}, ensure_ascii=False), encoding="utf-8")
            temporary.replace(STATE_FILE)
            performance_tmp = PERFORMANCE_FILE.with_suffix(".tmp")
            performance_tmp.write_text(json.dumps(self.performance_records[-2000:], ensure_ascii=False), encoding="utf-8")
            performance_tmp.replace(PERFORMANCE_FILE)
            radar_tmp = EARLY_RADAR_FILE.with_suffix(".tmp")
            radar_tmp.write_text(json.dumps(self.early_radar_records[-4000:], ensure_ascii=False),
                                 encoding="utf-8")
            radar_tmp.replace(EARLY_RADAR_FILE)
            # Keep date-based counts separately; a new KST date naturally starts at zero.
            recent_days = sorted(self.daily_counts)[-90:]
            daily_tmp = DAILY_COUNT_FILE.with_suffix(".tmp")
            daily_tmp.write_text(json.dumps({day: self.daily_counts[day] for day in recent_days},
                                            ensure_ascii=False), encoding="utf-8")
            daily_tmp.replace(DAILY_COUNT_FILE)
            history_tmp = DAILY_HISTORY_FILE.with_suffix(".tmp")
            history_tmp.write_text(json.dumps(self.daily_history, ensure_ascii=False), encoding="utf-8")
            history_tmp.replace(DAILY_HISTORY_FILE)

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
                previous = float(self.previous_trade_values.get(code, value24))
                self.light_tickers[code] = {
                    "price": float(payload.get("trade_price", 0.0)),
                    "trade_value_24h": value24,
                    "trade_value_delta": max(0.0, value24 - previous),
                    "signed_change_rate": float(payload.get("signed_change_rate", 0.0)),
                }

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
                coin = self.coins[code]
                event = classify(coin, self.latest[code], self.percentiles.get(code, 0.0), now)
                previous_flow_stage = coin.flow_stage
                previous_first_at = coin.flow_first_at
                btc_row = self.latest.get("KRW-BTC", {})
                btc_falling = float(btc_row.get("change_3m", 0.0)) <= -0.35 or float(btc_row.get("change_1m", 0.0)) <= -0.20
                update_flow_sequence(coin, self.latest[code], now, btc_falling)
                self.update_confirm_entry(coin, self.latest[code], now)
                abc_tracking = coin.abc_stage in {"PRE-A", "A 방어", "A 확인", "B 진행", "C 눌림 대기"}
                confirm_tracking = coin.entry_cycle_state in {"CONFIRM 재확인 대기", "첫 시도", "단기 실패", "재진입"}
                row = self.latest[code]
                early_activity = (row.get("trade_count_30s", 0.0) >= 2
                                  and row.get("flow_3m", 0.0) >= 0.45
                                  and row.get("buy_1m", 0.0) >= 0.47)
                refresh_due = A_CONTEXT_REFRESH_MS if (coin.flow_stage or abc_tracking or confirm_tracking) else EARLY_RADAR_REFRESH_MS
                if (coin.flow_stage or abc_tracking or confirm_tracking or coin.radar_stage or early_activity) \
                        and now - coin.a_checked_at >= refresh_due:
                    asyncio.create_task(self.refresh_a_context(coin))
                if coin.flow_stage != previous_flow_stage:
                    self.record_flow_transition(coin, previous_flow_stage, previous_first_at, self.latest[code], now)
                self.update_flow_performance(coin, self.latest[code], now)
                self.update_early_radar_performance(coin, float(self.latest[code].get("price", 0.0)), now)
                self.record_decision(coin, self.latest[code], now, btc_falling)
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
                    detail_codes = sorted(self.select_detail_codes())
                    await socket.send_json([
                        {"ticket": f"bes-{uuid.uuid4()}"},
                        {"type": "ticker", "codes": codes, "isOnlyRealtime": True},
                        {"type": "trade", "codes": detail_codes, "isOnlyRealtime": True},
                        {"type": "orderbook", "codes": detail_codes, "isOnlyRealtime": True},
                    ])
                    self.connected = True
                    delay = 1
                    rotate_at = time.monotonic() + DETAIL_ROTATION_MS / 1000
                    while time.monotonic() < rotate_at:
                        try:
                            message = await socket.receive(timeout=30)
                        except asyncio.TimeoutError:
                            continue
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

    def _build_snapshot(self) -> dict[str, Any]:
        now = int(time.time() * 1000)
        session_start, session_end = kst_session_bounds(now)
        btc_row = self.latest.get("KRW-BTC", {})
        btc_falling = float(btc_row.get("change_3m", 0.0)) <= -0.35 or float(btc_row.get("change_1m", 0.0)) <= -0.20
        rows = []
        for code, coin in self.coins.items():
            radar_visible = bool(coin.radar_stage) and (
                not coin.radar_checked_at or now - coin.radar_checked_at <= 45 * 60_000
            )
            abc_visible = bool(coin.abc_stage) and (
                coin.abc_stage not in {"ABC 확인", "A 실패"} or now - coin.abc_updated_at <= 30 * 60_000
            )
            confirm_visible = coin.entry_cycle_state in {"CONFIRM 재확인 대기", "첫 시도", "단기 실패", "재진입"}
            if code not in self.latest or (not coin.flow_stage and not abc_visible
                                           and not confirm_visible and not radar_visible):
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
            row.update(trade_decision(coin, self.latest[code], btc_falling))
            early_structure_ok = bool(
                coin.a_near
                or coin.a_defended
                or coin.radar_stage in {"바닥 준비 관찰", "A 초기 후보", "돌파 준비"}
                or coin.abc_stage in {"PRE-A", "B 진행", "C 눌림 대기", "ABC 확인"}
            )
            if coin.entry_cycle_state in {"첫 시도", "재진입"}:
                row["action"] = "소액 시도 가능"
                row["decision_reason"] = coin.entry_cycle_reason
            elif coin.entry_cycle_state in {"추격 금지", "구조 종료"}:
                row["action"] = "매수 금지"
                row["decision_reason"] = coin.entry_cycle_reason
            elif row["action"] in {"소액 시도 가능", "돌파 확인"} and not early_structure_ok:
                row["action"] = "기다림"
                row["decision_reason"] = "바닥/A 구조 또는 4H CONFIRM 재확인 전"
            row["risk"] = "BTC 단기 하락" if btc_falling else "일반"
            row["stop_price_3pct"] = round(float(row["first_seen_price"]) * 0.97, 8) if row.get("first_seen_price") else None
            prices = [float(point[1]) for point in row.get("chart_prices", [])]
            row["recent_low"] = round(min(prices), 8) if prices else None
            row.update(self.public_daily_count(coin, now))
            row.update({"abc_stage": coin.abc_stage or "PRE-A 탐색", "abc_reason": coin.abc_reason,
                        "abc_a_price": coin.abc_a_price, "abc_b_price": coin.abc_b_price,
                        "abc_c_price": coin.abc_c_price, "abc_updated_at_ms": coin.abc_updated_at})
            row.update({"confirm_price": coin.confirm_price, "confirm_at_ms": coin.confirm_at,
                        "confirm_a_price": coin.confirm_a_price, "confirm_b_price": coin.confirm_b_price,
                        "confirm_c_price": coin.confirm_c_price,
                        "confirm_distance": round(pct(float(row["current_price"]), coin.confirm_price), 3) if coin.confirm_price else None,
                        "entry_cycle_state": coin.entry_cycle_state or "CONFIRM 구조 대기",
                        "entry_attempt_count": coin.entry_attempt_count,
                        "entry_attempt_price": coin.entry_attempt_price,
                        "entry_stop_price": coin.entry_stop_price,
                        "entry_cycle_reason": coin.entry_cycle_reason})
            row.update({"radar_score": coin.radar_score, "radar_stage": coin.radar_stage,
                        "radar_reason": coin.radar_reason, "radar_first_at_ms": coin.radar_first_at,
                        "radar_first_price": coin.radar_first_price,
                        "radar_l1_price": coin.radar_l1_price, "radar_l2_price": coin.radar_l2_price,
                        "radar_atr_percent": coin.radar_atr_percent,
                        "radar_obv_divergence": coin.radar_obv_divergence,
                        "radar_volume_contraction": coin.radar_volume_contraction,
                        "radar_ema_recovered": coin.radar_ema_recovered,
                        "radar_breakout_ready": coin.radar_breakout_ready})
            entry_price = float(coin.entry_attempt_price or row.get("current_price") or 0.0)
            stop_price = float(coin.entry_stop_price or row.get("decision_stop_price") or 0.0)
            target_price = entry_price * 1.05 if entry_price else 0.0
            risk = entry_price - stop_price if entry_price and 0 < stop_price < entry_price else 0.0
            reward = target_price - entry_price if entry_price else 0.0
            row.update({
                "daily_state": coin.daily_state,
                "h4_state": coin.h4_state,
                "swing_state": coin.swing_state,
                "swing_buy_zone": coin.swing_buy_zone,
                "review_price": round(entry_price, 8) if entry_price else None,
                "review_stop_price": round(stop_price, 8) if stop_price else None,
                "review_stop_percent": round(pct(stop_price, entry_price), 2) if risk else None,
                "review_target_price": round(target_price, 8) if target_price else None,
                "review_rr": round(reward / risk, 2) if risk else None,
            })
            rows.append(row)
        stage_order = {"돌파 확인": 0, "소액 시도 가능": 1, "기다림": 2, "매수 금지": 3}
        rows.sort(key=lambda row: (stage_order.get(row["action"], 9), -row["score"], -(row["first_seen_at"] or 0)))
        for row in rows:
            a_price = float(row.get("abc_a_price") or 0.0)
            current = float(row.get("current_price") or 0.0)
            a_distance = pct(current, a_price) if a_price and current else None
            row["abc_current_distance"] = round(a_distance, 3) if a_distance is not None else None
            stage = str(row.get("abc_stage", ""))
            row["entry_review"] = (
                row.get("entry_cycle_state") in {"첫 시도", "재진입"}
                or row.get("action") in {"소액 시도 가능", "돌파 확인"}
            )
            if row.get("entry_cycle_state") in {"첫 시도", "재진입"}:
                row["entry_review_reason"] = row.get("entry_cycle_reason")
            elif row["entry_review"]:
                row["entry_review_reason"] = row.get("decision_reason")
            elif row.get("entry_cycle_state") == "CONFIRM 재확인 대기":
                row["entry_review_reason"] = "파란 CONFIRM 가격 눌림·재수급 대기"
            elif row.get("entry_cycle_state") == "단기 실패":
                row["entry_review_reason"] = "A 유지 시 CONFIRM 재회복·새 수급 대기"
            else:
                row["entry_review_reason"] = row.get("entry_cycle_reason", "조건 재확인 필요")
        entry_review = [row for row in rows if row.get("entry_review")]
        entry_review.sort(key=lambda row: (
            {"재진입": 0, "첫 시도": 1}.get(row.get("entry_cycle_state"), 9),
            -int(row.get("score", 0))))
        a_tracking = [row for row in rows if not row.get("entry_review")
                      and (row.get("radar_stage") in {"바닥 준비 관찰", "A 초기 후보", "돌파 준비"}
                           or row.get("abc_stage") in {"PRE-A", "B 진행", "C 눌림 대기"}
                           or row.get("entry_cycle_state") in {"CONFIRM 재확인 대기", "단기 실패"})]
        a_tracking.sort(key=lambda row: (
            {"단기 실패": 0, "CONFIRM 재확인 대기": 1}.get(row.get("entry_cycle_state"),
                {"돌파 준비": 2, "A 초기 후보": 3, "바닥 준비 관찰": 4}.get(
                    row.get("radar_stage"),
                    {"C 눌림 대기": 5, "B 진행": 6, "PRE-A": 7}.get(row.get("abc_stage"), 9))),
            -int(row.get("daily_pre_a_count", 0)),
            -int(row.get("score", 0)),
        ))
        actionable_records = [record for record in self.performance_records
                              if record.get("first_actionable_at_ms")]
        actionable_records.sort(key=lambda record: int(record.get("first_actionable_at_ms") or 0), reverse=True)
        recent_actionable = actionable_records[:20]
        hit_5_first = 0
        hit_minus_3_first = 0
        pending = 0
        for record in recent_actionable:
            hit_5 = int(record.get("actionable_hit_5_at_ms") or 0)
            hit_minus_3 = int(record.get("actionable_hit_minus_3_at_ms") or 0)
            if hit_5 and (not hit_minus_3 or hit_5 < hit_minus_3):
                hit_5_first += 1
            elif hit_minus_3 and (not hit_5 or hit_minus_3 < hit_5):
                hit_minus_3_first += 1
            else:
                pending += 1
        return {
            "engine": "BES Flow CONFIRM Re-entry V2.8",
            "connected": self.connected,
            "updated_at_ms": self.updated_at,
            "market_count": len(self.coins),
            "detail_market_count": len(self.detail_codes),
            "scan_mode": "저비용 스윙 모드",
            "candidate_count": len(rows),
            "buy_review_count": sum(row["action"] in {"소액 시도 가능", "돌파 확인"} for row in rows),
            "champion_count": sum(row.get("detection_route") == "기존 수급" for row in rows),
            "challenger_count": sum(row.get("detection_route") == "A+수급" for row in rows),
            "btc_market": {**self.btc_swing_regime,
                           "short_warning": "단기 급락 주의" if btc_falling else "없음",
                           "blocking": False},
            "results": rows,
            "events": self.events[-100:],
            "performance_records": self.performance_records[-200:],
            "counting_window": {"start_at_ms": session_start, "end_at_ms": session_end,
                                "label": "매일 오전 9시 ~ 다음 날 오전 9시 (KST)"},
            "top_detection_counts": self.top_daily_counts(now),
            "entry_review_results": entry_review[:3],
            "a_tracking_results": a_tracking[:5],
            "early_radar_count": sum(bool(row.get("radar_stage")) for row in rows),
            "early_radar_results": [row for row in a_tracking if row.get("radar_stage")][:10],
            "daily_counts": self.daily_counts.get(kst_session_date(now), {}),
            "actionable_validation": {
                "sample_count": len(recent_actionable),
                "hit_5_first": hit_5_first,
                "hit_minus_3_first": hit_minus_3_first,
                "pending": pending,
                "win_rate": round(hit_5_first / max(1, hit_5_first + hit_minus_3_first) * 100.0, 1),
            },
        }

    def snapshot(self) -> dict[str, Any]:
        """Publish one stable, complete dashboard decision set every 15 minutes."""
        now = int(time.time() * 1000)
        # In low-cost mode only the rotating detail set builds trade/orderbook
        # rows. Readiness must therefore follow that set, not all ticker-only
        # markets, or a fresh deployment could never publish a stable snapshot.
        detailed_rows = set(self.latest).intersection(self.detail_codes)
        ready = bool(self.detail_codes) and len(detailed_rows) >= max(
            1, int(len(self.detail_codes) * 0.80)
        )
        due = self.published_snapshot is None or now - self.published_at >= DISPLAY_SNAPSHOT_MS

        if ready and due:
            self.published_snapshot = self._build_snapshot()
            self.published_at = now

        if self.published_snapshot is None:
            result = self._build_snapshot()
            result["snapshot_status"] = "데이터 준비 중"
            result["snapshot_complete"] = False
            return result

        # Keep the candidate membership stable, while reporting the current
        # connection health separately.
        result = dict(self.published_snapshot)
        result["connected"] = self.connected
        result["snapshot_status"] = "3분 확정본"
        result["snapshot_complete"] = True
        result["snapshot_at_ms"] = self.published_at
        result["next_snapshot_at_ms"] = self.published_at + DISPLAY_SNAPSHOT_MS
        return result


async def main() -> None:
    scanner = Scanner()
    async with ClientSession(headers={"User-Agent": "bes-realtime-scanner/1.0"}) as session:
        await scanner.bootstrap(session)
        app = web.Application()
        app.router.add_get("/api/state", lambda _: web.json_response(scanner.snapshot()))
        app.router.add_get("/api/performance", lambda _: web.json_response({
            "engine": "BES Flow CONFIRM Re-entry V2.8",
            "records": scanner.performance_records[-2000:],
        }))
        app.router.add_get("/api/daily-history", lambda _: web.json_response({
            "engine": "BES Flow CONFIRM Re-entry V2.8",
            "days": scanner.daily_history,
        }))
        app.router.add_get("/api/early-radar-performance", lambda _: web.json_response({
            "engine": "BES V2.8 Early Bottom Radar 1",
            "records": scanner.early_radar_records[-4000:],
        }))
        app.router.add_get("/health", lambda _: web.json_response({"ok": scanner.connected, "markets": len(scanner.coins)}))
        app.router.add_static("/", STATIC_DIR, show_index=True)
        runner = web.AppRunner(app)
        await runner.setup()
        await web.TCPSite(runner, "0.0.0.0", int(os.environ.get("PORT", "8080"))).start()
        await asyncio.gather(scanner.stream(session), scanner.evaluate(), scanner.save_loop(),
                             scanner.refresh_btc_swing_regime())


if __name__ == "__main__":
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    asyncio.run(main())
