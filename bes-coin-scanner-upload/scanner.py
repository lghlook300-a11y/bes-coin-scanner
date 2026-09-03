#!/usr/bin/env python3
"""Bithumb KRW 15-minute dual-route momentum scanner.

The former ABC-only scanner is preserved in bes_core.py for comparison.
This production scanner ranks early price acceleration, trading-value expansion,
recent-high breakouts, relative strength, and liquidity.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
import json
import math
from pathlib import Path
import re
import time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

API = "https://api.bithumb.com/v1"
OUT = Path("data/latest.json")
WATCH_STATE = Path("data/watch_state.json")
STABLE_SYMBOLS = {"USDT", "USDC", "DAI", "TUSD", "FDUSD", "USDE", "PYUSD"}
MIN_TRADE_VALUE_24H = 500_000_000
MAX_RESULTS = 20
TRACK_HOURS = 12
SUCCESS_RETURN = 10.0
FAILURE_RETURN = -5.0
COMPLETED_HISTORY_LIMIT = 1000
WATCH_HOURS = 12
MIN_WATCH_HOURS = 6
MAX_WATCHLIST = 500
PAPER_START_CASH = 1_000_000.0
PAPER_POSITION_KRW = 200_000.0
PAPER_MAX_POSITIONS = 3
PAPER_FEE_RATE = 0.0004
PAPER_SLIPPAGE_RATE = 0.001
PAPER_HISTORY_LIMIT = 500
PAPER_PENDING_MINUTES = 45
MIN_ROTATION_PRICE = 0.01
SAFETY_RESEARCH_ONLY = True

PAPER_STRATEGIES = {
    "A_EARLY": "A 초입형",
    "B_BREAKOUT": "B 돌파형",
    "C_PULLBACK": "C 눌림형",
    "D_CHAMPION": "D 현행형",
    "E_ROTATION": "E 자금순환형",
}


def api_get(path: str, params: dict[str, Any] | None = None) -> Any:
    url = f"{API}{path}"
    if params:
        url += "?" + urlencode(params)
    request = Request(url, headers={"User-Agent": "bes-bithumb-breakout/3.0"})
    for attempt in range(6):
        try:
            with urlopen(request, timeout=30) as response:
                return json.load(response)
        except HTTPError as exc:
            if exc.code not in {429, 500, 502, 503, 504} or attempt == 5:
                raise
        except (URLError, TimeoutError):
            if attempt == 5:
                raise
        time.sleep(min(2 ** attempt, 15))
    raise RuntimeError("Upbit API retry exhausted")


def ticker_map(markets: list[str]) -> dict[str, dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for index in range(0, len(markets), 100):
        rows.extend(api_get("/ticker", {"markets": ",".join(markets[index:index + 100])}))
        time.sleep(0.12)
    return {str(row["market"]): row for row in rows}


def completed_15m_candles(market: str, count: int = 200) -> list[dict[str, Any]]:
    rows = api_get("/candles/minutes/15", {"market": market, "count": count})
    now = datetime.now(timezone.utc)
    completed: list[dict[str, Any]] = []
    for row in rows:
        started = datetime.fromisoformat(row["candle_date_time_utc"]).replace(tzinfo=timezone.utc)
        if started + timedelta(minutes=15) <= now:
            completed.append(row)
    completed.reverse()
    return completed


def ema(values: list[float], length: int) -> float:
    alpha = 2.0 / (length + 1.0)
    value = values[0]
    for item in values[1:]:
        value = alpha * item + (1.0 - alpha) * value
    return value


def pct_change(new: float, old: float) -> float:
    return ((new / old) - 1.0) * 100.0 if old > 0 else 0.0


def percentile_ranks(rows: list[dict[str, Any]], field: str) -> dict[str, float]:
    ordered = sorted(rows, key=lambda row: float(row[field]))
    denominator = max(1, len(ordered) - 1)
    return {row["market"]: index / denominator for index, row in enumerate(ordered)}


def scan_market(market: str, ticker: dict[str, Any]) -> dict[str, Any]:
    candles = completed_15m_candles(market)
    if len(candles) < 120:
        raise ValueError(f"15m bars={len(candles)}")
    closes = [float(row["trade_price"]) for row in candles]
    last = candles[-1]
    close = closes[-1]
    prior_high_20 = max(float(row["high_price"]) for row in candles[-97:-1])
    prior_low_20 = min(float(row["low_price"]) for row in candles[-97:-1])
    values = [float(row.get("candle_acc_trade_price", 0.0)) for row in candles]
    baseline = sorted(values[-97:-1])[48]
    value_ratio_15m = values[-1] / baseline if baseline > 0 else 0.0
    value_ratio_1h = sum(values[-4:]) / (baseline * 4.0) if baseline > 0 else 0.0
    value_ratio_4h = sum(values[-16:]) / (baseline * 16.0) if baseline > 0 else 0.0
    open_price = float(last["opening_price"])
    high = float(last["high_price"])
    low = float(last["low_price"])
    candle_range = max(high - low, close * 1e-9)
    lows = [float(row["low_price"]) for row in candles]
    highs = [float(row["high_price"]) for row in candles]
    hourly_lows = [min(lows[index:index + 4]) for index in range(len(lows) - 12, len(lows), 4)]
    higher_low_count = sum(
        hourly_lows[index] > hourly_lows[index - 1]
        for index in range(1, len(hourly_lows))
    )
    obv = 0.0
    obv_series: list[float] = []
    for index in range(1, len(candles)):
        if closes[index] > closes[index - 1]:
            obv += values[index]
        elif closes[index] < closes[index - 1]:
            obv -= values[index]
        obv_series.append(obv)
    obv_improving = len(obv_series) >= 17 and obv_series[-1] > obv_series[-17]
    prior_high_4h = max(highs[-17:-1])
    ema_20h = ema(closes[-80:], 80)
    return {
        "market": market,
        "symbol": market.removeprefix("KRW-"),
        "current_price": float(ticker["trade_price"]),
        "trade_value_24h": float(ticker["acc_trade_price_24h"]),
        "change_24h": float(ticker["signed_change_rate"]) * 100.0,
        "change_15m": pct_change(close, closes[-2]),
        "change_1h": pct_change(close, closes[-5]),
        "change_4h": pct_change(close, closes[-17]),
        "change_12h": pct_change(close, closes[-49]),
        "value_ratio_15m": value_ratio_15m,
        "value_ratio_1h": value_ratio_1h,
        "value_ratio_4h": value_ratio_4h,
        "breakout_20h": close > prior_high_20,
        "distance_to_high_20h": ((prior_high_20 - close) / close) * 100.0,
        "distance_to_high_4h": ((prior_high_4h - close) / close) * 100.0,
        "drawdown_from_high_4h": pct_change(close, prior_high_4h),
        "compression_24h": pct_change(prior_high_20, prior_low_20),
        "above_ema20": close > ema_20h,
        "distance_from_ema20": pct_change(close, ema_20h),
        "higher_low_count": higher_low_count,
        "obv_improving_4h": obv_improving,
        "close_position": (close - low) / candle_range,
        "upper_wick_ratio": (high - max(open_price, close)) / candle_range,
        "signal_candle_time": last["candle_date_time_utc"] + "Z",
        # Used only while evaluating a tracked episode. Removed before JSON output.
        "_tracking_bars": [
            {
                "time": row["candle_date_time_utc"] + "Z",
                "high": float(row["high_price"]),
                "low": float(row["low_price"]),
            }
            for row in candles[-50:]
        ],
    }


def stage_for(row: dict[str, Any]) -> str:
    change = float(row["change_12h"])
    if change >= 12.0 or row["change_1h"] >= 8.0:
        return "강한 상승"
    if change < 2.0:
        return "준비 관찰"
    if change < 8.0:
        return "초기 포착"
    if change < 15.0:
        return "돌파 진행"
    if change < 25.0:
        return "강한 상승"
    return "급등 후기"


def score_row(row: dict[str, Any], relative_strength: float) -> dict[str, Any]:
    acceleration = min(
        30.0,
        max(0.0, row["change_1h"] * 4.0)
        + max(0.0, row["change_4h"] * 1.5)
        + max(0.0, row["change_12h"] * 0.5),
    )
    flow_ratio = max(float(row["value_ratio_15m"]), float(row["value_ratio_1h"]))
    flow = min(25.0, max(0.0, 8.0 + (flow_ratio - 1.0) * 12.0))
    breakout = 20.0 if row["breakout_20h"] else max(0.0, 20.0 - max(0.0, row["distance_to_high_20h"]) * 6.0)
    strength = relative_strength * 15.0
    liquidity = min(10.0, max(2.0, 2.0 + 3.0 * math.log10(max(1.0, row["trade_value_24h"] / 1_000_000_000))))
    wick_penalty = 10.0 if row["upper_wick_ratio"] > 0.55 else 0.0
    score = round(max(0.0, min(100.0, acceleration + flow + breakout + strength + liquidity - wick_penalty)))
    return {
        **row,
        "stage": stage_for(row),
        "score": score,
        "relative_strength_percentile": round(relative_strength * 100.0, 1),
        "score_components": {
            "acceleration": round(acceleration, 1),
            "capital_flow": round(flow, 1),
            "breakout": round(breakout, 1),
            "relative_strength": round(strength, 1),
            "liquidity": round(liquidity, 1),
            "wick_penalty": round(wick_penalty, 1),
        },
    }


def btc_market_context(row: dict[str, Any] | None) -> dict[str, Any]:
    """Separate favorable, neutral, and falling BTC regimes."""
    if row is None:
        return {"state": "데이터 없음", "allows_entry": False, "reasons": ["BTC 데이터 없음"]}
    change_4h = float(row["change_4h"])
    change_12h = float(row["change_12h"])
    above_ema = bool(row["above_ema20"])
    falling = change_4h < 0.0 and (change_12h < 0.0 or not above_ema)
    favorable = change_4h > 0.0 and change_12h > 0.0 and above_ema
    state = "상승·안정" if favorable else ("하락·매수 금지" if falling else "중립·강한 종목만")
    reasons = ["BTC 하락 국면"] if falling else []
    return {
        "state": state,
        "allows_entry": favorable,
        "allows_strong_watch": not falling,
        "reasons": reasons,
        "change_4h": round(change_4h, 4),
        "change_12h": round(change_12h, 4),
        "above_ema20": above_ema,
    }


def safety_risk_reasons(row: dict[str, Any], btc: dict[str, Any]) -> list[str]:
    """Record conservative exclusions for later validation, without claiming safety."""
    reasons = list(btc.get("reasons", []))
    if float(row["change_12h"]) >= 8.0:
        reasons.append("최근 12시간 급등")
    if float(row["change_4h"]) <= -2.0:
        reasons.append("종목 4시간 하락")
    if not bool(row["above_ema20"]):
        reasons.append("종목 20시간 추세선 아래")
    if int(row["higher_low_count"]) < 1:
        reasons.append("저점 방어 미확인")
    if not bool(row["obv_improving_4h"]):
        reasons.append("4시간 자금흐름 개선 미확인")
    if float(row["upper_wick_ratio"]) > 0.45:
        reasons.append("긴 윗꼬리")
    return reasons


def initial_research_snapshot(row: dict[str, Any], btc: dict[str, Any]) -> dict[str, Any]:
    fields = (
        "change_15m", "change_1h", "change_4h", "change_12h",
        "value_ratio_15m", "value_ratio_1h", "value_ratio_4h",
        "distance_to_high_4h", "distance_from_ema20", "above_ema20",
        "higher_low_count", "obv_improving_4h", "close_position",
        "upper_wick_ratio", "trade_value_24h", "score",
    )
    return {
        "features": {name: row.get(name) for name in fields},
        "btc": btc,
        "risk_reasons": safety_risk_reasons(row, btc),
    }


def conservative_action(row: dict[str, Any], btc: dict[str, Any]) -> str:
    coin_reasons = safety_risk_reasons(row, {**btc, "reasons": []})
    strict = (
        not coin_reasons
        and int(row["rotation_score"]) >= 90
        and int(row["ignition_score"]) >= 65
        and int(row["preparation_score"]) >= 55
        and int(row["rotation_streak"]) >= 2
        and str(row["original_action"]) == "자금유입 2회 확인"
        and 0.0 <= float(row["change_12h"]) < 6.0
        and float(row["distance_from_ema20"]) < 5.0
    )
    if not btc.get("allows_strong_watch", False):
        return "시장 위험·제외"
    if btc.get("allows_entry", False) and strict:
        return "초입 검토"
    if strict and int(row["rotation_score"]) >= 95 and int(row["ignition_score"]) >= 75:
        return "강한 관찰"
    return "위험 제외" if coin_reasons else "조건 대기"


def classic_qualifies(row: dict[str, Any]) -> bool:
    has_acceleration = row["change_15m"] >= 0.35 or row["change_1h"] >= 1.2
    has_flow = row["value_ratio_15m"] >= 1.7 or row["value_ratio_1h"] >= 1.5
    near_or_breaking = row["breakout_20h"] or row["distance_to_high_20h"] <= 1.5
    compressed_or_early = row["compression_24h"] <= 12.0 or row["change_12h"] <= 8.0
    return (
        row["trade_value_24h"] >= MIN_TRADE_VALUE_24H
        and row["above_ema20"]
        and has_acceleration
        and has_flow
        and near_or_breaking
        and compressed_or_early
        and row["close_position"] >= 0.50
    )


def surge_qualifies(row: dict[str, Any], score: int) -> bool:
    """Catch fresh volume-led moves that are no longer tightly compressed."""
    has_acceleration = (
        row["change_15m"] >= 0.50
        or row["change_1h"] >= 1.80
        or row["change_4h"] >= 4.00
    )
    has_flow = (
        row["value_ratio_15m"] >= 1.40
        or row["value_ratio_1h"] >= 1.40
        or row["value_ratio_4h"] >= 1.80
    )
    return (
        row["trade_value_24h"] >= MIN_TRADE_VALUE_24H
        and row["above_ema20"]
        and score >= 75
        and row["change_12h"] < SUCCESS_RETURN
        and has_acceleration
        and has_flow
        and row["close_position"] >= 0.45
        and row["upper_wick_ratio"] <= 0.60
    )


def detection_route(row: dict[str, Any], score: int) -> str | None:
    # A coin already above the success threshold is shown as a late surge, not
    # opened as a new recommendation or counted in performance.
    if row["change_12h"] >= SUCCESS_RETURN:
        return None
    if classic_qualifies(row) and score >= 60:
        return "압축 초입형"
    if surge_qualifies(row, score):
        return "거래량 급증형"
    return None


def exclusion_reasons(row: dict[str, Any], score: int) -> list[str]:
    reasons: list[str] = []
    if row["change_12h"] >= SUCCESS_RETURN:
        reasons.append("이미 12시간 +10% 이상 급등")
    if row["trade_value_24h"] < MIN_TRADE_VALUE_24H:
        reasons.append("24시간 거래대금 부족")
    if not row["above_ema20"]:
        reasons.append("20시간 추세선 아래")
    if not (row["change_15m"] >= 0.35 or row["change_1h"] >= 1.2 or row["change_4h"] >= 4.0):
        reasons.append("가격 가속 부족")
    if not (row["value_ratio_15m"] >= 1.4 or row["value_ratio_1h"] >= 1.4 or row["value_ratio_4h"] >= 1.8):
        reasons.append("거래량 증가 부족")
    if row["close_position"] < 0.45:
        reasons.append("봉 종가 위치 약함")
    if row["upper_wick_ratio"] > 0.60:
        reasons.append("긴 윗꼬리")
    if score < 60:
        reasons.append("점수 60 미만")
    return reasons or ["세부 조합 조건 미충족"]


def preparation_score(row: dict[str, Any]) -> int:
    """Score conditions that normally appear before acceleration."""
    score = 0.0
    if row["trade_value_24h"] >= MIN_TRADE_VALUE_24H:
        score += 15.0
    score += max(0.0, 20.0 - max(0.0, float(row["compression_24h"]) - 4.0) * 2.5)
    if -2.0 <= float(row["change_1h"]) <= 3.0:
        score += 15.0
    if -3.0 <= float(row["change_4h"]) <= 5.0:
        score += 10.0
    score += min(15.0, float(row["higher_low_count"]) * 7.5)
    if row["obv_improving_4h"]:
        score += 15.0
    if row["above_ema20"]:
        score += 10.0
    score += min(15.0, float(row["relative_strength_percentile"]) * 0.15)
    if float(row["change_1h"]) >= 5.0 or float(row["change_12h"]) >= 8.0:
        score -= 25.0
    if float(row["distance_from_ema20"]) >= 7.0:
        score -= 20.0
    return round(max(0.0, min(100.0, score)))


def ignition_score(row: dict[str, Any]) -> int:
    """Score the first capital-flow expansion, before a full pump."""
    score = 0.0
    flow = max(
        float(row["value_ratio_15m"]),
        float(row["value_ratio_1h"]),
        float(row["value_ratio_4h"]),
    )
    score += min(30.0, max(0.0, (flow - 1.0) * 20.0))
    if 0.20 <= float(row["change_15m"]) <= 2.50:
        score += 20.0
    if -1.0 <= float(row["change_1h"]) < 5.0:
        score += 10.0
    if float(row["close_position"]) >= 0.65:
        score += 15.0
    if float(row["distance_to_high_4h"]) <= 3.0:
        score += 10.0
    if row["obv_improving_4h"]:
        score += 10.0
    if row["above_ema20"]:
        score += 5.0
    if float(row["upper_wick_ratio"]) > 0.55:
        score -= 25.0
    return round(max(0.0, min(100.0, score)))


def rotation_score(row: dict[str, Any]) -> int:
    """Detect fresh capital rotation without requiring a prior compression setup."""
    if float(row["current_price"]) < MIN_ROTATION_PRICE or is_pump_late(row):
        return 0
    score = 0.0
    ratio_15m = float(row["value_ratio_15m"])
    ratio_1h = float(row["value_ratio_1h"])
    flow_percentile = float(row.get("flow_percentile_15m", 0.0))
    if flow_percentile >= 90.0:
        score += 15.0
    elif flow_percentile >= 80.0:
        score += 8.0
    if ratio_15m >= 2.0:
        score += min(35.0, 20.0 + (ratio_15m - 2.0) * 7.5)
    if ratio_1h >= 1.35:
        score += min(20.0, 10.0 + (ratio_1h - 1.35) * 10.0)
    if 0.20 <= float(row["change_15m"]) <= 2.50:
        score += 15.0
    if -0.50 <= float(row["change_1h"]) < 5.0:
        score += 10.0
    if float(row["close_position"]) >= 0.65:
        score += 10.0
    if float(row["upper_wick_ratio"]) <= 0.45:
        score += 5.0
    if row["obv_improving_4h"]:
        score += 5.0
    if not row["above_ema20"] and not row["obv_improving_4h"]:
        score -= 20.0
    return round(max(0.0, min(100.0, score)))


def is_pump_late(row: dict[str, Any]) -> bool:
    return (
        float(row["change_1h"]) >= 8.0
        or float(row["change_12h"]) >= 12.0
        or float(row["distance_from_ema20"]) >= 10.0
        or (
            float(row["drawdown_from_high_4h"]) <= -8.0
            and float(row["change_4h"]) >= 5.0
        )
    )


def watch_stage(
    row: dict[str, Any], prep: int, ignition: int, rotation: int, rotation_streak: int
) -> tuple[str, str]:
    if is_pump_late(row):
        return "펌핑 후기", "추격 금지"
    if rotation >= 70 and rotation_streak >= 2 and float(row["change_1h"]) < 6.0:
        return "매수 검토", "자금유입 2회 확인"
    if rotation >= 65:
        return "자금 유입", "초입 관찰"
    if (
        row["breakout_20h"]
        and ignition >= 65
        and float(row["close_position"]) >= 0.65
    ):
        return "매수 검토", "매수 검토"
    if ignition >= 60 and float(row["distance_to_high_4h"]) <= 2.0:
        return "돌파 임박", "돌파 대기"
    if ignition >= 45:
        return "점화", "점화 확인"
    return "준비", "급등 준비"


def load_watch_state() -> dict[str, dict[str, Any]]:
    try:
        payload = json.loads(WATCH_STATE.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        # GitHub Actions already commits latest.json. This fallback keeps V5
        # state persistent even before the workflow is upgraded to commit the
        # dedicated state file as well.
        try:
            payload = json.loads(OUT.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return {}
    rows = payload.get("watchlist", []) if isinstance(payload, dict) else []
    return {
        str(row["market"]): row
        for row in rows
        if isinstance(row, dict) and row.get("market")
    }


def update_watch_state(
    scored_rows: dict[str, dict[str, Any]], now: datetime, now_iso: str
) -> list[dict[str, Any]]:
    previous = load_watch_state()
    watchlist: list[dict[str, Any]] = []
    for market, row in scored_rows.items():
        prep = preparation_score(row)
        ignition = ignition_score(row)
        rotation = rotation_score(row)
        prior = previous.get(market)
        rotation_streak = (
            int(prior.get("rotation_streak", 0)) + 1
            if prior is not None and rotation >= 65
            else (1 if rotation >= 65 else 0)
        )
        first_dt = parse_utc(prior.get("first_watch_at")) if prior else None
        age = max(0.0, (now - first_dt).total_seconds() / 3600.0) if first_dt else 0.0
        should_start = (
            (prep >= 55 or rotation >= 55)
            and float(row["trade_value_24h"]) >= MIN_TRADE_VALUE_24H
            and not is_pump_late(row)
        )
        should_keep = prior is not None and age < WATCH_HOURS and (
            age < MIN_WATCH_HOURS or prep >= 35 or ignition >= 35 or rotation >= 35
        )
        if not should_start and not should_keep:
            continue
        stage, action = watch_stage(row, prep, ignition, rotation, rotation_streak)
        first_price = float(prior.get("first_watch_price", row["current_price"])) if prior else float(row["current_price"])
        current_return = pct_change(float(row["current_price"]), first_price)
        item = {
            **public_row(row),
            "engine_version": "V5",
            "first_watch_at": prior.get("first_watch_at", now_iso) if prior else now_iso,
            "first_watch_price": first_price,
            "first_detected_at": prior.get("first_watch_at", now_iso) if prior else now_iso,
            "first_detected_price": first_price,
            "watch_age_hours": round(age, 2),
            "preparation_score": prep,
            "ignition_score": ignition,
            "rotation_score": rotation,
            "rotation_streak": rotation_streak,
            "watch_stage": stage,
            "action": action,
            "return_since_watch": round(current_return, 4),
            "peak_return_since_watch": round(max(float(prior.get("peak_return_since_watch", current_return)) if prior else current_return, current_return), 4),
            "mae_since_watch": round(min(float(prior.get("mae_since_watch", current_return)) if prior else current_return, current_return), 4),
            "state_changed_at": (
                now_iso if not prior or prior.get("watch_stage") != stage
                else prior.get("state_changed_at", now_iso)
            ),
        }
        watchlist.append(item)
    watchlist.sort(
        key=lambda row: (
            {"매수 검토": 0, "돌파 임박": 1, "자금 유입": 2, "점화": 3, "준비": 4, "펌핑 후기": 5}.get(str(row["watch_stage"]), 6),
            -int(row["ignition_score"]),
            -int(row["preparation_score"]),
        )
    )
    return watchlist[:MAX_WATCHLIST]


def load_previous() -> dict[str, Any]:
    try:
        payload = json.loads(OUT.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {"tracking": {}, "completed": [], "paper_league": {}, "signal_memory": {}}

    tracking_rows = payload.get("tracking")
    if not isinstance(tracking_rows, list):
        # V2 migration: candidates were the only persisted detection records.
        tracking_rows = payload.get("candidates", [])
    tracking = {
        row["market"]: row
        for row in tracking_rows
        if isinstance(row, dict) and "market" in row
    }
    completed = [
        row for row in payload.get("completed", [])
        if isinstance(row, dict) and "market" in row
    ]
    return {
        "tracking": tracking,
        "completed": completed,
        "paper_league": payload.get("paper_league", {}),
        "signal_memory": payload.get("signal_memory", {}),
    }


def parse_utc(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(timezone.utc)


def public_row(row: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in row.items() if not key.startswith("_")}


def tracking_extremes(
    row: dict[str, Any], first_dt: datetime, first_price: float
) -> tuple[float, float, str | None, str | None]:
    peak = pct_change(float(row["current_price"]), first_price)
    mae = peak
    success_at: str | None = None
    failure_at: str | None = None
    for bar in row.get("_tracking_bars", []):
        bar_dt = parse_utc(bar.get("time"))
        if bar_dt is None or bar_dt < first_dt or bar_dt > first_dt + timedelta(hours=TRACK_HOURS):
            continue
        high_return = pct_change(float(bar["high"]), first_price)
        low_return = pct_change(float(bar["low"]), first_price)
        peak = max(peak, high_return)
        mae = min(mae, low_return)
        # With OHLC data the intrabar order is unknown, so failure wins ties.
        if low_return <= FAILURE_RETURN and failure_at is None:
            failure_at = str(bar["time"])
        if high_return >= SUCCESS_RETURN and success_at is None:
            success_at = str(bar["time"])
        if success_at or failure_at:
            break
    return peak, mae, success_at, failure_at


def update_signal_memory(
    previous: Any,
    scored_rows: dict[str, dict[str, Any]],
    now: datetime,
    now_iso: str,
) -> dict[str, dict[str, Any]]:
    prior_map = previous if isinstance(previous, dict) else {}
    memory: dict[str, dict[str, Any]] = {}
    for market, row in scored_rows.items():
        prep = preparation_score(row)
        ignition = ignition_score(row)
        rotation = rotation_score(row)
        prior = prior_map.get(market, {}) if isinstance(prior_map.get(market, {}), dict) else {}
        early = float(row["change_1h"]) < 5.0 and float(row["change_12h"]) < 8.0
        first_prep = prior.get("first_preparation_at")
        first_ignition = prior.get("first_ignition_at")
        first_rotation = prior.get("first_rotation_at")
        if early and prep >= 55 and not first_prep:
            first_prep = now_iso
        if early and ignition >= 45 and not first_ignition:
            first_ignition = now_iso
        if early and rotation >= 65 and not first_rotation:
            first_rotation = now_iso
        memory[market] = {
            "market": market,
            "symbol": row["symbol"],
            "first_observed_at": prior.get("first_observed_at", now_iso),
            "last_observed_at": now_iso,
            "first_preparation_at": first_prep,
            "first_ignition_at": first_ignition,
            "first_rotation_at": first_rotation,
            "first_observed_price": prior.get("first_observed_price", row["current_price"]),
            "max_preparation_score": max(int(prior.get("max_preparation_score", 0)), prep),
            "max_ignition_score": max(int(prior.get("max_ignition_score", 0)), ignition),
            "max_rotation_score": max(int(prior.get("max_rotation_score", 0)), rotation),
            "earliest_change_12h": prior.get("earliest_change_12h", row["change_12h"]),
            "current_change_12h": row["change_12h"],
            "current_change_1h": row["change_1h"],
            "current_value_ratio_15m": round(float(row["value_ratio_15m"]), 4),
            "current_value_ratio_1h": round(float(row["value_ratio_1h"]), 4),
            "previous_change_1h": prior.get("current_change_1h"),
            "previous_value_ratio_15m": prior.get("current_value_ratio_15m"),
        }
    return memory


def build_market_monitor(
    scored_rows: dict[str, dict[str, Any]], signal_memory: dict[str, dict[str, Any]]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Keep every normal market observable and surface fresh flow independently."""
    monitor: list[dict[str, Any]] = []
    for market, row in scored_rows.items():
        rotation = rotation_score(row)
        memory = signal_memory.get(market, {})
        if is_pump_late(row):
            state = "급등 후기"
        elif rotation >= 70:
            state = "강한 자금 유입"
        elif rotation >= 55:
            state = "자금 유입 감시"
        elif rotation >= 40:
            state = "유입 준비"
        else:
            state = "일반 감시"
        monitor.append({
            "market": market,
            "symbol": row["symbol"],
            "current_price": row["current_price"],
            "monitor_state": state,
            "rotation_score": rotation,
            "flow_percentile_15m": row.get("flow_percentile_15m"),
            "flow_percentile_1h": row.get("flow_percentile_1h"),
            "value_ratio_15m": round(float(row["value_ratio_15m"]), 4),
            "value_ratio_1h": round(float(row["value_ratio_1h"]), 4),
            "change_15m": row["change_15m"],
            "change_1h": row["change_1h"],
            "change_12h": row["change_12h"],
            "close_position": row["close_position"],
            "upper_wick_ratio": row["upper_wick_ratio"],
            "first_rotation_at": memory.get("first_rotation_at"),
        })
    monitor.sort(
        key=lambda row: (
            -int(row["rotation_score"]),
            -float(row.get("flow_percentile_15m") or 0.0),
            -float(row["value_ratio_15m"]),
        )
    )
    rotation_watch = [
        row for row in monitor
        if int(row["rotation_score"]) >= 40 and row["monitor_state"] != "급등 후기"
    ][:30]
    return monitor, rotation_watch


def strategy_signal(
    strategy: str,
    row: dict[str, Any],
    watch: dict[str, Any] | None,
) -> tuple[bool, str]:
    if is_pump_late(row):
        return False, "펌핑 후기"
    if strategy == "A_EARLY":
        ok = bool(
            watch
            and watch.get("watch_stage") == "점화"
            and int(watch.get("preparation_score", 0)) >= 70
            and int(watch.get("ignition_score", 0)) >= 55
            and float(row["change_1h"]) < 5.0
        )
        return ok, "점화 단계"
    if strategy == "B_BREAKOUT":
        return bool(watch and watch.get("watch_stage") == "매수 검토"), "20시간 고점 돌파"
    if strategy == "C_PULLBACK":
        ok = bool(
            watch
            and watch.get("watch_stage") in {"돌파 임박", "매수 검토"}
            and int(watch.get("ignition_score", 0)) >= 55
            and -3.0 <= float(row["drawdown_from_high_4h"]) <= -0.3
            and -1.0 <= float(row["change_15m"]) <= 0.3
            and row["above_ema20"]
        )
        return ok, "고점 아래 첫 눌림"
    if strategy == "D_CHAMPION":
        return detection_route(row, int(row["score"])) is not None, "V4 현행 조건"
    if strategy == "E_ROTATION":
        ok = bool(
            watch
            and watch.get("action") == "자금유입 2회 확인"
            and int(watch.get("rotation_score", 0)) >= 70
            and int(watch.get("rotation_streak", 0)) >= 2
        )
        return ok, "자금유입 2회 확인"
    return False, "알 수 없는 전략"


def empty_paper_account(strategy: str) -> dict[str, Any]:
    return {
        "strategy": strategy,
        "name": PAPER_STRATEGIES[strategy],
        "starting_cash": PAPER_START_CASH,
        "cash": PAPER_START_CASH,
        "open_positions": [],
        "pending_orders": [],
        "closed_trades": [],
        "realized_pnl": 0.0,
        "fees_paid": 0.0,
        "peak_equity": PAPER_START_CASH,
        "max_drawdown_percent": 0.0,
        "day_start_date": None,
        "day_start_equity": PAPER_START_CASH,
    }


def update_paper_league(
    previous: Any,
    scored_rows: dict[str, dict[str, Any]],
    watchlist: list[dict[str, Any]],
    now: datetime,
    now_iso: str,
) -> dict[str, Any]:
    prior_accounts = previous.get("accounts", {}) if isinstance(previous, dict) else {}
    watch_by_market = {row["market"]: row for row in watchlist}
    accounts: dict[str, dict[str, Any]] = {}

    for strategy in PAPER_STRATEGIES:
        prior = prior_accounts.get(strategy, {}) if isinstance(prior_accounts, dict) else {}
        account = {**empty_paper_account(strategy), **prior}
        account["strategy"] = strategy
        account["name"] = PAPER_STRATEGIES[strategy]
        account["open_positions"] = list(account.get("open_positions", []))
        account["pending_orders"] = list(account.get("pending_orders", []))
        account["closed_trades"] = list(account.get("closed_trades", []))

        still_open: list[dict[str, Any]] = []
        for position in account["open_positions"]:
            row = scored_rows.get(str(position.get("market")))
            if row is None:
                still_open.append(position)
                continue
            exit_price: float | None = None
            exit_reason: str | None = None
            last_checked = parse_utc(position.get("last_checked_at"))
            for bar in row.get("_tracking_bars", []):
                bar_dt = parse_utc(bar.get("time"))
                if bar_dt is None or (last_checked is not None and bar_dt <= last_checked):
                    continue
                if float(bar["low"]) <= float(position["stop_price"]):
                    exit_price = float(position["stop_price"])
                    exit_reason = "손절 -5%"
                    break
                if float(bar["high"]) >= float(position["target_price"]):
                    exit_price = float(position["target_price"])
                    exit_reason = "목표 +10%"
                    break
            entered_at = parse_utc(position.get("entered_at"))
            if exit_price is None and entered_at is not None and now - entered_at >= timedelta(hours=TRACK_HOURS):
                exit_price = float(row["current_price"])
                exit_reason = "12시간 종료"
            if exit_price is None:
                position["last_checked_at"] = now_iso
                position["current_price"] = row["current_price"]
                position["unrealized_return_percent"] = round(
                    pct_change(float(row["current_price"]), float(position["entry_fill_price"])), 4
                )
                still_open.append(position)
                continue

            sell_fill = exit_price * (1.0 - PAPER_SLIPPAGE_RATE)
            gross = float(position["quantity"]) * sell_fill
            exit_fee = gross * PAPER_FEE_RATE
            net = gross - exit_fee
            pnl = net - float(position["total_entry_cost"])
            account["cash"] = float(account["cash"]) + net
            account["realized_pnl"] = float(account.get("realized_pnl", 0.0)) + pnl
            account["fees_paid"] = float(account.get("fees_paid", 0.0)) + exit_fee
            account["closed_trades"].append({
                **position,
                "exit_at": now_iso,
                "exit_price": round(exit_price, 12),
                "exit_fill_price": round(sell_fill, 12),
                "exit_fee": round(exit_fee, 4),
                "pnl": round(pnl, 4),
                "return_percent": round(pnl / float(position["total_entry_cost"]) * 100.0, 4),
                "exit_reason": exit_reason,
            })
        account["open_positions"] = still_open

        retained_pending: list[dict[str, Any]] = []
        for order in account["pending_orders"]:
            row = scored_rows.get(str(order.get("market")))
            created_at = parse_utc(order.get("created_at"))
            if created_at is not None and now - created_at > timedelta(minutes=PAPER_PENDING_MINUTES):
                continue
            if row is None or created_at is None or created_at >= now:
                retained_pending.append(order)
                continue
            if is_pump_late(row) or float(row["current_price"]) > float(order["signal_price"]) * 1.03:
                continue
            if len(account["open_positions"]) >= PAPER_MAX_POSITIONS:
                retained_pending.append(order)
                continue
            gross_budget = min(
                PAPER_POSITION_KRW,
                float(account["cash"]) / (1.0 + PAPER_FEE_RATE),
            )
            if gross_budget < 10_000.0:
                retained_pending.append(order)
                continue
            fill = float(row["current_price"]) * (1.0 + PAPER_SLIPPAGE_RATE)
            quantity = gross_budget / fill
            entry_fee = gross_budget * PAPER_FEE_RATE
            total_cost = gross_budget + entry_fee
            account["cash"] = float(account["cash"]) - total_cost
            account["fees_paid"] = float(account.get("fees_paid", 0.0)) + entry_fee
            account["open_positions"].append({
                "market": row["market"],
                "symbol": row["symbol"],
                "signal_at": order["created_at"],
                "signal_price": order["signal_price"],
                "signal_reason": order["signal_reason"],
                "entered_at": now_iso,
                "entry_market_price": row["current_price"],
                "entry_fill_price": round(fill, 12),
                "quantity": quantity,
                "gross_budget": round(gross_budget, 4),
                "entry_fee": round(entry_fee, 4),
                "total_entry_cost": round(total_cost, 4),
                "stop_price": round(fill * 0.95, 12),
                "target_price": round(fill * 1.10, 12),
                "last_checked_at": now_iso,
                "current_price": row["current_price"],
                "unrealized_return_percent": round(pct_change(float(row["current_price"]), fill), 4),
            })
        account["pending_orders"] = retained_pending

        occupied = {str(row["market"]) for row in account["open_positions"]}
        occupied |= {str(row["market"]) for row in account["pending_orders"]}
        recently_closed = {
            str(row["market"])
            for row in account["closed_trades"]
            if parse_utc(row.get("exit_at")) is not None
            and now - parse_utc(row.get("exit_at")) < timedelta(hours=TRACK_HOURS)
        }
        signal_candidates: list[tuple[float, str, dict[str, Any], str]] = []
        for market, row in scored_rows.items():
            if market in occupied or market in recently_closed:
                continue
            signal, reason = strategy_signal(strategy, row, watch_by_market.get(market))
            if signal:
                watch = watch_by_market.get(market, {})
                if strategy == "A_EARLY":
                    priority = float(watch.get("preparation_score", 0)) + float(watch.get("ignition_score", 0))
                elif strategy in {"B_BREAKOUT", "C_PULLBACK"}:
                    priority = float(watch.get("ignition_score", 0))
                elif strategy == "E_ROTATION":
                    priority = float(watch.get("rotation_score", 0))
                else:
                    priority = float(row.get("score", 0))
                signal_candidates.append((priority, market, row, reason))
        available_slots = max(0, PAPER_MAX_POSITIONS - len(account["open_positions"]) - len(account["pending_orders"]))
        for _, market, row, reason in sorted(signal_candidates, reverse=True)[:available_slots]:
                account["pending_orders"].append({
                    "market": market,
                    "symbol": row["symbol"],
                    "created_at": now_iso,
                    "signal_price": row["current_price"],
                    "signal_reason": reason,
                })
                occupied.add(market)

        market_value = 0.0
        unrealized = 0.0
        for position in account["open_positions"]:
            row = scored_rows.get(str(position["market"]))
            price = float(row["current_price"]) if row else float(position.get("current_price", position["entry_fill_price"]))
            liquidation = float(position["quantity"]) * price * (1.0 - PAPER_SLIPPAGE_RATE) * (1.0 - PAPER_FEE_RATE)
            market_value += liquidation
            unrealized += liquidation - float(position["total_entry_cost"])
        equity = float(account["cash"]) + market_value
        kst_date = (now + timedelta(hours=9)).date().isoformat()
        prior_equity = float(prior.get("equity", PAPER_START_CASH))
        if account.get("day_start_date") != kst_date:
            account["day_start_date"] = kst_date
            account["day_start_equity"] = prior_equity
        day_start_equity = float(account.get("day_start_equity", prior_equity))
        peak_equity = max(float(account.get("peak_equity", PAPER_START_CASH)), equity)
        drawdown = pct_change(equity, peak_equity)
        closed_trades = account["closed_trades"][-PAPER_HISTORY_LIMIT:]
        wins = [row for row in closed_trades if float(row.get("pnl", 0.0)) > 0]
        losses = [row for row in closed_trades if float(row.get("pnl", 0.0)) <= 0]
        gross_profit = sum(float(row.get("pnl", 0.0)) for row in wins)
        gross_loss = abs(sum(float(row.get("pnl", 0.0)) for row in losses))
        today_closed = [
            row for row in closed_trades
            if parse_utc(row.get("exit_at")) is not None
            and (parse_utc(row.get("exit_at")) + timedelta(hours=9)).date().isoformat() == kst_date
        ]
        loss_reasons: dict[str, float] = {}
        for trade in today_closed:
            if float(trade.get("pnl", 0.0)) >= 0:
                continue
            reason = f'{trade.get("exit_reason", "기타")} · {trade.get("signal_reason", "신호")}'
            loss_reasons[reason] = loss_reasons.get(reason, 0.0) + float(trade["pnl"])
        account.update({
            "cash": round(float(account["cash"]), 4),
            "market_value": round(market_value, 4),
            "equity": round(equity, 4),
            "total_return_percent": round(pct_change(equity, PAPER_START_CASH), 4),
            "daily_pnl": round(equity - day_start_equity, 4),
            "daily_return_percent": round(pct_change(equity, day_start_equity), 4),
            "unrealized_pnl": round(unrealized, 4),
            "realized_pnl": round(float(account.get("realized_pnl", 0.0)), 4),
            "fees_paid": round(float(account.get("fees_paid", 0.0)), 4),
            "peak_equity": round(peak_equity, 4),
            "max_drawdown_percent": round(min(float(account.get("max_drawdown_percent", 0.0)), drawdown), 4),
            "win_count": len(wins),
            "loss_count": len(losses),
            "win_rate_percent": round(len(wins) / len(closed_trades) * 100.0, 2) if closed_trades else None,
            "profit_factor": round(gross_profit / gross_loss, 4) if gross_loss > 0 else None,
            "daily_loss_reasons": [
                {"reason": reason, "pnl": round(pnl, 4)}
                for reason, pnl in sorted(loss_reasons.items(), key=lambda item: item[1])
            ],
            "updated_at": now_iso,
            "closed_trades": closed_trades,
        })
        accounts[strategy] = account

    ranking = sorted(
        (
            {
                "strategy": key,
                "name": value["name"],
                "equity": value["equity"],
                "total_return_percent": value["total_return_percent"],
                "max_drawdown_percent": value["max_drawdown_percent"],
                "closed_trade_count": len(value["closed_trades"]),
            }
            for key, value in accounts.items()
        ),
        key=lambda row: (-float(row["equity"]), -float(row["max_drawdown_percent"])),
    )
    return {
        "mode": "모의매매 전용·실제 주문 없음",
        "starting_cash_per_account": PAPER_START_CASH,
        "fee_rate_each_side": PAPER_FEE_RATE,
        "slippage_rate_each_side": PAPER_SLIPPAGE_RATE,
        "max_positions": PAPER_MAX_POSITIONS,
        "position_krw": PAPER_POSITION_KRW,
        "pending_order_expiry_minutes": PAPER_PENDING_MINUTES,
        "updated_at": now_iso,
        "accounts": accounts,
        "ranking": ranking,
    }


def main() -> None:
    markets_raw = api_get("/market/all", {"isDetails": "true"})
    risk_exclusions = [
        {
            "market": row["market"],
            "reason": "BITHUMB_CAUTION",
        }
        for row in markets_raw
        if row["market"].startswith("KRW-")
        and (
            str(row.get("market_warning", "NONE")).upper() != "NONE"
        )
    ]
    markets = sorted(
        row["market"] for row in markets_raw
        if row["market"].startswith("KRW-")
        and row["market"].split("-", 1)[1] not in STABLE_SYMBOLS
        and str(row.get("market_warning", "NONE")).upper() == "NONE"
    )
    tickers = ticker_map(markets)
    scanned_rows: list[dict[str, Any]] = []
    exclusions: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {
            executor.submit(scan_market, market, tickers[market]): market
            for market in markets
        }
        for index, future in enumerate(as_completed(futures), 1):
            market = futures[future]
            try:
                scanned_rows.append(future.result())
            except Exception as exc:
                exclusions.append({"market": market, "reason": f"{type(exc).__name__}: {exc}"})
            print(f"[{index}/{len(markets)}] {market}", flush=True)

    rank_4h = percentile_ranks(scanned_rows, "change_1h")
    rank_12h = percentile_ranks(scanned_rows, "change_12h")
    rank_flow_15m = percentile_ranks(scanned_rows, "value_ratio_15m")
    rank_flow_1h = percentile_ranks(scanned_rows, "value_ratio_1h")
    state = load_previous()
    previous_tracking: dict[str, dict[str, Any]] = state["tracking"]
    completed: list[dict[str, Any]] = state["completed"]
    completed_keys = {
        (row.get("market"), row.get("first_detected_at")) for row in completed
    }
    now = datetime.now(timezone.utc)
    now_iso = now.isoformat().replace("+00:00", "Z")
    scored_rows: dict[str, dict[str, Any]] = {}
    qualified_routes: dict[str, str] = {}
    for row in scanned_rows:
        row["flow_percentile_15m"] = round(rank_flow_15m[row["market"]] * 100.0, 1)
        row["flow_percentile_1h"] = round(rank_flow_1h[row["market"]] * 100.0, 1)
        strength = (rank_4h[row["market"]] + rank_12h[row["market"]]) / 2.0
        scored = score_row(row, strength)
        scored_rows[row["market"]] = scored
    watchlist = update_watch_state(scored_rows, now, now_iso)
    btc_context = btc_market_context(scored_rows.get("KRW-BTC"))
    for item in watchlist:
        item["original_action"] = item["action"]
        item["risk_reasons"] = safety_risk_reasons(item, btc_context)
        item["action"] = conservative_action(item, btc_context)
        item["btc_market_state"] = btc_context["state"]
    watch_by_market = {row["market"]: row for row in watchlist}
    signal_memory = update_signal_memory(state.get("signal_memory"), scored_rows, now, now_iso)
    market_monitor, rotation_watch = build_market_monitor(scored_rows, signal_memory)
    paper_league = update_paper_league(
        state.get("paper_league"), scored_rows, watchlist, now, now_iso
    )
    for item in watchlist:
        if item["watch_stage"] in {"점화", "자금 유입", "돌파 임박", "매수 검토"}:
            qualified_routes[item["market"]] = str(item["watch_stage"])
    qualified_markets = set(qualified_routes)

    tracking: list[dict[str, Any]] = []
    completed_now: list[dict[str, Any]] = []

    # Existing episodes remain tracked for the full 12 hours, regardless of score.
    for market, prior in previous_tracking.items():
        row = scored_rows.get(market)
        first_dt = parse_utc(prior.get("first_detected_at"))
        if row is None or first_dt is None:
            continue
        first_price = max(float(prior.get("first_detected_price", row["current_price"])), 1e-12)
        current_return = pct_change(float(row["current_price"]), first_price)
        observed_peak, observed_mae, success_at, failure_at = tracking_extremes(row, first_dt, first_price)
        peak = max(float(prior.get("peak_return_since_detection", current_return)), observed_peak)
        mae = min(float(prior.get("mae_since_detection", current_return)), observed_mae)
        elapsed_hours = max(0.0, (now - first_dt).total_seconds() / 3600.0)

        first_change = float(prior.get("first_detected_change_12h", prior.get("change_12h", 0.0)))
        watch = watch_by_market.get(market)
        if watch is not None:
            current_action = str(watch["action"])
        elif market not in qualified_markets:
            current_action = "성과 추적"
        elif current_return >= SUCCESS_RETURN or float(row["change_12h"]) >= SUCCESS_RETURN:
            current_action = "추격 금지"
        elif float(row["change_12h"]) >= 6.0:
            current_action = "눌림 대기"
        elif 1.5 <= float(row["change_12h"]) and float(row["change_1h"]) <= 6.0:
            current_action = "매수 검토"
        else:
            current_action = "관찰"

        episode = {
            **public_row(row),
            "first_detected_at": prior.get("first_detected_at"),
            "first_detected_price": first_price,
            "first_detected_change_12h": first_change,
            "return_since_detection": round(current_return, 4),
            "peak_return_since_detection": round(peak, 4),
            "mae_since_detection": round(mae, 4),
            "elapsed_hours": round(elapsed_hours, 2),
            "status": "진행 중",
            "entry_quality": prior.get(
                "entry_quality",
                "정상 후보" if first_change < 6.0 else ("추격주의" if first_change < 10.0 else "추천 제외"),
            ),
            "action": current_action,
            "detection_route": prior.get("detection_route", qualified_routes.get(market, "V3 이관")),
            "engine_version": prior.get("engine_version", "LEGACY"),
        }

        if failure_at and (not success_at or failure_at == success_at):
            episode.update({"status": "실패", "completed_at": failure_at, "failure_reason": "-5% 선도달"})
        elif success_at:
            episode.update({"status": "성공", "completed_at": success_at, "success_reason": "+10% 선도달"})
        elif peak >= SUCCESS_RETURN:
            episode.update({"status": "성공", "completed_at": now_iso, "success_reason": "+10% 도달"})
        elif mae <= FAILURE_RETURN:
            episode.update({"status": "실패", "completed_at": now_iso, "failure_reason": "-5% 선도달"})
        elif elapsed_hours >= TRACK_HOURS:
            episode.update({"status": "실패", "completed_at": now_iso, "failure_reason": "12시간 내 +10% 미도달"})

        if episode["status"] == "진행 중":
            tracking.append(episode)
        elif (episode["market"], episode["first_detected_at"]) not in completed_keys:
            completed_now.append(episode)
            completed_keys.add((episode["market"], episode["first_detected_at"]))

    completed.extend(completed_now)

    # Completed outcomes stay fixed, but their current price/return remains live.
    refreshed_completed: list[dict[str, Any]] = []
    for prior in completed:
        row = scored_rows.get(str(prior.get("market")))
        if row is None:
            refreshed_completed.append(prior)
            continue
        first_price = max(float(prior.get("first_detected_price", row["current_price"])), 1e-12)
        item = {**prior, **public_row(row)}
        item["return_at_completion"] = prior.get(
            "return_at_completion", prior.get("return_since_detection")
        )
        item["return_since_detection"] = round(
            pct_change(float(row["current_price"]), first_price), 4
        )
        item["action"] = "성과 완료"
        refreshed_completed.append(item)
    completed = refreshed_completed

    # Start a new episode only when the market is not already being tracked and
    # was not completed during this run. Already-risen coins are tracked for
    # measurement but are not presented as actionable recommendations.
    occupied_markets = {row["market"] for row in tracking}
    just_completed_markets = {row["market"] for row in completed_now}
    new_signals: list[dict[str, Any]] = []
    for market in qualified_markets:
        if market in occupied_markets or market in just_completed_markets:
            continue
        recent_completed = next(
            (
                row for row in reversed(completed)
                if row.get("market") == market
                and parse_utc(row.get("completed_at")) is not None
                and now - parse_utc(row.get("completed_at")) < timedelta(hours=TRACK_HOURS)
            ),
            None,
        )
        if recent_completed:
            continue
        row = scored_rows[market]
        first_price = float(row["current_price"])
        chase_risk = float(row["change_12h"])
        episode = {
            **public_row(row),
            "first_detected_at": now_iso,
            "first_detected_price": first_price,
            "first_detected_change_12h": chase_risk,
            "return_since_detection": 0.0,
            "peak_return_since_detection": 0.0,
            "mae_since_detection": 0.0,
            "elapsed_hours": 0.0,
            "status": "진행 중",
            "entry_quality": "정상 후보" if chase_risk < 6.0 else ("추격주의" if chase_risk < 10.0 else "추천 제외"),
            "action": str(watch_by_market[market]["action"]),
            "detection_route": qualified_routes[market],
            "engine_version": "V5.1",
            "first_watch_at": watch_by_market[market]["first_watch_at"],
            "first_watch_price": watch_by_market[market]["first_watch_price"],
            "initial_research_snapshot": initial_research_snapshot(row, btc_context),
        }
        tracking.append(episode)
        new_signals.append(episode)

    # The screen now shows persistent pre-pump states, not only instant signals.
    candidates = [
        row for row in watchlist
        if row["action"] in {"초입 검토", "강한 관찰"}
    ]

    candidates.sort(
        key=lambda row: (
            row["action"] != "초입 검토",
            -row.get("rotation_score", 0),
            -row["ignition_score"],
            -row["preparation_score"],
            -row["relative_strength_percentile"],
            -row["trade_value_24h"],
        )
    )
    candidates = candidates[:MAX_RESULTS]
    actionable_count = sum(row["action"] == "초입 검토" for row in candidates)
    leaders = sorted(
        (public_row(row) for row in scored_rows.values()),
        key=lambda row: float(row["change_12h"]),
        reverse=True,
    )[:20]
    detected_markets = {row["market"] for row in tracking} | {row["market"] for row in completed}
    missed_leaders = [
        {
            "market": row["market"],
            "symbol": row["symbol"],
            "current_price": row["current_price"],
            "change_12h": row["change_12h"],
            "score": row["score"],
            "reason": "; ".join(exclusion_reasons(row, int(row["score"]))),
            "pre_signal_memory": signal_memory.get(row["market"]),
        }
        for row in leaders
        if row["market"] not in detected_markets
    ]
    late_surges = [
        {
            **public_row(row),
            "action": "이미 급등·추격 금지",
            "reason": "12시간 +10% 이상 상승 후 확인",
        }
        for row in scored_rows.values()
        if int(row["score"]) >= 70 and float(row["change_12h"]) >= SUCCESS_RETURN
    ]
    late_surges.sort(key=lambda row: (-float(row["change_12h"]), -int(row["score"])))
    high_score_blocked = [
        {
            **public_row(row),
            "action": "관찰",
            "blocked_reasons": exclusion_reasons(row, int(row["score"])),
        }
        for row in scored_rows.values()
        if int(row["score"]) >= 75
        and row["market"] not in qualified_markets
        and float(row["change_12h"]) < SUCCESS_RETURN
    ]
    high_score_blocked.sort(key=lambda row: (-int(row["score"]), -float(row["change_12h"])))
    completed = completed[-COMPLETED_HISTORY_LIMIT:]
    success_count = sum(row.get("status") == "성공" for row in completed)
    failure_count = sum(row.get("status") == "실패" for row in completed)
    win_rate = round(success_count / (success_count + failure_count) * 100.0, 2) if success_count + failure_count else None
    v5_completed = [row for row in completed if str(row.get("engine_version", "")).startswith("V5")]
    v5_success = sum(row.get("status") == "성공" for row in v5_completed)
    v5_failure = sum(row.get("status") == "실패" for row in v5_completed)
    v5_ongoing = sum(str(row.get("engine_version", "")).startswith("V5") for row in tracking)
    v5_win_rate = round(v5_success / (v5_success + v5_failure) * 100.0, 2) if v5_success + v5_failure else None
    result = {
        "engine": "BES BTC-Gated Early V0.2",
        "mode": "BTC 시장 우선·희소 초입 후보",
        "btc_market": btc_context,
        "scan_interval_minutes": 15,
        "updated_at": now_iso,
        "source": "Bithumb official public REST API",
        "market_count": len(markets),
        "scanned_count": len(scanned_rows),
        "excluded_count": len(exclusions),
        "active_count": len(candidates),
        "actionable_count": actionable_count,
        "tracking_count": len(tracking),
        "completed_count": len(completed),
        "performance": {
            "success_count": success_count,
            "failure_count": failure_count,
            "ongoing_count": len(tracking),
            "win_rate_percent": win_rate,
            "rule": "+10% within 12h = success; -5% first or no +10% within 12h = failure",
        },
        "performance_v5": {
            "success_count": v5_success,
            "failure_count": v5_failure,
            "ongoing_count": v5_ongoing,
            "win_rate_percent": v5_win_rate,
            "rule": "+10% within 12h = success; -5% first or no +10% within 12h = failure",
        },
        "risk_excluded_count": len(risk_exclusions),
        "monitor_count": len(market_monitor),
        "rotation_watch_count": len(rotation_watch),
        "candidates": candidates,
        "watchlist": watchlist,
        "rotation_watch": rotation_watch,
        "market_monitor": market_monitor,
        "signal_memory": signal_memory,
        "paper_league": paper_league,
        "new_signals": new_signals,
        "tracking": tracking,
        "completed": completed,
        "bithumb_12h_leaders": leaders,
        "missed_leaders": missed_leaders,
        "late_surges": late_surges[:20],
        "high_score_blocked": high_score_blocked[:20],
        "risk_exclusions": risk_exclusions,
        "exclusions": exclusions,
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    WATCH_STATE.write_text(
        json.dumps(
            {"engine": result["engine"], "updated_at": now_iso, "watchlist": watchlist},
            ensure_ascii=False,
            indent=2,
        ) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({
        "market_count": result["market_count"],
        "scanned_count": result["scanned_count"],
        "active_count": result["active_count"],
        "actionable_count": result["actionable_count"],
        "tracking_count": result["tracking_count"],
        "completed_count": result["completed_count"],
        "win_rate_percent": result["performance"]["win_rate_percent"],
    }, ensure_ascii=False))


if __name__ == "__main__":
    main()
