#!/usr/bin/env python3
"""Bithumb KRW 15-minute compression-breakout scanner.

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
STABLE_SYMBOLS = {"USDT", "USDC", "DAI", "TUSD", "FDUSD", "USDE", "PYUSD"}
MIN_TRADE_VALUE_24H = 500_000_000
MAX_RESULTS = 20
TRACK_HOURS = 12
SUCCESS_RETURN = 10.0
FAILURE_RETURN = -5.0
COMPLETED_HISTORY_LIMIT = 1000


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
        "compression_24h": pct_change(prior_high_20, prior_low_20),
        "above_ema20": close > ema(closes[-80:], 80),
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


def qualifies(row: dict[str, Any]) -> bool:
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


def load_previous() -> dict[str, Any]:
    try:
        payload = json.loads(OUT.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {"tracking": {}, "completed": []}

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
    return {"tracking": tracking, "completed": completed}


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
    state = load_previous()
    previous_tracking: dict[str, dict[str, Any]] = state["tracking"]
    completed: list[dict[str, Any]] = state["completed"]
    completed_keys = {
        (row.get("market"), row.get("first_detected_at")) for row in completed
    }
    now = datetime.now(timezone.utc)
    now_iso = now.isoformat().replace("+00:00", "Z")
    scored_rows: dict[str, dict[str, Any]] = {}
    qualified_markets: set[str] = set()
    for row in scanned_rows:
        strength = (rank_4h[row["market"]] + rank_12h[row["market"]]) / 2.0
        scored = score_row(row, strength)
        scored_rows[row["market"]] = scored
        if qualifies(row) and scored["score"] >= 60:
            qualified_markets.add(row["market"])

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

        episode = {
            **public_row(row),
            "first_detected_at": prior.get("first_detected_at"),
            "first_detected_price": first_price,
            "first_detected_change_12h": float(prior.get("first_detected_change_12h", prior.get("change_12h", 0.0))),
            "return_since_detection": round(current_return, 4),
            "peak_return_since_detection": round(peak, 4),
            "mae_since_detection": round(mae, 4),
            "elapsed_hours": round(elapsed_hours, 2),
            "status": "진행 중",
            "entry_quality": prior.get(
                "entry_quality",
                "정상 후보" if float(prior.get("first_detected_change_12h", prior.get("change_12h", 0.0))) < 6.0
                else ("추격주의" if float(prior.get("first_detected_change_12h", prior.get("change_12h", 0.0))) < 10.0 else "추천 제외"),
            ),
            "action": prior.get("action", "눌림 대기"),
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
            "action": "매수 검토" if 1.5 <= chase_risk < 6.0 and float(row["change_1h"]) <= 6.0 else "눌림 대기",
        }
        tracking.append(episode)
        new_signals.append(episode)

    # Candidate screen: only currently qualified episodes; tracking remains complete.
    candidates: list[dict[str, Any]] = []
    tracking_by_market = {row["market"]: row for row in tracking}
    for market in qualified_markets:
        candidate = tracking_by_market.get(market)
        if candidate is not None:
            candidates.append(candidate)

    candidates.sort(
        key=lambda row: (
            row["action"] != "매수 검토",
            -row["score"],
            -row["relative_strength_percentile"],
            -row["trade_value_24h"],
        )
    )
    candidates = candidates[:MAX_RESULTS]
    actionable_count = sum(row["action"] == "매수 검토" for row in candidates)
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
            "reason": "12시간 상승 상위지만 포착 기록 없음",
        }
        for row in leaders
        if row["market"] not in detected_markets
    ]
    completed = completed[-COMPLETED_HISTORY_LIMIT:]
    success_count = sum(row.get("status") == "성공" for row in completed)
    failure_count = sum(row.get("status") == "실패" for row in completed)
    win_rate = round(success_count / (success_count + failure_count) * 100.0, 2) if success_count + failure_count else None
    result = {
        "engine": "BES Momentum 15m V3 Tracking",
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
        "risk_excluded_count": len(risk_exclusions),
        "candidates": candidates,
        "new_signals": new_signals,
        "tracking": tracking,
        "completed": completed,
        "bithumb_12h_leaders": leaders,
        "missed_leaders": missed_leaders,
        "risk_exclusions": risk_exclusions,
        "exclusions": exclusions,
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
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
