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


def load_previous() -> dict[str, dict[str, Any]]:
    try:
        payload = json.loads(OUT.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return {row["market"]: row for row in payload.get("candidates", []) if "market" in row}


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
    previous = load_previous()
    now_iso = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    candidates: list[dict[str, Any]] = []
    for row in scanned_rows:
        prior = previous.get(row["market"], {})
        first_detected = prior.get("first_detected_at")
        try:
            first_dt = datetime.fromisoformat(first_detected.replace("Z", "+00:00")) if first_detected else None
        except ValueError:
            first_dt = None
        still_tracking = first_dt is not None and datetime.now(timezone.utc) - first_dt <= timedelta(hours=TRACK_HOURS)
        if not qualifies(row) and not still_tracking:
            continue
        strength = (rank_4h[row["market"]] + rank_12h[row["market"]]) / 2.0
        candidate = score_row(row, strength)
        if candidate["score"] < 60:
            continue
        candidate["first_detected_at"] = prior.get("first_detected_at", now_iso)
        candidate["first_detected_price"] = prior.get("first_detected_price", row["current_price"])
        first_price = max(float(candidate["first_detected_price"]), 1e-12)
        current_return = pct_change(candidate["current_price"], first_price)
        candidate["return_since_detection"] = current_return
        candidate["peak_return_since_detection"] = max(float(prior.get("peak_return_since_detection", current_return)), current_return)
        candidate["mae_since_detection"] = min(float(prior.get("mae_since_detection", current_return)), current_return)
        early_trigger = qualifies(row) and 1.5 <= candidate["change_12h"] <= 10.0 and candidate["change_1h"] <= 6.0
        candidate["action"] = "매수 검토" if early_trigger else "눌림 대기"
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
    result = {
        "engine": "BES Momentum 15m V2",
        "scan_interval_minutes": 15,
        "updated_at": now_iso,
        "source": "Bithumb official public REST API",
        "market_count": len(markets),
        "scanned_count": len(scanned_rows),
        "excluded_count": len(exclusions),
        "active_count": len(candidates),
        "actionable_count": actionable_count,
        "risk_excluded_count": len(risk_exclusions),
        "candidates": candidates,
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
    }, ensure_ascii=False))


if __name__ == "__main__":
    main()

