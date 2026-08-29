#!/usr/bin/env python3
"""Upbit KRW hourly momentum scanner.

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

API = "https://api.upbit.com/v1"
NOTICE_API = "https://api-manager.upbit.com/api/v1/notices"
OUT = Path("data/latest.json")
STABLE_SYMBOLS = {"USDT", "USDC", "DAI", "TUSD", "FDUSD", "USDE", "PYUSD"}
MIN_TRADE_VALUE_24H = 1_000_000_000
MAX_RESULTS = 20


def api_get(path: str, params: dict[str, Any] | None = None) -> Any:
    url = f"{API}{path}"
    if params:
        url += "?" + urlencode(params)
    request = Request(url, headers={"User-Agent": "bes-momentum-scanner/2.0"})
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


def fetch_termination_symbols() -> set[str]:
    params = urlencode({"page": 1, "per_page": 100, "thread_name": "general"})
    request = Request(
        f"{NOTICE_API}?{params}",
        headers={"User-Agent": "Mozilla/5.0 bes-momentum-scanner/2.0"},
    )
    try:
        with urlopen(request, timeout=30) as response:
            payload = json.load(response)
    except (HTTPError, URLError, TimeoutError, ValueError, KeyError):
        return set()
    blocked: set[str] = set()
    for notice in payload.get("data", {}).get("list", []):
        title = str(notice.get("title", ""))
        if "거래지원 종료" in title:
            blocked.update(re.findall(r"\(([A-Z0-9]{2,12})\)", title.upper()))
    return blocked


def ticker_map(markets: list[str]) -> dict[str, dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for index in range(0, len(markets), 100):
        rows.extend(api_get("/ticker", {"markets": ",".join(markets[index:index + 100])}))
        time.sleep(0.12)
    return {str(row["market"]): row for row in rows}


def completed_hourly_candles(market: str, count: int = 60) -> list[dict[str, Any]]:
    rows = api_get("/candles/minutes/60", {"market": market, "count": count})
    now = datetime.now(timezone.utc)
    completed: list[dict[str, Any]] = []
    for row in rows:
        started = datetime.fromisoformat(row["candle_date_time_utc"]).replace(tzinfo=timezone.utc)
        if started + timedelta(hours=1) <= now:
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
    candles = completed_hourly_candles(market)
    if len(candles) < 30:
        raise ValueError(f"hourly bars={len(candles)}")
    closes = [float(row["trade_price"]) for row in candles]
    last = candles[-1]
    close = closes[-1]
    prior_high_20 = max(float(row["high_price"]) for row in candles[-21:-1])
    hourly_values = [float(row.get("candle_acc_trade_price", 0.0)) for row in candles]
    average_20 = sum(hourly_values[-21:-1]) / 20.0
    value_ratio_1h = hourly_values[-1] / average_20 if average_20 > 0 else 0.0
    value_ratio_4h = sum(hourly_values[-4:]) / (average_20 * 4.0) if average_20 > 0 else 0.0
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
        "change_1h": pct_change(close, closes[-2]),
        "change_4h": pct_change(close, closes[-5]),
        "change_12h": pct_change(close, closes[-13]),
        "value_ratio_1h": value_ratio_1h,
        "value_ratio_4h": value_ratio_4h,
        "breakout_20h": close > prior_high_20,
        "distance_to_high_20h": ((prior_high_20 - close) / close) * 100.0,
        "above_ema20": close > ema(closes[-30:], 20),
        "close_position": (close - low) / candle_range,
        "upper_wick_ratio": (high - max(open_price, close)) / candle_range,
        "signal_candle_time": last["candle_date_time_utc"] + "Z",
    }


def stage_for(row: dict[str, Any]) -> str:
    change = float(row["change_12h"])
    if row["change_1h"] > 5.0:
        return "강한 상승"
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
    flow_ratio = max(float(row["value_ratio_1h"]), float(row["value_ratio_4h"]))
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
    has_acceleration = row["change_1h"] >= 0.5 or row["change_4h"] >= 2.0 or row["change_12h"] >= 3.0
    has_flow = row["value_ratio_1h"] >= 1.3 or row["value_ratio_4h"] >= 1.2
    near_or_breaking = row["breakout_20h"] or row["distance_to_high_20h"] <= 2.5
    return (
        row["trade_value_24h"] >= MIN_TRADE_VALUE_24H
        and row["above_ema20"]
        and has_acceleration
        and has_flow
        and near_or_breaking
        and row["change_12h"] <= 35.0
        and row["close_position"] >= 0.45
    )


def load_previous() -> dict[str, dict[str, Any]]:
    try:
        payload = json.loads(OUT.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return {row["market"]: row for row in payload.get("candidates", []) if "market" in row}


def main() -> None:
    markets_raw = api_get("/market/all", {"is_details": "true"})
    termination_symbols = fetch_termination_symbols()
    risk_exclusions = [
        {
            "market": row["market"],
            "reason": "UPBIT_WARNING" if row.get("market_event", {}).get("warning") is True else "TRADING_SUPPORT_TERMINATION_NOTICE",
        }
        for row in markets_raw
        if row["market"].startswith("KRW-")
        and (
            row.get("market_event", {}).get("warning") is True
            or row["market"].split("-", 1)[1] in termination_symbols
        )
    ]
    markets = sorted(
        row["market"] for row in markets_raw
        if row["market"].startswith("KRW-")
        and row["market"].split("-", 1)[1] not in STABLE_SYMBOLS
        and row.get("market_event", {}).get("warning") is not True
        and row["market"].split("-", 1)[1] not in termination_symbols
    )
    tickers = ticker_map(markets)
    scanned_rows: list[dict[str, Any]] = []
    exclusions: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=20) as executor:
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

    rank_4h = percentile_ranks(scanned_rows, "change_4h")
    rank_12h = percentile_ranks(scanned_rows, "change_12h")
    previous = load_previous()
    now_iso = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    candidates: list[dict[str, Any]] = []
    for row in scanned_rows:
        if not qualifies(row):
            continue
        strength = (rank_4h[row["market"]] + rank_12h[row["market"]]) / 2.0
        candidate = score_row(row, strength)
        if candidate["score"] < 60:
            continue
        prior = previous.get(row["market"], {})
        candidate["first_detected_at"] = prior.get("first_detected_at", now_iso)
        candidate["first_detected_price"] = prior.get("first_detected_price", row["current_price"])
        candidate["action"] = "매수 검토" if candidate["stage"] in {"초기 포착", "돌파 진행"} and candidate["change_1h"] <= 5.0 else "눌림 대기"
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
        "engine": "BES Momentum V1",
        "updated_at": now_iso,
        "source": "Upbit official public REST API",
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
