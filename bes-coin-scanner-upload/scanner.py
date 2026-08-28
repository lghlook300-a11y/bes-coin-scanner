#!/usr/bin/env python3
"""Upbit KRW BES V4.9.1 structure scanner."""

from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from bes_core import Bar, scan_bull, scan_daily_bear

API = "https://api.upbit.com/v1"
OUT = Path("data/latest.json")
MIN_DAILY = 400
MIN_4H = 450
STABLE_SYMBOLS = {"USDT", "USDC", "DAI", "TUSD", "FDUSD", "USDE", "PYUSD"}


def api_get(path: str, params: dict[str, Any] | None = None) -> Any:
    url = f"{API}{path}"
    if params:
        url += "?" + urlencode(params)
    request = Request(url, headers={"User-Agent": "bes-coin-scanner/1.0"})
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


def candle_start_ms(row: dict[str, Any]) -> int:
    dt = datetime.fromisoformat(row["candle_date_time_utc"]).replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def fetch_candles(market: str, unit: str, minimum: int) -> list[Bar]:
    path = "/candles/days" if unit == "1d" else "/candles/minutes/240"
    rows: list[dict[str, Any]] = []
    to_value: str | None = None
    while len(rows) < minimum + 2:
        params: dict[str, Any] = {"market": market, "count": 200}
        if to_value:
            params["to"] = to_value
        batch = api_get(path, params)
        if not batch:
            break
        rows.extend(batch)
        oldest = datetime.fromisoformat(batch[-1]["candle_date_time_utc"]).replace(tzinfo=timezone.utc)
        to_value = (oldest - timedelta(seconds=1)).isoformat().replace("+00:00", "Z")
        if len(batch) < 200:
            break
        time.sleep(0.12)

    unique: dict[int, dict[str, Any]] = {candle_start_ms(row): row for row in rows}
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    interval_ms = 86_400_000 if unit == "1d" else 14_400_000
    bars: list[Bar] = []
    for started, row in sorted(unique.items()):
        if started + interval_ms > now_ms:
            continue
        bars.append(Bar(
            time=started,
            open=float(row["opening_price"]),
            high=float(row["high_price"]),
            low=float(row["low_price"]),
            close=float(row["trade_price"]),
            volume=float(row.get("candle_acc_trade_volume", 0.0)),
        ))
    if any(a.time >= b.time for a, b in zip(bars, bars[1:])):
        raise ValueError("duplicate or reversed candle time")
    return bars


def event_json(event: Any | None) -> dict[str, Any] | None:
    return asdict(event) if event else None


def iso_time(ms: int | None) -> str | None:
    if ms is None:
        return None
    return datetime.fromtimestamp(ms / 1000, timezone.utc).isoformat().replace("+00:00", "Z")


def scan_market(market: str) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    daily = fetch_candles(market, "1d", MIN_DAILY)
    four_h = fetch_candles(market, "4h", MIN_4H)
    if len(daily) < MIN_DAILY or len(four_h) < MIN_4H:
        return None, {"market": market, "reason": f"bars 1D={len(daily)}, 4H={len(four_h)}"}

    events4, snap4 = scan_bull(four_h, prefix="4H", rearm_bars=72)
    daily_bull_events, daily_bull = scan_bull(daily, prefix="1D", rearm_bars=30, invalidate_on_low=True)
    daily_bear_events, daily_bear = scan_daily_bear(daily)
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    recent_confirm = next((e for e in reversed(events4) if e.signal == "CONFIRM" and now_ms - e.detected_at <= 86_400_000), None)
    active_state = snap4.state in {"PRE-A 관찰", "ABC 진행", "CONFIRM 대기"}
    if not active_state and recent_confirm is None:
        candidate = None
    else:
        latest = snap4.latest_event or recent_confirm
        candidate = {
            "market": market,
            "symbol": market.removeprefix("KRW-"),
            "stage": snap4.state if active_state else "최근 CONFIRM",
            "latest_signal": latest.signal if latest else None,
            "signal_time": iso_time(latest.detected_at if latest else None),
            "signal_price": latest.price if latest else None,
            "a_base": snap4.base if active_state else recent_confirm.base,
            "b_resistance": snap4.resistance if active_state else recent_confirm.resistance,
            "daily_bull_s": daily_bull.state == "일봉 S 활성",
            "daily_bear_warning": daily_bear.state != "하락 구조 없음",
            "bars": {"1d": len(daily), "4h": len(four_h)},
        }

    invalid = next((e for e in reversed(events4) if e.signal in {"P-FAIL", "FAIL"} and now_ms - e.detected_at <= 7 * 86_400_000), None)
    invalid_json = None
    if invalid:
        invalid_json = {
            "market": market,
            "signal": invalid.signal,
            "time": iso_time(invalid.detected_at),
            "price": invalid.price,
        }
    return candidate, invalid_json


def main() -> None:
    markets_raw = api_get("/market/all", {"is_details": "true"})
    markets = sorted(
        row["market"] for row in markets_raw
        if row["market"].startswith("KRW-")
        and row["market"].split("-", 1)[1] not in STABLE_SYMBOLS
        and row.get("market_event", {}).get("warning") is not True
    )
    candidates: list[dict[str, Any]] = []
    invalidations: list[dict[str, Any]] = []
    exclusions: list[dict[str, Any]] = []
    scanned = 0
    for index, market in enumerate(markets, 1):
        try:
            candidate, invalid = scan_market(market)
            if candidate is None and invalid and "reason" in invalid:
                exclusions.append(invalid)
            else:
                scanned += 1
                if candidate:
                    candidates.append(candidate)
                if invalid:
                    invalidations.append(invalid)
        except Exception as exc:
            exclusions.append({"market": market, "reason": f"{type(exc).__name__}: {exc}"})
        print(f"[{index}/{len(markets)}] {market}", flush=True)
        time.sleep(0.12)

    order = {"CONFIRM 대기": 0, "ABC 진행": 1, "PRE-A 관찰": 2, "최근 CONFIRM": 3}
    candidates.sort(key=lambda row: (order[row["stage"]], row["market"]))
    result = {
        "engine": "BES V4.9.1",
        "updated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "source": "Upbit official public REST API",
        "market_count": len(markets),
        "scanned_count": scanned,
        "excluded_count": len(exclusions),
        "active_count": len(candidates),
        "fail_excluded_count": len(invalidations),
        "candidates": candidates,
        "invalidations": invalidations,
        "exclusions": exclusions,
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({k: result[k] for k in ("market_count", "scanned_count", "excluded_count", "active_count", "fail_excluded_count")}, ensure_ascii=False))


if __name__ == "__main__":
    main()
