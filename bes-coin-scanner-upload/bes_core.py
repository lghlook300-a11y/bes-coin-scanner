#!/usr/bin/env python3
"""BES V4.9.1 structure scanner core.

Translates the verified Pine state machines used for scanning only:
Daily bullish S, Daily bearish S, and 4H bullish PRE-A/A/CONFIRM/P-FAIL/FAIL.
No ranking, prediction score, or buy recommendation is produced.
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Iterable, Sequence
import argparse
import json


@dataclass(frozen=True)
class Bar:
    time: int
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0


@dataclass(frozen=True)
class Event:
    signal: str
    detected_at: int
    pivot_at: int | None
    price: float
    base: float | None
    resistance: float | None


@dataclass(frozen=True)
class Snapshot:
    state: str
    base: float | None
    resistance: float | None
    pullback: float | None
    latest_event: Event | None


def _rolling_min(values: Sequence[float], end: int, length: int) -> float | None:
    start = end - length + 1
    if start < 0:
        return None
    return min(values[start : end + 1])


def _rolling_max(values: Sequence[float], end: int, length: int) -> float | None:
    start = end - length + 1
    if start < 0:
        return None
    return max(values[start : end + 1])


def _pivot_flags(bars: Sequence[Bar], strength: int) -> tuple[list[bool], list[bool]]:
    """Return Pine-style pivot confirmations indexed by confirmation bar."""
    n = len(bars)
    lows = [False] * n
    highs = [False] * n
    for detected in range(strength * 2, n):
        pivot = detected - strength
        lo_window = [bars[i].low for i in range(pivot - strength, pivot + strength + 1)]
        hi_window = [bars[i].high for i in range(pivot - strength, pivot + strength + 1)]
        lows[detected] = bars[pivot].low == min(lo_window)
        highs[detected] = bars[pivot].high == max(hi_window)
    return lows, highs


def scan_bull(
    bars: Sequence[Bar],
    *,
    prefix: str,
    pivot_strength: int = 12,
    structural_lookback: int = 360,
    rearm_bars: int = 72,
    invalidate_on_low: bool = False,
) -> tuple[list[Event], Snapshot]:
    """Scan the V4.9.1 bullish ABC state machine.

    prefix='4H' emits PRE-A/A/CONFIRM/P-FAIL/FAIL.
    prefix='1D' emits S and internal invalidation/confirmation events.
    """
    if not bars:
        return [], Snapshot("NO_DATA", None, None, None, None)

    lows = [b.low for b in bars]
    pivot_low, pivot_high = _pivot_flags(bars, pivot_strength)
    state = "WAIT_A"
    a = b = c = None
    a_idx = b_idx = c_idx = None
    last_break_idx = None
    events: list[Event] = []

    for detected, bar in enumerate(bars):
        pidx = detected - pivot_strength
        has_low = pidx >= 0 and pivot_low[detected]
        has_high = pidx >= 0 and pivot_high[detected]
        plow = bars[pidx].low if has_low else None
        phigh = bars[pidx].high if has_high else None

        if prefix == "4H" and state in {"WAIT_B", "WAIT_C"} and a is not None and bar.close < a:
            events.append(Event("P-FAIL", bar.time, None, bar.close, a, b))
            state, a, b, c = "WAIT_A", None, None, None
            a_idx = b_idx = c_idx = None
            continue

        if state == "WAIT_A":
            structural = _rolling_min(lows, pidx, structural_lookback) if has_low else None
            rearmed = last_break_idx is None or pidx >= last_break_idx + rearm_bars
            if has_low and structural is not None and plow is not None and plow <= structural and rearmed:
                a, a_idx = plow, pidx
                b = c = None
                b_idx = c_idx = None
                state = "WAIT_B"
                if prefix == "4H":
                    events.append(Event("PRE-A", bar.time, bars[pidx].time, plow, a, None))

        elif state == "WAIT_B":
            if has_low and a_idx is not None and pidx > a_idx and plow is not None and a is not None and plow < a:
                a, a_idx = plow, pidx
                if prefix == "4H":
                    events.append(Event("PRE-A", bar.time, bars[pidx].time, plow, a, None))
            elif has_high and a_idx is not None and pidx > a_idx:
                b, b_idx = phigh, pidx
                state = "WAIT_C"

        elif state == "WAIT_C":
            if has_low and b_idx is not None and pidx > b_idx and plow is not None and a is not None and plow <= a:
                a, a_idx = plow, pidx
                b = c = None
                b_idx = c_idx = None
                state = "WAIT_B"
                if prefix == "4H":
                    events.append(Event("PRE-A", bar.time, bars[pidx].time, plow, a, None))
            elif has_high and b_idx is not None and pidx > b_idx and phigh is not None and b is not None and phigh > b:
                b, b_idx = phigh, pidx
            elif has_low and b_idx is not None and pidx > b_idx and plow is not None and a is not None and plow > a:
                c, c_idx = plow, pidx
                state = "WAIT_BREAK"
                signal = "A" if prefix == "4H" else "S"
                events.append(Event(signal, bar.time, bars[a_idx].time if a_idx is not None else None, a, a, b))

        elif state == "WAIT_BREAK":
            invalid = (bar.low <= a) if invalidate_on_low and a is not None else (bar.close < a if a is not None else False)
            if invalid:
                if prefix == "4H":
                    events.append(Event("FAIL", bar.time, None, bar.close, a, b))
                state, a, b, c = "WAIT_A", None, None, None
                a_idx = b_idx = c_idx = None
            elif b is not None and bar.close > b:
                signal = "CONFIRM" if prefix == "4H" else "DAILY-CONFIRM"
                events.append(Event(signal, bar.time, None, bar.close, a, b))
                last_break_idx = detected
                state, a, b, c = "WAIT_A", None, None, None
                a_idx = b_idx = c_idx = None

    names = {
        "WAIT_A": "신호 없음",
        "WAIT_B": "PRE-A 관찰" if prefix == "4H" else "일봉 바닥 후보",
        "WAIT_C": "ABC 진행",
        "WAIT_BREAK": "CONFIRM 대기" if prefix == "4H" else "일봉 S 활성",
    }
    return events, Snapshot(names[state], a, b, c, events[-1] if events else None)


def scan_daily_bear(
    bars: Sequence[Bar], pivot_strength: int = 12, structural_lookback: int = 360, rearm_bars: int = 12
) -> tuple[list[Event], Snapshot]:
    if not bars:
        return [], Snapshot("NO_DATA", None, None, None, None)
    highs = [b.high for b in bars]
    pivot_low, pivot_high = _pivot_flags(bars, pivot_strength)
    state = "WAIT_A"
    a = b = c = None
    a_idx = b_idx = c_idx = None
    last_break_idx = None
    events: list[Event] = []
    for detected, bar in enumerate(bars):
        pidx = detected - pivot_strength
        has_low = pidx >= 0 and pivot_low[detected]
        has_high = pidx >= 0 and pivot_high[detected]
        plow = bars[pidx].low if has_low else None
        phigh = bars[pidx].high if has_high else None
        if state == "WAIT_A":
            structural = _rolling_max(highs, pidx, structural_lookback) if has_high else None
            rearmed = last_break_idx is None or pidx >= last_break_idx + rearm_bars
            if has_high and structural is not None and phigh is not None and phigh >= structural and rearmed:
                a, a_idx = phigh, pidx
                b = c = None
                b_idx = c_idx = None
                state = "WAIT_B"
        elif state == "WAIT_B":
            if has_high and a_idx is not None and pidx > a_idx and phigh is not None and a is not None and phigh > a:
                a, a_idx = phigh, pidx
            elif has_low and a_idx is not None and pidx > a_idx:
                b, b_idx = plow, pidx
                state = "WAIT_C"
                events.append(Event("BEAR-S", bar.time, bars[a_idx].time, a, a, b))
        elif state == "WAIT_C":
            if has_high and b_idx is not None and pidx > b_idx and phigh is not None and a is not None and phigh >= a:
                a, a_idx = phigh, pidx
                b = c = None
                b_idx = c_idx = None
                state = "WAIT_B"
            elif has_low and b_idx is not None and pidx > b_idx and plow is not None and b is not None and plow < b:
                b, b_idx = plow, pidx
            elif has_high and b_idx is not None and pidx > b_idx and phigh is not None and a is not None and phigh < a:
                c, c_idx = phigh, pidx
                state = "WAIT_BREAK"
        elif state == "WAIT_BREAK":
            if a is not None and bar.high >= a:
                state, a, b, c = "WAIT_A", None, None, None
                a_idx = b_idx = c_idx = None
            elif b is not None and bar.close < b:
                events.append(Event("BEAR-CONFIRM", bar.time, None, bar.close, a, b))
                last_break_idx = detected
                state, a, b, c = "WAIT_A", None, None, None
                a_idx = b_idx = c_idx = None
    names = {"WAIT_A": "하락 구조 없음", "WAIT_B": "일봉 천장 후보", "WAIT_C": "하락 ABC 진행", "WAIT_BREAK": "하락 확인 대기"}
    return events, Snapshot(names[state], a, b, c, events[-1] if events else None)


def _load_bars(path: str) -> list[Bar]:
    with open(path, "r", encoding="utf-8") as handle:
        raw = json.load(handle)
    return [Bar(**row) for row in raw]


def main() -> None:
    parser = argparse.ArgumentParser(description="BES V4.9.1 structure scanner core")
    parser.add_argument("bars", help="JSON array containing time/open/high/low/close/volume")
    parser.add_argument("--timeframe", choices=("4h", "1d"), required=True)
    args = parser.parse_args()
    bars = _load_bars(args.bars)
    if args.timeframe == "4h":
        events, snapshot = scan_bull(bars, prefix="4H", rearm_bars=72)
        result = {"bull": {"events": [asdict(e) for e in events], "snapshot": asdict(snapshot)}}
    else:
        bull_events, bull = scan_bull(bars, prefix="1D", rearm_bars=30, invalidate_on_low=True)
        bear_events, bear = scan_daily_bear(bars)
        result = {
            "bull": {"events": [asdict(e) for e in bull_events], "snapshot": asdict(bull)},
            "bear": {"events": [asdict(e) for e in bear_events], "snapshot": asdict(bear)},
        }
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
