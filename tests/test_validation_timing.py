"""Regression checks for persisted scanner performance timestamps."""

import importlib.util
from datetime import datetime, timedelta, timezone
from pathlib import Path
import unittest
import json
import tempfile
from unittest.mock import patch


SPEC = importlib.util.spec_from_file_location(
    "scanner_under_test", Path(__file__).resolve().parents[1] / "bes-coin-scanner-upload" / "scanner.py"
)
scanner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(scanner)


class ValidationTimingTest(unittest.TestCase):
    def setUp(self):
        self.first = datetime(2026, 9, 27, 0, 7, tzinfo=timezone.utc)
        self.bars = []
        for i in range(100):
            opened = datetime(2026, 9, 27, 0, 15, tzinfo=timezone.utc) + timedelta(minutes=15 * i)
            self.bars.append({
                "time": opened.isoformat().replace("+00:00", "Z"),
                "close": 100 + i / 10,
                "high": 101 + i / 10,
                "low": 99 + i / 10,
            })

    def test_archive_preserves_scan_gap_and_initial_stage(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "latest.json"
            out.write_text(json.dumps({"updated_at": "2026-09-26T22:00:00Z"}))
            result = {"updated_at": "2026-09-27T00:07:00Z", "new_signals": [{
                "market": "KRW-TEST", "first_detected_at": "2026-09-27T00:07:00Z",
                "first_detected_price": 100, "initial_stage": "준비 관찰"}],
                "tracking": [], "completed": []}
            with patch.object(scanner, "OUT", out):
                scanner.record_validation_snapshot(result, self.first)
            self.assertEqual(result["recording_health"]["gap_minutes"], 127)
            self.assertTrue(result["recording_health"]["gap_detected"])
            archive = json.loads((Path(tmp) / "validation/2026-09-27.jsonl").read_text())
            self.assertEqual(archive["records"][0]["initial_stage"], "준비 관찰")
            self.assertEqual(archive["saved_at"], result["updated_at"])

    def test_each_horizon_has_its_own_price_after_delayed_scan(self):
        values, times = scanner.forward_observations({"_tracking_bars": self.bars}, self.first, 100)
        self.assertNotEqual(values["3h"], values["6h"])
        self.assertNotEqual(times["3h"]["candle_closed_at"], times["6h"]["candle_closed_at"])

    def test_horizons_use_candle_times_and_discard_legacy_scan_prices(self):
        returns, times = scanner.forward_observations(
            {"_tracking_bars": self.bars}, self.first, 100,
            {"forward_returns": {"3h": 999.0}},
        )
        self.assertEqual(returns["3h"], 1.1)
        self.assertIn("24h", returns)
        self.assertLessEqual(times["24h"]["minutes_after_target"], 30)

    def test_large_gap_does_not_fake_a_three_hour_return(self):
        returns, _ = scanner.forward_observations(
            {"_tracking_bars": [self.bars[0], self.bars[40]]}, self.first, 100
        )
        self.assertNotIn("3h", returns)

    def test_24h_extrema_separate_from_outcome_and_reject_gaps(self):
        row = {"_tracking_bars": self.bars, "current_price": 200}
        self.assertTrue(scanner.validation_extremes(row, self.first, 100)["complete"])
        sparse = {**row, "_tracking_bars": [b for i, b in enumerate(self.bars) if i < 5 or i > 32]}
        self.assertFalse(scanner.validation_extremes(sparse, self.first, 100)["complete"])
        outcome_peak, _, _, _ = scanner.tracking_extremes(
            {"_tracking_bars": [], "current_price": 200}, self.first, 100
        )
        self.assertEqual(outcome_peak, 0.0)

    def test_barrier_reports_candle_close_and_ambiguous_same_candle(self):
        first_bar = dict(self.bars[0], high=106, low=96)
        result = scanner.validation_barrier(
            {"_tracking_bars": [first_bar]}, self.first, 100
        )
        self.assertEqual(result["result"], "동일 봉 동시 도달·순서 미확정")
        self.assertEqual(result["observed_at"], "2026-09-27T00:30:00Z")

    def test_outcome_requires_candle_coverage_through_twelve_hours(self):
        end = self.first + timedelta(hours=12)
        self.assertTrue(scanner.candle_coverage({"_tracking_bars": self.bars}, self.first, end))
        sparse = [bar for i, bar in enumerate(self.bars) if i < 3 or i > 25]
        self.assertFalse(scanner.candle_coverage({"_tracking_bars": sparse}, self.first, end))


if __name__ == "__main__":
    unittest.main()
