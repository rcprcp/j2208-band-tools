"""Unit tests for daemon helpers (no BLE hardware required)."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))

try:
    from j2208_daemon import has_live_metrics
except ImportError:  # pragma: no cover - bleak is optional for store-only tests
    has_live_metrics = None


@unittest.skipUnless(has_live_metrics is not None, "bleak not installed")
class LiveMetricsTests(unittest.TestCase):
    def test_all_zero_is_ignored(self):
        rec = {
            "hr": 0,
            "spo2": 0,
            "hrv": 0,
            "stress": 0,
            "bp_sys": 0,
            "bp_dia": 0,
            "temp_c": 0.0,
        }
        self.assertFalse(has_live_metrics(rec))

    def test_empty_record_is_ignored(self):
        self.assertFalse(has_live_metrics({}))

    def test_hr_is_kept(self):
        self.assertTrue(has_live_metrics({"hr": 63}))

    def test_spo2_only_is_kept(self):
        self.assertTrue(has_live_metrics({"spo2": 99}))

    def test_bp_only_is_kept(self):
        self.assertTrue(has_live_metrics({"bp_sys": 119, "bp_dia": 69}))

    def test_hrv_and_stress_are_kept(self):
        self.assertTrue(has_live_metrics({"hrv": 46, "stress": 11}))

    def test_temp_only_is_kept(self):
        self.assertTrue(has_live_metrics({"temp_c": 36.3}))


if __name__ == "__main__":
    unittest.main()
