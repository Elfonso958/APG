import unittest
from datetime import datetime, timezone

from app.sync.envision_apg_sync import (
    _find_apg_plan_id_by_local_clock,
    _sync_plan_id_for_key,
)


class ApgPlanLinkingTests(unittest.TestCase):
    def test_matches_plan_on_same_nz_operating_date(self):
        row = {"std_utc": "2026-08-26T19:00:00+00:00"}
        candidates = [
            (datetime(2026, 8, 26, 19, 0, tzinfo=timezone.utc), 1001),
            (datetime(2026, 8, 27, 19, 0, tzinfo=timezone.utc), 1002),
        ]

        self.assertEqual(_find_apg_plan_id_by_local_clock(row, candidates), 1001)

    def test_rejects_identical_flight_time_on_adjacent_nz_date(self):
        row = {"std_utc": "2026-08-26T19:00:00+00:00"}
        next_day_plan = [
            (datetime(2026, 8, 27, 19, 0, tzinfo=timezone.utc), 1002),
        ]

        self.assertIsNone(_find_apg_plan_id_by_local_clock(row, next_day_plan))

    def test_importer_creates_when_only_adjacent_date_plan_exists(self):
        target = ("L816", "NFTF", "NFTV", "2026-08-26T23:20Z")
        adjacent_date = {
            ("L816", "NFTF", "NFTV", "2026-08-27T23:20Z"): 2002,
        }

        self.assertIsNone(_sync_plan_id_for_key(target, adjacent_date, cached_plan_id=2002))

    def test_importer_updates_same_date_plan_after_time_change(self):
        target = ("L816", "NFTF", "NFTV", "2026-08-26T23:20Z")
        same_operating_date = {
            ("L816", "NFTF", "NFTV", "2026-08-26T22:50Z"): 2001,
        }

        self.assertEqual(_sync_plan_id_for_key(target, same_operating_date), 2001)

    def test_importer_rejects_stale_cached_id_not_visible_in_apg(self):
        target = ("3C718", "NZWU", "NZAA", "2026-08-27T03:00Z")

        self.assertIsNone(_sync_plan_id_for_key(target, {}, cached_plan_id=9999))


if __name__ == "__main__":
    unittest.main()
