import unittest
from datetime import datetime, timezone

from app.sync.envision_apg_sync import _find_apg_plan_id_by_local_clock


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


if __name__ == "__main__":
    unittest.main()
