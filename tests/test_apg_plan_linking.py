import unittest
from datetime import datetime, timezone

from app.sync.envision_apg_sync import (
    _find_apg_plan_id_by_local_clock,
    _sync_plan_id_for_key,
    _cached_plan_owner_is_unambiguous,
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

    def test_cached_id_cannot_override_departure_tolerance(self):
        morning = ("CVA332", "NZCI", "NZWN", "2026-11-24T21:45Z")
        afternoon = ("CVA332", "NZCI", "NZWN", "2026-11-25T04:15Z")
        self.assertIsNone(_sync_plan_id_for_key(morning, {afternoon: 4971425}, 4971425))
        self.assertEqual(_sync_plan_id_for_key(afternoon, {afternoon: 4971425}, 4971425), 4971425)

    def test_duplicate_cache_id_is_owned_by_closest_sector_only(self):
        afternoon = ("CVA332", "NZCI", "NZWN", "2026-11-25T04:15Z")
        cache = {
            "83014": {"apg_id": 4971425, "core": {"flight_no": "CVA332", "adep": "NZCI", "ades": "NZWN", "eobt": "2026-11-24T21:45:00+00:00"}},
            "83016": {"apg_id": 4971425, "core": {"flight_no": "CVA332", "adep": "NZCI", "ades": "NZWN", "eobt": "2026-11-25T04:15:00+00:00"}},
        }
        self.assertFalse(_cached_plan_owner_is_unambiguous(cache, "83014", 4971425, {afternoon: 4971425}))
        self.assertTrue(_cached_plan_owner_is_unambiguous(cache, "83016", 4971425, {afternoon: 4971425}))

    def test_equal_claims_require_review(self):
        key = ("CVA332", "NZCI", "NZWN", "2026-11-25T04:15Z")
        core = {"flight_no": "CVA332", "adep": "NZCI", "ades": "NZWN", "eobt": "2026-11-25T04:15:00+00:00"}
        cache = {"1": {"apg_id": 42, "core": core}, "2": {"apg_id": 42, "core": core}}
        self.assertFalse(_cached_plan_owner_is_unambiguous(cache, "1", 42, {key: 42}))
        self.assertFalse(_cached_plan_owner_is_unambiguous(cache, "2", 42, {key: 42}))


if __name__ == "__main__":
    unittest.main()
