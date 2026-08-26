import os
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from app.sync.envision_apg_sync import (
    _find_apg_plan_id_by_local_clock,
    _validated_persisted_apg_plan_id,
)


def _gantt_row(std: str) -> dict:
    return {
        "flight": "3C718",
        "dep": "AKL",
        "dest": "WAG",
        "std": std,
    }


class ApgGanttLinkingTests(unittest.TestCase):
    def test_persisted_plan_is_restored_after_full_validation(self):
        row = _gantt_row("2026-08-26 12:30:00")
        row.update({"flight": "3C713", "dep": "WAG", "dest": "AKL"})
        plan = {
            "id": 4877793, "flight_no": "CVA713", "adep": "NZWU", "ades": "NZAA",
            "eobt": "2026-08-26T00:30:00+00:00",
        }

        self.assertEqual(_validated_persisted_apg_plan_id(row, 4877793, plan), 4877793)

    def test_persisted_plan_is_rejected_for_another_day(self):
        row = _gantt_row("2026-08-26 15:00:00")
        plan = {
            "id": 4875855, "flight_no": "CVA718", "adep": "NZAA", "ades": "NZWU",
            "eobt": "2026-08-28T03:00:00+00:00",
        }

        self.assertIsNone(_validated_persisted_apg_plan_id(row, 4875855, plan))

    @patch.dict(os.environ, {}, clear=False)
    def test_local_clock_match_does_not_cross_calendar_days(self):
        os.environ.pop("APG_GANTT_LINK_LOCAL_DATE_DIFF_DAYS", None)
        row = _gantt_row("2026-08-27 15:00:00")
        next_day_plan = datetime(2026, 8, 28, 3, 0, tzinfo=timezone.utc)

        self.assertIsNone(
            _find_apg_plan_id_by_local_clock(row, [(next_day_plan, 4875855)])
        )

    @patch.dict(os.environ, {}, clear=False)
    def test_local_clock_match_still_accepts_same_day_plan(self):
        os.environ.pop("APG_GANTT_LINK_LOCAL_DATE_DIFF_DAYS", None)
        row = _gantt_row("2026-08-28 15:00:00")
        same_day_plan = datetime(2026, 8, 28, 3, 0, tzinfo=timezone.utc)

        self.assertEqual(
            _find_apg_plan_id_by_local_clock(row, [(same_day_plan, 4875855)]),
            4875855,
        )
