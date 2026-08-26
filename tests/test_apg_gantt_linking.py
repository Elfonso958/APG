import os
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from app.sync.envision_apg_sync import _find_apg_plan_id_by_local_clock


def _gantt_row(std: str) -> dict:
    return {
        "flight": "3C718",
        "dep": "AKL",
        "dest": "WAG",
        "std": std,
    }


class ApgGanttLinkingTests(unittest.TestCase):
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
