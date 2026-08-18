import unittest
from datetime import date

from app.otp_cache_job import rolling_otp_cache_dates


class OtpCacheJobTests(unittest.TestCase):
    def test_rolling_dates_include_today_and_previous_two_days(self):
        self.assertEqual(
            rolling_otp_cache_dates(today=date(2026, 8, 18), lookback_days=2),
            ("2026-08-16", "2026-08-18"),
        )


if __name__ == "__main__":
    unittest.main()
