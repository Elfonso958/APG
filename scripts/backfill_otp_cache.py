import argparse
from datetime import date, timedelta

from app import create_app
from app.envision_otp_client import build_envision_otp_client
from app.otp_cache import upsert_otp_cache_rows


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def _windows(start: date, end: date, window_days: int):
    cursor = start
    while cursor <= end:
        window_end = min(cursor + timedelta(days=window_days - 1), end)
        yield cursor.isoformat(), window_end.isoformat()
        cursor = window_end + timedelta(days=1)


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill the Envision OTP Power BI cache.")
    parser.add_argument("--date-from", required=True, help="Start date, yyyy-mm-dd")
    parser.add_argument("--date-to", required=True, help="End date, yyyy-mm-dd")
    parser.add_argument("--window-days", type=int, default=7, help="Backfill window size per DB commit")
    parser.add_argument("--chunk-days", type=int, default=1, help="Envision /Flights query chunk size")
    parser.add_argument("--page-size", type=int, default=100)
    parser.add_argument("--include-details", action="store_true", help="Include slow passenger/freight/baggage/fuel enrichment")
    args = parser.parse_args()

    app = create_app()
    start = _parse_date(args.date_from)
    end = _parse_date(args.date_to)

    with app.app_context():
        client = build_envision_otp_client(app.config, app.logger)
        total = {"created": 0, "updated": 0, "skipped": 0, "rows": 0}
        for window_from, window_to in _windows(start, end, args.window_days):
            rows = client.fetch_otp_flights(
                window_from,
                window_to,
                page_size=args.page_size,
                include_details=args.include_details,
                chunk_days=args.chunk_days,
            )
            result = upsert_otp_cache_rows(rows)
            total["rows"] += len(rows)
            total["created"] += result["created"]
            total["updated"] += result["updated"]
            total["skipped"] += result["skipped"]
            print(
                f"{window_from} to {window_to}: rows={len(rows)} "
                f"created={result['created']} updated={result['updated']} skipped={result['skipped']}",
                flush=True,
            )

        print(
            f"Done: rows={total['rows']} created={total['created']} "
            f"updated={total['updated']} skipped={total['skipped']}",
            flush=True,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

