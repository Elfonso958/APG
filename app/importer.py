from datetime import datetime, timedelta, timezone
from . import db
from .models import ImportRun, Flight, Passenger
from .source_client import fetch_flights
from .apg_client import post_flights_to_apg

def _map_and_persist(import_run: ImportRun, flights_json: dict):
    flights = flights_json.get("Flights", []) or []
    pax_total = 0
    for f in flights:
        flight = Flight(
            import_run_id=import_run.id,
            flight_number=(f.get("FlightNumber") or "").strip(),
            flight_date=datetime.fromisoformat(f.get("FlightDate").replace("Z","+00:00")),
            flight_status=f.get("FlightStatus"),
            origin=f.get("Origin"),
            destination=f.get("Destination"),
        )
        db.session.add(flight)
        for p in (f.get("Passengers") or []):
            pax = Passenger(
                flight=flight,
                uid=p.get("UID"),
                booking_reference_id=p.get("BookingReferenceID"),
                name_prefix=p.get("NamePrefix"),
                given_name=p.get("GivenName"),
                surname=p.get("Surname"),
                booking_class_code=p.get("BookingClassCode"),
                seat=p.get("Seat"),
                status=p.get("Status"),
                iata_status=p.get("IataStatus"),
                tvl_iata_segment_status=p.get("TVLIATASegmentStatus"),
                baggage_allowance=p.get("BaggageAllowance"),
                mobile_phone_number=p.get("MobilePhoneNumber"),
                phone_number=p.get("PhoneNumber"),
                office_phone=p.get("OfficePhone"),
                email=p.get("Email"),
                eticket_number=p.get("ETicketNumber"),
                nationality=p.get("Nationality"),
                document_type=p.get("DocumentType"),
            )
            db.session.add(pax)
            pax_total += 1
    import_run.flights_count = len(flights)
    import_run.pax_count = pax_total

def run_import(from_dt: datetime=None, to_dt: datetime=None) -> ImportRun:
    if not from_dt or not to_dt:
        to_dt = datetime.now(timezone.utc)
        from_dt = to_dt - timedelta(hours=24)
    imp = ImportRun(source_from=from_dt, source_to=to_dt, status="running")
    db.session.add(imp); db.session.commit()
    try:
        flights_json = fetch_flights(from_dt, to_dt)
        _map_and_persist(imp, flights_json); db.session.commit()
        apg_resp = post_flights_to_apg(flights_json)
        imp.apg_response_summary = f"APG status {apg_resp.get('status_code')}; body (trimmed): {str(apg_resp.get('response'))[:900]}"
        imp.status = "success" if 200 <= apg_resp.get("status_code", 500) < 300 else "partial"
    except Exception as e:
        db.session.rollback()
        imp.status = "failed"; imp.error_summary = str(e)
    finally:
        db.session.add(imp); db.session.commit()
    return imp
