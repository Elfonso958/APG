import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, time
from typing import Any

import requests


class EnvisionOtpError(RuntimeError):
    pass


class EnvisionDateError(ValueError):
    pass


class EnvisionAuthError(EnvisionOtpError):
    pass


class EnvisionFlightsFetchError(EnvisionOtpError):
    pass


def _normalise_base_url(base_url: str | None) -> str:
    base = str(base_url or "").strip().rstrip("/")
    if not base:
        raise EnvisionAuthError("ENVISION_BASE is not configured.")
    return base if base.endswith("/v1") else f"{base}/v1"


def _parse_date_param(value: str | None, name: str) -> datetime | date:
    raw = str(value or "").strip()
    if not raw:
        raise EnvisionDateError(f"{name} is required.")
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw):
        try:
            return date.fromisoformat(raw)
        except ValueError as exc:
            raise EnvisionDateError(f"{name} must be ISO datetime or yyyy-MM-dd.") from exc
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise EnvisionDateError(f"{name} must be ISO datetime or yyyy-MM-dd.") from exc


def validate_date_range(date_from: str | None, date_to: str | None) -> tuple[str, str]:
    parsed_from = _parse_date_param(date_from, "dateFrom")
    parsed_to = _parse_date_param(date_to, "dateTo")
    compare_from = datetime.combine(parsed_from, time.min) if isinstance(parsed_from, date) and not isinstance(parsed_from, datetime) else parsed_from
    compare_to = datetime.combine(parsed_to, time.min) if isinstance(parsed_to, date) and not isinstance(parsed_to, datetime) else parsed_to
    if isinstance(compare_from, datetime):
        compare_from = compare_from.replace(tzinfo=None)
    if isinstance(compare_to, datetime):
        compare_to = compare_to.replace(tzinfo=None)
    if compare_to < compare_from:
        raise EnvisionDateError("dateTo must be greater than or equal to dateFrom.")
    return str(date_from).strip(), str(date_to).strip()


def parse_page_size(value: str | None, default: int = 500) -> int:
    raw = str(value or "").strip()
    if not raw:
        return default
    try:
        page_size = int(raw)
    except ValueError as exc:
        raise EnvisionDateError("limit/pageSize must be a positive integer.") from exc
    if page_size <= 0:
        raise EnvisionDateError("limit/pageSize must be a positive integer.")
    return page_size


def _parse_datetime(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value
    raw = str(value).strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None


def _minutes_between(actual: Any, scheduled: Any) -> int | None:
    actual_dt = _parse_datetime(actual)
    scheduled_dt = _parse_datetime(scheduled)
    if not actual_dt or not scheduled_dt:
        return None
    return int(round((actual_dt - scheduled_dt).total_seconds() / 60))


def _normalise_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").lower())


def _dict_key_lookup(payload: dict[str, Any]) -> dict[str, Any]:
    return {_normalise_key(k): v for k, v in payload.items()}


def _first_value(payload: Any, candidates: list[str]) -> Any:
    if payload is None:
        return None
    if isinstance(payload, dict):
        lookup = _dict_key_lookup(payload)
        for candidate in candidates:
            key = _normalise_key(candidate)
            if key in lookup:
                return lookup[key]
        for container in ("result", "data", "items"):
            nested = lookup.get(container)
            value = _first_value(nested, candidates)
            if value is not None:
                return value
        return None
    if isinstance(payload, list):
        values = [_first_value(item, candidates) for item in payload]
        numeric_values = [_to_number(v) for v in values if _to_number(v) is not None]
        if numeric_values:
            return sum(numeric_values)
    return None


def _to_number(value: Any) -> float | int | None:
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return value
    try:
        num = float(str(value).strip())
    except (TypeError, ValueError):
        return None
    return int(num) if num.is_integer() else num


def _number(payload: Any, candidates: list[str]) -> float | int | None:
    return _to_number(_first_value(payload, candidates))


def _sum_numbers(values: list[Any]) -> float | int | None:
    nums = [_to_number(v) for v in values]
    nums = [v for v in nums if v is not None]
    if not nums:
        return None
    total = sum(nums)
    return int(total) if float(total).is_integer() else total


def _coerce_list(payload: Any, label: str) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        for key in ("result", "data", "items", label):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
    raise EnvisionFlightsFetchError(f"Unexpected /{label} response shape.")


def _items(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        for key in ("items", "data", "result"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
        return [payload]
    return []


def _text(value: Any) -> str:
    return str(value).strip() if value not in (None, "") else ""


def _additional_oil_fuel_summary(payload: Any) -> tuple[float | int | None, str | None]:
    rows = _items(payload)
    total = _sum_numbers([row.get("upliftUsage") for row in rows])
    details: list[str] = []
    for row in rows:
        parts = [
            _text(row.get("upliftType")),
            _text(row.get("inputName")),
            _text(row.get("location")),
            _text(row.get("upliftUsage")),
            _text(row.get("upliftUnitOfMeasure")),
        ]
        detail = " ".join(part for part in parts if part)
        if detail:
            details.append(detail)
    return total, " | ".join(details) if details else None


class EnvisionOtpClient:
    def __init__(
        self,
        base_url: str | None,
        username: str | None,
        password: str | None,
        *,
        nonce: str = "api",
        timeout: int = 12,
        max_workers: int = 12,
        session: requests.Session | None = None,
        logger: logging.Logger | None = None,
        tenant: str | None = None,
    ) -> None:
        self.base_url = _normalise_base_url(base_url)
        self.username = str(username or "").strip()
        self.password = str(password or "").strip()
        self.nonce = nonce
        self.timeout = timeout
        self.max_workers = max(1, int(max_workers or 1))
        self.session = session or requests.Session()
        self.logger = logger or logging.getLogger(__name__)
        self.tenant = str(tenant or "").strip()

    def _headers(self, token: str | None = None) -> dict[str, str]:
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        if self.tenant:
            headers["X-Tenant-Id"] = self.tenant
        return headers

    def authenticate(self) -> str:
        if not self.username or not self.password:
            raise EnvisionAuthError("Envision username/password are not configured.")
        url = f"{self.base_url}/Authenticate"
        payload = {
            "username": self.username,
            "password": self.password,
            "nonce": self.nonce,
        }
        try:
            resp = self.session.post(url, json=payload, headers=self._headers(), timeout=self.timeout)
            resp.raise_for_status()
        except requests.RequestException as exc:
            raise EnvisionAuthError(f"Envision authentication failed: {exc}") from exc

        data = resp.json() or {}
        token = data.get("token")
        if not token:
            raise EnvisionAuthError("Envision authentication response did not include a token.")
        return str(token)

    def fetch_flights(self, token: str, date_from: str, date_to: str, page_size: int) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        offset = 0
        while True:
            params = {
                "dateFrom": date_from,
                "dateTo": date_to,
                "offset": offset,
                "limit": page_size,
            }
            try:
                resp = self.session.get(
                    f"{self.base_url}/Flights",
                    headers=self._headers(token),
                    params=params,
                    timeout=self.timeout,
                )
                resp.raise_for_status()
                page = _coerce_list(resp.json() or [], "Flights")
            except requests.RequestException as exc:
                raise EnvisionFlightsFetchError(f"Envision flights fetch failed: {exc}") from exc

            rows.extend(page)
            if len(page) < page_size:
                break
            offset += page_size
        return rows

    def fetch_optional_section(self, token: str, flight_id: Any, section: str) -> Any:
        try:
            resp = self.session.get(
                f"{self.base_url}/Flights/{flight_id}/{section}",
                headers=self._headers(token),
                timeout=self.timeout,
            )
            resp.raise_for_status()
            return resp.json() if resp.content else None
        except Exception as exc:
            self.logger.warning(
                "Envision optional flight enrichment failed: flight_id=%s section=%s error=%s",
                flight_id,
                section,
                exc,
            )
            return None

    def fetch_otp_flights(self, date_from: str, date_to: str, page_size: int = 500) -> list[dict[str, Any]]:
        token = self.authenticate()
        flights = self.fetch_flights(token, date_from, date_to, page_size)
        if not flights:
            return []

        worker_count = min(self.max_workers, len(flights))
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            return list(executor.map(lambda flight: self.safe_enrich_flight(token, flight), flights))

    def safe_enrich_flight(self, token: str, flight: dict[str, Any]) -> dict[str, Any]:
        try:
            return self.enrich_flight(token, flight)
        except Exception as exc:
            self.logger.warning(
                "Envision flight enrichment failed: flight_id=%s error=%s",
                flight.get("id"),
                exc,
            )
            return flatten_otp_flight(flight, {})

    def enrich_flight(self, token: str, flight: dict[str, Any]) -> dict[str, Any]:
        flight_id = flight.get("id")
        sections = {
            "passengers": self.fetch_optional_section(token, flight_id, "Passengers") if flight_id else None,
            "freight": self.fetch_optional_section(token, flight_id, "Freight") if flight_id else None,
            "baggage": self.fetch_optional_section(token, flight_id, "Baggage") if flight_id else None,
            "oil_fuel": self.fetch_optional_section(token, flight_id, "OilAndFuel") if flight_id else None,
            "additional_oil_fuel": self.fetch_optional_section(token, flight_id, "AdditionalOilAndFuel") if flight_id else None,
        }
        return flatten_otp_flight(flight, sections)


def flatten_otp_flight(flight: dict[str, Any], sections: dict[str, Any]) -> dict[str, Any]:
    row = dict(flight)
    dep_delay = _minutes_between(flight.get("departureActual"), flight.get("departureScheduled"))
    arr_delay = _minutes_between(flight.get("arrivalActual"), flight.get("arrivalScheduled"))
    status = str(flight.get("flightStatusDescription") or "").lower()

    passengers = sections.get("passengers")
    freight = sections.get("freight")
    baggage = sections.get("baggage")
    oil_fuel = sections.get("oil_fuel")
    additional_oil_fuel = sections.get("additional_oil_fuel")
    additional_total, additional_details = _additional_oil_fuel_summary(additional_oil_fuel)

    passenger_adult = _number(passengers, ["adult", "adults", "passengerAdult", "actualAdult"])
    passenger_child = _number(passengers, ["child", "children", "passengerChild", "actualChild"])
    passenger_infant = _number(passengers, ["infant", "infants", "passengerInfant", "actualInfant"])

    row.update(
        {
            "Route": f"{flight.get('departurePlaceDescription') or ''}-{flight.get('arrivalPlaceDescription') or ''}",
            "Departure Delay Minutes": dep_delay,
            "Arrival Delay Minutes": arr_delay,
            "Departure OTP": dep_delay is not None and dep_delay <= 15,
            "Arrival OTP": arr_delay is not None and arr_delay <= 15,
            "Cancelled": (
                "cancel" in status
                or "not flown" in status
                or bool(flight.get("abortCancelCodeDescription"))
            ),
            "Diverted": bool(flight.get("divertedPlaceDescription") or flight.get("divertedReason")),
            "Passenger Expected": _number(passengers, ["expected", "passengerExpected", "expectedPassengers"]),
            "Passenger Adult": passenger_adult,
            "Passenger Male": _number(passengers, ["male", "passengerMale", "actualMale"]),
            "Passenger Female": _number(passengers, ["female", "passengerFemale", "actualFemale"]),
            "Passenger Child": passenger_child,
            "Passenger Infant": passenger_infant,
            "Passenger Expected Adult": _number(passengers, ["expectedAdult", "expectedAdults", "passengerExpectedAdult"]),
            "Passenger Expected Male": _number(passengers, ["expectedMale", "passengerExpectedMale"]),
            "Passenger Expected Female": _number(passengers, ["expectedFemale", "passengerExpectedFemale"]),
            "Passenger Expected Child": _number(passengers, ["expectedChild", "expectedChildren", "passengerExpectedChild"]),
            "Passenger Expected Infant": _number(passengers, ["expectedInfant", "expectedInfants", "passengerExpectedInfant"]),
            "Passenger Actual Total": _sum_numbers([passenger_adult, passenger_child, passenger_infant]),
            "Freight Expected": _number(freight, ["expected", "freightExpected", "expectedFreight", "expectedWeight"]),
            "Freight Actual": _number(freight, ["actual", "freightActual", "actualFreight", "actualWeight"]),
            "Baggage Actual Pieces": _number(baggage, ["actualPieces", "baggageActualPieces"]),
            "Baggage Expected Pieces": _number(baggage, ["expectedPieces", "baggageExpectedPieces"]),
            "Baggage Actual Weight": _number(baggage, ["actualWeight", "baggageActualWeight"]),
            "Baggage Expected Weight": _number(baggage, ["expectedWeight", "baggageExpectedWeight"]),
            "Fuel UOM Id": _number(oil_fuel, ["fuelUomId", "fuelUOMId", "fuelUnitOfMeasureId"]),
            "Fuel Carried From Last Journey": _number(oil_fuel, ["fuelCarriedFromLastJourney", "carriedFromLastJourney"]),
            "Fuel Uplift Planned": _number(oil_fuel, ["fuelUpliftPlanned", "upliftPlanned"]),
            "Fuel Uplift Actual": _number(oil_fuel, ["fuelUpliftActual", "upliftActual"]),
            "Departure Fuel": _number(oil_fuel, ["departureFuel", "fuelDeparture"]),
            "Arrival Fuel": _number(oil_fuel, ["arrivalFuel", "fuelArrival"]),
            "Oil Uplift": _number(oil_fuel, ["oilUplift", "oilUpliftActual"]),
            "Additional Oil/Fuel Usage Total": additional_total,
            "Additional Oil/Fuel Details": additional_details,
        }
    )
    return row


def build_envision_otp_client(config: dict[str, Any], logger: logging.Logger | None = None) -> EnvisionOtpClient:
    username = (
        config.get("ENVISION_USER")
        or config.get("ENVISION_USERNAME")
        or os.getenv("ENVISION_USER")
        or os.getenv("ENVISION_USERNAME")
    )
    password = (
        config.get("ENVISION_PASS")
        or config.get("ENVISION_PASSWORD")
        or os.getenv("ENVISION_PASS")
        or os.getenv("ENVISION_PASSWORD")
    )
    return EnvisionOtpClient(
        config.get("ENVISION_BASE") or os.getenv("ENVISION_BASE"),
        username,
        password,
        timeout=int(config.get("ENVISION_OTP_TIMEOUT") or os.getenv("ENVISION_OTP_TIMEOUT") or 12),
        max_workers=int(config.get("ENVISION_OTP_MAX_WORKERS") or os.getenv("ENVISION_OTP_MAX_WORKERS") or 12),
        logger=logger,
        tenant=config.get("ENVISION_TENANT") or os.getenv("ENVISION_TENANT"),
    )
