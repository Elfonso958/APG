import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, time, timedelta
from threading import Lock
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


DEFAULT_OTP_DATE_FROM = "2022-01-01"
DEFAULT_OTP_DATE_TO = "2035-12-31"


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


def parse_chunk_days(value: str | None, default: int = 31) -> int:
    raw = str(value or "").strip()
    if not raw:
        return default
    try:
        chunk_days = int(raw)
    except ValueError as exc:
        raise EnvisionDateError("chunkDays must be a positive integer.") from exc
    if chunk_days <= 0:
        raise EnvisionDateError("chunkDays must be a positive integer.")
    return chunk_days


def _date_part(value: datetime | date) -> date:
    return value.date() if isinstance(value, datetime) else value


def _next_month(value: date) -> date:
    if value.month == 12:
        return date(value.year + 1, 1, 1)
    return date(value.year, value.month + 1, 1)


def _monthly_date_chunks(date_from: str, date_to: str) -> list[tuple[str, str]]:
    start = _date_part(_parse_date_param(date_from, "dateFrom"))
    end = _date_part(_parse_date_param(date_to, "dateTo"))
    if start.year == end.year and start.month == end.month:
        return [(date_from, date_to)]

    chunks: list[tuple[str, str]] = []
    cursor = start
    while cursor <= end:
        month_end = min(_next_month(cursor) - timedelta(days=1), end)
        chunk_start = date_from if cursor == start else cursor.isoformat()
        chunk_end = date_to if month_end == end else month_end.isoformat()
        chunks.append((chunk_start, chunk_end))
        cursor = month_end + timedelta(days=1)
    return chunks


def _date_chunks(date_from: str, date_to: str, chunk_days: int = 31) -> list[tuple[str, str]]:
    if chunk_days >= 28:
        return _monthly_date_chunks(date_from, date_to)

    start = _date_part(_parse_date_param(date_from, "dateFrom"))
    end = _date_part(_parse_date_param(date_to, "dateTo"))
    chunks: list[tuple[str, str]] = []
    cursor = start
    while cursor <= end:
        chunk_end = min(cursor + timedelta(days=chunk_days - 1), end)
        chunk_start = date_from if cursor == start else cursor.isoformat()
        final_chunk_end = date_to if chunk_end == end else chunk_end.isoformat()
        chunks.append((chunk_start, final_chunk_end))
        cursor = chunk_end + timedelta(days=1)
    return chunks


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


def _to_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


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


def _position_text(row: dict[str, Any]) -> str:
    return " ".join(
        str(row.get(key) or "").strip().upper()
        for key in (
            "crewPosition",
            "description",
            "crewPositionDescription",
            "positionCode",
            "positionDescription",
        )
        if str(row.get(key) or "").strip()
    )


def _looks_like_captain_position(row: dict[str, Any]) -> bool:
    text = _position_text(row)
    return bool(row.get("isCaptain")) or "CPT" in text or "CAPTAIN" in text or "PIC" in text


def _looks_like_fo_position(row: dict[str, Any]) -> bool:
    text = _position_text(row)
    return bool(row.get("isFirstOfficer")) or re.search(r"\bFO\b|FIRST\s+OFFICER", text) is not None


def _employee_display(employee: dict[str, Any], crew_row: dict[str, Any]) -> tuple[str | None, str | None, int | None]:
    first = _text(employee.get("firstName") or employee.get("givenName") or crew_row.get("firstName") or crew_row.get("givenName"))
    last = _text(
        employee.get("surname")
        or employee.get("lastName")
        or employee.get("familyName")
        or crew_row.get("surname")
        or crew_row.get("lastName")
        or crew_row.get("familyName")
    )
    name = f"{first} {last}".strip()
    if not name:
        name = _text(
            employee.get("shortDisplayName")
            or employee.get("employeeName")
            or employee.get("displayName")
            or employee.get("employeeUsername")
            or crew_row.get("employeeName")
            or crew_row.get("displayName")
            or crew_row.get("crewMemberName")
        )
    employee_no = _text(employee.get("employeeNo") or crew_row.get("employeeNo") or crew_row.get("employeeCode")).upper() or None
    employee_id = _to_int(crew_row.get("employeeId"))
    return name or None, employee_no, employee_id


def _crew_sort_key(row: dict[str, Any]) -> tuple[int, int]:
    display_order = _to_int(row.get("displayOrder"))
    crew_id = _to_int(row.get("id"))
    return (display_order if display_order is not None else 9999, crew_id if crew_id is not None else 999999)


def _crew_position_id(row: dict[str, Any]) -> int | None:
    return _to_int(row.get("crewPositionId") or row.get("positionId"))


def _crew_role_summary(
    crew: Any,
    crew_positions: Any,
    employee_lookup,
) -> dict[str, Any]:
    crew_rows = [row for row in _items(crew) if row.get("employeeId")]
    positions = [row for row in _items(crew_positions) if _to_int(row.get("id")) is not None]
    position_by_id = {_to_int(row.get("id")): row for row in positions}

    captain_position_ids = {
        pos_id for pos_id, row in position_by_id.items()
        if pos_id is not None and _looks_like_captain_position(row)
    }
    fo_position_ids = {
        pos_id for pos_id, row in position_by_id.items()
        if pos_id is not None and _looks_like_fo_position(row)
    }
    pilot_position_ids = captain_position_ids | fo_position_ids

    def is_captain(row: dict[str, Any]) -> bool:
        pos_id = _crew_position_id(row)
        return (
            (pos_id in captain_position_ids)
            or _looks_like_captain_position(row)
            or _looks_like_captain_position(position_by_id.get(pos_id) or {})
        )

    def is_fo(row: dict[str, Any]) -> bool:
        pos_id = _crew_position_id(row)
        return (
            (pos_id in fo_position_ids)
            or _looks_like_fo_position(row)
            or _looks_like_fo_position(position_by_id.get(pos_id) or {})
        )

    def is_pilot(row: dict[str, Any]) -> bool:
        pos_id = _crew_position_id(row)
        return pos_id in pilot_position_ids or is_captain(row) or is_fo(row)

    def resolve(row: dict[str, Any] | None) -> tuple[str | None, str | None, int | None]:
        if not row:
            return None, None, None
        employee_id = row.get("employeeId")
        employee = employee_lookup(employee_id) if employee_id not in (None, "") else {}
        return _employee_display(employee or {}, row)

    operating = [row for row in crew_rows if bool(row.get("isOperating", True))]
    selectable = operating or crew_rows

    captain_candidates = sorted([row for row in selectable if is_captain(row)], key=_crew_sort_key)
    captain_row = captain_candidates[0] if captain_candidates else None
    if captain_row is None:
        pilot_flying = sorted([row for row in selectable if row.get("isPilotFlying") and is_pilot(row)], key=_crew_sort_key)
        captain_row = pilot_flying[0] if pilot_flying else None
    if captain_row is None:
        pilots = sorted([row for row in selectable if is_pilot(row)], key=_crew_sort_key)
        captain_row = pilots[0] if pilots else None

    captain_name, captain_no, captain_id = resolve(captain_row)

    fo_candidates = sorted(
        [
            row for row in selectable
            if row is not captain_row and (is_fo(row) or (is_pilot(row) and not is_captain(row)))
        ],
        key=_crew_sort_key,
    )
    fo_row = fo_candidates[0] if fo_candidates else None
    fo_name, fo_no, fo_id = resolve(fo_row)

    return {
        "Captain": captain_name,
        "Captain Employee No": captain_no,
        "Captain Employee Id": captain_id,
        "First Officer": fo_name,
        "First Officer Employee No": fo_no,
        "First Officer Employee Id": fo_id,
    }


def _normalise_registration(value: Any) -> str:
    return re.sub(r"\s+", "", str(value or "").strip().upper())


def _is_active_registration(registration: dict[str, Any]) -> bool:
    status = _normalise_registration(registration.get("status"))
    status_id = str(registration.get("statusId") or "").strip()
    return status == "ACTIVE" or status_id == "1"


def _friendly_aircraft_type(model: Any) -> str | None:
    raw = str(model or "").strip()
    if not raw:
        return None
    compact = re.sub(r"[\s_-]+", "", raw.upper())
    if compact.startswith("SF340") or "SAAB340" in compact:
        return "Saab 340"
    if compact.startswith("ATR72") or compact.startswith("ATR4272"):
        return "ATR72"
    if compact in {"DC3", "DOUGLASDC3"}:
        return "DC-3"
    if compact in {"CV580", "CONVAIRCV580"}:
        return "Convair 580"
    return raw


def _registration_lookups(registrations: list[dict[str, Any]]) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    by_id: dict[str, dict[str, Any]] = {}
    by_reg: dict[str, dict[str, Any]] = {}
    for registration in registrations:
        if not _is_active_registration(registration):
            continue
        reg_id = str(registration.get("id") or "").strip()
        reg = _normalise_registration(registration.get("registration"))
        if reg_id:
            by_id[reg_id] = registration
        if reg:
            by_reg[reg] = registration
    return by_id, by_reg


def _find_flight_registration(
    flight: dict[str, Any],
    by_id: dict[str, dict[str, Any]],
    by_reg: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    for key in ("flightRegistrationId", "registrationId", "regId", "aircraftRegistrationId"):
        reg_id = str(flight.get(key) or "").strip()
        if reg_id and reg_id in by_id:
            return by_id[reg_id]

    for key in ("flightRegistrationDescription", "aircraftRegistration", "registration", "reg"):
        reg = _normalise_registration(flight.get(key))
        if reg and reg in by_reg:
            return by_reg[reg]
    return None


def _with_registration_details(flight: dict[str, Any], registration: dict[str, Any]) -> dict[str, Any]:
    row = dict(flight)
    model = registration.get("model")
    row["aircraftType"] = _friendly_aircraft_type(model)
    row["aircraftModel"] = model
    row["aircraftModelId"] = registration.get("modelId")
    row["aircraftStatus"] = registration.get("status")
    row["aircraftStatusId"] = registration.get("statusId")
    row["aircraftSerialNo"] = registration.get("serialNo")
    row["aircraftOperator"] = registration.get("operator")
    row["aircraftAssetId"] = registration.get("assetId")
    return row


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


def _delay_summary(payload: Any) -> tuple[float | int | None, str | None, str | None]:
    rows = _items(payload)
    total = _sum_numbers([
        _first_value(row, ["delayMinutes", "minutes", "durationMinutes"])
        for row in rows
    ])

    codes: list[str] = []
    details: list[str] = []
    for row in rows:
        code = _text(_first_value(row, ["delayCode", "code"]))
        if code and code not in codes:
            codes.append(code)

        is_arrival = row.get("isArrival")
        timing = "ARR" if is_arrival is True else "DEP" if is_arrival is False else ""
        minutes = _text(_first_value(row, ["delayMinutes", "minutes", "durationMinutes"]))
        description = _text(_first_value(row, ["delayCodeDescription", "description", "reason"]))
        remarks = _text(_first_value(row, ["remarks", "remark", "comments", "comment"]))

        parts = [
            timing,
            code,
            f"{minutes}m" if minutes else "",
            description,
            remarks,
        ]
        detail = " ".join(part for part in parts if part)
        if detail:
            details.append(detail)

    return (
        total,
        " | ".join(codes) if codes else None,
        " | ".join(details) if details else None,
    )


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
        self._employee_cache: dict[str, dict[str, Any]] = {}
        self._employee_cache_lock = Lock()

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

    def _fetch_flights_for_range(self, token: str, date_from: str, date_to: str, page_size: int) -> list[dict[str, Any]]:
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

    def fetch_flights(
        self,
        token: str,
        date_from: str,
        date_to: str,
        page_size: int,
        chunk_days: int = 31,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        seen_ids: set[str] = set()
        for chunk_from, chunk_to in _date_chunks(date_from, date_to, chunk_days=chunk_days):
            chunk_rows = self._fetch_flights_for_range(token, chunk_from, chunk_to, page_size)
            for row in chunk_rows:
                flight_id = str(row.get("id") or "").strip()
                if flight_id:
                    if flight_id in seen_ids:
                        continue
                    seen_ids.add(flight_id)
                rows.append(row)
        return rows

    def fetch_registrations(self, token: str) -> list[dict[str, Any]]:
        try:
            resp = self.session.get(
                f"{self.base_url}/Registrations",
                headers=self._headers(token),
                timeout=self.timeout,
            )
            resp.raise_for_status()
            return _coerce_list(resp.json() or [], "Registrations")
        except requests.RequestException as exc:
            raise EnvisionFlightsFetchError(f"Envision registrations fetch failed: {exc}") from exc

    def fetch_crew_positions(self, token: str) -> list[dict[str, Any]]:
        try:
            resp = self.session.get(
                f"{self.base_url}/Crews/Positions",
                headers=self._headers(token),
                timeout=self.timeout,
            )
            resp.raise_for_status()
            return _coerce_list(resp.json() or [], "Crews/Positions")
        except requests.RequestException as exc:
            raise EnvisionFlightsFetchError(f"Envision crew positions fetch failed: {exc}") from exc

    def fetch_flight_crew(self, token: str, flight_id: Any) -> list[dict[str, Any]]:
        try:
            resp = self.session.get(
                f"{self.base_url}/Flights/{flight_id}/Crew",
                headers=self._headers(token),
                timeout=self.timeout,
            )
            resp.raise_for_status()
            return _coerce_list(resp.json() or [], "Flights/Crew")
        except requests.RequestException as exc:
            raise EnvisionFlightsFetchError(f"Envision flight crew fetch failed: {exc}") from exc

    def fetch_employee(self, token: str, employee_id: Any) -> dict[str, Any]:
        emp_id = str(employee_id or "").strip()
        if not emp_id:
            return {}
        with self._employee_cache_lock:
            cached = self._employee_cache.get(emp_id)
        if cached is not None:
            return cached

        try:
            resp = self.session.get(
                f"{self.base_url}/Employees/{emp_id}",
                headers=self._headers(token),
                timeout=self.timeout,
            )
            resp.raise_for_status()
            employee = resp.json() or {}
            if not isinstance(employee, dict):
                employee = {}
        except requests.RequestException as exc:
            self.logger.warning("Envision employee fetch failed: employee_id=%s error=%s", emp_id, exc)
            employee = {}

        with self._employee_cache_lock:
            self._employee_cache[emp_id] = employee
        return employee

    def filter_active_registration_flights(
        self,
        flights: list[dict[str, Any]],
        registrations: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        by_id, by_reg = _registration_lookups(registrations)
        active_flights: list[dict[str, Any]] = []
        for flight in flights:
            registration = _find_flight_registration(flight, by_id, by_reg)
            if registration:
                active_flights.append(_with_registration_details(flight, registration))
        return active_flights

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

    def fetch_otp_flights(
        self,
        date_from: str,
        date_to: str,
        page_size: int = 500,
        include_details: bool = True,
        chunk_days: int = 31,
    ) -> list[dict[str, Any]]:
        token = self.authenticate()
        flights = self.fetch_flights(token, date_from, date_to, page_size, chunk_days=chunk_days)
        registrations = self.fetch_registrations(token)
        flights = self.filter_active_registration_flights(flights, registrations)
        if not flights:
            return []

        try:
            crew_positions = self.fetch_crew_positions(token)
        except EnvisionFlightsFetchError as exc:
            self.logger.warning("Envision crew position enrichment disabled: %s", exc)
            crew_positions = []

        worker_count = min(self.max_workers, len(flights))
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            return list(
                executor.map(
                    lambda flight: self.safe_enrich_flight(
                        token,
                        flight,
                        crew_positions,
                        include_details=include_details,
                    ),
                    flights,
                )
            )

    def safe_enrich_flight(
        self,
        token: str,
        flight: dict[str, Any],
        crew_positions: list[dict[str, Any]],
        *,
        include_details: bool,
    ) -> dict[str, Any]:
        try:
            return self.enrich_flight(token, flight, crew_positions, include_details=include_details)
        except Exception as exc:
            self.logger.warning(
                "Envision flight enrichment failed: flight_id=%s error=%s",
                flight.get("id"),
                exc,
            )
            return flatten_otp_flight(flight, {})

    def enrich_flight(
        self,
        token: str,
        flight: dict[str, Any],
        crew_positions: list[dict[str, Any]],
        *,
        include_details: bool,
    ) -> dict[str, Any]:
        flight_id = flight.get("id")
        section_names = {
            "passengers": "Passengers",
            "freight": "Freight",
            "baggage": "Baggage",
            "oil_fuel": "OilAndFuel",
            "additional_oil_fuel": "AdditionalOilAndFuel",
            "delays": "Delays",
        }
        if not flight_id:
            return flatten_otp_flight(flight, {key: None for key in section_names})

        sections: dict[str, Any] = {}
        if include_details:
            with ThreadPoolExecutor(max_workers=len(section_names)) as executor:
                futures = {
                    key: executor.submit(self.fetch_optional_section, token, flight_id, section)
                    for key, section in section_names.items()
                }
                for key, future in futures.items():
                    sections[key] = future.result()
        try:
            sections["crew"] = self.fetch_flight_crew(token, flight_id)
        except EnvisionFlightsFetchError as exc:
            self.logger.warning("Envision crew enrichment failed: flight_id=%s error=%s", flight_id, exc)
            sections["crew"] = []
        sections["crew_positions"] = crew_positions
        sections["employee_lookup"] = lambda employee_id: self.fetch_employee(token, employee_id)
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
    delay_total, delay_codes, delay_details = _delay_summary(sections.get("delays"))
    employee_lookup = sections.get("employee_lookup")
    if not callable(employee_lookup):
        employee_lookup = lambda employee_id: {}
    crew_summary = {
        "Captain": None,
        "Captain Employee No": None,
        "Captain Employee Id": None,
        "First Officer": None,
        "First Officer Employee No": None,
        "First Officer Employee Id": None,
    }
    if "crew" in sections or "crew_positions" in sections:
        crew_summary.update(
            _crew_role_summary(
                sections.get("crew"),
                sections.get("crew_positions"),
                employee_lookup,
            )
        )

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
            "Delay Codes": delay_codes,
            "Delay Minutes Total": delay_total,
            "Delay Details": delay_details,
            **crew_summary,
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
