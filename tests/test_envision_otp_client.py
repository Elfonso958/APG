import unittest

import requests

from app.envision_otp_client import EnvisionOtpClient, flatten_otp_flight, validate_date_range


class FakeResponse:
    def __init__(self, payload=None, status_code=200):
        self.payload = payload
        self.status_code = status_code
        self.content = b"{}" if payload is not None else b""

    def json(self):
        return self.payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")


class FakeSession:
    def __init__(self):
        self.get_calls = []

    def post(self, url, json=None, headers=None, timeout=None):
        self.post_call = {"url": url, "json": json, "headers": headers, "timeout": timeout}
        return FakeResponse({"token": "token-123"})

    def get(self, url, headers=None, params=None, timeout=None):
        self.get_calls.append({"url": url, "headers": headers, "params": params, "timeout": timeout})
        if url.endswith("/Flights"):
            if params["offset"] == 0:
                return FakeResponse(
                    [
                        {
                            "id": 10,
                            "flightRegistrationId": 1021,
                            "departurePlaceDescription": "AKL",
                            "arrivalPlaceDescription": "CHT",
                            "departureScheduled": "2026-06-01T10:00:00Z",
                            "departureActual": "2026-06-01T10:12:00Z",
                            "arrivalScheduled": "2026-06-01T12:00:00Z",
                            "arrivalActual": "2026-06-01T12:20:00Z",
                        },
                        {
                            "id": 11,
                            "flightRegistrationId": 4,
                            "departurePlaceDescription": "AKL",
                            "arrivalPlaceDescription": "WLG",
                        }
                    ]
                )
            return FakeResponse([])
        if url.endswith("/Registrations"):
            return FakeResponse(
                [
                    {
                        "id": 1021,
                        "registration": "ZK-CIT",
                        "statusId": 1,
                        "status": "ACTIVE",
                        "model": "SF340A",
                    },
                    {
                        "id": 4,
                        "registration": "ZK-CIC",
                        "statusId": 2,
                        "status": "INACTIVE",
                        "model": "SA227AC",
                    },
                ]
            )
        if url.endswith("/Passengers"):
            return FakeResponse({"expected": 8, "adult": 5, "child": 2, "infant": 1, "male": 4, "female": 3})
        if url.endswith("/Freight"):
            return FakeResponse({"error": "upstream failed"}, status_code=500)
        if url.endswith("/Baggage"):
            return FakeResponse({"actualPieces": 7, "expectedPieces": 8, "actualWeight": 90, "expectedWeight": 100})
        if url.endswith("/OilAndFuel"):
            return FakeResponse({"fuelUomId": 1, "departureFuel": 1200, "arrivalFuel": 800})
        if url.endswith("/AdditionalOilAndFuel"):
            return FakeResponse(
                [
                    {
                        "upliftType": "Oil",
                        "inputName": "Engine 1",
                        "location": "AKL",
                        "upliftUsage": 2,
                        "upliftUnitOfMeasure": "L",
                    }
                ]
            )
        raise AssertionError(f"Unexpected URL {url}")


class EnvisionOtpClientTests(unittest.TestCase):
    def test_validate_date_range_accepts_date_and_datetime(self):
        self.assertEqual(
            validate_date_range("2026-06-01", "2026-06-02T12:00:00"),
            ("2026-06-01", "2026-06-02T12:00:00"),
        )

    def test_fetch_otp_flights_paginates_and_continues_when_enrichment_fails(self):
        session = FakeSession()
        client = EnvisionOtpClient(
            "https://envision.example/v1",
            "user",
            "pass",
            session=session,
            timeout=5,
        )

        rows = client.fetch_otp_flights("2026-06-01", "2026-06-02", page_size=2)

        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["id"], 10)
        self.assertEqual(row["aircraftStatus"], "ACTIVE")
        self.assertEqual(row["aircraftType"], "Saab 340")
        self.assertEqual(row["Route"], "AKL-CHT")
        self.assertEqual(row["departureScheduled"], "2026-06-01T10:00:00Z")
        self.assertEqual(row["departureActual"], "2026-06-01T10:12:00Z")
        self.assertEqual(row["Departure Delay Minutes"], 12)
        self.assertTrue(row["Departure OTP"])
        self.assertEqual(row["Arrival Delay Minutes"], 20)
        self.assertFalse(row["Arrival OTP"])
        self.assertEqual(row["Passenger Actual Total"], 8)
        self.assertIsNone(row["Freight Actual"])
        self.assertEqual(row["Baggage Actual Weight"], 90)
        self.assertEqual(row["Additional Oil/Fuel Usage Total"], 2)
        self.assertIn("Engine 1", row["Additional Oil/Fuel Details"])
        self.assertEqual(session.post_call["json"]["nonce"], "api")

    def test_flatten_marks_cancelled_and_diverted(self):
        row = flatten_otp_flight(
            {
                "flightStatusDescription": "Not Flown",
                "abortCancelCodeDescription": "WX",
                "divertedReason": "Weather",
            },
            {},
        )

        self.assertTrue(row["Cancelled"])
        self.assertTrue(row["Diverted"])

if __name__ == "__main__":
    unittest.main()
