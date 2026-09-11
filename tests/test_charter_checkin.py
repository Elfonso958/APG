import unittest
from io import BytesIO
from unittest.mock import patch

from app import create_app, db
from app.config import Config
from app.models import CharterManifest
from app.views import _apply_charter_manifests


class CharterCheckinTest(unittest.TestCase):
    def setUp(self):
        Config.SQLALCHEMY_DATABASE_URI = "sqlite:///:memory:"
        self.app = create_app()
        self.app.config.update(TESTING=True)
        self.ctx = self.app.app_context()
        self.ctx.push()
        db.drop_all()
        db.create_all()
        self.client = self.app.test_client()

    def tearDown(self):
        db.session.remove()
        db.drop_all()
        self.ctx.pop()

    def test_charter_passengers_start_booked_and_can_be_checked_in(self):
        saved = self.client.put("/api/dcs/charter_manifest", json={
            "flight_id": "charter-123",
            "flight_number": "3C724",
            "dep": "AKL",
            "ades": "WAG",
            "passengers": [{
                "GivenName": "Jane",
                "Surname": "Passenger",
                "PassengerType": "AD",
                "BaggageWeight": 14.5,
                "BaggagePieces": 1,
            }, {
                "GivenName": "John",
                "Surname": "Passenger",
                "PassengerType": "AD",
            }],
        })
        self.assertEqual(saved.status_code, 200)
        passenger = saved.get_json()["passengers"][0]
        second_passenger = saved.get_json()["passengers"][1]
        self.assertEqual(passenger["Status"], "Booked")
        self.assertFalse(passenger["Boarded"])
        self.assertTrue(passenger["PassengerId"])

        updated = self.client.patch("/api/dcs/charter_manifest/passenger", json={
            "flight_id": "charter-123",
            "passenger_id": passenger["PassengerId"],
            "Status": "Checked In",
            "Seat": "2A",
            "BaggageWeight": 18.2,
            "BaggagePieces": 2,
            "SSR": "WCHR (Ramp assistance)",
            "Comments": "Confirm assistance at gate",
        })
        self.assertEqual(updated.status_code, 200)
        checkin = updated.get_json()["passenger"]
        self.assertEqual(checkin["Status"], "Checked In")
        self.assertEqual(checkin["Seat"], "2A")
        self.assertEqual(checkin["BaggageWeight"], 18.2)
        self.assertEqual(checkin["BaggagePieces"], 2)
        self.assertFalse(checkin["Boarded"])
        self.assertIsNotNone(checkin["CheckedInAt"])
        self.assertEqual(checkin["Ssrs"][0]["Code"], "WCHR")
        self.assertEqual(checkin["Comments"], "Confirm assistance at gate")

        identity_update = self.client.patch("/api/dcs/charter_manifest/passenger", json={
            "flight_id": "charter-123",
            "passenger_id": passenger["PassengerId"],
            "NamePrefix": "Ms",
            "GivenName": "Janet",
            "Surname": "Updated",
            "PassengerType": "T",
            "PassengerWeight": 50,
        })
        self.assertEqual(identity_update.status_code, 200)
        identity = identity_update.get_json()["passenger"]
        self.assertEqual(identity["NamePrefix"], "Ms")
        self.assertEqual(identity["GivenName"], "Janet")
        self.assertEqual(identity["Surname"], "Updated")
        self.assertEqual(identity["PassengerType"], "T")
        self.assertEqual(identity["PassengerWeight"], 96.0)

        duplicate_seat = self.client.patch("/api/dcs/charter_manifest/passenger", json={
            "flight_id": "charter-123",
            "passenger_id": second_passenger["PassengerId"],
            "Seat": "2A",
        })
        self.assertEqual(duplicate_seat.status_code, 409)

        qr_response = self.client.get("/api/dcs/charter_manifest/boarding-qr", query_string={
            "flight_id": "charter-123",
            "passenger_id": passenger["PassengerId"],
        })
        self.assertEqual(qr_response.status_code, 200)
        self.assertEqual(qr_response.mimetype, "image/png")

        changed_seat_scan = self.client.post("/api/dcs/charter_manifest/board-scan", json={
            "code": f"ACCI|charter-123|{passenger['PassengerId']}|1A",
        })
        self.assertEqual(changed_seat_scan.status_code, 409)
        self.assertTrue(changed_seat_scan.get_json()["seat_changed"])
        self.assertEqual(changed_seat_scan.get_json()["current_seat"], "2A")

        scanned = self.client.post("/api/dcs/charter_manifest/board-scan", json={
            "code": f"ACCI|charter-123|{passenger['PassengerId']}|1A",
            "acknowledge_seat_change": True,
        })
        self.assertEqual(scanned.status_code, 200)
        self.assertTrue(scanned.get_json()["passenger"]["Boarded"])

        duplicate_scan = self.client.post("/api/dcs/charter_manifest/board-scan", json={
            "code": f"ACCI|charter-123|{passenger['PassengerId']}",
        })
        self.assertEqual(duplicate_scan.status_code, 409)
        self.assertIn("already boarded", duplicate_scan.get_json()["error"])

        late_passenger = self.client.post("/api/dcs/charter_manifest/passenger", json={
            "flight_id": "charter-123",
            "GivenName": "Late",
            "Surname": "Addition",
            "PassengerType": "AD",
            "Status": "Checked In",
            "Seat": "2B",
            "BaggageWeight": 9.0,
        })
        self.assertEqual(late_passenger.status_code, 201)
        self.assertEqual(late_passenger.get_json()["passenger"]["Status"], "Checked In")

        boarded = self.client.patch("/api/dcs/charter_manifest/passenger", json={
            "flight_id": "charter-123",
            "passenger_id": passenger["PassengerId"],
            "Status": "Boarded",
        })
        self.assertEqual(boarded.status_code, 200)
        self.assertTrue(boarded.get_json()["passenger"]["Boarded"])

        unboarded = self.client.patch("/api/dcs/charter_manifest/passenger", json={
            "flight_id": "charter-123",
            "passenger_id": passenger["PassengerId"],
            "Status": "Checked In",
        })
        self.assertEqual(unboarded.status_code, 200)
        self.assertEqual(unboarded.get_json()["passenger"]["Status"], "Checked In")
        self.assertFalse(unboarded.get_json()["passenger"]["Boarded"])
        self.assertIsNone(unboarded.get_json()["passenger"]["BoardedAt"])

    def test_charter_checkin_page_loads(self):
        response = self.client.get("/dcs/charter-checkin?date=2026-09-11")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Charter Check-in", response.data)
        logo = self.client.get("/dcs/charter-brand/main-logo")
        self.assertEqual(logo.status_code, 200)
        self.assertEqual(logo.mimetype, "image/png")

    def test_uploaded_charter_manifest_is_not_treated_as_a_dcs_link(self):
        db.session.add(CharterManifest(
            envision_flight_id="charter-456",
            flight_no="3C456",
            dep="AKL",
            ades="WAG",
            pax_json="[]",
        ))
        db.session.commit()
        row = {
            "envision_flight_id": "charter-456",
            "service_type": "Charter",
            "dcs_linked": False,
        }
        _apply_charter_manifests([row])
        self.assertTrue(row["charter_manifest_uploaded"])
        self.assertTrue(row["charter_manifest_linked"])
        self.assertFalse(row["dcs_linked"])

    @patch("app.routes._require_openpyxl", side_effect=RuntimeError("openpyxl is required for Excel manifest import"))
    def test_manifest_upload_returns_json_when_excel_import_is_unavailable(self, _require_openpyxl):
        response = self.client.post("/api/dcs/charter_manifest/upload", data={
            "flight_id": "charter-789",
            "file": (BytesIO(b"not an excel file"), "manifest.xlsx"),
        })
        self.assertEqual(response.status_code, 400)
        self.assertTrue(response.is_json)
        self.assertIn("openpyxl is required", response.get_json()["error"])

    @patch("app.routes._send_charter_flight_closure_email")
    def test_closing_flight_emails_manifest_and_reopening_allows_changes(self, send_closure_email):
        saved = self.client.put("/api/dcs/charter_manifest", json={
            "flight_id": "charter-close-1",
            "flight_number": "3C724",
            "dep": "AKL",
            "ades": "WAG",
            "passengers": [{"GivenName": "Jane", "Surname": "Passenger", "PassengerType": "AD"}],
        })
        self.assertEqual(saved.status_code, 200)
        passenger_id = saved.get_json()["passengers"][0]["PassengerId"]

        with patch.dict("app.routes.os.environ", {
            "FLIGHT_OPERATIONS_EMAIL": "flightops@example.test",
            "SMTP_HOST": "smtp.example.test",
            "SMTP_FROM": "checkin@example.test",
        }):
            closed = self.client.post("/api/dcs/charter_manifest/flight-close", json={"flight_id": "charter-close-1"})
        self.assertEqual(closed.status_code, 200)
        self.assertTrue(closed.get_json()["closed_at"])
        send_closure_email.assert_called_once()

        locked = self.client.patch("/api/dcs/charter_manifest/passenger", json={
            "flight_id": "charter-close-1", "passenger_id": passenger_id, "Status": "Checked In",
        })
        self.assertEqual(locked.status_code, 409)

        reopened = self.client.post("/api/dcs/charter_manifest/flight-reopen", json={"flight_id": "charter-close-1"})
        self.assertEqual(reopened.status_code, 200)
        self.assertIsNone(reopened.get_json()["closed_at"])

        updated = self.client.patch("/api/dcs/charter_manifest/passenger", json={
            "flight_id": "charter-close-1", "passenger_id": passenger_id, "Status": "Checked In",
        })
        self.assertEqual(updated.status_code, 200)

    def test_closing_flight_is_allowed_before_email_is_configured(self):
        self.client.put("/api/dcs/charter_manifest", json={
            "flight_id": "charter-close-no-email",
            "passengers": [{"GivenName": "No", "Surname": "Email"}],
        })
        closed = self.client.post("/api/dcs/charter_manifest/flight-close", json={"flight_id": "charter-close-no-email"})
        self.assertEqual(closed.status_code, 200)
        self.assertFalse(closed.get_json()["email_sent"])
        self.assertTrue(closed.get_json()["closed_at"])

    def test_agent_can_assign_a_gate_to_an_open_charter_flight(self):
        self.client.put("/api/dcs/charter_manifest", json={
            "flight_id": "charter-gate-1",
            "passengers": [{"GivenName": "Gate", "Surname": "Test"}],
        })
        saved = self.client.patch("/api/dcs/charter_manifest/flight-gate", json={
            "flight_id": "charter-gate-1", "gate": " 4a ",
        })
        self.assertEqual(saved.status_code, 200)
        self.assertEqual(saved.get_json()["gate"], "4A")
        manifest = self.client.get("/api/dcs/charter_manifest", query_string={"flight_id": "charter-gate-1"})
        self.assertEqual(manifest.get_json()["gate"], "4A")
