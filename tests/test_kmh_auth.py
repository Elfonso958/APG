import unittest
from contextlib import ExitStack
from unittest.mock import patch

from app import create_app, db
from app.config import Config


class KmhAuthTest(unittest.TestCase):
    def setUp(self):
        Config.SQLALCHEMY_DATABASE_URI = "sqlite:///:memory:"
        self.app = create_app()
        self.app.config.update(TESTING=True)
        self.context = self.app.app_context()
        self.context.push()
        db.create_all()
        self.client = self.app.test_client()

    def tearDown(self):
        db.session.remove()
        db.drop_all()
        self.context.pop()

    def test_kmh_lookup_uses_kmh_session_without_apg_login(self):
        with patch("app.routes._kmh_session_token", return_value="token"), \
             patch("app.routes._kmh_pilot_options", return_value=[]), \
             patch("app.routes._resolve_kmh_registration", return_value={"id": 1, "modelId": 2}), \
             patch("app.routes.build_pic_pilot_position_sets", return_value=({71}, {71})), \
             patch("app.routes.envision_get_crew_position_setups", return_value=[]), \
             patch("app.routes.envision_get_crew_position_setup_items", return_value=[]), \
             patch("app.routes.envision_get_flight_types", return_value=[]), \
             patch("app.routes.envision_get_cancel_codes", return_value=[]):
            response = self.client.get("/api/kmh/lookups")
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json["ok"])

    def test_kmh_endpoints_still_require_kmh_session(self):
        for path in ("/api/kmh/lookups", "/api/kmh/flights", "/api/kmh/export"):
            response = self.client.get(path)
            self.assertEqual(response.status_code, 401, path)
            self.assertTrue(response.json["session_expired"])

    def test_other_operations_api_still_requires_apg_login(self):
        response = self.client.get("/api/envision/flight_types")
        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.json["error"], "Sign in is required.")

    def test_place_mismatch_requires_confirmation_before_retry(self):
        payload = {"flight_date": "2026-12-02", "flight_number": "3C1", "dep": "CHT",
                   "arr": "PIT", "etd": "15:30", "eta": "15:50",
                   "flight_type_id": 15, "pilot_employee_id": 1}
        error = RuntimeError('406 | body={"typeDescription":"INFO","messages":["Place mismatch with previous flight - 3C2/AKL"]} | payload={"ignoreValidations":false}')
        calls = []
        def create(_token, request):
            calls.append(dict(request))
            if not request["ignoreValidations"]:
                raise error
            return {"id": 42}

        with ExitStack() as stack:
            for name, value in {
                "_kmh_session_token": "token",
                "_kmh_pilot_options": [{"employee_id": 1, "name": "Pilot"}],
                "_resolve_kmh_registration": {"id": 1, "modelId": 2},
                "_kmh_pic_position_for_type": (71, {}),
                "_resolve_place_id": (1, "Place"),
                "_assign_kmh_pilot_to_existing_or_new_slot": 5,
            }.items():
                stack.enter_context(patch(f"app.routes.{name}", return_value=value))
            stack.enter_context(patch("app.routes.envision_create_flight", side_effect=create))
            response = self.client.post("/api/kmh/flights", json=payload)
            self.assertEqual(response.status_code, 409)
            self.assertTrue(response.json["confirmation_required"])
            self.assertEqual([call["ignoreValidations"] for call in calls], [False])
            response = self.client.post("/api/kmh/flights", json={**payload, "confirm_place_mismatch": True})
            self.assertEqual(response.status_code, 200)
            self.assertEqual([call["ignoreValidations"] for call in calls], [False, False, True])
