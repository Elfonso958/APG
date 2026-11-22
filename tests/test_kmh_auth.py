import unittest
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
