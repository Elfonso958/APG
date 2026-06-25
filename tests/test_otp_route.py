import unittest
from unittest.mock import patch

from flask import Flask

from app.routes import api_bp


class FakeOtpClient:
    def fetch_otp_flights(self, date_from, date_to, page_size=500):
        self.args = (date_from, date_to, page_size)
        return [{"id": 1, "Route": "AKL-CHT"}]


class OtpRouteTests(unittest.TestCase):
    def create_client(self):
        app = Flask(__name__)
        app.config.update(
            ENVISION_BASE="https://envision.example/v1",
            ENVISION_USER="user",
            ENVISION_PASS="pass",
            TESTING=True,
        )
        app.register_blueprint(api_bp, url_prefix="/api")
        return app.test_client()

    def test_otp_flights_defaults_to_powerbi_date_range(self):
        client = self.create_client()
        fake_client = FakeOtpClient()

        with patch("app.routes.build_envision_otp_client", return_value=fake_client):
            resp = client.get("/api/envision/otp-flights")

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.get_json(), [{"id": 1, "Route": "AKL-CHT"}])
        self.assertEqual(fake_client.args, ("2022-01-01", "2035-12-31", 500))

    def test_otp_flights_returns_powerbi_array(self):
        client = self.create_client()
        fake_client = FakeOtpClient()

        with patch("app.routes.build_envision_otp_client", return_value=fake_client):
            resp = client.get("/api/envision/otp-flights?dateFrom=2026-06-01&dateTo=2026-06-02&pageSize=250")

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.get_json(), [{"id": 1, "Route": "AKL-CHT"}])
        self.assertEqual(fake_client.args, ("2026-06-01", "2026-06-02", 250))

    def test_otp_flights_live_alias_returns_powerbi_array(self):
        client = self.create_client()
        fake_client = FakeOtpClient()

        with patch("app.routes.build_envision_otp_client", return_value=fake_client):
            resp = client.get("/api/envision/otp-flights-live?dateFrom=2026-06-01&dateTo=2026-06-07")

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.get_json(), [{"id": 1, "Route": "AKL-CHT"}])
        self.assertEqual(fake_client.args, ("2026-06-01", "2026-06-07", 500))


if __name__ == "__main__":
    unittest.main()
