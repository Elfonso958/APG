import unittest
from unittest.mock import patch

from flask import Flask

from app import db
from app.models import PowerBiApiKey
from app.routes import api_bp


class FakeOtpClient:
    def fetch_otp_flights(self, date_from, date_to, page_size=500, include_details=True, chunk_days=31):
        self.args = (date_from, date_to, page_size, include_details, chunk_days)
        return [{"id": 1, "Route": "AKL-CHT", "departureScheduled": "2026-06-01T10:00:00Z"}]


class OtpRouteTests(unittest.TestCase):
    def create_client(self):
        app = Flask(__name__)
        app.config.update(
            ENVISION_BASE="https://envision.example/v1",
            ENVISION_USER="user",
            ENVISION_PASS="pass",
            SQLALCHEMY_DATABASE_URI="sqlite:///:memory:",
            SQLALCHEMY_TRACK_MODIFICATIONS=False,
            TESTING=True,
            SECRET_KEY="test",
        )
        db.init_app(app)
        app.register_blueprint(api_bp, url_prefix="/api")
        with app.app_context():
            db.create_all()
            db.session.add(PowerBiApiKey(
                name="Test key",
                key_prefix="test-powerbi",
                key_hash=PowerBiApiKey.hash_secret("test-powerbi-key"),
            ))
            db.session.commit()
        return app.test_client()

    def test_otp_flights_defaults_to_powerbi_date_range(self):
        client = self.create_client()
        fake_client = FakeOtpClient()

        with patch("app.routes.build_envision_otp_client", return_value=fake_client):
            resp = client.get("/api/envision/otp-flights")

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.get_json(), [{"id": 1, "Route": "AKL-CHT", "departureScheduled": "2026-06-01T10:00:00Z"}])
        self.assertEqual(fake_client.args, ("2022-01-01", "2035-12-31", 500, True, 31))

    def test_otp_flights_returns_powerbi_array(self):
        client = self.create_client()
        fake_client = FakeOtpClient()

        with patch("app.routes.build_envision_otp_client", return_value=fake_client):
            resp = client.get("/api/envision/otp-flights?dateFrom=2026-06-01&dateTo=2026-06-02&pageSize=250")

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.get_json(), [{"id": 1, "Route": "AKL-CHT", "departureScheduled": "2026-06-01T10:00:00Z"}])
        self.assertEqual(fake_client.args, ("2026-06-01", "2026-06-02", 250, True, 31))

    def test_otp_flights_live_alias_returns_powerbi_array(self):
        client = self.create_client()
        fake_client = FakeOtpClient()

        with patch("app.routes.build_envision_otp_client", return_value=fake_client):
            resp = client.get("/api/envision/otp-flights-live?dateFrom=2026-06-01&dateTo=2026-06-07")

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.get_json(), [{"id": 1, "Route": "AKL-CHT", "departureScheduled": "2026-06-01T10:00:00Z"}])
        self.assertEqual(fake_client.args, ("2026-06-01", "2026-06-07", 500, False, 1))

    def test_otp_flights_live_can_include_details_when_requested(self):
        client = self.create_client()
        fake_client = FakeOtpClient()

        with patch("app.routes.build_envision_otp_client", return_value=fake_client):
            resp = client.get("/api/envision/otp-flights-live?dateFrom=2026-06-01&dateTo=2026-06-07&includeDetails=1")

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(fake_client.args, ("2026-06-01", "2026-06-07", 500, True, 1))

    def test_otp_flights_live_chunk_days_can_be_overridden(self):
        client = self.create_client()
        fake_client = FakeOtpClient()

        with patch("app.routes.build_envision_otp_client", return_value=fake_client):
            resp = client.get("/api/envision/otp-flights-live?dateFrom=2026-06-01&dateTo=2026-06-07&chunkDays=2")

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(fake_client.args, ("2026-06-01", "2026-06-07", 500, False, 2))

    def test_otp_cache_refresh_and_cached_read(self):
        client = self.create_client()
        fake_client = FakeOtpClient()

        with patch("app.routes.build_envision_otp_client", return_value=fake_client):
            refresh = client.post("/api/envision/otp-flights-cache/refresh?dateFrom=2026-06-01&dateTo=2026-06-01")

        self.assertEqual(refresh.status_code, 200)
        self.assertEqual(refresh.get_json()["created"], 1)

        cached = client.get(
            "/api/envision/otp-flights-cached?dateFrom=2026-06-01&dateTo=2026-06-01",
            headers={"X-API-Key": "test-powerbi-key"},
        )
        self.assertEqual(cached.status_code, 200)
        self.assertEqual(
            cached.get_json(),
            [{"id": 1, "Route": "AKL-CHT", "departureScheduled": "2026-06-01T10:00:00Z"}],
        )

    def test_otp_cache_requires_powerbi_api_key(self):
        client = self.create_client()

        missing = client.get("/api/envision/otp-flights-cached")
        self.assertEqual(missing.status_code, 401)

        invalid = client.get(
            "/api/envision/otp-flights-cached",
            headers={"X-API-Key": "wrong-key"},
        )
        self.assertEqual(invalid.status_code, 401)

        valid = client.get(
            "/api/envision/otp-flights-cached",
            headers={"X-API-Key": "test-powerbi-key"},
        )
        self.assertEqual(valid.status_code, 200)

    def test_defects_powerbi_requires_powerbi_api_key(self):
        client = self.create_client()

        missing = client.get("/api/envision/defects_powerbi")
        self.assertEqual(missing.status_code, 401)

        invalid = client.get(
            "/api/envision/defects_powerbi",
            headers={"X-API-Key": "wrong-key"},
        )
        self.assertEqual(invalid.status_code, 401)


if __name__ == "__main__":
    unittest.main()
