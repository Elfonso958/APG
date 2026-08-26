import json
import unittest
from unittest.mock import patch

from app import create_app, db
from app.config import Config
from app.models import AppConfig, FlightFreightAllocation
from app.sync.envision_apg_sync import update_apg_plan_from_dcs_row


class FreightAllocationTest(unittest.TestCase):
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

    def test_two_seats_share_one_tare_weight(self):
        response = self.client.put("/api/dcs/freight/123", json={"seats": ["2B", "2A"], "freight_kg": 100})
        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        self.assertEqual(data["seats"], ["2A", "2B"])
        self.assertEqual(data["total_kg"], 107.0)
        self.assertEqual(data["per_seat_kg"], 53.5)

        saved = FlightFreightAllocation.query.filter_by(envision_flight_id="123").one()
        self.assertEqual(json.loads(saved.seats_json), ["2A", "2B"])

    def test_setting_updates_saved_allocations(self):
        self.client.put("/api/dcs/freight/123", json={"seats": ["1A", "1B"], "freight_kg": 100})
        response = self.client.put("/api/dcs/freight-settings", json={"seat_bag_tare_kg": 8})
        self.assertEqual(response.status_code, 200)
        data = self.client.get("/api/dcs/freight/123").get_json()
        self.assertEqual(data["total_kg"], 108.0)
        self.assertEqual(data["per_seat_kg"], 54.0)

    @patch("app.sync.envision_apg_sync.apg_plan_get")
    def test_apg_individual_seats_receive_split_weight(self, plan_get):
        plan_get.return_value = {"massAndBalance": {"loading": [
            {"label": "Passenger 2A", "customLoad": {"mass": 0, "pob_count": 0}},
            {"label": "Passenger 2B", "customLoad": {"mass": 0, "pob_count": 0}},
        ], "fuelMass": 50}}
        result = update_apg_plan_from_dcs_row(
            "token", 99, {"Passengers": [], "reg": "ZK-CIT"},
            seat_freight_loads=[{"seat": "2A", "mass_kg": 53.5}, {"seat": "2B", "mass_kg": 53.5}],
            preview_only=True,
        )
        loading = result["payload"]["massAndBalance"]["loading"]
        self.assertEqual([row["customLoad"]["mass"] for row in loading], [53.5, 53.5])

    @patch("app.sync.envision_apg_sync.apg_plan_get")
    def test_apg_atr_row_receives_both_seats(self, plan_get):
        plan_get.return_value = {"massAndBalance": {"loading": [
            {"label": "Row 2", "customLoad": {"mass": 0, "pob_count": 0}},
        ], "fuelMass": 50}}
        result = update_apg_plan_from_dcs_row(
            "token", 99, {"Passengers": [], "reg": "ZK-MCU"},
            seat_freight_loads=[{"seat": "2A", "mass_kg": 53.5}, {"seat": "2B", "mass_kg": 53.5}],
            preview_only=True,
        )
        loading = result["payload"]["massAndBalance"]["loading"]
        self.assertEqual(loading[0]["customLoad"]["mass"], 107.0)


if __name__ == "__main__":
    unittest.main()
