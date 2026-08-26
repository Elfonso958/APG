import json
import unittest
from unittest.mock import patch

from app import create_app, db
from app.config import Config
from app.models import AppConfig, FlightFreightAllocation, FlightCargoAllocation
from app.routes import _seat_bag_front_conflicts, _calculate_apg_loaded_trim
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
        response = self.client.put("/api/dcs/freight/123", json={"seats": ["2B", "2A"], "freight_kg": 100, "aircraft_type": "ATR72"})
        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        self.assertEqual(data["allocations"][0]["seats"], ["2A", "2B"])
        self.assertEqual(data["total_kg"], 107.0)
        self.assertEqual(data["allocations"][0]["per_seat_kg"], 53.5)

        saved = FlightFreightAllocation.query.filter_by(envision_flight_id="123").one()
        self.assertEqual(json.loads(saved.seats_json)[0]["seats"], ["2A", "2B"])

    def test_setting_updates_saved_allocations(self):
        self.client.put("/api/dcs/freight/123", json={"seats": ["1A", "1B"], "freight_kg": 100, "aircraft_type": "ATR72"})
        response = self.client.put("/api/dcs/freight-settings", json={"seat_bag_tare_kg": 8})
        self.assertEqual(response.status_code, 200)
        data = self.client.get("/api/dcs/freight/123").get_json()
        self.assertEqual(data["total_kg"], 108.0)
        self.assertEqual(data["allocations"][0]["per_seat_kg"], 54.0)

    def test_multiple_pairs_each_receive_tare(self):
        response = self.client.put("/api/dcs/freight/123", json={"aircraft_type": "ATR72", "allocations": [
            {"seats": ["9A", "9B"], "freight_kg": 50},
            {"seats": ["10C", "10D"], "freight_kg": 100, "override": True},
        ]})
        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        self.assertEqual(len(data["allocations"]), 2)
        self.assertEqual(data["total_kg"], 164.0)
        self.assertEqual(data["allocations"][0]["per_seat_kg"], 28.5)
        self.assertEqual(data["allocations"][1]["per_seat_kg"], 53.5)

    def test_cargo_weights_persist_and_reject_stale_update(self):
        first = self.client.put("/api/dcs/cargo-allocation/123", json={
            "expected_revision": 0,
            "allocations": [{"label": "Cargo B1", "baggage_kg": 0, "freight_kg": 120}],
            "atr_rows": [],
        })
        self.assertEqual(first.status_code, 200)
        self.assertEqual(first.get_json()["revision"], 1)
        saved = self.client.get("/api/dcs/cargo-allocation/123").get_json()
        self.assertEqual(saved["allocations"][0]["freight_kg"], 120.0)
        self.assertEqual(FlightCargoAllocation.query.filter_by(envision_flight_id="123").count(), 1)

        stale = self.client.put("/api/dcs/cargo-allocation/123", json={
            "expected_revision": 0,
            "allocations": [{"label": "Cargo B1", "baggage_kg": 0, "freight_kg": 90}],
            "atr_rows": [],
        })
        self.assertEqual(stale.status_code, 409)
        self.assertTrue(stale.get_json()["stale"])

    def test_seat_bag_revision_rejects_stale_update(self):
        first = self.client.put("/api/dcs/freight/123", json={"seats": ["2A", "2B"], "freight_kg": 10, "aircraft_type": "ATR72", "expected_revision": 0})
        self.assertEqual(first.status_code, 200)
        stale = self.client.put("/api/dcs/freight/123", json={"seats": ["2A", "2B"], "freight_kg": 20, "aircraft_type": "ATR72", "expected_revision": 0})
        self.assertEqual(stale.status_code, 409)
        self.assertTrue(stale.get_json()["stale"])

    def test_rejects_seats_from_different_rows(self):
        response = self.client.put("/api/dcs/freight/123", json={"seats": ["2A", "10A"], "freight_kg": 100, "aircraft_type": "ATR72"})
        self.assertEqual(response.status_code, 400)

    def test_rejects_atr_seats_across_the_aisle(self):
        response = self.client.put("/api/dcs/freight/123", json={"seats": ["2B", "2C"], "freight_kg": 100, "aircraft_type": "ATR72"})
        self.assertEqual(response.status_code, 400)

    def test_front_conflict_only_checks_same_columns_immediate_row(self):
        occupied = {"9C", "9D", "9A", "8C"}
        self.assertEqual(_seat_bag_front_conflicts(["10C", "10D"], occupied), ["9C", "9D"])

    def test_loaded_trim_uses_apg_station_arms(self):
        plan = {"massAndBalance": {"loading": [
            {"label": "BEW", "customLoad": {"mass": 1000}},
            {"label": "Cargo 1", "customLoad": {"mass": 100}},
        ]}}
        aircraft_mb = {
            "bem": {"label": "BEW", "arm": {"lon": 100}},
            "mac": {"lemac": 100, "mac": 100},
            "stations": [
                {"label": "Forward", "arm": {"lon": 50}},
                {"label": "Cargo 1", "arm": {"lon": 200}},
            ],
        }
        trim = _calculate_apg_loaded_trim(plan, aircraft_mb)
        self.assertTrue(trim["available"])
        self.assertAlmostEqual(trim["cg_arm"], 109.0909, places=3)
        self.assertAlmostEqual(trim["percent_mac"], 9.0909, places=3)
        self.assertAlmostEqual(trim["position_percent"], 39.3939, places=3)

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
