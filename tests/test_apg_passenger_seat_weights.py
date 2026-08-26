import unittest

from app.sync.envision_apg_sync import (
    apply_dcs_passengers_to_apg_rows,
    calculate_dcs_passenger_seat_loads,
)


def _station(seat):
    return {"label": f"Passenger {seat}", "customLoad": {"mass": 0, "pob_count": 0}}


class ApgPassengerSeatWeightTests(unittest.TestCase):
    def test_lap_infant_weight_is_added_to_ssr_parent_seat(self):
        loading = [_station("7C"), _station("10A")]
        flight = {"Passengers": [
            {
                "PassengerType": "AD", "Seat": "7C", "Status": "Flown",
                "BookingReferenceID": "009SBW",
                "Ssrs": [{"Code": "INFT", "FreeText": "ASHFORD/TAHLIA MISS"}],
            },
            {
                "PassengerType": "AD", "Seat": "10A", "Status": "Flown",
                "BookingReferenceID": "OTHER",
            },
            {
                "PassengerType": "INF", "Status": "Flown",
                "BookingReferenceID": "009SBW", "GivenName": "Tahlia", "Surname": "Ashford",
            },
        ]}

        apply_dcs_passengers_to_apg_rows(loading, flight, aircraft_reg="ZK-CIZ")

        masses = {row["label"]: row["customLoad"]["mass"] for row in loading}
        self.assertEqual(masses["Passenger 7C"], 101.0)
        self.assertEqual(masses["Passenger 10A"], 86.0)
        self.assertEqual(
            calculate_dcs_passenger_seat_loads(flight),
            {"10A": 86.0, "7C": 101.0},
        )

    def test_umnr_and_child_ssrs_override_incorrect_adult_type(self):
        loading = [_station("10A"), _station("2B")]
        flight = {"Passengers": [
            {
                "PassengerType": "AD", "Seat": "10A", "Status": "Flown",
                "Ssrs": [{"Code": "UMNR", "FreeText": "UMNR"}],
            },
            {
                "PassengerType": "AD", "Seat": "2B", "Status": "Flown",
                "Ssrs": [{"Code": "CHLD", "FreeText": "19NOV22"}],
            },
        ]}

        apply_dcs_passengers_to_apg_rows(loading, flight, aircraft_reg="ZK-CIZ")

        masses = {row["label"]: row["customLoad"]["mass"] for row in loading}
        self.assertEqual(masses["Passenger 10A"], 46.0)
        self.assertEqual(masses["Passenger 2B"], 46.0)


if __name__ == "__main__":
    unittest.main()
