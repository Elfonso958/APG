import unittest

from app.sync.envision_apg_sync import (
    apply_dcs_passengers_to_apg_rows,
    calculate_dcs_passenger_seat_loads,
)


def _station(seat):
    return {"label": f"Passenger {seat}", "customLoad": {"mass": 0, "pob_count": 0}}


class ApgPassengerSeatWeightTests(unittest.TestCase):
    def test_3c841_includes_checked_and_issued_allocated_seats(self):
        loading = [
            _station(seat)
            for seat in ["3C", "4A", "7A", "7C", "8A", "8C", "9A", "10A", "11A", "11C", "11D"]
        ]
        flight = {"Passengers": [
            {"PassengerType": "AD", "Seat": "3C", "Status": "Checked"},
            {"PassengerType": "AD", "Seat": "4A", "Status": "Checked"},
            {"PassengerType": "AD", "Seat": "7A", "Status": "Issued"},
            {"PassengerType": "AD", "Seat": "7C", "Status": "Checked"},
            {
                "PassengerType": "UM", "Seat": "8A", "Status": "Issued",
                "Ssrs": [{"Code": "UMNR", "FreeText": "UMNR"}],
            },
            {"PassengerType": "AD", "Seat": "8C", "Status": "Checked"},
            {"PassengerType": "AD", "Seat": "9A", "Status": "Issued"},
            {"PassengerType": "AD", "Seat": "10A", "Status": "Issued"},
            {"PassengerType": "AD", "Seat": "11A", "Status": "Checked"},
            {"PassengerType": "AD", "Seat": "11C", "Status": "Checked"},
            {"PassengerType": "AD", "Seat": "11D", "Status": "Checked"},
            {"PassengerType": "AD", "Status": "Issued"},
        ]}

        apply_dcs_passengers_to_apg_rows(loading, flight, aircraft_reg="ZK-CIZ")

        masses = {row["label"].split(" ", 1)[1]: row["customLoad"]["mass"] for row in loading}
        self.assertEqual(len([mass for mass in masses.values() if mass > 0]), 11)
        self.assertEqual(masses["7A"], 86.0)
        self.assertEqual(masses["8A"], 46.0)
        self.assertEqual(masses["9A"], 86.0)
        self.assertEqual(masses["10A"], 86.0)
        self.assertEqual(sum(masses.values()), 906.0)

    def test_3c838_full_expected_seat_configuration(self):
        issued_seats = [
            "8A", "3B", "5A", "4C", "5B", "5C", "7A", "10A", "4A", "7C",
            "11D", "8B", "8C", "9B", "10C", "9C", "2B", "9A", "7B", "2C",
        ]
        checked_seats = ["3C", "10B", "3A", "1C", "2A"]
        all_seats = issued_seats + checked_seats
        loading = [_station(seat) for seat in all_seats]
        passengers = [
            {"PassengerType": "AD", "Seat": seat, "Status": "Issued"}
            for seat in issued_seats
        ] + [
            {"PassengerType": "AD", "Seat": seat, "Status": "Checked"}
            for seat in checked_seats
        ]
        waiting_list = next(p for p in passengers if p["Seat"] == "11D")
        waiting_list["Status"] = "Issued Waiting List"
        parent = next(p for p in passengers if p["Seat"] == "9A")
        parent.update({
            "BookingReferenceID": "009S6K",
            "Ssrs": [{"Code": "INFT", "FreeText": "MIRA/WEAVERSBROOKSINF 25NOV25"}],
        })
        passengers.append({
            "PassengerType": "INF", "Status": "Issued",
            "BookingReferenceID": "009S6K", "GivenName": "Weavers-Brooks", "Surname": "Mira",
        })

        apply_dcs_passengers_to_apg_rows(
            loading,
            {"Passengers": passengers},
            aircraft_reg="ZK-CIZ",
        )

        masses = {row["label"].split(" ", 1)[1]: row["customLoad"]["mass"] for row in loading}
        self.assertEqual(len([mass for mass in masses.values() if mass > 0]), 25)
        self.assertEqual(masses["9A"], 101.0)
        self.assertEqual(masses["11D"], 86.0)
        self.assertEqual(sum(masses.values()), 2165.0)

    def test_issued_seated_passengers_and_lap_infant_are_loaded(self):
        loading = [_station("9A"), _station("11D"), _station("3C")]
        flight = {"Passengers": [
            {
                "PassengerType": "AD", "Seat": "9A", "Status": "Issued",
                "BookingReferenceID": "FAMILY",
                "Ssrs": [{"Code": "INFT", "FreeText": "MIRA WEAVERSBROOKS INF"}],
            },
            {
                "PassengerType": "AD", "Seat": "11D", "Status": "Issued Waiting List",
            },
            {
                "PassengerType": "AD", "Seat": "3C", "Status": "Checked",
            },
            {
                "PassengerType": "INF", "Status": "Issued",
                "BookingReferenceID": "FAMILY", "GivenName": "Mira", "Surname": "Weavers-Brooks",
            },
        ]}

        apply_dcs_passengers_to_apg_rows(loading, flight, aircraft_reg="ZK-CIZ")

        masses = {row["label"]: row["customLoad"]["mass"] for row in loading}
        self.assertEqual(masses["Passenger 9A"], 101.0)
        self.assertEqual(masses["Passenger 11D"], 86.0)
        self.assertEqual(masses["Passenger 3C"], 86.0)

    def test_booked_no_show_and_cancelled_passengers_are_not_loaded(self):
        loading = [_station("1A"), _station("1B"), _station("1C")]
        flight = {"Passengers": [
            {"PassengerType": "AD", "Seat": "1A", "Status": "Booked"},
            {"PassengerType": "AD", "Seat": "1B", "Status": "No Show"},
            {"PassengerType": "AD", "Seat": "1C", "Status": "Cancelled"},
        ]}

        apply_dcs_passengers_to_apg_rows(loading, flight, aircraft_reg="ZK-CIZ")

        self.assertTrue(all(row["customLoad"]["mass"] == 0.0 for row in loading))

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
