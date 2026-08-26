import unittest

from app.routes import _longitudinal_envelope_limits


SAAB_ENVELOPE = [
    {"mass": 7257.48, "arm": 1064.01},
    {"mass": 8708.97, "arm": 1064.01},
    {"mass": 13154.18, "arm": 1086.87},
    {"mass": 13154.18, "arm": 1134.87},
    {"mass": 10432.62, "arm": 1134.87},
    {"mass": 9162.57, "arm": 1123.95},
    {"mass": 7257.48, "arm": 1107.69},
]


class TrimEnvelopeTests(unittest.TestCase):
    def test_interpolates_forward_and_aft_limits_at_loaded_mass(self):
        result = _longitudinal_envelope_limits(SAAB_ENVELOPE, 10860)
        self.assertTrue(result["available"])
        self.assertTrue(result["mass_in_range"])
        self.assertLess(result["forward_arm"], result["aft_arm"])
        self.assertAlmostEqual(result["aft_arm"], 1134.87, places=2)

    def test_mass_above_envelope_is_outside(self):
        result = _longitudinal_envelope_limits(SAAB_ENVELOPE, 13656)
        self.assertTrue(result["available"])
        self.assertFalse(result["mass_in_range"])
        self.assertEqual(result["max_mass"], 13154.18)


if __name__ == "__main__":
    unittest.main()
