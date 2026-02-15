import unittest

from scripts.load_epstein_sqlite import infer_locator_type, normalize_claim_status


class LoadSQLiteUtilityTests(unittest.TestCase):
    def test_infer_locator_type(self) -> None:
        self.assertEqual(infer_locator_type("p. 12"), "page")
        self.assertEqual(infer_locator_type("line 44"), "line")
        self.assertEqual(infer_locator_type("Section 8"), "section")
        self.assertEqual(infer_locator_type("00:03:15"), "timestamp")
        self.assertEqual(infer_locator_type("custom"), "unknown")

    def test_normalize_claim_status_maps_legacy_verified_to_primary(self) -> None:
        status = normalize_claim_status(
            "verified",
            [
                {
                    "evidence_type": "release",
                    "evidence_strength": "direct",
                }
            ],
        )
        self.assertEqual(status, "verified_primary")

    def test_normalize_claim_status_maps_legacy_verified_to_secondary(self) -> None:
        status = normalize_claim_status(
            "verified",
            [
                {
                    "evidence_type": "secondary",
                    "evidence_strength": "direct",
                }
            ],
        )
        self.assertEqual(status, "verified_secondary")

    def test_normalize_claim_status_maps_unverified_to_alleged(self) -> None:
        self.assertEqual(normalize_claim_status("unverified", []), "alleged")


if __name__ == "__main__":
    unittest.main()
