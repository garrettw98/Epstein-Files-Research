import unittest

from scripts.triage_claim_quality_flags import priority_from_counts, recommended_action_for_rules


class ClaimQualityTriageTests(unittest.TestCase):
    def test_priority_from_counts(self) -> None:
        self.assertEqual(priority_from_counts(high_count=1, warn_count=0), "p1")
        self.assertEqual(priority_from_counts(high_count=0, warn_count=2), "p2")
        self.assertEqual(priority_from_counts(high_count=0, warn_count=0), "p3")

    def test_recommended_action_for_rules(self) -> None:
        self.assertIn(
            "neutral mention-only framing",
            recommended_action_for_rules({"name_only_implication_risk"}),
        )
        self.assertIn(
            "DOJ/court/congressional primary source",
            recommended_action_for_rules({"no_primary_evidence"}),
        )
        self.assertIn(
            "direct locator and snippet-hash",
            recommended_action_for_rules({"no_direct_context"}),
        )


if __name__ == "__main__":
    unittest.main()
