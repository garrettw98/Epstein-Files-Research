import datetime as dt
import pathlib
import unittest

from scripts.generate_research_command_center import (
    aggregate_entity_mentions,
    bar_width,
    parse_compact_utc,
    relative_href,
    summarize_quality_flags,
    summarize_review_queue,
)


class ResearchCommandCenterTests(unittest.TestCase):
    def test_parse_compact_utc(self) -> None:
        parsed = parse_compact_utc("20260219T012215Z")
        self.assertIsNotNone(parsed)
        assert parsed is not None
        self.assertEqual(parsed.tzinfo, dt.timezone.utc)
        self.assertEqual(parsed.year, 2026)
        self.assertEqual(parsed.month, 2)
        self.assertEqual(parsed.day, 19)

    def test_summarize_review_queue_open_states(self) -> None:
        rows = [
            {"priority": "p1", "triage_status": "open"},
            {"priority": "p2", "triage_status": "in_review"},
            {"priority": "p3", "triage_status": ""},
            {"priority": "p1", "triage_status": "closed"},
            {"priority": "p2", "triage_status": "resolved"},
        ]
        counts = summarize_review_queue(rows)
        self.assertEqual(counts["p1"], 1)
        self.assertEqual(counts["p2"], 1)
        self.assertEqual(counts["p3"], 1)

    def test_summarize_quality_flags_open_states(self) -> None:
        rows = [
            {"severity": "high", "flag_status": "open"},
            {"severity": "warn", "flag_status": "in_review"},
            {"severity": "info", "flag_status": ""},
            {"severity": "high", "flag_status": "resolved"},
        ]
        counts = summarize_quality_flags(rows)
        self.assertEqual(counts["high"], 1)
        self.assertEqual(counts["warn"], 1)
        self.assertEqual(counts["info"], 1)

    def test_aggregate_entity_mentions(self) -> None:
        rows = [
            {"canonical_name": "Jeffrey Epstein", "mention_count": "4"},
            {"canonical_name": "Jeffrey Epstein", "mention_count": "2"},
            {"canonical_name": "Ghislaine Maxwell", "mention_count": "3"},
            {"canonical_name": "Pam Bondi", "mention_count": "1"},
        ]
        top = aggregate_entity_mentions(rows, limit=2)
        self.assertEqual(top[0][0], "Jeffrey Epstein")
        self.assertEqual(top[0][1], 6.0)
        self.assertEqual(top[1][0], "Ghislaine Maxwell")
        self.assertEqual(top[1][1], 3.0)

    def test_relative_href(self) -> None:
        root = pathlib.Path("/tmp/repo").resolve()
        out_dir = (root / "derived" / "reports").resolve()
        self.assertEqual(relative_href(root, out_dir, "README.md"), "../../README.md")
        self.assertEqual(relative_href(root, out_dir, "timeline/Full_Timeline.md"), "../../timeline/Full_Timeline.md")

    def test_bar_width(self) -> None:
        self.assertEqual(bar_width(0, 0), 0.0)
        self.assertEqual(bar_width(0, 9), 0.0)
        self.assertAlmostEqual(bar_width(1, 4), 25.0)
        self.assertAlmostEqual(bar_width(1, 100), 6.0)


if __name__ == "__main__":
    unittest.main()
