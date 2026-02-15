import unittest

from scripts.generate_coverage_gap_dashboard import (
    extract_expected_dataset_max,
    extract_ingested_dataset_numbers,
)


class CoverageGapDashboardTests(unittest.TestCase):
    def test_extract_expected_dataset_max(self) -> None:
        readme = "DOJ library status lists releases through Data Set 12 and Data Set 9."
        self.assertEqual(extract_expected_dataset_max(readme), 12)

    def test_extract_ingested_dataset_numbers(self) -> None:
        rows = [
            {"url": "https://www.justice.gov/epstein/data-set-9", "path": "", "snapshot_file": ""},
            {"url": "https://www.justice.gov/epstein/dataset12", "path": "", "snapshot_file": ""},
            {"url": "https://example.com/no-dataset", "path": "/set_4", "snapshot_file": ""},
        ]
        self.assertEqual(extract_ingested_dataset_numbers(rows), {4, 9, 12})


if __name__ == "__main__":
    unittest.main()
