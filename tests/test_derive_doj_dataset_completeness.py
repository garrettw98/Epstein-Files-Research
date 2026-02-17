import unittest

from scripts.derive_doj_dataset_completeness import (
    canonicalize_dataset_page_url,
    extract_dataset_number_from_file_url,
    extract_dataset_number_from_url,
    is_dataset_file_url,
)


class DatasetCompletenessTests(unittest.TestCase):
    def test_extract_dataset_number_from_url(self) -> None:
        self.assertEqual(
            extract_dataset_number_from_url("https://www.justice.gov/epstein/doj-disclosures/data-set-12-files"),
            12,
        )
        self.assertIsNone(extract_dataset_number_from_url("https://www.justice.gov/epstein"))

    def test_extract_dataset_number_from_file_url(self) -> None:
        self.assertEqual(
            extract_dataset_number_from_file_url(
                "https://www.justice.gov/epstein/files/DataSet%2012/EFTA02731498.pdf"
            ),
            12,
        )
        self.assertIsNone(extract_dataset_number_from_file_url("https://www.justice.gov/epstein/doj-disclosures"))

    def test_is_dataset_file_url(self) -> None:
        self.assertTrue(
            is_dataset_file_url("https://www.justice.gov/epstein/files/DataSet%2011/EFTA12345678.pdf")
        )
        self.assertFalse(is_dataset_file_url("https://www.justice.gov/epstein/search"))

    def test_canonicalize_dataset_page_url(self) -> None:
        self.assertEqual(
            canonicalize_dataset_page_url(
                "https://www.justice.gov/epstein/doj-disclosures/data-set-12-files?page=2&utm_source=x#top"
            ),
            "https://www.justice.gov/epstein/doj-disclosures/data-set-12-files?page=2",
        )


if __name__ == "__main__":
    unittest.main()
