import unittest

from scripts.derive_entity_mentions import classify_link_context, extract_name_phrases


class DeriveEntityMentionTests(unittest.TestCase):
    def test_extract_name_phrases(self) -> None:
        text = "Howard Lutnick met Jeffrey Epstein while Les Wexner was referenced."
        phrases = extract_name_phrases(text)
        self.assertIn("Howard Lutnick", phrases)
        self.assertIn("Jeffrey Epstein", phrases)
        self.assertIn("Les Wexner", phrases)

    def test_classify_link_context_email(self) -> None:
        ctx = classify_link_context(
            claim_type="factual",
            evidence_type="secondary",
            text="Email exchange shows sender and reply.",
            url="https://apnews.com/article/example",
        )
        self.assertEqual(ctx, "email_body")

    def test_classify_link_context_allegation(self) -> None:
        ctx = classify_link_context(
            claim_type="allegation",
            evidence_type="secondary",
            text="A witness alleged misconduct.",
            url="https://example.com",
        )
        self.assertEqual(ctx, "allegation")


if __name__ == "__main__":
    unittest.main()
