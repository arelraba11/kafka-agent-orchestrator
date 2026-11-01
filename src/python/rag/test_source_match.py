"""
Tests for source_match.py (stdlib unittest). Imports only source_match.py.

Run: bun run test:py
"""

import unittest
from pathlib import Path

import source_match
from source_match import prefer_product_match, select_top_source

# The embedding order that caused the bug: for "Galaxy S25 price" the Ultra ranked first.
ULTRA_FIRST = ["galaxy_s25_ultra", "galaxy_s25", "ps5_pro"]
S25_FIRST = ["galaxy_s25", "galaxy_s25_ultra", "ps5_pro"]


class SelectTopSource(unittest.TestCase):
    def test_exact_name_beats_a_longer_sibling_ranked_first(self):
        self.assertEqual(select_top_source("Galaxy S25 price", ULTRA_FIRST), "galaxy_s25")

    def test_brand_prefix_in_the_query_is_ignored(self):
        self.assertEqual(select_top_source("Samsung Galaxy S25 price", ULTRA_FIRST), "galaxy_s25")

    def test_naming_the_longer_sibling_selects_it_whatever_the_embedding_order(self):
        for ranked in (ULTRA_FIRST, S25_FIRST):
            with self.subTest(ranked=ranked):
                self.assertEqual(select_top_source("Galaxy S25 Ultra price", ranked), "galaxy_s25_ultra")
                self.assertEqual(select_top_source("Samsung Galaxy S25 Ultra", ranked), "galaxy_s25_ultra")

    def test_query_naming_no_product_keeps_the_embedding_order(self):
        self.assertEqual(select_top_source("which phone has the best camera?", ULTRA_FIRST), "galaxy_s25_ultra")
        self.assertEqual(select_top_source("which phone has the best camera?", S25_FIRST), "galaxy_s25")

    def test_partial_name_is_not_a_match(self):
        """"ps5" alone lacks the "pro" token of ps5_pro, so the embedding order stands."""
        self.assertEqual(select_top_source("ps5 price", ["switch_2", "ps5_pro"]), "switch_2")

    def test_single_token_names(self):
        self.assertEqual(select_top_source("Tesla price", ["honda", "tesla", "macbook"]), "tesla")
        self.assertEqual(select_top_source("iPhone price", ["ipad", "iphone"]), "iphone")

    def test_digit_tokens_and_case(self):
        self.assertEqual(select_top_source("NINTENDO SWITCH 2 PRICE", ["ps5_pro", "switch_2"]), "switch_2")

    def test_hyphenated_model_number_matches_the_file_stem(self):
        self.assertEqual(select_top_source("Sony WH-1000XM5 price", ["ps5_pro", "sony_wh1000xm5"]), "sony_wh1000xm5")

    def test_equal_specificity_keeps_the_better_ranked_source(self):
        self.assertEqual(select_top_source("iphone or tesla price", ["tesla", "iphone"]), "tesla")
        self.assertEqual(select_top_source("iphone or tesla price", ["iphone", "tesla"]), "iphone")

    def test_a_token_must_be_a_whole_word(self):
        """"ipad" is not found inside "ipadOS", and "s25" not inside "s250"."""
        self.assertEqual(select_top_source("ipados features", ["iphone", "ipad"]), "iphone")
        self.assertEqual(select_top_source("s250 price", ["galaxy_s25_ultra", "galaxy_s25"]), "galaxy_s25_ultra")

    def test_no_sources(self):
        self.assertIsNone(select_top_source("Galaxy S25 price", []))

    def test_does_not_modify_its_input(self):
        ranked = list(ULTRA_FIRST)
        select_top_source("Galaxy S25 price", ranked)
        self.assertEqual(ranked, ULTRA_FIRST)


def chunk(source: str, n: int) -> dict:
    return {"source": source, "content": f"{source} #{n}", "relevance_score": 0.5}


class PreferProductMatch(unittest.TestCase):
    def test_selected_source_chunks_move_to_the_front_in_their_order(self):
        chunks = [chunk("galaxy_s25_ultra", 1), chunk("galaxy_s25", 2), chunk("galaxy_s25_ultra", 3)]
        got = prefer_product_match("Galaxy S25 price", chunks)
        self.assertEqual([c["content"] for c in got], ["galaxy_s25 #2", "galaxy_s25_ultra #1", "galaxy_s25_ultra #3"])

    def test_no_matching_name_returns_the_same_order(self):
        chunks = [chunk("galaxy_s25_ultra", 1), chunk("galaxy_s25", 2)]
        self.assertEqual(prefer_product_match("best camera phone", chunks), chunks)

    def test_nothing_is_added_or_dropped_and_the_input_is_unchanged(self):
        chunks = [chunk("galaxy_s25_ultra", 1), chunk("galaxy_s25", 2), chunk("ps5_pro", 3)]
        before = list(chunks)
        got = prefer_product_match("Galaxy S25 price", chunks)
        self.assertEqual(chunks, before)
        self.assertEqual(sorted(c["content"] for c in got), sorted(c["content"] for c in chunks))

    def test_empty(self):
        self.assertEqual(prefer_product_match("Galaxy S25 price", []), [])

    def test_module_has_no_third_party_imports(self):
        source = Path(source_match.__file__).read_text(encoding="utf-8")
        for name in ("chromadb", "kafka", "sentence_transformers"):
            self.assertNotRegex(source, rf"(?m)^\s*(import|from)\s+{name}\b", name)


if __name__ == "__main__":
    unittest.main()
