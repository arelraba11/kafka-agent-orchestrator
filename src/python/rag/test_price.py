"""
Tests for price.py (stdlib unittest). Imports only price.py: no chromadb, kafka or
sentence-transformers, so a minimal CI venv can run it.

Run: bun run test:py
"""

import json
import unittest
from pathlib import Path

import price

# Chunks exactly as index_kb.split_into_chunks produces them (and as the live ChromaDB
# index holds them): flattened onto one line, "Price:" mid-line. index_kb.py imports
# chromadb and sentence_transformers at module level, so it cannot be imported here;
# regenerate this fixture by running split_into_chunks over data/products/*.txt.
KB = json.loads(Path(__file__).resolve().parent.joinpath("fixtures", "kb_chunks.json").read_text(encoding="utf-8"))


# Headline price (digits only) of every product in data/products/, and the one place it is listed.
# Launch MSRP in USD, except the existing five files, whose headline prices are sample data.
EXPECTED = {
    "iphone": "1199",
    "ipad": "1299",
    "macbook": "2000",
    "tesla": "74990",
    "honda": "24000",
    "galaxy_s25_ultra": "1299.99",
    "galaxy_s25": "799.99",
    "sony_wh1000xm5": "399.99",
    "ps5_pro": "699.99",
    "switch_2": "449.99",
    "toyota_camry": "28400",
}
# Products added with the "Price: only in the headline line" rule (the older files keep AppleCare/FSD add-ons).
HEADLINE_ONLY = ["galaxy_s25_ultra", "galaxy_s25", "sony_wh1000xm5", "ps5_pro", "switch_2", "toyota_camry"]


def headline(source: str) -> dict:
    """The chunk that carries the product's headline price: the first one containing 'Price:'."""
    return next(c for c in KB[source] if "Price:" in c["content"])


def chunk(source: str, n: int, content: str) -> dict:
    return {"id": f"{source}_chunk_{n}", "source": source, "content": content}


class ExtractPrice(unittest.TestCase):
    def test_headline_chunk_of_every_product(self):
        for name, want in EXPECTED.items():
            with self.subTest(product=name):
                self.assertEqual(price.extract_price([headline(name)]), want)

    def test_flattened_headline_chunk_with_price_mid_line(self):
        """Pins the shape: the indexed iPhone headline has 'Price:' mid-line, not at a line start."""
        content = headline("iphone")["content"]
        self.assertNotRegex(content, r"(?m)^Price:")
        self.assertIn(": Price: $1,199 ", content)
        self.assertEqual(price.extract_price([content]), "1199")

    def test_first_chunk_with_a_price_wins_over_every_add_on(self):
        """AppleCare ($199/year, $299) and FSD ($8,000) lose when the headline chunk comes first."""
        for name, want in EXPECTED.items():
            with self.subTest(product=name):
                self.assertEqual(price.extract_price(KB[name]), want)

    def test_every_data_file_has_a_fixture_and_an_expected_price(self):
        """The fixture and EXPECTED cover exactly the files in data/products/."""
        files = sorted(p.stem for p in Path(__file__).resolve().parents[3].joinpath("data", "products").glob("*.txt"))
        self.assertEqual(sorted(KB), files)
        self.assertEqual(sorted(EXPECTED), files)

    def test_headline_price_is_the_only_price_line_in_the_newer_products(self):
        """Add-ons in the newer files carry no 'Price:' label, so no other chunk can win the rule."""
        for name in HEADLINE_ONLY:
            with self.subTest(product=name):
                carrying = [c["id"] for c in KB[name] if "Price:" in c["content"]]
                self.assertEqual(carrying, [headline(name)["id"]])
                self.assertEqual(sum(c["content"].count("Price:") for c in KB[name]), 1)

    def test_accepts_chunk_dicts_and_plain_strings(self):
        self.assertEqual(price.extract_price([{"content": "Price: $5"}]), "5")
        self.assertEqual(price.extract_price(["Price: $5"]), "5")

    def test_recurring_prices_are_not_product_prices(self):
        self.assertIsNone(price.extract_price(["Price: $199/year or $9.99/month"]))
        self.assertIsNone(price.extract_price(["Price: $99/year"]))

    def test_commas_are_stripped_and_decimals_kept(self):
        self.assertEqual(price.extract_price(["Price: $1,299.50"]), "1299.50")

    def test_starting_at_and_base_price_forms(self):
        self.assertEqual(price.extract_price(["Price: Starting at $2,000"]), "2000")
        self.assertEqual(price.extract_price(["Base Price: $74,990"]), "74990")

    def test_first_match_wins_and_skips_recurring_ones(self):
        text = "AppleCare+: Price: $199/year or $9.99/month Price: $1,199"
        self.assertEqual(price.extract_price([text]), "1199")

    def test_text_with_no_price_is_none(self):
        self.assertIsNone(price.extract_price(["Storage: 256GB\nColors: Black"]))
        self.assertIsNone(price.extract_price([]))
        self.assertIsNone(price.extract_price(["Accessories: Magic Mouse: $79"]))


class PriceWithFallback(unittest.TestCase):
    def fetcher(self, chunks, calls):
        def fetch(source):
            calls.append(source)
            return chunks
        return fetch

    def test_price_in_a_retrieved_chunk_does_not_call_fetch(self):
        calls: list = []
        retrieved = [chunk("iphone", 0, "iPhone 16 Pro Max:\nPrice: $1,199"), chunk("iphone", 1, "Storage")]
        self.assertEqual(price.price_with_fallback(retrieved, self.fetcher([], calls)), "1199")
        self.assertEqual(calls, [])

    def addon(self, source: str, marker: str) -> dict:
        return next(c for c in KB[source] if marker in c["content"])

    def test_addon_loses_to_the_headline_in_retrieved_rank_order(self):
        """MacBook AppleCare $299 and Tesla FSD $8,000 match the rule but lose when the headline ranks first."""
        cases = {"macbook": ("Price: $299", "2000"), "tesla": ("Price: $8,000 one-time", "74990")}
        for source, (marker, want) in cases.items():
            with self.subTest(product=source):
                addon = self.addon(source, marker)
                self.assertNotEqual(addon["id"], headline(source)["id"])
                self.assertIsNotNone(price.extract_price([addon]))  # it would match on its own
                calls: list = []
                retrieved = [headline(source), addon]
                self.assertEqual(price.price_with_fallback(retrieved, self.fetcher([], calls)), want)
                self.assertEqual(calls, [])

    def test_addon_loses_to_the_headline_in_full_source_order(self):
        """Same, via the fallback: the retrieved chunks carry no price, the full source is string-sorted."""
        cases = {"macbook": ("Price: $299", "2000"), "tesla": ("Price: $8,000 one-time", "74990")}
        for source, (marker, want) in cases.items():
            with self.subTest(product=source):
                no_price = next(c for c in KB[source] if "Price:" not in c["content"])
                full = sorted(KB[source], key=lambda c: c["id"], reverse=True)  # scrambled order
                calls: list = []
                self.assertEqual(price.price_with_fallback([no_price], self.fetcher(full, calls)), want)
                self.assertEqual(calls, [source])

    def test_price_only_in_a_non_retrieved_chunk_uses_the_fallback(self):
        calls: list = []
        retrieved = [chunk("iphone", 1, "Storage: 256GB"), chunk("iphone", 2, "Camera: 48MP")]
        full = [chunk("iphone", 0, "Price: $1,199"), chunk("iphone", 1, "Storage: 256GB")]
        self.assertEqual(price.price_with_fallback(retrieved, self.fetcher(full, calls)), "1199")
        self.assertEqual(calls, ["iphone"])

    def test_fallback_chunks_are_ordered_numerically_not_by_id_string(self):
        calls: list = []
        retrieved = [chunk("tesla", 5, "Warranty")]
        # string order: _chunk_0, _chunk_10, _chunk_2 → the $8,000 line in _chunk_10 would win
        full = [
            chunk("tesla", 0, "Tesla Vehicle Catalog"),
            chunk("tesla", 10, "Price: $8,000 one-time"),
            chunk("tesla", 2, "Base Price: $74,990"),
        ]
        self.assertEqual(price.price_with_fallback(retrieved, self.fetcher(full, calls)), "74990")

    def test_only_the_top_ranked_source_is_considered(self):
        calls: list = []
        retrieved = [chunk("ipad", 1, "Display"), chunk("iphone", 0, "Price: $1,199")]
        self.assertIsNone(price.price_with_fallback(retrieved, self.fetcher([chunk("ipad", 1, "Display")], calls)))
        self.assertEqual(calls, ["ipad"])

    def test_retrieved_chunks_are_scanned_in_rank_order(self):
        retrieved = [chunk("macbook", 3, "Price: $299"), chunk("macbook", 0, "Price: Starting at $2,000")]
        self.assertEqual(price.price_with_fallback(retrieved, lambda s: []), "299")

    def test_no_price_anywhere_is_none(self):
        calls: list = []
        retrieved = [chunk("honda", 1, "Trims: LX")]
        self.assertIsNone(price.price_with_fallback(retrieved, self.fetcher([chunk("honda", 2, "Colors")], calls)))

    def test_empty_retrieved_list_is_none_and_never_fetches(self):
        calls: list = []
        self.assertIsNone(price.price_with_fallback([], self.fetcher([], calls)))
        self.assertEqual(calls, [])


if __name__ == "__main__":
    unittest.main()
