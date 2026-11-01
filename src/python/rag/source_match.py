"""
source_match.py — pure product-name matching for the RAG worker (stdlib only).

Embedding similarity alone can rank a longer sibling first: "Galaxy S25 price" is closer
to the galaxy_s25_ultra chunks than to the galaxy_s25 chunks, so the worker would read the
Ultra's price. This module picks the top source by product name instead, and falls back to
the embedding rank when no source name matches the query. Like price.py it imports no
chromadb, kafka or sentence-transformers, so a minimal CI venv can test it.

A source is a data/products file stem ("galaxy_s25_ultra"); its name tokens are the
stem split on "_" ("galaxy", "s25", "ultra"). A source MATCHES a query when every one of
its name tokens appears among the query's tokens. A source with a token the query does not
mention (galaxy_s25_ultra for "Galaxy S25 price") does not match.
"""

import re
from typing import Sequence

_TOKEN = re.compile(r"[a-z0-9]+")
_NAME_SEPARATOR = "_"


def _query_tokens(query: str) -> set[str]:
    # "WH-1000XM5" and "wh1000xm5" are the same product name: drop hyphens first.
    return set(_TOKEN.findall(query.lower().replace("-", "")))


def _name_tokens(source: str) -> list[str]:
    return [t for t in source.lower().split(_NAME_SEPARATOR) if t]


def select_top_source(query: str, ranked_sources: Sequence[str]) -> str | None:
    """
    The source to treat as the top one for `query`.
    `ranked_sources` holds distinct source names in embedding-rank order (best first).

      1. Sources whose name tokens ALL appear in the query are candidates.
      2. The most specific candidate wins (the most name tokens), so "Galaxy S25 Ultra
         price" picks galaxy_s25_ultra over galaxy_s25. Equal specificity: embedding rank.
      3. No candidate: the embedding-rank top (`ranked_sources[0]`).
      4. No sources: None.
    """
    if not ranked_sources:
        return None
    wanted = _query_tokens(query)
    best: str | None = None
    best_size = 0
    for source in ranked_sources:  # rank order, so a strict ">" keeps the better-ranked tie
        tokens = _name_tokens(source)
        if tokens and wanted.issuperset(tokens) and len(tokens) > best_size:
            best, best_size = source, len(tokens)
    return best if best is not None else ranked_sources[0]


def prefer_product_match(query: str, chunks: Sequence[dict]) -> list[dict]:
    """
    `chunks` (dicts with a `source` key, embedding-rank order) with the chunks of the
    selected top source first. Both groups keep their relative order. Nothing is added or
    dropped, and `chunks` is not modified.
    """
    ranked_sources = list(dict.fromkeys(c["source"] for c in chunks))
    top = select_top_source(query, ranked_sources)
    return [c for c in chunks if c["source"] == top] + [c for c in chunks if c["source"] != top]
