"""
price.py — pure product-price extraction for the RAG worker (stdlib only).

The worker sets `result["value"]` to the price so `{{step_N.result}}` hands `exchange`
the KB price instead of the first number in the context text (the model "16" in
"iPhone 16 Pro Max"). The chromadb lookup lives in rag_retriever.py and is passed in
as a callable, so this module never imports chromadb, kafka or sentence-transformers.
"""

import re
from typing import Callable, Iterable

# "Price: $1,199", "Base Price: $74,990", "Price: Starting at $2,000".
# NOT anchored to a line start: index_kb.py flattens a paragraph it splits by sentence
# onto one line, so the indexed headline chunk reads
# "iPhone 16 Pro Max: Price: $1,199 Storage: 256GB ..." with "Price:" mid-line.
_PRICE = re.compile(r"\b(?:Base )?Price:\s*(?:Starting at\s*)?\$([\d,]+(?:\.\d+)?)")
# AppleCare+ and subscriptions: "$199/year", "$9.99/month"
_RECURRING = re.compile(r"\s*/\s*(?:year|month)\b", re.IGNORECASE)
_CHUNK_NUMBER = re.compile(r"_chunk_(\d+)$")


def _text(chunk) -> str:
    return chunk if isinstance(chunk, str) else chunk.get("content", "")


def extract_price(chunks: Iterable) -> str | None:
    """
    First `Price:` amount (in chunk order, then text order) that is not a recurring
    /year or /month charge. Returns the amount digits-only ("1199"), or None.
    `chunks` holds chunk dicts with a `content` key, or plain strings.
    """
    for chunk in chunks:
        text = _text(chunk)
        for m in _PRICE.finditer(text):
            if _RECURRING.match(text[m.end():]):
                continue
            return m.group(1).replace(",", "")
    return None


def _chunk_order(chunk: dict) -> tuple[int, int]:
    m = _CHUNK_NUMBER.search(chunk.get("id", ""))
    # numeric, never by id string (string order puts _chunk_10 before _chunk_2)
    return (0, int(m.group(1))) if m else (1, 0)


def price_with_fallback(
    retrieved: list[dict],
    fetch_source_chunks: Callable[[str], list[dict]],
) -> str | None:
    """
    Price for the top-ranked retrieved source.
      1. extract_price over the retrieved chunks of that source, in rank order.
      2. Otherwise fetch ALL chunks of the source (dicts with `id` and `content`),
         order them by their numeric chunk number, and extract again.
      3. Otherwise None.
    """
    if not retrieved:
        return None
    source = retrieved[0]["source"]

    found = extract_price(c for c in retrieved if c["source"] == source)
    if found is not None:
        return found

    return extract_price(sorted(fetch_source_chunks(source), key=_chunk_order))
