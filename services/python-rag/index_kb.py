"""
index_kb.py — Knowledge Base Indexer

Loads product documents from data/products/, splits them into chunks,
generates embeddings with sentence-transformers, and stores vectors in
a local ChromaDB collection.

Usage:
    python index_kb.py
"""

import sys
from pathlib import Path

import chromadb
from sentence_transformers import SentenceTransformer

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parent.parent.parent
PRODUCTS_DIR = REPO_ROOT / "data" / "products"
CHROMA_DIR = Path(__file__).resolve().parent / "chroma_db"

COLLECTION_NAME = "product_knowledge_base"
CHUNK_SIZE = 400  # max characters per chunk


# ---------------------------------------------------------------------------
# Document loading
# ---------------------------------------------------------------------------
def load_documents(products_dir: Path) -> list[dict]:
    docs = []
    for file_path in sorted(products_dir.glob("*.txt")):
        text = file_path.read_text(encoding="utf-8").strip()
        product_name = file_path.stem  # e.g. "iphone", "macbook", "tesla"
        docs.append({"source": product_name, "text": text})
        print(f"[index_kb] Loaded: {file_path.name}  ({len(text)} chars)")
    return docs


# ---------------------------------------------------------------------------
# Text chunking
# ---------------------------------------------------------------------------
def split_into_chunks(text: str, source: str, chunk_size: int = CHUNK_SIZE) -> list[dict]:
    """
    Split text into chunks of at most `chunk_size` characters.
    Prefers paragraph boundaries, falls back to sentence boundaries.
    """
    paragraphs = [p.strip() for p in text.split("\n\n") if p.strip()]
    chunks: list[dict] = []
    current = ""
    idx = 0

    def flush(buf: str) -> None:
        nonlocal idx
        if buf:
            chunks.append({"id": f"{source}_chunk_{idx}", "text": buf, "source": source})
            idx += 1

    for para in paragraphs:
        if len(current) + len(para) + 2 <= chunk_size:
            current = (current + "\n\n" + para).strip() if current else para
        else:
            flush(current)
            if len(para) <= chunk_size:
                current = para
            else:
                # Paragraph alone exceeds limit — split by sentence
                current = ""
                for sentence in para.replace(". ", ".\n").split("\n"):
                    sentence = sentence.strip()
                    if not sentence:
                        continue
                    if len(current) + len(sentence) + 1 <= chunk_size:
                        current = (current + " " + sentence).strip() if current else sentence
                    else:
                        flush(current)
                        current = sentence

    flush(current)
    return chunks


# ---------------------------------------------------------------------------
# Main indexing routine
# ---------------------------------------------------------------------------
def index_knowledge_base() -> None:
    print("[index_kb] ===== Knowledge Base Indexing =====")

    # Validate source directory
    if not PRODUCTS_DIR.exists():
        print(f"[index_kb] ERROR: Products directory not found: {PRODUCTS_DIR}")
        sys.exit(1)

    # Load embedding model
    print("[index_kb] Loading embedding model: all-MiniLM-L6-v2 ...")
    model = SentenceTransformer("all-MiniLM-L6-v2")
    print("[index_kb] Model loaded.")

    # Connect to ChromaDB (persistent)
    print(f"[index_kb] Initializing ChromaDB at: {CHROMA_DIR}")
    CHROMA_DIR.mkdir(parents=True, exist_ok=True)
    client = chromadb.PersistentClient(path=str(CHROMA_DIR))

    # Drop existing collection so re-runs produce a clean index
    try:
        client.delete_collection(COLLECTION_NAME)
        print(f"[index_kb] Dropped existing collection '{COLLECTION_NAME}'")
    except Exception:
        pass

    collection = client.create_collection(
        name=COLLECTION_NAME,
        metadata={"hnsw:space": "cosine"},
    )
    print(f"[index_kb] Created collection '{COLLECTION_NAME}'")

    # Load documents
    docs = load_documents(PRODUCTS_DIR)
    if not docs:
        print(f"[index_kb] ERROR: No .txt files found in {PRODUCTS_DIR}")
        sys.exit(1)

    # Split into chunks
    all_chunks: list[dict] = []
    for doc in docs:
        chunks = split_into_chunks(doc["text"], doc["source"])
        all_chunks.extend(chunks)
        print(f"[index_kb] '{doc['source']}' → {len(chunks)} chunks")

    print(f"[index_kb] Total chunks to embed: {len(all_chunks)}")

    # Generate embeddings
    print("[index_kb] Generating embeddings ...")
    texts = [c["text"] for c in all_chunks]
    embeddings = model.encode(texts, show_progress_bar=True).tolist()

    # Insert into ChromaDB
    collection.add(
        ids=[c["id"] for c in all_chunks],
        embeddings=embeddings,
        documents=texts,
        metadatas=[{"source": c["source"]} for c in all_chunks],
    )

    print(f"[index_kb] Indexed {len(all_chunks)} chunks into ChromaDB.")
    print(f"[index_kb] Vector DB persisted at: {CHROMA_DIR}")
    print("[index_kb] ===== Indexing complete =====")


if __name__ == "__main__":
    index_knowledge_base()
