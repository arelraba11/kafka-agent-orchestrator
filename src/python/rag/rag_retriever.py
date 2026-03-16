"""
rag_retriever.py — RAG Tool Worker

Kafka consumer for topic `tool-invocation-requests`.
Filters events where payload.toolName == "getProductInformation".
Queries ChromaDB for relevant product knowledge chunks.
Emits ToolInvocationResulted events to topic `conversation-events`.

Usage:
    python rag_retriever.py

Prerequisites:
    Run index_kb.py at least once to build the ChromaDB index.
"""

import json
import os
import time
from pathlib import Path

import chromadb
from kafka import KafkaConsumer, KafkaProducer
from sentence_transformers import SentenceTransformer

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
CONSUMER_GROUP = "rag-tool-worker"
INPUT_TOPIC = "tool-invocation-requests"
OUTPUT_TOPIC = "conversation-events"

TOOL_NAME = "getProductInformation"
COLLECTION_NAME = "product_knowledge_base"
CHROMA_DIR = Path(__file__).resolve().parent / "chroma_db"
TOP_K = 3  # number of chunks to retrieve per query


# ---------------------------------------------------------------------------
# ChromaDB loader
# ---------------------------------------------------------------------------
def load_collection() -> chromadb.Collection:
    if not CHROMA_DIR.exists():
        raise FileNotFoundError(
            f"ChromaDB not found at {CHROMA_DIR}. Run index_kb.py first."
        )
    client = chromadb.PersistentClient(path=str(CHROMA_DIR))
    collection = client.get_collection(COLLECTION_NAME)
    count = collection.count()
    print(f"[rag_retriever] ChromaDB loaded — collection '{COLLECTION_NAME}' has {count} chunks")
    return collection


# ---------------------------------------------------------------------------
# Kafka helpers
# ---------------------------------------------------------------------------
def make_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        key_deserializer=lambda b: b.decode("utf-8") if b else None,
    )


def make_producer() -> KafkaProducer:
    return KafkaProducer(bootstrap_servers=KAFKA_BROKER)


def emit_result(
    producer: KafkaProducer,
    conversation_id: str,
    result: dict,
) -> None:
    event = {
        "conversationId": conversation_id,
        "timestamp": int(time.time() * 1000),
        "eventType": "ToolInvocationResulted",
        "payload": {
            "toolName": TOOL_NAME,
            "result": result,
        },
    }
    producer.send(
        OUTPUT_TOPIC,
        key=conversation_id.encode("utf-8"),
        value=json.dumps(event).encode("utf-8"),
    )
    producer.flush()
    print(
        f"[rag_retriever] Emitted ToolInvocationResulted "
        f"→ conversationId={conversation_id}"
    )


# ---------------------------------------------------------------------------
# RAG query
# ---------------------------------------------------------------------------
def retrieve(
    model: SentenceTransformer,
    collection: chromadb.Collection,
    query: str,
) -> list[dict]:
    embedding = model.encode([query])[0].tolist()
    results = collection.query(
        query_embeddings=[embedding],
        n_results=TOP_K,
        include=["documents", "metadatas", "distances"],
    )

    chunks = []
    for doc, meta, dist in zip(
        results.get("documents", [[]])[0],
        results.get("metadatas", [[]])[0],
        results.get("distances", [[]])[0],
    ):
        chunks.append(
            {
                "source": meta.get("source", "unknown"),
                "content": doc,
                # cosine distance → similarity score
                "relevance_score": round(1.0 - float(dist), 4),
            }
        )
    return chunks


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------
def run() -> None:
    print("[rag_retriever] ===== RAG Retriever Service =====")
    print(f"[rag_retriever] Kafka broker  : {KAFKA_BROKER}")
    print(f"[rag_retriever] Consumer group: {CONSUMER_GROUP}")
    print(f"[rag_retriever] Input topic   : {INPUT_TOPIC}")
    print(f"[rag_retriever] Output topic  : {OUTPUT_TOPIC}")
    print(f"[rag_retriever] Tool filter   : {TOOL_NAME}")

    # Load resources
    print("[rag_retriever] Loading embedding model: all-MiniLM-L6-v2 ...")
    model = SentenceTransformer("all-MiniLM-L6-v2")
    print("[rag_retriever] Model loaded.")

    collection = load_collection()

    consumer = make_consumer()
    producer = make_producer()

    print("[rag_retriever] Ready — waiting for messages ...\n")

    for message in consumer:
        event = message.value
        if not isinstance(event, dict):
            continue

        # Only handle ToolInvocationRequested
        if event.get("eventType") != "ToolInvocationRequested":
            continue

        payload = event.get("payload", {})

        # Only handle our tool
        if payload.get("toolName") != TOOL_NAME:
            continue

        conversation_id: str = event.get("conversationId", "unknown")
        tool_input: dict = payload.get("input", {})

        # Accept 'query', 'product', or any string value as the search term
        query: str = (
            tool_input.get("query")
            or tool_input.get("product")
            or tool_input.get("question")
            or " ".join(str(v) for v in tool_input.values())
            or "product information"
        )

        print(
            f"[rag_retriever] Query received | "
            f"conversationId={conversation_id} | "
            f'query="{query}"'
        )

        t_start = time.time()
        chunks = retrieve(model, collection, query)
        latency_ms = round((time.time() - t_start) * 1000, 1)

        if chunks:
            top_source = chunks[0]["source"]
            top_score = chunks[0]["relevance_score"]
            print(
                f"[rag_retriever] Retrieved {len(chunks)} chunks in {latency_ms}ms "
                f"(top: source={top_source}, score={top_score})"
            )
        else:
            print(f"[rag_retriever] No chunks retrieved for query: \"{query}\"")

        # Build combined context string for the synthesizer
        context = "\n\n---\n\n".join(
            f"[{c['source']}]\n{c['content']}" for c in chunks
        )

        result = {
            "query": query,
            "context": context,
            "chunks": chunks,
            "tool": TOOL_NAME,
            "retrieval_latency_ms": latency_ms,
            "success": True,
        }

        emit_result(producer, conversation_id, result)


if __name__ == "__main__":
    run()
