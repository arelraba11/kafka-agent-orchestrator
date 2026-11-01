"""
rag_retriever.py — RAG Tool Worker

Kafka consumer for topic `tool-invocation-requests`.
Validates every ToolInvocationRequested against the generated contract (contracts.py),
then filters events where payload.toolName == "getProductInformation".
Queries ChromaDB for relevant product knowledge chunks.
Emits ToolInvocationResulted events to topic `conversation-events` (validated first).
Poison messages (malformed, contract-violating, key mismatch) are written to
`dead-letter-queue` as MessageDeadLettered records and their offsets committed.

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

import contracts
from price import price_with_fallback
from source_match import prefer_product_match

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
CONSUMER_GROUP = "rag-tool-worker"
INPUT_TOPIC = "tool-invocation-requests"
OUTPUT_TOPIC = "conversation-events"
DLQ_TOPIC = "dead-letter-queue"  # keep in sync with shared/topics.ts
SERVICE = "rag"

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
        auto_offset_reset="latest",  # matches the TS consumers (fromBeginning: false)
        enable_auto_commit=False,   # commit manually, only after the result send is acked
        # raw bytes: a malformed value must not raise inside the consumer iterator
    )


def make_producer() -> KafkaProducer:
    return KafkaProducer(bootstrap_servers=KAFKA_BROKER)


def _send_confirmed(producer: KafkaProducer, topic: str, key: bytes | None, value: bytes) -> None:
    # .get() raises if the send failed, so the caller never commits past a lost record
    producer.send(topic, key=key, value=value).get(timeout=10)
    producer.flush()


def emit_result(producer: KafkaProducer, event: dict) -> None:
    """Sends an already-validated ToolInvocationResulted (see contracts.result_event)."""
    _send_confirmed(
        producer,
        OUTPUT_TOPIC,
        event["conversationId"].encode("utf-8"),
        json.dumps(event).encode("utf-8"),
    )
    print(
        f"[rag_retriever] Emitted ToolInvocationResulted "
        f"→ conversationId={event['conversationId']}"
    )


def dead_letter(producer: KafkaProducer, consumer: KafkaConsumer, message, exc: Exception) -> None:
    """
    Writes a MessageDeadLettered record for a poison message, then commits its offset.
    If the DLQ send fails this raises before the commit, so the process exits and the
    message is redelivered: a message is never skipped without a DLQ record.
    """
    record = contracts.dead_letter_record(
        topic=message.topic,
        partition=message.partition,
        offset=message.offset,
        key=message.key,
        timestamp_ms=message.timestamp,
        value=message.value,
        group=CONSUMER_GROUP,
        service=SERVICE,
        exc=exc,
    )
    _send_confirmed(
        producer,
        DLQ_TOPIC,
        message.key,  # keyed by the original key (partition affinity for replay)
        json.dumps(record, ensure_ascii=False).encode("utf-8"),
    )
    consumer.commit()
    err = record["payload"]["error"]
    print(
        f"[kafka] dead-lettered topic={message.topic} partition={message.partition} "
        f"offset={message.offset} group={CONSUMER_GROUP} class={err['class']} "
        f"reason={json.dumps(err['reason'])}"
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
        (results.get("documents") or [[]])[0],
        (results.get("metadatas") or [[]])[0],
        (results.get("distances") or [[]])[0],
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


def fetch_source_chunks(collection: chromadb.Collection, source: str) -> list[dict]:
    """ALL chunks of one source document ({id, content}), for the price fallback."""
    got = collection.get(where={"source": source}, include=["documents", "metadatas"])
    return [
        {"id": chunk_id, "content": doc}
        for chunk_id, doc in zip(got.get("ids") or [], got.get("documents") or [])
    ]


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
        # Validate: decode → probe → full schema → key. Poison goes to the DLQ.
        try:
            event = contracts.decode(message.value)
            message_type = contracts.message_type_of(event)
            if message_type is None:
                raise contracts.ContractViolation(
                    "message has no eventType or commandType",
                    [{"path": "eventType", "message": "missing discriminator"}],
                )
            # Only ToolInvocationRequested lives on this topic; skip anything else.
            if message_type != contracts.REQUEST_TYPE:
                consumer.commit()
                continue
            contracts.validate_request(event)
            contracts.check_key(message.key, event)
        except (contracts.MalformedMessage, contracts.ContractViolation, contracts.KeyMismatch) as exc:
            dead_letter(producer, consumer, message, exc)
            continue

        payload = event["payload"]

        # Only handle our tool
        if payload["toolName"] != TOOL_NAME:
            consumer.commit()
            continue

        conversation_id: str = event["conversationId"]
        tool_input: dict = payload["input"]

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

        result: dict
        try:
            t_start = time.time()
            # Embedding rank first, then the product named in the query goes first: a longer
            # sibling ("galaxy_s25_ultra") must not outrank the exact name ("Galaxy S25").
            chunks = prefer_product_match(query, retrieve(model, collection, query))
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

            # `value` is what a downstream {{step_N.result}} resolves to: the product
            # price (digits only), present only when one was found. Without it the
            # placeholder falls back to `context` and exchange parses the first number.
            price = price_with_fallback(chunks, lambda src: fetch_source_chunks(collection, src))
            if price is not None:
                result["value"] = price
                print(f"[rag_retriever] Price found: value={price}")

        except Exception as exc:
            error_msg = str(exc)
            print(f"[rag_retriever] Error processing query: {error_msg}")
            result = {"error": error_msg, "success": False}

        # Build and validate the result, then send it, all outside the try: a failed
        # send raises, the process exits uncommitted and the record is redelivered,
        # instead of becoming a plan-failing error result.
        try:
            result_message = contracts.result_event(
                conversation_id, payload["stepIndex"], result, TOOL_NAME
            )
        except contracts.OutboundContractViolation as exc:
            # The input was valid; this worker's own output is not. Dead-letter the input.
            dead_letter(producer, consumer, message, exc)
            continue

        emit_result(producer, result_message)
        consumer.commit()


if __name__ == "__main__":
    run()
