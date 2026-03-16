"""
urgency_worker.py

Consumes sanitized-messages and classifies urgency using
facebook/bart-large-mnli zero-shot classification (HuggingFace).

Usage:
    python urgency_worker.py

Topics:
    IN  <- sanitized-messages   (group: urgency-group)
    OUT -> analysis-urgency

Model:
    facebook/bart-large-mnli
    Candidate labels: Urgent, Complaint, General Inquiry
"""

import json
import os

from kafka import KafkaConsumer, KafkaProducer
from transformers import pipeline

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "127.0.0.1:9092")
MODEL_NAME = "facebook/bart-large-mnli"
CANDIDATE_LABELS = ["Urgent", "Complaint", "General Inquiry"]

TOPIC_IN = "sanitized-messages"
TOPIC_OUT = "analysis-urgency"
GROUP_ID = "urgency-group"


def main() -> None:
    print(f"[urgency] Loading model {MODEL_NAME}...")
    classifier = pipeline("zero-shot-classification", model=MODEL_NAME)
    print("[urgency] Model loaded.")

    consumer = KafkaConsumer(
        TOPIC_IN,
        bootstrap_servers=KAFKA_BROKER,
        group_id=GROUP_ID,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )

    print(f"[urgency] Connected. Subscribed to '{TOPIC_IN}'. Waiting for messages...")

    for kafka_message in consumer:
        msg = kafka_message.value
        message_id = msg["id"]
        text = msg["text"]

        print(f"[urgency] received {message_id}")

        result = classifier(text, candidate_labels=CANDIDATE_LABELS)

        # result["labels"] and result["scores"] are sorted highest-score first
        top_label = result["labels"][0]
        top_score = round(result["scores"][0], 4)

        print(f"[urgency] urgency={top_label}")

        payload = {
            "id": message_id,
            "urgency": top_label,
            "score": top_score,
        }

        producer.send(TOPIC_OUT, key=message_id, value=payload)
        producer.flush()


if __name__ == "__main__":
    main()
