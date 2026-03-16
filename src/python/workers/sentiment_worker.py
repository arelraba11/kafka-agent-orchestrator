"""
sentiment_worker.py

Consumes sanitized-messages and classifies sentiment using
distilbert-base-uncased-finetuned-sst-2-english (HuggingFace).

Usage:
    python sentiment_worker.py

Topics:
    IN  <- sanitized-messages   (group: sentiment-group)
    OUT -> analysis-sentiment

Model:
    distilbert-base-uncased-finetuned-sst-2-english
    Labels: POSITIVE | NEGATIVE
"""

import json
import os

from kafka import KafkaConsumer, KafkaProducer
from transformers import pipeline

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "127.0.0.1:9092")
MODEL_NAME = "distilbert-base-uncased-finetuned-sst-2-english"

TOPIC_IN = "sanitized-messages"
TOPIC_OUT = "analysis-sentiment"
GROUP_ID = "sentiment-group"


def main() -> None:
    print(f"[sentiment] Loading model {MODEL_NAME}...")
    classifier = pipeline("sentiment-analysis", model=MODEL_NAME)
    print("[sentiment] Model loaded.")

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

    print(f"[sentiment] Connected. Subscribed to '{TOPIC_IN}'. Waiting for messages...")

    for kafka_message in consumer:
        msg = kafka_message.value
        message_id = msg["id"]
        text = msg["text"]

        print(f"[sentiment] received {message_id}")

        result = classifier(text)[0]
        sentiment = result["label"]   # "POSITIVE" or "NEGATIVE"
        score = round(result["score"], 4)

        print(f"[sentiment] sentiment={sentiment}")

        payload = {
            "id": message_id,
            "sentiment": sentiment,
            "score": score,
        }

        producer.send(TOPIC_OUT, key=message_id, value=payload)
        producer.flush()


if __name__ == "__main__":
    main()
