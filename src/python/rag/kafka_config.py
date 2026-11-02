"""
kafka_config.py — env-driven Kafka settings for the RAG worker (stdlib only).

Mirrors shared/kafka/config.ts and shared/topics.ts. Every default equals the previous
hardcoded behavior. Only the integration suite sets these variables.

  KAFKA_BROKERS               comma-separated; KAFKA_BROKER (legacy) is a fallback
  KAFKA_NAMESPACE             prefix for every topic and consumer group, applied only at
                              the Kafka edge; DLQ records and logs keep logical names
  KAFKA_OFFSET_RESET          earliest | latest (default latest)
  KAFKA_SESSION_TIMEOUT_MS / KAFKA_HEARTBEAT_INTERVAL_MS / KAFKA_REBALANCE_TIMEOUT_MS
  KAFKA_RETRIES / KAFKA_REQUEST_TIMEOUT_MS / KAFKA_MAX_BLOCK_MS   producer (max block: how long send()
                              waits for topic metadata; the library default is 60000)
  RAG_SEND_TIMEOUT_S          confirmed-send timeout (default 10)
"""

import os
from typing import Mapping, Optional

Env = Mapping[str, str]

TOPICS = ("user-commands", "conversation-events", "tool-invocation-requests", "dead-letter-queue")


def _int(env: Env, name: str) -> Optional[int]:
    raw = env.get(name)
    if raw is None or raw == "":
        return None
    try:
        n = int(raw)
    except ValueError:
        n = 0
    if n <= 0:
        raise ValueError(f"{name} must be a positive integer, got {raw!r}")
    return n


def brokers(env: Env = os.environ) -> list[str]:
    raw = env.get("KAFKA_BROKERS") or env.get("KAFKA_BROKER") or "localhost:9092"
    return [b.strip() for b in raw.split(",") if b.strip()]


def namespace(env: Env = os.environ) -> str:
    return env.get("KAFKA_NAMESPACE", "")


def physical_topic(ns: str, name: str) -> str:
    return ns + name


def logical_topic(ns: str, physical: str) -> Optional[str]:
    """The logical topic behind a physical one, or None when outside the namespace or unknown."""
    if not physical.startswith(ns):
        return None
    name = physical[len(ns):]
    return name if name in TOPICS else None


def physical_group(ns: str, group: str) -> str:
    return ns + group


def offset_reset(env: Env = os.environ) -> str:
    v = env.get("KAFKA_OFFSET_RESET") or "latest"
    if v not in ("earliest", "latest"):
        raise ValueError(f'KAFKA_OFFSET_RESET must be "earliest" or "latest", got {v!r}')
    return v


def consumer_kwargs(env: Env = os.environ) -> dict:
    """kafka-python KafkaConsumer options; empty by default (library defaults)."""
    out = {}
    for var, key in (
        ("KAFKA_SESSION_TIMEOUT_MS", "session_timeout_ms"),
        ("KAFKA_HEARTBEAT_INTERVAL_MS", "heartbeat_interval_ms"),
        ("KAFKA_REBALANCE_TIMEOUT_MS", "max_poll_interval_ms"),
    ):
        n = _int(env, var)
        if n is not None:
            out[key] = n
    return out


def producer_kwargs(env: Env = os.environ) -> dict:
    """kafka-python KafkaProducer options; empty by default (library defaults)."""
    out = {}
    for var, key in (
        ("KAFKA_RETRIES", "retries"),
        ("KAFKA_REQUEST_TIMEOUT_MS", "request_timeout_ms"),
        ("KAFKA_MAX_BLOCK_MS", "max_block_ms"),
    ):
        n = _int(env, var)
        if n is not None:
            out[key] = n
    return out


def send_timeout_s(env: Env = os.environ) -> int:
    return _int(env, "RAG_SEND_TIMEOUT_S") or 10
