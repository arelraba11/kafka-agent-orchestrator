"""
contracts.py — message contracts for the RAG worker.

The contract is defined once, in zod (shared/schemas/*.ts). `bun run schemas:gen`
turns it into JSON Schema (shared/schemas/<Name>.json); this module loads those files
and validates with `jsonschema`. Never hand-edit the JSON.

Mirrors shared/kafka/consume.ts: a message that can never be processed is dead-lettered
(MessageDeadLettered) and its offset committed; the caller decides what to do with
transient failures (here: crash without committing, so the record is redelivered).

Imports only the standard library and jsonschema, so tests run without kafka/chromadb.
"""

import base64
import json
import re
import time
from functools import lru_cache
from pathlib import Path

from jsonschema import Draft202012Validator

# rag → python → src → repo root
SCHEMA_DIR = Path(__file__).resolve().parents[3] / "shared" / "schemas"

REQUEST_TYPE = "ToolInvocationRequested"
RESULT_TYPE = "ToolInvocationResulted"
DEAD_LETTER_TYPE = "MessageDeadLettered"

MAX_ORIGINAL_BYTES = 512 * 1024
MAX_ENCODED_CHARS = 700_000  # base64 of MAX_ORIGINAL_BYTES is ~699k
MAX_ISSUES = 50
MAX_TEXT = 1000


# ---------------------------------------------------------------------------
# Errors (mirror shared/kafka/errors.ts)
# ---------------------------------------------------------------------------
class MalformedMessage(Exception):
    """Null value, or a value that is not JSON. kind: malformed-json | missing-value."""

    def __init__(self, kind: str, message: str):
        super().__init__(message)
        self.kind = kind


class ContractViolation(Exception):
    """An inbound message failed its schema (or has no usable discriminator)."""

    def __init__(self, message: str, issues: list[dict]):
        super().__init__(message)
        self.issues = issues


class OutboundContractViolation(Exception):
    """A message this worker tried to publish failed its schema: a bug in the worker."""

    def __init__(self, message: str, issues: list[dict], message_type: str, topic: str):
        super().__init__(message)
        self.issues = issues
        self.message_type = message_type
        self.topic = topic


class KeyMismatch(Exception):
    """The Kafka key is not the message's conversationId."""


# ---------------------------------------------------------------------------
# Validators
# ---------------------------------------------------------------------------
@lru_cache(maxsize=None)
def validator(name: str) -> Draft202012Validator:
    """Validator for the generated JSON Schema of message `name` (format checks on)."""
    schema = json.loads((SCHEMA_DIR / f"{name}.json").read_text(encoding="utf-8"))
    return Draft202012Validator(schema, format_checker=Draft202012Validator.FORMAT_CHECKER)


def issues_of(v: Draft202012Validator, instance) -> list[dict]:
    """Flat [{path, message}] list. A missing required property is reported at its own path."""
    issues: list[dict] = []
    for err in sorted(v.iter_errors(instance), key=lambda e: list(map(str, e.absolute_path))):
        base = [str(p) for p in err.absolute_path]
        if err.validator == "required" and isinstance(err.instance, dict):
            for missing in err.validator_value:
                if missing not in err.instance:
                    issues.append(
                        {"path": ".".join(base + [missing]), "message": "Invalid input: required"}
                    )
            continue
        issues.append({"path": ".".join(base) or "(root)", "message": err.message})
    return issues


def _clip(s: str, n: int) -> str:
    return s if len(s) <= n else s[:n] + "…"


def _cap(issues: list[dict]) -> list[dict]:
    return [
        {"path": _clip(i["path"], 200), "message": _clip(i["message"], 500)}
        for i in issues[:MAX_ISSUES]
    ]


# ---------------------------------------------------------------------------
# Inbound
# ---------------------------------------------------------------------------
def decode(raw: bytes | None):
    """Bytes → parsed JSON. Raises MalformedMessage for a null value or invalid JSON."""
    if raw is None:
        raise MalformedMessage("missing-value", "message has a null value")
    try:
        return json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise MalformedMessage("malformed-json", f"value is not valid JSON: {exc}") from exc


def message_type_of(value) -> str | None:
    """`eventType`, or `commandType` for commands; None if there is no usable discriminator."""
    if not isinstance(value, dict):
        return None
    t = value.get("eventType", value.get("commandType"))
    return t if isinstance(t, str) else None


def validate_request(event) -> None:
    """Full ToolInvocationRequested validation. Raises ContractViolation."""
    issues = issues_of(validator(REQUEST_TYPE), event)
    if issues:
        raise ContractViolation(f"invalid {REQUEST_TYPE}", _cap(issues))


def check_key(key: bytes | None, event: dict) -> None:
    """Phase 2's partition invariant: the Kafka key is the conversationId."""
    k = None if key is None else key.decode("utf-8", errors="replace")
    if k != event["conversationId"]:
        raise KeyMismatch(f"key {json.dumps(k)} does not match conversationId {event['conversationId']}")


# ---------------------------------------------------------------------------
# Outbound
# ---------------------------------------------------------------------------
def result_event(
    conversation_id: str,
    step_index,
    result: dict,
    tool_name: str,
    now_ms: int | None = None,
) -> dict:
    """Builds a ToolInvocationResulted and validates it before it is ever sent."""
    event = {
        "conversationId": conversation_id,
        "timestamp": int(time.time() * 1000) if now_ms is None else now_ms,
        "eventType": RESULT_TYPE,
        "payload": {"toolName": tool_name, "stepIndex": step_index, "result": result},
    }
    issues = issues_of(validator(RESULT_TYPE), event)
    if issues:
        raise OutboundContractViolation(
            f"outbound {RESULT_TYPE} for conversation-events violates its contract",
            _cap(issues),
            RESULT_TYPE,
            "conversation-events",
        )
    return event


def _encode_original(value: bytes | None) -> dict | None:
    if value is None:
        return None
    truncated = len(value) > MAX_ORIGINAL_BYTES
    data = value[:MAX_ORIGINAL_BYTES] if truncated else value
    # Prefer readable utf8; a cut in the middle of a multi-byte character is repaired by
    # dropping the dangling bytes. JSON escaping can expand control characters, so utf8
    # is used only when its escaped size stays small.
    for drop in range(0, 4 if truncated else 1):
        chunk = data[: len(data) - drop]
        try:
            text = chunk.decode("utf-8")
        except UnicodeDecodeError:
            continue
        if len(json.dumps(text, ensure_ascii=False)) <= MAX_ENCODED_CHARS:
            return {"encoding": "utf8", "data": text, "truncated": truncated}
    return {"encoding": "base64", "data": base64.b64encode(data).decode("ascii"), "truncated": truncated}


def _conversation_id_of(value: bytes | None) -> str | None:
    if value is None:
        return None
    try:
        cid = json.loads(value.decode("utf-8")).get("conversationId")
    except (UnicodeDecodeError, ValueError, AttributeError):
        return None
    schema = validator(REQUEST_TYPE).schema["properties"]["conversationId"]
    ok = Draft202012Validator(schema, format_checker=Draft202012Validator.FORMAT_CHECKER)
    return cid if isinstance(cid, str) and ok.is_valid(cid) else None


def error_class_of(exc: Exception) -> str:
    if isinstance(exc, MalformedMessage):
        return exc.kind
    if isinstance(exc, ContractViolation):
        return "contract-violation"
    if isinstance(exc, OutboundContractViolation):
        return "outbound-contract-violation"
    if isinstance(exc, KeyMismatch):
        return "key-mismatch"
    raise TypeError(f"not a poison-message error: {exc!r}")


def dead_letter_record(
    *,
    topic: str,
    partition: int,
    offset: int | str,
    key: bytes | None,
    timestamp_ms: int | None,
    value: bytes | None,
    group: str,
    service: str,
    exc: Exception,
    attempts: int = 1,
    now_ms: int | None = None,
) -> dict:
    """
    Builds and validates a MessageDeadLettered record (mirrors shared/kafka/deadLetter.ts).
    For an OutboundContractViolation, `value` is the valid input and the issues/outbound
    describe the message this worker tried to produce.
    """
    error: dict = {
        "class": error_class_of(exc),
        "name": type(exc).__name__,
        "reason": _clip(str(exc), MAX_TEXT),
        "attempts": attempts,
    }
    if isinstance(exc, (ContractViolation, OutboundContractViolation)):
        error["issues"] = _cap(exc.issues)
    if isinstance(exc, OutboundContractViolation):
        error["outbound"] = {"topic": exc.topic, "messageType": exc.message_type}

    record = {
        "eventType": DEAD_LETTER_TYPE,
        "schemaVersion": 1,
        "conversationId": _conversation_id_of(value),
        "timestamp": int(time.time() * 1000) if now_ms is None else now_ms,
        "payload": {
            "source": {
                "topic": topic,
                "partition": partition,
                "offset": str(offset),
                "key": None if key is None else key.decode("utf-8", errors="replace"),
                "timestamp": None if timestamp_ms is None else str(timestamp_ms),
            },
            "consumer": {"group": group, "service": service},
            "error": error,
            "originalValue": _encode_original(value),
        },
    }
    problems = issues_of(validator(DEAD_LETTER_TYPE), record)
    if problems:  # a bug in this builder: fail loudly rather than skip without a record
        raise ValueError(f"dead-letter record violates its contract: {problems}")
    return record
