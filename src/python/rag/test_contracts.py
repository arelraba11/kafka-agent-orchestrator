"""
Tests for contracts.py (stdlib unittest; imports no kafka/chromadb).

Run: bun run test:py
"""

import json
import unittest
from pathlib import Path

import contracts as c

FIXTURES = c.SCHEMA_DIR / "__fixtures__"
CID = "3f2b8c1e-9d4a-4b6f-8a2e-1c5d7e9f0a3b"


def fixture_files():
    for msg_dir in sorted(p for p in FIXTURES.iterdir() if p.is_dir()):
        for kind in ("valid", "invalid"):
            for f in sorted((msg_dir / kind).glob("*.json")):
                yield msg_dir.name, kind, f


def request(**over):
    payload = {"toolName": "getProductInformation", "stepIndex": 0, "input": {"query": "iPhone price"}}
    payload.update(over.pop("payload", {}))
    ev = {"conversationId": CID, "timestamp": 1, "eventType": "ToolInvocationRequested", "payload": payload}
    ev.update(over)
    return ev


class FixtureParity(unittest.TestCase):
    """Every fixture is accepted/rejected by jsonschema exactly as zod does (see schemas.test.ts)."""

    def test_there_are_fixtures_for_every_message(self):
        names = {n for n, _, _ in fixture_files()}
        schemas = {p.stem for p in c.SCHEMA_DIR.glob("*.json")}
        self.assertEqual(names, schemas)

    def test_every_fixture(self):
        count = 0
        for name, kind, f in fixture_files():
            with self.subTest(fixture=f"{name}/{kind}/{f.name}"):
                body = json.loads(f.read_text(encoding="utf-8"))
                ok = c.validator(name).is_valid(body)
                self.assertEqual(ok, kind == "valid", c.issues_of(c.validator(name), body))
            count += 1
        self.assertGreater(count, 40)


class Decode(unittest.TestCase):
    def test_bad_json_is_malformed(self):
        with self.assertRaises(c.MalformedMessage) as cm:
            c.decode(b"{bad")
        self.assertEqual(cm.exception.kind, "malformed-json")

    def test_invalid_utf8_is_malformed(self):
        with self.assertRaises(c.MalformedMessage) as cm:
            c.decode(b"\xff\xfe")
        self.assertEqual(cm.exception.kind, "malformed-json")

    def test_null_value_is_missing(self):
        with self.assertRaises(c.MalformedMessage) as cm:
            c.decode(None)
        self.assertEqual(cm.exception.kind, "missing-value")

    def test_valid_json_is_returned(self):
        self.assertEqual(c.decode(b'{"a": 1}'), {"a": 1})

    def test_message_type_of(self):
        self.assertEqual(c.message_type_of({"eventType": "PlanFailed"}), "PlanFailed")
        self.assertEqual(c.message_type_of({"commandType": "X"}), "X")
        self.assertIsNone(c.message_type_of({}))
        self.assertIsNone(c.message_type_of([1]))
        self.assertIsNone(c.message_type_of(5))


class ValidateRequest(unittest.TestCase):
    def test_valid_request_passes(self):
        c.validate_request(request())

    def test_unknown_fields_are_tolerated(self):
        c.validate_request(request(traceId="x", payload={"future": 1}))

    def test_missing_step_index_reports_its_path(self):
        ev = request()
        del ev["payload"]["stepIndex"]
        with self.assertRaises(c.ContractViolation) as cm:
            c.validate_request(ev)
        self.assertIn("payload.stepIndex", [i["path"] for i in cm.exception.issues])

    def test_unknown_conversation_id_is_rejected(self):
        with self.assertRaises(c.ContractViolation):
            c.validate_request(request(conversationId="unknown"))

    def test_key_check(self):
        c.check_key(CID.encode(), request())
        with self.assertRaises(c.KeyMismatch):
            c.check_key(b"other", request())
        with self.assertRaises(c.KeyMismatch):
            c.check_key(None, request())


class Outbound(unittest.TestCase):
    def test_result_event_validates_against_its_schema(self):
        ev = c.result_event(CID, 1, {"query": "q", "context": "c", "success": True}, "getProductInformation", now_ms=5)
        self.assertTrue(c.validator("ToolInvocationResulted").is_valid(ev))
        self.assertEqual(ev["payload"]["stepIndex"], 1)
        self.assertEqual(ev["timestamp"], 5)

    def test_error_result_is_a_valid_event(self):
        ev = c.result_event(CID, 0, {"error": "boom", "success": False}, "getProductInformation")
        self.assertTrue(c.validator("ToolInvocationResulted").is_valid(ev))

    def test_non_integer_step_index_is_an_outbound_violation(self):
        with self.assertRaises(c.OutboundContractViolation) as cm:
            c.result_event(CID, 1.5, {"success": True}, "getProductInformation")
        self.assertEqual(cm.exception.message_type, "ToolInvocationResulted")
        self.assertEqual(cm.exception.topic, "conversation-events")
        self.assertIn("payload.stepIndex", [i["path"] for i in cm.exception.issues])

    def test_none_step_index_is_an_outbound_violation(self):
        with self.assertRaises(c.OutboundContractViolation):
            c.result_event(CID, None, {"success": True}, "getProductInformation")


class DeadLetter(unittest.TestCase):
    def build(self, exc, value=b"{}", **over):
        kw = dict(
            topic="tool-invocation-requests", partition=2, offset=17, key=CID.encode(),
            timestamp_ms=1700000000123, value=value, group="rag-tool-worker",
            service="rag", exc=exc, now_ms=1000,
        )
        kw.update(over)
        rec = c.dead_letter_record(**kw)
        self.assertTrue(c.validator("MessageDeadLettered").is_valid(rec), c.issues_of(c.validator("MessageDeadLettered"), rec))
        return rec

    def test_malformed_record(self):
        try:
            c.decode(b"{bad")
        except c.MalformedMessage as exc:
            rec = self.build(exc, value=b"{bad")
        self.assertEqual(rec["payload"]["error"]["class"], "malformed-json")
        self.assertIsNone(rec["conversationId"])
        self.assertEqual(rec["payload"]["source"]["offset"], "17")
        self.assertEqual(rec["payload"]["source"]["key"], CID)
        self.assertEqual(rec["payload"]["originalValue"], {"encoding": "utf8", "data": "{bad", "truncated": False})

    def test_null_value_and_null_key(self):
        rec = self.build(c.MalformedMessage("missing-value", "null"), value=None, key=None, timestamp_ms=None)
        self.assertEqual(rec["payload"]["error"]["class"], "missing-value")
        self.assertIsNone(rec["payload"]["originalValue"])
        self.assertIsNone(rec["payload"]["source"]["key"])
        self.assertIsNone(rec["payload"]["source"]["timestamp"])

    def test_contract_violation_record_has_issues_and_conversation_id(self):
        ev = request()
        del ev["payload"]["stepIndex"]
        try:
            c.validate_request(ev)
        except c.ContractViolation as exc:
            rec = self.build(exc, value=json.dumps(ev).encode())
        self.assertEqual(rec["payload"]["error"]["class"], "contract-violation")
        self.assertEqual(rec["conversationId"], CID)
        self.assertIn("payload.stepIndex", [i["path"] for i in rec["payload"]["error"]["issues"]])

    def test_key_mismatch_record(self):
        rec = self.build(c.KeyMismatch("x"), key=b"other")
        self.assertEqual(rec["payload"]["error"]["class"], "key-mismatch")

    def test_outbound_violation_record(self):
        """An invalid result → OutboundContractViolation → DLQ class outbound-contract-violation."""
        valid_input = json.dumps(request()).encode()
        try:
            c.result_event(CID, 1.5, {"success": True}, "getProductInformation")
        except c.OutboundContractViolation as exc:
            rec = self.build(exc, value=valid_input)
        err = rec["payload"]["error"]
        self.assertEqual(err["class"], "outbound-contract-violation")
        self.assertEqual(err["outbound"], {"topic": "conversation-events", "messageType": "ToolInvocationResulted"})
        self.assertIn("payload.stepIndex", [i["path"] for i in err["issues"]])
        self.assertEqual(rec["payload"]["originalValue"]["data"], valid_input.decode())

    def test_non_utf8_value_is_base64(self):
        rec = self.build(c.MalformedMessage("malformed-json", "x"), value=b"\xff\xfe\x00")
        self.assertEqual(rec["payload"]["originalValue"]["encoding"], "base64")

    def test_oversized_value_is_truncated_and_stays_under_1mib(self):
        for value in (b"a" * (c.MAX_ORIGINAL_BYTES + 1000), b"\x01" * (c.MAX_ORIGINAL_BYTES + 10),
                      b"\xff" * (c.MAX_ORIGINAL_BYTES * 2)):
            rec = self.build(c.MalformedMessage("malformed-json", "x"), value=value)
            self.assertTrue(rec["payload"]["originalValue"]["truncated"])
            self.assertLess(len(json.dumps(rec, ensure_ascii=False).encode()), 1024 * 1024)

    def test_non_poison_error_is_rejected(self):
        with self.assertRaises(TypeError):
            c.error_class_of(RuntimeError("x"))


if __name__ == "__main__":
    unittest.main()
