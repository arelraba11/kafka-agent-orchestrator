"""
Tests for kafka_config.py (stdlib unittest; imports no kafka/chromadb).

Run: bun run test:py
"""

import unittest

import kafka_config as k

TOPICS = ["user-commands", "conversation-events", "tool-invocation-requests", "dead-letter-queue"]


class Brokers(unittest.TestCase):
    def test_default(self):
        self.assertEqual(k.brokers({}), ["localhost:9092"])

    def test_kafka_brokers_is_split_on_commas(self):
        self.assertEqual(k.brokers({"KAFKA_BROKERS": "a:1,b:2"}), ["a:1", "b:2"])
        self.assertEqual(k.brokers({"KAFKA_BROKERS": " a:1 , b:2 "}), ["a:1", "b:2"])

    def test_legacy_kafka_broker_is_only_a_fallback(self):
        self.assertEqual(k.brokers({"KAFKA_BROKER": "x:9"}), ["x:9"])
        self.assertEqual(k.brokers({"KAFKA_BROKER": "x:9", "KAFKA_BROKERS": "a:1"}), ["a:1"])

    def test_empty_kafka_brokers_falls_through(self):
        self.assertEqual(k.brokers({"KAFKA_BROKERS": ""}), ["localhost:9092"])


class Naming(unittest.TestCase):
    def test_namespace_default_is_empty(self):
        self.assertEqual(k.namespace({}), "")
        self.assertEqual(k.namespace({"KAFKA_NAMESPACE": "r-f-"}), "r-f-")

    def test_physical_and_logical_round_trip(self):
        for t in TOPICS:
            self.assertEqual(k.physical_topic("", t), t)
            p = k.physical_topic("r-f-", t)
            self.assertEqual(p, "r-f-" + t)
            self.assertEqual(k.logical_topic("r-f-", p), t)

    def test_logical_topic_outside_namespace_is_none(self):
        self.assertIsNone(k.logical_topic("ns-", "conversation-events"))
        self.assertIsNone(k.logical_topic("ns-", "ns-nope"))

    def test_physical_group(self):
        self.assertEqual(k.physical_group("ns-", "rag-tool-worker"), "ns-rag-tool-worker")
        self.assertEqual(k.physical_group("", "rag-tool-worker"), "rag-tool-worker")


class Knobs(unittest.TestCase):
    def test_offset_reset_default_and_values(self):
        self.assertEqual(k.offset_reset({}), "latest")
        self.assertEqual(k.offset_reset({"KAFKA_OFFSET_RESET": "earliest"}), "earliest")
        with self.assertRaises(ValueError):
            k.offset_reset({"KAFKA_OFFSET_RESET": "oldest"})

    def test_consumer_kwargs_default_to_library_defaults(self):
        self.assertEqual(k.consumer_kwargs({}), {})

    def test_consumer_kwargs_map_to_kafka_python_names(self):
        env = {
            "KAFKA_SESSION_TIMEOUT_MS": "3000",
            "KAFKA_HEARTBEAT_INTERVAL_MS": "500",
            "KAFKA_REBALANCE_TIMEOUT_MS": "5000",
        }
        self.assertEqual(
            k.consumer_kwargs(env),
            {"session_timeout_ms": 3000, "heartbeat_interval_ms": 500, "max_poll_interval_ms": 5000},
        )

    def test_producer_kwargs_default_and_override(self):
        self.assertEqual(k.producer_kwargs({}), {})
        self.assertEqual(
            k.producer_kwargs({"KAFKA_RETRIES": "1", "KAFKA_REQUEST_TIMEOUT_MS": "2000"}),
            {"retries": 1, "request_timeout_ms": 2000},
        )

    def test_send_timeout_default_is_10_seconds(self):
        self.assertEqual(k.send_timeout_s({}), 10)
        self.assertEqual(k.send_timeout_s({"RAG_SEND_TIMEOUT_S": "3"}), 3)

    def test_garbage_is_rejected_with_the_variable_name(self):
        with self.assertRaises(ValueError) as cm:
            k.send_timeout_s({"RAG_SEND_TIMEOUT_S": "soon"})
        self.assertIn("RAG_SEND_TIMEOUT_S", str(cm.exception))


if __name__ == "__main__":
    unittest.main()
