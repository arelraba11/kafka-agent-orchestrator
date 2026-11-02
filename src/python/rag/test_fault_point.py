"""
Tests for fault_point.py (stdlib unittest; imports no kafka/chromadb).

Run: bun run test:py
"""

import os
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

import fault_point as f

HERE = Path(__file__).resolve().parent


def hits(d, point):
    p = Path(d) / f"{point}.hits"
    return p.read_text().strip().split("\n") if p.exists() else []


class ParseSpec(unittest.TestCase):
    def test_bare_point(self):
        self.assertEqual(f.parse_spec("rag:before-send"), ("rag:before-send", None))

    def test_point_at_decision(self):
        self.assertEqual(f.parse_spec("rag:before-send@x"), ("rag:before-send", "x"))

    def test_unset_or_empty(self):
        self.assertIsNone(f.parse_spec(None))
        self.assertIsNone(f.parse_spec(""))


class ParseNth(unittest.TestCase):
    def test_no_selector(self):
        self.assertEqual(f.parse_nth("rag:before-send"), ("rag:before-send", None))
        self.assertEqual(f.parse_nth("rag:before-send@x"), ("rag:before-send@x", None))
        self.assertEqual(f.parse_nth(None), (None, None))

    def test_nth_call_selector(self):
        self.assertEqual(f.parse_nth("rag:before-send#2"), ("rag:before-send", 2))
        self.assertEqual(f.parse_nth("rag:before-send@x#3"), ("rag:before-send@x", 3))

    def test_garbage_is_rejected(self):
        for bad in ("rag:before-send#0", "rag:before-send#x", "rag:before-send#"):
            with self.assertRaises(ValueError):
                f.parse_nth(bad)


class NoOpWithoutDir(unittest.TestCase):
    def test_no_files_and_no_crash(self):
        with tempfile.TemporaryDirectory() as d:
            fp = f.FaultPoints({"FAULT_CRASH": "rag:after-send"})
            fp.hit("rag:after-send")
            self.assertEqual(os.listdir(d), [])


class Count(unittest.TestCase):
    def test_every_call_appends_a_line(self):
        with tempfile.TemporaryDirectory() as d:
            fp = f.FaultPoints({"FAULT_DIR": d})
            fp.hit("rag:before-send")
            fp.hit("rag:before-send")
            self.assertEqual(len(hits(d, "rag:before-send")), 2)


class Crash(unittest.TestCase):
    def run_child(self, d, crash):
        code = (
            f"import sys; sys.path.insert(0, {str(HERE)!r}); import fault_point; "
            "fault_point.fault_point('rag:after-send'); print('survived')"
        )
        env = {"PATH": os.environ.get("PATH", ""), "FAULT_DIR": d, "FAULT_CRASH": crash}
        return subprocess.run([sys.executable, "-c", code], env=env, capture_output=True, text=True)

    def test_sigkills_at_the_named_point(self):
        with tempfile.TemporaryDirectory() as d:
            r = self.run_child(d, "rag:after-send")
            self.assertEqual(r.returncode, -9)
            self.assertNotIn("survived", r.stdout)
            self.assertEqual(len(hits(d, "rag:after-send")), 1)

    def test_other_point_survives(self):
        with tempfile.TemporaryDirectory() as d:
            r = self.run_child(d, "rag:before-send")
            self.assertEqual(r.returncode, 0)
            self.assertIn("survived", r.stdout)


class NthCall(unittest.TestCase):
    def test_crash_only_on_the_nth_call(self):
        code = (
            f"import sys; sys.path.insert(0, {str(HERE)!r}); import fault_point; "
            "[fault_point.fault_point('rag:after-send') or print('call', i, flush=True) for i in (1, 2, 3)]"
        )
        with tempfile.TemporaryDirectory() as d:
            env = {"PATH": os.environ.get("PATH", ""), "FAULT_DIR": d, "FAULT_CRASH": "rag:after-send#2"}
            r = subprocess.run([sys.executable, "-c", code], env=env, capture_output=True, text=True)
            self.assertEqual(r.returncode, -9)
            self.assertIn("call 1", r.stdout)
            self.assertNotIn("call 2", r.stdout)
            self.assertEqual(len(hits(d, "rag:after-send")), 2)

    def test_hold_skips_the_first_call_and_holds_the_second(self):
        with tempfile.TemporaryDirectory() as d:
            fp = f.FaultPoints({"FAULT_DIR": d, "FAULT_HOLD": "rag:before-send#2"})
            reached = Path(d) / "rag:before-send.reached"
            fp.hit("rag:before-send")  # call 1 passes straight through
            self.assertFalse(reached.exists())

            done = threading.Event()

            def run():
                fp.hit("rag:before-send")  # call 2 holds
                done.set()

            t = threading.Thread(target=run)
            t.start()
            deadline = time.time() + 2
            while not reached.exists() and time.time() < deadline:
                time.sleep(0.01)
            self.assertTrue(reached.exists())
            self.assertFalse(done.is_set())
            (Path(d) / "rag:before-send.release").write_text("")
            t.join(2)
            self.assertTrue(done.is_set())
            fp.hit("rag:before-send")  # call 3 passes straight through
            self.assertEqual(len(hits(d, "rag:before-send")), 3)


class Hold(unittest.TestCase):
    def test_waits_for_release_and_only_first_call_holds(self):
        with tempfile.TemporaryDirectory() as d:
            fp = f.FaultPoints({"FAULT_DIR": d, "FAULT_HOLD": "rag:before-send"})
            done = threading.Event()

            def run():
                fp.hit("rag:before-send")
                done.set()

            t = threading.Thread(target=run)
            t.start()
            deadline = time.time() + 2
            while not (Path(d) / "rag:before-send.reached").exists() and time.time() < deadline:
                time.sleep(0.01)
            self.assertTrue((Path(d) / "rag:before-send.reached").exists())
            self.assertFalse(done.is_set())
            (Path(d) / "rag:before-send.release").write_text("")
            t.join(2)
            self.assertTrue(done.is_set())
            fp.hit("rag:before-send")  # second call passes straight through


if __name__ == "__main__":
    unittest.main()
