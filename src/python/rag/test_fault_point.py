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
