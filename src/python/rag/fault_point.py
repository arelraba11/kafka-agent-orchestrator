"""
fault_point.py — env-gated fault injection for the integration suite (stdlib only).

Python twin of shared/testing/faultPoint.ts. Without FAULT_DIR, fault_point() returns
immediately.

  FAULT_DIR                          enables everything; per-point files live here
  FAULT_CRASH=<point>[@<decision>]   SIGKILL this process when the point is reached
  FAULT_HOLD=<point>[@<decision>]    on the FIRST matching call: write <point>.reached,
                                     then wait for <point>.release (polled every 20 ms)

Every call appends one line to <FAULT_DIR>/<point>.hits.
Points used here: "rag:before-send", "rag:after-send".
"""

import os
import signal
import time
from pathlib import Path
from typing import Mapping, Optional, Tuple

POLL_S = 0.02


def parse_spec(raw: Optional[str]) -> Optional[Tuple[str, Optional[str]]]:
    if not raw:
        return None
    point, sep, decision = raw.partition("@")
    return (point, decision if sep else None)


def _matches(spec, name: str, decision: Optional[str]) -> bool:
    if spec is None or spec[0] != name:
        return False
    return spec[1] is None or spec[1] == decision


class FaultPoints:
    def __init__(self, env: Mapping[str, str]):
        self.dir = env.get("FAULT_DIR")
        self.crash = parse_spec(env.get("FAULT_CRASH"))
        self.hold = parse_spec(env.get("FAULT_HOLD"))
        self.held = False

    def hit(self, name: str, decision: Optional[str] = None) -> None:
        if not self.dir:
            return
        base = Path(self.dir)
        with open(base / f"{name}.hits", "a", encoding="utf-8") as fh:
            fh.write(f"{decision or '-'}\n")
            fh.flush()
            os.fsync(fh.fileno())

        if _matches(self.crash, name, decision):
            os.kill(os.getpid(), signal.SIGKILL)
            time.sleep(60)  # SIGKILL is not catchable; never run the caller's next line

        if not self.held and _matches(self.hold, name, decision):
            self.held = True
            (base / f"{name}.reached").write_text("")
            release = base / f"{name}.release"
            while not release.exists():
                time.sleep(POLL_S)


fault_point = FaultPoints(os.environ).hit
