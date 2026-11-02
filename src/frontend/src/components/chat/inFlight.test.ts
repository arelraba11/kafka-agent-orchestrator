import { describe, expect, test } from "bun:test";
import {
  IDLE_TIMEOUT_MS,
  IDLE_TIMEOUT_MESSAGE,
  DISCONNECT_MESSAGE,
  initialInFlight,
  reduceInFlight,
  type InFlightState,
  type ServerFrame,
} from "./inFlight";

const CID = "3f2b8c1e-9d4a-4b6f-8a2e-1c5d7e9f0a3b";
const OTHER = "aaaaaaaa-9d4a-4b6f-8a2e-1c5d7e9f0a3b";
const BUBBLE = "bubble-1";

const ack = (conversationId = CID): ServerFrame => ({ type: "ack", conversationId });
const tool = (toolName: string, conversationId = CID): ServerFrame => ({ type: "tool", toolName, conversationId });
const answer = (text: string, conversationId = CID): ServerFrame => ({ type: "answer", answer: text, conversationId });
const error = (reason: string, conversationId = CID): ServerFrame => ({
  type: "error",
  reason,
  conversationId,
  code: "tool-error",
});

function sent(now = 0): InFlightState {
  return reduceInFlight(initialInFlight, { type: "send", bubbleId: BUBBLE, now }).state;
}

describe("happy path", () => {
  test("send → ack → tool → answer resolves the bubble created on send", () => {
    let r = reduceInFlight(initialInFlight, { type: "send", bubbleId: BUBBLE, now: 0 });
    expect(r.resolution).toBeNull();
    expect(r.state.active?.bubbleId).toBe(BUBBLE);
    expect(r.state.active?.conversationId).toBeNull();

    r = reduceInFlight(r.state, { type: "frame", frame: ack(), now: 100 });
    expect(r.state.active?.conversationId).toBe(CID);
    expect(r.resolution).toBeNull();

    r = reduceInFlight(r.state, { type: "frame", frame: tool("math"), now: 200 });
    expect(r.state.active?.tools).toEqual(["math"]);
    r = reduceInFlight(r.state, { type: "frame", frame: tool("math"), now: 210 });
    expect(r.state.active?.tools).toEqual(["math"]);

    r = reduceInFlight(r.state, { type: "frame", frame: answer("108"), now: 300 });
    expect(r.resolution).toEqual({ bubbleId: BUBBLE, role: "bot", content: "108" });
    expect(r.state.active).toBeNull();
  });

  test("an error for the bound conversation resolves it as an error bubble", () => {
    const s = reduceInFlight(sent(), { type: "frame", frame: ack(), now: 1 }).state;
    const r = reduceInFlight(s, { type: "frame", frame: error("The math tool failed: Division by zero"), now: 2 });
    expect(r.resolution).toEqual({
      bubbleId: BUBBLE,
      role: "error",
      content: "The math tool failed: Division by zero",
    });
    expect(r.state.active).toBeNull();
  });

  test("a second send while one is in flight is ignored", () => {
    const s = sent();
    const r = reduceInFlight(s, { type: "send", bubbleId: "bubble-2", now: 5 });
    expect(r.state).toBe(s);
    expect(r.resolution).toBeNull();
  });
});

describe("error before ack", () => {
  test("an error arriving before any ack resolves the unbound request", () => {
    const r = reduceInFlight(sent(), {
      type: "frame",
      frame: { type: "error", reason: "The service is unavailable right now. Try again.", conversationId: OTHER, code: "unavailable" },
      now: 1,
    });
    expect(r.resolution).toEqual({
      bubbleId: BUBBLE,
      role: "error",
      content: "The service is unavailable right now. Try again.",
    });
    expect(r.state.active).toBeNull();
  });

  test("tool and answer frames before any ack are ignored", () => {
    const s = sent();
    expect(reduceInFlight(s, { type: "frame", frame: tool("math"), now: 1 }).state).toBe(s);
    const r = reduceInFlight(s, { type: "frame", frame: answer("late"), now: 1 });
    expect(r.state).toBe(s);
    expect(r.resolution).toBeNull();
  });
});

describe("conversationId matching", () => {
  test("frames for another conversationId are ignored once bound", () => {
    const s = reduceInFlight(sent(), { type: "frame", frame: ack(), now: 1 }).state;
    for (const frame of [answer("x", OTHER), error("boom", OTHER), tool("math", OTHER), ack(OTHER)]) {
      const r = reduceInFlight(s, { type: "frame", frame, now: 2 });
      expect(r.state).toBe(s);
      expect(r.resolution).toBeNull();
    }
  });

  test("a frame with no request in flight is ignored", () => {
    const r = reduceInFlight(initialInFlight, { type: "frame", frame: answer("x"), now: 1 });
    expect(r.state).toBe(initialInFlight);
    expect(r.resolution).toBeNull();
  });
});

describe("idle timeout", () => {
  test("is 90 s", () => {
    expect(IDLE_TIMEOUT_MS).toBe(90_000);
  });

  test("resets on ack and on tool, and fires 90 s after the last frame", () => {
    let s = sent(0);
    expect(s.active?.deadline).toBe(90_000);

    s = reduceInFlight(s, { type: "frame", frame: ack(), now: 10_000 }).state;
    expect(s.active?.deadline).toBe(100_000);

    s = reduceInFlight(s, { type: "frame", frame: tool("math"), now: 80_000 }).state;
    expect(s.active?.deadline).toBe(170_000);

    // The original deadline (90 s after send, 100 s after ack) has passed: no timeout.
    expect(reduceInFlight(s, { type: "tick", now: 169_999 }).resolution).toBeNull();

    const r = reduceInFlight(s, { type: "tick", now: 170_000 });
    expect(r.resolution).toEqual({ bubbleId: BUBBLE, role: "error", content: IDLE_TIMEOUT_MESSAGE });
    expect(r.state.active).toBeNull();
  });

  test("the message says 90 s and that the request may still finish", () => {
    expect(IDLE_TIMEOUT_MESSAGE).toBe("No response after 90 s. The request may still finish; try again.");
  });

  test("a tick with nothing in flight does nothing", () => {
    const r = reduceInFlight(initialInFlight, { type: "tick", now: 1e9 });
    expect(r.state).toBe(initialInFlight);
    expect(r.resolution).toBeNull();
  });

  test("a late answer after the timeout is ignored", () => {
    let s = reduceInFlight(sent(0), { type: "frame", frame: ack(), now: 1 }).state;
    s = reduceInFlight(s, { type: "tick", now: 100_000 }).state;
    const r = reduceInFlight(s, { type: "frame", frame: answer("too late"), now: 100_001 });
    expect(r.resolution).toBeNull();
    expect(r.state.active).toBeNull();
  });

  test("a late answer for the timed-out request does not resolve the next one", () => {
    let s = reduceInFlight(sent(0), { type: "frame", frame: ack(), now: 1 }).state;
    s = reduceInFlight(s, { type: "tick", now: 100_000 }).state;
    s = reduceInFlight(s, { type: "send", bubbleId: "bubble-2", now: 100_010 }).state;
    s = reduceInFlight(s, { type: "frame", frame: ack(OTHER), now: 100_020 }).state;
    const r = reduceInFlight(s, { type: "frame", frame: answer("old answer", CID), now: 100_030 });
    expect(r.resolution).toBeNull();
    expect(r.state.active?.bubbleId).toBe("bubble-2");
  });
});

describe("disconnect", () => {
  test("fails the in-flight request, bound or not", () => {
    for (const s of [sent(), reduceInFlight(sent(), { type: "frame", frame: ack(), now: 1 }).state]) {
      const r = reduceInFlight(s, { type: "disconnect" });
      expect(r.resolution).toEqual({ bubbleId: BUBBLE, role: "error", content: DISCONNECT_MESSAGE });
      expect(r.state.active).toBeNull();
    }
    expect(DISCONNECT_MESSAGE).toBe("Connection lost while waiting for the answer.");
  });

  test("with nothing in flight it does nothing", () => {
    const r = reduceInFlight(initialInFlight, { type: "disconnect" });
    expect(r.state).toBe(initialInFlight);
    expect(r.resolution).toBeNull();
  });

  test("an answer after the disconnect is ignored", () => {
    let s = reduceInFlight(sent(), { type: "frame", frame: ack(), now: 1 }).state;
    s = reduceInFlight(s, { type: "disconnect" }).state;
    expect(reduceInFlight(s, { type: "frame", frame: answer("x"), now: 2 }).resolution).toBeNull();
  });
});
