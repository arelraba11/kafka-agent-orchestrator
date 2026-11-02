// Pure reducer for the one request a tab can have in flight. Imports nothing from React or
// the DOM so it is unit-tested directly (inFlight.test.ts).
//
// The bubble is created on send. The server's `ack` binds the request to its
// conversationId; after that only frames for that id resolve it. An `error` that arrives
// before any ack resolves the unbound request (the server could not publish the query).
// The idle timeout is a deadline: 90 s without a frame for the in-flight request, reset on
// ack and tool. The component arms a timer for the deadline and feeds back a `tick`.

export const IDLE_TIMEOUT_MS = 90_000;
export const IDLE_TIMEOUT_MESSAGE = "No response after 90 s. The request may still finish; try again.";
export const DISCONNECT_MESSAGE = "Connection lost while waiting for the answer.";

export type ServerFrame =
  | { type: "ack"; conversationId: string }
  | { type: "tool"; toolName: string; conversationId: string }
  | { type: "answer"; answer: string; conversationId: string }
  | { type: "error"; reason?: string; conversationId: string; code?: string; failedTool?: string };

export interface ActiveRequest {
  bubbleId: string;
  /** null until the server's ack binds it. */
  conversationId: string | null;
  /** Epoch ms after which the request is failed for being idle. */
  deadline: number;
  tools: string[];
}

export interface InFlightState {
  active: ActiveRequest | null;
}

export type InFlightAction =
  | { type: "send"; bubbleId: string; now: number }
  | { type: "frame"; frame: ServerFrame; now: number }
  | { type: "tick"; now: number }
  | { type: "disconnect" };

export interface Resolution {
  bubbleId: string;
  role: "bot" | "error";
  content: string;
}

export interface InFlightResult {
  state: InFlightState;
  resolution: Resolution | null;
}

export const initialInFlight: InFlightState = { active: null };

const unchanged = (state: InFlightState): InFlightResult => ({ state, resolution: null });

const fail = (active: ActiveRequest, content: string): InFlightResult => ({
  state: initialInFlight,
  resolution: { bubbleId: active.bubbleId, role: "error", content },
});

export function reduceInFlight(state: InFlightState, action: InFlightAction): InFlightResult {
  const { active } = state;

  switch (action.type) {
    case "send":
      if (active) return unchanged(state); // one in-flight request per tab
      return {
        state: {
          active: {
            bubbleId: action.bubbleId,
            conversationId: null,
            deadline: action.now + IDLE_TIMEOUT_MS,
            tools: [],
          },
        },
        resolution: null,
      };

    case "tick":
      if (!active || action.now < active.deadline) return unchanged(state);
      return fail(active, IDLE_TIMEOUT_MESSAGE);

    case "disconnect":
      if (!active) return unchanged(state);
      return fail(active, DISCONNECT_MESSAGE);

    case "frame": {
      if (!active) return unchanged(state);
      const { frame, now } = action;

      if (frame.type === "ack") {
        if (active.conversationId !== null) return unchanged(state);
        return {
          state: {
            active: { ...active, conversationId: frame.conversationId, deadline: now + IDLE_TIMEOUT_MS },
          },
          resolution: null,
        };
      }

      if (frame.type === "error" && active.conversationId === null) {
        return fail(active, frame.reason ?? "Something went wrong");
      }

      if (active.conversationId === null || frame.conversationId !== active.conversationId) {
        return unchanged(state);
      }

      switch (frame.type) {
        case "tool":
          return {
            state: {
              active: {
                ...active,
                deadline: now + IDLE_TIMEOUT_MS,
                tools: active.tools.includes(frame.toolName) ? active.tools : [...active.tools, frame.toolName],
              },
            },
            resolution: null,
          };
        case "answer":
          return {
            state: initialInFlight,
            resolution: { bubbleId: active.bubbleId, role: "bot", content: frame.answer },
          };
        case "error":
          return fail(active, frame.reason ?? "Something went wrong");
      }
    }
  }
}
