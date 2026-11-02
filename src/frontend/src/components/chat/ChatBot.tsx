import { useState, useRef, useEffect, useCallback } from "react";
import type { Message } from "./MessageBubble";
import ChatHeader from "./ChatHeader";
import ChatMessages from "./ChatMessages";
import ChatInput from "./ChatInput";
import TracePanel from "./TracePanel";
import type { TraceState } from "./TracePanel";
import {
  initialInFlight,
  reduceInFlight,
  type InFlightAction,
  type InFlightState,
  type ServerFrame,
} from "./inFlight";

type Status = "connecting" | "connected" | "disconnected";

export default function ChatBot() {
  const [messages, setMessages] = useState<Message[]>([]);
  const [status, setStatus] = useState<Status>("connecting");
  const [typingPhase, setTypingPhase] = useState<"thinking" | "processing" | null>(null);
  const [lastUserMessage, setLastUserMessage] = useState("");
  const [prefill, setPrefill] = useState("");
  const [traceState, setTraceState] = useState<TraceState>({
    status: "idle",
    ackTime: null,
    answerTime: null,
    toolsUsed: [],
  });
  const wsRef = useRef<WebSocket | null>(null);
  // The single in-flight request of this tab; all transitions go through reduceInFlight.
  const inFlightRef = useRef<InFlightState>(initialInFlight);
  const typingTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const idleTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const dispatch = useCallback((action: InFlightAction) => {
    const prev = inFlightRef.current;
    const { state, resolution } = reduceInFlight(prev, action);
    inFlightRef.current = state;

    // Idle timer: one timeout armed for the current deadline, fed back as a tick.
    if (idleTimerRef.current) clearTimeout(idleTimerRef.current);
    idleTimerRef.current = null;
    if (state.active) {
      idleTimerRef.current = setTimeout(
        () => dispatch({ type: "tick", now: Date.now() }),
        Math.max(0, state.active.deadline - Date.now())
      );
    }

    if (state.active && state.active.tools !== prev.active?.tools) {
      const toolsUsed = state.active.tools;
      setTraceState((s) => ({ ...s, toolsUsed }));
    }

    if (resolution) {
      if (typingTimerRef.current) clearTimeout(typingTimerRef.current);
      setTypingPhase(null);
      setTraceState((s) => ({ ...s, status: "done", answerTime: Date.now() }));
      setMessages((msgs) =>
        msgs.map((m) =>
          m.id === resolution.bubbleId
            ? { ...m, role: resolution.role, content: resolution.content, pending: false }
            : m
        )
      );
    }
  }, []);

  const connect = useCallback(() => {
    const ws = new WebSocket(
      window.location.hostname === "localhost"
        ? "ws://localhost:3001/ws"
        : `ws://${window.location.host}/ws`
    );
    wsRef.current = ws;

    ws.onopen = () => setStatus("connected");
    ws.onclose = () => {
      setStatus("disconnected");
      dispatch({ type: "disconnect" });
      setTimeout(connect, 3000);
    };
    ws.onerror = () => ws.close();

    ws.onmessage = (event) => {
      const frame = JSON.parse(event.data) as ServerFrame;
      const before = inFlightRef.current.active;
      dispatch({ type: "frame", frame, now: Date.now() });
      const after = inFlightRef.current.active;

      // The ack bound the request to its conversationId: the trace starts counting.
      if (frame.type === "ack" && before && !before.conversationId && after?.conversationId) {
        setTraceState((s) => ({ ...s, ackTime: Date.now() }));
      }
    };
  }, [dispatch]);

  useEffect(() => {
    connect();
    return () => {
      wsRef.current?.close();
      if (typingTimerRef.current) clearTimeout(typingTimerRef.current);
      if (idleTimerRef.current) clearTimeout(idleTimerRef.current);
    };
  }, [connect]);

  const sendMessage = useCallback(
    (text: string) => {
      if (!wsRef.current || wsRef.current.readyState !== WebSocket.OPEN) return;
      if (inFlightRef.current.active) return; // one in-flight request per tab
      setLastUserMessage(text);
      // The bot bubble exists from the moment of sending; the ack only binds its conversationId.
      const bubbleId = crypto.randomUUID();
      const now = Date.now();
      setMessages((prev) => [
        ...prev,
        { id: crypto.randomUUID(), role: "user", content: text, timestamp: now },
        { id: bubbleId, role: "bot", content: "", pending: true, timestamp: now },
      ]);
      setTypingPhase("thinking");
      setTraceState({ status: "thinking", ackTime: null, answerTime: null, toolsUsed: [] });
      if (typingTimerRef.current) clearTimeout(typingTimerRef.current);
      typingTimerRef.current = setTimeout(() => {
        setTypingPhase("processing");
        setTraceState((s) => ({ ...s, status: "processing" }));
      }, 2000);
      dispatch({ type: "send", bubbleId, now });
      wsRef.current.send(text);
    },
    [dispatch]
  );

  const handleRetry = useCallback(() => {
    if (lastUserMessage) sendMessage(lastUserMessage);
  }, [lastUserMessage, sendMessage]);

  const isLoading = messages.some((m) => m.pending);

  return (
    <div className="flex flex-row h-screen w-full overflow-hidden">
      <div className="flex flex-col flex-1 min-w-0 bg-(--surface) bg-grid">
        <ChatHeader status={status} />
        <ChatMessages
          messages={messages}
          onSuggestion={setPrefill}
          onRetry={handleRetry}
          typingPhase={typingPhase}
        />
        <ChatInput
          onSend={sendMessage}
          disabled={status !== "connected"}
          isLoading={isLoading}
          prefill={prefill}
          onPrefillConsumed={() => setPrefill("")}
        />
      </div>
      <TracePanel trace={traceState} />
    </div>
  );
}
