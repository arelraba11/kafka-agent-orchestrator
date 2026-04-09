import { useState, useRef, useEffect, useCallback } from "react";
import type { Message } from "./MessageBubble";
import ChatHeader from "./ChatHeader";
import ChatMessages from "./ChatMessages";
import ChatInput from "./ChatInput";
import TracePanel from "./TracePanel";
import type { TraceState } from "./TracePanel";
import SuggestionChips from "./SuggestionChips";

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
  const pendingRef = useRef<Map<string, string>>(new Map());
  const typingTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

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
      setTimeout(connect, 3000);
    };
    ws.onerror = () => ws.close();

    ws.onmessage = (event) => {
      const data = JSON.parse(event.data);

      if (data.type === "ack") {
        const pendingId = crypto.randomUUID();
        pendingRef.current.set(data.conversationId, pendingId);
        setTypingPhase("thinking");
        setTraceState({ status: "thinking", ackTime: Date.now(), answerTime: null, toolsUsed: [] });
        if (typingTimerRef.current) clearTimeout(typingTimerRef.current);
        typingTimerRef.current = setTimeout(() => {
          setTypingPhase("processing");
          setTraceState((s) => ({ ...s, status: "processing" }));
        }, 2000);
        setMessages((prev) => [
          ...prev,
          { id: pendingId, role: "bot", content: "", pending: true, timestamp: Date.now() },
        ]);
        return;
      }

      if (data.type === "tool") {
        setTraceState((s) => ({
          ...s,
          toolsUsed: s.toolsUsed.includes(data.toolName)
            ? s.toolsUsed
            : [...s.toolsUsed, data.toolName],
        }));
        return;
      }

      if (data.type === "answer" || data.type === "error") {
        if (typingTimerRef.current) clearTimeout(typingTimerRef.current);
        setTypingPhase(null);
        setTraceState((s) => ({ ...s, status: "done", answerTime: Date.now() }));
        const pendingId = [...pendingRef.current.values()][0];
        pendingRef.current.clear();
        setMessages((prev) =>
          prev.map((m) =>
            m.id === pendingId
              ? {
                  ...m,
                  role: data.type === "error" ? "error" : "bot",
                  content:
                    data.type === "error"
                      ? data.reason ?? "Something went wrong"
                      : data.answer,
                  pending: false,
                }
              : m
          )
        );
      }
    };
  }, []);

  useEffect(() => {
    connect();
    return () => {
      wsRef.current?.close();
      if (typingTimerRef.current) clearTimeout(typingTimerRef.current);
    };
  }, [connect]);

  const sendMessage = useCallback(
    (text: string) => {
      if (!wsRef.current || wsRef.current.readyState !== WebSocket.OPEN) return;
      setLastUserMessage(text);
      setMessages((prev) => [
        ...prev,
        { id: crypto.randomUUID(), role: "user", content: text, timestamp: Date.now() },
      ]);
      wsRef.current.send(text);
    },
    []
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
