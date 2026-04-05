import { useEffect, useRef, useState, useCallback } from "react";
import { ChatMessages, type Message } from "./ChatMessages";
import { ChatInput } from "./ChatInput";
import { ConnectionStatus, type Status } from "./ConnectionStatus";

const WS_URL = "ws://localhost:3001/ws";
const RECONNECT_DELAY_MS = 3000;

export function ChatBot() {
  const [messages, setMessages] = useState<Message[]>([]);
  const [status, setStatus] = useState<Status>("connecting");
  const wsRef = useRef<WebSocket | null>(null);
  // Map<conversationId, pendingMessageId>
  const pendingRef = useRef<Map<string, string>>(new Map());

  const connect = useCallback(() => {
    setStatus("connecting");
    const ws = new WebSocket(WS_URL);
    wsRef.current = ws;

    ws.onopen = () => setStatus("connected");

    ws.onmessage = (event) => {
      const data = JSON.parse(event.data as string) as
        | { type: "ack"; conversationId: string }
        | { type: "answer"; answer: string }
        | { type: "error"; reason: string; failedTool?: string };

      if (data.type === "ack") {
        const pendingId = crypto.randomUUID();
        pendingRef.current.set(data.conversationId, pendingId);
        setMessages((prev) => [
          ...prev,
          { id: pendingId, role: "bot", content: "", pending: true },
        ]);
        return;
      }

      if (data.type === "answer" || data.type === "error") {
        const content =
          data.type === "answer"
            ? data.answer
            : `Error: ${data.reason}${data.failedTool ? ` (tool: ${data.failedTool})` : ""}`;
        const role = data.type === "answer" ? "bot" : "error";

        // Find and replace the oldest pending bubble
        setMessages((prev) => {
          let replaced = false;
          return prev.map((m) => {
            if (!replaced && m.pending) {
              // Remove the matching conversationId from the map
              for (const [cid, id] of pendingRef.current.entries()) {
                if (id === m.id) {
                  pendingRef.current.delete(cid);
                  break;
                }
              }
              replaced = true;
              return { ...m, role, content, pending: false };
            }
            return m;
          });
        });
      }
    };

    ws.onclose = () => {
      setStatus("disconnected");
      setTimeout(connect, RECONNECT_DELAY_MS);
    };
  }, []);

  useEffect(() => {
    connect();
    return () => wsRef.current?.close();
  }, [connect]);

  const sendMessage = (text: string) => {
    if (wsRef.current?.readyState !== WebSocket.OPEN) return;
    setMessages((prev) => [
      ...prev,
      { id: crypto.randomUUID(), role: "user", content: text },
    ]);
    wsRef.current.send(text);
  };

  return (
    <div className="flex flex-col h-screen max-w-2xl mx-auto">
      <header className="flex items-center justify-between px-4 py-3 border-b border-[var(--border)]">
        <h1 className="font-semibold text-lg">Kafka AI Agent</h1>
        <ConnectionStatus status={status} />
      </header>
      <ChatMessages messages={messages} />
      <ChatInput onSend={sendMessage} disabled={status !== "connected"} />
    </div>
  );
}
