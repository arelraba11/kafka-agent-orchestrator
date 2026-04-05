import { useEffect, useRef } from "react";
import ReactMarkdown from "react-markdown";
import { TypingIndicator } from "./TypingIndicator";

export type Message = {
  id: string;
  role: "user" | "bot" | "error";
  content: string;
  pending?: boolean;
};

export function ChatMessages({ messages }: { messages: Message[] }) {
  const bottomRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  return (
    <div className="flex flex-col gap-3 p-4 overflow-y-auto flex-1">
      {messages.map((msg) => {
        const isUser = msg.role === "user";
        const isError = msg.role === "error";

        return (
          <div
            key={msg.id}
            className={`flex ${isUser ? "justify-end" : "justify-start"}`}
          >
            <div
              className={`max-w-[75%] px-4 py-3 rounded-2xl text-sm leading-relaxed ${
                isUser
                  ? "bg-[var(--primary)] text-[var(--primary-foreground)]"
                  : isError
                  ? "bg-[var(--destructive)] text-white"
                  : "bg-[var(--muted)] text-[var(--foreground)]"
              }`}
            >
              {msg.pending ? (
                <TypingIndicator />
              ) : (
                <ReactMarkdown>{msg.content}</ReactMarkdown>
              )}
            </div>
          </div>
        );
      })}
      <div ref={bottomRef} />
    </div>
  );
}
