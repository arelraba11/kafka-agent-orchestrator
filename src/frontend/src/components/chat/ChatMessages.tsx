import { useEffect, useRef } from "react";
import type { Message } from "./MessageBubble";
import MessageBubble from "./MessageBubble";
import EmptyState from "./EmptyState";

interface ChatMessagesProps {
  messages: Message[];
  onSuggestion: (text: string) => void;
  onRetry: () => void;
  typingPhase: "thinking" | "processing" | null;
}

export type { Message };

export default function ChatMessages({
  messages,
  onSuggestion,
  onRetry,
  typingPhase,
}: ChatMessagesProps) {
  const bottomRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  if (messages.length === 0) {
    return <EmptyState onSuggestion={onSuggestion} />;
  }

  return (
    <div
      role="log"
      aria-live="polite"
      aria-label="Conversation"
      className="flex-1 overflow-y-auto flex flex-col gap-1 px-4 py-4 bg-(--surface)"
    >
      {messages.map((msg, index) => {
        const prev = messages[index - 1];
        const next = messages[index + 1];
        const isFirstInGroup = !prev || prev.role !== msg.role;
        const isLastInGroup = !next || next.role !== msg.role;

        return (
          <div
            key={msg.id}
            className={isFirstInGroup ? "mt-3 first:mt-0" : "mt-0.5"}
          >
            <MessageBubble
              message={msg}
              isFirstInGroup={isFirstInGroup}
              isLastInGroup={isLastInGroup}
              onRetry={onRetry}
              typingPhase={typingPhase}
            />
          </div>
        );
      })}
      <div ref={bottomRef} />
    </div>
  );
}
