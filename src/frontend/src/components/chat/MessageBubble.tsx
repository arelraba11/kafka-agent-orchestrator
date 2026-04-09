import { useState } from "react";
import { Bot, AlertCircle, Copy, Check } from "lucide-react";
import ReactMarkdown from "react-markdown";
import rehypeHighlight from "rehype-highlight";
import TypingIndicator from "./TypingIndicator";
import { Button } from "../ui/button";
import { formatRelativeTime } from "../../lib/time";

export type Message = {
  id: string;
  role: "user" | "bot" | "error";
  content: string;
  pending?: boolean;
  timestamp: number;
};

interface MessageBubbleProps {
  message: Message;
  isFirstInGroup: boolean;
  isLastInGroup: boolean;
  onRetry?: () => void;
  typingPhase?: "thinking" | "processing" | null;
}

export default function MessageBubble({
  message,
  isFirstInGroup: _isFirstInGroup,
  isLastInGroup,
  onRetry,
  typingPhase,
}: MessageBubbleProps) {
  const [copied, setCopied] = useState(false);

  const handleCopy = async () => {
    await navigator.clipboard.writeText(message.content);
    setCopied(true);
    setTimeout(() => setCopied(false), 1500);
  };

  const bubbleRadius = (base: string, isLast: boolean, side: "right" | "left") => {
    if (side === "right") {
      return `${base} ${isLast ? "rounded-br-sm" : ""}`;
    }
    return `${base} ${isLast ? "rounded-bl-sm" : ""}`;
  };

  if (message.role === "user") {
    return (
      <div className="animate-message flex flex-col items-end gap-1">
        <div
          className={bubbleRadius(
            "max-w-[72%] px-4 py-3 text-sm leading-relaxed bg-linear-to-br from-cyan-500 to-teal-600 text-white shadow-lg shadow-cyan-500/25 rounded-2xl",
            isLastInGroup,
            "right"
          )}
        >
          {message.content}
        </div>
        {isLastInGroup && (
          <span className="text-[10px] text-(--foreground-2) mr-1">
            {formatRelativeTime(message.timestamp)}
          </span>
        )}
      </div>
    );
  }

  if (message.role === "error") {
    return (
      <div className="animate-message flex items-end gap-2">
        <div className="h-7 w-7 rounded-full bg-(--destructive-surface) border border-(--destructive) flex items-center justify-center shrink-0">
          <AlertCircle className="h-3.5 w-3.5 text-(--destructive)" />
        </div>
        <div className="max-w-[72%]">
          <div className="bg-(--destructive-surface) border border-(--destructive) rounded-2xl rounded-bl-sm px-4 py-3 text-sm text-(--foreground)">
            <div className="flex items-start gap-2">
              <AlertCircle className="h-3.5 w-3.5 text-(--destructive) mt-0.5 shrink-0" />
              <span>{message.content}</span>
            </div>
            {onRetry && (
              <button
                onClick={onRetry}
                className="mt-2 text-xs text-(--primary) hover:underline focus-visible:outline-none"
              >
                Try again
              </button>
            )}
          </div>
          <span className="text-[10px] text-(--foreground-2) ml-1 mt-1 block">
            {formatRelativeTime(message.timestamp)}
          </span>
        </div>
      </div>
    );
  }

  // Bot message
  return (
    <div className="animate-message flex items-end gap-2">
      {isLastInGroup ? (
        <div className="h-7 w-7 rounded-full bg-linear-to-br from-cyan-400 to-teal-600 flex items-center justify-center shrink-0 shadow-sm shadow-cyan-500/20">
          <Bot className="h-3.5 w-3.5 text-white" />
        </div>
      ) : (
        <div className="h-7 w-7 shrink-0" />
      )}
      <div className="max-w-[72%]">
        <div
          className={bubbleRadius(
            "relative group bg-(--surface-3) border border-(--border) rounded-2xl px-4 py-3 text-sm text-(--foreground)",
            isLastInGroup,
            "left"
          )}
          style={{ borderLeftColor: "oklch(0.74 0.14 195 / 0.5)", borderLeftWidth: "2px" }}
        >
          {message.pending ? (
            <TypingIndicator phase={typingPhase} />
          ) : (
            <>
              <div className="prose-chat">
                <ReactMarkdown rehypePlugins={[rehypeHighlight]}>
                  {message.content}
                </ReactMarkdown>
              </div>
              <Button
                variant="subtle"
                size="xs"
                onClick={handleCopy}
                aria-label={copied ? "Copied!" : "Copy message"}
                className="absolute top-2 right-2 opacity-0 group-hover:opacity-100 transition-opacity h-6 w-6"
              >
                {copied ? (
                  <Check className="h-3 w-3 text-(--success)" />
                ) : (
                  <Copy className="h-3 w-3" />
                )}
              </Button>
            </>
          )}
        </div>
        {isLastInGroup && !message.pending && (
          <span className="text-[10px] text-(--foreground-2) ml-1 mt-1 block">
            {formatRelativeTime(message.timestamp)}
          </span>
        )}
      </div>
    </div>
  );
}
