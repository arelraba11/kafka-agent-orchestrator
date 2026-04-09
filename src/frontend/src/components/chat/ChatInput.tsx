import { useState, useRef, useEffect } from "react";
import { Send, Loader2 } from "lucide-react";
import { Button } from "../ui/button";

const MAX_CHARS = 2000;
const LINE_HEIGHT = 24;
const MAX_LINES = 6;

interface ChatInputProps {
  onSend: (text: string) => void;
  disabled: boolean;
  isLoading: boolean;
  prefill?: string;
  onPrefillConsumed?: () => void;
}

export default function ChatInput({ onSend, disabled, isLoading, prefill, onPrefillConsumed }: ChatInputProps) {
  const [value, setValue] = useState("");
  const textareaRef = useRef<HTMLTextAreaElement>(null);

  useEffect(() => {
    if (prefill) {
      setValue(prefill);
      onPrefillConsumed?.();
      textareaRef.current?.focus();
    }
  }, [prefill]);

  useEffect(() => {
    const el = textareaRef.current;
    if (!el) return;
    el.style.height = "auto";
    el.style.height = Math.min(el.scrollHeight, LINE_HEIGHT * MAX_LINES) + "px";
  }, [value]);

  const handleSubmit = (e?: React.FormEvent) => {
    e?.preventDefault();
    const trimmed = value.trim();
    if (!trimmed || disabled || isLoading || trimmed.length > MAX_CHARS) return;
    onSend(trimmed);
    setValue("");
    if (textareaRef.current) {
      textareaRef.current.style.height = "auto";
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if ((e.key === "Enter" && !e.shiftKey) || (e.key === "Enter" && (e.metaKey || e.ctrlKey))) {
      e.preventDefault();
      handleSubmit();
    }
  };

  const isOverLimit = value.length > MAX_CHARS;
  const showCounter = value.length > MAX_CHARS * 0.75;
  const canSend = !disabled && !isLoading && value.trim().length > 0 && !isOverLimit;

  return (
    <form onSubmit={handleSubmit} className="px-4 pb-4 pt-2 bg-(--surface-2)">
      <div className="flex flex-col gap-2 rounded-2xl border border-(--border) bg-(--surface-3) p-3 focus-within:border-(--primary)/50 focus-within:ring-1 focus-within:ring-(--accent-glow) focus-within:[box-shadow:0_0_0_3px_oklch(0.74_0.14_195_/_0.18)] transition-all">
        <textarea
          ref={textareaRef}
          value={value}
          onChange={(e) => setValue(e.target.value)}
          onKeyDown={handleKeyDown}
          disabled={disabled}
          rows={1}
          placeholder="Ask me anything…"
          aria-label="Message input"
          aria-multiline="true"
          className="w-full resize-none bg-transparent text-sm text-(--foreground) placeholder:text-(--foreground-2) outline-none leading-relaxed disabled:opacity-50"
          style={{ minHeight: `${LINE_HEIGHT}px` }}
        />
        <div className="flex items-center justify-between">
          <span className="text-[10px] text-(--foreground-2) select-none">
            <kbd className="border border-(--border) rounded px-1 py-0.5 font-sans">Enter</kbd>
            {" "}to send · Shift+Enter for new line
          </span>
          <div className="flex items-center gap-2">
            {showCounter && (
              <span className={`text-[10px] tabular-nums ${isOverLimit ? "text-(--destructive)" : "text-(--foreground-2)"}`}>
                {value.length}/{MAX_CHARS}
              </span>
            )}
            <Button
              type="submit"
              size="icon"
              disabled={!canSend}
              aria-label={isLoading ? "Sending…" : "Send message"}
              className="h-7 w-7 rounded-xl"
            >
              {isLoading ? (
                <Loader2 className="h-3.5 w-3.5 animate-spin" />
              ) : (
                <Send className="h-3.5 w-3.5" />
              )}
            </Button>
          </div>
        </div>
      </div>
    </form>
  );
}
