import { Bot } from "lucide-react";
import ConnectionStatus from "./ConnectionStatus";

type Status = "connecting" | "connected" | "disconnected";

interface ChatHeaderProps {
  status: Status;
}

export default function ChatHeader({ status }: ChatHeaderProps) {
  return (
    <header className="relative sticky top-0 z-10 flex items-center justify-between px-4 py-3 bg-(--surface-2) backdrop-blur-sm shrink-0">
      <div className="flex items-center gap-3">
        <div className="h-9 w-9 rounded-xl bg-linear-to-br from-cyan-400 to-teal-600 flex items-center justify-center shadow-md shadow-cyan-500/30 shrink-0">
          <Bot className="h-4 w-4 text-white" />
        </div>
        <div>
          <div className="text-sm font-semibold text-(--foreground) leading-tight">
            Kafka AI
          </div>
          <div className="flex items-center gap-1.5">
            <span className="text-xs text-(--foreground-2) leading-tight">
              Powered by Apache Kafka
            </span>
            <span className="text-[10px] font-mono px-1.5 py-0.5 rounded bg-(--surface-3) text-(--foreground-2) border border-(--border) leading-none">
              v3.8
            </span>
          </div>
        </div>
      </div>
      <ConnectionStatus status={status} />
      {/* Animated shimmer underline */}
      <div className="absolute bottom-0 left-0 right-0 h-px header-shimmer-line" />
    </header>
  );
}
