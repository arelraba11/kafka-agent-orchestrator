import { Sparkles } from "lucide-react";
import ConnectionStatus from "./ConnectionStatus";

type Status = "connecting" | "connected" | "disconnected";

interface ChatHeaderProps {
  status: Status;
}

export default function ChatHeader({ status }: ChatHeaderProps) {
  return (
    <header className="sticky top-0 z-10 flex items-center justify-between px-4 py-3 bg-(--surface-2) border-b border-(--border) backdrop-blur-sm">
      <div className="flex items-center gap-3">
        <div className="h-9 w-9 rounded-xl bg-linear-to-br from-violet-500 to-indigo-600 flex items-center justify-center shadow-md shadow-violet-500/20 shrink-0">
          <Sparkles className="h-4 w-4 text-white" />
        </div>
        <div>
          <div className="text-sm font-semibold text-(--foreground) leading-tight">
            Kafka AI
          </div>
          <div className="text-xs text-(--foreground-2) leading-tight">
            Powered by Apache Kafka
          </div>
        </div>
      </div>
      <ConnectionStatus status={status} />
    </header>
  );
}
