type Status = "connecting" | "connected" | "disconnected";

interface ConnectionStatusProps {
  status: Status;
}

const config: Record<Status, { pill: string; dot: string; label: string }> = {
  connected: {
    pill: "bg-[oklch(0.72_0.18_145_/_0.15)] text-(--success) border border-[oklch(0.72_0.18_145_/_0.3)]",
    dot: "bg-(--success) animate-pulse",
    label: "Live",
  },
  connecting: {
    pill: "bg-[oklch(0.78_0.15_70_/_0.15)] text-(--warning) border border-[oklch(0.78_0.15_70_/_0.3)]",
    dot: "bg-(--warning) animate-pulse",
    label: "Connecting",
  },
  disconnected: {
    pill: "bg-(--destructive-surface) text-(--destructive) border border-(--destructive)",
    dot: "bg-(--destructive)",
    label: "Offline",
  },
};

export default function ConnectionStatus({ status }: ConnectionStatusProps) {
  const { pill, dot, label } = config[status];
  return (
    <div
      role="status"
      aria-live="polite"
      aria-label={`Connection status: ${label}`}
      className={`flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium transition-all duration-300 ${pill}`}
    >
      <span className={`h-1.5 w-1.5 rounded-full shrink-0 ${dot}`} />
      {label}
    </div>
  );
}
