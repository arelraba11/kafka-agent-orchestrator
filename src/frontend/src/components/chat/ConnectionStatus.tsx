export type Status = "connecting" | "connected" | "disconnected";

const colors: Record<Status, string> = {
  connecting: "bg-gray-400",
  connected: "bg-green-500",
  disconnected: "bg-red-500",
};

const labels: Record<Status, string> = {
  connecting: "Connecting…",
  connected: "Connected",
  disconnected: "Disconnected — reconnecting…",
};

export function ConnectionStatus({ status }: { status: Status }) {
  return (
    <div className="flex items-center gap-2 text-sm text-[var(--muted-foreground)]">
      <span className={`h-2 w-2 rounded-full ${colors[status]}`} />
      {labels[status]}
    </div>
  );
}
