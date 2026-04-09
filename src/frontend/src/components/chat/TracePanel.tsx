import { Activity, CheckCircle2, Circle } from "lucide-react";

type TraceStatus = "idle" | "thinking" | "processing" | "done";

export type TraceState = {
  status: TraceStatus;
  ackTime: number | null;
  answerTime: number | null;
  toolsUsed: string[];
};

interface TracePanelProps {
  trace: TraceState;
}

type StepStatus = "pending" | "active" | "done";

type TraceStep = {
  id: number;
  label: string;
  sublabel: string;
  activeIn: TraceStatus[];
  completedIn: TraceStatus[];
};

const STEPS: TraceStep[] = [
  {
    id: 1,
    label: "User Query",
    sublabel: "Received by WebServer",
    activeIn: ["thinking", "processing", "done"],
    completedIn: ["thinking", "processing", "done"],
  },
  {
    id: 2,
    label: "Router",
    sublabel: "Plan Generated",
    activeIn: ["thinking"],
    completedIn: ["processing", "done"],
  },
  {
    id: 3,
    label: "Orchestrator",
    sublabel: "Dispatching Tools",
    activeIn: ["thinking", "processing"],
    completedIn: ["done"],
  },
  {
    id: 4,
    label: "Tool Workers",
    sublabel: "Processing",
    activeIn: ["processing"],
    completedIn: ["done"],
  },
  {
    id: 5,
    label: "Synthesizer",
    sublabel: "Building Answer",
    activeIn: ["done"],
    completedIn: ["done"],
  },
];

const STEP_FRACTIONS: Record<number, number> = {
  1: 0,
  2: 0.25,
  3: 0.15,
  4: 0.50,
  5: 0.10,
};

function formatDuration(ms: number): string {
  if (ms < 1000) return `${ms}ms`;
  return `${(ms / 1000).toFixed(1)}s`;
}

function formatToolName(name: string): string {
  const map: Record<string, string> = {
    math: "Math",
    weather: "Weather",
    exchange: "Exchange",
    chat: "Chat",
    getProductInformation: "Product Info",
  };
  return map[name] ?? name;
}

export default function TracePanel({ trace }: TracePanelProps) {
  const { status, ackTime, answerTime, toolsUsed } = trace;
  const totalMs = ackTime && answerTime ? answerTime - ackTime : null;
  const isLive = status === "thinking" || status === "processing";

  function getStepStatus(step: TraceStep): StepStatus {
    if ((step.completedIn as string[]).includes(status)) return "done";
    if ((step.activeIn as string[]).includes(status)) return "active";
    return "pending";
  }

  return (
    <aside className="w-80 shrink-0 h-full border-l border-(--border) bg-(--surface) flex flex-col overflow-hidden">
      {/* Panel header */}
      <div className="flex items-center justify-between px-4 py-3 border-b border-(--border) bg-(--surface-2) shrink-0">
        <div className="flex items-center gap-2">
          <Activity className="h-4 w-4 text-(--primary)" />
          <span className="text-sm font-semibold text-(--foreground)">Event Trace</span>
        </div>
        {isLive && (
          <span className="flex items-center gap-1.5 text-[10px] font-medium text-(--success) bg-(--surface-3) px-2 py-0.5 rounded-full border border-(--border)">
            <span className="h-1.5 w-1.5 rounded-full bg-(--success) animate-pulse shrink-0" />
            LIVE
          </span>
        )}
      </div>

      {/* Pipeline steps */}
      <div className="flex-1 px-4 py-5 flex flex-col overflow-y-auto">
        {status === "idle" ? (
          <div className="flex flex-col items-center justify-center h-full gap-3 text-center">
            <Activity className="h-8 w-8 text-(--border-strong)" />
            <p className="text-xs text-(--foreground-2)">Waiting for a query…</p>
            <p className="text-[10px] text-(--border-strong)">Pipeline steps will appear here</p>
          </div>
        ) : (
          STEPS.map((step, index) => {
            const stepStatus = getStepStatus(step);
            const durationMs =
              stepStatus === "done" && totalMs !== null
                ? step.id === 1
                  ? 0
                  : Math.round(totalMs * STEP_FRACTIONS[step.id])
                : null;

            const showTools = step.id === 4 && toolsUsed.length > 0;

            return (
              <div key={step.id} className="flex gap-3">
                {/* Icon + connector line */}
                <div className="flex flex-col items-center">
                  <div
                    className={`h-5 w-5 rounded-full flex items-center justify-center shrink-0 mt-0.5 transition-all duration-500 ${
                      stepStatus === "done"
                        ? "bg-(--success)/15 border border-(--success)"
                        : stepStatus === "active"
                        ? "bg-(--primary)/15 border border-(--primary) trace-step-active"
                        : "bg-(--surface-2) border border-(--border)"
                    }`}
                  >
                    {stepStatus === "done" ? (
                      <CheckCircle2 className="h-3 w-3 text-(--success)" />
                    ) : stepStatus === "active" ? (
                      <span className="h-2 w-2 rounded-full bg-(--primary) animate-pulse" />
                    ) : (
                      <Circle className="h-3 w-3 text-(--border-strong)" />
                    )}
                  </div>
                  {index < STEPS.length - 1 && (
                    <div
                      className={`w-px flex-1 min-h-[20px] my-0.5 transition-colors duration-500 ${
                        stepStatus === "done" ? "bg-(--success)/30" : "bg-(--border)"
                      }`}
                    />
                  )}
                </div>

                {/* Step label + sublabel + tool badges */}
                <div className="pb-4 flex-1 min-w-0">
                  <div className="flex items-center justify-between gap-2">
                    <span
                      className={`text-xs font-medium transition-colors duration-300 truncate ${
                        stepStatus === "done"
                          ? "text-(--foreground)"
                          : stepStatus === "active"
                          ? "text-(--primary)"
                          : "text-(--foreground-2)"
                      }`}
                    >
                      {step.label}
                    </span>
                    {durationMs !== null && (
                      <span className="text-[10px] font-mono text-(--foreground-2) tabular-nums shrink-0">
                        {step.id === 1 ? "0ms" : formatDuration(durationMs)}
                      </span>
                    )}
                  </div>
                  <span className="text-[10px] text-(--foreground-2) leading-snug">
                    {step.sublabel}
                  </span>

                  {/* Tool badges — visible when tools are known */}
                  {showTools && (
                    <div className="flex flex-wrap gap-1 mt-2">
                      {toolsUsed.map((tool) => (
                        <span
                          key={tool}
                          className="inline-flex items-center gap-1 text-[10px] font-mono px-2 py-0.5 rounded-full bg-(--primary)/10 text-(--primary) border border-(--primary)/25"
                        >
                          <span className="h-1 w-1 rounded-full bg-(--primary) shrink-0" />
                          {formatToolName(tool)}
                        </span>
                      ))}
                    </div>
                  )}
                </div>
              </div>
            );
          })
        )}
      </div>

      {/* Total duration footer */}
      {status === "done" && totalMs !== null && (
        <div className="px-4 py-3 border-t border-(--border) bg-(--surface-2) shrink-0">
          <div className="flex items-center justify-between">
            <span className="text-xs text-(--foreground-2)">Total duration</span>
            <span className="text-xs font-mono font-semibold text-(--foreground) tabular-nums">
              {formatDuration(totalMs)}
            </span>
          </div>
        </div>
      )}
    </aside>
  );
}
