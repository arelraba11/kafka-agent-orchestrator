import { Bot, Cloud, TrendingUp, Calculator, Zap } from "lucide-react";

interface EmptyStateProps {
  onSuggestion: (text: string) => void;
}

const tiles = [
  {
    icon: Cloud,
    title: "Current weather",
    subtitle: "Ask about any city",
    query: "What's the weather in Tel Aviv?",
  },
  {
    icon: TrendingUp,
    title: "Exchange rates",
    subtitle: "USD, EUR, GBP...",
    query: "What is 1 USD to EUR?",
  },
  {
    icon: Calculator,
    title: "Math & calculations",
    subtitle: "Solve equations",
    query: "What is 15% of 847?",
  },
  {
    icon: Zap,
    title: "Quick answers",
    subtitle: "Facts and data",
    query: "Tell me something interesting",
  },
];

export default function EmptyState({ onSuggestion }: EmptyStateProps) {
  return (
    <div className="flex-1 flex flex-col items-center justify-center gap-6 p-8">
      <div className="h-16 w-16 rounded-2xl bg-linear-to-br from-violet-500 to-indigo-600 flex items-center justify-center shadow-lg shadow-violet-500/20">
        <Bot className="h-8 w-8 text-white" />
      </div>
      <div className="text-center space-y-2">
        <h2 className="text-2xl font-semibold text-(--foreground)">
          What can I help you with?
        </h2>
        <p className="text-sm text-(--foreground-2) max-w-xs">
          Ask me about weather, exchange rates, math calculations, or anything else.
        </p>
      </div>
      <div className="grid grid-cols-2 gap-3 w-full max-w-sm">
        {tiles.map(({ icon: Icon, title, subtitle, query }) => (
          <button
            key={title}
            onClick={() => onSuggestion(query)}
            aria-label={`Ask: ${query}`}
            className="bg-(--surface-2) hover:bg-(--surface-3) border border-(--border) hover:border-(--border-strong) rounded-xl p-4 text-left transition-colors cursor-pointer group focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-(--primary)"
          >
            <Icon className="h-5 w-5 text-(--primary) mb-2 group-hover:scale-110 transition-transform" />
            <div className="text-sm font-medium text-(--foreground)">{title}</div>
            <div className="text-xs text-(--foreground-2) mt-0.5">{subtitle}</div>
          </button>
        ))}
      </div>
    </div>
  );
}
