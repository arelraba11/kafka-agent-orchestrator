interface SuggestionChipsProps {
  onSend: (text: string) => void;
  disabled: boolean;
}

const chips = [
  { label: "Weather in Paris", query: "What's the weather in Paris?" },
  { label: "USD → EUR rate", query: "What is 1 USD to EUR?" },
  { label: "15% of 847", query: "What is 15% of 847?" },
];

export default function SuggestionChips({ onSend, disabled }: SuggestionChipsProps) {
  return (
    <div className="flex gap-2 px-4 pt-2 overflow-x-auto flex-wrap">
      {chips.map(({ label, query }) => (
        <button
          key={label}
          onClick={() => onSend(query)}
          disabled={disabled}
          aria-label={`Send: ${query}`}
          className="shrink-0 px-3 py-1.5 rounded-full text-xs font-medium bg-(--surface-2) border border-(--border) text-(--foreground-2) hover:border-(--primary) hover:text-(--primary) transition-colors disabled:opacity-40 disabled:cursor-not-allowed whitespace-nowrap focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-(--primary)"
        >
          {label}
        </button>
      ))}
    </div>
  );
}
