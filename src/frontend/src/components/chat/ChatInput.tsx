import { useForm } from "react-hook-form";
import { Button } from "../ui/button";
import { Send } from "lucide-react";

type FormValues = { message: string };

export function ChatInput({
  onSend,
  disabled,
}: {
  onSend: (text: string) => void;
  disabled: boolean;
}) {
  const { register, handleSubmit, reset } = useForm<FormValues>();

  const submit = ({ message }: FormValues) => {
    if (!message.trim()) return;
    onSend(message.trim());
    reset();
  };

  return (
    <form
      onSubmit={handleSubmit(submit)}
      className="flex gap-2 p-4 border-t border-[var(--border)]"
    >
      <textarea
        {...register("message")}
        disabled={disabled}
        rows={1}
        placeholder="Ask something…"
        className="flex-1 resize-none rounded-md border border-[var(--border)] bg-[var(--muted)] px-3 py-2 text-sm outline-none focus:ring-2 focus:ring-[var(--primary)] disabled:opacity-50"
        onKeyDown={(e) => {
          if (e.key === "Enter" && !e.shiftKey) {
            e.preventDefault();
            handleSubmit(submit)();
          }
        }}
      />
      <Button type="submit" size="icon" disabled={disabled}>
        <Send className="h-4 w-4" />
      </Button>
    </form>
  );
}
