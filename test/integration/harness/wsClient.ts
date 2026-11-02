import { waitFor } from "./wait";

export interface WsResult {
  /** Every frame received, in order. */
  frames: any[];
  /** The conversationId from the `ack` frame. */
  cid: string;
  /** The terminal frame (`answer` or `error`). */
  final: any;
}

/**
 * Sends one query over the web server's WebSocket and collects frames until the terminal
 * one (`answer` or `error`) arrives.
 */
export async function ask(port: number, input: string, timeoutMs = 20_000): Promise<WsResult> {
  const frames: any[] = [];
  const ws = new WebSocket(`ws://127.0.0.1:${port}/ws`);
  let open = false;
  ws.onopen = () => {
    open = true;
  };
  await waitFor(() => open, { timeoutMs: 10_000, what: `WebSocket to open on port ${port}` });
  ws.onmessage = (m) => frames.push(JSON.parse(String(m.data)));
  ws.send(input);
  try {
    const final = await waitFor(() => frames.find((f) => f.type === "answer" || f.type === "error"), {
      timeoutMs,
      what: `a terminal WebSocket frame for ${JSON.stringify(input.slice(0, 60))}`,
      context: () => `  frames so far: ${JSON.stringify(frames)}`,
    });
    const ack = frames.find((f) => f.type === "ack");
    return { frames, cid: ack?.conversationId ?? final.conversationId, final };
  } finally {
    ws.close();
  }
}
