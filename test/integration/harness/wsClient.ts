import { waitFor } from "./wait";

export interface WsResult {
  /** Every frame received, in order. */
  frames: any[];
  /** The conversationId from the `ack` frame. */
  cid: string;
  /** The terminal frame (`answer` or `error`). */
  final: any;
}

export interface Query {
  /** Frames received so far (live). */
  frames: any[];
  /** Resolves with the conversationId when the `ack` frame arrives. */
  cid: Promise<string>;
  /** Resolves with all frames when the terminal frame (`answer` or `error`) arrives. */
  result: Promise<WsResult>;
}

/**
 * Sends one query over the web server's WebSocket without waiting for the answer, so a
 * test can act while the conversation is in flight (it knows the conversationId early).
 */
export function query(port: number, input: string, timeoutMs = 20_000): Query {
  const frames: any[] = [];
  const ws = new WebSocket(`ws://127.0.0.1:${port}/ws`);
  let open = false;
  ws.onopen = () => {
    open = true;
  };
  ws.onmessage = (m) => frames.push(JSON.parse(String(m.data)));

  const sent = waitFor(() => open, { timeoutMs: 10_000, what: `WebSocket to open on port ${port}` }).then(() => ws.send(input));

  const cid = sent.then(() =>
    waitFor(() => frames.find((f) => f.type === "ack")?.conversationId as string | undefined, {
      timeoutMs: 10_000,
      what: `an ack frame for ${JSON.stringify(input.slice(0, 60))}`,
      context: () => `  frames so far: ${JSON.stringify(frames)}`,
    })
  );

  const result = sent.then(async () => {
    try {
      const final = await waitFor(() => frames.find((f) => f.type === "answer" || f.type === "error"), {
        timeoutMs,
        what: `a terminal WebSocket frame for ${JSON.stringify(input.slice(0, 60))}`,
        context: () => `  frames so far: ${JSON.stringify(frames)}`,
      });
      const ack = frames.find((f) => f.type === "ack");
      return { frames, cid: (ack?.conversationId ?? final.conversationId) as string, final };
    } finally {
      ws.close();
    }
  });
  // A test that fails earlier never awaits these; do not turn that into an unhandled rejection.
  cid.catch(() => {});
  result.catch(() => {});
  return { frames, cid, result };
}

/** Sends one query and waits for the terminal frame. */
export const ask = (port: number, input: string, timeoutMs?: number): Promise<WsResult> => query(port, input, timeoutMs).result;
