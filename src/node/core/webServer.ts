import {
  createProducer,
  createConsumer,
  sendMessage,
  subscribeAndRun,
  registerShutdown,
} from "../../../shared/kafka/client";
import { TOPICS } from "../../../shared/topics";
import { join } from "path";

const PORT = 3001;
const CONSUMER_GROUP = "ui-web-final-answer";
const DIST_DIR = join(import.meta.dir, "../../../src/frontend/dist");

// Map<conversationId, WebSocket> — tracks in-flight requests
// eslint-disable-next-line @typescript-eslint/no-explicit-any
const pending = new Map<string, any>();

// Map<WebSocket, sessionId> — one sessionId per browser tab / connection
// eslint-disable-next-line @typescript-eslint/no-explicit-any
const sessions = new Map<any, string>();

const producer = await createProducer();
const consumer = await createConsumer(CONSUMER_GROUP);
registerShutdown([producer, consumer]);

// Start consumer BEFORE Bun.serve — eliminates race window where a fast pipeline
// could emit FinalAnswerSynthesized before the pending map entry exists.
await subscribeAndRun(
  consumer,
  [TOPICS.CONVERSATION_EVENTS],
  async (_topic, _key, value) => {
    const event = value as {
      eventType: string;
      conversationId: string;
      payload: Record<string, unknown>;
    };

    const ws = pending.get(event.conversationId);
    if (!ws) return; // belongs to terminal UI or another client

    if (event.eventType === "ToolInvocationResulted") {
      ws.send(JSON.stringify({ type: "tool", toolName: event.payload.toolName }));
    }

    if (event.eventType === "FinalAnswerSynthesized") {
      pending.delete(event.conversationId);
      ws.send(JSON.stringify({ type: "answer", answer: event.payload.answer }));
    }

    if (event.eventType === "PlanFailed") {
      pending.delete(event.conversationId);
      ws.send(
        JSON.stringify({
          type: "error",
          reason: event.payload.reason,
          failedTool: event.payload.failedTool,
        })
      );
    }
  }
);

Bun.serve({
  port: PORT,

  async fetch(req, server) {
    const { pathname } = new URL(req.url);

    if (pathname === "/ws") {
      server.upgrade(req);
      return undefined;
    }

    // Serve built React app assets
    const filePath = join(DIST_DIR, pathname === "/" ? "index.html" : pathname);
    const file = Bun.file(filePath);
    if (await file.exists()) return new Response(file);

    // SPA fallback — let React Router handle unknown paths
    return new Response(Bun.file(join(DIST_DIR, "index.html")), {
      headers: { "Content-Type": "text/html; charset=utf-8" },
    });
  },

  websocket: {
    open(ws) {
      sessions.set(ws, crypto.randomUUID());
      console.log("[webserver] client connected");
    },

    async message(ws, raw) {
      const userInput = (
        typeof raw === "string" ? raw : new TextDecoder().decode(raw)
      ).trim();
      if (!userInput) return;

      const conversationId = crypto.randomUUID();
      const sessionId = sessions.get(ws) ?? crypto.randomUUID();

      // Register BEFORE producing — closes race window
      pending.set(conversationId, ws);

      await sendMessage(producer, TOPICS.USER_COMMANDS, conversationId, {
        conversationId,
        timestamp: Date.now(),
        eventType: "UserQueryReceived",
        payload: { userInput, sessionId },
      });

      ws.send(JSON.stringify({ type: "ack", conversationId }));
      console.log(`[webserver] conversationId=${conversationId} sessionId=${sessionId} query="${userInput}"`);
    },

    close(ws) {
      // Purge all in-flight entries for this socket (tab closed mid-request)
      for (const [id, socket] of pending.entries()) {
        if (socket === ws) pending.delete(id);
      }
      sessions.delete(ws);
      console.log("[webserver] client disconnected");
    },
  },
});

console.log(`[webserver] Listening on http://localhost:${PORT}`);
