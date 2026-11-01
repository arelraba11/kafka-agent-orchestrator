import {
  createProducer,
  createConsumer,
  publish,
  subscribeAndRun,
  registerShutdown,
} from "../../../shared/kafka/client";
import { TOPICS } from "../../../shared/topics";
import type { UserQueryReceived } from "../../../shared/schemas/UserQueryReceived";
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
await subscribeAndRun({
  consumer,
  groupId: CONSUMER_GROUP,
  service: "web",
  producer,
  topic: TOPICS.CONVERSATION_EVENTS,
  types: ["ToolInvocationResulted", "FinalAnswerSynthesized", "PlanFailed"],
  handler: async (event) => {
    const ws = pending.get(event.conversationId);
    if (!ws) return; // belongs to another client

    switch (event.eventType) {
      case "ToolInvocationResulted":
        ws.send(JSON.stringify({ type: "tool", toolName: event.payload.toolName }));
        break;
      case "FinalAnswerSynthesized":
        pending.delete(event.conversationId);
        ws.send(JSON.stringify({ type: "answer", answer: event.payload.answer }));
        break;
      case "PlanFailed":
        pending.delete(event.conversationId);
        ws.send(
          JSON.stringify({
            type: "error",
            reason: event.payload.reason,
            failedTool: event.payload.failedTool,
          })
        );
        break;
    }
  },
});

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

    // SPA fallback — serve index.html for unknown paths
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

      const queryEvent: UserQueryReceived = {
        conversationId,
        timestamp: Date.now(),
        eventType: "UserQueryReceived",
        payload: { userInput, sessionId },
      };
      // Not inside a consumer handler: there is no input message to dead-letter. A
      // contract violation (or any send failure) is logged and the frame dropped.
      try {
        await publish(producer, TOPICS.USER_COMMANDS, queryEvent);
      } catch (err) {
        pending.delete(conversationId);
        console.error(
          `[webserver] conversationId=${conversationId} dropped frame: ${err instanceof Error ? `${err.name}: ${err.message}` : String(err)}`
        );
        return;
      }

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
