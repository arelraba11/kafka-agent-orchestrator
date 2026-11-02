import {
  createProducer,
  createConsumer,
  publish,
  subscribeAndRun,
  registerShutdown,
} from "../../../shared/kafka/client";
import { TOPICS } from "../../../shared/topics";
import type { UserQueryReceived } from "../../../shared/schemas/UserQueryReceived";
import type { ConversationFailed } from "../../../shared/schemas/ConversationFailed";
import { WEB_OWNED_TYPES } from "../../../shared/kafka/failureOwnership";
import { stepTimeoutMs } from "../../../shared/stepTimeout";
import { webPort } from "../../../shared/webPort";
import { answerFrame, toClientFrame, toolFrame, unavailableFrame } from "./clientFrames";
import { sweepPending } from "./pendingSweep";
import { join } from "path";

const PORT = webPort();
const CONSUMER_GROUP = "ui-web-final-answer";
const DIST_DIR = join(import.meta.dir, "../../../src/frontend/dist");
const FRAME_CONFIG = { stepTimeoutMs: stepTimeoutMs() };

// Map<conversationId, {ws, at}> — tracks in-flight requests. `at` feeds the TTL sweeper.
// eslint-disable-next-line @typescript-eslint/no-explicit-any
const pending = new Map<string, { ws: any; at: number }>();

// Memory hygiene only (the client's idle timeout is what tells the user): drop entries
// whose answer never came.
setInterval(() => {
  for (const id of sweepPending(pending, Date.now())) {
    console.warn(`[webserver] conversationId=${id} dropped pending entry (ttl)`);
  }
}, 30_000);

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
  types: ["ToolInvocationResulted", "FinalAnswerSynthesized", "PlanFailed", "ConversationFailed"],
  // A poison final answer or failure event can never reach the user as itself: if the
  // conversation is still pending, send a direct error frame (no new event).
  onDeadLettered: {
    owns: WEB_OWNED_TYPES,
    handle: async (record) => {
      const conversationId = record.conversationId!;
      const ws = pending.get(conversationId)?.ws;
      if (!ws) return;
      pending.delete(conversationId);
      const failed: ConversationFailed = {
        conversationId,
        timestamp: Date.now(),
        eventType: "ConversationFailed",
        payload: {
          stage: "synthesis",
          code: "internal-error",
          reason: `${record.payload.error.class}: ${record.payload.error.reason}`,
        },
      };
      ws.send(JSON.stringify(toClientFrame(failed, FRAME_CONFIG)));
    },
  },
  handler: async (event) => {
    const ws = pending.get(event.conversationId)?.ws;
    if (!ws) return; // belongs to another client

    switch (event.eventType) {
      case "ToolInvocationResulted":
        ws.send(JSON.stringify(toolFrame(event.conversationId, event.payload.toolName)));
        break;
      case "FinalAnswerSynthesized":
        pending.delete(event.conversationId);
        ws.send(JSON.stringify(answerFrame(event.conversationId, event.payload.answer)));
        break;
      case "PlanFailed":
      case "ConversationFailed":
        pending.delete(event.conversationId);
        ws.send(JSON.stringify(toClientFrame(event, FRAME_CONFIG)));
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
      pending.set(conversationId, { ws, at: Date.now() });

      const queryEvent: UserQueryReceived = {
        conversationId,
        timestamp: Date.now(),
        eventType: "UserQueryReceived",
        payload: { userInput, sessionId },
      };
      // Not inside a consumer handler: there is no input message to dead-letter. A
      // publish failure (Kafka down, contract violation) is logged and the user gets an
      // `unavailable` error frame instead of waiting for an answer that cannot come.
      try {
        await publish(producer, TOPICS.USER_COMMANDS, queryEvent);
      } catch (err) {
        pending.delete(conversationId);
        console.error(
          `[webserver] conversationId=${conversationId} publish failed: ${err instanceof Error ? `${err.name}: ${err.message}` : String(err)}`
        );
        ws.send(JSON.stringify(unavailableFrame(conversationId)));
        return;
      }

      ws.send(JSON.stringify({ type: "ack", conversationId }));
      console.log(`[webserver] conversationId=${conversationId} sessionId=${sessionId} query="${userInput}"`);
    },

    close(ws) {
      // Purge all in-flight entries for this socket (tab closed mid-request)
      for (const [id, entry] of pending.entries()) {
        if (entry.ws === ws) pending.delete(id);
      }
      sessions.delete(ws);
      console.log("[webserver] client disconnected");
    },
  },
});

console.log(`[webserver] Listening on http://localhost:${PORT}`);
