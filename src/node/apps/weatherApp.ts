import {
  createProducer,
  createConsumer,
  sendMessage,
  subscribeAndRun,
  registerShutdown,
} from "../../../shared/kafka/client";
import { TOPICS } from "../../../shared/topics";
import type { IntentWeatherEvent, AppResultEvent } from "../../../shared/types/events";
import type { ToolInvocationRequested } from "../../../shared/schemas/ToolInvocationRequested";
import type { ToolInvocationResulted } from "../../../shared/schemas/ToolInvocationResulted";

// ─── Mock weather data ────────────────────────────────────────────────────────

interface WeatherData {
  temp: number;
  condition: string;
}

const MOCK_WEATHER: Record<string, WeatherData> = {
  "tel aviv":    { temp: 28, condition: "sunny" },
  "jerusalem":   { temp: 22, condition: "partly cloudy" },
  "haifa":       { temp: 25, condition: "breezy" },
  "eilat":       { temp: 35, condition: "hot and clear" },
  "new york":    { temp: 15, condition: "cloudy" },
  "london":      { temp: 12, condition: "rainy" },
  "paris":       { temp: 17, condition: "overcast" },
  "berlin":      { temp: 10, condition: "windy" },
  "tokyo":       { temp: 20, condition: "humid" },
  "dubai":       { temp: 38, condition: "hot and sunny" },
};

const DEFAULT_WEATHER: WeatherData = { temp: 20, condition: "clear" };

function getMockWeather(city: string): WeatherData {
  return MOCK_WEATHER[city.toLowerCase()] ?? DEFAULT_WEATHER;
}

function formatResult(city: string, data: WeatherData): string {
  return `Weather in ${city} is ${data.temp}°C and ${data.condition}.`;
}

// ─── Main ────────────────────────────────────────────────────────────────────

const producer = await createProducer();
const consumer = await createConsumer("weather-service");
const consumerTool = await createConsumer("weather-tool-worker");

registerShutdown([producer, consumer, consumerTool]);

// ── Ex1/2: intent-weather consumer (unchanged) ────────────────────────────────

await subscribeAndRun(
  consumer,
  [TOPICS.INTENT_WEATHER],
  async (_topic, _key, value) => {
    const event = value as IntentWeatherEvent;
    const { userId, city } = event;

    console.log(`[weather] userId=${userId} city="${city}"`);

    let payload: AppResultEvent;

    try {
      const data = getMockWeather(city);
      const result = formatResult(city, data);

      payload = {
        userId,
        type: "weather",
        result,
        success: true,
        timestamp: new Date().toISOString(),
      };

      console.log(`[weather] result="${result}"`);
    } catch (err) {
      const error = err instanceof Error ? err.message : String(err);

      payload = {
        userId,
        type: "weather",
        result: "",
        success: false,
        error,
        timestamp: new Date().toISOString(),
      };

      console.error(`[weather] error="${error}"`);
    }

    await sendMessage(producer, TOPICS.APP_RESULTS, userId, payload);
  }
);

// ── Final Project: tool-invocation-requests consumer ─────────────────────────

subscribeAndRun(
  consumerTool,
  [TOPICS.TOOL_INVOCATION_REQUESTS],
  async (_topic, _key, value) => {
    const req = value as ToolInvocationRequested;
    if (req.payload.toolName !== "weather") return;

    const { conversationId } = req;
    const city = (req.payload.input.city as string) ?? "Tel Aviv";

    console.log(`[weather] tool conversationId=${conversationId} city="${city}"`);

    const data = getMockWeather(city);
    const resultStr = formatResult(city, data);

    console.log(`[weather] tool result="${resultStr}"`);

    const event: ToolInvocationResulted = {
      conversationId,
      timestamp: Date.now(),
      eventType: "ToolInvocationResulted",
      payload: {
        toolName: "weather",
        result: { value: resultStr, city, temp: data.temp, condition: data.condition, success: true },
      },
    };

    await sendMessage(producer, TOPICS.CONVERSATION_EVENTS, conversationId, event);
  }
).catch(err => console.error("[weather] Tool worker error:", err));

console.log("[weather] WeatherApp started.");
