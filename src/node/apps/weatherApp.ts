import {
  createProducer,
  createConsumer,
  sendMessage,
  subscribeAndRun,
  registerShutdown,
} from "../../../shared/kafka/client";
import { TOPICS } from "../../../shared/topics";
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
  "sao paulo":   { temp: 27, condition: "warm and humid" },
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
const consumer = await createConsumer("weather-tool-worker");

registerShutdown([producer, consumer]);

await subscribeAndRun(
  consumer,
  [TOPICS.TOOL_INVOCATION_REQUESTS],
  async (_topic, _key, value) => {
    const req = value as ToolInvocationRequested;
    if (req.payload.toolName !== "weather") return;

    const { conversationId } = req;
    const city = (req.payload.input.city as string) ?? "Tel Aviv";

    console.log(`[weather] conversationId=${conversationId} city="${city}"`);

    try {
      const data = getMockWeather(city);
      const resultStr = formatResult(city, data);

      console.log(`[weather] result="${resultStr}"`);

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
    } catch (err) {
      const errorMsg = err instanceof Error ? err.message : String(err);
      console.error(`[weather] error="${errorMsg}"`);

      const event: ToolInvocationResulted = {
        conversationId,
        timestamp: Date.now(),
        eventType: "ToolInvocationResulted",
        payload: {
          toolName: "weather",
          result: { value: "", success: false, error: errorMsg },
        },
      };

      await sendMessage(producer, TOPICS.CONVERSATION_EVENTS, conversationId, event);
    }
  }
);

console.log("[weather] WeatherApp started.");
