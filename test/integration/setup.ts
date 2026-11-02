// Preload for `bun run test:int` (bun --no-env-file test … --preload ./test/integration/setup.ts).
// Brings the dedicated test broker up once for the whole multi-file run and removes it at the
// end (KEEP_BROKER=1 keeps it for local iteration). Aborts if a real API key is reachable.
import { afterAll, beforeAll } from "bun:test";
import { brokerDown, brokerUp, TEST_PORT } from "./harness/broker";
import { FAKE_API_KEY } from "./harness/fakeLlm";
import { runId } from "./harness/ids";

const key = process.env.OPENAI_API_KEY;
if (key !== undefined && key !== "" && key !== FAKE_API_KEY) {
  throw new Error(
    "OPENAI_API_KEY is set to a real value in this environment. The integration suite never uses a real key: " +
      "run it through `bun run test:int` (bun --no-env-file) from a shell without OPENAI_API_KEY."
  );
}

runId(); // one id for the whole run, shared by every test file

beforeAll(async () => {
  await brokerUp();
  console.log(`[int] test broker up on localhost:${TEST_PORT} (run ${runId()})`);
}, 90_000);

afterAll(async () => {
  if (process.env.KEEP_BROKER === "1") {
    console.log("[int] KEEP_BROKER=1: leaving the test broker running");
    return;
  }
  await brokerDown();
}, 60_000);
