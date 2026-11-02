import { describe, expect, test } from "bun:test";
import { webPort } from "./webPort";

describe("webPort", () => {
  test("defaults to 3001", () => expect(webPort({})).toBe(3001));
  test("reads WEB_PORT", () => expect(webPort({ WEB_PORT: "45678" })).toBe(45678));
  test("rejects garbage", () => expect(() => webPort({ WEB_PORT: "x" })).toThrow(/WEB_PORT/));
});
