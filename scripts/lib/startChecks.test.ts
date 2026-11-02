import { describe, expect, test } from "bun:test";
import { hasModel, shouldPullModel } from "./startChecks";

describe("shouldPullModel", () => {
  test("auto and ollama pull", () => {
    expect(shouldPullModel("auto")).toBe(true);
    expect(shouldPullModel("ollama")).toBe(true);
  });

  test("openai does not", () => {
    expect(shouldPullModel("openai")).toBe(false);
  });

  test("unset is auto, which pulls", () => {
    expect(shouldPullModel(undefined)).toBe(true);
    expect(shouldPullModel("")).toBe(true);
  });
});

describe("hasModel", () => {
  const tags = { models: [{ name: "llama3.2:1b" }, { name: "mistral:latest" }] };

  test("matches an exact name", () => {
    expect(hasModel(tags, "llama3.2:1b")).toBe(true);
    expect(hasModel(tags, "llama3.2:3b")).toBe(false);
  });

  test("a model given without a tag means :latest", () => {
    expect(hasModel(tags, "mistral")).toBe(true);
    expect(hasModel(tags, "llama3.2")).toBe(false);
  });

  test("tolerates a malformed tags response", () => {
    expect(hasModel({}, "x")).toBe(false);
    expect(hasModel(null, "x")).toBe(false);
  });
});
