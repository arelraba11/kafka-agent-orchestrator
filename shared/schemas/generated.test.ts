import { expect, test } from "bun:test";
import { MESSAGE_SCHEMAS } from "./index";
import { renderSchemas, staleFiles } from "../../scripts/gen-schemas";

test("shared/schemas/*.json match the zod schemas (run `bun run schemas:gen`)", () => {
  expect(staleFiles()).toEqual([]);
});

test("a JSON Schema file is generated for every registered message", () => {
  const files = [...renderSchemas().keys()].map((p) => p.split("/").pop());
  expect(files.sort()).toEqual(Object.keys(MESSAGE_SCHEMAS).map((n) => `${n}.json`).sort());
});
