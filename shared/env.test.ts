import { describe, expect, test } from "bun:test";
import { envInt } from "./env";

describe("envInt", () => {
  test("unset or empty is undefined", () => {
    expect(envInt({}, "X")).toBeUndefined();
    expect(envInt({ X: "" }, "X")).toBeUndefined();
  });
  test("a positive integer is parsed", () => {
    expect(envInt({ X: "2500" }, "X")).toBe(2500);
  });
  test.each(["abc", "0", "-5", "1.5", "NaN"])("%s is rejected with the variable name", (bad) => {
    expect(() => envInt({ X: bad }, "X")).toThrow(/X/);
  });
});
