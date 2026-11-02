import { describe, expect, test } from "bun:test";
import { needsWebBuild } from "./webBuild";

describe("needsWebBuild", () => {
  test("builds when dist/index.html is missing", () => {
    expect(needsWebBuild(null, [1, 2, 3])).toBe(true);
    expect(needsWebBuild(null, [])).toBe(true);
  });

  test("builds when any source is newer than the build", () => {
    expect(needsWebBuild(100, [50, 101, 20])).toBe(true);
  });

  test("does not build when the build is newer than every source", () => {
    expect(needsWebBuild(100, [50, 99, 20])).toBe(false);
  });

  test("a source with the same mtime as the build is not stale", () => {
    expect(needsWebBuild(100, [100])).toBe(false);
  });

  test("does not build when there are no sources and a build exists", () => {
    expect(needsWebBuild(100, [])).toBe(false);
  });
});
