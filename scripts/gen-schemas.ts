// Generates shared/schemas/<Name>.json from the zod schemas (the source of truth).
//   bun scripts/gen-schemas.ts          write the files
//   bun scripts/gen-schemas.ts --check  exit 1 if any file on disk is stale
//
// io: "input" keeps objects open (additionalProperties is not forced to false), so
// the Python validator accepts the same extra fields the zod tolerant reader does.
import { existsSync, readFileSync, writeFileSync } from "fs";
import { join } from "path";
import { z } from "zod";
import { MESSAGE_SCHEMAS } from "../shared/schemas/index";

export const SCHEMAS_DIR = join(import.meta.dir, "..", "shared", "schemas");

/** Absolute path → file content, for every registered message. Deterministic. */
export function renderSchemas(): Map<string, string> {
  const out = new Map<string, string>();
  for (const [name, schema] of Object.entries(MESSAGE_SCHEMAS)) {
    const json = z.toJSONSchema(schema, { target: "draft-2020-12", io: "input" });
    const { $schema, ...rest } = json as Record<string, unknown>;
    // $schema first, then $id, then the generated body.
    const doc = { $schema, $id: name, ...rest };
    out.set(join(SCHEMAS_DIR, `${name}.json`), JSON.stringify(doc, null, 2) + "\n");
  }
  return out;
}

/** Paths whose on-disk content differs from the rendered content (or is missing). */
export function staleFiles(rendered = renderSchemas()): string[] {
  const stale: string[] = [];
  for (const [path, content] of rendered) {
    if (!existsSync(path) || readFileSync(path, "utf8") !== content) stale.push(path);
  }
  return stale;
}

if (import.meta.main) {
  const rendered = renderSchemas();
  if (process.argv.includes("--check")) {
    const stale = staleFiles(rendered);
    if (stale.length > 0) {
      console.error("Stale generated JSON Schema files (run `bun run schemas:gen`):");
      for (const f of stale) console.error(`  ${f}`);
      process.exit(1);
    }
    console.log(`schemas:check ok (${rendered.size} files up to date)`);
  } else {
    for (const [path, content] of rendered) writeFileSync(path, content);
    console.log(`schemas:gen wrote ${rendered.size} files to ${SCHEMAS_DIR}`);
  }
}
