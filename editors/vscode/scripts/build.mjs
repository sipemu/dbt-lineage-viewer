// Bundle the extension with esbuild into out/extension.js and copy Mermaid's
// browser build next to it, so the package needs no node_modules.
//
//   node scripts/build.mjs [--production] [--watch]
import esbuild from "esbuild";
import fs from "node:fs";
import path from "node:path";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const production = process.argv.includes("--production");
const watch = process.argv.includes("--watch");

const out = path.join(root, "out");
fs.mkdirSync(out, { recursive: true });
const require = createRequire(import.meta.url);
fs.copyFileSync(
  require.resolve("mermaid/dist/mermaid.min.js"),
  path.join(out, "mermaid.min.js"),
);

const options = {
  entryPoints: [path.join(root, "src", "extension.ts")],
  outfile: path.join(out, "extension.js"),
  bundle: true,
  platform: "node",
  format: "cjs",
  target: "node18",
  external: ["vscode"],
  minify: production,
  sourcemap: !production,
  logLevel: "info",
};

if (watch) {
  const ctx = await esbuild.context(options);
  await ctx.watch();
} else {
  await esbuild.build(options);
}
