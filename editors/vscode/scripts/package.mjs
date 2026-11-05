// Build one .vsix per VS Code target, each with the matching dbt-lineage
// binary in bin/, plus a universal .vsix without a binary (it uses
// dbt-lineage from PATH) for platforms without a release build.
//
//   node scripts/package.mjs [target ...]     (default: all)
//
// Output: dist/dbt-lineage-viewer-<target>.vsix
import { execFileSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ALL = ["linux-x64", "linux-arm64", "darwin-x64", "darwin-arm64", "win32-x64", "universal"];
const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const targets = process.argv.slice(2).length ? process.argv.slice(2) : ALL;
for (const t of targets) {
  if (!ALL.includes(t)) {
    console.error(`unknown target ${t}; expected one of ${ALL.join(", ")}`);
    process.exit(2);
  }
}

const npx = process.platform === "win32" ? "npx.cmd" : "npx";
const run = (cmd, args) => execFileSync(cmd, args, { cwd: root, stdio: "inherit" });

fs.mkdirSync(path.join(root, "dist"), { recursive: true });
for (const target of targets) {
  const out = path.join("dist", `dbt-lineage-viewer-${target}.vsix`);
  const args = ["vsce", "package", "--no-dependencies", "--out", out];
  if (target === "universal") {
    fs.rmSync(path.join(root, "bin"), { recursive: true, force: true });
  } else {
    run(process.execPath, [path.join(root, "scripts", "fetch-binary.mjs"), target]);
    args.push("--target", target);
  }
  run(npx, args);
  console.log(`${out}: ${(fs.statSync(path.join(root, out)).size / 1048576).toFixed(1)} MB`);
}
fs.rmSync(path.join(root, "bin"), { recursive: true, force: true });
