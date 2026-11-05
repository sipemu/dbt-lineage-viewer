// Headless check of the impact logic outside VS Code: runs the same code as
// "dbt-lineage: Show impact of my changes" (src/report.ts + src/render.ts)
// against a dbt project and asserts a report came out. Optionally writes the
// panel HTML (with copies of the webview assets) for a browser screenshot.
//
//   node scripts/test-impact.mjs <project-dir> [--base <ref>] [--binary <path>]
//                                [--html <out.html>] [--theme dark|light]
import esbuild from "esbuild";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const args = process.argv.slice(2);
const flag = (name, dflt) => {
  const i = args.indexOf(name);
  return i >= 0 ? args[i + 1] : dflt;
};
const project = args[0];
if (!project || project.startsWith("--")) {
  console.error("usage: test-impact.mjs <project-dir> [--base ref] [--binary path] [--html out.html]");
  process.exit(2);
}
const exe = process.platform === "win32" ? "dbt-lineage.exe" : "dbt-lineage";
const bundled = path.join(root, "bin", exe);
const binary = flag("--binary", fs.existsSync(bundled) ? bundled : "dbt-lineage");

// Bundle the two vscode-free modules into a temporary CJS file.
const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "dbt-lineage-test-"));
const entry = path.join(tmp, "entry.ts");
fs.writeFileSync(
  entry,
  `export * from ${JSON.stringify(path.join(root, "src", "report.ts"))};\n` +
    `export * from ${JSON.stringify(path.join(root, "src", "render.ts"))};\n`,
);
await esbuild.build({
  entryPoints: [entry],
  outfile: path.join(tmp, "lib.cjs"),
  bundle: true,
  platform: "node",
  format: "cjs",
  logLevel: "error",
  nodePaths: [path.join(root, "node_modules")],
});
const require = createRequire(import.meta.url);
const lib = require(path.join(tmp, "lib.cjs"));

const base = await lib.resolveBase(flag("--base", "auto"), project);
console.log(`binary: ${binary}\nproject: ${project}\nbase: ${base}`);
const result = await lib.runReport(binary, project, base);
for (const n of result.notices) {
  console.log(`notice: ${n}`);
}
const body = lib.renderReport(result.markdown);

assert.match(result.markdown, /## dbt-lineage report/, "report heading");
if (!result.notices.length) {
  assert.match(body, /<table>/, "a Markdown table was rendered");
  assert.match(body, /<pre class="mermaid">/, "Mermaid impact diagram present");
  console.log(`changed nodes: ${lib.changedCount(result.markdown)}`);
}
assert.doesNotMatch(body, /&lt;details&gt;/, "<details> handled");

const htmlOut = flag("--html");
if (htmlOut) {
  const dir = path.dirname(path.resolve(htmlOut));
  fs.mkdirSync(path.join(dir, "assets"), { recursive: true });
  fs.copyFileSync(path.join(root, "media", "impact.css"), path.join(dir, "assets", "impact.css"));
  fs.copyFileSync(path.join(root, "media", "impact.js"), path.join(dir, "assets", "impact.js"));
  fs.copyFileSync(
    require.resolve("mermaid/dist/mermaid.min.js", { paths: [root] }),
    path.join(dir, "assets", "mermaid.min.js"),
  );
  let html = lib.page({
    body,
    cspSource: "file:",
    nonce: "test-nonce",
    cssUri: "assets/impact.css",
    scriptUri: "assets/impact.js",
    mermaidUri: "assets/mermaid.min.js",
    subtitle: `${path.basename(path.resolve(project))} · working tree vs ${base}`,
    notices: result.notices,
  });
  if (flag("--theme", "dark") === "dark") {
    // A few VS Code "Dark Modern" theme variables, as the webview host sets them.
    const vars = {
      "--vscode-foreground": "#cccccc",
      "--vscode-editor-background": "#1f1f1f",
      "--vscode-descriptionForeground": "#9d9d9d",
      "--vscode-panel-border": "#3c3c3c",
      "--vscode-editorWidget-background": "#202020",
      "--vscode-textCodeBlock-background": "#2b2b2b",
      "--vscode-textLink-foreground": "#4daafc",
      "--vscode-button-background": "#0078d4",
      "--vscode-button-foreground": "#ffffff",
      "--vscode-font-family": "'Segoe UI', 'Ubuntu', 'Droid Sans', sans-serif",
      "--vscode-font-size": "13px",
    };
    const style = Object.entries(vars).map(([k, v]) => `${k}: ${v}`).join("; ");
    // VS Code sets the theme variables on <html> and the theme kind on <body>.
    html = html
      .replace('<html lang="en">', `<html lang="en" style="${style}">`)
      .replace("<body>", '<body class="vscode-dark">');
  } else {
    html = html.replace("<body>", `<body class="vscode-light">`);
  }
  fs.writeFileSync(htmlOut, html);
  console.log(`wrote ${htmlOut}`);
}
fs.rmSync(tmp, { recursive: true, force: true });
console.log("ok");
